package service

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/metrico/quackpipe/model"
	"github.com/metrico/quackpipe/service/db"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"html/template"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var CHSQL_VER = "v1.0.10"

const CHSQL_EXT_URL = "https://github.com/quackscience/duckdb-extension-clickhouse-sql/releases/download/{{.VER}}/chsql.{{.DUCKDB_VER}}.{{.ARCH}}.duckdb_extension"

type mergeService interface {
	GetFilesToMerge(iteration int) ([]FileDesc, error)
	PlanMerge([]FileDesc, int64, int) []PlanMerge
	DoMerge([]PlanMerge) error
}

type fsMergeService struct {
	dataPath string
	tmpPath  string
	table    *model.Table
}

func (f *fsMergeService) GetFilesToMerge(iteration int) ([]FileDesc, error) {
	dataDir := f.dataPath
	files, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}
	var parquetFiles []FileDesc
	suffix := fmt.Sprintf("%d.parquet", iteration)
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), suffix) {
			continue
		}
		name := filepath.Join(dataDir, file.Name())
		stat, err := os.Stat(name)
		if err != nil {
			return nil, err
		}
		if f.table.Index != nil {
			abs, err := filepath.Abs(name)
			if err != nil {
				return nil, err
			}
			entry := f.table.Index.Get(abs)
			if entry == nil {
				continue
			}
		}

		parquetFiles = append(parquetFiles, struct {
			name string
			size int64
		}{name, stat.Size()})
	}
	sort.Slice(parquetFiles, func(a, b int) bool {
		return parquetFiles[a].size > parquetFiles[b].size
	})
	return parquetFiles, nil
}

func (f *fsMergeService) PlanMerge(files []FileDesc, maxResSize int64, iteration int) []PlanMerge {
	var res []PlanMerge
	mergeSize := int64(0)
	uid, _ := uuid.NewUUID()
	_res := PlanMerge{
		To:        path.Join(fmt.Sprintf("%s.%d.parquet", uid.String(), iteration+1)),
		Iteration: iteration,
	}
	for _, file := range files {
		mergeSize += file.size
		_res.From = append(_res.From, file.name)
		if mergeSize > maxResSize {
			res = append(res, _res)
			uid, _ := uuid.NewUUID()
			_res = PlanMerge{
				To:        path.Join(fmt.Sprintf("%s.%d.parquet", uid.String(), iteration+1)),
				Iteration: iteration,
			}
			mergeSize = 0
		}
	}
	if len(_res.From) > 0 {
		res = append(res, _res)
	}
	return res
}

var tmpl = func() *template.Template {
	_tmpl, err := template.New("chsql_url").Parse(CHSQL_EXT_URL)
	if err != nil {
		panic(err)
	}
	return _tmpl
}()

func downloadToTempFile(url string, fname string) (string, error) {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", fname)
	if err != nil {
		return "", fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer tmpFile.Close()

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to GET from %s: %w", url, err)
	}
	defer resp.Body.Close()

	// Check server response
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("bad status: %s", resp.Status)
	}

	// Write the body to file
	_, err = io.Copy(tmpFile, resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to write to temporary file: %w", err)
	}

	return tmpFile.Name(), nil
}

func installChSql(db *sql.DB) error {
	if CHSQL_EXT_URL == "community" {
		_, err := db.Exec("INSTALL chsql FROM community")
		if err != nil {
			return fmt.Errorf("failed to install chsql extension: %w", err)
		}

		_, err = db.Exec("LOAD chsql")
		return err
	}

	var (
		ver  string
		arch string
	)
	row := db.QueryRow("SELECT version()")
	if row == nil {
		return fmt.Errorf("failed to get version")
	}
	err := row.Scan(&ver)
	if err != nil {
		return fmt.Errorf("failed to scan version: %w", err)
	}

	row = db.QueryRow("PRAGMA platform")
	if row == nil {
		return fmt.Errorf("failed to get platform")
	}
	err = row.Scan(&arch)
	if err != nil {
		return fmt.Errorf("failed to scan platform: %w", err)
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, map[string]string{
		"VER":        CHSQL_VER,
		"DUCKDB_VER": ver,
		"ARCH":       arch,
	})
	if err != nil {
		return fmt.Errorf("failed to execute template: %w", err)
	}

	chsqlURL := buf.String()

	fname, err := downloadToTempFile(chsqlURL, "chsql.duckdb_extension")

	_, err = db.Exec(fmt.Sprintf("INSTALL '%s'", fname))
	if err != nil {
		return fmt.Errorf("failed to install chsql extension: %w", err)
	}

	_, err = db.Exec("LOAD 'chsql'")
	return err
}

func (f *fsMergeService) merge(p PlanMerge) error {

	tmpFilePath := filepath.Join(f.tmpPath, p.To)
	finalFilePath := filepath.Join(f.dataPath, p.To)
	/*
		fmt.Printf("Merging files:\n  Base path: %s\n", f.path)
		for _, file := range p.From {
			fmt.Printf("  %s\n", file)
		}
		fmt.Printf("  Tmp path: %s\n", tmpFilePath)
		fmt.Printf("  Data path: %s\n", finalFilePath)
	*/
	if len(p.From) == 1 {
		return os.Rename(p.From[0], finalFilePath)
	}

	conn, err := db.ConnectDuckDB("?allow_unsigned_extensions=1")
	if err != nil {
		return err
	}
	err = installChSql(conn)
	if err != nil {
		return err
	}
	defer conn.Close()

	createTableSQL := fmt.Sprintf(
		`COPY(SELECT * FROM read_parquet_mergetree(ARRAY['%s'], '%s'))TO '%s' (FORMAT 'parquet')`,
		strings.Join(p.From, "','"),
		strings.Join(f.table.OrderBy, ","), tmpFilePath)
	_, err = conn.Exec(createTableSQL)

	if err != nil {
		fmt.Println("Error read_parquet_mergetree: ", err)
		return err
	}

	err = os.Rename(tmpFilePath, finalFilePath)
	if err != nil {
		return err
	}

	if f.table.Index != nil {
		err = f.updateIndex(p)
		if err != nil {
			return err
		}
	}

	for _, file := range p.From {
		_file := file
		go func() {
			<-time.After(time.Second * 30)
			os.Remove(_file)
		}()
	}

	return nil
}

func (f *fsMergeService) updateIndex(merge PlanMerge) error {
	_min := make(map[string]any)
	_max := make(map[string]any)
	var rowCount int64
	toDelete := make([]string, len(merge.From))
	for i, file := range merge.From {
		path, err := filepath.Abs(file)
		if err != nil {
			return err
		}
		toDelete[i] = path
		fromIdx := f.table.Index.Get(path)
		if i == 0 {
			_min["__timestamp"] = fromIdx.Min["__timestamp"]
			_max["__timestamp"] = fromIdx.Max["__timestamp"]
		} else {
			_min["__timestamp"] = min(_min["__timestamp"].(int64), fromIdx.Min["__timestamp"].(int64))
			_max["__timestamp"] = max(_max["__timestamp"].(int64), fromIdx.Max["__timestamp"].(int64))
		}
		rowCount += fromIdx.RowCount
	}
	path, err := filepath.Abs(path.Join(f.dataPath, merge.To))
	if err != nil {
		return err
	}
	stat, err := os.Stat(path)
	if err != nil {
		return err
	}
	newIdx := &model.IndexEntry{
		Path:      path,
		SizeBytes: stat.Size(),
		RowCount:  rowCount,
		ChunkTime: time.Now().UnixNano(),
		Min:       _min,
		Max:       _max,
	}
	prom := f.table.Index.Batch([]*model.IndexEntry{newIdx}, toDelete)
	_, err = prom.Get()
	return err
}

func (f *fsMergeService) doMerge(merges []PlanMerge, merge func(p PlanMerge) error) error {
	errGroup := errgroup.Group{}
	sem := semaphore.NewWeighted(10)
	for _, m := range merges {

		_m := m
		errGroup.Go(func() error {
			sem.Acquire(context.Background(), 1)
			defer sem.Release(1)
			return merge(_m)
		})
	}
	return errGroup.Wait()
}

func (f *fsMergeService) DoMerge(merges []PlanMerge) error {
	_merges := make([]PlanMerge, len(merges))
	copy(_merges, merges)
	return f.doMerge(_merges, f.merge)
}
