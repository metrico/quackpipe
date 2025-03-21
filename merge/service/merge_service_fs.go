package service

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"html/template"
	"os"
	"path"
	"path/filepath"
	"quackpipe/model"
	"quackpipe/service/db"
	"sort"
	"strings"
)

var CHSQL_VER = "v1.0.9"

const CHSQL_EXT_URL = "https://github.com/quackscience/duckdb-extension-clickhouse-sql/releases/download/{{.VER}}/chsql.{{.DUCKDB_VER}}.{{.ARCH}}.duckdb_extension"

type mergeService interface {
	GetFilesToMerge(iteration int) ([]FileDesc, error)
	PlanMerge([]FileDesc, int64, int) []PlanMerge
	DoMerge([]PlanMerge) error
}

type fsMergeService struct {
	path  string
	table *model.Table
}

func (f *fsMergeService) GetFilesToMerge(iteration int) ([]FileDesc, error) {
	dataDir := filepath.Join(f.path, "data")
	files, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}
	var parquetFiles []FileDesc
	suffix := fmt.Sprintf("%d.parquet", iteration)
	for _, file := range files {
		if strings.HasSuffix(file.Name(), suffix) {
			name := filepath.Join(dataDir, file.Name())
			stat, err := os.Stat(name)
			if err != nil {
				return nil, err
			}
			parquetFiles = append(parquetFiles, struct {
				name string
				size int64
			}{name, stat.Size()})
		}
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

	_, err = db.Exec(fmt.Sprintf("INSTALL '%s'", chsqlURL))
	if err != nil {
		return fmt.Errorf("failed to install chsql extension: %w", err)
	}

	_, err = db.Exec("LOAD 'chsql'")
	return err
}

func (f *fsMergeService) merge(p PlanMerge) error {

	tmpFilePath := filepath.Join(f.path, "tmp", p.To)
	finalFilePath := filepath.Join(f.path, "data", p.To)
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
	for _, file := range p.From {
		os.Remove(file)
	}

	return nil
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
