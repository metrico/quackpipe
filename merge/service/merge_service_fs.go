package service

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"os"
	"path"
	"path/filepath"
	"quackpipe/model"
	"quackpipe/service/db"
	"sort"
	"strings"
)

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
		To:        path.Join(f.path, fmt.Sprintf("%s.%d.parquet", uid.String(), iteration+1)),
		Iteration: iteration,
	}
	for _, file := range files {
		mergeSize += file.size
		_res.From = append(_res.From, file.name)
		if mergeSize > maxResSize {
			res = append(res, _res)
			uid, _ := uuid.NewUUID()
			_res = PlanMerge{
				To:        path.Join(f.path, fmt.Sprintf("%s.%d.parquet", uid.String(), iteration+1)),
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

func (f *fsMergeService) merge(p PlanMerge) error {
	// Create a temporary merged file
	tmpFilePath := filepath.Join(f.path, "tmp", p.To)

	conn, err := db.ConnectDuckDB("?allow_unsigned_extensions=1")
	if err != nil {
		return err
	}
	_, err = conn.Exec("INSTALL chsql FROM community")
	if err != nil {
		fmt.Println("Error loading chsql extension: ", err)
		return err
	}
	_, err = conn.Exec("LOAD chsql")
	if err != nil {
		fmt.Println("Error loading chsql extension: ", err)
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

	finalFilePath := filepath.Join(f.path, "data", p.To)
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
	_merges := make([]PlanMerge, 0, len(merges))
	for _, m := range merges {
		if len(m.From) == 1 {
			err := os.Rename(m.From[0], m.To)
			if err != nil {
				return err
			}
			continue
		}
		_merges = append(_merges, m)
	}
	return f.doMerge(_merges, f.merge)
}
