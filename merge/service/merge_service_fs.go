package service

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"os"
	"path/filepath"
	"quackpipe/model"
	"sort"
	"strings"
)

type mergeService interface {
	GetFilesToMerge(iteration int) ([]FileDesc, error)
	PlanMerge([]FileDesc, int64, int) []PlanMerge
	DoMerge([]PlanMerge) error
	UploadTmp(absolutePathFrom string, absolutePathTo string) error
	Drop(files []string)
	DropTmp(files []string)
	TmpDir() string
	DataDir() string
	Join(parts ...string) string
}

type fsMergeService struct {
	path  string
	table *model.Table
}

func (f *fsMergeService) GetFilesToMerge(iteration int) ([]FileDesc, error) {
	dataDir := f.DataDir()
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
			parquetFiles = append(parquetFiles, FileDesc{file.Name(), stat.Size()})
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
		To:        fmt.Sprintf("%s.%d.parquet", uid.String(), iteration+1),
		Iteration: iteration,
	}
	for _, file := range files {
		mergeSize += file.size
		_res.From = append(_res.From, file.name)
		if mergeSize > maxResSize || len(_res.From) == 10 {
			res = append(res, _res)
			uid, _ := uuid.NewUUID()
			_res = PlanMerge{
				To:        fmt.Sprintf("%s.%d.parquet", uid.String(), iteration+1),
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

func (f *fsMergeService) TmpDir() string {
	return filepath.Join(f.path, "tmp")
}

func (f *fsMergeService) merge(p PlanMerge) error {
	// Create a temporary merged file
	tmpFilePath := filepath.Join(f.TmpDir(), p.To)
	conn, err := getConn()
	if err != nil {
		return err
	}
	defer releaseConn(conn)

	from := make([]string, len(p.From))
	for i, file := range p.From {
		from[i] = filepath.Join(f.DataDir(), file)
	}

	createTableSQL := fmt.Sprintf(
		`COPY(SELECT * FROM read_parquet_mergetree(ARRAY['%s'], '%s'))TO '%s' (FORMAT 'parquet')`,
		strings.Join(from, "','"),
		strings.Join(f.table.OrderBy, ","), tmpFilePath)
	_, err = conn.Exec(createTableSQL)

	if err != nil {
		fmt.Println("Error read_parquet_mergetree: ", err)
		return err
	}

	return nil
}

func (f *fsMergeService) doMerge(merges []PlanMerge, merge func(p PlanMerge) error) error {
	errGroup := errgroup.Group{}
	for _, m := range merges {
		_m := m
		errGroup.Go(func() error {
			sem.Acquire(context.Background(), int64(_m.Iteration))
			defer sem.Release(int64(_m.Iteration))
			return merge(_m)
		})
	}
	return errGroup.Wait()
}

func (f *fsMergeService) UploadTmp(absolutePathFrom string, absolutePathTo string) error {
	err := os.Rename(absolutePathFrom, filepath.Join(f.DataDir(), absolutePathTo))
	if err != nil {
		return err
	}
	return nil
}

func (f *fsMergeService) Drop(files []string) {
	for _, file := range files {
		os.Remove(filepath.Join(f.DataDir(), file))
	}
}

func (f *fsMergeService) DropTmp(files []string) {
	for _, file := range files {
		os.Remove(filepath.Join(f.TmpDir(), file))
	}
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

func (f *fsMergeService) DataDir() string {
	return filepath.Join(f.path, "data")
}

func (f *fsMergeService) Join(parts ...string) string {
	return filepath.Join(parts...)
}
