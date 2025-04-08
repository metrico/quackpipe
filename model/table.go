package model

import (
	"github.com/metrico/quackpipe/merge/data_types"
	"github.com/metrico/quackpipe/utils/promise"
)

type PartitionDesc struct {
	Values   [][2]string
	IndexMap []byte
}

type IndexEntry struct {
	Path      string
	SizeBytes int64
	RowCount  int64
	ChunkTime int64
	Min       map[string]any
	Max       map[string]any
}

type Index interface {
	Batch(add []*IndexEntry, rm []string) promise.Promise[int32]
	Get(path string) *IndexEntry
	Run()
	Stop()
}

type Table struct {
	Name          string
	Path          string
	Engine        string
	OrderBy       []string
	PartitionBy   func(map[string]data_types.IColumn) ([]PartitionDesc, error)
	AutoTimestamp bool
	IndexCreator  func(values [][2]string) (Index, error)
}
