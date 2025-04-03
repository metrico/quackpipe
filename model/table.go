package model

import "github.com/metrico/quackpipe/merge/data_types"

type PartitionDesc struct {
	Values   [][2]string
	IndexMap []byte
}

type Table struct {
	Name          string
	Path          string
	Engine        string
	OrderBy       []string
	PartitionBy   func(map[string]*ColumnStore) ([]PartitionDesc, error)
	AutoTimestamp bool
}

type ColumnStore struct {
	Data   any
	Valids []bool
	Tp     data_types.DataType
}

func NewColumnStore(tp data_types.DataType, initialSize int) *ColumnStore {
	return &ColumnStore{
		Data:   tp.MakeStore(initialSize),
		Valids: make([]bool, initialSize),
		Tp:     tp,
	}
}
