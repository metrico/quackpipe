package data_types

import (
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/go-faster/jx"
	"github.com/tidwall/btree"
)

type Int64 struct {
	generic[int64]
}

func (i Int64) ParseJson(dec *jx.Decoder, store any) (any, error) {
	return i.generic.ParseJson(dec.Int64, store.([]int64))
}

func (i Int64) GetName() string {
	return DATA_TYPE_NAME_INT64
}

func (i Int64) Less(a any, k int32, j int32) bool {
	return a.([]int64)[k] <= a.([]int64)[j]
}

func (i Int64) ArrowDataType() arrow.DataType {
	return arrow.PrimitiveTypes.Int64
}

func (i Int64) WriteToBatch(batch array.Builder, data any, index *btree.BTreeG[int32], valid []bool) error {
	_batch := batch.(*array.Int64Builder)
	return i.generic.WriteToBatch(_batch.AppendValues, _batch.Append, _batch.AppendNull, data, valid, index)
}
