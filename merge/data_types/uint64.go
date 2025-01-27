package data_types

import (
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/go-faster/jx"
	"github.com/tidwall/btree"
)

type UInt64 struct {
	generic[uint64]
}

func (i UInt64) ParseJson(dec *jx.Decoder, store any) (any, error) {
	return i.generic.ParseJson(dec.UInt64, store.([]uint64))
}

func (i UInt64) Less(a any, k int32, j int32) bool {
	return a.([]uint64)[k] < a.([]uint64)[j]
}

func (i UInt64) ArrowDataType() arrow.DataType {
	return arrow.PrimitiveTypes.Uint64
}

func (i UInt64) WriteToBatch(batch array.Builder, data any, index *btree.BTreeG[int32]) error {
	_batch := batch.(*array.Uint64Builder)
	return i.generic.WriteToBatch(_batch.AppendValues, _batch.Append, data, index)
}
