package data_types

import (
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/go-faster/jx"
	"github.com/tidwall/btree"
)

type Float64 struct {
	generic[float64]
}

func (f Float64) ParseJson(dec *jx.Decoder, store any) (any, error) {
	return f.generic.ParseJson(dec.Float64, store.([]float64))
}

func (f Float64) Less(a any, i int32, j int32) bool {
	return a.([]float64)[i] < a.([]float64)[j]
}

func (f Float64) ArrowDataType() arrow.DataType {
	return arrow.PrimitiveTypes.Float64
}

func (f Float64) WriteToBatch(batch array.Builder, data any, index *btree.BTreeG[int32]) error {
	_batch := batch.(*array.Float64Builder)
	return f.generic.WriteToBatch(_batch.AppendValues, _batch.Append, data, index)
}
