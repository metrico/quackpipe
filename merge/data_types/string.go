package data_types

import (
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/go-faster/jx"
	"github.com/tidwall/btree"
)

type String struct {
	generic[string]
}

func (i String) ParseJson(dec *jx.Decoder, store any) (any, error) {
	return i.generic.ParseJson(dec.Str, store.([]string))
}

func (i String) Less(a any, k int32, j int32) bool {
	return a.([]string)[k] < a.([]string)[j]
}

func (i String) ArrowDataType() arrow.DataType {
	return arrow.BinaryTypes.String
}

func (i String) WriteToBatch(batch array.Builder, data any, index *btree.BTreeG[int32]) error {
	_batch := batch.(*array.StringBuilder)
	return i.generic.WriteToBatch(_batch.AppendValues, _batch.Append, data, index)
}
