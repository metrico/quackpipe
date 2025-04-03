package data_types

import (
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/go-faster/jx"
	"sort"
)

type String struct {
	generic[string]
}

func (i String) ParseJson(dec *jx.Decoder, store any) (any, error) {
	return i.generic.ParseJson(dec.Str, store.([]string))
}

func (i String) GetName() string {
	return DATA_TYPE_NAME_STRING
}

func (i String) Less(a any, k int32, j int32) bool {
	return a.([]string)[k] <= a.([]string)[j]
}

func (f String) BLess(a any, b any) bool {
	return a.(float64) <= b.(float64)
}

func (i String) ArrowDataType() arrow.DataType {
	return arrow.BinaryTypes.String
}

func (i String) WriteToBatch(batch array.Builder, data any, index IndexType, valid []bool) error {
	_batch := batch.(*array.StringBuilder)
	return i.generic.WriteToBatch(_batch.AppendValues, _batch.Append, _batch.AppendNull, data, valid, index)
}

func (f String) GetSorter(data any) sort.Interface {
	return &GenericSorter[string]{data: data.([]string)}
}

func (f String) GetMerger(data1 any, valid1 []bool, data2 any, valid2 []bool, s1 int64, s2 int64) IGenericMerger {
	return NewGenericMerger(data1.([]string), data2.([]string), valid1, valid2, s1, s2)
}
