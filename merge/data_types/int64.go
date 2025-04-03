package data_types

import (
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/go-faster/jx"
	"sort"
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

func (i Int64) BLess(a any, b any) bool {
	return a.(int64) <= b.(int64)
}

func (i Int64) ArrowDataType() arrow.DataType {
	return arrow.PrimitiveTypes.Int64
}

func (i Int64) WriteToBatch(batch array.Builder, data any, index IndexType, valid []bool) error {
	_batch := batch.(*array.Int64Builder)
	return i.generic.WriteToBatch(_batch.AppendValues, _batch.Append, _batch.AppendNull, data, valid, index)
}

func (f Int64) GetSorter(data any) sort.Interface {
	return &GenericSorter[int64]{data: data.([]int64)}
}

func (f Int64) GetMerger(data1 any, valid1 []bool, data2 any, valid2 []bool, s1 int64, s2 int64) IGenericMerger {
	return NewGenericMerger(data1.([]int64), data2.([]int64), valid1, valid2, s1, s2)
}
