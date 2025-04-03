package data_types

import (
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/go-faster/jx"
	"sort"
)

type UInt64 struct {
	generic[uint64]
}

func (i UInt64) ParseJson(dec *jx.Decoder, store any) (any, error) {
	return i.generic.ParseJson(dec.UInt64, store.([]uint64))
}

func (i UInt64) GetName() string {
	return DATA_TYPE_NAME_UINT64
}

func (i UInt64) Less(a any, k int32, j int32) bool {
	return a.([]uint64)[k] <= a.([]uint64)[j]
}

func (f UInt64) BLess(a any, b any) bool {
	return a.(float64) <= b.(float64)
}

func (i UInt64) ArrowDataType() arrow.DataType {
	return arrow.PrimitiveTypes.Uint64
}

func (i UInt64) WriteToBatch(batch array.Builder, data any, index IndexType, valid []bool) error {
	_batch := batch.(*array.Uint64Builder)
	return i.generic.WriteToBatch(_batch.AppendValues, _batch.Append, _batch.AppendNull, data, valid, index)
}

func (f UInt64) GetSorter(data any) sort.Interface {
	return &GenericSorter[uint64]{data: data.([]uint64)}
}

func (f UInt64) GetMerger(data1 any, valid1 []bool, data2 any, valid2 []bool, s1 int64, s2 int64) IGenericMerger {
	return NewGenericMerger(data1.([]uint64), data2.([]uint64), valid1, valid2, s1, s2)
}
