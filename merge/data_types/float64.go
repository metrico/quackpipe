package data_types

import (
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/go-faster/jx"
	"sort"
	"strconv"
)

type Float64 struct {
	generic[float64]
}

func (f Float64) ParseJson(dec *jx.Decoder, store any) (any, error) {
	return f.generic.ParseJson(dec.Float64, store.([]float64))
}

func (f Float64) GetName() string {
	return DATA_TYPE_NAME_FLOAT64
}

func (f Float64) Less(a any, i int32, j int32) bool {
	return a.([]float64)[i] <= a.([]float64)[j]
}

func (f Float64) BLess(a any, b any) bool {
	return a.(float64) <= b.(float64)
}

func (f Float64) ArrowDataType() arrow.DataType {
	return arrow.PrimitiveTypes.Float64
}

func (f Float64) WriteToBatch(batch array.Builder, data any, index IndexType, valid []bool) error {
	_batch := batch.(*array.Float64Builder)
	return f.generic.WriteToBatch(_batch.AppendValues, _batch.Append, _batch.AppendNull, data, valid, index)
}

func (f Float64) GetSorter(data any) sort.Interface {
	return &GenericSorter[float64]{data: data.([]float64)}
}

func (f Float64) GetMerger(data1 any, valid1 []bool, data2 any, valid2 []bool, s1 int64, s2 int64) IGenericMerger {
	return NewGenericMerger(data1.([]float64), data2.([]float64), valid1, valid2, s1, s2)
}

func (f Float64) ParseFromStr(s string) (any, error) {
	return strconv.ParseFloat(s, 64)
}
