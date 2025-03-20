package data_types

import (
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/go-faster/jx"
	"github.com/tidwall/btree"
)

type UnknownType struct{}

func (u UnknownType) GetName() string {
	return DATA_TYPE_NAME_UNKNOWN
}

func (u UnknownType) GetLength(store any) int64 {
	return 0
}

func (u UnknownType) MakeStore(sizeAndCap ...int) any {
	return nil
}

func (u UnknownType) AppendDefault(size int, store any) any {
	return nil
}

func (u UnknownType) ParseJson(dec *jx.Decoder, store any) (any, error) {
	return nil, fmt.Errorf("unknown data type")
}

func (u UnknownType) Less(store any, i int32, j int32) bool {
	return false
}

func (u UnknownType) ValidateData(data any) error {
	return fmt.Errorf("unknown data type")
}

func (u UnknownType) ArrowDataType() arrow.DataType {
	return nil
}

func (u UnknownType) AppendStore(store any, data any) (any, error) {
	return nil, fmt.Errorf("unknown data type")
}

func (u UnknownType) WriteToBatch(batch array.Builder, data any, indexes *btree.BTreeG[int32], valid []bool) error {
	return fmt.Errorf("unknown data type")
}
