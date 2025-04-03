package data_types

import (
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/go-faster/jx"
)

type IndexType []int32

type DataType interface {
	MakeStore(sizeAndCap ...int) any
	AppendDefault(size int, store any) any
	GetLength(store any) int64
	ParseJson(dec *jx.Decoder, store any) (any, error)
	Less(store any, i int32, j int32) bool
	BLess(a any, b any) bool
	ValidateData(data any) error
	ArrowDataType() arrow.DataType
	AppendStore(store any, data any) (any, error)
	AppendOne(val any, data any) any
	AppendByMask(data any, toAppend any, mask []byte) (any, error)
	WriteToBatch(batch array.Builder, data any, index IndexType, valid []bool) error
	GetName() string
	GetVal(i int64, store any) any
	GetValI32(i int32, store any) any
}

func GoTypeToDataType(valOrCol any) (string, DataType) {
	switch valOrCol.(type) {
	case int64, []int64:
		return DATA_TYPE_NAME_INT64, Int64{}
	case uint64, []uint64:
		return DATA_TYPE_NAME_UINT64, UInt64{}
	case float64, []float64:
		return DATA_TYPE_NAME_FLOAT64, Float64{}
	case string, []string:
		return DATA_TYPE_NAME_STRING, String{}
	default:
		return DATA_TYPE_NAME_UNKNOWN, UnknownType{}
	}
}

const DATA_TYPE_NAME_INT64 = "INT8"
const DATA_TYPE_NAME_UINT64 = "UBIGINT"
const DATA_TYPE_NAME_FLOAT64 = "FLOAT8"
const DATA_TYPE_NAME_STRING = "VARCHAR"
const DATA_TYPE_NAME_UNKNOWN = "UNKNOWN"

var DataTypes = map[string]DataType{
	"Int64":  Int64{},
	"BIGINT": Int64{},
	"INT8":   Int64{},
	"LONG":   Int64{},

	"UInt64":  UInt64{},
	"UBIGINT": UInt64{},

	"Float64": Float64{},
	"DOUBLE":  Float64{},
	"FLOAT8":  Float64{},

	"String":  String{},
	"STRING":  String{},
	"VARCHAR": String{},
	"CHAR":    String{},
	"BPCHAR":  String{},
	"TEXT":    String{},
	"UNKNOWN": UnknownType{},

	/*"UHUGEINT":  UInt64{},
	"UINTEGER":  UInt64{},
	"USMALLINT": UInt64{},
	"UTINYINT":  UInt64{},


		"INTEGER":  Int64{},
		"INT4":     Int64{},
		"INT":      Int64{},
		"SIGNED":   Int64{},
		"SMALLINT": Int64{},
		"INT2":     Int64{},
		"SHORT":    Int64{},
		"TINYINT":  Int64{},
		"INT1":     Int64{},
		"HUGEINT":  Int64{},

	"FLOAT":  Float64{},
	"FLOAT4": Float64{},
	"REAL":   Float64{},

	"BIT":                      Bit{},
	"BITSTRING":                Bit{},
	"BLOB":                     Blob{},
	"BYTEA":                    Blob{},
	"BINARY":                   Blob{},
	"VARBINARY":                Blob{},
	"BOOLEAN":                  Boolean{},
	"BOOL":                     Boolean{},
	"LOGICAL":                  Boolean{},
	"DATE":                     Date{},
	"DECIMAL":                  Decimal{},
	"NUMERIC":                  Decimal{},
	"INTERVAL":                 Interval{},
	"JSON":                     Json{},
	"TIME":                     Time{},
	"TIMESTAMP WITH TIME ZONE": TimestampWithTimeZone{},
	"TIMESTAMPTZ":              TimestampWithTimeZone{},
	"TIMESTAMP":                Timestamp{},
	"DATETIME":                 Timestamp{},
	"UUID":                     Uuid{},*/
}
