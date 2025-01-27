package data_types

import (
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/go-faster/jx"
	"github.com/tidwall/btree"
)

type DataType interface {
	MakeStore() any
	ParseJson(dec *jx.Decoder, store any) (any, error)
	Less(store any, i int32, j int32) bool
	ValidateData(data any) error
	ArrowDataType() arrow.DataType
	AppendStore(store any, data any) (any, error)
	WriteToBatch(batch array.Builder, data any, indexes *btree.BTreeG[int32]) error
}

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
