package tests

import (
	"fmt"
	"github.com/metrico/quackpipe/model"
	"github.com/metrico/quackpipe/service"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStore_Success(t *testing.T) {
	var table = &model.Table{
		Name:    "experimental",
		Path:    "/tmp/example",
		Fields:  [][2]string{{"timestamp", "UInt64"}, {"str2", "String"}, {"value", "Float64"}},
		OrderBy: []string{"timestamp"},
	}

	// Initialize the MergeTreeService
	mt, err := service.NewMergeTreeService("test")
	assert.NoError(t, err, "Failed to create MergeTreeService")

	// Call Store method
	err = mt.Store(table, map[string][]any{
		"str2":      []any{"a", "b", "c"},
		"timestamp": []any{uint64(1628596100), uint64(1628596001), uint64(1628596002)},
		"value":     []any{float64(1.1), float64(2.2), float64(3.3)},
	})

	err = mt.Merge(table)
	// Assert no error occurred
	assert.NoError(t, err, "Store method returned an error")
}

func TestStore(t *testing.T) {
	table := &model.Table{
		Fields: [][2]string{
			{"str", "String"},
			{"timestamp", "UInt64"},
			{"value", "Float64"},
		},
	}
	mt, err := service.NewMergeTreeService("test")
	if err != nil {
		fmt.Println(err.Error())
	}
	// Test: data entries have the invalid type
	t.Run("InvalidDataType", func(t *testing.T) {
		err := mt.Store(table, map[string][]any{
			"str":       []any{123, "b", "c"}, // invalid: int instead of string
			"timestamp": []any{uint64(1628596000), uint64(1628596001), uint64(1628596002)},
			"value":     []any{float64(1.1), float64(2.2), float64(3.3)},
		})

		assert.Error(t, err)
		assert.EqualError(t, err, "invalid data type for column str: expected string")
	})

	// Test: data entries are not of the same size
	t.Run("UnequalDataSizes", func(t *testing.T) {
		err := mt.Store(table, map[string][]any{
			"str":       []any{"a", "b"},                                                   // size 2
			"timestamp": []any{uint64(1628596000), uint64(1628596001), uint64(1628596002)}, // size 3
			"value":     []any{float64(1.1), float64(2.2), float64(3.3)},                   // size 3
		})

		assert.Error(t, err)
		assert.EqualError(t, err, "columns length and data length mismatch")
	})

	// Test: data size is less than columns size
	t.Run("DataSizeLessThanColumns", func(t *testing.T) {
		err := mt.Store(table, map[string][]any{
			"str":       []any{"a"},                                                        // size 1
			"timestamp": []any{uint64(1628596000), uint64(1628596001), uint64(1628596002)}, // size 3
			"value":     []any{float64(1.1), float64(2.2), float64(3.3)},                   // size 3
		})

		assert.Error(t, err)
		assert.EqualError(t, err, "columns length and data length mismatch")
	})

	// Test: columns size is not equal to the table.Fields size
	t.Run("ColumnsSizeMismatch", func(t *testing.T) {
		err := mt.Store(table, map[string][]any{
			"str":       []any{"a", "b", "c"},
			"timestamp": []any{uint64(1628596000), uint64(1628596001), uint64(1628596002)},
		}) // Missing "value" column

		assert.Error(t, err)
		assert.EqualError(t, err, "columns size does not match table fields size")
	})
}
