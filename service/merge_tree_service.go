package service

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/google/uuid"
	_ "github.com/marcboeker/go-duckdb"
	"os"
	"path/filepath"
	"quackpipe/model"
	"strings"
	"time"
)

type IMergeTree interface {
	Store(table *model.Table, columns map[string][]any) error
	Merge(table *model.Table) error
}

type MergeTreeService struct {
	db *sql.DB
}

func NewMergeTreeService(dbPath string) (*MergeTreeService, error) {
	conn, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB connection: %v", err)
	}

	return &MergeTreeService{db: conn}, nil
}

func (s *MergeTreeService) Close() error {
	return s.db.Close()
}

func validateData(table *model.Table, columns map[string][]any) error {

	fieldMap := make(map[string]string)
	for _, field := range table.Fields {
		fieldMap[field[0]] = field[1]
	}

	// Check if columns map size matches the table.Fields size
	if len(columns) != len(table.Fields) {
		return errors.New("columns size does not match table fields size")
	}

	var dataLength int
	for _, data := range columns {
		if dataLength == 0 {
			dataLength = len(data) // Initialize dataLength with the length of the first column
		} else if len(data) != dataLength {
			return errors.New("columns length and data length mismatch")
		}
	}
	for column, data := range columns {

		// Validate if the column exists in the table definition
		columnType, ok := fieldMap[column]
		if !ok {
			return fmt.Errorf("invalid column: %s", column)
		}
		// Validate data types for each column
		switch columnType {
		case "UInt64":
			for _, val := range data {
				if _, ok := val.(uint64); !ok {
					return fmt.Errorf("invalid data type for column %s: expected uint64", column)
				}
			}
		case "Int64":
			for _, val := range data {
				if _, ok := val.(int64); !ok {
					return fmt.Errorf("invalid data type for column %s: expected int64", column)
				}
			}
		case "String":
			for _, val := range data {
				if _, ok := val.(string); !ok {
					return fmt.Errorf("invalid data type for column %s: expected string", column)
				}
			}
		case "Float64":
			for _, val := range data {
				if _, ok := val.(float64); !ok {
					return fmt.Errorf("invalid data type for column %s: expected float64", column)
				}
			}
		default:
			return fmt.Errorf("unsupported column type: %s", columnType)
		}
	}

	return nil
}

func (s *MergeTreeService) createParquetSchema(table *model.Table) *arrow.Schema {
	fields := make([]arrow.Field, len(table.Fields))
	for i, field := range table.Fields {
		var fieldType arrow.DataType
		switch field[1] {
		case "UInt64":
			fieldType = arrow.PrimitiveTypes.Uint64
		case "Int64":
			fieldType = arrow.PrimitiveTypes.Int64
		case "String":
			fieldType = arrow.BinaryTypes.String
		case "Float64":
			fieldType = arrow.PrimitiveTypes.Float64
		default:
			panic(fmt.Sprintf("unsupported field type: %s", field[1]))
		}
		fields[i] = arrow.Field{Name: field[0], Type: fieldType}
	}
	return arrow.NewSchema(fields, nil)
}

func (s *MergeTreeService) writeParquetFile(table *model.Table, columns map[string][]any) error {
	schema := s.createParquetSchema(table)
	outputFile := filepath.Join(table.Path, "data", table.Name+uuid.New().String()+".parquet")
	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create parquet file: %v", err)
	}
	defer file.Close()

	// Create a new Arrow memory pool
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())

	// Create Arrow RecordBatch
	recordBatch := array.NewRecordBuilder(pool, schema)
	defer recordBatch.Release()

	// Create a field map for easier access to column types
	fieldMap := make(map[string]string)
	for _, field := range table.Fields {
		fieldMap[field[0]] = field[1]
	}
	for columnName, dataSlice := range columns {
		columnType, ok := fieldMap[columnName]
		if !ok {
			return fmt.Errorf("unknown column: %s", columnName)
		}

		// Get the index of the column from the schema
		columnIndex := -1
		for i, field := range schema.Fields() {
			if field.Name == columnName {
				columnIndex = i
				break
			}
		}
		if columnIndex == -1 {
			return fmt.Errorf("column %s not found in schema", columnName)
		}

		builder := recordBatch.Field(columnIndex).(array.Builder)

		// Handle data slice based on its type
		switch columnType {
		case "UInt64":
			if b, ok := builder.(*array.Uint64Builder); ok {
				for _, value := range dataSlice {
					if v, ok := value.(uint64); ok {
						b.Append(v)
					} else {
						return fmt.Errorf("invalid data type for column %s, expected uint64", columnName)
					}
				}
			} else {
				return fmt.Errorf("type mismatch for column %s", columnName)
			}
		case "Int64":
			if b, ok := builder.(*array.Int64Builder); ok {
				for _, value := range dataSlice {
					if v, ok := value.(int64); ok {
						b.Append(v)
					} else {
						return fmt.Errorf("invalid data type for column %s, expected int64", columnName)
					}
				}
			} else {
				return fmt.Errorf("type mismatch for column %s", columnName)
			}
		case "String":
			if b, ok := builder.(*array.StringBuilder); ok {
				for _, value := range dataSlice {
					if v, ok := value.(string); ok {
						b.Append(v)
					} else {
						return fmt.Errorf("invalid data type for column %s, expected string", columnName)
					}
				}
			} else {
				return fmt.Errorf("type mismatch for column %s", columnName)
			}
		case "Float64":
			if b, ok := builder.(*array.Float64Builder); ok {
				for _, value := range dataSlice {
					if v, ok := value.(float64); ok {
						b.Append(v)
					} else {
						return fmt.Errorf("invalid data type for column %s, expected float64", columnName)
					}
				}
			} else {
				return fmt.Errorf("type mismatch for column %s", columnName)
			}
		default:
			return fmt.Errorf("unsupported column type for column %s: %s", columnName, columnType)
		}
	}

	// Finalize the record batch
	batch := recordBatch.NewRecord()
	defer batch.Release()

	// Set up Parquet writer properties
	writerProps := parquet.NewWriterProperties(
		parquet.WithMaxRowGroupLength(100),
	)
	arrprops := pqarrow.NewArrowWriterProperties()

	// Create Parquet file writer
	writer, err := pqarrow.NewFileWriter(schema, file, writerProps, arrprops)
	if err != nil {
		return fmt.Errorf("failed to create Parquet file writer: %v", err)
	}
	defer writer.Close()

	// Write the record batch to the Parquet file
	if err := writer.Write(batch); err != nil {
		return fmt.Errorf("failed to write record batch to parquet file: %v", err)
	}

	return nil
}

func (s *MergeTreeService) Store(table *model.Table, columns map[string][]any) error {
	if err := validateData(table, columns); err != nil {
		return err
	}

	if err := s.writeParquetFile(table, columns); err != nil {
		return err
	}

	return nil
}

// Merge method implementation
func (s *MergeTreeService) Merge(table *model.Table) error {
	dataDir := filepath.Join(table.Path, "data")
	tmpDir := filepath.Join(table.Path, "tmp")

	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return err
	}

	files, err := os.ReadDir(dataDir)
	if err != nil {
		return err
	}

	var parquetFiles []string
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".parquet") {
			parquetFiles = append(parquetFiles, filepath.Join(dataDir, file.Name()))
		}
	}

	if len(parquetFiles) == 0 {
		return errors.New("no parquet files to merge")
	}

	// Plan the merge to keep the size under 4GB
	const maxFileSize = 4 * 1024 * 1024 * 1024
	var filesToMerge []string
	var currentSize int64

	for _, file := range parquetFiles {
		fileInfo, err := os.Stat(file)
		if err != nil {
			return err
		}

		if currentSize+fileInfo.Size() > maxFileSize {
			if err := mergeFiles(filesToMerge, table, tmpDir); err != nil {
				return err
			}
			filesToMerge = nil
			currentSize = 0
		}

		filesToMerge = append(filesToMerge, file)
		currentSize += fileInfo.Size()
	}

	if len(filesToMerge) > 0 {
		if err := mergeFiles(filesToMerge, table, tmpDir); err != nil {
			return err
		}
	}

	return nil
}

func mergeFiles(files []string, table *model.Table, tmpDir string) error {
	// Create a temporary merged file
	tmpFilePath := filepath.Join(tmpDir, fmt.Sprintf("%s_%d.parquet", table.Name, time.Now().UnixNano()))

	// Prepare DuckDB connection
	conn, err := sql.Open("duckdb", "test")
	if err != nil {
		return err
	}
	defer conn.Close()

	//// Drop the table if it exists
	//dropTableSQL := `DROP TABLE IF EXISTS temp_table`
	//_, err = conn.Exec(dropTableSQL)
	//if err != nil {
	//	return err
	//}

	// Create a temporary table in DuckDB using parquet_scan with an array of files
	createTableSQL := fmt.Sprintf(`COPY(SELECT * FROM read_parquet (ARRAY['%s']) order by %s)TO '%s' (FORMAT 'parquet')`, strings.Join(files, "','"), strings.Join(table.OrderBy, ","), tmpFilePath)
	_, err = conn.Exec(createTableSQL)
	if err != nil {
		return err
	}

	//// Perform the merge
	//mergeSQL := fmt.Sprintf(
	//	`COPY (SELECT * FROM temp_table ORDER BY %s) TO '%s' (FORMAT 'parquet')`,
	//	strings.Join(table.OrderBy, ","),
	//	tmpFilePath,
	//)
	//_, err = conn.Exec(mergeSQL)
	//if err != nil {
	//	return err
	//}

	// Cleanup old files
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			return err
		}
	}

	finalFilePath := filepath.Join(filepath.Dir(files[0]), filepath.Base(tmpFilePath))
	if err := os.Rename(tmpFilePath, finalFilePath); err != nil {
		return err
	}

	return nil
}
