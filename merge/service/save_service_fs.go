package service

import (
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/google/uuid"
	"github.com/tidwall/btree"
	"os"
	"path"
	"quackpipe/merge/data_types"
)

type saveService interface {
	Save(fields [][2]string, data map[string]any, index *btree.BTreeG[int32]) error
}

type fsSaveService struct {
	path        string
	recordBatch *array.RecordBuilder
	schema      *arrow.Schema
}

func (fs *fsSaveService) saveTmpFile(filename string,
	fields [][2]string, data map[string]any, index *btree.BTreeG[int32]) error {
	for i, f := range fields {
		err := data_types.DataTypes[f[1]].WriteToBatch(fs.recordBatch.Field(i), data[f[0]], index)
		if err != nil {
			return err
		}
	}
	record := fs.recordBatch.NewRecord()
	defer record.Release()
	if record.Column(0).Data().Len() == 0 {
		return nil
	}
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	// Set up Parquet writer properties
	writerProps := parquet.NewWriterProperties(
		parquet.WithMaxRowGroupLength(8124),
	)
	arrprops := pqarrow.NewArrowWriterProperties()

	// Create Parquet file writer
	writer, err := pqarrow.NewFileWriter(fs.schema, file, writerProps, arrprops)
	if err != nil {
		return err
	}
	defer writer.Close()
	return writer.Write(record)
}

func (fs *fsSaveService) Save(fields [][2]string, data map[string]any, index *btree.BTreeG[int32]) error {
	filename, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	tmpFileName := path.Join(fs.path, "tmp", filename.String()+".1.parquet")
	err = fs.saveTmpFile(tmpFileName, fields, data, index)
	if err != nil {
		return err
	}
	return os.Rename(tmpFileName, path.Join(fs.path, "data", filename.String()+".1.parquet"))
}
