package service

import (
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/google/uuid"
	"github.com/tidwall/btree"
	"os"
	"path"
	"quackpipe/merge/data_types"
)

type fieldDesc [2]string

func (f fieldDesc) GetType() string       { return f[0] }
func (f fieldDesc) GetName() string       { return f[1] }
func fd(tp string, name string) fieldDesc { return [2]string{tp, name} }

type saveService interface {
	Save(fields []fieldDesc, unorderedData map[string]*columnStore, orderedData map[string]*columnStore,
		index *btree.BTreeG[int32]) error
}

type fsSaveService struct {
	path        string
	recordBatch *array.RecordBuilder
	schema      *arrow.Schema
}

func (fs *fsSaveService) shouldRecreateSchema(fields []fieldDesc) bool {
	if fs.schema == nil {
		return true
	}
	for _, f := range fields {
		found := false
		for _, _f := range fs.schema.Fields() {
			if _f.Name == f.GetName() {
				found = true
			}
		}
		if !found {
			return true
		}
	}
	return false
}

// @param: filename []fieldDesc: [data type - fields name]
func (fs *fsSaveService) maybeRecreateSchema(fields []fieldDesc) {
	if !fs.shouldRecreateSchema(fields) {
		return
	}
	arrowFields := make([]arrow.Field, len(fields))
	for i, field := range fields {
		var fieldType = data_types.DataTypes[field.GetType()]
		arrowFields[i] = arrow.Field{Name: field.GetName(), Type: fieldType.ArrowDataType(), Nullable: true}
	}

	fs.schema = arrow.NewSchema(arrowFields, nil)
	fs.recordBatch = array.NewRecordBuilder(memory.DefaultAllocator, fs.schema)
}

func (fs *fsSaveService) dumpData(fields []fieldDesc, data map[string]*columnStore, index *btree.BTreeG[int32]) error {
	var sz int64
	for _, col := range data {
		sz = col.tp.GetLength(col.data)
		break
	}
	for _, f := range fields {
		fieldsIdx := fs.schema.FieldIndices(f.GetName())
		fieldIdx := fieldsIdx[0]
		field := data[f.GetName()]
		if field == nil {
			fs.recordBatch.Field(fieldIdx).AppendNulls(int(sz))
			continue
		}
		err := data_types.DataTypes[f.GetType()].WriteToBatch(fs.recordBatch.Field(fieldIdx), field.data,
			index, field.valids)
		if err != nil {
			return err
		}
	}
	return nil
}

func (fs *fsSaveService) saveTmpFile(filename string,
	fields []fieldDesc, unorderedData, orderedData map[string]*columnStore, index *btree.BTreeG[int32]) error {
	fs.maybeRecreateSchema(fields)
	err := fs.dumpData(fields, unorderedData, nil)
	if err != nil {
		return err
	}
	err = fs.dumpData(fields, orderedData, index)
	if err != nil {
		return err
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

func (fs *fsSaveService) Save(fields []fieldDesc, unorderedData map[string]*columnStore,
	orderedData map[string]*columnStore, index *btree.BTreeG[int32]) error {
	filename, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	tmpFileName := path.Join(fs.path, "tmp", filename.String()+".1.parquet")
	err = fs.saveTmpFile(tmpFileName, fields, unorderedData, orderedData, index)
	if err != nil {
		return err
	}
	return os.Rename(tmpFileName, path.Join(fs.path, "data", filename.String()+".1.parquet"))
}
