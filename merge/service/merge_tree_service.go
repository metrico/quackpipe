package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/google/uuid"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"os"
	"path/filepath"
	"quackpipe/model"
	"quackpipe/service/db"
	"quackpipe/utils/promise"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type IMergeTree interface {
	Store(columns map[string][]any) error
	Merge() error
	Run()
	Stop()
}

type MergeTreeService struct {
	Table             *model.Table
	ticker            *time.Ticker
	working           uint32
	promises          []*promise.Promise[int32]
	recordBatch       *array.RecordBuilder
	mtx               sync.Mutex
	schema            *arrow.Schema
	lastIterationTime [3]time.Time
	dataIndexes       *btree.BTreeG[int32]
	dataStore         map[string]any
}

func NewMergeTreeService(t *model.Table) *MergeTreeService {
	res := &MergeTreeService{
		Table:       t,
		working:     0,
		promises:    []*promise.Promise[int32]{},
		recordBatch: nil,
	}
	res.dataStore = res.createDataStore()
	res.dataIndexes = btree.NewBTreeG(res.Less)
	res.schema = res.createParquetSchema()
	pool := memory.NewGoAllocator()
	res.recordBatch = array.NewRecordBuilder(pool, res.schema)

	return res
}

func (s *MergeTreeService) createDataStore() map[string]any {
	res := make(map[string]any)
	for _, f := range s.Table.Fields {
		switch f[1] {
		case "UInt64":
			res[f[0]] = make([]uint64, 0, 1000000)
		case "Int64":
			res[f[0]] = make([]int64, 0, 1000000)
		case "String":
			res[f[0]] = make([]string, 0, 1000000)
		case "Float64":
			res[f[0]] = make([]float64, 0, 1000000)
		}
	}
	return res
}

func (s *MergeTreeService) size() int32 {
	return int32(s.dataIndexes.Len())
}

func getFieldType(t *model.Table, fieldName string) string {
	for _, field := range t.Fields {
		if field[0] == fieldName {
			return field[1]
		}
	}
	return ""
}

func (s *MergeTreeService) Less(a, b int32) bool {
	for _, o := range s.Table.OrderBy {
		t := getFieldType(s.Table, o)
		switch t {
		case "UInt64":
			if s.dataStore[o].([]uint64)[a] > s.dataStore[o].([]uint64)[b] {
				return false
			}
		case "Int64":
			if s.dataStore[o].([]int64)[a] > s.dataStore[o].([]int64)[b] {
				return false
			}
		case "String":
			if s.dataStore[o].([]string)[a] > s.dataStore[o].([]string)[b] {
				return false
			}
		case "Float64":
			if s.dataStore[o].([]float64)[a] > s.dataStore[o].([]float64)[b] {
				return false
			}
		}
	}
	return true
}

func GetColumnLength(column any) int {
	switch column := column.(type) {
	case []string:
		return len(column)
	case []int64:
		return len(column)
	case []uint64:
		return len(column)
	case []float64:
		return len(column)
	default:
		return 0
	}
}

func validateData(table *model.Table, columns map[string]any) error {

	fieldMap := make(map[string]string)
	for _, field := range table.Fields {
		fieldMap[field[0]] = field[1]
	}

	// Check if columns map size matches the table.Fields size
	if len(columns) != len(table.Fields) {
		return errors.New("columns size does not match table fields size")
	}

	var (
		dataLength int
		first      = true
	)
	for _, data := range columns {
		if first {
			dataLength = GetColumnLength(data)
			first = false
			continue
		}
		if GetColumnLength(data) != dataLength {
			return errors.New("columns length mismatch")
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
			if _, ok := data.([]uint64); !ok {
				return fmt.Errorf("invalid data type for column %s: expected uint64", column)
			}
		case "Int64":
			if _, ok := data.([]int64); !ok {
				return fmt.Errorf("invalid data type for column %s: expected int64", column)
			}
		case "String":
			if _, ok := data.([]string); !ok {
				return fmt.Errorf("invalid data type for column %s: expected string", column)
			}
		case "Float64":
			if _, ok := data.([]float64); !ok {
				return fmt.Errorf("invalid data type for column %s: expected float64", column)
			}
		default:
			return fmt.Errorf("unsupported column type: %s", columnType)
		}
	}

	return nil
}

func (s *MergeTreeService) createParquetSchema() *arrow.Schema {
	fields := make([]arrow.Field, len(s.Table.Fields))
	for i, field := range s.Table.Fields {
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

func (s *MergeTreeService) writeParquetFile(columns map[string]any) *promise.Promise[int32] {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	var oldSize, newSize int32
	for k, v := range columns {
		tp := getFieldType(s.Table, k)
		switch tp {
		case "UInt64":
			oldSize = int32(len(s.dataStore[k].([]uint64)))
			s.dataStore[k] = append(s.dataStore[k].([]uint64), v.([]uint64)...)
			newSize = int32(len(s.dataStore[k].([]uint64)))
		case "Int64":
			oldSize = int32(len(s.dataStore[k].([]int64)))
			s.dataStore[k] = append(s.dataStore[k].([]int64), v.([]int64)...)
			newSize = int32(len(s.dataStore[k].([]int64)))
		case "String":
			oldSize = int32(len(s.dataStore[k].([]string)))
			s.dataStore[k] = append(s.dataStore[k].([]string), v.([]string)...)
			newSize = int32(len(s.dataStore[k].([]string)))
		case "Float64":
			oldSize = int32(len(s.dataStore[k].([]float64)))
			s.dataStore[k] = append(s.dataStore[k].([]float64), v.([]float64)...)
			newSize = int32(len(s.dataStore[k].([]float64)))
		}
	}
	for i := oldSize; i < newSize; i++ {
		s.dataIndexes.Set(i)
	}

	p := promise.New[int32]()
	s.promises = append(s.promises, p)
	return p
}

func (s *MergeTreeService) flush() {
	s.mtx.Lock()
	dataStore := s.dataStore
	indexes := s.dataIndexes
	s.dataStore = s.createDataStore()
	s.dataIndexes = btree.NewBTreeG(s.Less)
	promises := s.promises
	s.promises = nil
	s.mtx.Unlock()
	onError := func(err error) {
		for _, p := range promises {
			p.Done(0, err)
		}
	}
	if indexes.Len() == 0 {
		onError(nil)
		return
	}
	for i, f := range s.Table.Fields {
		it := indexes.Iter()
		switch f[1] {
		case "UInt64":
			_data := dataStore[f[0]].([]uint64)
			for it.Next() {
				s.recordBatch.Field(i).(*array.Uint64Builder).Append(_data[it.Item()])
			}
		case "Int64":
			_data := dataStore[f[0]].([]int64)
			for it.Next() {
				s.recordBatch.Field(i).(*array.Int64Builder).Append(_data[it.Item()])
			}
		case "String":
			_data := dataStore[f[0]].([]string)
			for it.Next() {
				s.recordBatch.Field(i).(*array.StringBuilder).Append(_data[it.Item()])
			}
		case "Float64":
			_data := dataStore[f[0]].([]float64)
			for it.Next() {
				s.recordBatch.Field(i).(*array.Float64Builder).Append(_data[it.Item()])
			}
		}
	}
	record := s.recordBatch.NewRecord()
	defer record.Release()
	if record.Column(0).Data().Len() == 0 {
		onError(nil)
		return
	}
	fileName := uuid.New().String() + ".1.parquet"
	outputTmpFile := filepath.Join(s.Table.Path, "data", fileName)
	outputFile := filepath.Join(s.Table.Path, "data", fileName)
	file, err := os.Create(outputTmpFile)
	if err != nil {
		onError(err)
		return
	}
	defer file.Close()
	// Set up Parquet writer properties
	writerProps := parquet.NewWriterProperties(
		parquet.WithMaxRowGroupLength(100),
	)
	arrprops := pqarrow.NewArrowWriterProperties()

	// Create Parquet file writer
	writer, err := pqarrow.NewFileWriter(s.schema, file, writerProps, arrprops)
	if err != nil {
		onError(err)
		return
	}
	defer writer.Close()
	err = writer.Write(record)
	if err != nil {
		onError(err)
		return
	}
	onError(os.Rename(outputTmpFile, outputFile))
}

func (s *MergeTreeService) Run() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if !atomic.CompareAndSwapUint32(&s.working, 0, 1) {
		return
	}
	go func() {
		s.ticker = time.NewTicker(time.Millisecond * 100)
		for range s.ticker.C {
			s.flush()
		}
	}()
}

func (s *MergeTreeService) Stop() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if s.ticker != nil {
		s.ticker.Stop()
	}
	if s.recordBatch != nil {
		s.recordBatch.Release()
	}
	atomic.StoreUint32(&s.working, 0)
}

func (s *MergeTreeService) Store(columns map[string]any) *promise.Promise[int32] {
	if err := validateData(s.Table, columns); err != nil {
		return promise.Fulfilled(err, int32(0))
	}

	return s.writeParquetFile(columns)
}

type PlanMerge struct {
	From      []string
	To        string
	Iteration int
}

type FileDesc struct {
	name string
	size int64
}

func (s *MergeTreeService) planMerge(dataDir string) ([]PlanMerge, error) {
	files, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}
	var parquetFiles []FileDesc
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".parquet") {
			name := filepath.Join(dataDir, file.Name())
			stat, err := os.Stat(name)
			if err != nil {
				return nil, err
			}
			parquetFiles = append(parquetFiles, struct {
				name string
				size int64
			}{name, stat.Size()})
		}
	}
	sort.Slice(parquetFiles, func(a, b int) bool {
		return parquetFiles[a].size > parquetFiles[b].size
	})
	res := make([]PlanMerge, 0)
	if time.Now().Sub(s.lastIterationTime[0]).Seconds() > 10 {
		var _res []PlanMerge
		parquetFiles, _res = s._planMerge(parquetFiles, 40*1024*1024, 40*1024*1024, 1)
		res = append(res, _res...)
		s.lastIterationTime[0] = time.Now()
	}
	if time.Now().Sub(s.lastIterationTime[1]).Seconds() > 100 {
		var _res []PlanMerge
		parquetFiles, _res = s._planMerge(parquetFiles, 400*1024*1024, 400*1024*1024, 2)
		res = append(res, _res...)
		s.lastIterationTime[1] = time.Now()
	}
	if time.Now().Sub(s.lastIterationTime[2]).Seconds() > 1000 {
		var _res []PlanMerge
		parquetFiles, _res = s._planMerge(parquetFiles, 4000*1024*1024, 4000*1024*1024, 3)
		res = append(res, _res...)
		s.lastIterationTime[2] = time.Now()
	}
	return res, nil
}

func checkSuffix(name string, iteration int) bool {
	for i := iteration + 1; i >= 1; i-- {
		if strings.HasSuffix(name, fmt.Sprintf("%d.parquet", i)) {
			return true
		}
	}
	return false
}

func (s *MergeTreeService) _planMerge(parquetFiles []FileDesc, maxFileSize int64,
	maxResSize int64, iteration int) ([]FileDesc, []PlanMerge) {
	res := make([]PlanMerge, 1)
	res[0].To = fmt.Sprintf("%s_%d.%d.parquet", s.Table.Name, time.Now().UnixNano(), iteration+1)
	res[0].Iteration = iteration
	mergeSize := int64(0)
	for i := len(parquetFiles) - 1; i >= 0; i-- {
		if !checkSuffix(parquetFiles[i].name, iteration) {
			continue
		}
		if parquetFiles[i].size > maxFileSize {
			break
		}
		mergeSize += parquetFiles[i].size
		res[len(res)-1].From = append(res[len(res)-1].From, parquetFiles[i].name)
		if mergeSize > maxResSize {
			res = append(res, PlanMerge{
				From:      nil,
				To:        fmt.Sprintf("%s_%d.%d.parquet", s.Table.Name, time.Now().UnixNano(), iteration+1),
				Iteration: iteration,
			})
			mergeSize = 0
		}
		parquetFiles = parquetFiles[:i]
	}
	for len(res) > 0 && len(res[len(res)-1].From) < 1 {
		res = res[:len(res)-1]
	}
	return parquetFiles, res
}

// Merge method implementation
func (s *MergeTreeService) Merge() error {
	dataDir := filepath.Join(s.Table.Path, "data")
	tmpDir := filepath.Join(s.Table.Path, "tmp")

	plan, err := s.planMerge(dataDir)
	if err != nil {
		return err
	}
	sem := semaphore.NewWeighted(10)
	wg := errgroup.Group{}
	for _, p := range plan {
		_p := p
		wg.Go(func() error {
			sem.Acquire(context.Background(), 1)
			defer sem.Release(1)
			return mergeFiles(s.Table, &_p, tmpDir, dataDir)
		})
	}
	return nil
}

func mergeFiles(table *model.Table, p *PlanMerge, tmpDir, dataDir string) error {
	// Create a temporary merged file
	tmpFilePath := filepath.Join(tmpDir, p.To)

	// Prepare DuckDB connection

	conn, err := db.ConnectDuckDB("?allow_unsigned_extensions=1")
	if err != nil {
		return err
	}
	_, err = conn.Exec("INSTALL chsql FROM community")
	if err != nil {
		fmt.Println("Error loading chsql extension: ", err)
		return err
	}
	_, err = conn.Exec("LOAD chsql")
	if err != nil {
		fmt.Println("Error loading chsql extension: ", err)
		return err
	}
	defer conn.Close()

	createTableSQL := fmt.Sprintf(
		`COPY(SELECT * FROM read_parquet_mergetree(ARRAY['%s'], '%s'))TO '%s' (FORMAT 'parquet')`,
		strings.Join(p.From, "','"),
		strings.Join(table.OrderBy, ","), tmpFilePath)
	_, err = conn.Exec(createTableSQL)

	if err != nil {
		fmt.Println("Error read_parquet_mergetree: ", err)
		return err
	}

	// Cleanup old files
	for _, file := range p.From {
		if err := os.Remove(file); err != nil {
			return err
		}
	}

	finalFilePath := filepath.Join(dataDir, p.To)
	if err := os.Rename(tmpFilePath, finalFilePath); err != nil {
		return err
	}

	return nil
}
