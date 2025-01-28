package service

import (
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/tidwall/btree"
	url2 "net/url"
	"path"
	"path/filepath"
	"quackpipe/config"
	"quackpipe/merge/data_types"
	"quackpipe/model"
	"quackpipe/utils/promise"
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
	mtx               sync.Mutex
	save              saveService
	merge             mergeService
	lastIterationTime [3]time.Time
	dataIndexes       *btree.BTreeG[int32]
	dataStore         map[string]any
}

func NewMergeTreeService(t *model.Table) (*MergeTreeService, error) {
	res := &MergeTreeService{
		Table:    t,
		working:  0,
		promises: []*promise.Promise[int32]{},
	}
	res.dataStore = res.createDataStore()
	res.dataIndexes = btree.NewBTreeG(res.Less)
	var err error
	path := t.Path
	if path == "" {
		path = filepath.Join(config.Config.QuackPipe.Root, t.Name)
	}
	res.save, err = res.newSaveService(path)
	if err != nil {
		return nil, err
	}
	res.merge, err = res.newMergeService()
	return res, err
}

func (s *MergeTreeService) newMergeService() (mergeService, error) {
	if strings.HasPrefix(s.Table.Path, "s3://") {
		return s.newS3MergeService()
	}
	return s.newFileMergeService()
}

func (s *MergeTreeService) newFileMergeService() (mergeService, error) {
	return &fsMergeService{
		path:  s.Table.Path,
		table: s.Table,
	}, nil
}

func (s *MergeTreeService) newS3MergeService() (mergeService, error) {
	s3Conf, err := s.getS3Config(s.Table.Path)
	if err != nil {
		return nil, err
	}
	return &s3MergeService{
		fsMergeService: fsMergeService{
			path:  path.Join(config.Config.QuackPipe.Root, s.Table.Name, "tmp"),
			table: s.Table,
		},
		s3Config: s3Conf,
	}, nil
}

func (s *MergeTreeService) newSaveService(path string) (saveService, error) {
	if strings.HasPrefix(path, "s3://") {
		return s.newS3SaveService(path)
	}
	return s.newFileSaveService(path)
}

func (s *MergeTreeService) newFileSaveService(path string) (saveService, error) {
	schema := s.createParquetSchema()
	return &fsSaveService{
		path:        path,
		recordBatch: array.NewRecordBuilder(memory.NewGoAllocator(), schema),
		schema:      schema,
	}, nil
}

func (s *MergeTreeService) getS3Config(path string) (s3Config, error) {
	url, err := url2.Parse(path)
	if err != nil {
		return s3Config{}, err
	}
	if url.Scheme != "s3" {
		return s3Config{}, errors.New("invalid S3 URL")
	}
	pass, _ := url.User.Password()
	bucketPath := strings.SplitN(strings.TrimPrefix(url.Path, "/"), "/", 2)
	secure := !(url.Query().Get("secure") == "false")
	region := ""
	if url.Query().Get("region") != "" {
		region = url.Query().Get("region")
	}
	return s3Config{
		url:    url.Host,
		key:    url.User.Username(),
		secret: pass,
		bucket: bucketPath[0],
		region: region,
		path:   bucketPath[1],
		secure: secure,
	}, nil
}

func (s *MergeTreeService) newS3SaveService(path string) (saveService, error) {
	s3Conf, err := s.getS3Config(path)
	if err != nil {
		return nil, err
	}
	schema := s.createParquetSchema()
	res := &s3SaveService{
		fsSaveService: fsSaveService{
			path:        "",
			recordBatch: array.NewRecordBuilder(memory.NewGoAllocator(), schema),
			schema:      schema,
		},
		s3Config: s3Conf,
	}
	return res, nil
}

func (s *MergeTreeService) createDataStore() map[string]any {
	res := make(map[string]any)
	for _, f := range s.Table.Fields {
		res[f[0]] = data_types.DataTypes[f[1]].MakeStore()
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
		if !data_types.DataTypes[t].Less(s.dataStore[o], a, b) {
			return false
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
		t, ok := data_types.DataTypes[columnType]
		if !ok {
			return fmt.Errorf("unsupported column type: %s", columnType)
		}
		err := t.ValidateData(data)
		if err != nil {
			return fmt.Errorf("invalid data for column %s: %w", column, err)
		}
	}

	return nil
}

func (s *MergeTreeService) createParquetSchema() *arrow.Schema {
	fields := make([]arrow.Field, len(s.Table.Fields))
	for i, field := range s.Table.Fields {
		var fieldType = data_types.DataTypes[field[1]].ArrowDataType()
		fields[i] = arrow.Field{Name: field[0], Type: fieldType}
	}
	return arrow.NewSchema(fields, nil)
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
	err := s.save.Save(s.Table.Fields, dataStore, indexes)
	onError(err)
}

func (s *MergeTreeService) Run() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if !atomic.CompareAndSwapUint32(&s.working, 0, 1) {
		return
	}
	go func() {
		s.ticker = time.NewTicker(time.Millisecond * time.Duration(config.Config.QuackPipe.SaveTimeoutS*1000))
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
	atomic.StoreUint32(&s.working, 0)
}

func (s *MergeTreeService) Store(columns map[string]any) *promise.Promise[int32] {
	if err := validateData(s.Table, columns); err != nil {
		return promise.Fulfilled(err, int32(0))
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	var oldSize, newSize int32
	for k, v := range columns {
		tp := getFieldType(s.Table, k)

		var err error
		oldSize = int32(GetColumnLength(s.dataStore[k]))
		s.dataStore[k], err = data_types.DataTypes[tp].AppendStore(s.dataStore[k], v)
		if err != nil {
			return promise.Fulfilled[int32](err, 0)
		}
		newSize = int32(GetColumnLength(s.dataStore[k]))
	}
	for i := oldSize; i < newSize; i++ {
		s.dataIndexes.Set(i)
	}

	p := promise.New[int32]()
	s.promises = append(s.promises, p)
	return p
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

func (s *MergeTreeService) planMerge() ([]PlanMerge, error) {
	var res []PlanMerge
	// configuration - timeout_s - max_res_size_bytes - iteration_id
	configurations := [][3]int64{
		{10, 40 * 1024 * 1024, 1},
		{100, 400 * 1024 * 1024, 2},
		{1000, 4000 * 1024 * 1024, 3},
	}
	for _, conf := range configurations {
		if time.Now().Sub(s.lastIterationTime[0]).Seconds() > float64(conf[0]) {
			files, err := s.merge.GetFilesToMerge(int(conf[2]))
			if err != nil {
				return nil, err
			}
			plans := s.merge.PlanMerge(files, conf[1], int(conf[2]))
			res = append(res, plans...)
		}
	}
	return res, nil
}

// Merge method implementation
func (s *MergeTreeService) Merge() error {
	plan, err := s.planMerge()
	if err != nil {
		return err
	}
	return s.merge.DoMerge(plan)
}
