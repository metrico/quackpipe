package service

import (
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	_ "github.com/marcboeker/go-duckdb/v2"
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

type columnStore struct {
	data   any
	valids []bool
	tp     data_types.DataType
}

type MergeTreeService struct {
	Table              *model.Table
	ticker             *time.Ticker
	working            uint32
	promises           []*promise.Promise[int32]
	mtx                sync.Mutex
	save               saveService
	merge              mergeService
	lastIterationTime  [3]time.Time
	dataIndexes        *btree.BTreeG[int32]
	unorderedDataStore map[string]*columnStore
	orderedDataStore   map[string]*columnStore

	less func(store any, i int32, j int32) bool
}

func NewMergeTreeService(t *model.Table) (*MergeTreeService, error) {
	res := &MergeTreeService{
		Table:    t,
		working:  0,
		promises: []*promise.Promise[int32]{},
	}
	res.unorderedDataStore = make(map[string]*columnStore)
	res.orderedDataStore = make(map[string]*columnStore)
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
	res.lastIterationTime = [3]time.Time{time.Now(), time.Now(), time.Now()}
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
	return &fsSaveService{
		path: path,
	}, nil
}

func (s *MergeTreeService) newS3SaveService(path string) (saveService, error) {
	s3Conf, err := s.getS3Config(path)
	if err != nil {
		return nil, err
	}
	res := &s3SaveService{
		fsSaveService: fsSaveService{
			path: "",
		},
		s3Config: s3Conf,
	}
	return res, nil
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

func (s *MergeTreeService) size() int32 {
	return int32(s.dataIndexes.Len())
}

func (s *MergeTreeService) getFieldType(fieldName string) (data_types.DataType, error) {
	field, ok := s.unorderedDataStore[fieldName]
	if !ok {
		field, ok = s.orderedDataStore[fieldName]
	}
	if !ok {
		return data_types.DataTypes["UNKNOWN"], fmt.Errorf("column %s not found", fieldName)
	}
	return field.tp, nil
}

func (s *MergeTreeService) Less(a, b int32) bool {
	if s.less == nil {
		return false
	}
	return s.less(s.orderedDataStore[s.Table.OrderBy[0]].data, a, b)
}

func (s *MergeTreeService) validateData(columns map[string]any) error {
	for k, v := range columns {
		insertTypeName, _ := data_types.GoTypeToDataType(v)
		field, ok := s.unorderedDataStore[k]
		if ok {
			existTypeName, _ := data_types.GoTypeToDataType(field.data)
			if insertTypeName != existTypeName {
				return fmt.Errorf("column `%s` type mismatch: expected %s, got %s",
					k, existTypeName, insertTypeName)
			}
		}
		field, ok = s.orderedDataStore[k]
		if ok {
			existTypeName, _ := data_types.GoTypeToDataType(field.data)
			if insertTypeName != existTypeName {
				return fmt.Errorf("column `%s` type mismatch: expected %s, got %s",
					k, existTypeName, insertTypeName)
			}
		}
	}
	return nil
}

func mergeColumns(unordered, ordered map[string]*columnStore) []fieldDesc {
	mergedCols := make(map[string]string)
	for f, _ := range unordered {
		mergedCols[f] = unordered[f].tp.GetName()
	}
	for f, _ := range ordered {
		mergedCols[f] = ordered[f].tp.GetName()
	}
	var res []fieldDesc
	for f, t := range mergedCols {
		res = append(res, fd(t, f))
	}
	return res
}

func (s *MergeTreeService) createParquetSchema() *arrow.Schema {
	mergedCols := mergeColumns(s.unorderedDataStore, s.orderedDataStore)
	fields := make([]arrow.Field, len(mergedCols))
	i := 0
	for _, field := range mergedCols {
		var fieldType = data_types.DataTypes[field[0]]
		fields[i] = arrow.Field{Name: field[1], Type: fieldType.ArrowDataType()}
		i++
	}
	return arrow.NewSchema(fields, nil)
}

func storeSize(columns map[string]*columnStore) int64 {
	for _, col := range columns {
		return col.tp.GetLength(col.data)
	}
	return 0
}

func (s *MergeTreeService) flush() {
	s.mtx.Lock()
	unorderedDataStore := s.unorderedDataStore
	orderedDataStore := s.orderedDataStore
	indexes := s.dataIndexes
	s.unorderedDataStore = make(map[string]*columnStore)
	s.orderedDataStore = make(map[string]*columnStore)
	s.dataIndexes = btree.NewBTreeG(s.Less)
	promises := s.promises
	s.promises = nil
	s.mtx.Unlock()
	onError := func(err error) {
		for _, p := range promises {
			p.Done(0, err)
		}
	}
	if indexes.Len() == 0 && storeSize(unorderedDataStore) == 0 {
		onError(nil)
		return
	}
	go func() {
		err := s.save.Save(mergeColumns(unorderedDataStore, orderedDataStore),
			unorderedDataStore, orderedDataStore, indexes)
		onError(err)
	}()
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

func fastFillArray[T any](arr []T, data T) []T {
	if len(arr) == 0 {
		return arr
	}
	arr[0] = data
	for i := 1; i < len(arr); i *= 2 {
		copy(arr[i:], arr[:i])
	}
	return arr
}

func appendDataStore(store *map[string]*columnStore, data map[string]*columnStore) error {
	var columnsToCreate []fieldDesc
	for k, v := range data {
		if _, ok := (*store)[k]; !ok {
			columnsToCreate = append(columnsToCreate, fd(v.tp.GetName(), k))
		}
	}
	var columnsToAppend []string
	for k, _ := range *store {
		if _, ok := data[k]; !ok {
			columnsToAppend = append(columnsToAppend, k)
		}
	}
	var originalStoreSize = 0
	for _, f := range *store {
		originalStoreSize = int(f.tp.GetLength(f.data))
		break
	}

	var originalDataSize = 0
	for _, f := range data {
		originalDataSize = int(f.tp.GetLength(f.data))
		break
	}

	for _, f := range columnsToCreate {
		newCol := &columnStore{
			data:   data_types.DataTypes[f[0]].MakeStore(originalStoreSize),
			valids: make([]bool, originalStoreSize),
			tp:     data_types.DataTypes[f[0]],
		}
		(*store)[f[1]] = newCol
	}
	for _, f := range columnsToAppend {
		(*store)[f].data = (*store)[f].tp.AppendDefault(originalDataSize, (*store)[f].data)
		(*store)[f].valids = append(
			(*store)[f].valids, make([]bool, originalDataSize)...,
		)
	}
	for k, v := range data {
		var err error
		(*store)[k].data, err = (*store)[k].tp.AppendStore((*store)[k].data, v.data)
		if err != nil {
			return fmt.Errorf("appending data to column `%s`: %w", k, err)
		}
		(*store)[k].valids = append((*store)[k].valids, v.valids...)
	}
	return nil
}

func (s *MergeTreeService) Store(columns map[string]any) *promise.Promise[int32] {
	if err := s.validateData(columns); err != nil {
		return promise.Fulfilled(err, int32(0))
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	_columns := make(map[string]*columnStore, len(columns)+1)
	var sz int

	for k, v := range columns {
		_, tp := data_types.GoTypeToDataType(v)
		sz = int(tp.GetLength(v))
		_columns[k] = &columnStore{
			data:   v,
			tp:     tp,
			valids: fastFillArray(make([]bool, sz), true),
		}
	}
	if s.Table.AutoTimestamp {
		tsCol := &columnStore{
			data:   data_types.DataTypes["INT8"].MakeStore(sz),
			valids: fastFillArray(make([]bool, sz), true),
			tp:     data_types.DataTypes["INT8"],
		}
		for i := 0; i < sz; i++ {
			tsCol.data.([]int64)[i] = time.Now().UnixNano()
		}
		_columns["__timestamp"] = tsCol
	}

	if _, ok := _columns[s.Table.OrderBy[0]]; !ok {
		err := appendDataStore(&s.unorderedDataStore, _columns)
		if err != nil {
			return promise.Fulfilled(err, int32(0))
		}
	} else {
		if s.less == nil {
			s.less = _columns[s.Table.OrderBy[0]].tp.Less
		}
		var oldSize, newSize int32
		if col, ok := s.orderedDataStore[s.Table.OrderBy[0]]; ok {
			oldSize = int32(col.tp.GetLength(col.data))
		}
		err := appendDataStore(&s.orderedDataStore, _columns)
		if err != nil {
			return promise.Fulfilled(err, int32(0))
		}
		if col, ok := s.orderedDataStore[s.Table.OrderBy[0]]; ok {
			newSize = int32(col.tp.GetLength(col.data))
		}
		for i := oldSize; i < newSize; i++ {
			s.dataIndexes.Set(i)
		}
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

func (s *MergeTreeService) PlanMerge() ([]PlanMerge, error) {
	var res []PlanMerge
	// configuration - timeout_s - max_res_size_bytes - iteration_id
	configurations := [][3]int64{
		{10, 40 * 1024 * 1024, 1},
		{100, 400 * 1024 * 1024, 2},
		{1000, 4000 * 1024 * 1024, 3},
	}
	for _, conf := range configurations {
		if time.Now().Sub(s.lastIterationTime[conf[2]-1]).Seconds() > float64(conf[0]) {
			files, err := s.merge.GetFilesToMerge(int(conf[2]))
			if err != nil {
				return nil, err
			}
			plans := s.merge.PlanMerge(files, conf[1], int(conf[2]))
			res = append(res, plans...)
			s.lastIterationTime[conf[2]-1] = time.Now()
		}
	}
	return res, nil
}

// Merge method implementation
func (s *MergeTreeService) Merge(plan []PlanMerge) error {
	return s.merge.DoMerge(plan)
}
