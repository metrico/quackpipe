package service

import (
	"errors"
	"fmt"
	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/metrico/quackpipe/config"
	"github.com/metrico/quackpipe/merge/data_types"
	"github.com/metrico/quackpipe/model"
	"github.com/metrico/quackpipe/utils/promise"
	url2 "net/url"
	"path"
	"path/filepath"
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
	Table              *model.Table
	ticker             *time.Ticker
	working            uint32
	promises           []promise.Promise[int32]
	mtx                sync.Mutex
	save               saveService
	merge              mergeService
	lastIterationTime  [3]time.Time
	unorderedDataStore *unorderedDataStore
	orderedDataStore   *orderedDataStore

	less func(store any, i int32, j int32) bool
}

func NewMergeTreeService(t *model.Table) (*MergeTreeService, error) {
	res := &MergeTreeService{
		Table:    t,
		working:  0,
		promises: nil,
	}
	res.unorderedDataStore = newUnorderedDataStore()
	res.orderedDataStore = newOrderedDataStore(t.OrderBy[0])
	var err error
	tablePath := t.Path
	if tablePath == "" {
		tablePath = filepath.Join(config.Config.QuackPipe.Root, t.Name)
	}
	res.save, err = res.newSaveService(path.Join(tablePath, "data"), path.Join(tablePath, "tmp"))
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
		dataPath: path.Join(s.Table.Path, "data"),
		tmpPath:  path.Join(s.Table.Path, "tmp"),
		table:    s.Table,
	}, nil
}

func (s *MergeTreeService) newS3MergeService() (mergeService, error) {
	s3Conf, err := s.getS3Config(s.Table.Path)
	if err != nil {
		return nil, err
	}
	return &s3MergeService{
		fsMergeService: fsMergeService{
			tmpPath: path.Join(config.Config.QuackPipe.Root, s.Table.Name, "tmp"),
			table:   s.Table,
		},
		s3Config: s3Conf,
	}, nil
}

func (s *MergeTreeService) newSaveService(dataPath, tmpPath string) (saveService, error) {
	if strings.HasPrefix(dataPath, "s3://") {
		return s.newS3SaveService(dataPath)
	}
	return s.newFileSaveService(dataPath, tmpPath)
}

func (s *MergeTreeService) newFileSaveService(dataPath, tmpPath string) (saveService, error) {
	return &fsSaveService{
		dataPath: dataPath,
		tmpPath:  tmpPath,
	}, nil
}

func (s *MergeTreeService) newS3SaveService(dataPath string) (saveService, error) {
	s3Conf, err := s.getS3Config(dataPath)
	if err != nil {
		return nil, err
	}
	res := &s3SaveService{
		fsSaveService: fsSaveService{
			dataPath: "",
			tmpPath:  path.Join(config.Config.QuackPipe.Root, s.Table.Name, "tmp"),
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
	return s.unorderedDataStore.GetSize() + s.orderedDataStore.GetSize()
}

func validateData(dataStore dataStore, data map[string]*model.ColumnStore) error {
	var (
		size int64
		i    int
	)
	for k, field := range data {
		_, tp := data_types.GoTypeToDataType(field)
		if i == 0 {
			size = tp.GetLength(field)
		} else if size != tp.GetLength(field) {
			return fmt.Errorf("column %s size mismatch: expected %d rows, got %d rows",
				k, size, tp.GetLength(field))
		}
		i++
	}

	return dataStore.VerifyData(data)
}

func mergeColumns(unordered, ordered dataStore) []fieldDesc {
	mergedCols := make(map[string]data_types.DataType)
	names, types := unordered.GetSchema()
	for i, name := range names {
		mergedCols[name] = types[i]
	}
	names, types = ordered.GetSchema()
	for i, name := range names {
		mergedCols[name] = types[i]
	}
	var res []fieldDesc
	for f, t := range mergedCols {
		res = append(res, fd(t.GetName(), f))
	}
	return res
}

func (s *MergeTreeService) flush() {
	s.mtx.Lock()
	unorderedDataStore := s.unorderedDataStore
	orderedDataStore := s.orderedDataStore
	s.unorderedDataStore = newUnorderedDataStore()
	s.orderedDataStore = newOrderedDataStore(s.Table.OrderBy[0])
	promises := s.promises
	s.promises = nil
	s.mtx.Unlock()
	onError := func(err error) {
		for _, p := range promises {
			p.Done(0, err)
		}
	}
	if unorderedDataStore.GetSize()+orderedDataStore.GetSize() == 0 {
		onError(nil)
		return
	}
	go func() {
		_, err := s.save.Save(mergeColumns(unorderedDataStore, orderedDataStore),
			unorderedDataStore, orderedDataStore)
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

func (s *MergeTreeService) validateColSizes(columns map[string]*model.ColumnStore) error {
	var size int64
	i := -1
	for k, col := range columns {
		i++
		if i == 0 {
			size = col.Tp.GetLength(col.Data)
			continue
		}
		if size != col.Tp.GetLength(col.Data) {
			return fmt.Errorf("column %s size mismatch: expected %d rows, got %d rows",
				k, size, col.Tp.GetLength(col.Data))
		}
	}
	return nil
}

func (s *MergeTreeService) validateData(columns map[string]*model.ColumnStore) error {
	err := s.validateColSizes(columns)
	if err != nil {
		return err
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()
	err = validateData(s.unorderedDataStore, columns)
	if err != nil {
		return err
	}
	return validateData(s.orderedDataStore, columns)
}

func (s *MergeTreeService) wrapColumns(columns map[string]any) map[string]*model.ColumnStore {
	_columns := make(map[string]*model.ColumnStore, len(columns)+1)
	var sz int

	for k, v := range columns {
		_, tp := data_types.GoTypeToDataType(v)
		sz = int(tp.GetLength(v))
		_columns[k] = &model.ColumnStore{
			Data:   v,
			Tp:     tp,
			Valids: fastFillArray(make([]bool, sz), true),
		}
	}
	return _columns
}

func (s *MergeTreeService) AutoTimestamp(columns map[string]*model.ColumnStore) map[string]*model.ColumnStore {
	if !s.Table.AutoTimestamp {
		return columns
	}

	var sz int
	for _, col := range columns {
		sz = int(col.Tp.GetLength(col.Data))
		break
	}

	tsCol := &model.ColumnStore{
		Data:   data_types.DataTypes["INT8"].MakeStore(sz),
		Valids: fastFillArray(make([]bool, sz), true),
		Tp:     data_types.DataTypes["INT8"],
	}
	for i := 0; i < sz; i++ {
		tsCol.Data.([]int64)[i] = time.Now().UnixNano()
	}
	columns["__timestamp"] = tsCol
	return columns
}

func (s *MergeTreeService) Store(columns map[string]any) promise.Promise[int32] {
	_columns := s.wrapColumns(columns)

	err := s.validateData(_columns)
	if err != nil {
		return promise.Fulfilled(err, int32(0))
	}

	_columns = s.AutoTimestamp(_columns)

	s.mtx.Lock()
	defer s.mtx.Unlock()

	var ds dataStore = s.unorderedDataStore
	if _, ok := _columns[s.Table.OrderBy[0]]; ok {
		ds = s.orderedDataStore
	}
	err = ds.AppendData(_columns)
	if err != nil {
		return promise.Fulfilled(err, int32(0))
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

func (s *MergeTreeService) DoMerge() error {
	plan, err := s.PlanMerge()
	if err != nil {
		return err
	}
	return s.Merge(plan)
}

type MergeService interface {
	Run()
	Stop()
	Store(columns map[string]any) promise.Promise[int32]
	DoMerge() error
	/*PlanMerge() ([]PlanMerge, error)
	Merge(plan []PlanMerge) error*/
}
