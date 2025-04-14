package service

import (
	"github.com/metrico/quackpipe/merge/data_types"
	"github.com/metrico/quackpipe/model"
	"github.com/metrico/quackpipe/utils/promise"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Partition struct {
	Values            [][2]string
	index             model.Index
	unordered         *unorderedDataStore
	saveService       saveService
	mergeService      mergeService
	promises          []promise.Promise[int32]
	m                 sync.Mutex
	table             *model.Table
	lastStore         time.Time
	lastSave          time.Time
	lastIterationTime [3]time.Time
	dataPath          string
}

func NewPartition(values [][2]string, tmpPath, dataPath string, t *model.Table) (*Partition, error) {
	res := &Partition{
		Values:            values,
		unordered:         newUnorderedDataStore(),
		table:             t,
		lastIterationTime: [3]time.Time{time.Now(), time.Now(), time.Now()},
		dataPath:          dataPath,
	}
	if t.IndexCreator != nil {
		var err error
		res.index, err = t.IndexCreator(values)
		if err != nil {
			return nil, err
		}
	}
	err := res.initServices(tmpPath, dataPath, t)
	return res, err
}

func (p *Partition) initServices(tmpPath, dataPath string, t *model.Table) error {
	err := os.MkdirAll(tmpPath, 0755)
	if err != nil {
		return err
	}
	err = os.MkdirAll(dataPath, 0755)
	if err != nil {
		return err
	}

	p.saveService = &fsSaveService{
		dataPath: dataPath,
		tmpPath:  tmpPath,
	}
	p.mergeService = &fsMergeService{
		dataPath: dataPath,
		tmpPath:  tmpPath,
		table:    t,
		index:    p.index,
	}
	return nil
}

func (p *Partition) GetSchema() map[string]string {
	//TODO: create map[columnName]columnTypename
	return nil
}

func (p *Partition) StoreByMask(data map[string]data_types.IColumn, mask []byte) promise.Promise[int32] {
	p.m.Lock()
	defer p.m.Unlock()
	err := p.unordered.AppendByMask(data, mask)
	if err != nil {
		return promise.Fulfilled(err, int32(0))
	}
	res := promise.New[int32]()
	p.promises = append(p.promises, res)
	p.lastStore = time.Now()
	return res
}

func (p *Partition) Store(data map[string]data_types.IColumn) promise.Promise[int32] {
	p.m.Lock()
	defer p.m.Unlock()
	var err error
	err = p.unordered.AppendData(data)
	if err != nil {
		return promise.Fulfilled(err, int32(0))
	}
	res := promise.New[int32]()
	p.promises = append(p.promises, res)
	p.lastStore = time.Now()
	return res
}

func (p *Partition) Size() int64 {
	return p.unordered.GetSize()
}

func (p *Partition) Save() {
	p.m.Lock()
	promises := p.promises
	p.promises = nil
	unordered := p.unordered
	p.unordered = newUnorderedDataStore()
	p.lastSave = time.Now()
	p.m.Unlock()

	onErr := func(err error) {
		for _, p := range promises {
			p.Done(0, err)
		}
	}

	if len(promises) == 0 {
		return
	}
	//TODO: remove the logic of dynamic schema
	fName, err := p.saveService.Save(mergeColumns(unordered), unordered)
	if err != nil {
		onErr(err)
		return
	}

	_min := make(map[string]any)
	_max := make(map[string]any)

	if col, ok := unordered.store[p.table.OrderBy[0]]; ok {
		_min[p.table.OrderBy[0]], _max[p.table.OrderBy[0]] = col.GetMinMax()
	}

	if p.index != nil {
		absDataPath, err := filepath.Abs(fName)
		if err != nil {
			onErr(err)
			return
		}
		stat, err := os.Stat(absDataPath)
		if err != nil {
			onErr(err)
			return
		}

		size := unordered.GetSize()

		prom := p.index.Batch([]*model.IndexEntry{{
			Path:      absDataPath,
			SizeBytes: stat.Size(),
			RowCount:  size,
			ChunkTime: time.Now().UnixNano(),
			Min:       _min,
			Max:       _max,
		}}, nil)
		_, err = prom.Get()
		if err != nil {
			onErr(err)
			return
		}
	}
	onErr(nil)
}

func (p *Partition) PlanMerge() ([]PlanMerge, error) {
	var res []PlanMerge

	configurations := getMergeConfigurations()
	for _, conf := range configurations {
		if time.Now().Sub(p.lastIterationTime[conf[2]-1]).Seconds() > float64(conf[0]) {
			files, err := p.mergeService.GetFilesToMerge(int(conf[2]))
			if err != nil {
				return nil, err
			}
			plans := p.mergeService.PlanMerge(files, conf[1], int(conf[2]))
			res = append(res, plans...)
			p.lastIterationTime[conf[2]-1] = time.Now()
		}
	}
	return res, nil
}

func (p *Partition) DoMerge(plan []PlanMerge) error {
	return p.mergeService.DoMerge(plan)
}
