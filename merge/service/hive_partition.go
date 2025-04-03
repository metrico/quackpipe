package service

import (
	"github.com/metrico/quackpipe/model"
	"github.com/metrico/quackpipe/utils/promise"
	"os"
	"sync"
	"time"
)

type Partition struct {
	Values            [][2]string
	unordered         *unorderedDataStore
	ordered           *orderedDataStore
	saveService       saveService
	mergeService      mergeService
	promises          []promise.Promise[int32]
	m                 sync.Mutex
	table             *model.Table
	lastStore         time.Time
	lastSave          time.Time
	lastIterationTime [3]time.Time
}

func NewPartition(values [][2]string, tmpPath, dataPath string, t *model.Table) (*Partition, error) {
	res := &Partition{
		Values:            values,
		unordered:         newUnorderedDataStore(),
		ordered:           newOrderedDataStore(t.OrderBy[0]),
		table:             t,
		lastIterationTime: [3]time.Time{time.Now(), time.Now(), time.Now()},
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
	}
	return nil
}

func (p *Partition) Store(columns map[string]*model.ColumnStore) promise.Promise[int32] {
	p.m.Lock()
	defer p.m.Unlock()
	var err error
	if _, ok := columns[p.table.OrderBy[0]]; ok {
		err = p.ordered.AppendData(columns)
	} else {
		err = p.unordered.AppendData(columns)
	}
	if err != nil {
		return promise.Fulfilled(err, int32(0))
	}
	res := promise.New[int32]()
	p.promises = append(p.promises, res)
	p.lastStore = time.Now()
	return res
}

func (p *Partition) Save() {
	p.m.Lock()
	promises := p.promises
	p.promises = nil
	unordered := p.unordered
	p.unordered = newUnorderedDataStore()
	ordered := p.ordered
	p.ordered = newOrderedDataStore(p.table.OrderBy[0])
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

	onErr(p.saveService.Save(mergeColumns(unordered, ordered), unordered, ordered))
}

func (p *Partition) PlanMerge() ([]PlanMerge, error) {
	var res []PlanMerge

	configurations := [][3]int64{
		{10, 4000 * 1024 * 1024, 1},
		{100, 4000 * 1024 * 1024, 2},
		{1000, 4000 * 1024 * 1024, 3},
	}
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
