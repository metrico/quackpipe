package service

import (
	"github.com/metrico/quackpipe/merge/data_types"
	"sort"
)

type DataStoreSorter struct {
	orderByCols []int
	sorters     []sort.Interface
}

func (d *DataStoreSorter) Len() int {
	return d.sorters[0].Len()
}

func (d *DataStoreSorter) Less(i, j int) bool {
	for _, k := range d.orderByCols {
		if !d.sorters[k].Less(i, j) {
			return false
		}
	}
	return true
}

func (d *DataStoreSorter) Swap(i, j int) {
	for _, k := range d.sorters {
		k.Swap(i, j)
	}
}

type DataStoreMerger struct {
	names       []string
	mergers     []data_types.IGenericMerger
	orderByCols []int
}

func (d *DataStoreMerger) End() bool {
	return d.mergers[0].End()
}

func (d *DataStoreMerger) Less() bool {
	for _, i := range d.orderByCols {
		if !d.mergers[i].Less() {
			return false
		}
	}
	return true
}

func (d *DataStoreMerger) Merge() {
	arranged, first := d.mergers[d.orderByCols[0]].Arranged()
	if arranged {
		for _, m := range d.mergers {
			m.Arrange(first)
		}
		return
	}
	size, first := d.mergers[d.orderByCols[0]].Head()
	for _, m := range d.mergers {
		m.PickArr(first, size)
	}
	for !d.End() {
		l := d.Less()
		for _, m := range d.mergers {
			m.Pick(l)
		}
	}
	for _, m := range d.mergers {
		m.Finish()
	}
}

func (d *DataStoreMerger) Res() ([]string, []any, [][]bool) {
	var val []any
	var valid [][]bool
	for _, m := range d.mergers {
		_val, _valid := m.Res()
		val = append(val, _val)
		valid = append(valid, _valid)
	}
	return d.names, val, valid
}
