package service

import (
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/metrico/quackpipe/merge/data_types"
	"github.com/metrico/quackpipe/model"
	"sync"
)

type dataStore interface {
	VerifyData(data map[string]*model.ColumnStore) error
	AppendData(data map[string]*model.ColumnStore) error
	GetSize() int32
	GetSchema() ([]string, []data_types.DataType)
	StoreToArrow(schema *arrow.Schema, builder *array.RecordBuilder) error
}
type unorderedDataStore struct {
	store map[string]*model.ColumnStore
	size  int32
	mtx   sync.Mutex
}

func newUnorderedDataStore() *unorderedDataStore {
	return &unorderedDataStore{
		store: make(map[string]*model.ColumnStore),
		size:  0,
	}
}

func (uds *unorderedDataStore) VerifyData(data map[string]*model.ColumnStore) error {
	uds.mtx.Lock()
	defer uds.mtx.Unlock()
	for k, field := range data {
		dataCol, ok := data[k]
		if !ok {
			continue
		}
		if dataCol.Tp.GetName() != field.Tp.GetName() {
			return fmt.Errorf("column `%s` type mismatch: expected %s, got %s",
				k, field.Tp.GetName(), dataCol.Tp.GetName())
		}
	}
	return nil
}

func (uds *unorderedDataStore) MergeColumns(data map[string]*model.ColumnStore) []string {
	c := map[string]bool{}
	for k := range data {
		c[k] = true
	}
	for k := range uds.store {
		c[k] = true
	}
	var res []string
	for k := range c {
		res = append(res, k)
	}
	return res
}

func (uds *unorderedDataStore) AppendData(data map[string]*model.ColumnStore) error {
	uds.mtx.Lock()
	defer uds.mtx.Unlock()
	var sz int64
	for _, c := range data {
		sz = c.Tp.GetLength(c.Data)
		break
	}
	cols := uds.MergeColumns(data)
	for _, k := range cols {
		_, ok := uds.store[k]
		if !ok {
			uds.store[k] = model.NewColumnStore(data[k].Tp, int(uds.getSize()))
		}
		_, ok = data[k]
		if !ok {
			AppendNullsColumnStore(uds.store[k], int(sz))
			continue
		}
		if err := AppendColumnStore(uds.store[k], data[k].Data); err != nil {
			return err
		}
	}
	uds.size += int32(sz)
	return nil
}

func (uds *unorderedDataStore) getSize() int32 {
	return uds.size
}

func (uds *unorderedDataStore) GetSize() int32 {
	uds.mtx.Lock()
	defer uds.mtx.Unlock()
	return uds.getSize()
}

func (uds *unorderedDataStore) GetSchema() ([]string, []data_types.DataType) {
	var columns []string
	var types []data_types.DataType
	for k := range uds.store {
		columns = append(columns, k)
		types = append(types, uds.store[k].Tp)
	}
	return columns, types
}

func (uds *unorderedDataStore) storeToArrow(schema *arrow.Schema, builder *array.RecordBuilder,
	index /**btree.BTreeG[int32]*/ []int32) error {
	for i, field := range schema.Fields() {
		dataField, ok := uds.store[field.Name]
		arrowField := builder.Field(i)
		if !ok {
			arrowField.AppendNulls(int(uds.GetSize()))
			continue
		}
		err := dataField.Tp.WriteToBatch(arrowField, dataField.Data, index, dataField.Valids)
		if err != nil {
			return err
		}
	}
	return nil
}

func (uds *unorderedDataStore) StoreToArrow(schema *arrow.Schema, builder *array.RecordBuilder) error {
	return uds.storeToArrow(schema, builder, nil)
}

type orderedDataStore struct {
	*unorderedDataStore
	//dataIndexes  *btree.BTreeG[int32]
	dataIndexes  []int32
	lessDType    data_types.DataType
	ordeByCol    string
	orderByStore *model.ColumnStore
}

func (o *orderedDataStore) Less(i, j int32) int {
	b := o.lessDType.Less(o.orderByStore.Data, i, j)
	var k int
	if b {
		k = -1
	} else {
		k = 1
	}
	return k
}

func newOrderedDataStore(orderByCol string) *orderedDataStore {
	res := &orderedDataStore{
		unorderedDataStore: newUnorderedDataStore(),
		lessDType:          nil,
		ordeByCol:          orderByCol,
	}
	//res.dataIndexes = btree.NewBTreeG(res.Less)
	return res
}

func (o *orderedDataStore) VerifyData(data map[string]*model.ColumnStore) error {
	return o.unorderedDataStore.VerifyData(data)
}

func (o *orderedDataStore) AppendData(data map[string]*model.ColumnStore) error {
	if o.lessDType == nil {
		o.lessDType = data[o.ordeByCol].Tp
	}

	var dataSize int64
	for _, c := range data {
		dataSize = c.Tp.GetLength(c.Data)
		break
	}

	storeSize := o.unorderedDataStore.getSize()
	var err error
	if o.getSize() == 0 {
		err = o.unorderedDataStore.AppendData(data)
	} else {
		m := DataStoreMerger{}
		fields := o.MergeColumns(data)
		m.names = fields
		fieldTypes := make(map[string]data_types.DataType, len(fields))
		for i, k := range fields {
			var (
				data1  any
				valid1 []bool
				data2  any
				valid2 []bool
				tp     data_types.DataType
			)

			if _, ok := o.store[k]; ok {
				data1, valid1 = o.store[k].Data, o.store[k].Valids
				tp = o.store[k].Tp
			}
			if _, ok := data[k]; ok {
				data2, valid2 = data[k].Data, data[k].Valids
				tp = data[k].Tp
			}
			fieldTypes[k] = tp
			m.mergers = append(m.mergers, tp.GetMerger(data1, valid1, data2, valid2, int64(storeSize), dataSize))
			if k == o.ordeByCol {
				m.orderByCols = append(m.orderByCols, i)
			}
		}
		m.Merge()
		fields, val, valid := m.Res()
		for i, f := range fields {
			o.size = int32(len(valid[0]))
			o.store[f] = &model.ColumnStore{
				Tp:     fieldTypes[f],
				Data:   val[i],
				Valids: valid[i],
			}
		}
	}
	return err
}

func (o *orderedDataStore) StoreToArrow(schema *arrow.Schema, builder *array.RecordBuilder) error {
	return o.storeToArrow(schema, builder, nil)
}
