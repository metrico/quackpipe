package service

import (
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/metrico/quackpipe/merge/data_types"
	"github.com/metrico/quackpipe/model"
	"slices"
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

func (uds *unorderedDataStore) AppendData(data map[string]*model.ColumnStore) error {
	uds.mtx.Lock()
	defer uds.mtx.Unlock()
	var sz int64
	for _, c := range data {
		sz = c.Tp.GetLength(c.Data)
		break
	}
	for k, field := range data {
		_, ok := uds.store[k]
		if !ok {
			uds.store[k] = model.NewColumnStore(field.Tp, int(uds.getSize()))
		}
		if err := AppendColumnStore(uds.store[k], field.Data); err != nil {
			return err
		}
	}
	for k, field := range uds.store {
		if _, ok := data[k]; !ok {
			AppendNullsColumnStore(field, int(sz))
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
	storeSize := o.unorderedDataStore.getSize()
	err := o.unorderedDataStore.AppendData(data)
	if err != nil {
		return err
	}
	newStoreSize := o.unorderedDataStore.getSize()

	for i := storeSize; i < newStoreSize; i++ {
		o.dataIndexes = append(o.dataIndexes, i)
		//o.dataIndexes.Set(i)
	}
	return nil
}

func (o *orderedDataStore) StoreToArrow(schema *arrow.Schema, builder *array.RecordBuilder) error {
	o.orderByStore = o.store[o.ordeByCol]
	slices.SortFunc(o.dataIndexes, o.Less)
	return o.storeToArrow(schema, builder, o.dataIndexes)
}
