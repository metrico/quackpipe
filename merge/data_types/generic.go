package data_types

import (
	"fmt"
	"github.com/tidwall/btree"
)

type generic[T any] struct{}

func (i generic[T]) MakeStore(sizeAndCap ...int) any {
	cp := 1000
	sz := 0
	if len(sizeAndCap) > 0 {
		sz = sizeAndCap[0]
	}
	if len(sizeAndCap) > 1 {
		cp = sizeAndCap[1]
	}
	return make([]T, sz, cp)
}

func (i generic[T]) AppendDefault(size int, store any) any {
	defaults := make([]T, size)
	store = append(store.([]T), defaults...)
	return store
}

func (i generic[T]) GetLength(store any) int64 {
	return int64(len(store.([]T)))
}

func (i generic[T]) ParseJson(dec func() (T, error), store []T) ([]T, error) {
	_i, err := dec()
	if err != nil {
		return store, err
	}
	store = append(store, _i)
	return store, nil
}

func (i generic[T]) ValidateData(data any) error {
	if _, ok := data.([]T); !ok {
		return fmt.Errorf("invalid data type")
	}
	return nil
}

func (i generic[T]) AppendStore(store any, data any) (any, error) {
	_data := data.([]T)
	_store := store.([]T)
	_store = append(_store, _data...)
	return _store, nil
}

func (i generic[T]) WriteToBatch(appendArray func([]T, []bool), append func(T),
	appendNull func(), data any, valid []bool, index *btree.BTreeG[int32]) error {
	if index == nil {
		appendArray(data.([]T), valid)
		return nil
	}
	_data := data.([]T)
	it := index.Iter()
	for it.Next() {
		if !valid[it.Item()] {
			appendNull()
			continue
		}
		append(_data[it.Item()])
	}
	return nil
}
