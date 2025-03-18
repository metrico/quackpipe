package data_types

import (
	"fmt"
	"github.com/tidwall/btree"
)

type generic[T any] struct{}

func (i generic[T]) MakeStore() any {
	return make([]T, 0, 1000)
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
	data any, index *btree.BTreeG[int32]) error {
	if index == nil {
		appendArray(data.([]T), nil)
		return nil
	}
	_data := data.([]T)
	it := index.Iter()
	for it.Next() {
		append(_data[it.Item()])
	}
	return nil
}
