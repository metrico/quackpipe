package data_types

import (
	"fmt"
	"unsafe"
)

type generic[T any] struct{}

func (i generic[T]) MakeStore(sizeAndCap ...int) any {
	cp := 1000
	sz := 0
	if len(sizeAndCap) > 0 {
		sz = sizeAndCap[0]
	}
	if cp < sz*2 {
		cp = sz * 2
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
	appendNull func(), data any, valid []bool, index /**btree.BTreeG[int32]*/ IndexType) error {
	if index == nil {
		appendArray(data.([]T), valid)
		return nil
	}
	_data := data.([]T)
	/*it := index.Iter()
	for it.Next() {
		if !valid[it.Item()] {
			appendNull()
			continue
		}
		append(_data[it.Item()])
	}*/
	for _, j := range index {
		if !valid[j] {
			appendNull()
			continue
		}
		append(_data[j])
	}
	return nil
}

func (i generic[T]) GetVal(j int64, store any) any {
	return store.([]T)[j]
}

func (i generic[T]) GetValI32(j int32, store any) any {
	return store.([]T)[j]
}

func (i generic[T]) AppendOne(val any, data any) any {
	_data := data.([]T)
	_data = append(_data, val.(T))
	return _data
}

func (i generic[T]) AppendByMask(data any, toAppend any, mask []byte) (any, error) {
	_data := data.([]T)
	_toAppend := toAppend.([]T)
	if len(mask) != (len(_toAppend)+7)/8 {
		return nil, fmt.Errorf("invalid mask length")
	}

	u64Mask := unsafe.Slice((*uint64)(unsafe.Pointer(&mask[0])), len(mask)/8)

	k := len(_data)
	_data = append(_data, make([]T, len(_toAppend))...)
	_data = _data[:k]

	for i, u := range u64Mask {
		if u == 0xFFFFFFFFFFFFFFFF {
			_data = append(_data, _toAppend[i*64:i*64+64]...)
			continue
		}
		for j := 0; j < 64; j++ {
			if u&(1<<j) != 0 && i*64+j < len(_toAppend) {
				_data = append(_data, _toAppend[i*64+j])
			}
		}
	}

	for i := range mask[len(u64Mask)*8:] {
		k := i + len(u64Mask)*8
		if mask[i] == 0xFF {
			_data = append(_data, _toAppend[k*8:k*8+8]...)
			continue
		}
		for j := 0; j < 8; j++ {
			if mask[i+len(u64Mask)*8]&(1<<j) != 0 {
				_data = append(_data, _toAppend[k*8+j])
			}
		}
	}
	return _data, nil
}
