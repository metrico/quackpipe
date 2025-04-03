package data_types

import (
	"fmt"
	"sort"
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

type GenericSorter[T string | uint64 | int64 | float64] struct {
	data []T
}

func (g *GenericSorter[T]) Len() int {
	return len(g.data)
}

func (g *GenericSorter[T]) Less(i, j int) bool {
	return g.data[i] < g.data[j]
}

func (g *GenericSorter[T]) Swap(i, j int) {
	g.data[i], g.data[j] = g.data[j], g.data[i]
}

type GenericMerger[T string | uint64 | int64 | float64] struct {
	data1  []T
	valid1 []bool
	data2  []T
	valid2 []bool
	res    []T
	valid  []bool
	s1     int64
	s2     int64
	i      int64
	j      int64
	k      int64
}

func NewGenericMerger[T string | uint64 | int64 | float64](data1, data2 []T, valid1, valid2 []bool,
	s1, s2 int64) IGenericMerger {
	if data1 != nil && data2 != nil {
		return &GenericMerger[T]{
			data1:  data1,
			data2:  data2,
			valid1: valid1,
			valid2: valid2,
			s1:     s1,
			s2:     s2,
		}
	}
	data := data1
	valid := valid1
	first := true
	if data2 != nil {
		data = data2
		valid = valid2
		first = false
	}
	return &OneMerger[T]{
		data1:  data,
		valid1: valid,
		first:  first,
		s1:     s1,
		s2:     s2,
	}
}

func (m *GenericMerger[T]) Arranged() (bool, bool) {
	if m.data1[m.s1-1] < m.data2[0] {
		return true, true
	}
	if m.data2[m.s2-1] < m.data1[0] {
		return true, false
	}
	return false, false
}

func (m *GenericMerger[T]) Arrange(first bool) {
	if first {
		m.data1 = append(m.data1, m.data2...)
		m.valid1 = append(m.valid1, m.valid2...)
		m.res = m.data1
		m.valid = m.valid1
		return
	}

	m.data2 = append(m.data2, m.data1...)
	m.valid2 = append(m.valid2, m.valid1...)
	m.res = m.data2
	m.valid = m.valid2
}

func (m *GenericMerger[T]) Head() (int64, bool) {
	i := sort.Search(int(m.s1), func(i int) bool {
		return m.data2[0] < m.data1[i]
	})
	if i != 0 {
		return int64(i), true
	}
	i = sort.Search(int(m.s2), func(i int) bool {
		return m.data1[0] < m.data2[i]
	})
	return int64(i), true
}

func (m *GenericMerger[T]) End() bool {
	return m.i >= m.s1 || m.j >= m.s2
}

func (m *GenericMerger[T]) Less() bool {
	return m.data1[m.i] <= m.data2[m.j]
}

func (m *GenericMerger[T]) maybeInit() {
	if m.res == nil {
		m.res = make([]T, m.s1+m.s2)
		m.valid = make([]bool, m.s1+m.s2)
	}
}

func (m *GenericMerger[T]) PickArr(first bool, count int64) {
	m.maybeInit()
	if first {
		copy(m.res[m.k:], m.data1[m.i:m.i+count])
		copy(m.valid[m.k:], m.valid1[m.i:m.i+count])
		m.i += count
	} else {
		copy(m.res[m.k:], m.data2[m.j:m.j+count])
		copy(m.valid[m.k:], m.valid2[m.j:m.j+count])
		m.j += count
	}
	m.k += count
}

func (m *GenericMerger[T]) Pick(first bool) {
	if first {
		m.res[m.k] = m.data1[m.i]
		m.valid[m.k] = m.valid1[m.i]
		m.i++
	} else {
		m.res[m.k] = m.data2[m.j]
		m.valid[m.k] = m.valid2[m.j]
		m.j++
	}
	m.k++
}

func (m *GenericMerger[T]) Finish() {
	if m.i < m.s1 {
		copy(m.res[m.k:], m.data1[m.i:])
		copy(m.valid[m.k:], m.valid1[m.i:])
		m.k += m.s1 - m.i
		return
	}
	if m.j < m.s2 {
		copy(m.res[m.k:], m.data2[m.j:])
		copy(m.valid[m.k:], m.valid2[m.j:])
		m.k += m.s2 - m.j
	}
}

func (m *GenericMerger[T]) Res() (any, []bool) {
	return m.res, m.valid
}

type OneMerger[T string | uint64 | int64 | float64] struct {
	data1  []T
	valid1 []bool
	res    []T
	valid  []bool
	first  bool
	s1     int64
	s2     int64
	i      int64
	j      int64
	k      int64
}

func (m *OneMerger[T]) Arranged() (bool, bool) {
	return true, !m.first
}

func (m *OneMerger[T]) Arrange(first bool) {
	if m.first == first {
		m.data1 = append(m.data1, make([]T, m.s2)...)
		m.valid1 = append(m.valid1, make([]bool, m.s2)...)
		m.res = m.data1
		m.valid = m.valid1
		return
	}
	m.data1 = append(make([]T, m.s2), m.data1...)
	m.valid1 = append(make([]bool, m.s2), m.valid1...)
	m.res = m.data1
	m.valid = m.valid1
}

func (m *OneMerger[T]) Head() (int64, bool) {
	return 0, false
}

func (m *OneMerger[T]) maybeInit() {
	if m.res == nil {
		m.res = make([]T, m.s1+m.s2)
		m.valid = make([]bool, m.s1+m.s2)
	}
}

func (m *OneMerger[T]) PickArr(first bool, count int64) {
	m.maybeInit()
	if m.first == first {
		copy(m.res[m.k:], m.data1[m.i:m.i+count])
		copy(m.valid[m.k:], m.valid1[m.i:m.i+count])
		m.i += count
	} else {
		m.j += count
	}
	m.k += count
}

func (o *OneMerger[T]) End() bool {
	return o.i >= o.s1 || o.j >= o.s2
}

func (o *OneMerger[T]) Less() bool {
	return !o.first
}

func (o *OneMerger[T]) Pick(first bool) {
	if o.first == first {
		o.res[o.k] = o.data1[o.i]
		o.valid[o.k] = o.valid1[o.i]
		o.i++
	} else {
		var a T
		o.res[o.k] = a
		o.valid[o.k] = false
		o.j++
	}
	o.k++
}

func (o *OneMerger[T]) Finish() {
	if o.i < o.s1 {
		copy(o.res[o.k:], o.data1[o.i:])
		copy(o.valid[o.k:], o.valid1[o.i:])
		o.k += o.s1 - o.i
		return
	}
	o.res = o.res[:o.k]
	o.res = append(o.res, make([]T, o.s2-o.j)...)
	o.k += o.s2 - o.j
}

func (o *OneMerger[T]) Res() (any, []bool) {
	return o.res, o.valid
}
