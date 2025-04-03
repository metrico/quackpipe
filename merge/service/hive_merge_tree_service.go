package service

import (
	"encoding/binary"
	"fmt"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/vm"
	"github.com/go-faster/city"
	"github.com/metrico/quackpipe/merge/data_types"
	"github.com/metrico/quackpipe/model"
	"github.com/metrico/quackpipe/utils/promise"
	"golang.org/x/sync/errgroup"
	"math"
	"path"
	"reflect"
	"sync"
	"time"
	"unsafe"
)

func equals(a, b any) bool {
	if a == nil || b == nil {
		return a == b
	}

	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	if va.Type() != vb.Type() {
		return false
	}

	switch va.Kind() {
	case reflect.Bool:
		return va.Bool() == vb.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return va.Int() == vb.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return va.Uint() == vb.Uint()
	case reflect.Float32, reflect.Float64:
		return va.Float() == vb.Float()
	case reflect.Complex64, reflect.Complex128:
		return va.Complex() == vb.Complex()
	case reflect.String:
		return va.String() == vb.String()
	case reflect.Ptr, reflect.Interface:
		return equals(va.Elem().Interface(), vb.Elem().Interface())
	}

	// Handle time.Time comparison
	if ta, ok := a.(time.Time); ok {
		if tb, ok := b.(time.Time); ok {
			return ta.Equal(tb)
		}
	}

	return reflect.DeepEqual(a, b)
}

func hash(v any) uint64 {
	if v == nil {
		return 0
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Bool:
		if rv.Bool() {
			return city.Hash64([]byte{1})
		}
		return city.Hash64([]byte{0})
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return city.Hash64(int64ToBytes(rv.Int()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return city.Hash64(uint64ToBytes(rv.Uint()))
	case reflect.Float32:
		return city.Hash64(float32ToBytes(float32(rv.Float())))
	case reflect.Float64:
		return city.Hash64(float64ToBytes(rv.Float()))
	case reflect.Complex64:
		c := rv.Complex()
		return city.Hash64(append(float32ToBytes(float32(real(c))), float32ToBytes(float32(imag(c)))...))
	case reflect.Complex128:
		c := rv.Complex()
		return city.Hash64(append(float64ToBytes(real(c)), float64ToBytes(imag(c))...))
	case reflect.String:
		return city.Hash64([]byte(rv.String()))
	case reflect.Ptr, reflect.Interface:
		if rv.IsNil() {
			return 0
		}
		return hash(rv.Elem().Interface())
	}

	// Handle time.Time
	if t, ok := v.(time.Time); ok {
		return city.Hash64(int64ToBytes(t.UnixNano()))
	}

	// For unsupported types, use reflection to get a string representation
	return city.Hash64([]byte(fmt.Sprintf("%v", v)))
}

func int64ToBytes(i int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(i))
	return b
}

func uint64ToBytes(i uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, i)
	return b
}

func float32ToBytes(f float32) []byte {
	return uint32ToBytes(math.Float32bits(f))
}

func float64ToBytes(f float64) []byte {
	return uint64ToBytes(math.Float64bits(f))
}

func uint32ToBytes(i uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, i)
	return b
}

type HiveMergeTreeService struct {
	*MergeTreeService

	partitions map[uint64]*Partition

	storeTicker *time.Ticker
	mergeTicker *time.Ticker
}

func NewHiveMergeTreeService(t *model.Table) (*HiveMergeTreeService, error) {
	res := &HiveMergeTreeService{
		MergeTreeService: &MergeTreeService{
			Table: t,
		},
		partitions: make(map[uint64]*Partition),
	}
	//err := res.parsePartitionInfo()
	return res, nil
}

/*func (h *HiveMergeTreeService) parsePartitionInfo() error {
	h.partitionExressions = make([]*vm.Program, len(h.Table.PartitionBy))
	idents := make(map[string]bool)

	for i, partition := range h.Table.PartitionBy {
		prog, identifiers, err := h.parsePartitionExpression(partition)
		if err != nil {
			return err
		}
		h.partitionExressions[i] = prog
		for _, id := range identifiers {
			idents[id] = true
		}
	}

	h.requiredColumns = make([]string, 0, len(idents))
	for id := range idents {
		h.requiredColumns = append(h.requiredColumns, id)
	}
	return nil
}*/

type ExprParserHelper struct {
	Identifiers []string
}

func (e *ExprParserHelper) Visit(node *ast.Node) {
	n, ok := (*node).(*ast.IdentifierNode)
	if !ok {
		return
	}
	ast.Patch(node, &ast.CallNode{
		Callee:    &ast.IdentifierNode{Value: "getValue"},
		Arguments: []ast.Node{&ast.StringNode{Value: n.String()}},
	})
	e.Identifiers = append(e.Identifiers, n.Value)
}

func (h *HiveMergeTreeService) parsePartitionExpression(expression [2]string) (*vm.Program, []string, error) {
	helper := ExprParserHelper{}
	prog, err := expr.Compile(expression[1], expr.Patch(&helper))
	if err != nil {
		return nil, nil, err
	}
	return prog, helper.Identifiers, nil
}

func (h *HiveMergeTreeService) Run() {
	h.storeTicker = time.NewTicker(time.Second)
	go func() {
		for range h.storeTicker.C {
			h.flush()
		}
	}()
	/*h.mergeTicker = time.NewTicker(time.Minute)
	go func() {
		for range h.mergeTicker.C {
			plan, err := h.PlanMerge()
			if err != nil {
				log.Printf("Error planning merge: %v", err)
				continue
			}
			err = h.Merge(plan)
			if err != nil {
				log.Printf("Error merging: %v", err)
				continue
			}
		}
	}()*/
}

func (h *HiveMergeTreeService) flush() {
	wg := sync.WaitGroup{}
	for _, part := range h.partitions {
		wg.Add(1)
		go func(part *Partition) {
			defer wg.Done()
			part.Save()
		}(part)
	}
	wg.Wait()
}

func (h *HiveMergeTreeService) Stop() {
	h.storeTicker.Stop()
}

func (h *HiveMergeTreeService) calculateSchema() map[string]data_types.DataType {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	schema := make(map[string]data_types.DataType)
	for _, part := range h.partitions {
		names, types := part.ordered.GetSchema()
		for i, n := range names {
			schema[n] = types[i]
		}
		names, types = part.unordered.GetSchema()
		for i, n := range names {
			schema[n] = types[i]
		}
	}
	return schema
}

func (h *HiveMergeTreeService) validateData(columns map[string]*model.ColumnStore) error {
	err := h.validateColSizes(columns)
	if err != nil {
		return err
	}

	schema := h.calculateSchema()
	for name, col := range columns {
		if _, ok := schema[name]; !ok {
			continue
		}
		if col.Tp.GetName() != schema[name].GetName() {
			return fmt.Errorf("column %s has different data type", name)
		}
	}
	return nil
}

func (h *HiveMergeTreeService) calculatePartitionHash(values [][2]string) uint64 {
	valuesHashes := make([]uint64, len(values))
	for i, v := range values {
		valuesHashes[i] = hash(v[1])
	}
	return city.CH64(unsafe.Slice((*byte)(unsafe.Pointer(&valuesHashes[0])), len(valuesHashes)*8))
}

type shortPartDescr struct {
	store  map[string]*model.ColumnStore
	values [][2]string
}

func (h *HiveMergeTreeService) getDataPartitions(columns map[string]*model.ColumnStore,
) (map[uint64]*shortPartDescr, error) {
	/*size := int64(0)
	for _, c := range columns {
		size = c.Tp.GetLength(c.Data)
		break
	}*/

	res := make(map[uint64]*shortPartDescr)

	//i := int64(0)
	partsDesc, err := h.Table.PartitionBy(columns)
	if err != nil {
		return nil, err
	}

	for _, part := range partsDesc {
		partitionHash := h.calculatePartitionHash(part.Values)
		p, ok := res[partitionHash]

		if !ok {
			p = &shortPartDescr{store: make(map[string]*model.ColumnStore), values: part.Values}
			for col, val := range columns {
				p.store[col] = model.NewColumnStore(val.Tp, 0)
			}
			res[partitionHash] = p
		}

		pStore := make([]*model.ColumnStore, 0, len(columns))
		colStore := make([]*model.ColumnStore, 0, len(columns))
		for col := range columns {
			pStore = append(pStore, p.store[col])
			colStore = append(colStore, columns[col])
		}

		for j, val := range colStore {
			sizeBefore := pStore[j].Tp.GetLength(pStore[j].Data)
			pStore[j].Data, err = pStore[j].Tp.AppendByMask(pStore[j].Data, val.Data, part.IndexMap)
			if err != nil {
				return nil, err
			}
			sizeAfter := pStore[j].Tp.GetLength(pStore[j].Data)
			pStore[j].Valids = append(pStore[j].Valids, make([]bool, sizeAfter-sizeBefore)...)
			fastFillArray(pStore[j].Valids[sizeBefore:], true)
		}
	}
	/*for i = 0; i < size; i++ {
		values, err := h.Table.PartitionBy(i, columns)
		if err != nil {
			return nil, err
		}
		partitionHash := h.calculatePartitionHash(values)
		p, ok := res[partitionHash]
		if !ok {
			p = &shortPartDescr{store: make(map[string]*model.ColumnStore), values: values}
			for col, val := range columns {
				p.store[col] = model.NewColumnStore(val.Tp, 0)
			}
			res[partitionHash] = p
		}
		for col, val := range columns {
			p.store[col].Data = p.store[col].Tp.AppendOne(val.Tp.GetVal(i, val.Data), p.store[col].Data)
			p.store[col].Valids = append(p.store[col].Valids, true)
		}
	}*/
	return res, nil
}

func (h *HiveMergeTreeService) getDataPath(values [][2]string) string {
	p := []string{h.Table.Path}
	for _, v := range values {
		p = append(p, fmt.Sprintf("%s=%v", v[0], v[1]))
	}
	return path.Join(p...)
}

func (h *HiveMergeTreeService) Store(columns map[string]any) promise.Promise[int32] {
	_columns := h.wrapColumns(columns)

	err := h.validateData(_columns)
	if err != nil {
		return promise.Fulfilled[int32](err, 0)
	}

	_columns = h.AutoTimestamp(_columns)

	partitions, err := h.getDataPartitions(_columns)
	if err != nil {
		return promise.Fulfilled[int32](err, 0)
	}

	var promises []promise.Promise[int32]
	h.mtx.Lock()
	for id, part := range partitions {
		if _, ok := h.partitions[id]; !ok {
			h.partitions[id], err = NewPartition(part.values,
				path.Join(h.Table.Path, "tmp"),
				h.getDataPath(part.values),
				h.Table)
			if err != nil {
				h.mtx.Unlock()
				return promise.Fulfilled[int32](err, 0)
			}
		}
	}
	h.mtx.Unlock()
	for id, part := range partitions {
		promises = append(promises, h.partitions[id].Store(part.store))
	}

	return promise.NewWaitForAll(promises)
}

func (h *HiveMergeTreeService) PlanMerge() (map[uint64][]PlanMerge, error) {
	mergeByPartition := make(map[uint64][]PlanMerge)
	for id, part := range h.partitions {
		plan, err := part.PlanMerge()
		if err != nil {
			return nil, err
		}
		mergeByPartition[id] = append(mergeByPartition[id], plan...)
	}
	return mergeByPartition, nil
}

func (h *HiveMergeTreeService) Merge(plan map[uint64][]PlanMerge) error {
	errGroup := errgroup.Group{}
	fmt.Println("Starting merges...")
	start := time.Now()
	for id, merges := range plan {
		_id := id
		_merges := merges
		errGroup.Go(func() error {
			if part, ok := h.partitions[_id]; ok {
				err := part.DoMerge(_merges)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	err := errGroup.Wait()
	fmt.Printf("Merge time: %v\n", time.Since(start))
	return err
}

func (h *HiveMergeTreeService) DoMerge() error {
	plan, err := h.PlanMerge()
	if err != nil {
		return err
	}
	return h.Merge(plan)
}
