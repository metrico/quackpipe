package index

import (
	"context"
	"encoding/json"
	jsoniter "github.com/json-iterator/go"
	"github.com/metrico/quackpipe/model"
	"github.com/metrico/quackpipe/utils/promise"
	"os"
	"path"
	"sync"
	"sync/atomic"
)

type jsonIndexEntry struct {
	Id          uint32 `json:"id"`
	Path        string `json:"path"`
	SizeBytes   int64  `json:"size_bytes"`
	RowCount    int64  `json:"row_count"`
	ChunkTime   int64  `json:"chunk_time"`
	MinTime     int64  `json:"min_time"`
	MaxTime     int64  `json:"max_time"`
	Range       string `json:"range"`
	Type        string `json:"type"`
	_marshalled string `json:"-"`
}

type JSONIndex struct {
	t *model.Table

	entries   map[string]*jsonIndexEntry
	promises  []promise.Promise[int32]
	m         sync.Mutex
	updateCtx context.Context
	doUpdate  context.CancelFunc
	workCtx   context.Context
	stop      context.CancelFunc
	lastId    uint32

	parquetSizeBytes int64
	rowCount         int64
	minTime          int64
	maxTime          int64
}

func NewJSONIndex(t *model.Table) (model.Index, error) {
	res := &JSONIndex{
		t:       t,
		entries: make(map[string]*jsonIndexEntry),
	}
	err := res.populate()
	res.updateCtx, res.doUpdate = context.WithCancel(context.Background())
	res.workCtx, res.stop = context.WithCancel(context.Background())
	return res, err
}

func (J *JSONIndex) populate() error {
	if _, err := os.Stat(path.Join(J.t.Path, "metadata.json")); os.IsNotExist(err) {
		return nil
	}

	f, err := os.Open(path.Join(J.t.Path, "metadata.json"))
	if err != nil {
		return err
	}
	defer f.Close()

	iter := jsoniter.Parse(jsoniter.ConfigDefault, f, 4096)
	iter.ReadMapCB(func(iterator *jsoniter.Iterator, s string) bool {
		switch s {
		case "type":
			iterator.Skip()
		case "parquet_size_bytes":
			J.parquetSizeBytes = iterator.ReadInt64()
		case "row_count":
			J.rowCount = iterator.ReadInt64()
		case "min_time":
			J.minTime = iterator.ReadInt64()
		case "max_time":
			J.maxTime = iterator.ReadInt64()
		case "wal_sequence":
			iterator.Skip()
		case "files":
			err = J.populateFiles(iterator)
			if err != nil {
				return false
			}
		}
		return true
	})
	if err != nil {
		return err
	}
	if iter.Error != nil {
		return iter.Error
	}
	return nil
}

func (J *JSONIndex) populateFiles(iter *jsoniter.Iterator) error {
	for iter.ReadArray() {
		e := &jsonIndexEntry{}
		iter.ReadVal(e)
		_marshalled, err := json.Marshal(e)
		if err != nil {
			return err
		}
		e._marshalled = string(_marshalled)
		if e.Id > J.lastId {
			J.lastId = e.Id
		}
		J.entries[e.Path] = e
	}
	return nil
}

func (J *JSONIndex) Batch(add []*model.IndexEntry, rm []string) promise.Promise[int32] {
	_add, err := J.entry2JEntry(add)
	if err != nil {
		return promise.Fulfilled[int32](err, 0)
	}
	J.m.Lock()
	defer J.m.Unlock()
	J.add(_add)
	removed := J.rm(rm)
	if len(_add) == 0 && !removed {
		return promise.Fulfilled(nil, int32(0))
	}
	p := promise.New[int32]()
	J.promises = append(J.promises, p)
	J.doUpdate()
	return p
}

func (J *JSONIndex) entry2JEntry(entries []*model.IndexEntry) ([]*jsonIndexEntry, error) {
	res := make([]*jsonIndexEntry, len(entries))
	for i, entry := range entries {
		id := atomic.AddUint32(&J.lastId, 1)
		var (
			minTime, maxTime int64
		)
		if _, ok := entry.Min["__timestamp"]; ok {
			minTime = entry.Min["__timestamp"].(int64)
		}
		if _, ok := entry.Max["__timestamp"]; ok {
			maxTime = entry.Max["__timestamp"].(int64)
		}
		_entry := &jsonIndexEntry{
			Id:        id,
			Path:      entry.Path,
			SizeBytes: entry.SizeBytes,
			RowCount:  entry.RowCount,
			ChunkTime: entry.ChunkTime,
			MinTime:   minTime,
			MaxTime:   maxTime,
			Range:     "1h",
			Type:      "compacted",
		}
		_marshalled, err := json.Marshal(_entry)
		if err != nil {
			return nil, err
		}
		_entry._marshalled = string(_marshalled)
		res[i] = _entry
	}
	return res, nil
}

func (J *JSONIndex) add(entries []*jsonIndexEntry) {
	for _, entry := range entries {
		J.rowCount += entry.RowCount
		J.parquetSizeBytes += entry.SizeBytes
		J.entries[entry.Path] = entry
		if entry.Id == 1 {
			J.minTime = entry.MinTime
			J.maxTime = entry.MaxTime
			continue
		}
		if entry.MinTime != 0 {
			J.minTime = min(J.minTime, entry.MinTime)
		}
		if entry.MinTime != 0 {
			J.maxTime = max(J.maxTime, entry.MaxTime)
		}
	}
}

func (J *JSONIndex) recalcMin() {
	if J.entries == nil {
		J.minTime = 0
		return
	}
	var i int
	for _, entry := range J.entries {
		if i == 0 {
			J.minTime = entry.MinTime
			i++
			continue
		}
		J.minTime = min(J.minTime, entry.MinTime)
	}
}

func (J *JSONIndex) recalcMax() {
	if J.entries == nil {
		J.maxTime = 0
		return
	}
	var i int
	for _, entry := range J.entries {
		if i == 0 {
			J.maxTime = entry.MaxTime
			i++
			continue
		}
		J.maxTime = max(J.maxTime, entry.MaxTime)
	}
}

func (J *JSONIndex) rm(path []string) bool {
	rm := false
	for _, entry := range path {
		_e, ok := J.entries[entry]
		if !ok {
			continue
		}
		rm = true
		J.rowCount -= _e.RowCount
		J.parquetSizeBytes -= _e.SizeBytes
		delete(J.entries, entry)
		if _e.MinTime == J.minTime {
			J.recalcMin()
		}
		if _e.MaxTime == J.maxTime {
			J.recalcMax()
		}
	}
	return rm
}

func (J *JSONIndex) flush() {
	J.m.Lock()
	J.updateCtx, J.doUpdate = context.WithCancel(context.Background())
	entries := make([]string, 0, len(J.entries))
	parquetSizeBytes := J.parquetSizeBytes
	promises := J.promises
	J.promises = nil
	rowCount := J.rowCount
	minTime := J.minTime
	maxTime := J.maxTime
	for _, entry := range J.entries {
		entries = append(entries, entry._marshalled)
	}
	J.m.Unlock()

	onErr := func(err error) {
		for _, p := range promises {
			p.Done(0, err)
		}
	}

	f, err := os.Create(path.Join(J.t.Path, "metadata.json.bak"))
	if err != nil {
		onErr(err)
		return
	}
	defer f.Close()

	stream := jsoniter.NewStream(jsoniter.ConfigDefault, f, 4096)

	// Start encoding the JSON structure
	stream.WriteObjectStart()

	stream.WriteObjectField("type")
	stream.WriteString(J.t.Name)

	stream.WriteMore()
	stream.WriteObjectField("parquet_size_bytes")
	stream.WriteInt64(parquetSizeBytes)

	stream.WriteMore()
	stream.WriteObjectField("row_count")
	stream.WriteInt64(rowCount)

	stream.WriteMore()
	stream.WriteObjectField("min_time")
	stream.WriteInt64(minTime)

	stream.WriteMore()
	stream.WriteObjectField("max_time")
	stream.WriteInt64(maxTime)

	stream.WriteMore()
	stream.WriteObjectField("wal_sequence")
	stream.WriteInt64(0)

	stream.WriteMore()
	stream.WriteObjectField("files")
	stream.WriteArrayStart()

	// Write the entries
	for i, entry := range entries {
		if i > 0 {
			stream.WriteMore()
		}
		stream.WriteRaw(entry)
	}

	// Close the array and object
	stream.WriteArrayEnd()
	stream.WriteObjectEnd()

	if stream.Error != nil {
		onErr(stream.Error)
		return
	}

	err = stream.Flush()
	if err != nil {
		onErr(err)
		return
	}

	// Rename the backup file to the actual metadata file
	err = os.Rename(path.Join(J.t.Path, "metadata.json.bak"), path.Join(J.t.Path, "metadata.json"))
	if err != nil {
		onErr(err)
		return
	}

	onErr(nil)
}

func (J *JSONIndex) Run() {
	go func() {
		for {
			select {
			case <-J.updateCtx.Done():

				J.flush()
			case <-J.workCtx.Done():
				return
			}
		}
	}()
}

func (J *JSONIndex) Stop() {
	J.stop()
}

func (J *JSONIndex) Get(path string) *model.IndexEntry {
	_e := J.entries[path]
	if _e == nil {
		return nil
	}

	return &model.IndexEntry{
		Path:      _e.Path,
		SizeBytes: _e.SizeBytes,
		RowCount:  _e.RowCount,
		ChunkTime: _e.ChunkTime,
		Min:       map[string]any{"__timestamp": _e.MinTime},
		Max:       map[string]any{"__timestamp": _e.MaxTime},
	}
}
