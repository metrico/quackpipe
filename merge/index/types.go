package index

type IndexEntry struct {
	Path      string
	SizeBytes int64
	RowCount  int64
	ChunkTime int64
	Min       map[string]any
	Max       map[string]any
}

type Index interface {
	Add(entry *IndexEntry) error
	Rm(path string) error
	Run()
	Stop()
}
