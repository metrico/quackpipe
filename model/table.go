package model

const (
	ITERATIONS_LIMIT = 4
)

type Table struct {
	Name string
	// path to store the data and tmp files in the filesystem
	FSPath             string
	Paths              [ITERATIONS_LIMIT]string
	Fields             [][2]string
	Engine             string
	OrderBy            []string
	TimestampField     string
	TimestampPrecision string
	PartitionBy        string
}
