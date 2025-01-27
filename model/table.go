package model

type Table struct {
	Name               string
	Path               string
	Fields             [][2]string
	Engine             string
	OrderBy            []string
	TimestampField     string
	TimestampPrecision string
	PartitionBy        string
}
