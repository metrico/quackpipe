package model

type Table struct {
	Name               string
	Path               string
	Engine             string
	OrderBy            []string
	TimestampField     string
	TimestampPrecision string
	PartitionBy        string
	AutoTimestamp      bool
}
