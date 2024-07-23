package model

// Metadata is the metadata for a column
type Metadata struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// Statistics is the statistics for a query
type Statistics struct {
	Elapsed   float64 `json:"elapsed"`
	RowsRead  int     `json:"rows_read"`
	BytesRead int     `json:"bytes_read"`
}

// OutputJSON is the JSON output for a query
type OutputJSON struct {
	Meta                   []Metadata      `json:"meta"`
	Data                   [][]interface{} `json:"data"`
	Rows                   int             `json:"rows"`
	RowsBeforeLimitAtLeast int             `json:"rows_before_limit_at_least"`
	Statistics             Statistics      `json:"statistics"`
}
