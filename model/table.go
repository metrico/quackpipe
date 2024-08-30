package model

type Table struct {
	Name    string
	Path    string
	Fields  [][2]string
	OrderBy []string
}
