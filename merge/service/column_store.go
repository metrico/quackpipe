package service

import (
	"github.com/metrico/quackpipe/model"
)

func AppendColumnStore(c *model.ColumnStore, column any) error {
	storeSize := c.Tp.GetLength(c.Data)
	_data, err := c.Tp.AppendStore(c.Data, column)
	if err != nil {
		return err
	}
	dataSize := c.Tp.GetLength(column)
	c.Data = _data
	c.Valids = append(c.Valids, make([]bool, dataSize)...)
	fastFillArray(c.Valids[storeSize:], true)
	return nil
}

func AppendNullsColumnStore(c *model.ColumnStore, size int) {
	c.Tp.AppendDefault(size, c.Data)
	c.Valids = append(c.Valids, make([]bool, size)...)
}
