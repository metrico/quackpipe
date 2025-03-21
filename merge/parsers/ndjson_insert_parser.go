package parsers

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/go-faster/jx"
	"github.com/metrico/quackpipe/merge/data_types"
	"io"
)

type NDJSONParser struct {
	fields map[string]string
	lines  map[string]any
}

func (N *NDJSONParser) Parse(data []byte) (chan *ParserResponse, error) {
	return N.ParseReader(nil, bytes.NewReader(data))
}

func (N *NDJSONParser) ParseReader(ctx context.Context, r io.Reader) (chan *ParserResponse, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanLines)
	N.resetLines()
	res := make(chan *ParserResponse)
	go func() {
		defer close(res)
		bytesParsed := 0
		linesLen := 0
		for scanner.Scan() {
			err := N.parseLine(scanner.Bytes())
			if err != nil {
				res <- &ParserResponse{Error: err}
				return
			}
			bytesParsed += len(scanner.Bytes())
			linesLen++
			if bytesParsed >= 10*1024*1024 {
				res <- &ParserResponse{Data: N.lines}
				N.resetLines()
				bytesParsed = 0
				linesLen = 0
			}
		}
		if linesLen > 0 {
			res <- &ParserResponse{Data: N.lines}
			N.resetLines()
		}
	}()
	return res, nil
}

func (N *NDJSONParser) resetLines() {
	N.lines = make(map[string]any)
	for k, v := range N.fields {
		N.lines[k] = data_types.DataTypes[v].MakeStore()
	}
}

func (N *NDJSONParser) parseLine(line []byte) error {
	d := jx.DecodeBytes(line)
	return d.Obj(func(d *jx.Decoder, key string) error {
		tp, ok := N.fields[key]
		if !ok {
			return fmt.Errorf("field %s not found", key)
		}
		var err error
		N.lines[key], err = data_types.DataTypes[tp].ParseJson(d, N.lines[key])
		if err != nil {
			return fmt.Errorf("invalid data for field %s: %w", key, err)
		}
		return nil
	})
}

var _ = func() int {
	/*RegisterParser("application/x-ndjson", func(fieldNames []string, fieldTypes []string) IParser {
		fields := make(map[string]string)
		for i, name := range fieldNames {
			fields[name] = fieldTypes[i]
		}
		return &NDJSONParser{fields: fields}
	})*/
	return 0
}()
