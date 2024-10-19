package parsers

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/go-faster/jx"
	"io"
	"quackpipe/merge/shared"
)

type NDJSONParser struct {
	fields map[string]string
	lines  map[string]any
}

func (N *NDJSONParser) Parse(data []byte) (chan *ParserResponse, error) {
	return N.ParseReader(bytes.NewReader(data))
}

func (N *NDJSONParser) ParseReader(r io.Reader) (chan *ParserResponse, error) {
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
		switch v {
		case shared.TYPE_STRING:
			N.lines[k] = make([]string, 0)
		case shared.TYPE_INT64:
			N.lines[k] = make([]int64, 0)
		case shared.TYPE_UINT64:
			N.lines[k] = make([]uint64, 0)
		case shared.TYPE_FLOAT64:
			N.lines[k] = make([]float64, 0)
		}
	}
}

func (N *NDJSONParser) parseLine(line []byte) error {
	d := jx.DecodeBytes(line)
	return d.Obj(func(d *jx.Decoder, key string) error {
		tp, ok := N.fields[key]
		if !ok {
			return fmt.Errorf("field %s not found", key)
		}
		switch tp {
		case shared.TYPE_STRING:
			str, err := d.Str()
			if err != nil {
				return err
			}
			if _, ok := N.lines[key].([]string); !ok {
				return fmt.Errorf("field %s is not a string", key)
			}
			N.lines[key] = append(N.lines[key].([]string), str)
		case shared.TYPE_INT64:
			str, err := d.Int64()
			if err != nil {
				return err
			}
			field := N.lines[key]
			if _, ok := field.([]int64); !ok {
				return fmt.Errorf("field %s is not a string", key)
			}
			field = append(field.([]int64), str)
			N.lines[key] = field
		case shared.TYPE_UINT64:
			str, err := d.UInt64()
			if err != nil {
				return err
			}
			field := N.lines[key]
			if _, ok := field.([]uint64); !ok {
				return fmt.Errorf("field %s is not a string", key)
			}
			field = append(field.([]uint64), str)
			N.lines[key] = field
		case shared.TYPE_FLOAT64:
			str, err := d.Float64()
			if err != nil {
				return err
			}
			field := N.lines[key]
			if _, ok := field.([]float64); !ok {
				return fmt.Errorf("field %s is not a string", key)
			}
			field = append(field.([]float64), str)
			N.lines[key] = field
		}
		return nil
	})
}

var _ = func() int {
	RegisterParser("application/x-ndjson", func(fieldNames []string, fieldTypes []string) IParser {
		fields := make(map[string]string)
		for i, name := range fieldNames {
			fields[name] = fieldTypes[i]
		}
		return &NDJSONParser{fields: fields}
	})
	return 0
}()
