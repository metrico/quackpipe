package handlers

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"net/http"
	"path/filepath"
	"quackpipe/config"
	"quackpipe/merge/repository"
	"quackpipe/model"
)

type TimestampField struct {
	Field     string `json:"field" yaml:"field"`
	Precision string `json:"precision" yaml:"precision"`
}

type CreateTableRequest struct {
	CreateTable string            `json:"create_table" yaml:"create_table"`
	Fields      map[string]string `json:"fields" yaml:"fields"`
	Engine      string            `json:"engine" yaml:"engine"`
	OrderBy     []string          `json:"order_by" yaml:"order_by"`
	Timestamp   TimestampField    `json:"timestamp" yaml:"timestamp"`
	PartitionBy string            `json:"partition_by" yaml:"partition_by"`
}

func CreateTableHandler(w http.ResponseWriter, r *http.Request) error {
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	var req CreateTableRequest
	err = yaml.Unmarshal(body, &req)
	if err != nil {
		return err
	}

	var fields [][2]string
	for field, fieldType := range req.Fields {
		fields = append(fields, [2]string{field, fieldType})
	}

	for _, field := range req.OrderBy {
		if _, ok := req.Fields[field]; !ok {
			return fmt.Errorf("field %s does not exist", field)
		}
	}

	if _, ok := req.Fields[req.Timestamp.Field]; !ok {
		return fmt.Errorf("field %s does not exist", req.Timestamp.Field)
	}

	table := model.Table{
		Name:               req.CreateTable,
		Path:               filepath.Join(config.Config.QuackPipe.Root, req.CreateTable),
		Fields:             fields,
		Engine:             req.Engine,
		OrderBy:            req.OrderBy,
		TimestampField:     req.Timestamp.Field,
		TimestampPrecision: req.Timestamp.Precision,
		PartitionBy:        req.PartitionBy,
	}
	err = repository.RegisterNewTable(&table)
	if err != nil {
		return err
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Ok"))
	return nil
}
