package handlers

import (
	"fmt"
	"github.com/metrico/quackpipe/config"
	"github.com/metrico/quackpipe/merge/repository"
	"github.com/metrico/quackpipe/model"
	"gopkg.in/yaml.v3"
	"io"
	"net/http"
	"strings"
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
	S3Url       string            `json:"s3_url" yaml:"s3_url"`
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

	if !config.Config.QuackPipe.AllowSaveToHD {
		if req.S3Url == "" {
			return fmt.Errorf("s3_url is required")
		}

	}
	if req.S3Url != "" && !strings.HasPrefix(req.S3Url, "s3://") {
		return fmt.Errorf("s3_url must start with s3://")
	}

	table := model.Table{
		Name:        req.CreateTable,
		Engine:      req.Engine,
		OrderBy:     req.OrderBy,
		PartitionBy: nil,
		Path:        req.S3Url,
	}
	err = repository.RegisterNewTable(&table)
	if err != nil {
		return err
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Ok"))
	return nil
}
