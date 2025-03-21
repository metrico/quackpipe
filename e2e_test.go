package main

import (
	"fmt"
	"github.com/metrico/quackpipe/config"
	"github.com/metrico/quackpipe/merge"
	"github.com/metrico/quackpipe/model"
	"github.com/metrico/quackpipe/router"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestE2E(t *testing.T) {
	config.Config = &config.Configuration{
		QuackPipe: config.QuackPipeConfiguration{
			Enabled:       true,
			Root:          "_testdata",
			MergeTimeoutS: 10,
			Secret:        "XXXXXX",
		},
	}
	config.AppFlags = &model.CommandLineFlags{
		Host:   toPtr("localhost"),
		Port:   toPtr("8123"),
		Stdin:  toPtr(false),
		Alias:  toPtr(true),
		Format: toPtr(""),
		Params: toPtr(""),
		DBPath: toPtr("_testdata"),
		Config: toPtr(""),
	}
	go runServer()
	time.Sleep(1 * time.Second)
	resp, err := http.Post("http://localhost:8123/quackdb/create", "application/x-yaml",
		strings.NewReader(`create_table: test
fields:
  timestamp_ns: Int64
  fingerprint: Int64
  str: String
  value: Float64
engine: Merge
order_by:
  - timestamp_ns
timestamp:
  field: timestamp_ns
  precision: ns
partition_by: ""
`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("[%d]: %s", resp.StatusCode, string(body))
	}
	fmt.Println(string(body))

	resp, err = http.Post("http://localhost:8123/quackdb/test/insert", "application/x-ndjson",
		strings.NewReader(
			`{"timestamp_ns": 1668326823000000000, "fingerprint": 1234567890, "str": "hello", "value": 123.456}
{"timestamp_ns": 1668326823000000000, "fingerprint": 1234567890, "str": "hello", "value": 123.456}
{"timestamp_ns": 1668326823000000000, "fingerprint": 1234567890, "str": "hello", "value": 123.456}
{"timestamp_ns": 1668326823000000000, "fingerprint": 1234567890, "str": "hello", "value": 123.456}
{"timestamp_ns": 1668326823000000000, "fingerprint": 1234567890, "str": "hello", "value": 123.456}
{"timestamp_ns": 1668326823000000000, "fingerprint": 1234567890, "str": "hello", "value": 123.456}
{"timestamp_ns": 1668326823000000000, "fingerprint": 1234567890, "str": "hello", "value": 123.456}
{"timestamp_ns": 1668326823000000000, "fingerprint": 1234567890, "str": "hello", "value": 123.456}`,
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("[%d]: %s", resp.StatusCode, string(body))
	}
	fmt.Println(string(body))

	return
}

func toPtr[X any](val X) *X {
	return &val
}

func runServer() {
	if config.Config.QuackPipe.Enabled {
		merge.Init()
	}
	r := router.NewRouter(config.AppFlags)
	fmt.Printf("QuackPipe API Running: %s:%s\n", *config.AppFlags.Host, *config.AppFlags.Port)
	if err := http.ListenAndServe(*config.AppFlags.Host+":"+*config.AppFlags.Port, r); err != nil {
		panic(err)
	}
}
