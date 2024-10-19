package merge

import (
	"quackpipe/config"
	"quackpipe/merge/repository"
	"quackpipe/model"
	"testing"
)

func TestMerge(t *testing.T) {
	config.Config = &config.Configuration{
		QuackPipe: config.QuackPipeConfiguration{
			Enabled:       true,
			Root:          ".",
			MergeTimeoutS: 10,
			Secret:        "XXXXXX",
		},
		DBPath: ".",
	}
	Init()
	err := repository.RegisterNewTable(&model.Table{
		Name: "test",
		Path: "/tmp/test",
		Fields: [][2]string{
			{"timestamp", "UInt64"},
			{"value", "Float64"},
		},
		Engine: "Merge",
		OrderBy: []string{
			"timestamp",
		},
		TimestampField:     "timestamp",
		TimestampPrecision: "s",
		PartitionBy:        "timestamp / 3600 / 24",
	})
	if err != nil {
		t.Fatal(err)
	}
}
