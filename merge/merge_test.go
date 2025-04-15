package merge

import (
	"github.com/gigapi/gigapi/config"
	"github.com/gigapi/gigapi/merge/repository"
	"github.com/gigapi/gigapi/utils"
	"os"
	"path"
	"testing"
	"time"
)

func TestMerge(t *testing.T) {
	cwd, _ := os.Getwd()
	cwd = path.Join(cwd, "..", "_data")

	config.Config = &config.Configuration{
		Gigapi: config.GigapiConfiguration{
			Root:          cwd,
			MergeTimeoutS: 10,
			SaveTimeoutS:  1,
			Secret:        "XXXXXX",
		},
	}
	Init()

	var p [2]utils.Promise[int32]
	for i := 0; i < 100; i++ {
		p[0] = repository.Store("", "test", map[string]any{
			"a": []int64{
				time.Now().UnixNano(),
				time.Now().UnixNano(),
				time.Now().UnixNano(),
				time.Now().UnixNano(),
			},
			"b": []string{"x", "y", "z", "w"},
		})
		p[1] = repository.Store("", "test", map[string]any{
			"b": []string{"x", "y", "z", "w"},
		})
		for _, pp := range p {
			if _, err := pp.Get(); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(time.Second)
	}
}
