package merge

import (
	"github.com/metrico/quackpipe/config"
	"github.com/metrico/quackpipe/merge/repository"
	"github.com/metrico/quackpipe/utils/promise"
	"os"
	"path"
	"testing"
	"time"
)

func TestMerge(t *testing.T) {
	cwd, _ := os.Getwd()
	cwd = path.Join(cwd, "..", "_data")

	config.Config = &config.Configuration{
		QuackPipe: config.QuackPipeConfiguration{
			Root:          cwd,
			MergeTimeoutS: 10,
			SaveTimeoutS:  1,
			Secret:        "XXXXXX",
		},
	}
	Init()

	var p [2]promise.Promise[int32]
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
