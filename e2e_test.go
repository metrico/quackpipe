package main

import (
	"fmt"
	"github.com/gigapi/gigapi/config"
	"github.com/gigapi/gigapi/merge"
	"github.com/gigapi/gigapi/merge/repository"
	"github.com/gigapi/gigapi/utils"
	"os"
	"runtime/pprof"
	"sync"
	"testing"
	"time"
)

func startCPUProfile(t *testing.T) func() {
	cpuFile, err := os.Create("cpu.pprof")
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		t.Fatal(err)
	}
	return func() {
		pprof.StopCPUProfile()
		cpuFile.Close()
	}
}

func writeMemProfile(t *testing.T) {
	memFile, err := os.Create("mem.pprof")
	if err != nil {
		t.Fatal(err)
	}
	defer memFile.Close()
	if err := pprof.WriteHeapProfile(memFile); err != nil {
		t.Fatal(err)
	}
}

const N = 200
const S = 100000

func TestE2E(t *testing.T) {
	// Start CPU profiling
	stopCPUProfile := startCPUProfile(t)
	defer stopCPUProfile()

	config.Config = &config.Configuration{
		Gigapi: config.GigapiConfiguration{
			Root:          "_testdata",
			MergeTimeoutS: 10,
			Secret:        "XXXXXX",
		},
	}
	merge.Init()

	var data = map[string]any{
		"timestamp": []int64{},
		"value":     []float64{},
		"str":       []string{},
	}
	promises := make([]utils.Promise[int32], N)
	size := 0
	for i := 0; i < S; i++ {
		data["timestamp"] = append(data["timestamp"].([]int64), int64(time.Now().UnixNano()))
		data["value"] = append(data["value"].([]float64), float64(i)/100.0)
		str := fmt.Sprintf("str%d", i)
		data["str"] = append(data["str"].([]string), str)
		size += 8 + 8 + 8 + 1 + len(str)
	}
	start := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < N; i++ {
		_i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			promises[_i] = repository.Store("", "test", data)
		}()

	}
	wg.Wait()
	fmt.Printf("Appending data %v\n", time.Since(start))
	for _, pp := range promises {
		_, err := pp.Get()
		if err != nil {
			panic(err)
		}
	}
	fmt.Printf("%d rows / %v MB written in %v\n", S*N, float64(size*N)/(1024*1024), time.Since(start))
	fmt.Println("Wating for merge...")
	time.Sleep(time.Second * 60)
}
