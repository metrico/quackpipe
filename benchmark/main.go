package main

import (
	"bytes"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const CLIENTS = 1

var requestDuration = promauto.NewHistogram(prometheus.HistogramOpts{
	Name: "insert_request_duration_seconds",
	Help: "Duration of HTTP requests in seconds",
	ConstLabels: prometheus.Labels{
		"job":     "quackdb_benchmark",
		"clients": fmt.Sprintf("%d", CLIENTS),
		"mbps":    "30",
	},
	Buckets: []float64{0.1, 0.5, 1, 5, 10, 20, 30},
})

var totalRequests = promauto.NewCounter(prometheus.CounterOpts{
	Name: "total_insert_requests",
	Help: "Duration of HTTP requests in seconds",
	ConstLabels: prometheus.Labels{
		"job":     "quackdb_benchmark",
		"clients": fmt.Sprintf("%d", CLIENTS),
		"mbps":    "30",
	},
})

var totalBytes = promauto.NewCounter(prometheus.CounterOpts{
	Name: "total_insert_bytes",
	Help: "Duration of HTTP requests in seconds",
	ConstLabels: prometheus.Labels{
		"job":     "quackdb_benchmark",
		"clients": fmt.Sprintf("%d", CLIENTS),
		"mbps":    "30",
	},
})

func main() {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(":9090", nil); err != nil {
			panic(err)
		}
	}()
	time.Sleep(time.Minute)
	runBenchmark(30, CLIENTS, time.Minute*5)
}

func runBenchmark(mbps int, clients int, timeout time.Duration) {
	resp, err := http.Post("http://localhost:8333/quackdb/create", "application/x-yaml",
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
		panic(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		panic(fmt.Errorf("[%d]: %s", resp.StatusCode, string(body)))
	}

	bPClient := mbps * 1024 * 1024 / clients
	wg := &sync.WaitGroup{}
	var working int32 = 1
	t := time.NewTicker(time.Second)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var _wg sync.WaitGroup
			for range t.C {
				if atomic.LoadInt32(&working) != 1 {
					return
				}
				for i := 0; i < clients; i++ {
					_wg.Add(1)
					go func() {
						defer _wg.Done()
						bodyBuilder := strings.Builder{}
						for bodyBuilder.Len() < bPClient {
							s := fmt.Sprintf(
								"{\"timestamp_ns\": %d, \"fingerprint\": 1234567890, \"str\": \"hello %[1]d\", \"value\": 123.456}\n",
								time.Now().UnixNano())
							bodyBuilder.WriteString(s)
						}
						body = []byte(bodyBuilder.String())
						start := time.Now()
						res, err := http.Post("http://localhost:8333/quackdb/test/insert", "application/x-ndjson",
							bytes.NewReader(body),
						)
						if err != nil {
							panic(err)
						}
						defer res.Body.Close()
						requestDuration.Observe(time.Since(start).Seconds())
						totalRequests.Inc()
						totalBytes.Add(float64(len(body)))
					}()
				}
				_wg.Wait()
			}
		}(i)
	}
	time.Sleep(timeout)
	atomic.StoreInt32(&working, 0)
	wg.Wait()
}
