package main

import (
	"flag"
	"net/http"
	"time"
	"bufio"
	"io"

	"github.com/hagen1778/tsbs/query"
)

// Program option vars:
var url string

// Global vars:
var runner *query.BenchmarkRunner

func init() {
	runner = query.NewBenchmarkRunner()
	flag.StringVar(&url, "url", "http://localhost:9090", "Prometheus URL")
	flag.Parse()
}

func main() {
	runner.Run(&query.HTTPPool, newProcessor)
}

type processor struct {
	*http.Client
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	hq := q.(*query.HTTP)

	// populate a request with data from the Query:
	req, err := http.NewRequest(string(hq.Method), url+string(hq.Path), nil)
	if err != nil {
		panic(err)
	}

	// Perform the request while tracking latency:
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		panic("http request did not return status 200 OK")
	}

	reader := bufio.NewReader(resp.Body)
	buf := make([]byte, 8192)
	for {
		_, err = reader.Read(buf)
		if err == io.EOF {
			err = nil
			break
		} else if err != nil {
			panic(err)
		}
	}
	lag := float64(time.Since(start).Nanoseconds()) / 1e6 // milliseconds
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), lag)
	return []*query.Stat{stat}, nil
}
