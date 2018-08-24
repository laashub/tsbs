package main

import (
	"bufio"
	"flag"
	"log"
	"time"
	"net/http"

	"github.com/hagen1778/tsbs/load"
	"github.com/prometheus/prometheus/prompb"
	"fmt"
	"bytes"
	"io/ioutil"
	"github.com/klauspost/compress/snappy"
	"sync"
	_ "net/http/pprof"

)

// Global vars
var (
	loader  *load.BenchmarkRunner
)

var remoteStorageURL string

// Parse args:
func init() {
	loader = load.GetBenchmarkRunner()
	flag.StringVar(&remoteStorageURL, "url", "http://localhost:8080", "Prometheus Remote Storage Insert daemon URL")
	flag.Parse()
}

type benchmark struct{}

func (b *benchmark) GetPointDecoder(br *bufio.Reader) load.PointDecoder {
	return &decoder{scanner: bufio.NewScanner(br)}
}

func (b *benchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(_ uint) load.PointIndexer {
	return &load.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() load.Processor {
	return &processor{}
}

func (b *benchmark) GetDBCreator() load.DBCreator {
	return &dbCreator{}
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	loader.RunBenchmark(&benchmark{}, load.SingleQueue)
}

type processor struct {
	*http.Client
}

func (p *processor) Init(numWorker int, _ bool) {
	p.Client = &http.Client{
		Timeout: time.Minute,
	}
}

func (p *processor) Close(_ bool) {}

type buffer struct {
	b []byte
}

func newBufferForPool() interface{} {
	return &buffer{}
}

var (
	snappyBufferPool = &sync.Pool{
		New: newBufferForPool,
	}
)
func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)
	if doLoad {
		var err error
		req := &prompb.WriteRequest{
			Timeseries: batch.timeSeries,
		}
		data, err := req.Marshal()
		if err != nil{
			log.Fatal(err)
		}
		sb := snappyBufferPool.Get().(*buffer)
		sb.b = snappy.Encode(sb.b, data)
		url := fmt.Sprintf("%s/prometheus/insert", remoteStorageURL)
		httpReq, err := http.NewRequest("POST", url,  bytes.NewReader(sb.b))
		if err != nil {
			snappyBufferPool.Put(sb)
			log.Fatal(err)
		}
		httpReq.Header.Add("Content-Encoding", "snappy")
		httpReq.Header.Set("Content-Type", "application/x-protobuf")
		httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

		httpResp, err := p.Client.Do(httpReq)
		if err != nil {
			snappyBufferPool.Put(sb)
			log.Fatal(err)
		}
		snappyBufferPool.Put(sb)
		if httpResp.StatusCode/100 != 2 {
			b, _ := ioutil.ReadAll(httpResp.Body)
			log.Fatalf("server returned HTTP status %s: %s", httpResp.Status, string(b))
		}
			httpResp.Body.Close()
		if err != nil {
			log.Fatalf("Error writing: %s\n", err.Error())
		}
	}
	batchPool.Put(batch)

	return uint64(batch.Len()), 0
}