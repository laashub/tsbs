package main

import (
	"bufio"

	"github.com/hagen1778/tsbs/load"
	"log"
	"fmt"
	"github.com/prometheus/prometheus/pkg/textparse"
	"github.com/prometheus/prometheus/pkg/labels"
	"io"
	"github.com/prometheus/prometheus/prompb"
	"sync"
)

type decoder struct {
	scanner *bufio.Scanner
}

func (d *decoder) Decode(_ *bufio.Reader) *load.Point {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return nil
	} else if !ok {
		log.Fatal(fmt.Sprintf("scan error: %v", d.scanner.Err()))
		return nil
	}
	return load.NewPoint(d.scanner.Bytes())
}

var batchPool = &sync.Pool{New: func() interface{} { return &batch{} }}

type batch struct {
	timeSeries []*prompb.TimeSeries
}

func (b *batch) Len() int {
	return len(b.timeSeries)
}

func (b *batch) Append(item *load.Point) {
	data := item.Data.([]byte)
	p := textparse.New(data)
	var res labels.Labels
	for {
		et, err := p.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if et != textparse.EntrySeries {
			log.Fatalf("expected to have only entries in data")
		}

		_, ts, v := p.Series()
		p.Metric(&res)
		series := &prompb.TimeSeries{
			Labels: make([]*prompb.Label, len(res)+1),
			Samples: []*prompb.Sample{
				{
					Value: v,
					Timestamp: *ts,
				},
			},
		}
		for i, l := range res {
			series.Labels[i] = &prompb.Label{
				Value: l.Value,
				Name: l.Name,
			}
		}
		series.Labels[len(res)] =  &prompb.Label{
			Value: "benchmark",
			Name: "job",
		}

		b.timeSeries = append(b.timeSeries, series)
		res = res[:0]
	}
}

type factory struct{}

func (f *factory) New() load.Batch {
	return batchPool.Get().(*batch)
}
