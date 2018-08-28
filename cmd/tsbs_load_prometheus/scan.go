package main

import (
	"bufio"
	"log"
	"fmt"
	"io"

	"github.com/hagen1778/tsbs/load"
	"github.com/hagen1778/tsbs/cmd/tsbs_generate_data/serialize"
	"github.com/valyala/bytebufferpool"
)

type decoder struct {
	scanner *scanner
}

type scanner struct {
	r   io.Reader
	buf []byte
}

func (s *scanner) scan() bool {
	sizeBuf := make([]byte, 8)
	if _, err := io.ReadFull(s.r, sizeBuf); err != nil {
		if err != io.EOF {
			log.Printf("ERROR: cannot read packet size: %s", err)
		}
		return false
	}
	packetSize := unmarshalUint64(sizeBuf)

	s.buf = resize(s.buf, int(packetSize))
	if _, err := io.ReadFull(s.r, s.buf); err != nil {
		log.Printf("ERROR: cannot read body size: %s", err)
		return false
	}
	return true
}

func (d *decoder) Decode(_ *bufio.Reader) *load.Point {
	if ok := d.scanner.scan(); !ok {
		return nil
	}
	p := load.NewPoint(d.scanner.buf)
	d.scanner.buf = d.scanner.buf[:0]
	return p
}

type batch struct{
	*bytebufferpool.ByteBuffer
}

func (b *batch) Len() int {
	return serialize.PrometheusBatchSize
}

func (b *batch) Append(item *load.Point) {
	b.Write(item.Data.([]byte))
}

type factory struct{}

func (f *factory) New() load.Batch {
	return &batch{bytebufferpool.Get()}
}

func unmarshalUint64(src []byte) uint64 {
	if len(src) < 8 {
		panic(fmt.Errorf("BUG: not enough src bytes for decoding uint64; got %d bytes; want %d bytes", len(src), 8))
	}
	u := uint64(src[7]) | uint64(src[6])<<8 | uint64(src[5])<<16 | uint64(src[4])<<24 | uint64(src[3])<<32 | uint64(src[2])<<40 | uint64(src[1])<<48 | uint64(src[0])<<56
	return u
}

func resize(b []byte, n int) []byte {
	for cap(b) < n {
		b = append(b[:cap(b)], 0)
	}
	return b[:n]
}