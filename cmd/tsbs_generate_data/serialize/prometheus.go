package serialize

import (
	"io"
	"fmt"
	"strconv"
)

// PrometheusSerializer writes a Point in a serialized form for Prometheus
type PrometheusSerializer struct{}

// Serialize writes Point data to the given writer, conforming to the
// Prometheus wire protocol.
func (s *PrometheusSerializer) Serialize(p *Point, w io.Writer) (err error) {
	var labels []byte
	labelsLen := len(p.tagKeys)
	if labelsLen > 0 {
		labels = make([]byte, 0)
		labels = append(labels, '{')
		for i := 0; i < labelsLen; i++ {
			labels = append(labels, p.tagKeys[i]...)
			labels = append(labels,'=')
			labels = append(labels,'"')
			labels = append(labels, p.tagValues[i]...)
			labels = append(labels,'"')
			if i != labelsLen-1 {
				labels = append(labels, ',')
			}
		}
		labels = append(labels, '}')
	}

	buf := scratchBufPool.Get().([]byte)
	for i := 0; i < len(p.fieldKeys); i++ {
		buf = append(buf, p.measurementName...)
		buf = append(buf, '_')
		buf = append(buf, p.fieldKeys[i]...)

		if labelsLen > 0 {
			buf = append(buf, labels...)
		}

		buf = append(buf, ' ')
		buf = valueAppend(p.fieldValues[i], buf)
		buf = append(buf, ' ')
		buf = valueAppend(p.timestamp.UnixNano()/1e6, buf)
		buf = append(buf, '\n')
	}
	_, err = w.Write(buf)
	buf = buf[:0]
	scratchBufPool.Put(buf)
	return err
}

func valueAppend(v interface{}, buf []byte) []byte {
	switch v.(type) {
	case int:
		return strconv.AppendFloat(buf, float64(v.(int)), 'f', -1, 64)
	case int64:
		return strconv.AppendFloat(buf, float64(v.(int64)), 'f', -1, 64)
	case float64:
		return strconv.AppendFloat(buf, v.(float64), 'f', -1, 64)
	case float32:
		return strconv.AppendFloat(buf, float64(v.(float32)), 'f', -1, 32)
	default:
		panic(fmt.Sprintf("unknown field type for %#v", v))
	}
}