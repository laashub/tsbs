package serialize

import (
	"testing"
)

func TestPrometheusSerializerSerialize(t *testing.T) {
	cases := []serializeCase{
		{
			desc:       "a regular Point",
			inputPoint: testPointDefault,
			output:     "cpu_usage_guest_nice{hostname=\"host_0\",region=\"eu-west-1\",datacenter=\"eu-west-1b\"} 38.24311829 1451606400\n",
		},
		{
			desc:       "a regular Point using int as value",
			inputPoint: testPointInt,
			output:     "cpu_usage_guest{hostname=\"host_0\",region=\"eu-west-1\",datacenter=\"eu-west-1b\"} 38 1451606400\n",},
		{
			desc:       "a regular Point with multiple fields",
			inputPoint: testPointMultiField,
			output: "cpu_big_usage_guest{hostname=\"host_0\",region=\"eu-west-1\",datacenter=\"eu-west-1b\"} 5000000000 1451606400\n" +
				"cpu_usage_guest{hostname=\"host_0\",region=\"eu-west-1\",datacenter=\"eu-west-1b\"} 38 1451606400\n" +
				"cpu_usage_guest_nice{hostname=\"host_0\",region=\"eu-west-1\",datacenter=\"eu-west-1b\"} 38.24311829 1451606400\n",
		},
		{
			desc:       "a Point with no tags",
			inputPoint: testPointNoTags,
			output:     "cpu_usage_guest_nice 38.24311829 1451606400\n",
		},
	}

	testSerializer(t, cases, &PrometheusSerializer{})
}
