package prometheus

import (
	"fmt"
	"net/url"
	"strings"
	"time"
	"strconv"

	"github.com/hagen1778/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/hagen1778/tsbs/query"
)

// Devops produces Influx-specific queries for all the devops query types.
type Devops struct {
	*devops.Core
}

// NewDevops makes an Devops object ready to generate Queries.
func NewDevops(start, end time.Time, scale int) *Devops {
	return &Devops{devops.NewCore(start, end, scale)}
}

// GenerateEmptyQuery returns an empty query.HTTP
func (d *Devops) GenerateEmptyQuery() query.Query {
	return query.NewHTTP()
}

func getHostClause(hostnames []string) string {
	if len(hostnames) == 0 {
		return ""
	}
	if len(hostnames) == 1 {
		return fmt.Sprintf("hostname='%s'", hostnames[0])
	}
	return fmt.Sprintf("hostname=~'%s'", strings.Join(hostnames, "|"))
}

func (d *Devops) getSelectClausesAggMetrics(agg string, metrics, hosts []string) []string {
	selectClauses := make([]string, len(metrics))
	for i, m := range metrics {
		selectClauses[i] = fmt.Sprintf("%s(%s) by (__name__)", agg, m)
	}

	return selectClauses
}

func getSelectClause(metrics, hosts []string) string {
	if len(metrics) == 0 {
		panic("BUG: must be at least one metric name in clause")
	}
	metricsCPU := make([]string, len(metrics))
	for i, v := range metrics {
		metricsCPU[i] = fmt.Sprintf("cpu_%s", v)
	}

	metricsClause := strings.Join(metricsCPU, "|")
	hostClause := getHostClause(hosts)
	if len(metrics) == 1 {
		return fmt.Sprintf("%s{%s}", metricsCPU[0], hostClause)
	}
	if len(hosts) > 0 {
		return fmt.Sprintf("{__name__=~%q, %s}", metricsClause, hostClause)
	}
	return fmt.Sprintf("{__name__=~%q}", metricsClause)
}

// GroupByTime selects the MAX for numMetrics metrics under 'cpu',
// for nhosts hosts,
// e.g.:
// max({__name__=~"metric1|metric2...|metricN",hostname=~"hostname1|hostname2...|hostnameN"}) by (__name__)
func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	metrics := devops.GetCPUMetricsSlice(numMetrics)
	hosts := d.GetRandomHosts(nHosts)
	selectClause := getSelectClause(metrics, hosts)
	humanLabel := fmt.Sprintf("Prometheus %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	q := fmt.Sprintf("max(%s) by (__name__)", selectClause)
	// step is 1m
	d.fillInQuery(qi, "60", humanLabel, q, timeRange)
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in psuedo-SQL:
//
// avg({__name__=~"metric1|metric2...|metricN"}) by (hostname)
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	metrics := devops.GetCPUMetricsSlice(numMetrics)
	selectClause := getSelectClause( metrics, []string{})
	humanLabel := devops.GetDoubleGroupByLabel("Prometheus", numMetrics)
	q := fmt.Sprintf("avg(%s) by (hostname)", selectClause)
	// step is 1h
	d.fillInQuery(qi, "3600", humanLabel, q, devops.DoubleGroupByDuration)
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for nhosts hosts,
// e.g. in psuedo-SQL:
//
// SELECT MAX(metric1), ..., MAX(metricN)
// FROM cpu WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour ORDER BY hour
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int) {
	metrics := devops.GetAllCPUMetrics()
	hosts := d.GetRandomHosts(nHosts)
	selectClause := getSelectClause( metrics, hosts)
	humanLabel := devops.GetMaxAllLabel("Prometheus", nHosts)
	q := fmt.Sprintf("max(%s) by (__name__)", selectClause)
	// step is 1m
	d.fillInQuery(qi, "60", humanLabel, q, devops.MaxAllDuration)
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a number of hosts (if 0, it will search all hosts),
// e.g. in psuedo-SQL:
//
// SELECT * FROM cpu
// WHERE usage_user > 90.0
// AND time >= '$TIME_START' AND time < '$TIME_END'
// AND (hostname = '$HOST' OR hostname = '$HOST2'...)
func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	metrics := []string{"cpu", ".*"}
	hosts := d.GetRandomHosts(nHosts)
	selectClause := getSelectClause(metrics, hosts)
	humanLabel := devops.GetHighCPULabel("Prometheus", nHosts)
	q := fmt.Sprintf("%s > 90", selectClause)
	// step is 1m
	d.fillInQuery(qi, "60", humanLabel, q, devops.HighCPUDuration)
}

func (d *Devops) fillInQuery(qi query.Query, step, humanLabel, promQuery string, duration time.Duration) {
	interval := d.Interval.RandWindow(duration)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())

	v := url.Values{}
	v.Set("query", promQuery)
	v.Set("start", strconv.FormatInt(interval.StartUnixNano()/1e9, 10))
	v.Set("end", strconv.FormatInt(interval.EndUnixNano()/1e9, 10))
	v.Set("step", step)

	q := qi.(*query.HTTP)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/api/v1/query_range?%s", v.Encode()))
	q.Body = nil
}

/*


// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(qi query.Query) {
	humanLabel := "Influx last row per host"
	humanDesc := humanLabel + ": cpu"
	influxql := "SELECT * from cpu group by \"hostname\" order by time desc limit 1"
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}


*/
