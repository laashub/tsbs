package devops

import (
	"time"

	"github.com/hagen1778/tsbs/cmd/tsbs_generate_data/common"
	"github.com/hagen1778/tsbs/cmd/tsbs_generate_data/serialize"
)

var (
	labelPostgresql = []byte("postgresl") // heap optimization

	// Reuse NormalDistributions as arguments to other distributions. This is
	// safe to do because the higher-level distribution advances the ND and
	// immediately uses its value and saves the state
	pgND     = common.ND(5, 1)
	pgHighND = common.ND(1024, 1)

	PostgresqlFields = []labeledDistributionMaker{
		{[]byte("numbackends"), func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{[]byte("xact_commit"), func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{[]byte("xact_rollback"), func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{[]byte("blks_read"), func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{[]byte("blks_hit"), func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{[]byte("tup_returned"), func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{[]byte("tup_fetched"), func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{[]byte("tup_inserted"), func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{[]byte("tup_updated"), func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{[]byte("tup_deleted"), func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{[]byte("conflicts"), func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{[]byte("temp_files"), func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{[]byte("temp_bytes"), func() common.Distribution { return common.CWD(pgHighND, 0, 1024*1024*1024, 0) }},
		{[]byte("deadlocks"), func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{[]byte("blk_read_time"), func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
		{[]byte("blk_write_time"), func() common.Distribution { return common.CWD(pgND, 0, 1000, 0) }},
	}
)

type PostgresqlMeasurement struct {
	*subsystemMeasurement
}

func NewPostgresqlMeasurement(start time.Time) *PostgresqlMeasurement {
	sub := newSubsystemMeasurementWithDistributionMakers(start, PostgresqlFields)
	return &PostgresqlMeasurement{sub}
}

func (m *PostgresqlMeasurement) ToPoint(p *serialize.Point) {
	m.toPointAllInt64(p, labelPostgresql, PostgresqlFields)
}
