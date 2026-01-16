// Package catalog provides PostgreSQL-compatible pg_catalog views for dukdb-go.
// This file implements the pg_stat_statements extension compatibility.
package catalog

// pg_stat_statements columns - PostgreSQL statement statistics.
// Reference: https://www.postgresql.org/docs/current/pgstatstatements.html
var pgStatStatementsColumns = []string{
	"userid",               // OID of the user who executed the statement
	"dbid",                 // OID of the database in which the statement was executed
	"toplevel",             // True if the query was executed as a top-level statement
	"queryid",              // Hash code to identify identical statements
	"query",                // Text of a representative statement
	"plans",                // Number of times the statement was planned
	"total_plan_time",      // Total time spent planning the statement (milliseconds)
	"min_plan_time",        // Minimum time spent planning the statement
	"max_plan_time",        // Maximum time spent planning the statement
	"mean_plan_time",       // Mean time spent planning the statement
	"stddev_plan_time",     // Population standard deviation of planning time
	"calls",                // Number of times the statement was executed
	"total_exec_time",      // Total time spent executing the statement (milliseconds)
	"min_exec_time",        // Minimum time spent executing the statement
	"max_exec_time",        // Maximum time spent executing the statement
	"mean_exec_time",       // Mean time spent executing the statement
	"stddev_exec_time",     // Population standard deviation of execution time
	"rows",                 // Total number of rows retrieved or affected
	"shared_blks_hit",      // Total number of shared block cache hits
	"shared_blks_read",     // Total number of shared blocks read
	"shared_blks_dirtied",  // Total number of shared blocks dirtied
	"shared_blks_written",  // Total number of shared blocks written
	"local_blks_hit",       // Total number of local block cache hits
	"local_blks_read",      // Total number of local blocks read
	"local_blks_dirtied",   // Total number of local blocks dirtied
	"local_blks_written",   // Total number of local blocks written
	"temp_blks_read",       // Total number of temp blocks read
	"temp_blks_written",    // Total number of temp blocks written
	"blk_read_time",        // Total time spent reading blocks (milliseconds)
	"blk_write_time",       // Total time spent writing blocks (milliseconds)
	"wal_records",          // Total number of WAL records generated
	"wal_fpi",              // Total number of WAL full page images generated
	"wal_bytes",            // Total bytes of WAL generated
}

// StatementStatsProvider is an interface for getting statement statistics.
// The server package implements this interface to provide metrics data.
type StatementStatsProvider interface {
	// GetStatementStats returns all tracked statement statistics.
	GetStatementStats() []*StatementStatsEntry
}

// StatementStatsEntry represents statistics for a single statement type.
type StatementStatsEntry struct {
	// UserOID is the OID of the user who executed the statement.
	UserOID int64

	// DatabaseOID is the OID of the database.
	DatabaseOID int64

	// QueryID is a hash code identifying this statement type.
	QueryID int64

	// Query is a representative statement text.
	Query string

	// Calls is the number of times executed.
	Calls int64

	// TotalExecTime is the total execution time in milliseconds.
	TotalExecTime float64

	// MinExecTime is the minimum execution time in milliseconds.
	MinExecTime float64

	// MaxExecTime is the maximum execution time in milliseconds.
	MaxExecTime float64

	// MeanExecTime is the mean execution time in milliseconds.
	MeanExecTime float64

	// StddevExecTime is the standard deviation of execution time.
	StddevExecTime float64

	// Rows is the total number of rows retrieved or affected.
	Rows int64

	// SharedBlksHit is the total shared block cache hits.
	SharedBlksHit int64

	// SharedBlksRead is the total shared blocks read.
	SharedBlksRead int64

	// LocalBlksHit is the total local block cache hits.
	LocalBlksHit int64

	// LocalBlksRead is the total local blocks read.
	LocalBlksRead int64

	// BlkReadTime is the total block read time in milliseconds.
	BlkReadTime float64

	// BlkWriteTime is the total block write time in milliseconds.
	BlkWriteTime float64
}

// PgStatStatementsView handles pg_stat_statements queries.
type PgStatStatementsView struct {
	provider     StatementStatsProvider
	databaseName string
	databaseOID  int64
	userOID      int64
}

// NewPgStatStatementsView creates a new pg_stat_statements view handler.
func NewPgStatStatementsView(provider StatementStatsProvider, databaseName string) *PgStatStatementsView {
	return &PgStatStatementsView{
		provider:     provider,
		databaseName: databaseName,
		databaseOID:  generateOID("database:" + databaseName),
		userOID:      10, // Default user OID
	}
}

// Query returns pg_stat_statements data.
func (v *PgStatStatementsView) Query(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: pgStatStatementsColumns,
		Rows:    make([]map[string]any, 0),
	}

	if v.provider == nil {
		return result
	}

	stats := v.provider.GetStatementStats()
	for _, stat := range stats {
		userOID := stat.UserOID
		if userOID == 0 {
			userOID = v.userOID
		}

		dbOID := stat.DatabaseOID
		if dbOID == 0 {
			dbOID = v.databaseOID
		}

		row := map[string]any{
			"userid":              userOID,
			"dbid":                dbOID,
			"toplevel":            true,
			"queryid":             stat.QueryID,
			"query":               stat.Query,
			"plans":               int64(0), // We don't track plan count separately
			"total_plan_time":     float64(0),
			"min_plan_time":       float64(0),
			"max_plan_time":       float64(0),
			"mean_plan_time":      float64(0),
			"stddev_plan_time":    float64(0),
			"calls":               stat.Calls,
			"total_exec_time":     stat.TotalExecTime,
			"min_exec_time":       stat.MinExecTime,
			"max_exec_time":       stat.MaxExecTime,
			"mean_exec_time":      stat.MeanExecTime,
			"stddev_exec_time":    stat.StddevExecTime,
			"rows":                stat.Rows,
			"shared_blks_hit":     stat.SharedBlksHit,
			"shared_blks_read":    stat.SharedBlksRead,
			"shared_blks_dirtied": int64(0),
			"shared_blks_written": int64(0),
			"local_blks_hit":      stat.LocalBlksHit,
			"local_blks_read":     stat.LocalBlksRead,
			"local_blks_dirtied":  int64(0),
			"local_blks_written":  int64(0),
			"temp_blks_read":      int64(0),
			"temp_blks_written":   int64(0),
			"blk_read_time":       stat.BlkReadTime,
			"blk_write_time":      stat.BlkWriteTime,
			"wal_records":         int64(0),
			"wal_fpi":             int64(0),
			"wal_bytes":           int64(0),
		}

		if matchesFilters(row, filters) {
			result.Rows = append(result.Rows, row)
		}
	}

	return result
}
