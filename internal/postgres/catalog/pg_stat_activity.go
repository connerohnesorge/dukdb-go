// Package catalog provides PostgreSQL-compatible pg_catalog views for dukdb-go.
// This file implements the pg_stat_activity view for monitoring active sessions.
package catalog

import (
	"time"
)

// pg_stat_activity columns - PostgreSQL activity monitoring view.
// Reference: https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ACTIVITY-VIEW
var pgStatActivityColumns = []string{
	"datid",             // OID of the database
	"datname",           // Name of the database
	"pid",               // Process ID of this backend
	"leader_pid",        // Process ID of the parallel group leader (null if not parallel)
	"usesysid",          // OID of the user
	"usename",           // Name of the user
	"application_name",  // Name of the application
	"client_addr",       // IP address of the client
	"client_hostname",   // Hostname of the client (if available)
	"client_port",       // TCP port number of the client
	"backend_start",     // Time when this process started
	"xact_start",        // Time when current transaction started
	"query_start",       // Time when currently active query started
	"state_change",      // Time when state was last changed
	"wait_event_type",   // Type of event the backend is waiting for
	"wait_event",        // Wait event name if waiting
	"state",             // Current state (active, idle, etc.)
	"backend_xid",       // Transaction ID of this backend
	"backend_xmin",      // Minimum active transaction ID
	"query_id",          // Identifier of the currently executing query
	"query",             // Text of the currently executing query
	"backend_type",      // Type of backend (client backend, etc.)
}

// SessionActivityProvider is an interface for getting session activity information.
// The server package implements this interface to provide real-time session data.
type SessionActivityProvider interface {
	// GetActiveSessions returns all active sessions with their current state.
	GetActiveSessions() []*SessionActivity
}

// SessionActivity represents the activity of a single session.
type SessionActivity struct {
	// PID is the process/session ID.
	PID int64

	// DatabaseOID is the OID of the database.
	DatabaseOID int64

	// DatabaseName is the name of the database.
	DatabaseName string

	// UserOID is the OID of the user.
	UserOID int64

	// Username is the name of the user logged in.
	Username string

	// ApplicationName is the name of the application.
	ApplicationName string

	// ClientAddr is the IP address of the client.
	ClientAddr string

	// ClientPort is the TCP port number of the client.
	ClientPort int

	// BackendStart is when the backend process started.
	BackendStart time.Time

	// XactStart is when the current transaction started (nil if not in transaction).
	XactStart *time.Time

	// QueryStart is when the current query started (nil if not executing).
	QueryStart *time.Time

	// StateChange is when the state last changed.
	StateChange time.Time

	// WaitEventType is the type of wait event (nil if not waiting).
	WaitEventType string

	// WaitEvent is the name of the wait event (nil if not waiting).
	WaitEvent string

	// State is the current state (active, idle, idle in transaction, etc.).
	State string

	// Query is the text of the currently executing query.
	Query string

	// QueryID is the identifier of the currently executing query.
	QueryID int64

	// BackendType is the type of backend (client backend, etc.).
	BackendType string
}

// PgStatActivityView handles pg_stat_activity queries.
type PgStatActivityView struct {
	provider     SessionActivityProvider
	databaseName string
}

// NewPgStatActivityView creates a new pg_stat_activity view handler.
func NewPgStatActivityView(provider SessionActivityProvider, databaseName string) *PgStatActivityView {
	return &PgStatActivityView{
		provider:     provider,
		databaseName: databaseName,
	}
}

// Query returns pg_stat_activity data.
func (v *PgStatActivityView) Query(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: pgStatActivityColumns,
		Rows:    make([]map[string]any, 0),
	}

	if v.provider == nil {
		return result
	}

	sessions := v.provider.GetActiveSessions()
	for _, sess := range sessions {
		row := map[string]any{
			"datid":            sess.DatabaseOID,
			"datname":          sess.DatabaseName,
			"pid":              sess.PID,
			"leader_pid":       nil, // No parallel query support
			"usesysid":         sess.UserOID,
			"usename":          sess.Username,
			"application_name": sess.ApplicationName,
			"client_addr":      sess.ClientAddr,
			"client_hostname":  nil, // No reverse DNS
			"client_port":      sess.ClientPort,
			"backend_start":    formatStatActivityTimestamp(sess.BackendStart),
			"xact_start":       formatStatActivityTimestampPtr(sess.XactStart),
			"query_start":      formatStatActivityTimestampPtr(sess.QueryStart),
			"state_change":     formatStatActivityTimestamp(sess.StateChange),
			"wait_event_type":  nilIfEmptyStatActivity(sess.WaitEventType),
			"wait_event":       nilIfEmptyStatActivity(sess.WaitEvent),
			"state":            sess.State,
			"backend_xid":      nil, // No MVCC transaction ID
			"backend_xmin":     nil, // No MVCC min transaction ID
			"query_id":         sess.QueryID,
			"query":            sess.Query,
			"backend_type":     sess.BackendType,
		}

		if matchesFilters(row, filters) {
			result.Rows = append(result.Rows, row)
		}
	}

	return result
}

// formatStatActivityTimestamp formats a time.Time for PostgreSQL output.
func formatStatActivityTimestamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format("2006-01-02 15:04:05.000000-07")
}

// formatStatActivityTimestampPtr formats a *time.Time for PostgreSQL output.
func formatStatActivityTimestampPtr(t *time.Time) any {
	if t == nil {
		return nil
	}
	return formatStatActivityTimestamp(*t)
}

// nilIfEmptyStatActivity returns nil if the string is empty, otherwise the string.
func nilIfEmptyStatActivity(s string) any {
	if s == "" {
		return nil
	}
	return s
}
