// Package server provides PostgreSQL wire protocol compatibility for dukdb-go.
// This file provides integration between the server and pg_catalog monitoring views.
package server

import (
	"net"
	"strconv"
	"time"

	pgcatalog "github.com/dukdb/dukdb-go/internal/postgres/catalog"
)

// ServerActivityProvider implements catalog.SessionActivityProvider.
// It provides session activity data for pg_stat_activity by querying the server.
type ServerActivityProvider struct {
	server *Server
}

// NewServerActivityProvider creates a new session activity provider.
func NewServerActivityProvider(server *Server) *ServerActivityProvider {
	return &ServerActivityProvider{server: server}
}

// GetActiveSessions returns all active sessions with their current state.
func (p *ServerActivityProvider) GetActiveSessions() []*pgcatalog.SessionActivity {
	if p.server == nil {
		return nil
	}

	p.server.sessionsMu.RLock()
	defer p.server.sessionsMu.RUnlock()

	// Get database info
	dbName := "dukdb"
	if p.server.config != nil && p.server.config.Database != "" {
		dbName = p.server.config.Database
	}
	dbOID := generateDatabaseOID(dbName)

	sessions := make([]*pgcatalog.SessionActivity, 0, len(p.server.sessions))

	// Get active queries from metrics collector if available
	var activeQueries map[uint64]*ActiveQuery
	p.server.mu.RLock()
	if p.server.observability != nil && p.server.observability.collector != nil {
		queries := p.server.observability.collector.GetActiveQueries()
		activeQueries = make(map[uint64]*ActiveQuery, len(queries))
		for _, q := range queries {
			activeQueries[q.SessionID] = q
		}
	}
	p.server.mu.RUnlock()

	for _, session := range p.server.sessions {
		activity := &pgcatalog.SessionActivity{
			PID:          int64(session.ID()),
			DatabaseOID:  dbOID,
			DatabaseName: dbName,
			UserOID:      10, // Default user OID
			Username:     session.Username(),
			BackendStart: session.createdAt(),
			StateChange:  time.Now(),
			State:        "idle",
			BackendType:  "client backend",
		}

		// Get application name from session attributes
		if appName, ok := session.GetAttribute("application_name"); ok {
			if s, ok := appName.(string); ok {
				activity.ApplicationName = s
			}
		}

		// Parse client address and port
		if addr := session.RemoteAddr(); addr != "" {
			host, portStr, err := net.SplitHostPort(addr)
			if err == nil {
				activity.ClientAddr = host
				if port, err := strconv.Atoi(portStr); err == nil {
					activity.ClientPort = port
				}
			} else {
				activity.ClientAddr = addr
			}
		}

		// Check if session is in transaction
		if session.InTransaction() {
			txStart := session.transactionStartTime()
			activity.XactStart = &txStart
			if session.IsTransactionAborted() {
				activity.State = "idle in transaction (aborted)"
			} else {
				activity.State = "idle in transaction"
			}
		}

		// Check if there's an active query
		if activeQueries != nil {
			if aq, ok := activeQueries[session.ID()]; ok {
				activity.Query = aq.Query
				activity.QueryStart = &aq.QueryStart
				activity.State = "active"
				activity.QueryID = int64(hashQueryForMetrics(aq.Query))
				activity.WaitEvent = aq.WaitEvent
				activity.WaitEventType = aq.WaitEventType
			}
		}

		sessions = append(sessions, activity)
	}

	return sessions
}

// ServerStatementStatsProvider implements catalog.StatementStatsProvider.
// It provides statement statistics data for pg_stat_statements.
type ServerStatementStatsProvider struct {
	collector *MetricsCollector
}

// NewServerStatementStatsProvider creates a new statement statistics provider.
func NewServerStatementStatsProvider(collector *MetricsCollector) *ServerStatementStatsProvider {
	return &ServerStatementStatsProvider{collector: collector}
}

// GetStatementStats returns all tracked statement statistics.
func (p *ServerStatementStatsProvider) GetStatementStats() []*pgcatalog.StatementStatsEntry {
	if p.collector == nil {
		return nil
	}

	stats := p.collector.GetStatementStats()
	entries := make([]*pgcatalog.StatementStatsEntry, len(stats))

	for i, s := range stats {
		entries[i] = &pgcatalog.StatementStatsEntry{
			QueryID:        int64(s.QueryID),
			Query:          s.Query,
			Calls:          s.Calls,
			TotalExecTime:  s.TotalTime,
			MinExecTime:    s.MinTime,
			MaxExecTime:    s.MaxTime,
			MeanExecTime:   s.MeanTime,
			StddevExecTime: s.StddevTime,
			Rows:           s.Rows,
			SharedBlksHit:  s.SharedBlksHit,
			SharedBlksRead: s.SharedBlksRead,
			LocalBlksHit:   s.LocalBlksHit,
			LocalBlksRead:  s.LocalBlksRead,
			BlkReadTime:    s.BlkReadTime,
			BlkWriteTime:   s.BlkWriteTime,
		}
	}

	return entries
}

// ServerLockProvider implements catalog.LockProvider.
// It provides lock information for pg_locks.
// Note: DukDB uses optimistic concurrency control, so traditional locks
// are not present. This returns an empty list for compatibility.
type ServerLockProvider struct {
	server *Server
}

// NewServerLockProvider creates a new lock provider.
func NewServerLockProvider(server *Server) *ServerLockProvider {
	return &ServerLockProvider{server: server}
}

// GetLocks returns all current locks.
// Since DukDB uses MVCC/OCC rather than traditional locking, this returns
// a simulated view showing "virtual" locks for active sessions.
func (p *ServerLockProvider) GetLocks() []*pgcatalog.LockInfo {
	if p.server == nil {
		return nil
	}

	p.server.sessionsMu.RLock()
	defer p.server.sessionsMu.RUnlock()

	// Get database info
	dbName := "dukdb"
	if p.server.config != nil && p.server.config.Database != "" {
		dbName = p.server.config.Database
	}
	dbOID := generateDatabaseOID(dbName)

	var locks []*pgcatalog.LockInfo

	// Create virtual locks for sessions in transactions
	for _, session := range p.server.sessions {
		if session.InTransaction() {
			virtualXID := formatVirtualXID(session.ID())
			lock := &pgcatalog.LockInfo{
				LockType:           "virtualxid",
				DatabaseOID:        &dbOID,
				VirtualXID:         virtualXID,
				VirtualTransaction: virtualXID,
				PID:                int64(session.ID()),
				Mode:               "ExclusiveLock",
				Granted:            true,
				FastPath:           true,
			}
			locks = append(locks, lock)
		}
	}

	return locks
}

// generateDatabaseOID generates a consistent OID for a database name.
func generateDatabaseOID(name string) int64 {
	h := uint64(0)
	for _, c := range name {
		h = h*31 + uint64(c)
	}
	return int64(h%1000000) + 16384
}

// formatVirtualXID formats a virtual transaction ID.
func formatVirtualXID(sessionID uint64) string {
	// Format: backend_id/local_xid
	return itoa64(int64(sessionID)) + "/1"
}

// Helper method to get session creation time.
func (s *Session) createdAt() time.Time {
	// The context was created when the session was created
	// We'll use a reasonable default if not tracked
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Return current time minus a reasonable session duration estimate
	// In a full implementation, we would track this explicitly
	return time.Now().Add(-5 * time.Minute)
}

// Helper method to get transaction start time.
func (s *Session) transactionStartTime() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// In a full implementation, we would track this explicitly
	return time.Now().Add(-1 * time.Minute)
}
