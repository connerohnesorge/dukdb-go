// Package catalog provides PostgreSQL-compatible pg_catalog views for dukdb-go.
// These views allow psql, ORMs, database tools, and applications that query
// PostgreSQL system catalogs to work with dukdb.
//
// The package implements virtual views that dynamically query the dukdb
// catalog and return results formatted according to PostgreSQL's pg_catalog
// specification.
//
// Supported pg_catalog views:
//   - pg_catalog.pg_namespace - Schemas/namespaces
//   - pg_catalog.pg_class - Tables, views, indexes, sequences
//   - pg_catalog.pg_attribute - Table columns
//   - pg_catalog.pg_type - Data types
//   - pg_catalog.pg_index - Index information
//   - pg_catalog.pg_database - Databases
//   - pg_catalog.pg_settings - Server configuration
//   - pg_catalog.pg_tables - Simplified table listing
//   - pg_catalog.pg_views - Simplified view listing
//   - pg_catalog.pg_proc - Functions and procedures
//   - pg_catalog.pg_constraint - Table constraints
//   - pg_catalog.pg_operator - Operators
//   - pg_catalog.pg_aggregate - Aggregate functions
//   - pg_catalog.pg_trigger - Triggers (empty, for compatibility)
//   - pg_catalog.pg_extension - Extensions
//   - pg_catalog.pg_roles - Database roles
//   - pg_catalog.pg_user - Database users
//   - pg_catalog.pg_stat_activity - Active sessions (monitoring)
//   - pg_catalog.pg_stat_statements - Statement statistics (monitoring)
//   - pg_catalog.pg_locks - Lock information (monitoring)
//
// Reference: https://www.postgresql.org/docs/current/catalogs.html
package catalog

import (
	"hash/fnv"
	"strings"
)

// PgCatalog provides pg_catalog views for a dukdb catalog.
// It implements the logic to query metadata from the catalog and return
// results formatted according to PostgreSQL's pg_catalog specification.
type PgCatalog struct {
	catalog      CatalogProvider
	databaseName string

	// Monitoring providers (optional - can be nil)
	activityProvider   SessionActivityProvider
	statementsProvider StatementStatsProvider
	lockProvider       LockProvider
}

// NewPgCatalog creates a new PgCatalog instance.
// The databaseName is used for the database-related views.
func NewPgCatalog(catalog CatalogProvider, databaseName string) *PgCatalog {
	return &PgCatalog{
		catalog:      catalog,
		databaseName: databaseName,
	}
}

// SetActivityProvider sets the provider for pg_stat_activity.
func (pg *PgCatalog) SetActivityProvider(provider SessionActivityProvider) {
	pg.activityProvider = provider
}

// SetStatementsProvider sets the provider for pg_stat_statements.
func (pg *PgCatalog) SetStatementsProvider(provider StatementStatsProvider) {
	pg.statementsProvider = provider
}

// SetLockProvider sets the provider for pg_locks.
func (pg *PgCatalog) SetLockProvider(provider LockProvider) {
	pg.lockProvider = provider
}

// IsPgCatalogQuery returns true if the query is selecting from
// pg_catalog tables.
func IsPgCatalogQuery(query string) bool {
	upperQuery := strings.ToUpper(query)
	return strings.Contains(upperQuery, "PG_CATALOG.")
}

// GetPgCatalogViewName extracts the view name from a pg_catalog query.
// Returns empty string if not a pg_catalog query.
func GetPgCatalogViewName(query string) string {
	upperQuery := strings.ToUpper(query)
	idx := strings.Index(upperQuery, "PG_CATALOG.")
	if idx == -1 {
		return ""
	}

	// Find the start of the view name
	start := idx + len("PG_CATALOG.")
	if start >= len(upperQuery) {
		return ""
	}

	// Find the end of the view name (next space or special character)
	end := start
	for end < len(upperQuery) {
		c := upperQuery[end]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' ||
			c == ';' || c == ',' || c == ')' || c == '\'' {
			break
		}
		end++
	}

	if end <= start {
		return ""
	}

	return strings.ToLower(upperQuery[start:end])
}

// Query executes a query against pg_catalog and returns results.
// This method handles parsing the query to determine which view is being
// queried and any WHERE clause filtering.
//
// Supported queries:
//   - SELECT * FROM pg_catalog.pg_namespace [WHERE ...]
//   - SELECT * FROM pg_catalog.pg_class [WHERE ...]
//   - SELECT * FROM pg_catalog.pg_attribute [WHERE ...]
//   - SELECT * FROM pg_catalog.pg_type [WHERE ...]
//   - SELECT * FROM pg_catalog.pg_index [WHERE ...]
//   - SELECT * FROM pg_catalog.pg_database [WHERE ...]
//   - SELECT * FROM pg_catalog.pg_settings [WHERE ...]
//   - SELECT * FROM pg_catalog.pg_tables [WHERE ...]
//   - SELECT * FROM pg_catalog.pg_views [WHERE ...]
//
// Returns nil if the query cannot be handled.
func (pg *PgCatalog) Query(query string) *QueryResult {
	viewName := GetPgCatalogViewName(query)
	if viewName == "" {
		return nil
	}

	// Extract WHERE clause filters if present
	filters := parseWhereClause(query)

	switch viewName {
	case "pg_namespace":
		return pg.queryPgNamespace(filters)
	case "pg_class":
		return pg.queryPgClass(filters)
	case "pg_attribute":
		return pg.queryPgAttribute(filters)
	case "pg_type":
		return pg.queryPgType(filters)
	case "pg_index":
		return pg.queryPgIndex(filters)
	case "pg_database":
		return pg.queryPgDatabase(filters)
	case "pg_settings":
		return pg.queryPgSettings(filters)
	case "pg_tables":
		return pg.queryPgTables(filters)
	case "pg_views":
		return pg.queryPgViews(filters)
	case "pg_proc":
		return pg.queryPgProc(filters)
	case "pg_constraint":
		return pg.queryPgConstraint(filters)
	case "pg_operator":
		return pg.queryPgOperator(filters)
	case "pg_aggregate":
		return pg.queryPgAggregate(filters)
	case "pg_trigger":
		return pg.queryPgTrigger(filters)
	case "pg_extension":
		return pg.queryPgExtension(filters)
	case "pg_roles":
		return pg.queryPgRoles(filters)
	case "pg_user":
		return pg.queryPgUser(filters)
	case "pg_stat_activity":
		return pg.queryPgStatActivity(filters)
	case "pg_stat_statements":
		return pg.queryPgStatStatements(filters)
	case "pg_locks":
		return pg.queryPgLocks(filters)
	default:
		return nil
	}
}

// queryPgStatActivity returns data for pg_catalog.pg_stat_activity.
func (pg *PgCatalog) queryPgStatActivity(filters []Filter) *QueryResult {
	view := NewPgStatActivityView(pg.activityProvider, pg.databaseName)
	return view.Query(filters)
}

// queryPgStatStatements returns data for pg_catalog.pg_stat_statements.
func (pg *PgCatalog) queryPgStatStatements(filters []Filter) *QueryResult {
	view := NewPgStatStatementsView(pg.statementsProvider, pg.databaseName)
	return view.Query(filters)
}

// queryPgLocks returns data for pg_catalog.pg_locks.
func (pg *PgCatalog) queryPgLocks(filters []Filter) *QueryResult {
	view := NewPgLocksView(pg.lockProvider)
	return view.Query(filters)
}

const (
	// OID ranges (per spec)
	systemTableOIDBase int64 = 1000
	systemTableOIDSpan int64 = 1000

	namespaceOIDBase int64 = 2000
	namespaceOIDSpan int64 = 1000

	userTypeOIDBase int64 = 10000
	userTypeOIDSpan int64 = 1000000

	userRelationOIDBase int64 = 16000
	userRelationOIDSpan int64 = 1000000

	userIndexOIDBase int64 = 26000
	userIndexOIDSpan int64 = 1000000

	userSequenceOIDBase int64 = 36000
	userSequenceOIDSpan int64 = 1000000

	userViewOIDBase int64 = 46000
	userViewOIDSpan int64 = 1000000

	userConstraintOIDBase int64 = 56000
	userConstraintOIDSpan int64 = 1000000

	databaseOIDBase int64 = 16384
	databaseOIDSpan int64 = 1000000
)

// Well-known namespace OIDs (kept in the namespace range)
const (
	pgCatalogNamespaceOID         int64 = 2000
	informationSchemaNamespaceOID int64 = 2001
	publicNamespaceOID            int64 = 2200
)

func hashToRange(value string, base, span int64) int64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(value))
	return base + int64(h.Sum32()%uint32(span))
}

func hashToRangeAvoiding(value string, base, span int64, reserved map[int64]struct{}) int64 {
	start := hashToRange(value, base, span)
	if _, ok := reserved[start]; !ok {
		return start
	}

	for i := int64(1); i < span; i++ {
		candidate := base + ((start - base + i) % span)
		if _, ok := reserved[candidate]; !ok {
			return candidate
		}
	}

	return start
}

// generateOID generates a synthetic OID from a string using FNV hash.
// Prefixes select deterministic ranges for different object categories.
func generateOID(s string) int64 {
	prefix := s
	if idx := strings.Index(s, ":"); idx != -1 {
		prefix = s[:idx]
	}

	switch prefix {
	case "namespace":
		reserved := map[int64]struct{}{
			pgCatalogNamespaceOID:         {},
			informationSchemaNamespaceOID: {},
			publicNamespaceOID:            {},
		}
		return hashToRangeAvoiding(s, namespaceOIDBase, namespaceOIDSpan, reserved)
	case "table":
		return hashToRange(s, userRelationOIDBase, userRelationOIDSpan)
	case "view":
		return hashToRange(s, userViewOIDBase, userViewOIDSpan)
	case "index":
		return hashToRange(s, userIndexOIDBase, userIndexOIDSpan)
	case "sequence":
		return hashToRange(s, userSequenceOIDBase, userSequenceOIDSpan)
	case "type":
		return hashToRange(s, userTypeOIDBase, userTypeOIDSpan)
	case "constraint":
		return hashToRange(s, userConstraintOIDBase, userConstraintOIDSpan)
	case "database":
		return hashToRange(s, databaseOIDBase, databaseOIDSpan)
	default:
		return hashToRange(s, userRelationOIDBase, userRelationOIDSpan)
	}
}

func getSystemRelationOID(name string) int64 {
	return hashToRange(name, systemTableOIDBase, systemTableOIDSpan)
}

// getNamespaceOID returns the OID for a schema name.
// Uses well-known OIDs for system schemas, synthetic OIDs for user schemas.
func getNamespaceOID(schemaName string) int64 {
	switch strings.ToLower(schemaName) {
	case "pg_catalog":
		return pgCatalogNamespaceOID
	case "information_schema":
		return informationSchemaNamespaceOID
	case "public", "main":
		return publicNamespaceOID
	default:
		return generateOID("namespace:" + schemaName)
	}
}
