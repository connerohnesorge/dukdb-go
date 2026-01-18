// Package server provides PostgreSQL wire protocol compatibility for dukdb-go.
// This file implements catalog query handling for information_schema and pg_catalog.
package server

import (
	"context"
	"regexp"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	internalcatalog "github.com/dukdb/dukdb-go/internal/catalog"
	pgcatalog "github.com/dukdb/dukdb-go/internal/postgres/catalog"
	wire "github.com/jeroenrinzema/psql-wire"
)

// CatalogHandler handles queries to system catalog views.
// It intercepts queries to information_schema and pg_catalog and
// returns results from the dukdb catalog metadata.
type CatalogHandler struct {
	infoSchema *pgcatalog.InformationSchema
	pgCatalog  *pgcatalog.PgCatalog
	provider   pgcatalog.CatalogProvider
	dbName     string
}

// NewCatalogHandler creates a new catalog handler with the given catalog provider.
func NewCatalogHandler(provider pgcatalog.CatalogProvider, dbName string) *CatalogHandler {
	return &CatalogHandler{
		infoSchema: pgcatalog.NewInformationSchema(provider, dbName),
		pgCatalog:  pgcatalog.NewPgCatalog(provider, dbName),
		provider:   provider,
		dbName:     dbName,
	}
}

// IsCatalogQuery returns true if the query is for information_schema or pg_catalog.
func IsCatalogQuery(query string) bool {
	return pgcatalog.IsInformationSchemaQuery(query) || pgcatalog.IsPgCatalogQuery(query)
}

// ExecuteCatalogQuery executes a query against information_schema or pg_catalog
// and writes results to the DataWriter.
func (ch *CatalogHandler) ExecuteCatalogQuery(
	_ context.Context,
	query string,
	writer wire.DataWriter,
) error {
	var result *pgcatalog.QueryResult

	if pgcatalog.IsInformationSchemaQuery(query) {
		result = ch.infoSchema.Query(query)
	} else if pgcatalog.IsPgCatalogQuery(query) {
		result = ch.pgCatalog.Query(query)
	}

	if result == nil {
		// Query not handled by catalog views - return empty result
		return writer.Empty()
	}

	// Handle empty result set
	if len(result.Rows) == 0 {
		return writer.Empty()
	}

	// Write each row
	for _, row := range result.Rows {
		values := make([]any, len(result.Columns))
		for i, col := range result.Columns {
			values[i] = row[col]
		}
		if err := writer.Row(values); err != nil {
			return err
		}
	}

	return writer.Complete("SELECT " + itoa(len(result.Rows)))
}

// catalogProviderAdapter wraps the internal catalog to implement pgcatalog.CatalogProvider.
type catalogProviderAdapter struct {
	catalog *internalcatalog.Catalog
}

// newCatalogProviderAdapter creates a new adapter from the internal catalog.
func newCatalogProviderAdapter(cat *internalcatalog.Catalog) *catalogProviderAdapter {
	return &catalogProviderAdapter{catalog: cat}
}

// ListSchemas returns all schemas in the catalog.
func (a *catalogProviderAdapter) ListSchemas() []*internalcatalog.Schema {
	return a.catalog.ListSchemas()
}

// GetSchema returns a schema by name.
func (a *catalogProviderAdapter) GetSchema(name string) (*internalcatalog.Schema, bool) {
	return a.catalog.GetSchema(name)
}

// ListTablesInSchema returns all tables in a schema.
func (a *catalogProviderAdapter) ListTablesInSchema(schemaName string) []*internalcatalog.TableDef {
	return a.catalog.ListTablesInSchema(schemaName)
}

// GetTableInSchema returns a table from a specific schema.
func (a *catalogProviderAdapter) GetTableInSchema(
	schemaName, tableName string,
) (*internalcatalog.TableDef, bool) {
	return a.catalog.GetTableInSchema(schemaName, tableName)
}

// GetViewInSchema returns a view from a specific schema.
func (a *catalogProviderAdapter) GetViewInSchema(
	schemaName, viewName string,
) (*internalcatalog.ViewDef, bool) {
	return a.catalog.GetViewInSchema(schemaName, viewName)
}

// GetIndexesForTable returns all indexes for a table.
func (a *catalogProviderAdapter) GetIndexesForTable(
	schemaName, tableName string,
) []*internalcatalog.IndexDef {
	return a.catalog.GetIndexesForTable(schemaName, tableName)
}

// GetSequenceInSchema returns a sequence from a specific schema.
func (a *catalogProviderAdapter) GetSequenceInSchema(
	schemaName, sequenceName string,
) (*internalcatalog.SequenceDef, bool) {
	return a.catalog.GetSequenceInSchema(schemaName, sequenceName)
}

// Regular expressions for pg_* function matching
var (
	// Matches: pg_get_userbyid(oid) or pg_catalog.pg_get_userbyid(oid)
	pgGetUserByIdRe = regexp.MustCompile(`(?i)(?:pg_catalog\.)?pg_get_userbyid\s*\(\s*\d+\s*\)`)

	// Matches: pg_encoding_to_char(int) or pg_catalog.pg_encoding_to_char(int)
	pgEncodingToCharRe = regexp.MustCompile(
		`(?i)(?:pg_catalog\.)?pg_encoding_to_char\s*\(\s*\d+\s*\)`,
	)

	// Matches: current_database()
	currentDatabaseRe = regexp.MustCompile(`(?i)current_database\s*\(\s*\)`)

	// Matches: current_schema()
	currentSchemaRe = regexp.MustCompile(`(?i)current_schema\s*\(\s*\)`)

	// Matches: current_user
	currentUserRe = regexp.MustCompile(`(?i)\bcurrent_user\b`)

	// Matches: session_user
	sessionUserRe = regexp.MustCompile(`(?i)\bsession_user\b`)

	// Matches: version()
	versionRe = regexp.MustCompile(`(?i)\bversion\s*\(\s*\)`)

	// Matches: pg_backend_pid()
	pgBackendPidRe = regexp.MustCompile(`(?i)pg_backend_pid\s*\(\s*\)`)

	// Matches: pg_is_in_recovery()
	pgIsInRecoveryRe = regexp.MustCompile(`(?i)pg_is_in_recovery\s*\(\s*\)`)
)

// IsPgFunctionQuery returns true if the query contains PostgreSQL system functions.
func IsPgFunctionQuery(query string) bool {
	return pgGetUserByIdRe.MatchString(query) ||
		pgEncodingToCharRe.MatchString(query) ||
		currentDatabaseRe.MatchString(query) ||
		currentSchemaRe.MatchString(query) ||
		currentUserRe.MatchString(query) ||
		sessionUserRe.MatchString(query) ||
		versionRe.MatchString(query) ||
		pgBackendPidRe.MatchString(query) ||
		pgIsInRecoveryRe.MatchString(query)
}

// RewritePgFunctions rewrites PostgreSQL system function calls to literal values.
// This allows queries like "SELECT current_database()" to work.
func RewritePgFunctions(query, dbName, username, serverVersion string, sessionID uint64) string {
	result := query

	// Replace pg_get_userbyid(oid) with username
	result = pgGetUserByIdRe.ReplaceAllString(result, "'"+username+"'")

	// Replace pg_encoding_to_char(int) with 'UTF8'
	result = pgEncodingToCharRe.ReplaceAllString(result, "'UTF8'")

	// Replace current_database() with database name
	result = currentDatabaseRe.ReplaceAllString(result, "'"+dbName+"'")

	// Replace current_schema() with 'public' (PostgreSQL default)
	result = currentSchemaRe.ReplaceAllString(result, "'public'")

	// Replace current_user with username
	result = currentUserRe.ReplaceAllString(result, "'"+username+"'")

	// Replace session_user with username
	result = sessionUserRe.ReplaceAllString(result, "'"+username+"'")

	// Replace version() with PostgreSQL-compatible version string
	pgVersion := "PostgreSQL " + serverVersion + " (dukdb-go compatible)"
	result = versionRe.ReplaceAllString(result, "'"+pgVersion+"'")

	// Replace pg_backend_pid() with session ID
	result = pgBackendPidRe.ReplaceAllString(result, itoa64(int64(sessionID)))

	// Replace pg_is_in_recovery() with false
	result = pgIsInRecoveryRe.ReplaceAllString(result, "false")

	return result
}

// getCatalogFromConn extracts the internal catalog from a BackendConn if available.
func getCatalogFromConn(conn dukdb.BackendConn) *internalcatalog.Catalog {
	if conn == nil {
		return nil
	}

	// Try to get catalog via the BackendConnCatalog interface
	if catalogConn, ok := conn.(dukdb.BackendConnCatalog); ok {
		cat := catalogConn.GetCatalog()
		if internalCat, ok := cat.(*internalcatalog.Catalog); ok {
			return internalCat
		}
	}

	return nil
}

// initCatalogHandler initializes the catalog handler for a server.
// Returns nil if the catalog is not available.
func initCatalogHandler(conn dukdb.BackendConn, dbName string) *CatalogHandler {
	cat := getCatalogFromConn(conn)
	if cat == nil {
		return nil
	}

	adapter := newCatalogProviderAdapter(cat)

	return NewCatalogHandler(adapter, dbName)
}

// SetMonitoringProviders sets the monitoring providers for pg_catalog views.
// This allows pg_stat_activity, pg_stat_statements, and pg_locks to work.
func (ch *CatalogHandler) SetMonitoringProviders(
	activityProvider pgcatalog.SessionActivityProvider,
	statementsProvider pgcatalog.StatementStatsProvider,
	lockProvider pgcatalog.LockProvider,
) {
	if ch.pgCatalog != nil {
		ch.pgCatalog.SetActivityProvider(activityProvider)
		ch.pgCatalog.SetStatementsProvider(statementsProvider)
		ch.pgCatalog.SetLockProvider(lockProvider)
	}
}

// IsSelectFromDual returns true if the query is "SELECT ... FROM dual" or similar.
// These are Oracle/MySQL compatibility queries that some tools send.
func IsSelectFromDual(query string) bool {
	upperQuery := strings.ToUpper(strings.TrimSpace(query))

	return strings.Contains(upperQuery, "FROM DUAL")
}

// HandleSelectFromDual rewrites Oracle/MySQL "FROM dual" to a simple SELECT.
func HandleSelectFromDual(query string) string {
	// Simple regex replacement: remove "FROM dual" (case-insensitive)
	re := regexp.MustCompile(`(?i)\s+FROM\s+dual\b`)

	return re.ReplaceAllString(query, "")
}
