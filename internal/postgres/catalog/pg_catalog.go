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
}

// NewPgCatalog creates a new PgCatalog instance.
// The databaseName is used for the database-related views.
func NewPgCatalog(catalog CatalogProvider, databaseName string) *PgCatalog {
	return &PgCatalog{
		catalog:      catalog,
		databaseName: databaseName,
	}
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
	default:
		return nil
	}
}

// generateOID generates a synthetic OID from a string using FNV hash.
// This provides consistent OIDs for objects based on their names.
func generateOID(s string) int64 {
	h := fnv.New32a()
	h.Write([]byte(s))
	// Use a base of 16384 to avoid conflicts with PostgreSQL built-in OIDs
	return int64(h.Sum32())%1000000 + 16384
}

// Well-known PostgreSQL OIDs for built-in namespaces
const (
	pgCatalogNamespaceOID        int64 = 11
	informationSchemaNamespaceOID int64 = 13
	publicNamespaceOID           int64 = 2200
)

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
