// Package catalog provides PostgreSQL-compatible information_schema views
// for dukdb-go. These views allow ORMs, database tools, and applications
// that query PostgreSQL system catalogs to work with dukdb.
//
// The package implements virtual views that dynamically query the dukdb
// catalog and return results formatted according to PostgreSQL's
// information_schema specification.
//
// Supported views:
//   - information_schema.tables - List of tables and views
//   - information_schema.columns - List of columns with metadata
//   - information_schema.schemata - List of schemas
//   - information_schema.views - List of views
//   - information_schema.table_constraints - Primary keys, unique constraints
//   - information_schema.key_column_usage - Columns in constraints
//   - information_schema.sequences - List of sequences
//
// Reference: https://www.postgresql.org/docs/current/information-schema.html
package catalog

import (
	"strings"

	"github.com/dukdb/dukdb-go/internal/catalog"
)

// CatalogProvider is the interface that the dukdb catalog must satisfy
// to provide metadata for information_schema views.
type CatalogProvider interface {
	// ListSchemas returns all schemas in the catalog.
	ListSchemas() []*catalog.Schema

	// GetSchema returns a schema by name.
	GetSchema(name string) (*catalog.Schema, bool)

	// ListTablesInSchema returns all tables in a schema.
	ListTablesInSchema(schemaName string) []*catalog.TableDef

	// GetTableInSchema returns a table from a specific schema.
	GetTableInSchema(schemaName, tableName string) (*catalog.TableDef, bool)

	// GetViewInSchema returns a view from a specific schema.
	GetViewInSchema(schemaName, viewName string) (*catalog.ViewDef, bool)

	// GetIndexesForTable returns all indexes for a table.
	GetIndexesForTable(schemaName, tableName string) []*catalog.IndexDef

	// GetSequenceInSchema returns a sequence from a specific schema.
	GetSequenceInSchema(schemaName, sequenceName string) (*catalog.SequenceDef, bool)
}

// InformationSchema provides information_schema views for a dukdb catalog.
// It implements the logic to query metadata from the catalog and return
// results formatted according to PostgreSQL's information_schema specification.
type InformationSchema struct {
	catalog     CatalogProvider
	databaseName string
}

// NewInformationSchema creates a new InformationSchema instance.
// The databaseName is used for the table_catalog column in all views.
func NewInformationSchema(catalog CatalogProvider, databaseName string) *InformationSchema {
	return &InformationSchema{
		catalog:     catalog,
		databaseName: databaseName,
	}
}

// IsInformationSchemaQuery returns true if the query is selecting from
// information_schema tables.
func IsInformationSchemaQuery(query string) bool {
	upperQuery := strings.ToUpper(query)
	return strings.Contains(upperQuery, "INFORMATION_SCHEMA.")
}

// GetViewName extracts the view name from an information_schema query.
// Returns empty string if not an information_schema query.
func GetViewName(query string) string {
	upperQuery := strings.ToUpper(query)
	idx := strings.Index(upperQuery, "INFORMATION_SCHEMA.")
	if idx == -1 {
		return ""
	}

	// Find the start of the view name
	start := idx + len("INFORMATION_SCHEMA.")
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

// QueryResult represents the result of a metadata query.
type QueryResult struct {
	// Columns is the list of column names in the result.
	Columns []string
	// Rows is the list of rows, where each row is a map of column name to value.
	Rows []map[string]any
}

// Query executes a query against information_schema and returns results.
// This method handles parsing the query to determine which view is being
// queried and any WHERE clause filtering.
//
// Supported queries:
//   - SELECT * FROM information_schema.tables [WHERE ...]
//   - SELECT * FROM information_schema.columns [WHERE ...]
//   - SELECT * FROM information_schema.schemata [WHERE ...]
//   - SELECT * FROM information_schema.views [WHERE ...]
//   - SELECT * FROM information_schema.table_constraints [WHERE ...]
//   - SELECT * FROM information_schema.key_column_usage [WHERE ...]
//   - SELECT * FROM information_schema.sequences [WHERE ...]
//
// Returns nil if the query cannot be handled.
func (is *InformationSchema) Query(query string) *QueryResult {
	viewName := GetViewName(query)
	if viewName == "" {
		return nil
	}

	// Extract WHERE clause filters if present
	filters := parseWhereClause(query)

	switch viewName {
	case "tables":
		return is.queryTables(filters)
	case "columns":
		return is.queryColumns(filters)
	case "schemata":
		return is.querySchemata(filters)
	case "views":
		return is.queryViews(filters)
	case "table_constraints":
		return is.queryTableConstraints(filters)
	case "key_column_usage":
		return is.queryKeyColumnUsage(filters)
	case "sequences":
		return is.querySequences(filters)
	default:
		return nil
	}
}

// Filter represents a simple WHERE clause filter (column = 'value').
type Filter struct {
	Column string
	Value  string
}

// parseWhereClause extracts simple equality filters from a WHERE clause.
// This is a basic parser that handles common ORM patterns like:
//   - WHERE table_name = 'foo'
//   - WHERE table_schema = 'public' AND table_name = 'bar'
func parseWhereClause(query string) []Filter {
	var filters []Filter

	upperQuery := strings.ToUpper(query)
	whereIdx := strings.Index(upperQuery, " WHERE ")
	if whereIdx == -1 {
		return filters
	}

	// Extract the WHERE clause portion
	whereClause := query[whereIdx+7:]

	// Handle ORDER BY, LIMIT, etc. after WHERE
	for _, suffix := range []string{" ORDER BY", " LIMIT", " OFFSET", " GROUP BY", " HAVING"} {
		if idx := strings.Index(strings.ToUpper(whereClause), suffix); idx != -1 {
			whereClause = whereClause[:idx]
		}
	}

	// Split by AND (case-insensitive)
	parts := splitByAnd(whereClause)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Look for column = 'value' pattern
		eqIdx := strings.Index(part, "=")
		if eqIdx == -1 {
			continue
		}

		column := strings.TrimSpace(part[:eqIdx])
		column = strings.ToLower(column)

		value := strings.TrimSpace(part[eqIdx+1:])
		// Remove quotes from value
		value = strings.Trim(value, "'\"")

		filters = append(filters, Filter{
			Column: column,
			Value:  value,
		})
	}

	return filters
}

// splitByAnd splits a string by " AND " (case-insensitive).
func splitByAnd(s string) []string {
	var parts []string
	upper := strings.ToUpper(s)

	start := 0
	for {
		idx := strings.Index(upper[start:], " AND ")
		if idx == -1 {
			parts = append(parts, s[start:])
			break
		}
		parts = append(parts, s[start:start+idx])
		start = start + idx + 5 // len(" AND ")
	}

	return parts
}

// matchesFilters checks if a row matches all filters.
func matchesFilters(row map[string]any, filters []Filter) bool {
	for _, f := range filters {
		val, ok := row[f.Column]
		if !ok {
			return false
		}

		// Compare as strings (case-insensitive for identifiers)
		var strVal string

		switch v := val.(type) {
		case string:
			strVal = v
		case nil:
			if f.Value != "" {
				return false
			}

			continue
		default:
			// For non-string types, use fmt to convert
			strVal = toString(v)
		}

		if !strings.EqualFold(strVal, f.Value) {
			return false
		}
	}

	return true
}

// toString converts a value to string representation.
func toString(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case int:
		return intToString(int64(val))
	case int64:
		return intToString(val)
	case int32:
		return intToString(int64(val))
	case bool:
		if val {
			return "YES"
		}
		return "NO"
	case nil:
		return ""
	default:
		// Fallback - this should rarely happen
		return ""
	}
}

// intToString converts an int64 to string without using fmt.
func intToString(n int64) string {
	if n == 0 {
		return "0"
	}

	negative := n < 0
	if negative {
		n = -n
	}

	var buf [21]byte
	i := len(buf)

	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}

	if negative {
		i--
		buf[i] = '-'
	}

	return string(buf[i:])
}
