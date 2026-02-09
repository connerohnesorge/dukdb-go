// Package dukdb provides a pure Go implementation of a DuckDB-compatible database driver.
package dukdb

import (
	"database/sql"
	"fmt"
)

// GetTableNames extracts table names referenced in a SQL query.
//
// This function parses the given SQL query and returns a sorted, deduplicated list
// of table names that are referenced in the query. The conn parameter is used to
// access the underlying backend connection for query parsing.
//
// # Supported Statement Types
//
// The following statement types are fully supported:
//   - SELECT (including JOINs, subqueries in WHERE/HAVING, scalar subqueries, CTEs)
//   - INSERT (target table and SELECT subquery if present)
//   - UPDATE (target table and WHERE clause subqueries)
//   - DELETE (target table and WHERE clause subqueries)
//   - CREATE TABLE (the table being created)
//   - DROP TABLE (the table being dropped)
//   - CREATE TABLE AS SELECT (target table and SELECT source tables, including CTEs)
//
// # Not Supported
//
// The following features are NOT currently supported:
//   - UNION/INTERSECT/EXCEPT queries - parsed as single SELECT
//   - UPDATE ... FROM - requires AST enhancement for FROM clause in UPDATE
//   - Recursive CTEs - backend limitation
//
// # Edge Cases
//
//   - Empty query: returns []string{}, nil (not nil slice)
//   - Query with no tables (e.g., "SELECT 1"): returns []string{}, nil
//   - Table functions (e.g., read_parquet()): excluded (not real table references)
//   - Multiple statements: only the first statement is processed
//   - Parse errors: returns nil, error with "parse error:" prefix
//
// # Qualified Names
//
// When qualified is false, only the unqualified table name is returned (e.g., "users").
// When qualified is true, the full qualified name is returned (e.g., "main.public.users").
//
// # Examples
//
// Basic usage:
//
//	tables, err := dukdb.GetTableNames(conn, "SELECT * FROM users", false)
//	// tables = []string{"users"}
//
// Multiple tables with JOINs:
//
//	tables, err := dukdb.GetTableNames(conn, "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id", false)
//	// tables = []string{"customers", "orders"} (sorted alphabetically)
//
// Qualified names with schema:
//
//	tables, err := dukdb.GetTableNames(conn, "SELECT * FROM public.users", true)
//	// tables = []string{"public.users"}
//
// Subquery extraction:
//
//	tables, err := dukdb.GetTableNames(conn, "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers)", false)
//	// tables = []string{"customers", "orders"}
//
// Parameters:
//   - conn: A *sql.Conn instance. Used to access the underlying backend connection.
//   - query: The SQL query string to parse.
//   - qualified: If true, returns qualified table names (schema.table); if false, returns unqualified names.
//
// Returns:
//   - A sorted, deduplicated slice of table names, or an empty slice if none are found.
//   - An error if the query cannot be parsed or if the backend doesn't support table extraction.
func GetTableNames(conn *sql.Conn, query string, qualified bool) ([]string, error) {
	var tables []string
	var extractErr error

	err := conn.Raw(func(driverConn any) error {
		// Try to get the backend connection that supports table extraction
		if extractor, ok := driverConn.(BackendConnTableExtractor); ok {
			tables, extractErr = extractor.ExtractTableNames(query, qualified)

			return nil
		}

		// If the driver connection implements an interface with access to the backend
		// Check if it's our Conn type
		if dukdbConn, ok := driverConn.(*Conn); ok {
			if dukdbConn.backendConn != nil {
				if extractor, ok := dukdbConn.backendConn.(BackendConnTableExtractor); ok {
					tables, extractErr = extractor.ExtractTableNames(query, qualified)

					return nil
				}
			}
		}

		extractErr = fmt.Errorf("parse error: backend does not support table extraction")

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	if extractErr != nil {
		// Wrap parse errors with "parse error:" prefix for API compatibility
		errStr := extractErr.Error()
		if errStr != "" && errStr[0] >= 'A' && errStr[0] <= 'Z' {
			// Error message starts with uppercase, likely a structured error
			return nil, fmt.Errorf("parse error: %w", extractErr)
		}

		return nil, extractErr
	}

	// Ensure we never return nil - return empty slice instead
	if tables == nil {
		return []string{}, nil
	}

	return tables, nil
}
