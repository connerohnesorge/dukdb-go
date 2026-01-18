// Package parser provides SQL parsing for the native Go DuckDB implementation.
package parser

import "strings"

// EnhancedTableRef represents an enhanced table reference with catalog support.
// This is used by the TableExtractor for qualified name resolution.
type EnhancedTableRef struct {
	Catalog string // Optional catalog name (e.g., "main")
	Schema  string // Optional schema name (e.g., "public")
	Table   string // Table name (required)
	Alias   string // Optional alias (not used in output)
}

// QualifiedName returns the qualified name in catalog.schema.table format.
// If catalog is empty, returns schema.table.
// If schema is also empty, returns just table.
func (t EnhancedTableRef) QualifiedName() string {
	parts := []string{}
	if t.Catalog != "" {
		parts = append(parts, t.Catalog)
	}
	if t.Schema != "" {
		parts = append(parts, t.Schema)
	}
	parts = append(parts, t.Table)

	return strings.Join(parts, ".")
}

// String returns a string representation for debugging purposes.
func (t EnhancedTableRef) String() string {
	if t.Catalog != "" {
		return t.Catalog + "." + t.Schema + "." + t.Table
	}
	if t.Schema != "" {
		return t.Schema + "." + t.Table
	}

	return t.Table
}
