package catalog

import "strings"

// ViewDef represents a view definition in the catalog.
type ViewDef struct {
	// Name is the view name.
	Name string

	// Schema is the schema name.
	Schema string

	// Query is the serialized SELECT statement that defines the view.
	Query string

	// TableDependencies lists the tables this view depends on.
	// Each entry is a table name (lowercased for case-insensitive comparison).
	// These are unqualified table names within the same schema.
	TableDependencies []string
}

// NewViewDef creates a new ViewDef instance.
func NewViewDef(name, schema, query string) *ViewDef {
	return &ViewDef{
		Name:              name,
		Schema:            schema,
		Query:             query,
		TableDependencies: nil,
	}
}

// NewViewDefWithDependencies creates a new ViewDef instance with table dependencies.
func NewViewDefWithDependencies(name, schema, query string, tableDeps []string) *ViewDef {
	// Normalize table names for case-insensitive comparison
	normalizedDeps := make([]string, len(tableDeps))
	for i, dep := range tableDeps {
		normalizedDeps[i] = strings.ToLower(dep)
	}
	return &ViewDef{
		Name:              name,
		Schema:            schema,
		Query:             query,
		TableDependencies: normalizedDeps,
	}
}

// DependsOnTable checks if this view depends on the given table (case-insensitive).
func (v *ViewDef) DependsOnTable(tableName string) bool {
	normalizedName := strings.ToLower(tableName)
	for _, dep := range v.TableDependencies {
		if dep == normalizedName {
			return true
		}
	}
	return false
}
