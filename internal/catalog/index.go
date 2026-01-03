package catalog

// IndexDef represents an index definition in the catalog.
type IndexDef struct {
	// Name is the index name.
	Name string

	// Schema is the schema name.
	Schema string

	// Table is the name of the table being indexed.
	Table string

	// Columns is the list of column names included in the index.
	Columns []string

	// IsUnique indicates whether this is a unique index.
	IsUnique bool

	// IsPrimary indicates whether this is a primary key index.
	IsPrimary bool
}

// NewIndexDef creates a new IndexDef instance.
func NewIndexDef(name, schema, table string, columns []string, isUnique bool) *IndexDef {
	return &IndexDef{
		Name:      name,
		Schema:    schema,
		Table:     table,
		Columns:   columns,
		IsUnique:  isUnique,
		IsPrimary: false,
	}
}
