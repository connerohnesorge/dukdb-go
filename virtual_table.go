package dukdb

// VirtualTable represents a virtual table that can be queried.
// Virtual tables provide data from external sources (like Arrow RecordReaders)
// without storing the data in the database's storage layer.
type VirtualTable interface {
	// Name returns the table name.
	Name() string

	// Schema returns the table columns as a slice of VirtualColumnDef.
	Schema() []VirtualColumnDef

	// Scan returns an iterator over the table rows.
	// The iterator must be closed after use.
	Scan() (RowIterator, error)
}

// VirtualColumnDef represents a column definition for a virtual table.
type VirtualColumnDef struct {
	// Name is the column name.
	Name string

	// Type is the column data type.
	Type Type

	// TypeInfo provides extended type information for complex types.
	TypeInfo TypeInfo

	// Nullable indicates whether the column allows NULL values.
	Nullable bool
}

// RowIterator iterates over virtual table rows.
type RowIterator interface {
	// Next advances to the next row. Returns false when done.
	Next() bool

	// Values returns the current row values.
	// The returned slice is valid until the next call to Next.
	Values() []any

	// Err returns any error that occurred during iteration.
	Err() error

	// Close releases resources held by the iterator.
	Close() error
}

// VirtualTableRegistry provides methods for registering and managing virtual tables.
// Backends can implement this interface to support virtual table registration.
type VirtualTableRegistry interface {
	// RegisterVirtualTable registers a virtual table.
	// Returns an error if a table with the same name already exists.
	RegisterVirtualTable(vt VirtualTable) error

	// UnregisterVirtualTable removes a virtual table.
	// Returns an error if the table does not exist.
	UnregisterVirtualTable(name string) error

	// GetVirtualTable returns a virtual table by name.
	// Returns nil and false if not found.
	GetVirtualTable(
		name string,
	) (VirtualTable, bool)
}
