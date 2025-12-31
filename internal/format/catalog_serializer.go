package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/dukdb/dukdb-go/internal/catalog"
)

// WriteHeader writes the DuckDB binary file header to the writer.
//
// Binary Format:
//   - Magic Number: 4 bytes (0x4455434B = "DUCK")
//   - Version: 8 bytes (64 for DuckDB v1.1.3)
//
// All multi-byte values are written in little-endian byte order.
//
// This function should be called once at the beginning of a DuckDB file.
func WriteHeader(w io.Writer) error {
	// Write magic number (4 bytes)
	if err := binary.Write(w, ByteOrder, uint32(DuckDBMagicNumber)); err != nil {
		return fmt.Errorf(
			"failed to write magic number: %w",
			err,
		)
	}

	// Write format version (8 bytes)
	if err := binary.Write(w, ByteOrder, uint64(DuckDBFormatVersion)); err != nil {
		return fmt.Errorf(
			"failed to write format version: %w",
			err,
		)
	}

	return nil
}

// ValidateHeader reads and validates the DuckDB binary file header.
//
// Binary Format:
//   - Magic Number: 4 bytes (must be 0x4455434B = "DUCK")
//   - Version: 8 bytes (must be 64 for DuckDB v1.1.3)
//
// All multi-byte values are read in little-endian byte order.
//
// Returns ErrInvalidMagicNumber if the magic number doesn't match.
// Returns ErrUnsupportedVersion if the format version is not 64.
func ValidateHeader(r io.Reader) error {
	// Read magic number (4 bytes)
	var magic uint32
	if err := binary.Read(r, ByteOrder, &magic); err != nil {
		return fmt.Errorf(
			"failed to read magic number: %w",
			err,
		)
	}
	if magic != DuckDBMagicNumber {
		return fmt.Errorf(
			"%w: got 0x%08X, expected 0x%08X",
			ErrInvalidMagicNumber,
			magic,
			DuckDBMagicNumber,
		)
	}

	// Read format version (8 bytes)
	var version uint64
	if err := binary.Read(r, ByteOrder, &version); err != nil {
		return fmt.Errorf(
			"failed to read format version: %w",
			err,
		)
	}
	if version != DuckDBFormatVersion {
		return fmt.Errorf(
			"%w: got %d, expected %d",
			ErrUnsupportedVersion,
			version,
			DuckDBFormatVersion,
		)
	}

	return nil
}

// SerializeCatalog serializes an entire catalog to binary format.
//
// Binary Format:
//   - Schema count: uint64 (number of schemas)
//   - For each schema:
//   - Schema serialization (via SerializeSchema)
//
// The catalog is serialized by iterating through all schemas and serializing
// each one using property-based serialization.
//
// Note: Virtual tables are not serialized as they are runtime-only constructs.
func SerializeCatalog(
	w io.Writer,
	cat *catalog.Catalog,
) error {
	if cat == nil {
		return fmt.Errorf(
			"cannot serialize nil catalog",
		)
	}

	// Get all schemas from the catalog
	schemas := cat.ListSchemas()

	// Write schema count
	if err := binary.Write(w, ByteOrder, uint64(len(schemas))); err != nil {
		return fmt.Errorf(
			"failed to write schema count: %w",
			err,
		)
	}

	// Serialize each schema
	for i, schema := range schemas {
		if err := SerializeSchema(w, schema); err != nil {
			return fmt.Errorf(
				"failed to serialize schema %d: %w",
				i,
				err,
			)
		}
	}

	return nil
}

// SerializeSchema serializes a schema entry to binary format.
//
// Binary Format (property-based):
//   - Property 100: uint32 (CatalogEntryType_SCHEMA = 1)
//   - Property 101: string (schema name)
//   - Property 200: uint64 (table count)
//   - For each table:
//   - Table serialization (via SerializeTableEntry)
//
// A schema contains metadata about a namespace and its tables.
func SerializeSchema(
	w io.Writer,
	schema *catalog.Schema,
) error {
	if schema == nil {
		return fmt.Errorf(
			"cannot serialize nil schema",
		)
	}

	bw := NewBinaryWriter(w)

	// Property 100: Entry type (SCHEMA)
	if err := bw.WriteProperty(100, uint32(CatalogEntryType_SCHEMA)); err != nil {
		return fmt.Errorf(
			"failed to write schema entry type: %w",
			err,
		)
	}

	// Property 101: Schema name
	if err := bw.WriteProperty(101, schema.Name()); err != nil {
		return fmt.Errorf(
			"failed to write schema name: %w",
			err,
		)
	}

	// Get all tables in the schema
	tables := schema.ListTables()

	// Property 200: Table count
	if err := bw.WriteProperty(200, uint64(len(tables))); err != nil {
		return fmt.Errorf(
			"failed to write table count: %w",
			err,
		)
	}

	// Flush schema properties
	if err := bw.Flush(); err != nil {
		return fmt.Errorf(
			"failed to flush schema properties: %w",
			err,
		)
	}

	// Serialize each table
	for i, table := range tables {
		if err := SerializeTableEntry(w, table); err != nil {
			return fmt.Errorf(
				"failed to serialize table %d (%s): %w",
				i,
				table.Name,
				err,
			)
		}
	}

	return nil
}

// SerializeTableEntry serializes a table entry to binary format.
//
// Binary Format (property-based):
//   - Property 100: uint32 (CatalogEntryType_TABLE = 2)
//   - Property 101: string (table name)
//   - Property 102: string (schema name)
//   - Property 200: uint64 (column count)
//   - For each column:
//   - Column serialization (via SerializeColumn)
//   - Property 201: []int (primary key column indices, optional)
//
// A table entry contains the table definition including all columns and constraints.
func SerializeTableEntry(
	w io.Writer,
	table *catalog.TableDef,
) error {
	if table == nil {
		return fmt.Errorf(
			"cannot serialize nil table",
		)
	}

	bw := NewBinaryWriter(w)

	// Property 100: Entry type (TABLE)
	if err := bw.WriteProperty(100, uint32(CatalogEntryType_TABLE)); err != nil {
		return fmt.Errorf(
			"failed to write table entry type: %w",
			err,
		)
	}

	// Property 101: Table name
	if err := bw.WriteProperty(101, table.Name); err != nil {
		return fmt.Errorf(
			"failed to write table name: %w",
			err,
		)
	}

	// Property 102: Schema name
	if err := bw.WriteProperty(102, table.Schema); err != nil {
		return fmt.Errorf(
			"failed to write schema name: %w",
			err,
		)
	}

	// Property 200: Column count
	if err := bw.WriteProperty(200, uint64(len(table.Columns))); err != nil {
		return fmt.Errorf(
			"failed to write column count: %w",
			err,
		)
	}

	// Flush table properties before writing columns
	if err := bw.Flush(); err != nil {
		return fmt.Errorf(
			"failed to flush table properties: %w",
			err,
		)
	}

	// Serialize each column
	for i, col := range table.Columns {
		if err := SerializeColumn(w, col); err != nil {
			return fmt.Errorf(
				"failed to serialize column %d (%s): %w",
				i,
				col.Name,
				err,
			)
		}
	}

	// Note: Property 201 (primary key) would be written here if needed
	// This is simplified for now - full implementation would handle constraints

	return nil
}

// SerializeColumn serializes a column definition to binary format.
//
// Binary Format (property-based):
//   - Property 100: string (column name)
//   - Property 101: TypeInfo (serialized via SerializeTypeInfo)
//   - Property 102: bool (nullable flag, optional, default=true)
//   - Property 103: any (default value, optional)
//
// A column contains metadata about a table column including its name, type,
// nullability, and default value.
func SerializeColumn(
	w io.Writer,
	col *catalog.ColumnDef,
) error {
	if col == nil {
		return fmt.Errorf(
			"cannot serialize nil column",
		)
	}

	bw := NewBinaryWriter(w)

	// Property 100: Column name
	if err := bw.WriteProperty(100, col.Name); err != nil {
		return fmt.Errorf(
			"failed to write column name: %w",
			err,
		)
	}

	// Property 101: TypeInfo (recursive serialization)
	// Serialize TypeInfo to a buffer first
	typeInfoBuf := new(bytes.Buffer)
	typeInfoWriter := NewBinaryWriter(typeInfoBuf)
	typeInfo := col.GetTypeInfo()
	if err := SerializeTypeInfo(typeInfoWriter, typeInfo); err != nil {
		return fmt.Errorf(
			"failed to serialize column TypeInfo: %w",
			err,
		)
	}
	if err := typeInfoWriter.Flush(); err != nil {
		return fmt.Errorf(
			"failed to flush column TypeInfo: %w",
			err,
		)
	}

	// Write the serialized TypeInfo as property 101
	if err := bw.WriteProperty(101, typeInfoBuf.Bytes()); err != nil {
		return fmt.Errorf(
			"failed to write column TypeInfo property: %w",
			err,
		)
	}

	// Property 102: Nullable flag (optional, default=true)
	// Only write if not nullable (non-default value)
	if !col.Nullable {
		if err := bw.WriteProperty(102, uint8(0)); err != nil {
			return fmt.Errorf(
				"failed to write nullable flag: %w",
				err,
			)
		}
	}

	// Property 103: Default value (optional)
	// For now, we skip default value serialization as it requires
	// type-specific encoding. This would be implemented in a full version.
	if col.HasDefault {
		// TODO: Serialize default value based on column type
		// This is complex and would need type-specific serialization
		// Skipping for now
	}

	// Flush column properties
	if err := bw.Flush(); err != nil {
		return fmt.Errorf(
			"failed to flush column properties: %w",
			err,
		)
	}

	return nil
}
