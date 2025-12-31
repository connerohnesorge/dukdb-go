package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/dukdb/dukdb-go/internal/catalog"
)

// DeserializeCatalog deserializes an entire catalog from binary format.
//
// Binary Format:
//   - Schema count: uint64 (number of schemas)
//   - For each schema:
//   - Schema serialization (via DeserializeSchema)
//
// The catalog is deserialized by reading all schemas and their tables
// using property-based deserialization.
//
// Note: Virtual tables are not deserialized as they are runtime-only constructs.
// They must be re-registered after loading the catalog from disk.
func DeserializeCatalog(r io.Reader) (*catalog.Catalog, error) {
	// Read schema count
	var schemaCount uint64
	if err := binary.Read(r, ByteOrder, &schemaCount); err != nil {
		return nil, fmt.Errorf("failed to read schema count: %w", err)
	}

	// Create new catalog
	cat := catalog.NewCatalog()

	// Deserialize each schema
	for i := range schemaCount {
		schema, err := DeserializeSchema(r)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize schema %d: %w", i, err)
		}

		// Add tables from the deserialized schema to the catalog
		// If the schema is "main", add to existing main schema
		// Otherwise, create a new schema first
		if schema.Name() != "main" {
			if _, err := cat.CreateSchema(schema.Name()); err != nil {
				return nil, fmt.Errorf("failed to create schema %s: %w", schema.Name(), err)
			}
		}

		// Add all tables to the appropriate schema
		for _, table := range schema.ListTables() {
			if err := cat.CreateTableInSchema(schema.Name(), table); err != nil {
				return nil, fmt.Errorf("failed to add table %s to schema %s: %w", table.Name, schema.Name(), err)
			}
		}
	}

	return cat, nil
}

// DeserializeSchema deserializes a schema entry from binary format.
//
// Binary Format (property-based):
//   - Property 100: uint32 (CatalogEntryType_SCHEMA = 1)
//   - Property 101: string (schema name)
//   - Property 200: uint64 (table count)
//   - For each table:
//   - Table serialization (via DeserializeTableEntry)
//
// A schema contains metadata about a namespace and its tables.
func DeserializeSchema(r io.Reader) (*catalog.Schema, error) {
	br := NewBinaryReader(r)
	if err := br.Load(); err != nil {
		return nil, fmt.Errorf("failed to load schema properties: %w", err)
	}

	// Property 100: Entry type (should be SCHEMA)
	var entryType uint32
	if err := br.ReadProperty(100, &entryType); err != nil {
		return nil, fmt.Errorf("failed to read schema entry type: %w", err)
	}
	if entryType != CatalogEntryType_SCHEMA {
		return nil, fmt.Errorf("expected schema entry type %d, got %d", CatalogEntryType_SCHEMA, entryType)
	}

	// Property 101: Schema name
	var schemaName string
	if err := br.ReadProperty(101, &schemaName); err != nil {
		return nil, fmt.Errorf("failed to read schema name: %w", err)
	}

	// Property 200: Table count
	var tableCount uint64
	if err := br.ReadProperty(200, &tableCount); err != nil {
		return nil, fmt.Errorf("failed to read table count: %w", err)
	}

	// Create new schema
	schema := catalog.NewSchema(schemaName)

	// Deserialize each table
	for i := range tableCount {
		table, err := DeserializeTableEntry(r)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize table %d: %w", i, err)
		}

		if err := schema.CreateTable(table); err != nil {
			return nil, fmt.Errorf("failed to add table %s to schema: %w", table.Name, err)
		}
	}

	return schema, nil
}

// DeserializeTableEntry deserializes a table entry from binary format.
//
// Binary Format (property-based):
//   - Property 100: uint32 (CatalogEntryType_TABLE = 2)
//   - Property 101: string (table name)
//   - Property 102: string (schema name)
//   - Property 200: uint64 (column count)
//   - For each column:
//   - Column serialization (via DeserializeColumn)
//   - Property 201: []int (primary key column indices, optional)
//
// A table entry contains the table definition including all columns and constraints.
func DeserializeTableEntry(r io.Reader) (*catalog.TableDef, error) {
	br := NewBinaryReader(r)
	if err := br.Load(); err != nil {
		return nil, fmt.Errorf("failed to load table properties: %w", err)
	}

	// Property 100: Entry type (should be TABLE)
	var entryType uint32
	if err := br.ReadProperty(100, &entryType); err != nil {
		return nil, fmt.Errorf("failed to read table entry type: %w", err)
	}
	if entryType != CatalogEntryType_TABLE {
		return nil, fmt.Errorf("expected table entry type %d, got %d", CatalogEntryType_TABLE, entryType)
	}

	// Property 101: Table name
	var tableName string
	if err := br.ReadProperty(101, &tableName); err != nil {
		return nil, fmt.Errorf("failed to read table name: %w", err)
	}

	// Property 102: Schema name
	var schemaName string
	if err := br.ReadProperty(102, &schemaName); err != nil {
		return nil, fmt.Errorf("failed to read schema name: %w", err)
	}

	// Property 200: Column count
	var columnCount uint64
	if err := br.ReadProperty(200, &columnCount); err != nil {
		return nil, fmt.Errorf("failed to read column count: %w", err)
	}

	// Deserialize each column
	columns := make([]*catalog.ColumnDef, columnCount)
	for i := range columnCount {
		col, err := DeserializeColumn(r)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize column %d: %w", i, err)
		}
		columns[i] = col
	}

	// Create table definition
	table := catalog.NewTableDef(tableName, columns)
	table.Schema = schemaName

	// Property 201: Primary key (optional)
	// For now, we skip primary key deserialization as it's optional
	// This would be implemented in a full version

	return table, nil
}

// DeserializeColumn deserializes a column definition from binary format.
//
// Binary Format (property-based):
//   - Property 100: string (column name)
//   - Property 101: TypeInfo (serialized via SerializeTypeInfo)
//   - Property 102: bool (nullable flag, optional, default=true)
//   - Property 103: any (default value, optional)
//
// A column contains metadata about a table column including its name, type,
// nullability, and default value.
func DeserializeColumn(r io.Reader) (*catalog.ColumnDef, error) {
	br := NewBinaryReader(r)
	if err := br.Load(); err != nil {
		return nil, fmt.Errorf("failed to load column properties: %w", err)
	}

	// Property 100: Column name
	var columnName string
	if err := br.ReadProperty(100, &columnName); err != nil {
		return nil, fmt.Errorf("failed to read column name: %w", err)
	}

	// Property 101: TypeInfo (recursive deserialization)
	var typeInfoBytes []byte
	if err := br.ReadProperty(101, &typeInfoBytes); err != nil {
		return nil, fmt.Errorf("failed to read column TypeInfo: %w", err)
	}

	// Deserialize the TypeInfo
	typeInfoBuf := bytes.NewReader(typeInfoBytes)
	typeInfoReader := NewBinaryReader(typeInfoBuf)
	if err := typeInfoReader.Load(); err != nil {
		return nil, fmt.Errorf("failed to load column TypeInfo properties: %w", err)
	}

	typeInfo, err := DeserializeTypeInfo(typeInfoReader)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize column TypeInfo: %w", err)
	}

	// Create column definition with the TypeInfo's internal type
	col := catalog.NewColumnDef(columnName, typeInfo.InternalType())
	col.TypeInfo = typeInfo

	// Property 102: Nullable flag (optional, default=true)
	var nullableFlag uint8
	if err := br.ReadPropertyWithDefault(102, &nullableFlag, uint8(1)); err != nil {
		return nil, fmt.Errorf("failed to read nullable flag: %w", err)
	}
	col.Nullable = (nullableFlag != 0)

	// Property 103: Default value (optional)
	// For now, we skip default value deserialization as it requires
	// type-specific decoding. This would be implemented in a full version.
	// If property 103 exists, we would mark HasDefault=true and decode the value

	return col, nil
}

// LoadCatalogFromDuckDBFormat loads a catalog from a DuckDB binary format file.
//
// This function reads a complete DuckDB file including header validation and
// catalog deserialization. It performs the following steps:
//  1. Opens the file for reading
//  2. Validates the DuckDB magic number and format version
//  3. Deserializes the catalog structure
//
// The file must be in DuckDB v64 binary format with the proper header.
//
// Returns an error if the file cannot be opened, has an invalid header, or
// contains malformed catalog data.
func LoadCatalogFromDuckDBFormat(path string) (*catalog.Catalog, error) {
	// Open file for reading
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open catalog file: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()

	// Validate DuckDB header
	if err := ValidateHeader(f); err != nil {
		return nil, fmt.Errorf("invalid DuckDB file header: %w", err)
	}

	// Deserialize catalog
	cat, err := DeserializeCatalog(f)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize catalog: %w", err)
	}

	return cat, nil
}

// SaveCatalogToDuckDBFormat saves a catalog to a DuckDB binary format file.
//
// This function writes a complete DuckDB file including header and catalog
// serialization. It performs the following steps:
//  1. Creates or truncates the output file
//  2. Writes the DuckDB magic number and format version
//  3. Serializes the catalog structure
//
// The resulting file will be in DuckDB v64 binary format and can be read by
// compatible DuckDB implementations.
//
// Returns an error if the file cannot be created or if serialization fails.
func SaveCatalogToDuckDBFormat(cat *catalog.Catalog, path string) error {
	// Create output file
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create catalog file: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()

	// Write DuckDB header
	if err := WriteHeader(f); err != nil {
		return fmt.Errorf("failed to write DuckDB header: %w", err)
	}

	// Serialize catalog
	if err := SerializeCatalog(f, cat); err != nil {
		return fmt.Errorf("failed to serialize catalog: %w", err)
	}

	return nil
}
