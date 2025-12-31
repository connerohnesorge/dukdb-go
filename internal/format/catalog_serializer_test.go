package format

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWriteHeader tests the WriteHeader function.
func TestWriteHeader(t *testing.T) {
	buf := new(bytes.Buffer)
	err := WriteHeader(buf)
	require.NoError(t, err)

	// Verify magic number (4 bytes)
	var magic uint32
	err = binary.Read(buf, ByteOrder, &magic)
	require.NoError(t, err)
	assert.Equal(
		t,
		uint32(DuckDBMagicNumber),
		magic,
	)

	// Verify version (8 bytes)
	var version uint64
	err = binary.Read(buf, ByteOrder, &version)
	require.NoError(t, err)
	assert.Equal(
		t,
		uint64(DuckDBFormatVersion),
		version,
	)

	// Ensure buffer is fully consumed
	assert.Equal(t, 0, buf.Len())
}

// TestValidateHeader_Success tests ValidateHeader with valid header.
func TestValidateHeader_Success(t *testing.T) {
	buf := new(bytes.Buffer)

	// Write valid header
	err := binary.Write(
		buf,
		ByteOrder,
		uint32(DuckDBMagicNumber),
	)
	require.NoError(t, err)
	err = binary.Write(
		buf,
		ByteOrder,
		uint64(DuckDBFormatVersion),
	)
	require.NoError(t, err)

	// Validate
	err = ValidateHeader(buf)
	assert.NoError(t, err)
}

// TestValidateHeader_InvalidMagicNumber tests detection of invalid magic number.
func TestValidateHeader_InvalidMagicNumber(
	t *testing.T,
) {
	buf := new(bytes.Buffer)

	// Write invalid magic number
	err := binary.Write(
		buf,
		ByteOrder,
		uint32(0x12345678),
	)
	require.NoError(t, err)
	err = binary.Write(
		buf,
		ByteOrder,
		uint64(DuckDBFormatVersion),
	)
	require.NoError(t, err)

	// Validate should fail
	err = ValidateHeader(buf)
	assert.ErrorIs(t, err, ErrInvalidMagicNumber)
	assert.Contains(t, err.Error(), "0x12345678")
}

// TestValidateHeader_UnsupportedVersion tests detection of unsupported version.
func TestValidateHeader_UnsupportedVersion(
	t *testing.T,
) {
	buf := new(bytes.Buffer)

	// Write valid magic but wrong version
	err := binary.Write(
		buf,
		ByteOrder,
		uint32(DuckDBMagicNumber),
	)
	require.NoError(t, err)
	err = binary.Write(
		buf,
		ByteOrder,
		uint64(999),
	)
	require.NoError(t, err)

	// Validate should fail
	err = ValidateHeader(buf)
	assert.ErrorIs(t, err, ErrUnsupportedVersion)
	assert.Contains(t, err.Error(), "999")
}

// TestValidateHeader_TruncatedMagic tests handling of truncated magic number.
func TestValidateHeader_TruncatedMagic(
	t *testing.T,
) {
	buf := bytes.NewBuffer(
		[]byte{0x44, 0x55},
	) // Only 2 bytes instead of 4

	err := ValidateHeader(buf)
	assert.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"failed to read magic number",
	)
}

// TestValidateHeader_TruncatedVersion tests handling of truncated version.
func TestValidateHeader_TruncatedVersion(
	t *testing.T,
) {
	buf := new(bytes.Buffer)

	// Write valid magic number
	err := binary.Write(
		buf,
		ByteOrder,
		uint32(DuckDBMagicNumber),
	)
	require.NoError(t, err)

	// Write only partial version (4 bytes instead of 8)
	buf.Write([]byte{0x01, 0x02, 0x03, 0x04})

	err = ValidateHeader(buf)
	assert.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"failed to read format version",
	)
}

// TestSerializeColumn tests basic column serialization.
func TestSerializeColumn(t *testing.T) {
	col := catalog.NewColumnDef(
		"test_col",
		dukdb.TYPE_INTEGER,
	)
	col.Nullable = true

	buf := new(bytes.Buffer)
	err := SerializeColumn(buf, col)
	require.NoError(t, err)

	// Verify we wrote something
	assert.Greater(t, buf.Len(), 0)

	// Read back and verify structure
	reader := NewBinaryReader(buf)
	err = reader.Load()
	require.NoError(t, err)

	// Property 100: Column name
	var name string
	err = reader.ReadProperty(100, &name)
	require.NoError(t, err)
	assert.Equal(t, "test_col", name)

	// Property 101: TypeInfo (should be present)
	var typeInfoBytes []byte
	err = reader.ReadProperty(101, &typeInfoBytes)
	require.NoError(t, err)
	assert.Greater(t, len(typeInfoBytes), 0)

	// Property 102: Nullable flag should not be present (default value)
	var nullableFlag uint8
	err = reader.ReadPropertyWithDefault(
		102,
		&nullableFlag,
		uint8(1),
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		uint8(1),
		nullableFlag,
	) // Default value used
}

// TestSerializeColumn_NotNullable tests column serialization with NOT NULL constraint.
func TestSerializeColumn_NotNullable(
	t *testing.T,
) {
	col := catalog.NewColumnDef(
		"id",
		dukdb.TYPE_BIGINT,
	)
	col.Nullable = false // NOT NULL

	buf := new(bytes.Buffer)
	err := SerializeColumn(buf, col)
	require.NoError(t, err)

	// Read back
	reader := NewBinaryReader(buf)
	err = reader.Load()
	require.NoError(t, err)

	// Property 102: Nullable flag should be present and set to 0
	var nullableFlag uint8
	err = reader.ReadProperty(102, &nullableFlag)
	require.NoError(t, err)
	assert.Equal(t, uint8(0), nullableFlag)
}

// TestSerializeColumn_ComplexType tests column with complex TypeInfo.
func TestSerializeColumn_ComplexType(
	t *testing.T,
) {
	// Create a LIST<INTEGER> type
	intInfo, err := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	require.NoError(t, err)

	listInfo, err := dukdb.NewListInfo(intInfo)
	require.NoError(t, err)

	col := &catalog.ColumnDef{
		Name:     "items",
		Type:     dukdb.TYPE_LIST,
		TypeInfo: listInfo,
		Nullable: true,
	}

	buf := new(bytes.Buffer)
	err = SerializeColumn(buf, col)
	require.NoError(t, err)
	assert.Greater(t, buf.Len(), 0)
}

// TestSerializeColumn_Nil tests error handling for nil column.
func TestSerializeColumn_Nil(t *testing.T) {
	buf := new(bytes.Buffer)
	err := SerializeColumn(buf, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil column")
}

// TestSerializeTableEntry tests table entry serialization.
func TestSerializeTableEntry(t *testing.T) {
	// Create a simple table
	columns := []*catalog.ColumnDef{
		catalog.NewColumnDef(
			"id",
			dukdb.TYPE_INTEGER,
		),
		catalog.NewColumnDef(
			"name",
			dukdb.TYPE_VARCHAR,
		),
	}
	table := catalog.NewTableDef("users", columns)
	table.Schema = "main"

	buf := new(bytes.Buffer)
	err := SerializeTableEntry(buf, table)
	require.NoError(t, err)
	assert.Greater(t, buf.Len(), 0)

	// Read back table properties (before columns)
	reader := NewBinaryReader(buf)
	err = reader.Load()
	require.NoError(t, err)

	// Property 100: Entry type
	var entryType uint32
	err = reader.ReadProperty(100, &entryType)
	require.NoError(t, err)
	assert.Equal(
		t,
		uint32(CatalogEntryType_TABLE),
		entryType,
	)

	// Property 101: Table name
	var tableName string
	err = reader.ReadProperty(101, &tableName)
	require.NoError(t, err)
	assert.Equal(t, "users", tableName)

	// Property 102: Schema name
	var schemaName string
	err = reader.ReadProperty(102, &schemaName)
	require.NoError(t, err)
	assert.Equal(t, "main", schemaName)

	// Property 200: Column count
	var colCount uint64
	err = reader.ReadProperty(200, &colCount)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), colCount)
}

// TestSerializeTableEntry_EmptyTable tests serialization of table with no columns.
func TestSerializeTableEntry_EmptyTable(
	t *testing.T,
) {
	table := catalog.NewTableDef(
		"empty",
		[]*catalog.ColumnDef{},
	)
	table.Schema = "main"

	buf := new(bytes.Buffer)
	err := SerializeTableEntry(buf, table)
	require.NoError(t, err)

	// Read back
	reader := NewBinaryReader(buf)
	err = reader.Load()
	require.NoError(t, err)

	var colCount uint64
	err = reader.ReadProperty(200, &colCount)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), colCount)
}

// TestSerializeTableEntry_Nil tests error handling for nil table.
func TestSerializeTableEntry_Nil(t *testing.T) {
	buf := new(bytes.Buffer)
	err := SerializeTableEntry(buf, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil table")
}

// TestSerializeSchema tests schema serialization.
func TestSerializeSchema(t *testing.T) {
	schema := catalog.NewSchema("test_schema")

	// Add some tables
	table1 := catalog.NewTableDef(
		"table1",
		[]*catalog.ColumnDef{
			catalog.NewColumnDef(
				"col1",
				dukdb.TYPE_INTEGER,
			),
		},
	)
	err := schema.CreateTable(table1)
	require.NoError(t, err)

	table2 := catalog.NewTableDef(
		"table2",
		[]*catalog.ColumnDef{
			catalog.NewColumnDef(
				"col1",
				dukdb.TYPE_VARCHAR,
			),
			catalog.NewColumnDef(
				"col2",
				dukdb.TYPE_BOOLEAN,
			),
		},
	)
	err = schema.CreateTable(table2)
	require.NoError(t, err)

	buf := new(bytes.Buffer)
	err = SerializeSchema(buf, schema)
	require.NoError(t, err)
	assert.Greater(t, buf.Len(), 0)

	// Read back schema properties
	reader := NewBinaryReader(buf)
	err = reader.Load()
	require.NoError(t, err)

	// Property 100: Entry type
	var entryType uint32
	err = reader.ReadProperty(100, &entryType)
	require.NoError(t, err)
	assert.Equal(
		t,
		uint32(CatalogEntryType_SCHEMA),
		entryType,
	)

	// Property 101: Schema name
	var schemaName string
	err = reader.ReadProperty(101, &schemaName)
	require.NoError(t, err)
	assert.Equal(t, "test_schema", schemaName)

	// Property 200: Table count
	var tableCount uint64
	err = reader.ReadProperty(200, &tableCount)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), tableCount)
}

// TestSerializeSchema_Empty tests serialization of schema with no tables.
func TestSerializeSchema_Empty(t *testing.T) {
	schema := catalog.NewSchema("empty_schema")

	buf := new(bytes.Buffer)
	err := SerializeSchema(buf, schema)
	require.NoError(t, err)

	// Read back
	reader := NewBinaryReader(buf)
	err = reader.Load()
	require.NoError(t, err)

	var tableCount uint64
	err = reader.ReadProperty(200, &tableCount)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), tableCount)
}

// TestSerializeSchema_Nil tests error handling for nil schema.
func TestSerializeSchema_Nil(t *testing.T) {
	buf := new(bytes.Buffer)
	err := SerializeSchema(buf, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil schema")
}

// TestSerializeCatalog tests catalog serialization.
func TestSerializeCatalog(t *testing.T) {
	cat := catalog.NewCatalog()

	// Add a table to the main schema
	table := catalog.NewTableDef(
		"test_table",
		[]*catalog.ColumnDef{
			catalog.NewColumnDef(
				"id",
				dukdb.TYPE_INTEGER,
			),
			catalog.NewColumnDef(
				"name",
				dukdb.TYPE_VARCHAR,
			),
		},
	)
	err := cat.CreateTable(table)
	require.NoError(t, err)

	buf := new(bytes.Buffer)
	err = SerializeCatalog(buf, cat)
	require.NoError(t, err)
	assert.Greater(t, buf.Len(), 0)

	// Read schema count
	var schemaCount uint64
	err = binary.Read(
		buf,
		ByteOrder,
		&schemaCount,
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		uint64(1),
		schemaCount,
	) // Should have "main" schema
}

// TestSerializeCatalog_Empty tests serialization of empty catalog.
func TestSerializeCatalog_Empty(t *testing.T) {
	cat := catalog.NewCatalog()

	buf := new(bytes.Buffer)
	err := SerializeCatalog(buf, cat)
	require.NoError(t, err)

	// Read schema count
	var schemaCount uint64
	err = binary.Read(
		buf,
		ByteOrder,
		&schemaCount,
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		uint64(1),
		schemaCount,
	) // "main" schema always exists
}

// TestSerializeCatalog_Nil tests error handling for nil catalog.
func TestSerializeCatalog_Nil(t *testing.T) {
	buf := new(bytes.Buffer)
	err := SerializeCatalog(buf, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil catalog")
}

// TestRoundTrip_HeaderOnly tests write and validate header round-trip.
func TestRoundTrip_HeaderOnly(t *testing.T) {
	buf := new(bytes.Buffer)

	// Write header
	err := WriteHeader(buf)
	require.NoError(t, err)

	// Validate header
	err = ValidateHeader(buf)
	assert.NoError(t, err)
}

// TestSerializeCatalogWithComplexTypes tests catalog with complex column types.
func TestSerializeCatalogWithComplexTypes(
	t *testing.T,
) {
	cat := catalog.NewCatalog()

	// Create DECIMAL column
	decimalInfo, err := dukdb.NewDecimalInfo(
		18,
		4,
	)
	require.NoError(t, err)

	// Create ENUM column
	enumInfo, err := dukdb.NewEnumInfo(
		"RED",
		"GREEN",
		"BLUE",
	)
	require.NoError(t, err)

	// Create LIST column
	intInfo2, err := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	require.NoError(t, err)

	listInfo, err := dukdb.NewListInfo(intInfo2)
	require.NoError(t, err)

	// Create table with complex types
	table := &catalog.TableDef{
		Name:   "complex_types",
		Schema: "main",
		Columns: []*catalog.ColumnDef{
			{
				Name:     "price",
				Type:     dukdb.TYPE_DECIMAL,
				TypeInfo: decimalInfo,
				Nullable: false,
			},
			{
				Name:     "color",
				Type:     dukdb.TYPE_ENUM,
				TypeInfo: enumInfo,
				Nullable: true,
			},
			{
				Name:     "items",
				Type:     dukdb.TYPE_LIST,
				TypeInfo: listInfo,
				Nullable: true,
			},
		},
	}

	err = cat.CreateTable(table)
	require.NoError(t, err)

	buf := new(bytes.Buffer)
	err = SerializeCatalog(buf, cat)
	require.NoError(t, err)
	assert.Greater(t, buf.Len(), 0)
}
