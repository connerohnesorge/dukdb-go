package catalog_test

import (
	"os"
	"path/filepath"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCatalogSaveLoadRoundTrip validates end-to-end database save/load.
// This test creates a catalog with tables and complex column types,
// saves it to a temp file, loads it back, and verifies all metadata matches.
func TestCatalogSaveLoadRoundTrip(t *testing.T) {
	// Create temporary file for testing
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.duckdb")

	// Create original catalog with complex schema
	originalCat := catalog.NewCatalog()

	// Create a table with various primitive types
	primitiveTable := createPrimitiveTable(t)
	err := originalCat.CreateTable(primitiveTable)
	require.NoError(
		t,
		err,
		"Failed to create primitive table",
	)

	// Create a table with complex types
	complexTable := createComplexTable(t)
	err = originalCat.CreateTable(complexTable)
	require.NoError(
		t,
		err,
		"Failed to create complex table",
	)

	// Create a table with nested complex types
	nestedTable := createNestedTable(t)
	err = originalCat.CreateTable(nestedTable)
	require.NoError(
		t,
		err,
		"Failed to create nested table",
	)

	// Create a custom schema with tables
	_, err = originalCat.CreateSchema(
		"custom_schema",
	)
	require.NoError(
		t,
		err,
		"Failed to create custom schema",
	)

	customTable := createCustomSchemaTable(t)
	err = originalCat.CreateTableInSchema(
		"custom_schema",
		customTable,
	)
	require.NoError(
		t,
		err,
		"Failed to create table in custom schema",
	)

	// Save the catalog to disk using the format package
	err = format.SaveCatalogToDuckDBFormat(
		originalCat,
		dbPath,
	)
	require.NoError(
		t,
		err,
		"Failed to save catalog",
	)

	// Verify file was created
	_, err = os.Stat(dbPath)
	require.NoError(
		t,
		err,
		"Database file was not created",
	)

	// Load the catalog from disk
	loadedCat, err := format.LoadCatalogFromDuckDBFormat(
		dbPath,
	)
	require.NoError(
		t,
		err,
		"Failed to load catalog",
	)

	// Validate the loaded catalog matches the original
	validateCatalogMatch(
		t,
		originalCat,
		loadedCat,
	)
}

// createPrimitiveTable creates a table with all primitive types.
func createPrimitiveTable(
	_ *testing.T,
) *catalog.TableDef {
	columns := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER).
			WithNullable(false),
		catalog.NewColumnDef(
			"name",
			dukdb.TYPE_VARCHAR,
		),
		catalog.NewColumnDef(
			"age",
			dukdb.TYPE_TINYINT,
		),
		catalog.NewColumnDef(
			"salary",
			dukdb.TYPE_DOUBLE,
		),
		catalog.NewColumnDef(
			"is_active",
			dukdb.TYPE_BOOLEAN,
		),
		catalog.NewColumnDef(
			"birth_date",
			dukdb.TYPE_DATE,
		),
		catalog.NewColumnDef(
			"created_at",
			dukdb.TYPE_TIMESTAMP,
		),
		catalog.NewColumnDef(
			"balance",
			dukdb.TYPE_BIGINT,
		),
	}

	return catalog.NewTableDef("users", columns)
}

// createComplexTable creates a table with complex column types.
func createComplexTable(
	t *testing.T,
) *catalog.TableDef {
	// Create DECIMAL type
	decimalInfo, err := dukdb.NewDecimalInfo(
		18,
		4,
	)
	require.NoError(t, err)

	// Create LIST type (LIST of INTEGERs)
	intInfo, err := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	require.NoError(t, err)
	listInfo, err := dukdb.NewListInfo(intInfo)
	require.NoError(t, err)

	// Create STRUCT type
	streetEntry, err := dukdb.NewStructEntry(
		mustTypeInfo(t, dukdb.TYPE_VARCHAR),
		"street",
	)
	require.NoError(t, err)
	cityEntry, err := dukdb.NewStructEntry(
		mustTypeInfo(t, dukdb.TYPE_VARCHAR),
		"city",
	)
	require.NoError(t, err)
	zipcodeEntry, err := dukdb.NewStructEntry(
		mustTypeInfo(t, dukdb.TYPE_INTEGER),
		"zipcode",
	)
	require.NoError(t, err)
	structInfo, err := dukdb.NewStructInfo(
		streetEntry,
		cityEntry,
		zipcodeEntry,
	)
	require.NoError(t, err)

	// Create ARRAY type (ARRAY of VARCHARs, size 10)
	varcharInfo, err := dukdb.NewTypeInfo(
		dukdb.TYPE_VARCHAR,
	)
	require.NoError(t, err)
	arrayInfo, err := dukdb.NewArrayInfo(
		varcharInfo,
		10,
	)
	require.NoError(t, err)

	columns := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER).
			WithNullable(false),
		{
			Name:     "price",
			Type:     dukdb.TYPE_DECIMAL,
			TypeInfo: decimalInfo,
			Nullable: true,
		},
		{
			Name:     "tags",
			Type:     dukdb.TYPE_LIST,
			TypeInfo: listInfo,
			Nullable: true,
		},
		{
			Name:     "address",
			Type:     dukdb.TYPE_STRUCT,
			TypeInfo: structInfo,
			Nullable: true,
		},
		{
			Name:     "nicknames",
			Type:     dukdb.TYPE_ARRAY,
			TypeInfo: arrayInfo,
			Nullable: true,
		},
	}

	return catalog.NewTableDef(
		"products",
		columns,
	)
}

// createNestedTable creates a table with deeply nested complex types.
func createNestedTable(
	t *testing.T,
) *catalog.TableDef {
	// Create nested LIST: LIST(LIST(INTEGER))
	intInfo, err := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	require.NoError(t, err)
	innerListInfo, err := dukdb.NewListInfo(
		intInfo,
	)
	require.NoError(t, err)
	outerListInfo, err := dukdb.NewListInfo(
		innerListInfo,
	)
	require.NoError(t, err)

	// Create MAP type: MAP(VARCHAR, STRUCT)
	varcharInfo, err := dukdb.NewTypeInfo(
		dukdb.TYPE_VARCHAR,
	)
	require.NoError(t, err)

	countEntry, err := dukdb.NewStructEntry(
		mustTypeInfo(t, dukdb.TYPE_INTEGER),
		"count",
	)
	require.NoError(t, err)
	totalEntry, err := dukdb.NewStructEntry(
		mustTypeInfo(t, dukdb.TYPE_DOUBLE),
		"total",
	)
	require.NoError(t, err)
	valueStructInfo, err := dukdb.NewStructInfo(
		countEntry,
		totalEntry,
	)
	require.NoError(t, err)

	mapInfo, err := dukdb.NewMapInfo(
		varcharInfo,
		valueStructInfo,
	)
	require.NoError(t, err)

	// Create nested STRUCT with DECIMAL field
	decimalInfo, err := dukdb.NewDecimalInfo(
		10,
		2,
	)
	require.NoError(t, err)

	nameEntry, err := dukdb.NewStructEntry(
		mustTypeInfo(t, dukdb.TYPE_VARCHAR),
		"name",
	)
	require.NoError(t, err)
	amountEntry, err := dukdb.NewStructEntry(
		decimalInfo,
		"amount",
	)
	require.NoError(t, err)
	tagsEntry, err := dukdb.NewStructEntry(
		innerListInfo,
		"tags",
	)
	require.NoError(t, err)
	nestedStructInfo, err := dukdb.NewStructInfo(
		nameEntry,
		amountEntry,
		tagsEntry,
	)
	require.NoError(t, err)

	columns := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_BIGINT).
			WithNullable(false),
		{
			Name:     "matrix",
			Type:     dukdb.TYPE_LIST,
			TypeInfo: outerListInfo,
			Nullable: true,
		},
		{
			Name:     "statistics",
			Type:     dukdb.TYPE_MAP,
			TypeInfo: mapInfo,
			Nullable: true,
		},
		{
			Name:     "metadata",
			Type:     dukdb.TYPE_STRUCT,
			TypeInfo: nestedStructInfo,
			Nullable: true,
		},
	}

	return catalog.NewTableDef(
		"analytics",
		columns,
	)
}

// createCustomSchemaTable creates a table for a custom schema.
func createCustomSchemaTable(
	_ *testing.T,
) *catalog.TableDef {
	columns := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER).
			WithNullable(false),
		catalog.NewColumnDef(
			"description",
			dukdb.TYPE_VARCHAR,
		),
		catalog.NewColumnDef(
			"created_at",
			dukdb.TYPE_TIMESTAMP,
		),
	}

	return catalog.NewTableDef("logs", columns)
}

// mustTypeInfo creates a TypeInfo or fails the test.
func mustTypeInfo(
	t *testing.T,
	typ dukdb.Type,
) dukdb.TypeInfo {
	info, err := dukdb.NewTypeInfo(typ)
	require.NoError(t, err)

	return info
}

// validateCatalogMatch compares two catalogs to ensure they match.
func validateCatalogMatch(
	t *testing.T,
	original, loaded *catalog.Catalog,
) {
	// Get schemas from both catalogs
	originalSchemas := original.ListSchemas()
	loadedSchemas := loaded.ListSchemas()

	// Verify same number of schemas
	require.Equal(
		t,
		len(originalSchemas),
		len(loadedSchemas),
		"Schema count mismatch",
	)

	// Create maps for easier lookup
	originalSchemaMap := make(
		map[string]*catalog.Schema,
	)
	for _, s := range originalSchemas {
		originalSchemaMap[s.Name()] = s
	}

	loadedSchemaMap := make(
		map[string]*catalog.Schema,
	)
	for _, s := range loadedSchemas {
		loadedSchemaMap[s.Name()] = s
	}

	// Verify all schemas match
	for schemaName, origSchema := range originalSchemaMap {
		loadedSchema, exists := loadedSchemaMap[schemaName]
		require.True(
			t,
			exists,
			"Schema %s not found in loaded catalog",
			schemaName,
		)

		// Compare tables in each schema
		origTables := origSchema.ListTables()
		loadedTables := loadedSchema.ListTables()

		require.Equal(
			t,
			len(origTables),
			len(loadedTables),
			"Table count mismatch in schema %s",
			schemaName,
		)

		// Create maps for easier lookup
		origTableMap := make(
			map[string]*catalog.TableDef,
		)
		for _, t := range origTables {
			origTableMap[t.Name] = t
		}

		loadedTableMap := make(
			map[string]*catalog.TableDef,
		)
		for _, t := range loadedTables {
			loadedTableMap[t.Name] = t
		}

		// Verify all tables match
		for tableName, origTable := range origTableMap {
			loadedTable, exists := loadedTableMap[tableName]
			require.True(
				t,
				exists,
				"Table %s not found in loaded schema %s",
				tableName,
				schemaName,
			)

			validateTableMatch(
				t,
				origTable,
				loadedTable,
			)
		}
	}
}

// validateTableMatch compares two table definitions to ensure they match.
func validateTableMatch(
	t *testing.T,
	original, loaded *catalog.TableDef,
) {
	assert.Equal(
		t,
		original.Name,
		loaded.Name,
		"Table name mismatch",
	)
	assert.Equal(
		t,
		original.Schema,
		loaded.Schema,
		"Schema name mismatch",
	)
	require.Equal(
		t,
		len(original.Columns),
		len(loaded.Columns),
		"Column count mismatch for table %s",
		original.Name,
	)

	// Compare each column
	for i, origCol := range original.Columns {
		loadedCol := loaded.Columns[i]
		validateColumnMatch(
			t,
			origCol,
			loadedCol,
			original.Name,
		)
	}
}

// validateColumnMatch compares two column definitions to ensure they match.
func validateColumnMatch(
	t *testing.T,
	original, loaded *catalog.ColumnDef,
	tableName string,
) {
	assert.Equal(
		t,
		original.Name,
		loaded.Name,
		"Column name mismatch in table %s",
		tableName,
	)
	assert.Equal(
		t,
		original.Type,
		loaded.Type,
		"Column type mismatch for %s.%s",
		tableName,
		original.Name,
	)
	assert.Equal(
		t,
		original.Nullable,
		loaded.Nullable,
		"Nullable mismatch for %s.%s",
		tableName,
		original.Name,
	)

	// Compare TypeInfo if present
	if original.TypeInfo == nil {
		return
	}

	require.NotNil(
		t,
		loaded.TypeInfo,
		"TypeInfo missing for %s.%s",
		tableName,
		original.Name,
	)
	loc := typeLocation{tableName, original.Name}
	validateTypeInfoMatch(
		t,
		original.TypeInfo,
		loaded.TypeInfo,
		loc,
	)
}

// typeLocation helps format error messages with table and column context.
type typeLocation struct {
	tableName  string
	columnName string
}

func (tl typeLocation) String() string {
	return tl.tableName + "." + tl.columnName
}

// validateTypeInfoMatch compares two TypeInfo objects.
func validateTypeInfoMatch(
	t *testing.T,
	original, loaded dukdb.TypeInfo,
	loc typeLocation,
) {
	assert.Equal(
		t,
		original.InternalType(),
		loaded.InternalType(),
		"TypeInfo InternalType mismatch for %s",
		loc,
	)

	origDetails := original.Details()
	loadedDetails := loaded.Details()

	// Both should have details or both should not
	if origDetails == nil {
		assert.Nil(
			t,
			loadedDetails,
			"Loaded TypeInfo has details but original doesn't for %s",
			loc,
		)

		return
	}

	require.NotNil(
		t,
		loadedDetails,
		"Original TypeInfo has details but loaded doesn't for %s",
		loc,
	)

	// Compare based on type
	switch origDetails := origDetails.(type) {
	case *dukdb.DecimalDetails:
		loadedDecimal, ok := loadedDetails.(*dukdb.DecimalDetails)
		require.True(t, ok, "Type details mismatch for %s", loc)
		assert.Equal(t, origDetails.Width, loadedDecimal.Width)
		assert.Equal(t, origDetails.Scale, loadedDecimal.Scale)

	case *dukdb.ListDetails:
		loadedList, ok := loadedDetails.(*dukdb.ListDetails)
		require.True(t, ok, "Type details mismatch for %s", loc)
		childLoc := typeLocation{loc.tableName, loc.columnName + "[child]"}
		validateTypeInfoMatch(t, origDetails.Child, loadedList.Child, childLoc)

	case *dukdb.ArrayDetails:
		loadedArray, ok := loadedDetails.(*dukdb.ArrayDetails)
		require.True(t, ok, "Type details mismatch for %s", loc)
		assert.Equal(t, origDetails.Size, loadedArray.Size)
		childLoc := typeLocation{loc.tableName, loc.columnName + "[child]"}
		validateTypeInfoMatch(t, origDetails.Child, loadedArray.Child, childLoc)

	case *dukdb.StructDetails:
		loadedStruct, ok := loadedDetails.(*dukdb.StructDetails)
		require.True(t, ok, "Type details mismatch for %s", loc)

		origEntries := origDetails.Entries
		loadedEntries := loadedStruct.Entries
		require.Equal(t, len(origEntries), len(loadedEntries),
			"Struct field count mismatch for %s", loc)

		for i, origEntry := range origEntries {
			loadedEntry := loadedEntries[i]
			assert.Equal(t, origEntry.Name(), loadedEntry.Name(),
				"Struct field name mismatch for %s[%d]", loc, i)
			fieldLoc := typeLocation{loc.tableName, loc.columnName + "." + origEntry.Name()}
			validateTypeInfoMatch(t, origEntry.Info(), loadedEntry.Info(), fieldLoc)
		}

	case *dukdb.MapDetails:
		loadedMap, ok := loadedDetails.(*dukdb.MapDetails)
		require.True(t, ok, "Type details mismatch for %s", loc)
		keyLoc := typeLocation{loc.tableName, loc.columnName + "[key]"}
		validateTypeInfoMatch(t, origDetails.Key, loadedMap.Key, keyLoc)
		valueLoc := typeLocation{loc.tableName, loc.columnName + "[value]"}
		validateTypeInfoMatch(t, origDetails.Value, loadedMap.Value, valueLoc)

	case *dukdb.EnumDetails:
		loadedEnum, ok := loadedDetails.(*dukdb.EnumDetails)
		require.True(t, ok, "Type details mismatch for %s", loc)
		assert.Equal(t, origDetails.Values, loadedEnum.Values,
			"Enum values mismatch for %s", loc)
	}
}

// TestCatalogSaveLoadEmptyCatalog tests saving and loading an empty catalog.
func TestCatalogSaveLoadEmptyCatalog(
	t *testing.T,
) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(
		tmpDir,
		"empty.duckdb",
	)

	// Create empty catalog (only has default "main" schema)
	originalCat := catalog.NewCatalog()

	// Save it
	err := format.SaveCatalogToDuckDBFormat(
		originalCat,
		dbPath,
	)
	require.NoError(t, err)

	// Load it back
	loadedCat, err := format.LoadCatalogFromDuckDBFormat(
		dbPath,
	)
	require.NoError(t, err)

	// Verify it has just the main schema with no tables
	schemas := loadedCat.ListSchemas()
	require.Equal(t, 1, len(schemas))
	assert.Equal(t, "main", schemas[0].Name())
	assert.Equal(
		t,
		0,
		len(schemas[0].ListTables()),
	)
}

// TestCatalogLoadNonexistentFile tests loading from a file that doesn't exist.
func TestCatalogLoadNonexistentFile(
	t *testing.T,
) {
	_, err := format.LoadCatalogFromDuckDBFormat(
		"/nonexistent/path/to/file.duckdb",
	)
	require.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"failed to open catalog file",
	)
}

// TestCatalogSaveInvalidPath tests saving to an invalid path.
func TestCatalogSaveInvalidPath(t *testing.T) {
	cat := catalog.NewCatalog()
	err := format.SaveCatalogToDuckDBFormat(
		cat,
		"/invalid/path/that/does/not/exist/file.duckdb",
	)
	require.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"failed to create catalog file",
	)
}
