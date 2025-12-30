package format

import (
	"bytes"
	"os"
	"testing"

	"github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeserializeColumn(t *testing.T) {
	tests := []struct {
		name     string
		column   *catalog.ColumnDef
		wantName string
		wantType dukdb.Type
	}{
		{
			name: "integer column nullable",
			column: &catalog.ColumnDef{
				Name:     "id",
				Type:     dukdb.TYPE_INTEGER,
				Nullable: true,
			},
			wantName: "id",
			wantType: dukdb.TYPE_INTEGER,
		},
		{
			name: "varchar column not nullable",
			column: &catalog.ColumnDef{
				Name:     "name",
				Type:     dukdb.TYPE_VARCHAR,
				Nullable: false,
			},
			wantName: "name",
			wantType: dukdb.TYPE_VARCHAR,
		},
		{
			name: "decimal column",
			column: func() *catalog.ColumnDef {
				info, _ := dukdb.NewDecimalInfo(18, 4)
				return &catalog.ColumnDef{
					Name:     "price",
					Type:     dukdb.TYPE_DECIMAL,
					TypeInfo: info,
					Nullable: true,
				}
			}(),
			wantName: "price",
			wantType: dukdb.TYPE_DECIMAL,
		},
		{
			name: "list column",
			column: func() *catalog.ColumnDef {
				childInfo, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
				listInfo, _ := dukdb.NewListInfo(childInfo)
				return &catalog.ColumnDef{
					Name:     "numbers",
					Type:     dukdb.TYPE_LIST,
					TypeInfo: listInfo,
					Nullable: true,
				}
			}(),
			wantName: "numbers",
			wantType: dukdb.TYPE_LIST,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize the column
			var buf bytes.Buffer
			err := SerializeColumn(&buf, tt.column)
			require.NoError(t, err)

			// Deserialize the column
			col, err := DeserializeColumn(&buf)
			require.NoError(t, err)
			assert.NotNil(t, col)

			// Verify column properties
			assert.Equal(t, tt.wantName, col.Name)
			assert.Equal(t, tt.wantType, col.Type)
			assert.Equal(t, tt.column.Nullable, col.Nullable)

			// Verify TypeInfo
			assert.NotNil(t, col.TypeInfo)
			assert.Equal(t, tt.wantType, col.TypeInfo.InternalType())
		})
	}
}

func TestDeserializeTableEntry(t *testing.T) {
	tests := []struct {
		name       string
		table      *catalog.TableDef
		wantName   string
		wantSchema string
		wantCols   int
	}{
		{
			name: "simple table with two columns",
			table: catalog.NewTableDef("users", []*catalog.ColumnDef{
				{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
				{Name: "name", Type: dukdb.TYPE_VARCHAR, Nullable: true},
			}),
			wantName:   "users",
			wantSchema: "",
			wantCols:   2,
		},
		{
			name: "table with schema",
			table: func() *catalog.TableDef {
				t := catalog.NewTableDef("products", []*catalog.ColumnDef{
					{Name: "id", Type: dukdb.TYPE_BIGINT, Nullable: false},
					{Name: "name", Type: dukdb.TYPE_VARCHAR, Nullable: true},
					{Name: "price", Type: dukdb.TYPE_DOUBLE, Nullable: true},
				})
				t.Schema = "sales"
				return t
			}(),
			wantName:   "products",
			wantSchema: "sales",
			wantCols:   3,
		},
		{
			name: "table with complex types",
			table: catalog.NewTableDef("complex_table", []*catalog.ColumnDef{
				{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
				func() *catalog.ColumnDef {
					info, _ := dukdb.NewDecimalInfo(18, 4)
					return &catalog.ColumnDef{
						Name:     "amount",
						Type:     dukdb.TYPE_DECIMAL,
						TypeInfo: info,
						Nullable: true,
					}
				}(),
				func() *catalog.ColumnDef {
					childInfo, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)
					listInfo, _ := dukdb.NewListInfo(childInfo)
					return &catalog.ColumnDef{
						Name:     "tags",
						Type:     dukdb.TYPE_LIST,
						TypeInfo: listInfo,
						Nullable: true,
					}
				}(),
			}),
			wantName:   "complex_table",
			wantSchema: "",
			wantCols:   3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize the table
			var buf bytes.Buffer
			err := SerializeTableEntry(&buf, tt.table)
			require.NoError(t, err)

			// Deserialize the table
			table, err := DeserializeTableEntry(&buf)
			require.NoError(t, err)
			assert.NotNil(t, table)

			// Verify table properties
			assert.Equal(t, tt.wantName, table.Name)
			assert.Equal(t, tt.wantSchema, table.Schema)
			assert.Equal(t, tt.wantCols, len(table.Columns))

			// Verify each column matches
			for i, expectedCol := range tt.table.Columns {
				actualCol := table.Columns[i]
				assert.Equal(t, expectedCol.Name, actualCol.Name)
				assert.Equal(t, expectedCol.Type, actualCol.Type)
				assert.Equal(t, expectedCol.Nullable, actualCol.Nullable)
			}
		})
	}
}

func TestDeserializeSchema(t *testing.T) {
	tests := []struct {
		name       string
		schema     *catalog.Schema
		wantName   string
		wantTables int
	}{
		{
			name:       "empty schema",
			schema:     catalog.NewSchema("empty"),
			wantName:   "empty",
			wantTables: 0,
		},
		{
			name: "schema with single table",
			schema: func() *catalog.Schema {
				s := catalog.NewSchema("test")
				table := catalog.NewTableDef("users", []*catalog.ColumnDef{
					{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
					{Name: "name", Type: dukdb.TYPE_VARCHAR, Nullable: true},
				})
				_ = s.CreateTable(table)
				return s
			}(),
			wantName:   "test",
			wantTables: 1,
		},
		{
			name: "schema with multiple tables",
			schema: func() *catalog.Schema {
				s := catalog.NewSchema("multi")
				table1 := catalog.NewTableDef("users", []*catalog.ColumnDef{
					{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
				})
				table2 := catalog.NewTableDef("products", []*catalog.ColumnDef{
					{Name: "id", Type: dukdb.TYPE_BIGINT, Nullable: false},
					{Name: "name", Type: dukdb.TYPE_VARCHAR, Nullable: true},
				})
				_ = s.CreateTable(table1)
				_ = s.CreateTable(table2)
				return s
			}(),
			wantName:   "multi",
			wantTables: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize the schema
			var buf bytes.Buffer
			err := SerializeSchema(&buf, tt.schema)
			require.NoError(t, err)

			// Deserialize the schema
			schema, err := DeserializeSchema(&buf)
			require.NoError(t, err)
			assert.NotNil(t, schema)

			// Verify schema properties
			assert.Equal(t, tt.wantName, schema.Name())
			assert.Equal(t, tt.wantTables, len(schema.ListTables()))

			// Verify table names match
			originalTables := tt.schema.ListTables()
			deserializedTables := schema.ListTables()
			if len(originalTables) > 0 {
				originalNames := make(map[string]bool)
				for _, table := range originalTables {
					originalNames[table.Name] = true
				}
				for _, table := range deserializedTables {
					assert.True(t, originalNames[table.Name], "table %s not found in original schema", table.Name)
				}
			}
		})
	}
}

func TestDeserializeCatalog(t *testing.T) {
	tests := []struct {
		name         string
		setupCatalog func() *catalog.Catalog
		wantSchemas  int
		wantTables   map[string]int // schema name -> table count
	}{
		{
			name: "catalog with main schema only",
			setupCatalog: func() *catalog.Catalog {
				cat := catalog.NewCatalog()
				table := catalog.NewTableDef("users", []*catalog.ColumnDef{
					{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
					{Name: "name", Type: dukdb.TYPE_VARCHAR, Nullable: true},
				})
				_ = cat.CreateTable(table)
				return cat
			},
			wantSchemas: 1,
			wantTables: map[string]int{
				"main": 1,
			},
		},
		{
			name: "catalog with multiple schemas",
			setupCatalog: func() *catalog.Catalog {
				cat := catalog.NewCatalog()

				// Main schema
				mainTable := catalog.NewTableDef("users", []*catalog.ColumnDef{
					{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
				})
				_ = cat.CreateTable(mainTable)

				// Custom schema
				_, _ = cat.CreateSchema("sales")
				salesTable := catalog.NewTableDef("orders", []*catalog.ColumnDef{
					{Name: "id", Type: dukdb.TYPE_BIGINT, Nullable: false},
					{Name: "total", Type: dukdb.TYPE_DOUBLE, Nullable: true},
				})
				_ = cat.CreateTableInSchema("sales", salesTable)

				return cat
			},
			wantSchemas: 2,
			wantTables: map[string]int{
				"main":  1,
				"sales": 1,
			},
		},
		{
			name: "catalog with complex column types",
			setupCatalog: func() *catalog.Catalog {
				cat := catalog.NewCatalog()

				decimalInfo, _ := dukdb.NewDecimalInfo(18, 4)
				childInfo, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
				listInfo, _ := dukdb.NewListInfo(childInfo)

				table := catalog.NewTableDef("complex_table", []*catalog.ColumnDef{
					{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
					{
						Name:     "amount",
						Type:     dukdb.TYPE_DECIMAL,
						TypeInfo: decimalInfo,
						Nullable: true,
					},
					{
						Name:     "numbers",
						Type:     dukdb.TYPE_LIST,
						TypeInfo: listInfo,
						Nullable: true,
					},
				})
				_ = cat.CreateTable(table)
				return cat
			},
			wantSchemas: 1,
			wantTables: map[string]int{
				"main": 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			originalCat := tt.setupCatalog()

			// Serialize the catalog
			var buf bytes.Buffer
			err := SerializeCatalog(&buf, originalCat)
			require.NoError(t, err)

			// Deserialize the catalog
			cat, err := DeserializeCatalog(&buf)
			require.NoError(t, err)
			assert.NotNil(t, cat)

			// Verify schemas
			for schemaName, expectedTableCount := range tt.wantTables {
				schema, ok := cat.GetSchema(schemaName)
				assert.True(t, ok, "schema %s not found", schemaName)
				if ok {
					tables := schema.ListTables()
					assert.Equal(t, expectedTableCount, len(tables),
						"schema %s should have %d tables, got %d",
						schemaName, expectedTableCount, len(tables))
				}
			}

			// Verify tables have correct columns
			for schemaName := range tt.wantTables {
				originalSchema, _ := originalCat.GetSchema(schemaName)
				deserializedSchema, _ := cat.GetSchema(schemaName)

				originalTables := originalSchema.ListTables()
				for _, originalTable := range originalTables {
					deserializedTable, ok := deserializedSchema.GetTable(originalTable.Name)
					assert.True(t, ok, "table %s not found in schema %s", originalTable.Name, schemaName)
					if ok {
						assert.Equal(t, len(originalTable.Columns), len(deserializedTable.Columns),
							"table %s column count mismatch", originalTable.Name)

						for i, originalCol := range originalTable.Columns {
							deserializedCol := deserializedTable.Columns[i]
							assert.Equal(t, originalCol.Name, deserializedCol.Name,
								"column %d name mismatch in table %s", i, originalTable.Name)
							assert.Equal(t, originalCol.Type, deserializedCol.Type,
								"column %s type mismatch in table %s", originalCol.Name, originalTable.Name)
							assert.Equal(t, originalCol.Nullable, deserializedCol.Nullable,
								"column %s nullable mismatch in table %s", originalCol.Name, originalTable.Name)
						}
					}
				}
			}
		})
	}
}

func TestDeserializeColumn_ErrorHandling(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantErr bool
	}{
		{
			name:    "empty input",
			input:   []byte{},
			wantErr: true,
		},
		{
			name: "missing required property",
			input: func() []byte {
				var buf bytes.Buffer
				bw := NewBinaryWriter(&buf)
				// Only write property 100, missing property 101 (TypeInfo)
				_ = bw.WriteProperty(100, "test_col")
				_ = bw.Flush()
				return buf.Bytes()
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewReader(tt.input)
			_, err := DeserializeColumn(buf)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDeserializeTableEntry_ErrorHandling(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantErr bool
	}{
		{
			name:    "empty input",
			input:   []byte{},
			wantErr: true,
		},
		{
			name: "wrong entry type",
			input: func() []byte {
				var buf bytes.Buffer
				bw := NewBinaryWriter(&buf)
				// Write SCHEMA type instead of TABLE
				_ = bw.WriteProperty(100, uint32(CatalogEntryType_SCHEMA))
				_ = bw.WriteProperty(101, "test_table")
				_ = bw.WriteProperty(102, "main")
				_ = bw.WriteProperty(200, uint64(0))
				_ = bw.Flush()
				return buf.Bytes()
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewReader(tt.input)
			_, err := DeserializeTableEntry(buf)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDeserializeSchema_ErrorHandling(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantErr bool
	}{
		{
			name:    "empty input",
			input:   []byte{},
			wantErr: true,
		},
		{
			name: "wrong entry type",
			input: func() []byte {
				var buf bytes.Buffer
				bw := NewBinaryWriter(&buf)
				// Write TABLE type instead of SCHEMA
				_ = bw.WriteProperty(100, uint32(CatalogEntryType_TABLE))
				_ = bw.WriteProperty(101, "test_schema")
				_ = bw.WriteProperty(200, uint64(0))
				_ = bw.Flush()
				return buf.Bytes()
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewReader(tt.input)
			_, err := DeserializeSchema(buf)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCatalogRoundTrip_ComplexTypes(t *testing.T) {
	// Create a catalog with various complex types
	cat := catalog.NewCatalog()

	// Decimal column
	decimalInfo, err := dukdb.NewDecimalInfo(18, 4)
	require.NoError(t, err)

	// List column
	listChildInfo, err := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)
	require.NoError(t, err)
	listInfo, err := dukdb.NewListInfo(listChildInfo)
	require.NoError(t, err)

	// Struct column
	structField1, err := dukdb.NewStructEntry(
		func() dukdb.TypeInfo { info, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER); return info }(),
		"x",
	)
	require.NoError(t, err)
	structField2, err := dukdb.NewStructEntry(
		func() dukdb.TypeInfo { info, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR); return info }(),
		"y",
	)
	require.NoError(t, err)
	structInfo, err := dukdb.NewStructInfo(structField1, structField2)
	require.NoError(t, err)

	// Enum column
	enumInfo, err := dukdb.NewEnumInfo("RED", "GREEN", "BLUE")
	require.NoError(t, err)

	// Create table with all these types
	table := catalog.NewTableDef("complex_types", []*catalog.ColumnDef{
		{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
		{Name: "price", Type: dukdb.TYPE_DECIMAL, TypeInfo: decimalInfo, Nullable: true},
		{Name: "tags", Type: dukdb.TYPE_LIST, TypeInfo: listInfo, Nullable: true},
		{Name: "point", Type: dukdb.TYPE_STRUCT, TypeInfo: structInfo, Nullable: true},
		{Name: "color", Type: dukdb.TYPE_ENUM, TypeInfo: enumInfo, Nullable: true},
	})

	err = cat.CreateTable(table)
	require.NoError(t, err)

	// Serialize
	var buf bytes.Buffer
	err = SerializeCatalog(&buf, cat)
	require.NoError(t, err)

	// Deserialize
	deserializedCat, err := DeserializeCatalog(&buf)
	require.NoError(t, err)

	// Verify
	deserializedTable, ok := deserializedCat.GetTable("complex_types")
	assert.True(t, ok)
	assert.Equal(t, 5, len(deserializedTable.Columns))

	// Verify each column type
	assert.Equal(t, dukdb.TYPE_INTEGER, deserializedTable.Columns[0].Type)
	assert.Equal(t, dukdb.TYPE_DECIMAL, deserializedTable.Columns[1].Type)
	assert.Equal(t, dukdb.TYPE_LIST, deserializedTable.Columns[2].Type)
	assert.Equal(t, dukdb.TYPE_STRUCT, deserializedTable.Columns[3].Type)
	assert.Equal(t, dukdb.TYPE_ENUM, deserializedTable.Columns[4].Type)

	// Verify decimal details
	decimalDetails := deserializedTable.Columns[1].TypeInfo.Details().(*dukdb.DecimalDetails)
	assert.Equal(t, uint8(18), decimalDetails.Width)
	assert.Equal(t, uint8(4), decimalDetails.Scale)

	// Verify list details
	listDetails := deserializedTable.Columns[2].TypeInfo.Details().(*dukdb.ListDetails)
	assert.Equal(t, dukdb.TYPE_VARCHAR, listDetails.Child.InternalType())

	// Verify struct details
	structDetails := deserializedTable.Columns[3].TypeInfo.Details().(*dukdb.StructDetails)
	assert.Equal(t, 2, len(structDetails.Entries))
	assert.Equal(t, "x", structDetails.Entries[0].Name())
	assert.Equal(t, "y", structDetails.Entries[1].Name())

	// Verify enum details
	enumDetails := deserializedTable.Columns[4].TypeInfo.Details().(*dukdb.EnumDetails)
	assert.Equal(t, 3, len(enumDetails.Values))
	assert.Contains(t, enumDetails.Values, "RED")
	assert.Contains(t, enumDetails.Values, "GREEN")
	assert.Contains(t, enumDetails.Values, "BLUE")
}

func TestSaveCatalogToDuckDBFormat(t *testing.T) {
	// Create a test catalog
	cat := catalog.NewCatalog()
	table := catalog.NewTableDef("users", []*catalog.ColumnDef{
		{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
		{Name: "name", Type: dukdb.TYPE_VARCHAR, Nullable: true},
	})
	err := cat.CreateTable(table)
	require.NoError(t, err)

	// Create temporary file
	tmpFile := t.TempDir() + "/test_catalog.duckdb"

	// Save catalog
	err = SaveCatalogToDuckDBFormat(cat, tmpFile)
	require.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(tmpFile)
	assert.NoError(t, err)

	// Load catalog back
	loadedCat, err := LoadCatalogFromDuckDBFormat(tmpFile)
	require.NoError(t, err)
	assert.NotNil(t, loadedCat)

	// Verify loaded catalog
	loadedTable, ok := loadedCat.GetTable("users")
	assert.True(t, ok)
	assert.Equal(t, "users", loadedTable.Name)
	assert.Equal(t, 2, len(loadedTable.Columns))
}

func TestLoadCatalogFromDuckDBFormat(t *testing.T) {
	tests := []struct {
		name         string
		setupCatalog func() *catalog.Catalog
		verify       func(*testing.T, *catalog.Catalog)
	}{
		{
			name: "simple catalog",
			setupCatalog: func() *catalog.Catalog {
				cat := catalog.NewCatalog()
				table := catalog.NewTableDef("test_table", []*catalog.ColumnDef{
					{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
				})
				_ = cat.CreateTable(table)
				return cat
			},
			verify: func(t *testing.T, cat *catalog.Catalog) {
				table, ok := cat.GetTable("test_table")
				assert.True(t, ok)
				assert.Equal(t, 1, len(table.Columns))
			},
		},
		{
			name: "catalog with multiple schemas",
			setupCatalog: func() *catalog.Catalog {
				cat := catalog.NewCatalog()

				// Main schema
				table1 := catalog.NewTableDef("users", []*catalog.ColumnDef{
					{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
				})
				_ = cat.CreateTable(table1)

				// Sales schema
				_, _ = cat.CreateSchema("sales")
				table2 := catalog.NewTableDef("orders", []*catalog.ColumnDef{
					{Name: "id", Type: dukdb.TYPE_BIGINT, Nullable: false},
				})
				_ = cat.CreateTableInSchema("sales", table2)

				return cat
			},
			verify: func(t *testing.T, cat *catalog.Catalog) {
				// Verify main schema
				mainTable, ok := cat.GetTable("users")
				assert.True(t, ok)
				assert.Equal(t, "users", mainTable.Name)

				// Verify sales schema
				salesTable, ok := cat.GetTableInSchema("sales", "orders")
				assert.True(t, ok)
				assert.Equal(t, "orders", salesTable.Name)
			},
		},
		{
			name: "catalog with complex types",
			setupCatalog: func() *catalog.Catalog {
				cat := catalog.NewCatalog()

				decimalInfo, _ := dukdb.NewDecimalInfo(18, 4)
				childInfo, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)
				listInfo, _ := dukdb.NewListInfo(childInfo)

				table := catalog.NewTableDef("complex", []*catalog.ColumnDef{
					{Name: "id", Type: dukdb.TYPE_INTEGER, Nullable: false},
					{
						Name:     "amount",
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
				})
				_ = cat.CreateTable(table)
				return cat
			},
			verify: func(t *testing.T, cat *catalog.Catalog) {
				table, ok := cat.GetTable("complex")
				assert.True(t, ok)
				assert.Equal(t, 3, len(table.Columns))

				// Verify decimal column
				assert.Equal(t, dukdb.TYPE_DECIMAL, table.Columns[1].Type)
				decimalDetails := table.Columns[1].TypeInfo.Details().(*dukdb.DecimalDetails)
				assert.Equal(t, uint8(18), decimalDetails.Width)
				assert.Equal(t, uint8(4), decimalDetails.Scale)

				// Verify list column
				assert.Equal(t, dukdb.TYPE_LIST, table.Columns[2].Type)
				listDetails := table.Columns[2].TypeInfo.Details().(*dukdb.ListDetails)
				assert.Equal(t, dukdb.TYPE_VARCHAR, listDetails.Child.InternalType())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary file
			tmpFile := t.TempDir() + "/test.duckdb"

			// Save catalog
			originalCat := tt.setupCatalog()
			err := SaveCatalogToDuckDBFormat(originalCat, tmpFile)
			require.NoError(t, err)

			// Load catalog
			loadedCat, err := LoadCatalogFromDuckDBFormat(tmpFile)
			require.NoError(t, err)
			assert.NotNil(t, loadedCat)

			// Verify catalog
			tt.verify(t, loadedCat)
		})
	}
}

func TestLoadCatalogFromDuckDBFormat_InvalidFiles(t *testing.T) {
	tests := []struct {
		name      string
		setupFile func(string) error
		wantErr   bool
	}{
		{
			name: "file does not exist",
			setupFile: func(path string) error {
				// Don't create the file
				return nil
			},
			wantErr: true,
		},
		{
			name: "invalid magic number",
			setupFile: func(path string) error {
				f, err := os.Create(path)
				if err != nil {
					return err
				}
				defer func() {
					_ = f.Close()
				}()
				// Write invalid magic number
				_, err = f.Write([]byte{0x00, 0x00, 0x00, 0x00})
				return err
			},
			wantErr: true,
		},
		{
			name: "invalid format version",
			setupFile: func(path string) error {
				f, err := os.Create(path)
				if err != nil {
					return err
				}
				defer func() {
					_ = f.Close()
				}()
				// Write correct magic but wrong version
				_ = WriteHeader(f)
				// Overwrite version with incorrect value by seeking back
				_, _ = f.Seek(4, 0)
				_, err = f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
				return err
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpFile := t.TempDir() + "/invalid.duckdb"

			if tt.name != "file does not exist" {
				err := tt.setupFile(tmpFile)
				require.NoError(t, err)
			}

			_, err := LoadCatalogFromDuckDBFormat(tmpFile)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSaveAndLoadCatalog_EmptySchema(t *testing.T) {
	// Create catalog with empty main schema
	cat := catalog.NewCatalog()

	// Create temporary file
	tmpFile := t.TempDir() + "/empty.duckdb"

	// Save catalog
	err := SaveCatalogToDuckDBFormat(cat, tmpFile)
	require.NoError(t, err)

	// Load catalog
	loadedCat, err := LoadCatalogFromDuckDBFormat(tmpFile)
	require.NoError(t, err)

	// Verify main schema exists but has no tables
	schema, ok := loadedCat.GetSchema("main")
	assert.True(t, ok)
	assert.Equal(t, 0, len(schema.ListTables()))
}
