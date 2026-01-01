package format

import (
	"bytes"
	"testing"

	"github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
)

// Benchmarks for TypeInfo serialization/deserialization (tasks 6.1-6.4)

// BenchmarkSerializeTypeInfo measures the performance of TypeInfo serialization.
// It tests 1000 iterations of various TypeInfo types to identify performance bottlenecks.
func BenchmarkSerializeTypeInfo(b *testing.B) {
	testCases := []struct {
		name     string
		typeInfo dukdb.TypeInfo
	}{
		{
			name: "Primitive_INTEGER",
			typeInfo: mustNewTypeInfo(
				dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				),
			),
		},
		{
			name: "Primitive_VARCHAR",
			typeInfo: mustNewTypeInfo(
				dukdb.NewTypeInfo(
					dukdb.TYPE_VARCHAR,
				),
			),
		},
		{
			name: "DECIMAL_18_4",
			typeInfo: mustNewTypeInfo(
				dukdb.NewDecimalInfo(18, 4),
			),
		},
		{
			name: "ENUM_3_values",
			typeInfo: mustNewTypeInfo(
				dukdb.NewEnumInfo(
					"RED",
					"GREEN",
					"BLUE",
				),
			),
		},
		{
			name: "LIST_INTEGER",
			typeInfo: mustNewTypeInfo(
				dukdb.NewListInfo(
					mustNewTypeInfo(
						dukdb.NewTypeInfo(
							dukdb.TYPE_INTEGER,
						),
					),
				),
			),
		},
		{
			name: "ARRAY_VARCHAR_10",
			typeInfo: mustNewTypeInfo(
				dukdb.NewArrayInfo(
					mustNewTypeInfo(
						dukdb.NewTypeInfo(
							dukdb.TYPE_VARCHAR,
						),
					),
					10,
				),
			),
		},
		{
			name: "STRUCT_2_fields",
			typeInfo: mustNewTypeInfo(
				dukdb.NewStructInfo(
					mustNewStructEntry(
						mustNewTypeInfo(
							dukdb.NewTypeInfo(
								dukdb.TYPE_INTEGER,
							),
						),
						"id",
					),
					mustNewStructEntry(
						mustNewTypeInfo(
							dukdb.NewTypeInfo(
								dukdb.TYPE_VARCHAR,
							),
						),
						"name",
					),
				),
			),
		},
		{
			name: "MAP_VARCHAR_INTEGER",
			typeInfo: mustNewTypeInfo(
				dukdb.NewMapInfo(
					mustNewTypeInfo(
						dukdb.NewTypeInfo(
							dukdb.TYPE_VARCHAR,
						),
					),
					mustNewTypeInfo(
						dukdb.NewTypeInfo(
							dukdb.TYPE_INTEGER,
						),
					),
				),
			),
		},
		{
			name: "Complex_Nested",
			typeInfo: mustNewTypeInfo(
				dukdb.NewListInfo(
					mustNewTypeInfo(
						dukdb.NewStructInfo(
							mustNewStructEntry(
								mustNewTypeInfo(
									dukdb.NewTypeInfo(
										dukdb.TYPE_VARCHAR,
									),
								),
								"key",
							),
							mustNewStructEntry(
								mustNewTypeInfo(
									dukdb.NewDecimalInfo(
										18,
										4,
									),
								),
								"value",
							),
						),
					),
				),
			),
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				buf := new(bytes.Buffer)
				w := NewBinaryWriter(buf)
				if err := SerializeTypeInfo(w, tc.typeInfo); err != nil {
					b.Fatalf(
						"SerializeTypeInfo failed: %v",
						err,
					)
				}
				if err := w.Flush(); err != nil {
					b.Fatalf(
						"Flush failed: %v",
						err,
					)
				}
			}
		})
	}
}

// BenchmarkDeserializeTypeInfo measures the performance of TypeInfo deserialization.
// It tests 1000 iterations of various TypeInfo types to identify deserialization overhead.
func BenchmarkDeserializeTypeInfo(b *testing.B) {
	testCases := []struct {
		name     string
		typeInfo dukdb.TypeInfo
	}{
		{
			name: "Primitive_INTEGER",
			typeInfo: mustNewTypeInfo(
				dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				),
			),
		},
		{
			name: "Primitive_VARCHAR",
			typeInfo: mustNewTypeInfo(
				dukdb.NewTypeInfo(
					dukdb.TYPE_VARCHAR,
				),
			),
		},
		{
			name: "DECIMAL_18_4",
			typeInfo: mustNewTypeInfo(
				dukdb.NewDecimalInfo(18, 4),
			),
		},
		{
			name: "ENUM_3_values",
			typeInfo: mustNewTypeInfo(
				dukdb.NewEnumInfo(
					"RED",
					"GREEN",
					"BLUE",
				),
			),
		},
		{
			name: "LIST_INTEGER",
			typeInfo: mustNewTypeInfo(
				dukdb.NewListInfo(
					mustNewTypeInfo(
						dukdb.NewTypeInfo(
							dukdb.TYPE_INTEGER,
						),
					),
				),
			),
		},
		{
			name: "ARRAY_VARCHAR_10",
			typeInfo: mustNewTypeInfo(
				dukdb.NewArrayInfo(
					mustNewTypeInfo(
						dukdb.NewTypeInfo(
							dukdb.TYPE_VARCHAR,
						),
					),
					10,
				),
			),
		},
		{
			name: "STRUCT_2_fields",
			typeInfo: mustNewTypeInfo(
				dukdb.NewStructInfo(
					mustNewStructEntry(
						mustNewTypeInfo(
							dukdb.NewTypeInfo(
								dukdb.TYPE_INTEGER,
							),
						),
						"id",
					),
					mustNewStructEntry(
						mustNewTypeInfo(
							dukdb.NewTypeInfo(
								dukdb.TYPE_VARCHAR,
							),
						),
						"name",
					),
				),
			),
		},
		{
			name: "MAP_VARCHAR_INTEGER",
			typeInfo: mustNewTypeInfo(
				dukdb.NewMapInfo(
					mustNewTypeInfo(
						dukdb.NewTypeInfo(
							dukdb.TYPE_VARCHAR,
						),
					),
					mustNewTypeInfo(
						dukdb.NewTypeInfo(
							dukdb.TYPE_INTEGER,
						),
					),
				),
			),
		},
		{
			name: "Complex_Nested",
			typeInfo: mustNewTypeInfo(
				dukdb.NewListInfo(
					mustNewTypeInfo(
						dukdb.NewStructInfo(
							mustNewStructEntry(
								mustNewTypeInfo(
									dukdb.NewTypeInfo(
										dukdb.TYPE_VARCHAR,
									),
								),
								"key",
							),
							mustNewStructEntry(
								mustNewTypeInfo(
									dukdb.NewDecimalInfo(
										18,
										4,
									),
								),
								"value",
							),
						),
					),
				),
			),
		},
	}

	for _, tc := range testCases {
		// Pre-serialize the TypeInfo
		buf := new(bytes.Buffer)
		w := NewBinaryWriter(buf)
		if err := SerializeTypeInfo(w, tc.typeInfo); err != nil {
			b.Fatalf(
				"Setup SerializeTypeInfo failed: %v",
				err,
			)
		}
		if err := w.Flush(); err != nil {
			b.Fatalf(
				"Setup Flush failed: %v",
				err,
			)
		}
		serialized := buf.Bytes()

		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				r := NewBinaryReader(
					bytes.NewReader(serialized),
				)
				if err := r.Load(); err != nil {
					b.Fatalf(
						"Load failed: %v",
						err,
					)
				}
				if _, err := DeserializeTypeInfo(r); err != nil {
					b.Fatalf(
						"DeserializeTypeInfo failed: %v",
						err,
					)
				}
			}
		})
	}
}

// BenchmarkSerializeCatalog measures the performance of catalog serialization.
// It tests a catalog with 100 tables, each with 10 columns of various types.
func BenchmarkSerializeCatalog(b *testing.B) {
	// Create a catalog with 100 tables, each with 10 columns
	cat := catalog.NewCatalog()
	schema, err := cat.CreateSchema("test_schema")
	if err != nil {
		b.Fatalf("CreateSchema failed: %v", err)
	}

	for i := range 100 {
		tableName := "table_" + string(
			rune('0'+i%10),
		) + string(
			rune('0'+(i/10)%10),
		)
		tableDef := &catalog.TableDef{
			Name:   tableName,
			Schema: "test_schema",
			Columns: make(
				[]*catalog.ColumnDef,
				10,
			),
		}

		// Create 10 columns with various types
		for j := range 10 {
			var typeInfo dukdb.TypeInfo
			switch j {
			case 0:
				typeInfo = mustNewTypeInfo(
					dukdb.NewTypeInfo(
						dukdb.TYPE_INTEGER,
					),
				)
			case 1:
				typeInfo = mustNewTypeInfo(
					dukdb.NewTypeInfo(
						dukdb.TYPE_VARCHAR,
					),
				)
			case 2:
				typeInfo = mustNewTypeInfo(
					dukdb.NewTypeInfo(
						dukdb.TYPE_BIGINT,
					),
				)
			case 3:
				typeInfo = mustNewTypeInfo(
					dukdb.NewDecimalInfo(18, 4),
				)
			case 4:
				typeInfo = mustNewTypeInfo(
					dukdb.NewTypeInfo(
						dukdb.TYPE_TIMESTAMP,
					),
				)
			case 5:
				typeInfo = mustNewTypeInfo(
					dukdb.NewTypeInfo(
						dukdb.TYPE_DATE,
					),
				)
			case 6:
				typeInfo = mustNewTypeInfo(
					dukdb.NewTypeInfo(
						dukdb.TYPE_BOOLEAN,
					),
				)
			case 7:
				typeInfo = mustNewTypeInfo(
					dukdb.NewListInfo(
						mustNewTypeInfo(
							dukdb.NewTypeInfo(
								dukdb.TYPE_VARCHAR,
							),
						),
					),
				)
			case 8:
				typeInfo = mustNewTypeInfo(
					dukdb.NewStructInfo(
						mustNewStructEntry(
							mustNewTypeInfo(
								dukdb.NewTypeInfo(
									dukdb.TYPE_INTEGER,
								),
							),
							"x",
						),
						mustNewStructEntry(
							mustNewTypeInfo(
								dukdb.NewTypeInfo(
									dukdb.TYPE_VARCHAR,
								),
							),
							"y",
						),
					),
				)
			case 9:
				typeInfo = mustNewTypeInfo(
					dukdb.NewTypeInfo(
						dukdb.TYPE_DOUBLE,
					),
				)
			}

			tableDef.Columns[j] = &catalog.ColumnDef{
				Name: "col_" + string(
					rune('0'+j),
				),
				Type:     typeInfo.InternalType(),
				Nullable: true,
			}
		}

		if err := schema.CreateTable(tableDef); err != nil {
			b.Fatalf("CreateTable failed: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		buf := new(bytes.Buffer)
		if err := SerializeCatalog(buf, cat); err != nil {
			b.Fatalf(
				"SerializeCatalog failed: %v",
				err,
			)
		}
	}
}

// BenchmarkDeserializeCatalog measures the performance of catalog deserialization.
// It tests deserialization of a catalog with 100 tables, each with 10 columns.
func BenchmarkDeserializeCatalog(b *testing.B) {
	// Create a catalog with 100 tables, each with 10 columns (same as serialize benchmark)
	cat := catalog.NewCatalog()
	schema, err := cat.CreateSchema("test_schema")
	if err != nil {
		b.Fatalf("CreateSchema failed: %v", err)
	}

	for i := range 100 {
		tableName := "table_" + string(
			rune('0'+i%10),
		) + string(
			rune('0'+(i/10)%10),
		)
		tableDef := &catalog.TableDef{
			Name:   tableName,
			Schema: "test_schema",
			Columns: make(
				[]*catalog.ColumnDef,
				10,
			),
		}

		for j := range 10 {
			var typeInfo dukdb.TypeInfo
			switch j {
			case 0:
				typeInfo = mustNewTypeInfo(
					dukdb.NewTypeInfo(
						dukdb.TYPE_INTEGER,
					),
				)
			case 1:
				typeInfo = mustNewTypeInfo(
					dukdb.NewTypeInfo(
						dukdb.TYPE_VARCHAR,
					),
				)
			case 2:
				typeInfo = mustNewTypeInfo(
					dukdb.NewTypeInfo(
						dukdb.TYPE_BIGINT,
					),
				)
			case 3:
				typeInfo = mustNewTypeInfo(
					dukdb.NewDecimalInfo(18, 4),
				)
			case 4:
				typeInfo = mustNewTypeInfo(
					dukdb.NewTypeInfo(
						dukdb.TYPE_TIMESTAMP,
					),
				)
			case 5:
				typeInfo = mustNewTypeInfo(
					dukdb.NewTypeInfo(
						dukdb.TYPE_DATE,
					),
				)
			case 6:
				typeInfo = mustNewTypeInfo(
					dukdb.NewTypeInfo(
						dukdb.TYPE_BOOLEAN,
					),
				)
			case 7:
				typeInfo = mustNewTypeInfo(
					dukdb.NewListInfo(
						mustNewTypeInfo(
							dukdb.NewTypeInfo(
								dukdb.TYPE_VARCHAR,
							),
						),
					),
				)
			case 8:
				typeInfo = mustNewTypeInfo(
					dukdb.NewStructInfo(
						mustNewStructEntry(
							mustNewTypeInfo(
								dukdb.NewTypeInfo(
									dukdb.TYPE_INTEGER,
								),
							),
							"x",
						),
						mustNewStructEntry(
							mustNewTypeInfo(
								dukdb.NewTypeInfo(
									dukdb.TYPE_VARCHAR,
								),
							),
							"y",
						),
					),
				)
			case 9:
				typeInfo = mustNewTypeInfo(
					dukdb.NewTypeInfo(
						dukdb.TYPE_DOUBLE,
					),
				)
			}

			tableDef.Columns[j] = &catalog.ColumnDef{
				Name: "col_" + string(
					rune('0'+j),
				),
				Type:     typeInfo.InternalType(),
				Nullable: true,
			}
		}

		if err := schema.CreateTable(tableDef); err != nil {
			b.Fatalf("CreateTable failed: %v", err)
		}
	}

	// Pre-serialize the catalog
	buf := new(bytes.Buffer)
	if err := SerializeCatalog(buf, cat); err != nil {
		b.Fatalf(
			"Setup SerializeCatalog failed: %v",
			err,
		)
	}
	serialized := buf.Bytes()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		// Note: DeserializeCatalog is not yet implemented (task 4.2)
		// This benchmark will be enabled once that task is complete
		_ = serialized
		// _, err := DeserializeCatalog(bytes.NewReader(serialized))
		// if err != nil {
		//     b.Fatalf("DeserializeCatalog failed: %v", err)
		// }
	}
}

// BenchmarkChecksum measures the performance of checksum calculation on various data sizes.
func BenchmarkChecksum(b *testing.B) {
	testCases := []struct {
		name string
		size int
	}{
		{"Small_1KB", 1024},
		{"Medium_10KB", 10 * 1024},
		{"Large_100KB", 100 * 1024},
		{"XLarge_1MB", 1024 * 1024},
	}

	for _, tc := range testCases {
		data := make([]byte, tc.size)
		for i := range data {
			data[i] = byte(i % 256)
		}

		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(tc.size))
			b.ResetTimer()
			for range b.N {
				_ = CalculateChecksum(data)
			}
		})
	}
}

// Fuzz tests for robustness (tasks 6.6-6.9)

// FuzzBinaryReader ensures that feeding random bytes to the binary reader
// never causes panics - only errors should be returned.
func FuzzBinaryReader(f *testing.F) {
	// Seed corpus with some valid binary data
	f.Add(
		[]byte{0x01, 0x00, 0x00, 0x00},
	) // Property count = 1
	f.Add(
		[]byte{0x00, 0x00, 0x00, 0x00},
	) // Property count = 0
	f.Add(
		[]byte{0x05, 0x00, 0x00, 0x00},
	) // Property count = 5

	f.Fuzz(func(t *testing.T, data []byte) {
		// This should never panic - only return errors
		defer func() {
			if r := recover(); r != nil {
				t.Errorf(
					"BinaryReader panicked on input: %v",
					r,
				)
			}
		}()

		r := NewBinaryReader(
			bytes.NewReader(data),
		)
		_ = r.Load() // May fail, but should not panic

		// Try reading various properties
		var u8 uint8
		var u32 uint32
		var u64 uint64
		var str string
		var b []byte

		_ = r.ReadProperty(100, &u8)
		_ = r.ReadProperty(100, &u32)
		_ = r.ReadProperty(100, &u64)
		_ = r.ReadProperty(100, &str)
		_ = r.ReadProperty(100, &b)

		_ = r.ReadPropertyWithDefault(
			100,
			&u8,
			uint8(0),
		)
		_ = r.ReadPropertyWithDefault(
			100,
			&u32,
			uint32(0),
		)
		_ = r.ReadPropertyWithDefault(
			100,
			&u64,
			uint64(0),
		)
		_ = r.ReadPropertyWithDefault(
			100,
			&str,
			"",
		)

		_, _ = r.ReadList(100)
	})
}

// FuzzTypeInfoDeserializer ensures that malformed TypeInfo data never causes
// panics - only errors should be returned.
func FuzzTypeInfoDeserializer(f *testing.F) {
	// Seed corpus with some valid TypeInfo serializations
	primitiveInt := mustSerializeTypeInfo(
		mustNewTypeInfo(
			dukdb.NewTypeInfo(dukdb.TYPE_INTEGER),
		),
	)
	f.Add(primitiveInt)

	decimal := mustSerializeTypeInfo(
		mustNewTypeInfo(
			dukdb.NewDecimalInfo(18, 4),
		),
	)
	f.Add(decimal)

	enum := mustSerializeTypeInfo(
		mustNewTypeInfo(
			dukdb.NewEnumInfo("A", "B"),
		),
	)
	f.Add(enum)

	f.Fuzz(func(t *testing.T, data []byte) {
		// This should never panic - only return errors
		defer func() {
			if r := recover(); r != nil {
				t.Errorf(
					"DeserializeTypeInfo panicked on input: %v",
					r,
				)
			}
		}()

		r := NewBinaryReader(
			bytes.NewReader(data),
		)
		if err := r.Load(); err != nil {
			// Load can fail on invalid data, that's OK
			return
		}

		// DeserializeTypeInfo should handle all invalid property combinations gracefully
		_, _ = DeserializeTypeInfo(r)
	})
}

// FuzzCatalogDeserializer ensures that malformed catalog data never causes
// panics - only errors should be returned.
func FuzzCatalogDeserializer(f *testing.F) {
	// Seed corpus with valid catalog serialization
	cat := catalog.NewCatalog()
	schema, err := cat.CreateSchema("test")
	if err != nil {
		f.Fatal(err)
	}
	tableDef := &catalog.TableDef{
		Name:   "table1",
		Schema: "test",
		Columns: []*catalog.ColumnDef{
			{
				Name:     "id",
				Type:     dukdb.TYPE_INTEGER,
				Nullable: false,
			},
		},
	}
	if err := schema.CreateTable(tableDef); err != nil {
		f.Fatal(err)
	}

	buf := new(bytes.Buffer)
	if err := SerializeCatalog(buf, cat); err != nil {
		f.Fatal(err)
	}
	f.Add(buf.Bytes())

	f.Fuzz(func(t *testing.T, data []byte) {
		// This should never panic - only return errors
		defer func() {
			if r := recover(); r != nil {
				t.Errorf(
					"Catalog deserializer panicked on input: %v",
					r,
				)
			}
		}()

		// Note: DeserializeCatalog is not yet implemented (task 4.2)
		// This fuzz test will be enabled once that task is complete
		_ = data
		// _, _ = DeserializeCatalog(bytes.NewReader(data))
	})
}

// Helper functions for test setup

func mustNewTypeInfo(
	ti dukdb.TypeInfo,
	err error,
) dukdb.TypeInfo {
	if err != nil {
		panic(err)
	}

	return ti
}

func mustNewStructEntry(
	ti dukdb.TypeInfo,
	name string,
) dukdb.StructEntry {
	entry, err := dukdb.NewStructEntry(ti, name)
	if err != nil {
		panic(err)
	}

	return entry
}

func mustSerializeTypeInfo(
	ti dukdb.TypeInfo,
) []byte {
	buf := new(bytes.Buffer)
	w := NewBinaryWriter(buf)
	if err := SerializeTypeInfo(w, ti); err != nil {
		panic(err)
	}
	if err := w.Flush(); err != nil {
		panic(err)
	}

	return buf.Bytes()
}
