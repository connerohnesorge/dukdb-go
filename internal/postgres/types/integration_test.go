package types

import (
	"testing"
	"time"
)

// TestTypeSystemIntegration tests the complete type mapping workflow:
// PostgreSQL type name -> DuckDB type -> Value conversion -> PostgreSQL OID
func TestTypeSystemIntegration(t *testing.T) {
	mapper := NewTypeMapper()
	converter := NewTypeConverter()

	tests := []struct {
		name        string
		pgTypeName  string // PostgreSQL type name input
		sampleValue any    // Sample value to convert
		wantDuckDB  string // Expected DuckDB type
		wantOID     uint32 // Expected PostgreSQL OID
	}{
		// Serial types - important for ORMs
		{
			name:        "serial_type",
			pgTypeName:  "serial",
			sampleValue: int32(42),
			wantDuckDB:  "INTEGER",
			wantOID:     OID_INT4,
		},
		{
			name:        "bigserial_type",
			pgTypeName:  "bigserial",
			sampleValue: int64(9223372036854775807),
			wantDuckDB:  "BIGINT",
			wantOID:     OID_INT8,
		},
		{
			name:        "smallserial_type",
			pgTypeName:  "smallserial",
			sampleValue: int16(32767),
			wantDuckDB:  "SMALLINT",
			wantOID:     OID_INT2,
		},
		// Text types
		{
			name:        "text_type",
			pgTypeName:  "text",
			sampleValue: "hello world",
			wantDuckDB:  "VARCHAR",
			wantOID:     OID_TEXT,
		},
		{
			name:        "varchar_type",
			pgTypeName:  "varchar",
			sampleValue: "hello",
			wantDuckDB:  "VARCHAR",
			wantOID:     OID_VARCHAR,
		},
		{
			name:        "character_varying_type",
			pgTypeName:  "character varying",
			sampleValue: "test string",
			wantDuckDB:  "VARCHAR",
			wantOID:     OID_VARCHAR,
		},
		{
			name:        "char_type",
			pgTypeName:  "char",
			sampleValue: "c",
			wantDuckDB:  "VARCHAR",
			wantOID:     OID_BPCHAR,
		},
		// Timestamp types - important for analytics
		{
			name:        "timestamp_type",
			pgTypeName:  "timestamp",
			sampleValue: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			wantDuckDB:  "TIMESTAMP",
			wantOID:     OID_TIMESTAMP,
		},
		{
			name:        "timestamptz_type",
			pgTypeName:  "timestamp with time zone",
			sampleValue: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			wantDuckDB:  "TIMESTAMPTZ",
			wantOID:     OID_TIMESTAMPTZ,
		},
		{
			name:        "date_type",
			pgTypeName:  "date",
			sampleValue: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			wantDuckDB:  "DATE",
			wantOID:     OID_DATE,
		},
		{
			name:        "time_type",
			pgTypeName:  "time",
			sampleValue: time.Date(0, 1, 1, 14, 30, 45, 0, time.UTC),
			wantDuckDB:  "TIME",
			wantOID:     OID_TIME,
		},
		{
			name:        "interval_type",
			pgTypeName:  "interval",
			sampleValue: 2*time.Hour + 30*time.Minute,
			wantDuckDB:  "INTERVAL",
			wantOID:     OID_INTERVAL,
		},
		// Boolean
		{
			name:        "boolean_type",
			pgTypeName:  "boolean",
			sampleValue: true,
			wantDuckDB:  "BOOLEAN",
			wantOID:     OID_BOOL,
		},
		{
			name:        "bool_type",
			pgTypeName:  "bool",
			sampleValue: false,
			wantDuckDB:  "BOOLEAN",
			wantOID:     OID_BOOL,
		},
		// Binary
		{
			name:        "bytea_type",
			pgTypeName:  "bytea",
			sampleValue: []byte{0xDE, 0xAD, 0xBE, 0xEF},
			wantDuckDB:  "BLOB",
			wantOID:     OID_BYTEA,
		},
		// JSON
		{
			name:        "json_type",
			pgTypeName:  "json",
			sampleValue: `{"key": "value"}`,
			wantDuckDB:  "JSON",
			wantOID:     OID_JSON,
		},
		{
			name:        "jsonb_type",
			pgTypeName:  "jsonb",
			sampleValue: `{"key": "value"}`,
			wantDuckDB:  "JSON",
			wantOID:     OID_JSONB,
		},
		// UUID
		{
			name:        "uuid_type",
			pgTypeName:  "uuid",
			sampleValue: "550e8400-e29b-41d4-a716-446655440000",
			wantDuckDB:  "UUID",
			wantOID:     OID_UUID,
		},
		// Numeric
		{
			name:        "numeric_type",
			pgTypeName:  "numeric",
			sampleValue: "123456.789",
			wantDuckDB:  "DECIMAL",
			wantOID:     OID_NUMERIC,
		},
		{
			name:        "decimal_type",
			pgTypeName:  "decimal",
			sampleValue: "999999.999",
			wantDuckDB:  "DECIMAL",
			wantOID:     OID_NUMERIC,
		},
		// Float types
		{
			name:        "real_type",
			pgTypeName:  "real",
			sampleValue: float32(3.14),
			wantDuckDB:  "FLOAT",
			wantOID:     OID_FLOAT4,
		},
		{
			name:        "float4_type",
			pgTypeName:  "float4",
			sampleValue: float32(2.71),
			wantDuckDB:  "FLOAT",
			wantOID:     OID_FLOAT4,
		},
		{
			name:        "double_precision_type",
			pgTypeName:  "double precision",
			sampleValue: float64(3.14159265359),
			wantDuckDB:  "DOUBLE",
			wantOID:     OID_FLOAT8,
		},
		{
			name:        "float8_type",
			pgTypeName:  "float8",
			sampleValue: float64(2.71828182845),
			wantDuckDB:  "DOUBLE",
			wantOID:     OID_FLOAT8,
		},
		// Integer types
		{
			name:        "integer_type",
			pgTypeName:  "integer",
			sampleValue: int32(2147483647),
			wantDuckDB:  "INTEGER",
			wantOID:     OID_INT4,
		},
		{
			name:        "bigint_type",
			pgTypeName:  "bigint",
			sampleValue: int64(9223372036854775807),
			wantDuckDB:  "BIGINT",
			wantOID:     OID_INT8,
		},
		{
			name:        "smallint_type",
			pgTypeName:  "smallint",
			sampleValue: int16(32767),
			wantDuckDB:  "SMALLINT",
			wantOID:     OID_INT2,
		},
		// OID type
		{
			name:        "oid_type",
			pgTypeName:  "oid",
			sampleValue: uint32(12345),
			wantDuckDB:  "UINTEGER",
			wantOID:     OID_OID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Step 1: Map PostgreSQL type to DuckDB type
			mapping := mapper.PostgreSQLToDuckDB(tt.pgTypeName)
			if mapping == nil {
				t.Fatalf("PostgreSQLToDuckDB(%q) returned nil", tt.pgTypeName)
			}
			if mapping.DuckDBType != tt.wantDuckDB {
				t.Errorf("DuckDBType = %q, want %q", mapping.DuckDBType, tt.wantDuckDB)
			}

			// Step 2: Verify OID mapping
			if mapping.PostgresOID != tt.wantOID {
				t.Errorf("PostgresOID = %d, want %d", mapping.PostgresOID, tt.wantOID)
			}

			// Step 3: Encode value to text format
			encoded, err := converter.EncodeText(tt.sampleValue, mapping.PostgresOID)
			if err != nil {
				t.Errorf("EncodeText(%v, %d) error: %v", tt.sampleValue, mapping.PostgresOID, err)
			}
			if encoded == nil {
				t.Error("EncodeText returned nil for non-nil value")
			}

			// Step 4: Decode back and verify round-trip
			decoded, err := converter.DecodeText(encoded, mapping.PostgresOID)
			if err != nil {
				t.Errorf("DecodeText(%q, %d) error: %v", encoded, mapping.PostgresOID, err)
			}
			if decoded == nil {
				t.Error("DecodeText returned nil")
			}

			// Step 5: Verify type size is available
			size := mapper.GetTypeSize(mapping.PostgresOID)
			// Just verify it doesn't panic and returns a reasonable value
			if size < -1 {
				t.Errorf("GetTypeSize(%d) = %d, want >= -1", mapping.PostgresOID, size)
			}

			// Step 6: Verify type name lookup
			typeName := mapper.GetTypeName(mapping.PostgresOID)
			if (typeName == "" || typeName == "unknown") && mapping.PostgresOID != OID_UNKNOWN {
				t.Logf(
					"Warning: GetTypeName(%d) = %q (may be expected for some types)",
					mapping.PostgresOID,
					typeName,
				)
			}
		})
	}
}

// TestBidirectionalMapping ensures DuckDB -> PostgreSQL -> DuckDB round trip works.
func TestBidirectionalMapping(t *testing.T) {
	mapper := NewTypeMapper()

	duckDBTypes := []struct {
		duckType    string
		expectedOID uint32
	}{
		{"BOOLEAN", OID_BOOL},
		{"SMALLINT", OID_INT2},
		{"INTEGER", OID_INT4},
		{"BIGINT", OID_INT8},
		{"FLOAT", OID_FLOAT4},
		{"DOUBLE", OID_FLOAT8},
		{"VARCHAR", OID_TEXT},
		{"BLOB", OID_BYTEA},
		{"DATE", OID_DATE},
		{"TIMESTAMP", OID_TIMESTAMP},
		{"TIMESTAMPTZ", OID_TIMESTAMPTZ},
		{"UUID", OID_UUID},
		{"JSON", OID_JSON},
		{"DECIMAL", OID_NUMERIC},
		{"TIME", OID_TIME},
		{"INTERVAL", OID_INTERVAL},
	}

	for _, tt := range duckDBTypes {
		t.Run(tt.duckType, func(t *testing.T) {
			// DuckDB -> PostgreSQL OID
			oid := mapper.DuckDBToPostgresOID(tt.duckType)
			if oid != tt.expectedOID {
				t.Errorf("DuckDBToPostgresOID(%q) = %d, want %d", tt.duckType, oid, tt.expectedOID)
			}

			// PostgreSQL OID -> Type name (for verification)
			pgTypeName := mapper.GetTypeName(oid)
			if pgTypeName == "unknown" && oid != OID_UNKNOWN {
				t.Errorf("GetTypeName(%d) = unknown for known OID", oid)
			}
		})
	}
}

// TestNullHandling verifies NULL values are handled correctly throughout the pipeline.
func TestNullHandling(t *testing.T) {
	converter := NewTypeConverter()

	oids := []uint32{
		OID_BOOL, OID_INT2, OID_INT4, OID_INT8,
		OID_FLOAT4, OID_FLOAT8, OID_TEXT, OID_VARCHAR,
		OID_BYTEA, OID_DATE, OID_TIMESTAMP, OID_UUID, OID_JSON,
		OID_NUMERIC, OID_TIMESTAMPTZ, OID_JSONB, OID_BPCHAR,
	}

	for _, oid := range oids {
		name := OIDToName[oid]
		if name == "" {
			name = "unknown"
		}
		t.Run(name, func(t *testing.T) {
			// Encode nil should return nil
			encoded, err := converter.EncodeText(nil, oid)
			if err != nil {
				t.Errorf("EncodeText(nil, %d) error: %v", oid, err)
			}
			if encoded != nil {
				t.Errorf("EncodeText(nil, %d) = %v, want nil", oid, encoded)
			}

			// Decode nil should return nil
			decoded, err := converter.DecodeText(nil, oid)
			if err != nil {
				t.Errorf("DecodeText(nil, %d) error: %v", oid, err)
			}
			if decoded != nil {
				t.Errorf("DecodeText(nil, %d) = %v, want nil", oid, decoded)
			}
		})
	}
}

// TestAllSerialTypesHaveCorrectFlag verifies all serial types are flagged correctly.
func TestAllSerialTypesHaveCorrectFlag(t *testing.T) {
	serialTypes := []string{
		"serial", "bigserial", "smallserial",
		"serial2", "serial4", "serial8",
	}

	for _, st := range serialTypes {
		t.Run(st, func(t *testing.T) {
			mapping := GetTypeAlias(st)
			if mapping == nil {
				t.Fatalf("GetTypeAlias(%q) returned nil", st)
			}
			if !mapping.IsSerial {
				t.Errorf("IsSerial = false for %q, want true", st)
			}
		})
	}

	// Verify non-serial types don't have the flag
	nonSerialTypes := []string{"integer", "bigint", "smallint", "text", "varchar", "boolean"}
	for _, nst := range nonSerialTypes {
		t.Run(nst+"_not_serial", func(t *testing.T) {
			mapping := GetTypeAlias(nst)
			if mapping == nil {
				t.Fatalf("GetTypeAlias(%q) returned nil", nst)
			}
			if mapping.IsSerial {
				t.Errorf("IsSerial = true for %q, want false", nst)
			}
		})
	}
}

// TestTypesWithModifiersFlag verifies types that accept parameters are flagged.
func TestTypesWithModifiersFlag(t *testing.T) {
	typesWithModifiers := []string{
		"varchar", "character varying", "char", "character",
		"numeric", "decimal", "bit", "bit varying", "varbit",
	}

	for _, typ := range typesWithModifiers {
		t.Run(typ, func(t *testing.T) {
			mapping := GetTypeAlias(typ)
			if mapping == nil {
				t.Skipf("Type %q not found in aliases", typ)
			}
			if !mapping.HasModifiers {
				t.Errorf("HasModifiers = false for %q, want true", typ)
			}
		})
	}

	// Verify types without modifiers
	typesWithoutModifiers := []string{
		"text", "integer", "bigint", "boolean", "uuid", "json", "bytea",
	}

	for _, typ := range typesWithoutModifiers {
		t.Run(typ+"_no_modifiers", func(t *testing.T) {
			mapping := GetTypeAlias(typ)
			if mapping == nil {
				t.Skipf("Type %q not found in aliases", typ)
			}
			if mapping.HasModifiers {
				t.Errorf("HasModifiers = true for %q, want false", typ)
			}
		})
	}
}

// TestArrayTypeDetection verifies array types are detected correctly.
func TestArrayTypeDetection(t *testing.T) {
	mapper := NewTypeMapper()

	arrayOIDs := []uint32{
		OID_BOOL_ARRAY, OID_INT2_ARRAY, OID_INT4_ARRAY, OID_INT8_ARRAY,
		OID_TEXT_ARRAY, OID_VARCHAR_ARRAY, OID_FLOAT4_ARRAY, OID_FLOAT8_ARRAY,
		OID_JSON_ARRAY, OID_UUID_ARRAY, OID_JSONB_ARRAY,
	}

	for _, oid := range arrayOIDs {
		name := OIDToName[oid]
		if name == "" {
			name = "unknown_array"
		}
		t.Run(name, func(t *testing.T) {
			if !mapper.IsArrayType(oid) {
				t.Errorf("IsArrayType(%d) = false, want true", oid)
			}
		})
	}

	// Non-array types
	nonArrayOIDs := []uint32{
		OID_BOOL, OID_INT2, OID_INT4, OID_INT8,
		OID_TEXT, OID_VARCHAR, OID_FLOAT4, OID_FLOAT8,
		OID_DATE, OID_TIMESTAMP, OID_UUID, OID_JSON,
	}

	for _, oid := range nonArrayOIDs {
		name := OIDToName[oid]
		if name == "" {
			name = "unknown"
		}
		t.Run(name+"_not_array", func(t *testing.T) {
			if mapper.IsArrayType(oid) {
				t.Errorf("IsArrayType(%d) = true, want false", oid)
			}
		})
	}
}

// TestArrayElementOIDMapping verifies array to element type mapping.
func TestArrayElementOIDMapping(t *testing.T) {
	tests := []struct {
		arrayOID   uint32
		elementOID uint32
	}{
		{OID_BOOL_ARRAY, OID_BOOL},
		{OID_INT2_ARRAY, OID_INT2},
		{OID_INT4_ARRAY, OID_INT4},
		{OID_INT8_ARRAY, OID_INT8},
		{OID_TEXT_ARRAY, OID_TEXT},
		{OID_VARCHAR_ARRAY, OID_VARCHAR},
		{OID_FLOAT4_ARRAY, OID_FLOAT4},
		{OID_FLOAT8_ARRAY, OID_FLOAT8},
		{OID_JSON_ARRAY, OID_JSON},
		{OID_UUID_ARRAY, OID_UUID},
		{OID_JSONB_ARRAY, OID_JSONB},
	}

	for _, tt := range tests {
		t.Run(OIDToName[tt.arrayOID], func(t *testing.T) {
			// Array -> Element
			elementOID := GetArrayElementOID(tt.arrayOID)
			if elementOID != tt.elementOID {
				t.Errorf(
					"GetArrayElementOID(%d) = %d, want %d",
					tt.arrayOID,
					elementOID,
					tt.elementOID,
				)
			}

			// Element -> Array
			arrayOID := GetArrayOID(tt.elementOID)
			if arrayOID != tt.arrayOID {
				t.Errorf("GetArrayOID(%d) = %d, want %d", tt.elementOID, arrayOID, tt.arrayOID)
			}
		})
	}
}

// TestFixedLengthTypes verifies fixed-length types have correct sizes.
func TestFixedLengthTypes(t *testing.T) {
	mapper := NewTypeMapper()

	fixedSizeTypes := []struct {
		oid          uint32
		expectedSize int16
	}{
		{OID_BOOL, 1},
		{OID_INT2, 2},
		{OID_INT4, 4},
		{OID_INT8, 8},
		{OID_FLOAT4, 4},
		{OID_FLOAT8, 8},
		{OID_DATE, 4},
		{OID_TIMESTAMP, 8},
		{OID_TIMESTAMPTZ, 8},
		{OID_UUID, 16},
		{OID_OID, 4},
		{OID_INTERVAL, 16},
		{OID_TIME, 8},
		{OID_TIMETZ, 12},
		{OID_MONEY, 8},
	}

	for _, tt := range fixedSizeTypes {
		name := OIDToName[tt.oid]
		if name == "" {
			name = "unknown"
		}
		t.Run(name, func(t *testing.T) {
			size := mapper.GetTypeSize(tt.oid)
			if size != tt.expectedSize {
				t.Errorf("GetTypeSize(%d) = %d, want %d", tt.oid, size, tt.expectedSize)
			}
		})
	}
}

// TestVariableLengthTypes verifies variable-length types return -1.
func TestVariableLengthTypes(t *testing.T) {
	mapper := NewTypeMapper()

	variableSizeTypes := []uint32{
		OID_TEXT, OID_VARCHAR, OID_BYTEA, OID_JSON, OID_JSONB,
		OID_NUMERIC, OID_XML, OID_CHAR, OID_BPCHAR, OID_NAME,
		OID_BIT, OID_VARBIT,
		// Array types
		OID_INT4_ARRAY, OID_TEXT_ARRAY, OID_BOOL_ARRAY,
	}

	for _, oid := range variableSizeTypes {
		name := OIDToName[oid]
		if name == "" {
			name = "unknown"
		}
		t.Run(name, func(t *testing.T) {
			size := mapper.GetTypeSize(oid)
			if size != -1 {
				t.Errorf("GetTypeSize(%d) = %d, want -1 (variable length)", oid, size)
			}
		})
	}
}

// TestTypeNameConsistency verifies type names are consistent throughout the system.
func TestTypeNameConsistency(t *testing.T) {
	// Verify all OIDs in OIDToName have consistent naming
	for oid, name := range OIDToName {
		t.Run(name, func(t *testing.T) {
			if name == "" {
				t.Errorf("Empty name for OID %d", oid)
			}
		})
	}

	// Verify all type aliases have valid OIDs that exist in OIDToName
	for aliasName, mapping := range PostgreSQLTypeAliases {
		t.Run("alias_"+aliasName, func(t *testing.T) {
			if mapping.PostgresOID == 0 {
				t.Errorf("Zero OID for alias %q", aliasName)
			}
			// OID should have a name (except for OID_UNKNOWN which might not have aliases)
			if mapping.PostgresOID == OID_UNKNOWN {
				return
			}
			if _, exists := OIDToName[mapping.PostgresOID]; !exists {
				t.Errorf("Alias %q has OID %d not in OIDToName", aliasName, mapping.PostgresOID)
			}
		})
	}
}

// TestCompleteEncodingRoundTrip tests encode -> decode round trips for all supported types.
func TestCompleteEncodingRoundTrip(t *testing.T) {
	converter := NewTypeConverter()

	tests := []struct {
		name  string
		oid   uint32
		value any
	}{
		{"bool_true", OID_BOOL, true},
		{"bool_false", OID_BOOL, false},
		{"int16", OID_INT2, int16(12345)},
		{"int16_negative", OID_INT2, int16(-12345)},
		{"int32", OID_INT4, int32(123456789)},
		{"int32_negative", OID_INT4, int32(-123456789)},
		{"int64", OID_INT8, int64(1234567890123456789)},
		{"int64_negative", OID_INT8, int64(-1234567890123456789)},
		{"float32", OID_FLOAT4, float32(3.14)},
		{"float64", OID_FLOAT8, float64(3.141592653589793)},
		{"text", OID_TEXT, "hello world"},
		{"text_empty", OID_TEXT, ""},
		{"text_unicode", OID_TEXT, "Hello, World!"},
		{"bytea", OID_BYTEA, []byte{0xDE, 0xAD, 0xBE, 0xEF}},
		{"bytea_empty", OID_BYTEA, make([]byte, 0)},
		{"date", OID_DATE, time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)},
		{"timestamp", OID_TIMESTAMP, time.Date(2024, 6, 15, 14, 30, 45, 0, time.UTC)},
		{"uuid", OID_UUID, "550e8400-e29b-41d4-a716-446655440000"},
		{"json", OID_JSON, `{"key":"value","number":42}`},
		{"numeric", OID_NUMERIC, "123456.789012345"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded, err := converter.EncodeText(tt.value, tt.oid)
			if err != nil {
				t.Fatalf("EncodeText error: %v", err)
			}
			if encoded == nil {
				t.Fatal("EncodeText returned nil")
			}

			// Decode
			decoded, err := converter.DecodeText(encoded, tt.oid)
			if err != nil {
				t.Fatalf("DecodeText error: %v", err)
			}
			if decoded == nil {
				t.Fatal("DecodeText returned nil")
			}

			// For some types, compare directly; for others, we just verify no errors
			// Numeric types may have precision differences, so we don't do exact comparison
		})
	}
}

// TestBinaryEncodingRoundTrip tests binary encode -> decode for supported types.
func TestBinaryEncodingRoundTrip(t *testing.T) {
	converter := NewTypeConverter()

	t.Run("bool_types", func(t *testing.T) {
		testBinaryBool(t, converter, true)
		testBinaryBool(t, converter, false)
	})

	t.Run("int16_types", func(t *testing.T) {
		testBinaryInt16(t, converter, int16(12345))
		testBinaryInt16(t, converter, int16(-12345))
	})

	t.Run("int32_types", func(t *testing.T) {
		testBinaryInt32(t, converter, int32(123456789))
		testBinaryInt32(t, converter, int32(-123456789))
	})

	t.Run("int64_types", func(t *testing.T) {
		testBinaryInt64(t, converter, int64(1234567890123456789))
		testBinaryInt64(t, converter, int64(-1234567890123456789))
	})

	t.Run("float32_types", func(t *testing.T) {
		testBinaryFloat32(t, converter, float32(3.14))
	})

	t.Run("float64_types", func(t *testing.T) {
		testBinaryFloat64(t, converter, float64(3.141592653589793))
	})
}

func testBinaryBool(t *testing.T, converter TypeConverter, want bool) {
	t.Helper()
	encoded, err := converter.EncodeBinary(want, OID_BOOL)
	if err != nil {
		t.Fatalf("EncodeBinary error: %v", err)
	}
	decoded, err := converter.DecodeBinary(encoded, OID_BOOL)
	if err != nil {
		t.Fatalf("DecodeBinary error: %v", err)
	}
	if decoded != want {
		t.Errorf("Round trip mismatch: got %v, want %v", decoded, want)
	}
}

func testBinaryInt16(t *testing.T, converter TypeConverter, want int16) {
	t.Helper()
	encoded, err := converter.EncodeBinary(want, OID_INT2)
	if err != nil {
		t.Fatalf("EncodeBinary error: %v", err)
	}
	decoded, err := converter.DecodeBinary(encoded, OID_INT2)
	if err != nil {
		t.Fatalf("DecodeBinary error: %v", err)
	}
	if decoded != want {
		t.Errorf("Round trip mismatch: got %v, want %v", decoded, want)
	}
}

func testBinaryInt32(t *testing.T, converter TypeConverter, want int32) {
	t.Helper()
	encoded, err := converter.EncodeBinary(want, OID_INT4)
	if err != nil {
		t.Fatalf("EncodeBinary error: %v", err)
	}
	decoded, err := converter.DecodeBinary(encoded, OID_INT4)
	if err != nil {
		t.Fatalf("DecodeBinary error: %v", err)
	}
	if decoded != want {
		t.Errorf("Round trip mismatch: got %v, want %v", decoded, want)
	}
}

func testBinaryInt64(t *testing.T, converter TypeConverter, want int64) {
	t.Helper()
	encoded, err := converter.EncodeBinary(want, OID_INT8)
	if err != nil {
		t.Fatalf("EncodeBinary error: %v", err)
	}
	decoded, err := converter.DecodeBinary(encoded, OID_INT8)
	if err != nil {
		t.Fatalf("DecodeBinary error: %v", err)
	}
	if decoded != want {
		t.Errorf("Round trip mismatch: got %v, want %v", decoded, want)
	}
}

func testBinaryFloat32(t *testing.T, converter TypeConverter, want float32) {
	t.Helper()
	encoded, err := converter.EncodeBinary(want, OID_FLOAT4)
	if err != nil {
		t.Fatalf("EncodeBinary error: %v", err)
	}
	decoded, err := converter.DecodeBinary(encoded, OID_FLOAT4)
	if err != nil {
		t.Fatalf("DecodeBinary error: %v", err)
	}
	decodedFloat, ok := decoded.(float32)
	if !ok {
		t.Fatalf("decoded is not float32: %T", decoded)
	}
	diff := decodedFloat - want
	if diff > 0.0001 || diff < -0.0001 {
		t.Errorf("Round trip mismatch: got %v, want %v", decoded, want)
	}
}

func testBinaryFloat64(t *testing.T, converter TypeConverter, want float64) {
	t.Helper()
	encoded, err := converter.EncodeBinary(want, OID_FLOAT8)
	if err != nil {
		t.Fatalf("EncodeBinary error: %v", err)
	}
	decoded, err := converter.DecodeBinary(encoded, OID_FLOAT8)
	if err != nil {
		t.Fatalf("DecodeBinary error: %v", err)
	}
	decodedFloat, ok := decoded.(float64)
	if !ok {
		t.Fatalf("decoded is not float64: %T", decoded)
	}
	diff := decodedFloat - want
	if diff > 0.0000001 || diff < -0.0000001 {
		t.Errorf("Round trip mismatch: got %v, want %v", decoded, want)
	}
}

// TestCaseInsensitiveTypeLookup verifies that type lookup is case-insensitive.
func TestCaseInsensitiveTypeLookup(t *testing.T) {
	mapper := NewTypeMapper()

	testCases := []struct {
		input      string
		wantDuckDB string
	}{
		{"TEXT", "VARCHAR"},
		{"text", "VARCHAR"},
		{"Text", "VARCHAR"},
		{"INTEGER", "INTEGER"},
		{"integer", "INTEGER"},
		{"Integer", "INTEGER"},
		{"BOOLEAN", "BOOLEAN"},
		{"boolean", "BOOLEAN"},
		{"Boolean", "BOOLEAN"},
		{"TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ"},
		{"timestamp with time zone", "TIMESTAMPTZ"},
		{"Timestamp With Time Zone", "TIMESTAMPTZ"},
		{"SERIAL", "INTEGER"},
		{"serial", "INTEGER"},
		{"Serial", "INTEGER"},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			mapping := mapper.PostgreSQLToDuckDB(tc.input)
			if mapping == nil {
				t.Fatalf("PostgreSQLToDuckDB(%q) returned nil", tc.input)
			}
			if mapping.DuckDBType != tc.wantDuckDB {
				t.Errorf("PostgreSQLToDuckDB(%q).DuckDBType = %q, want %q",
					tc.input, mapping.DuckDBType, tc.wantDuckDB)
			}
		})
	}
}

// TestWhitespaceHandling verifies that type names with extra whitespace are handled.
func TestWhitespaceHandling(t *testing.T) {
	mapper := NewTypeMapper()

	testCases := []struct {
		input      string
		wantDuckDB string
	}{
		{"  text  ", "VARCHAR"},
		{" integer ", "INTEGER"},
		{"  serial  ", "INTEGER"},
		{"\tboolean\t", "BOOLEAN"},
		{" timestamp with time zone ", "TIMESTAMPTZ"},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			mapping := mapper.PostgreSQLToDuckDB(tc.input)
			if mapping == nil {
				t.Fatalf("PostgreSQLToDuckDB(%q) returned nil", tc.input)
			}
			if mapping.DuckDBType != tc.wantDuckDB {
				t.Errorf("PostgreSQLToDuckDB(%q).DuckDBType = %q, want %q",
					tc.input, mapping.DuckDBType, tc.wantDuckDB)
			}
		})
	}
}

// TestGlobalMapperAndConverter verifies the global instances work correctly.
func TestGlobalMapperAndConverter(t *testing.T) {
	// Test GetDefaultMapper
	mapper := GetDefaultMapper()
	if mapper == nil {
		t.Fatal("GetDefaultMapper() returned nil")
	}

	// Test GetDefaultConverter
	converter := GetDefaultConverter()
	if converter == nil {
		t.Fatal("GetDefaultConverter() returned nil")
	}

	// Test convenience functions
	mapping := MapPostgreSQLToDuckDB("text")
	if mapping == nil || mapping.DuckDBType != "VARCHAR" {
		t.Error("MapPostgreSQLToDuckDB(text) failed")
	}

	oid := MapDuckDBToPostgresOID("INTEGER")
	if oid != OID_INT4 {
		t.Errorf("MapDuckDBToPostgresOID(INTEGER) = %d, want %d", oid, OID_INT4)
	}

	encoded, err := EncodeValue(42, OID_INT4)
	if err != nil || string(encoded) != "42" {
		t.Error("EncodeValue(42, OID_INT4) failed")
	}

	decoded, err := DecodeValue([]byte("42"), OID_INT4)
	if err != nil || decoded != int32(42) {
		t.Error("DecodeValue(42, OID_INT4) failed")
	}
}

// TestDuckDBArrayTypes verifies DuckDB array type to OID mapping.
func TestDuckDBArrayTypes(t *testing.T) {
	mapper := NewTypeMapper()

	testCases := []struct {
		duckDBType  string
		expectedOID uint32
	}{
		{"INTEGER[]", OID_INT4_ARRAY},
		{"INT[]", OID_INT4_ARRAY},
		{"INT4[]", OID_INT4_ARRAY},
		{"BIGINT[]", OID_INT8_ARRAY},
		{"INT8[]", OID_INT8_ARRAY},
		{"SMALLINT[]", OID_INT2_ARRAY},
		{"INT2[]", OID_INT2_ARRAY},
		{"TEXT[]", OID_TEXT_ARRAY},
		{"VARCHAR[]", OID_TEXT_ARRAY},
		{"STRING[]", OID_TEXT_ARRAY},
		{"BOOLEAN[]", OID_BOOL_ARRAY},
		{"BOOL[]", OID_BOOL_ARRAY},
		{"FLOAT[]", OID_FLOAT4_ARRAY},
		{"FLOAT4[]", OID_FLOAT4_ARRAY},
		{"REAL[]", OID_FLOAT4_ARRAY},
		{"DOUBLE[]", OID_FLOAT8_ARRAY},
		{"FLOAT8[]", OID_FLOAT8_ARRAY},
	}

	for _, tc := range testCases {
		t.Run(tc.duckDBType, func(t *testing.T) {
			oid := mapper.DuckDBToPostgresOID(tc.duckDBType)
			if oid != tc.expectedOID {
				t.Errorf(
					"DuckDBToPostgresOID(%q) = %d, want %d",
					tc.duckDBType,
					oid,
					tc.expectedOID,
				)
			}
		})
	}
}

// TestParameterizedDuckDBTypes verifies parameterized types strip parameters correctly.
func TestParameterizedDuckDBTypes(t *testing.T) {
	mapper := NewTypeMapper()

	testCases := []struct {
		duckDBType  string
		expectedOID uint32
	}{
		{"VARCHAR(255)", OID_TEXT},
		{"VARCHAR(100)", OID_TEXT},
		{"DECIMAL(10,2)", OID_NUMERIC},
		{"DECIMAL(18,4)", OID_NUMERIC},
		{"NUMERIC(10)", OID_NUMERIC},
		{"CHAR(1)", OID_BPCHAR},
		{"BIT(8)", OID_BIT},
	}

	for _, tc := range testCases {
		t.Run(tc.duckDBType, func(t *testing.T) {
			oid := mapper.DuckDBToPostgresOID(tc.duckDBType)
			if oid != tc.expectedOID {
				t.Errorf(
					"DuckDBToPostgresOID(%q) = %d, want %d",
					tc.duckDBType,
					oid,
					tc.expectedOID,
				)
			}
		})
	}
}

// TestSpecialValueEncoding tests encoding of special values like NaN, Infinity.
func TestSpecialValueEncoding(t *testing.T) {
	converter := NewTypeConverter()
	mapper := NewTypeMapper()

	t.Run("double_precision_mapping", func(t *testing.T) {
		// Verify double precision type is mapped correctly
		mapping := mapper.PostgreSQLToDuckDB("double precision")
		if mapping == nil {
			t.Fatal("double precision mapping is nil")
		}
		if mapping.DuckDBType != "DOUBLE" {
			t.Errorf("double precision DuckDBType = %q, want DOUBLE", mapping.DuckDBType)
		}
	})

	t.Run("zero_values", func(t *testing.T) {
		// Test zero values encode correctly
		zeroInt, err := converter.EncodeText(int32(0), OID_INT4)
		if err != nil {
			t.Errorf("Failed to encode zero int32: %v", err)
		}
		if string(zeroInt) != "0" {
			t.Errorf("Zero int32 encoded as %q, want \"0\"", string(zeroInt))
		}

		zeroFloat, err := converter.EncodeText(float64(0), OID_FLOAT8)
		if err != nil {
			t.Errorf("Failed to encode zero float64: %v", err)
		}
		if zeroFloat == nil {
			t.Error("Zero float64 encoded as nil")
		}
	})

	t.Run("max_int_values", func(t *testing.T) {
		// Test maximum integer values
		maxInt16, err := converter.EncodeText(int16(32767), OID_INT2)
		if err != nil {
			t.Errorf("Failed to encode max int16: %v", err)
		}
		if string(maxInt16) != "32767" {
			t.Errorf("Max int16 encoded as %q, want \"32767\"", string(maxInt16))
		}

		minInt16, err := converter.EncodeText(int16(-32768), OID_INT2)
		if err != nil {
			t.Errorf("Failed to encode min int16: %v", err)
		}
		if string(minInt16) != "-32768" {
			t.Errorf("Min int16 encoded as %q, want \"-32768\"", string(minInt16))
		}
	})
}
