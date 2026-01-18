package types

import (
	"testing"
)

func TestGetTypeAlias(t *testing.T) {
	tests := []struct {
		pgType     string
		wantDuckDB string
		wantOID    uint32
		wantSerial bool
	}{
		// Serial types
		{"serial", "INTEGER", OID_INT4, true},
		{"bigserial", "BIGINT", OID_INT8, true},
		{"smallserial", "SMALLINT", OID_INT2, true},
		{"serial2", "SMALLINT", OID_INT2, true},
		{"serial4", "INTEGER", OID_INT4, true},
		{"serial8", "BIGINT", OID_INT8, true},

		// Integer types
		{"integer", "INTEGER", OID_INT4, false},
		{"int", "INTEGER", OID_INT4, false},
		{"int4", "INTEGER", OID_INT4, false},
		{"bigint", "BIGINT", OID_INT8, false},
		{"int8", "BIGINT", OID_INT8, false},
		{"smallint", "SMALLINT", OID_INT2, false},
		{"int2", "SMALLINT", OID_INT2, false},

		// Floating point types
		{"real", "FLOAT", OID_FLOAT4, false},
		{"float4", "FLOAT", OID_FLOAT4, false},
		{"double precision", "DOUBLE", OID_FLOAT8, false},
		{"float8", "DOUBLE", OID_FLOAT8, false},
		{"float", "DOUBLE", OID_FLOAT8, false},

		// Numeric types
		{"numeric", "DECIMAL", OID_NUMERIC, false},
		{"decimal", "DECIMAL", OID_NUMERIC, false},

		// String types
		{"text", "VARCHAR", OID_TEXT, false},
		{"varchar", "VARCHAR", OID_VARCHAR, false},
		{"character varying", "VARCHAR", OID_VARCHAR, false},
		{"char", "VARCHAR", OID_BPCHAR, false},
		{"character", "VARCHAR", OID_BPCHAR, false},
		{"bpchar", "VARCHAR", OID_BPCHAR, false},
		{"name", "VARCHAR", OID_NAME, false},

		// Boolean types
		{"boolean", "BOOLEAN", OID_BOOL, false},
		{"bool", "BOOLEAN", OID_BOOL, false},

		// Binary types
		{"bytea", "BLOB", OID_BYTEA, false},

		// Date/Time types
		{"date", "DATE", OID_DATE, false},
		{"time", "TIME", OID_TIME, false},
		{"time without time zone", "TIME", OID_TIME, false},
		{"time with time zone", "TIMETZ", OID_TIMETZ, false},
		{"timetz", "TIMETZ", OID_TIMETZ, false},
		{"timestamp", "TIMESTAMP", OID_TIMESTAMP, false},
		{"timestamp without time zone", "TIMESTAMP", OID_TIMESTAMP, false},
		{"timestamp with time zone", "TIMESTAMPTZ", OID_TIMESTAMPTZ, false},
		{"timestamptz", "TIMESTAMPTZ", OID_TIMESTAMPTZ, false},
		{"interval", "INTERVAL", OID_INTERVAL, false},

		// JSON types
		{"json", "JSON", OID_JSON, false},
		{"jsonb", "JSON", OID_JSONB, false},

		// UUID type
		{"uuid", "UUID", OID_UUID, false},

		// OID type
		{"oid", "UINTEGER", OID_OID, false},

		// Bit string types
		{"bit", "BIT", OID_BIT, false},
		{"bit varying", "BIT", OID_VARBIT, false},
		{"varbit", "BIT", OID_VARBIT, false},

		// Special types
		{"money", "DECIMAL", OID_MONEY, false},
		{"xml", "VARCHAR", OID_XML, false},

		// Network types
		{"cidr", "VARCHAR", OID_CIDR, false},
		{"inet", "VARCHAR", OID_INET, false},
		{"macaddr", "VARCHAR", OID_MACADDR, false},
		{"macaddr8", "VARCHAR", OID_MACADDR8, false},

		// Geometric types
		{"point", "VARCHAR", OID_POINT, false},
		{"line", "VARCHAR", OID_LINE, false},
		{"box", "VARCHAR", OID_BOX, false},
		{"path", "VARCHAR", OID_PATH, false},
		{"polygon", "VARCHAR", OID_POLYGON, false},
		{"circle", "VARCHAR", OID_CIRCLE, false},

		// Text search types
		{"tsvector", "VARCHAR", OID_TSVECTOR, false},
		{"tsquery", "VARCHAR", OID_TSQUERY, false},

		// Case insensitivity tests
		{"TEXT", "VARCHAR", OID_TEXT, false},
		{"Serial", "INTEGER", OID_INT4, true},
		{"BIGSERIAL", "BIGINT", OID_INT8, true},
		{"VARCHAR", "VARCHAR", OID_VARCHAR, false},
		{"BOOLEAN", "BOOLEAN", OID_BOOL, false},
		{"TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ", OID_TIMESTAMPTZ, false},

		// Whitespace handling
		{"  text  ", "VARCHAR", OID_TEXT, false},
		{" serial ", "INTEGER", OID_INT4, true},
	}

	for _, tt := range tests {
		t.Run(tt.pgType, func(t *testing.T) {
			mapping := GetTypeAlias(tt.pgType)
			if mapping == nil {
				t.Fatalf("GetTypeAlias(%q) returned nil", tt.pgType)
			}
			if mapping.DuckDBType != tt.wantDuckDB {
				t.Errorf("DuckDBType = %q, want %q", mapping.DuckDBType, tt.wantDuckDB)
			}
			if mapping.PostgresOID != tt.wantOID {
				t.Errorf("PostgresOID = %d, want %d", mapping.PostgresOID, tt.wantOID)
			}
			if mapping.IsSerial != tt.wantSerial {
				t.Errorf("IsSerial = %v, want %v", mapping.IsSerial, tt.wantSerial)
			}
		})
	}
}

func TestGetTypeAliasUnknown(t *testing.T) {
	unknownTypes := []string{
		"nonexistent_type",
		"unknown_type",
		"custom_type",
		"array",
		"",
	}

	for _, pgType := range unknownTypes {
		t.Run(pgType, func(t *testing.T) {
			mapping := GetTypeAlias(pgType)
			if mapping != nil {
				t.Errorf("GetTypeAlias(%q) = %v, want nil", pgType, mapping)
			}
		})
	}
}

func TestIsSerialType(t *testing.T) {
	tests := []struct {
		pgType     string
		wantSerial bool
	}{
		// Serial types
		{"serial", true},
		{"bigserial", true},
		{"smallserial", true},
		{"serial4", true},
		{"serial8", true},
		{"serial2", true},

		// Case insensitivity
		{"SERIAL", true},
		{"BigSerial", true},
		{"SmallSerial", true},

		// Non-serial types
		{"integer", false},
		{"bigint", false},
		{"smallint", false},
		{"text", false},
		{"varchar", false},
		{"boolean", false},
		{"timestamp", false},
		{"uuid", false},
		{"json", false},

		// Unknown types
		{"unknown", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.pgType, func(t *testing.T) {
			if got := IsSerialType(tt.pgType); got != tt.wantSerial {
				t.Errorf("IsSerialType(%q) = %v, want %v", tt.pgType, got, tt.wantSerial)
			}
		})
	}
}

func TestHasModifiers(t *testing.T) {
	tests := []struct {
		pgType       string
		wantModifier bool
	}{
		// Types with modifiers
		{"varchar", true},
		{"character varying", true},
		{"char", true},
		{"character", true},
		{"numeric", true},
		{"decimal", true},
		{"bit", true},
		{"bit varying", true},
		{"varbit", true},

		// Types without modifiers
		{"text", false},
		{"integer", false},
		{"bigint", false},
		{"boolean", false},
		{"serial", false},
		{"timestamp", false},
		{"uuid", false},
		{"json", false},
		{"bytea", false},

		// Unknown types
		{"unknown", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.pgType, func(t *testing.T) {
			if got := HasModifiers(tt.pgType); got != tt.wantModifier {
				t.Errorf("HasModifiers(%q) = %v, want %v", tt.pgType, got, tt.wantModifier)
			}
		})
	}
}

func TestGetDuckDBType(t *testing.T) {
	tests := []struct {
		pgType   string
		wantType string
	}{
		{"serial", "INTEGER"},
		{"bigserial", "BIGINT"},
		{"text", "VARCHAR"},
		{"boolean", "BOOLEAN"},
		{"timestamp", "TIMESTAMP"},
		{"unknown_type", ""},
	}

	for _, tt := range tests {
		t.Run(tt.pgType, func(t *testing.T) {
			if got := GetDuckDBType(tt.pgType); got != tt.wantType {
				t.Errorf("GetDuckDBType(%q) = %q, want %q", tt.pgType, got, tt.wantType)
			}
		})
	}
}

func TestGetPostgresOID(t *testing.T) {
	tests := []struct {
		pgType  string
		wantOID uint32
	}{
		{"serial", OID_INT4},
		{"bigserial", OID_INT8},
		{"text", OID_TEXT},
		{"boolean", OID_BOOL},
		{"timestamp", OID_TIMESTAMP},
		{"uuid", OID_UUID},
		{"unknown_type", OID_UNKNOWN},
	}

	for _, tt := range tests {
		t.Run(tt.pgType, func(t *testing.T) {
			if got := GetPostgresOID(tt.pgType); got != tt.wantOID {
				t.Errorf("GetPostgresOID(%q) = %d, want %d", tt.pgType, got, tt.wantOID)
			}
		})
	}
}

func TestAllTypeAliases(t *testing.T) {
	aliases := AllTypeAliases()

	// Should have a reasonable number of aliases
	if len(aliases) < 50 {
		t.Errorf("AllTypeAliases() returned %d aliases, expected at least 50", len(aliases))
	}

	// Check for some expected types
	expectedTypes := []string{"serial", "text", "integer", "boolean", "timestamp", "uuid"}
	aliasMap := make(map[string]bool)
	for _, a := range aliases {
		aliasMap[a] = true
	}

	for _, expected := range expectedTypes {
		if !aliasMap[expected] {
			t.Errorf("AllTypeAliases() missing expected type %q", expected)
		}
	}
}

func TestSerialTypes(t *testing.T) {
	serialTypes := SerialTypes()

	expectedSerials := map[string]bool{
		"serial":      true,
		"bigserial":   true,
		"smallserial": true,
		"serial2":     true,
		"serial4":     true,
		"serial8":     true,
	}

	if len(serialTypes) != len(expectedSerials) {
		t.Errorf(
			"SerialTypes() returned %d types, expected %d",
			len(serialTypes),
			len(expectedSerials),
		)
	}

	for _, st := range serialTypes {
		if !expectedSerials[st] {
			t.Errorf("SerialTypes() returned unexpected type %q", st)
		}
	}
}

func TestTypeMappingConsistency(t *testing.T) {
	// Verify that all mappings have consistent data
	for name, mapping := range PostgreSQLTypeAliases {
		t.Run(name, func(t *testing.T) {
			// PostgreSQLName should not be empty
			if mapping.PostgreSQLName == "" {
				t.Errorf("mapping for %q has empty PostgreSQLName", name)
			}

			// DuckDBType should not be empty
			if mapping.DuckDBType == "" {
				t.Errorf("mapping for %q has empty DuckDBType", name)
			}

			// PostgresOID should not be zero (except for OID_UNKNOWN which is 705)
			if mapping.PostgresOID == 0 {
				t.Errorf("mapping for %q has zero PostgresOID", name)
			}

			// Serial types should have integer-like DuckDB types
			if mapping.IsSerial {
				validDuckDBTypes := map[string]bool{
					"SMALLINT": true,
					"INTEGER":  true,
					"BIGINT":   true,
				}
				if !validDuckDBTypes[mapping.DuckDBType] {
					t.Errorf("serial type %q has invalid DuckDBType %q", name, mapping.DuckDBType)
				}
			}
		})
	}
}

func TestOIDConstants(t *testing.T) {
	// Verify some well-known OID values
	tests := []struct {
		name     string
		oid      uint32
		expected uint32
	}{
		{"OID_BOOL", OID_BOOL, 16},
		{"OID_BYTEA", OID_BYTEA, 17},
		{"OID_INT8", OID_INT8, 20},
		{"OID_INT2", OID_INT2, 21},
		{"OID_INT4", OID_INT4, 23},
		{"OID_TEXT", OID_TEXT, 25},
		{"OID_JSON", OID_JSON, 114},
		{"OID_FLOAT4", OID_FLOAT4, 700},
		{"OID_FLOAT8", OID_FLOAT8, 701},
		{"OID_UNKNOWN", OID_UNKNOWN, 705},
		{"OID_VARCHAR", OID_VARCHAR, 1043},
		{"OID_DATE", OID_DATE, 1082},
		{"OID_TIME", OID_TIME, 1083},
		{"OID_TIMESTAMP", OID_TIMESTAMP, 1114},
		{"OID_TIMESTAMPTZ", OID_TIMESTAMPTZ, 1184},
		{"OID_INTERVAL", OID_INTERVAL, 1186},
		{"OID_TIMETZ", OID_TIMETZ, 1266},
		{"OID_NUMERIC", OID_NUMERIC, 1700},
		{"OID_UUID", OID_UUID, 2950},
		{"OID_JSONB", OID_JSONB, 3802},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.oid != tt.expected {
				t.Errorf("%s = %d, want %d", tt.name, tt.oid, tt.expected)
			}
		})
	}
}

func TestArrayOIDFunctions(t *testing.T) {
	t.Run("IsArrayOID", func(t *testing.T) {
		// Test array OIDs
		if !IsArrayOID(OID_INT4_ARRAY) {
			t.Error("IsArrayOID(OID_INT4_ARRAY) should return true")
		}
		if !IsArrayOID(OID_TEXT_ARRAY) {
			t.Error("IsArrayOID(OID_TEXT_ARRAY) should return true")
		}

		// Test non-array OIDs
		if IsArrayOID(OID_INT4) {
			t.Error("IsArrayOID(OID_INT4) should return false")
		}
		if IsArrayOID(OID_TEXT) {
			t.Error("IsArrayOID(OID_TEXT) should return false")
		}
	})

	t.Run("GetArrayElementOID", func(t *testing.T) {
		tests := []struct {
			arrayOID uint32
			wantOID  uint32
		}{
			{OID_INT4_ARRAY, OID_INT4},
			{OID_TEXT_ARRAY, OID_TEXT},
			{OID_BOOL_ARRAY, OID_BOOL},
			{OID_INT4, OID_UNKNOWN}, // Not an array
		}

		for _, tt := range tests {
			got := GetArrayElementOID(tt.arrayOID)
			if got != tt.wantOID {
				t.Errorf("GetArrayElementOID(%d) = %d, want %d", tt.arrayOID, got, tt.wantOID)
			}
		}
	})

	t.Run("GetArrayOID", func(t *testing.T) {
		tests := []struct {
			elementOID uint32
			wantOID    uint32
		}{
			{OID_INT4, OID_INT4_ARRAY},
			{OID_TEXT, OID_TEXT_ARRAY},
			{OID_BOOL, OID_BOOL_ARRAY},
			{OID_TIMESTAMP, OID_UNKNOWN}, // No array type defined
		}

		for _, tt := range tests {
			got := GetArrayOID(tt.elementOID)
			if got != tt.wantOID {
				t.Errorf("GetArrayOID(%d) = %d, want %d", tt.elementOID, got, tt.wantOID)
			}
		}
	})
}
