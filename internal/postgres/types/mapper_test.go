package types

import "testing"

func TestDefaultTypeMapper_PostgreSQLToDuckDB(t *testing.T) {
	mapper := NewTypeMapper()

	tests := []struct {
		pgType     string
		wantDuckDB string
		wantOID    uint32
	}{
		{"serial", "INTEGER", OID_INT4},
		{"text", "VARCHAR", OID_TEXT},
		{"integer", "INTEGER", OID_INT4},
		{"double precision", "DOUBLE", OID_FLOAT8},
		{"timestamp with time zone", "TIMESTAMPTZ", OID_TIMESTAMPTZ},
		{"boolean", "BOOLEAN", OID_BOOL},
		{"bytea", "BLOB", OID_BYTEA},
		{"json", "JSON", OID_JSON},
		{"uuid", "UUID", OID_UUID},
	}

	for _, tt := range tests {
		t.Run(tt.pgType, func(t *testing.T) {
			mapping := mapper.PostgreSQLToDuckDB(tt.pgType)
			if mapping == nil {
				t.Fatalf("PostgreSQLToDuckDB(%q) returned nil", tt.pgType)
			}
			if mapping.DuckDBType != tt.wantDuckDB {
				t.Errorf("DuckDBType = %q, want %q", mapping.DuckDBType, tt.wantDuckDB)
			}
			if mapping.PostgresOID != tt.wantOID {
				t.Errorf("PostgresOID = %d, want %d", mapping.PostgresOID, tt.wantOID)
			}
		})
	}
}

func TestDefaultTypeMapper_DuckDBToPostgresOID(t *testing.T) {
	mapper := NewTypeMapper()

	tests := []struct {
		duckDBType string
		wantOID    uint32
	}{
		{"BOOLEAN", OID_BOOL},
		{"BOOL", OID_BOOL},
		{"SMALLINT", OID_INT2},
		{"INTEGER", OID_INT4},
		{"INT", OID_INT4},
		{"BIGINT", OID_INT8},
		{"FLOAT", OID_FLOAT4},
		{"DOUBLE", OID_FLOAT8},
		{"VARCHAR", OID_TEXT},
		{"TEXT", OID_TEXT},
		{"STRING", OID_TEXT},
		{"BLOB", OID_BYTEA},
		{"DATE", OID_DATE},
		{"TIME", OID_TIME},
		{"TIMESTAMP", OID_TIMESTAMP},
		{"TIMESTAMPTZ", OID_TIMESTAMPTZ},
		{"JSON", OID_JSON},
		{"UUID", OID_UUID},
		{"DECIMAL", OID_NUMERIC},
		{"NUMERIC", OID_NUMERIC},
		// Parameterized types
		{"VARCHAR(255)", OID_TEXT},
		{"DECIMAL(10,2)", OID_NUMERIC},
		// Case insensitivity
		{"integer", OID_INT4},
		{"varchar", OID_TEXT},
		// Unknown type
		{"UNKNOWN_TYPE", OID_UNKNOWN},
	}

	for _, tt := range tests {
		t.Run(tt.duckDBType, func(t *testing.T) {
			oid := mapper.DuckDBToPostgresOID(tt.duckDBType)
			if oid != tt.wantOID {
				t.Errorf("DuckDBToPostgresOID(%q) = %d, want %d", tt.duckDBType, oid, tt.wantOID)
			}
		})
	}
}

func TestDefaultTypeMapper_GetTypeSize(t *testing.T) {
	mapper := NewTypeMapper()

	tests := []struct {
		oid      uint32
		wantSize int16
	}{
		{OID_BOOL, 1},
		{OID_INT2, 2},
		{OID_INT4, 4},
		{OID_INT8, 8},
		{OID_FLOAT4, 4},
		{OID_FLOAT8, 8},
		{OID_DATE, 4},
		{OID_TIMESTAMP, 8},
		{OID_UUID, 16},
		// Variable length
		{OID_TEXT, -1},
		{OID_VARCHAR, -1},
		{OID_BYTEA, -1},
		{OID_JSON, -1},
		{OID_NUMERIC, -1},
		// Unknown defaults to variable length
		{99999, -1},
	}

	for _, tt := range tests {
		t.Run(OIDToName[tt.oid], func(t *testing.T) {
			size := mapper.GetTypeSize(tt.oid)
			if size != tt.wantSize {
				t.Errorf("GetTypeSize(%d) = %d, want %d", tt.oid, size, tt.wantSize)
			}
		})
	}
}

func TestDefaultTypeMapper_GetTypeName(t *testing.T) {
	mapper := NewTypeMapper()

	tests := []struct {
		oid      uint32
		wantName string
	}{
		{OID_BOOL, "boolean"},
		{OID_INT2, "smallint"},
		{OID_INT4, "integer"},
		{OID_INT8, "bigint"},
		{OID_TEXT, "text"},
		{OID_VARCHAR, "character varying"},
		{OID_TIMESTAMP, "timestamp without time zone"},
		{OID_UUID, "uuid"},
		{99999, "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.wantName, func(t *testing.T) {
			name := mapper.GetTypeName(tt.oid)
			if name != tt.wantName {
				t.Errorf("GetTypeName(%d) = %q, want %q", tt.oid, name, tt.wantName)
			}
		})
	}
}

func TestDefaultTypeMapper_IsArrayType(t *testing.T) {
	mapper := NewTypeMapper()

	tests := []struct {
		oid       uint32
		wantArray bool
	}{
		{OID_INT4_ARRAY, true},
		{OID_INT8_ARRAY, true},
		{OID_TEXT_ARRAY, true},
		{OID_BOOL_ARRAY, true},
		{OID_INT4, false},
		{OID_TEXT, false},
		{OID_BOOL, false},
	}

	for _, tt := range tests {
		t.Run(OIDToName[tt.oid], func(t *testing.T) {
			isArray := mapper.IsArrayType(tt.oid)
			if isArray != tt.wantArray {
				t.Errorf("IsArrayType(%d) = %v, want %v", tt.oid, isArray, tt.wantArray)
			}
		})
	}
}

func TestConvenienceFunctions(t *testing.T) {
	// Test MapPostgreSQLToDuckDB
	mapping := MapPostgreSQLToDuckDB("serial")
	if mapping == nil || mapping.DuckDBType != "INTEGER" {
		t.Error("MapPostgreSQLToDuckDB(serial) failed")
	}

	// Test MapDuckDBToPostgresOID
	oid := MapDuckDBToPostgresOID("INTEGER")
	if oid != OID_INT4 {
		t.Errorf("MapDuckDBToPostgresOID(INTEGER) = %d, want %d", oid, OID_INT4)
	}
}
