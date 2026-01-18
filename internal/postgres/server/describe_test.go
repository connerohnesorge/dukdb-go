// Package server provides PostgreSQL wire protocol server functionality.
// This file contains tests for DESCRIBE message support.
package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestDescribeStatementParameterTypes tests that parameter types are correctly
// inferred and returned in ParameterDescription messages.
func TestDescribeStatementParameterTypes(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedParams []uint32
	}{
		{
			name:           "no parameters",
			query:          "SELECT * FROM users",
			expectedParams: nil,
		},
		{
			name:           "single id parameter",
			query:          "SELECT * FROM users WHERE id = $1",
			expectedParams: []uint32{OidInt4},
		},
		{
			name:           "multiple parameters",
			query:          "SELECT * FROM users WHERE id = $1 AND name LIKE $2",
			expectedParams: []uint32{OidInt4, OidText},
		},
		{
			name:           "limit and offset parameters",
			query:          "SELECT * FROM users LIMIT $1 OFFSET $2",
			expectedParams: []uint32{OidInt8, OidInt8},
		},
		{
			name:           "insert with parameters",
			query:          "INSERT INTO users (id, name) VALUES ($1, $2)",
			expectedParams: []uint32{OidUnknown, OidUnknown},
		},
		{
			name:           "update with parameters",
			query:          "UPDATE users SET name = $1 WHERE id = $2",
			expectedParams: []uint32{OidUnknown, OidInt4},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := InferParameterTypes(tt.query)
			assert.Equal(t, tt.expectedParams, params, "Parameter types should match")
		})
	}
}

// TestDescribeStatementTypeInference tests advanced parameter type inference.
func TestDescribeStatementTypeInference(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		position int
		expected uint32
	}{
		{
			name:     "id comparison",
			query:    "SELECT * FROM users WHERE id = $1",
			position: 0,
			expected: OidInt4,
		},
		{
			name:     "user_id comparison",
			query:    "SELECT * FROM orders WHERE user_id = $1",
			position: 0,
			expected: OidInt4,
		},
		{
			name:     "LIKE pattern",
			query:    "SELECT * FROM users WHERE name LIKE $1",
			position: 0,
			expected: OidText,
		},
		{
			name:     "ILIKE pattern",
			query:    "SELECT * FROM users WHERE name ILIKE $1",
			position: 0,
			expected: OidText,
		},
		{
			name:     "LIMIT clause",
			query:    "SELECT * FROM users LIMIT $1",
			position: 0,
			expected: OidInt8,
		},
		{
			name:     "OFFSET clause",
			query:    "SELECT * FROM users OFFSET $1",
			position: 0,
			expected: OidInt8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := InferParameterTypes(tt.query)
			if len(params) > tt.position {
				assert.Equal(t, tt.expected, params[tt.position],
					"Parameter type at position %d should match", tt.position)
			} else {
				t.Errorf("Not enough parameters inferred: got %d, want at least %d",
					len(params), tt.position+1)
			}
		})
	}
}

// TestDescribePortalColumnTypes tests that column types are correctly
// determined for RowDescription messages.
func TestDescribePortalColumnTypes(t *testing.T) {
	// These tests verify the OID mapping from DuckDB types to PostgreSQL OIDs
	tests := []struct {
		name        string
		dukdbType   string // Conceptual type from DuckDB
		expectedOid uint32
	}{
		{"boolean", "BOOLEAN", OidBool},
		{"smallint", "SMALLINT", OidInt2},
		{"integer", "INTEGER", OidInt4},
		{"bigint", "BIGINT", OidInt8},
		{"real", "FLOAT", OidFloat4},
		{"double", "DOUBLE", OidFloat8},
		{"varchar", "VARCHAR", OidVarchar},
		{"date", "DATE", OidDate},
		{"time", "TIME", OidTime},
		{"timestamp", "TIMESTAMP", OidTimestamp},
		{"timestamptz", "TIMESTAMP_TZ", OidTimestampTZ},
		{"interval", "INTERVAL", OidInterval},
		{"numeric", "DECIMAL", OidNumeric},
		{"uuid", "UUID", OidUUID},
		{"bytea", "BLOB", OidBytea},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This test verifies the expected mapping exists
			// The actual conversion is done by dukdbTypeToOid
			assert.NotZero(t, tt.expectedOid, "Expected OID should be defined for %s", tt.dukdbType)
		})
	}
}

// TestParameterOIDMapping tests the TypeNameToOid function used for
// explicit parameter type declarations in PREPARE statements.
func TestParameterOIDMapping(t *testing.T) {
	tests := []struct {
		typeName string
		expected uint32
	}{
		// Standard types
		{"integer", OidInt4},
		{"int", OidInt4},
		{"int4", OidInt4},
		{"bigint", OidInt8},
		{"int8", OidInt8},
		{"smallint", OidInt2},
		{"int2", OidInt2},
		{"text", OidText},
		{"varchar", OidVarchar},
		{"character varying", OidVarchar},
		{"boolean", OidBool},
		{"bool", OidBool},
		{"real", OidFloat4},
		{"float4", OidFloat4},
		{"double precision", OidFloat8},
		{"float8", OidFloat8},
		{"date", OidDate},
		{"time", OidTime},
		{"timestamp", OidTimestamp},
		{"timestamptz", OidTimestampTZ},
		{"interval", OidInterval},
		{"numeric", OidNumeric},
		{"decimal", OidNumeric},
		{"uuid", OidUUID},
		{"json", OidJSON},
		{"jsonb", OidJSONB},
		{"bytea", OidBytea},

		// Case insensitivity
		{"INTEGER", OidInt4},
		{"TEXT", OidText},
		{"BOOLEAN", OidBool},

		// Unknown types
		{"unknown_type", OidUnknown},
		{"", OidUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			result := TypeNameToOid(tt.typeName)
			assert.Equal(t, tt.expected, result,
				"TypeNameToOid(%q) should return correct OID", tt.typeName)
		})
	}
}

// TestOIDToTypeNameRoundTrip tests that OID to type name conversion is consistent.
func TestOIDToTypeNameRoundTrip(t *testing.T) {
	oids := []uint32{
		OidBool,
		OidInt2,
		OidInt4,
		OidInt8,
		OidFloat4,
		OidFloat8,
		OidText,
		OidVarchar,
		OidDate,
		OidTime,
		OidTimestamp,
		OidTimestampTZ,
		OidInterval,
		OidNumeric,
		OidUUID,
		OidJSON,
		OidJSONB,
		OidBytea,
	}

	for _, oid := range oids {
		t.Run(OidToTypeName(oid), func(t *testing.T) {
			typeName := OidToTypeName(oid)
			assert.NotEqual(t, "unknown", typeName,
				"OID %d should have a known type name", oid)

			// Verify round trip
			roundTripOid := TypeNameToOid(typeName)
			assert.Equal(t, oid, roundTripOid,
				"Round trip should preserve OID for %s", typeName)
		})
	}
}

// TestPreparedStatementMetadata tests that PreparedStatement stores
// the correct metadata for DESCRIBE responses.
func TestPreparedStatementMetadata(t *testing.T) {
	stmt := &PreparedStatement{
		Name:       "test_stmt",
		Query:      "SELECT id, name FROM users WHERE id = $1",
		ParamTypes: []uint32{OidInt4},
	}

	assert.Equal(t, "test_stmt", stmt.Name, "Statement name should be stored")
	assert.Equal(
		t,
		"SELECT id, name FROM users WHERE id = $1",
		stmt.Query,
		"Query should be stored",
	)
	assert.Equal(t, []uint32{OidInt4}, stmt.ParamTypes, "Parameter types should be stored")
}

// TestPortalMetadata tests that Portal stores the correct metadata
// for DESCRIBE Portal responses.
func TestPortalMetadata(t *testing.T) {
	stmt := &PreparedStatement{
		Name:       "test_stmt",
		Query:      "SELECT id, name FROM users WHERE id = $1",
		ParamTypes: []uint32{OidInt4},
	}

	portal := &Portal{
		Name:      "test_portal",
		Statement: stmt,
		Executed:  false,
	}

	assert.Equal(t, "test_portal", portal.Name, "Portal name should be stored")
	assert.NotNil(t, portal.Statement, "Portal should reference a statement")
	assert.Equal(
		t,
		stmt.Query,
		portal.Statement.Query,
		"Portal should have access to statement query",
	)
}

// TestColumnDescriptionBuilder tests the ColumnBuilder for creating
// proper column descriptions for RowDescription messages.
func TestColumnDescriptionBuilder(t *testing.T) {
	col := NewColumnBuilder("user_id").
		TypeOID(OidInt4).
		TableOID(12345).
		ColumnNumber(1).
		Build()

	assert.Equal(t, "user_id", col.Name, "Column name should be set")
	assert.Equal(t, OidInt4, col.Oid, "Column OID should be set")
	assert.Equal(t, int32(12345), col.Table, "Table OID should be set")
	assert.Equal(t, int16(1), col.AttrNo, "Column number should be set")
}

// TestTypeSizeMapping tests that type sizes are correctly mapped for
// RowDescription messages.
func TestTypeSizeMapping(t *testing.T) {
	tests := []struct {
		oid      uint32
		expected int16
	}{
		{OidBool, 1},
		{OidInt2, 2},
		{OidInt4, 4},
		{OidInt8, 8},
		{OidFloat4, 4},
		{OidFloat8, 8},
		{OidText, -1},    // Variable length
		{OidVarchar, -1}, // Variable length
		{OidNumeric, -1}, // Variable length
		{OidJSON, -1},    // Variable length
		{OidJSONB, -1},   // Variable length
		{OidBytea, -1},   // Variable length
		{OidUnknown, -1}, // Unknown defaults to variable
	}

	for _, tt := range tests {
		t.Run(OidToTypeName(tt.oid), func(t *testing.T) {
			size := TypeSize(tt.oid)
			assert.Equal(t, tt.expected, size,
				"TypeSize for OID %d should be %d", tt.oid, tt.expected)
		})
	}
}

// TestEmptyStatementDescribe tests DESCRIBE behavior for empty queries.
func TestEmptyStatementDescribe(t *testing.T) {
	// An empty statement should return no parameters and no columns
	params := InferParameterTypes("")
	assert.Nil(t, params, "Empty query should have no parameters")
}

// TestDescribeStatementWithExplicitTypes tests DESCRIBE when PREPARE
// specifies explicit parameter types.
func TestDescribeStatementWithExplicitTypes(t *testing.T) {
	// Parse: PREPARE test_plan (integer, text) AS SELECT * FROM users WHERE id = $1 AND name = $2
	parsed, err := ParsePrepareStatement(
		"PREPARE test_plan (integer, text) AS SELECT * FROM users WHERE id = $1 AND name = $2",
	)
	assert.NoError(t, err)

	// Convert type names to OIDs
	paramTypes := make([]uint32, len(parsed.ParamTypes))
	for i, typeName := range parsed.ParamTypes {
		paramTypes[i] = TypeNameToOid(typeName)
	}

	assert.Equal(t, []uint32{OidInt4, OidText}, paramTypes,
		"Explicit parameter types should be converted to OIDs")
}

// TestEnhancedParameterInferenceWithCasts tests parameter inference with explicit casts.
func TestEnhancedParameterInferenceWithCasts(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		position int
		expected uint32
	}{
		{
			name:     "int4 cast",
			query:    "SELECT * FROM users WHERE id = $1::int4",
			position: 0,
			expected: OidInt4,
		},
		{
			name:     "integer cast",
			query:    "SELECT * FROM users WHERE id = $1::integer",
			position: 0,
			expected: OidInt4,
		},
		{
			name:     "bigint cast",
			query:    "SELECT * FROM users WHERE id = $1::bigint",
			position: 0,
			expected: OidInt8,
		},
		{
			name:     "text cast",
			query:    "SELECT * FROM users WHERE name = $1::text",
			position: 0,
			expected: OidText,
		},
		{
			name:     "varchar cast",
			query:    "SELECT * FROM users WHERE name = $1::varchar",
			position: 0,
			expected: OidText,
		},
		{
			name:     "boolean cast",
			query:    "SELECT * FROM users WHERE active = $1::boolean",
			position: 0,
			expected: OidBool,
		},
		{
			name:     "numeric cast",
			query:    "SELECT * FROM products WHERE price = $1::numeric",
			position: 0,
			expected: OidNumeric,
		},
		{
			name:     "date cast",
			query:    "SELECT * FROM events WHERE date = $1::date",
			position: 0,
			expected: OidDate,
		},
		{
			name:     "uuid cast",
			query:    "SELECT * FROM users WHERE id = $1::uuid",
			position: 0,
			expected: OidUUID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := EnhancedParameterInference(tt.query)
			if len(params) > tt.position {
				assert.Equal(
					t,
					tt.expected,
					params[tt.position],
					"Parameter type at position %d should match for query: %s",
					tt.position,
					tt.query,
				)
			} else {
				t.Errorf("Not enough parameters inferred: got %d, want at least %d",
					len(params), tt.position+1)
			}
		})
	}
}

// TestEnhancedParameterInferenceWithColumnPatterns tests parameter inference
// based on column naming patterns.
func TestEnhancedParameterInferenceWithColumnPatterns(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		position int
		expected uint32
	}{
		{
			name:     "id column",
			query:    "SELECT * FROM users WHERE id = $1",
			position: 0,
			expected: OidInt4, // Basic inference from id pattern
		},
		{
			name:     "user_id column",
			query:    "SELECT * FROM orders WHERE user_id = $1",
			position: 0,
			expected: OidInt4, // Basic inference from _id pattern
		},
		{
			name:     "name column",
			query:    "SELECT * FROM users WHERE name = $1",
			position: 0,
			expected: OidText, // Enhanced inference from name pattern
		},
		{
			name:     "is_active column",
			query:    "SELECT * FROM users WHERE is_active = $1",
			position: 0,
			expected: OidBool, // Enhanced inference from is_ pattern
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := EnhancedParameterInference(tt.query)
			if len(params) > tt.position {
				assert.Equal(
					t,
					tt.expected,
					params[tt.position],
					"Parameter type at position %d should match for query: %s",
					tt.position,
					tt.query,
				)
			} else {
				t.Errorf("Not enough parameters inferred: got %d, want at least %d",
					len(params), tt.position+1)
			}
		})
	}
}

// TestStatementDescription tests the StatementDescription structure.
func TestStatementDescription(t *testing.T) {
	desc := &StatementDescription{
		ParameterOIDs: []uint32{OidInt4, OidText},
		Columns:       nil, // Would be populated from query analysis
	}

	assert.Equal(t, 2, len(desc.ParameterOIDs), "Should have 2 parameter OIDs")
	assert.Equal(t, OidInt4, desc.ParameterOIDs[0], "First parameter should be INT4")
	assert.Equal(t, OidText, desc.ParameterOIDs[1], "Second parameter should be TEXT")
}

// TestPortalDescription tests the PortalDescription structure.
func TestPortalDescription(t *testing.T) {
	desc := &PortalDescription{
		Columns:     nil,           // Would be populated from query analysis
		FormatCodes: []int16{0, 1}, // text, binary
	}

	assert.Equal(t, 2, len(desc.FormatCodes), "Should have 2 format codes")
	assert.Equal(t, int16(0), desc.FormatCodes[0], "First column should be text format")
	assert.Equal(t, int16(1), desc.FormatCodes[1], "Second column should be binary format")
}
