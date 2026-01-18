package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePrepareStatement(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantName  string
		wantTypes []string
		wantQuery string
		wantErr   bool
	}{
		{
			name:      "simple prepare without types",
			sql:       "PREPARE myplan AS SELECT * FROM users WHERE id = $1",
			wantName:  "myplan",
			wantTypes: nil,
			wantQuery: "SELECT * FROM users WHERE id = $1",
		},
		{
			name:      "prepare with single type",
			sql:       "PREPARE myplan (int) AS SELECT * FROM users WHERE id = $1",
			wantName:  "myplan",
			wantTypes: []string{"int"},
			wantQuery: "SELECT * FROM users WHERE id = $1",
		},
		{
			name:      "prepare with multiple types",
			sql:       "PREPARE myplan (int, text) AS SELECT * FROM users WHERE id = $1 AND name = $2",
			wantName:  "myplan",
			wantTypes: []string{"int", "text"},
			wantQuery: "SELECT * FROM users WHERE id = $1 AND name = $2",
		},
		{
			name:      "prepare with complex types",
			sql:       "PREPARE find_user (integer, character varying) AS SELECT * FROM users WHERE id = $1 AND name = $2",
			wantName:  "find_user",
			wantTypes: []string{"integer", "character varying"},
			wantQuery: "SELECT * FROM users WHERE id = $1 AND name = $2",
		},
		{
			name:      "prepare case insensitive",
			sql:       "prepare MYPLAN as SELECT 1",
			wantName:  "MYPLAN",
			wantTypes: nil,
			wantQuery: "SELECT 1",
		},
		{
			name:      "prepare with insert statement",
			sql:       "PREPARE insert_user (text, int) AS INSERT INTO users (name, age) VALUES ($1, $2)",
			wantName:  "insert_user",
			wantTypes: []string{"text", "int"},
			wantQuery: "INSERT INTO users (name, age) VALUES ($1, $2)",
		},
		{
			name:      "prepare with leading whitespace",
			sql:       "  PREPARE myplan AS SELECT 1",
			wantName:  "myplan",
			wantTypes: nil,
			wantQuery: "SELECT 1",
		},
		{
			name:    "invalid - missing AS",
			sql:     "PREPARE myplan SELECT 1",
			wantErr: true,
		},
		{
			name:    "invalid - missing name",
			sql:     "PREPARE AS SELECT 1",
			wantErr: true,
		},
		{
			name:    "invalid - empty",
			sql:     "PREPARE",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := ParsePrepareStatement(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantName, parsed.Name)
			assert.Equal(t, tt.wantTypes, parsed.ParamTypes)
			assert.Equal(t, tt.wantQuery, parsed.Query)
		})
	}
}

func TestParseExecuteStatement(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantName   string
		wantParams []string
		wantErr    bool
	}{
		{
			name:       "execute without parameters",
			sql:        "EXECUTE myplan",
			wantName:   "myplan",
			wantParams: nil,
		},
		{
			name:       "execute with single parameter",
			sql:        "EXECUTE myplan (1)",
			wantName:   "myplan",
			wantParams: []string{"1"},
		},
		{
			name:       "execute with multiple parameters",
			sql:        "EXECUTE myplan (1, 'hello')",
			wantName:   "myplan",
			wantParams: []string{"1", "'hello'"},
		},
		{
			name:       "execute with string parameter containing comma",
			sql:        "EXECUTE myplan ('hello, world', 42)",
			wantName:   "myplan",
			wantParams: []string{"'hello, world'", "42"},
		},
		{
			name:       "execute case insensitive",
			sql:        "execute MYPLAN",
			wantName:   "MYPLAN",
			wantParams: nil,
		},
		{
			name:       "execute with nested parentheses",
			sql:        "EXECUTE myplan (func(1,2,3))",
			wantName:   "myplan",
			wantParams: []string{"func(1,2,3)"},
		},
		{
			name:    "invalid - missing name",
			sql:     "EXECUTE",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := ParseExecuteStatement(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantName, parsed.Name)
			assert.Equal(t, tt.wantParams, parsed.Parameters)
		})
	}
}

func TestParseDeallocateStatement(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantName string
		wantAll  bool
		wantErr  bool
	}{
		{
			name:     "deallocate specific",
			sql:      "DEALLOCATE myplan",
			wantName: "myplan",
			wantAll:  false,
		},
		{
			name:     "deallocate prepare specific",
			sql:      "DEALLOCATE PREPARE myplan",
			wantName: "myplan",
			wantAll:  false,
		},
		{
			name:    "deallocate all",
			sql:     "DEALLOCATE ALL",
			wantAll: true,
		},
		{
			name:    "deallocate prepare all",
			sql:     "DEALLOCATE PREPARE ALL",
			wantAll: true,
		},
		{
			name:     "deallocate case insensitive",
			sql:      "deallocate MYPLAN",
			wantName: "MYPLAN",
			wantAll:  false,
		},
		{
			name:    "invalid - empty",
			sql:     "DEALLOCATE",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, deallocateAll, err := ParseDeallocateStatement(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantName, name)
			assert.Equal(t, tt.wantAll, deallocateAll)
		})
	}
}

func TestTypeNameToOid(t *testing.T) {
	tests := []struct {
		typeName string
		expected uint32
	}{
		{"boolean", OidBool},
		{"bool", OidBool},
		{"bytea", OidBytea},
		{"smallint", OidInt2},
		{"int2", OidInt2},
		{"integer", OidInt4},
		{"int", OidInt4},
		{"int4", OidInt4},
		{"bigint", OidInt8},
		{"int8", OidInt8},
		{"real", OidFloat4},
		{"float4", OidFloat4},
		{"double precision", OidFloat8},
		{"float8", OidFloat8},
		{"text", OidText},
		{"varchar", OidVarchar},
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
		{"unknown_type", OidUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			result := TypeNameToOid(tt.typeName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestOidToTypeName(t *testing.T) {
	tests := []struct {
		oid      uint32
		expected string
	}{
		{OidBool, "boolean"},
		{OidBytea, "bytea"},
		{OidInt2, "smallint"},
		{OidInt4, "integer"},
		{OidInt8, "bigint"},
		{OidFloat4, "real"},
		{OidFloat8, "double precision"},
		{OidText, "text"},
		{OidVarchar, "varchar"},
		{OidDate, "date"},
		{OidTime, "time"},
		{OidTimestamp, "timestamp"},
		{OidTimestampTZ, "timestamptz"},
		{OidInterval, "interval"},
		{OidNumeric, "numeric"},
		{OidUUID, "uuid"},
		{OidJSON, "json"},
		{OidJSONB, "jsonb"},
		{OidUnknown, "unknown"},
		{99999, "unknown"}, // Unknown OID
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := OidToTypeName(tt.oid)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestInferParameterTypes(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected []uint32
	}{
		{
			name:     "no parameters",
			query:    "SELECT * FROM users",
			expected: nil,
		},
		{
			name:     "id comparison with word boundary",
			query:    "SELECT * FROM users WHERE id = $1",
			expected: []uint32{OidInt4},
		},
		{
			name:     "user_id comparison with underscore pattern",
			query:    "SELECT * FROM users WHERE user_id = $1",
			expected: []uint32{OidInt4},
		},
		{
			name:     "LIMIT parameter",
			query:    "SELECT * FROM users LIMIT $1",
			expected: []uint32{OidInt8},
		},
		{
			name:     "OFFSET parameter",
			query:    "SELECT * FROM users OFFSET $1",
			expected: []uint32{OidInt8},
		},
		{
			name:     "LIKE parameter",
			query:    "SELECT * FROM users WHERE name LIKE $1",
			expected: []uint32{OidText},
		},
		{
			name:     "ILIKE parameter",
			query:    "SELECT * FROM users WHERE name ILIKE $1",
			expected: []uint32{OidText},
		},
		{
			name:     "multiple parameters mixed",
			query:    "SELECT * FROM users WHERE id = $1 LIMIT $2",
			expected: []uint32{OidInt4, OidInt8},
		},
		{
			name:     "unrecognized parameter pattern defaults to unknown",
			query:    "SELECT * FROM users WHERE age > $1",
			expected: []uint32{OidUnknown},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := InferParameterTypes(tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPreparedStmtCache(t *testing.T) {
	cache := NewPreparedStmtCache()

	// Test empty cache
	_, ok := cache.Get("nonexistent")
	assert.False(t, ok)
	assert.Equal(t, 0, cache.Count())

	// Test Set and Get
	stmt1 := &PreparedStatement{Name: "stmt1", Query: "SELECT 1"}
	err := cache.Set("stmt1", stmt1)
	require.NoError(t, err)

	retrieved, ok := cache.Get("stmt1")
	assert.True(t, ok)
	assert.Equal(t, stmt1.Name, retrieved.Name)
	assert.Equal(t, stmt1.Query, retrieved.Query)
	assert.Equal(t, 1, cache.Count())

	// Test overwrite existing
	stmt1b := &PreparedStatement{Name: "stmt1", Query: "SELECT 2"}
	err = cache.Set("stmt1", stmt1b)
	require.NoError(t, err)

	retrieved, ok = cache.Get("stmt1")
	assert.True(t, ok)
	assert.Equal(t, "SELECT 2", retrieved.Query)
	assert.Equal(t, 1, cache.Count())

	// Test multiple statements
	stmt2 := &PreparedStatement{Name: "stmt2", Query: "SELECT 3"}
	err = cache.Set("stmt2", stmt2)
	require.NoError(t, err)
	assert.Equal(t, 2, cache.Count())

	// Test Names
	names := cache.Names()
	assert.ElementsMatch(t, []string{"stmt1", "stmt2"}, names)

	// Test Delete
	deleted := cache.Delete("stmt1")
	assert.True(t, deleted)
	assert.Equal(t, 1, cache.Count())

	deleted = cache.Delete("nonexistent")
	assert.False(t, deleted)

	// Test Clear
	cache.Clear()
	assert.Equal(t, 0, cache.Count())

	// Test Close
	stmt3 := &PreparedStatement{Name: "stmt3", Query: "SELECT 4"}
	err = cache.Set("stmt3", stmt3)
	require.NoError(t, err)

	err = cache.Close()
	require.NoError(t, err)
	assert.Equal(t, 0, cache.Count())
}

func TestPortalCache(t *testing.T) {
	cache := NewPortalCache()

	// Test empty cache
	_, ok := cache.Get("nonexistent")
	assert.False(t, ok)

	// Test Set and Get
	portal := &Portal{Name: "portal1", Executed: false}
	cache.Set("portal1", portal)

	retrieved, ok := cache.Get("portal1")
	assert.True(t, ok)
	assert.Equal(t, portal.Name, retrieved.Name)
	assert.False(t, retrieved.Executed)

	// Test Delete
	deleted := cache.Delete("portal1")
	assert.True(t, deleted)

	deleted = cache.Delete("nonexistent")
	assert.False(t, deleted)

	_, ok = cache.Get("portal1")
	assert.False(t, ok)

	// Test Clear
	cache.Set("p1", &Portal{Name: "p1"})
	cache.Set("p2", &Portal{Name: "p2"})
	cache.Clear()

	_, ok = cache.Get("p1")
	assert.False(t, ok)
	_, ok = cache.Get("p2")
	assert.False(t, ok)

	// Test Close
	cache.Set("p3", &Portal{Name: "p3"})
	err := cache.Close()
	require.NoError(t, err)
	_, ok = cache.Get("p3")
	assert.False(t, ok)
}

func TestParseParameterList(t *testing.T) {
	tests := []struct {
		name     string
		params   string
		expected []string
	}{
		{
			name:     "empty",
			params:   "",
			expected: nil,
		},
		{
			name:     "single value",
			params:   "1",
			expected: []string{"1"},
		},
		{
			name:     "multiple values",
			params:   "1, 2, 3",
			expected: []string{"1", "2", "3"},
		},
		{
			name:     "string with comma",
			params:   "'hello, world', 42",
			expected: []string{"'hello, world'", "42"},
		},
		{
			name:     "nested parentheses",
			params:   "ARRAY(1,2,3), 42",
			expected: []string{"ARRAY(1,2,3)", "42"},
		},
		{
			name:     "double quoted string",
			params:   "\"column,name\", 42",
			expected: []string{"\"column,name\"", "42"},
		},
		{
			name:     "escaped single quote",
			params:   "'it''s a test', 42",
			expected: []string{"'it''s a test'", "42"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseParameterList(tt.params)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSessionPreparedStatements(t *testing.T) {
	config := NewConfig()
	server, err := NewServer(config)
	require.NoError(t, err)

	session := NewSession(server, "user", "db", "127.0.0.1:12345")

	// Verify prepared statement cache is initialized
	assert.NotNil(t, session.PreparedStatements())
	assert.Equal(t, 0, session.PreparedStatements().Count())

	// Verify portal cache is initialized
	assert.NotNil(t, session.Portals())

	// Add a prepared statement
	stmt := &PreparedStatement{Name: "test", Query: "SELECT 1"}
	err = session.PreparedStatements().Set("test", stmt)
	require.NoError(t, err)
	assert.Equal(t, 1, session.PreparedStatements().Count())

	// Verify close cleans up
	err = session.Close()
	require.NoError(t, err)
}
