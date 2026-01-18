package parser

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCreateFunction(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple SQL UDF",
			sql:     "CREATE FUNCTION add_one(x INTEGER) RETURNS INTEGER AS 'SELECT x + 1'",
			wantErr: false,
		},
		{
			name:    "UDF with multiple parameters",
			sql:     "CREATE FUNCTION add(a INTEGER, b INTEGER) RETURNS INTEGER AS 'SELECT a + b'",
			wantErr: false,
		},
		{
			name:    "UDF with OR REPLACE",
			sql:     "CREATE OR REPLACE FUNCTION multiply(x INTEGER, y INTEGER) RETURNS INTEGER AS 'SELECT x * y'",
			wantErr: false,
		},
		{
			name:    "UDF with dollar-quoted body",
			sql:     "CREATE FUNCTION greet(name VARCHAR) RETURNS VARCHAR AS $$SELECT 'Hello, ' || name$$",
			wantErr: false,
		},
		{
			name:    "UDF with multi-line dollar-quoted body",
			sql:     "CREATE FUNCTION complex_func(x INTEGER) RETURNS INTEGER AS $$\nSELECT\n  CASE\n    WHEN x < 0 THEN -x\n    ELSE x\n  END\n$$",
			wantErr: false,
		},
		{
			name:    "UDF with LANGUAGE clause",
			sql:     "CREATE FUNCTION py_len(s VARCHAR) RETURNS INTEGER LANGUAGE python AS 'return len(s)'",
			wantErr: false,
		},
		{
			name:    "UDF with IMMUTABLE attribute",
			sql:     "CREATE FUNCTION double(x INTEGER) RETURNS INTEGER IMMUTABLE AS 'SELECT x * 2'",
			wantErr: false,
		},
		{
			name:    "UDF with STABLE attribute",
			sql:     "CREATE FUNCTION get_config(key VARCHAR) RETURNS VARCHAR STABLE AS 'SELECT value FROM config WHERE name = key'",
			wantErr: false,
		},
		{
			name:    "UDF with VOLATILE attribute",
			sql:     "CREATE FUNCTION random_val() RETURNS INTEGER VOLATILE AS 'SELECT floor(random() * 100)::INTEGER'",
			wantErr: false,
		},
		{
			name:    "UDF with STRICT attribute",
			sql:     "CREATE FUNCTION safe_add(a INTEGER, b INTEGER) RETURNS INTEGER STRICT AS 'SELECT a + b'",
			wantErr: false,
		},
		{
			name:    "UDF with LEAKPROOF attribute",
			sql:     "CREATE FUNCTION compare(a INTEGER, b INTEGER) RETURNS BOOLEAN LEAKPROOF AS 'SELECT a = b'",
			wantErr: false,
		},
		{
			name:    "UDF with PARALLEL SAFE",
			sql:     "CREATE FUNCTION pure_func(x INTEGER) RETURNS INTEGER PARALLEL SAFE AS 'SELECT x'",
			wantErr: false,
		},
		{
			name:    "UDF with PARALLEL UNSAFE",
			sql:     "CREATE FUNCTION unsafe_func(x INTEGER) RETURNS INTEGER PARALLEL UNSAFE AS 'SELECT x'",
			wantErr: false,
		},
		{
			name:    "UDF with PARALLEL RESTRICTED",
			sql:     "CREATE FUNCTION restricted_func(x INTEGER) RETURNS INTEGER PARALLEL RESTRICTED AS 'SELECT x'",
			wantErr: false,
		},
		{
			name:    "UDF with all attributes",
			sql:     "CREATE OR REPLACE FUNCTION full_func(x INTEGER) RETURNS INTEGER LANGUAGE sql IMMUTABLE STRICT LEAKPROOF PARALLEL SAFE AS 'SELECT x'",
			wantErr: false,
		},
		{
			name:    "UDF with schema qualification",
			sql:     "CREATE FUNCTION myschema.myfunc(x INTEGER) RETURNS INTEGER AS 'SELECT x'",
			wantErr: false,
		},
		{
			name:    "UDF with no parameters",
			sql:     "CREATE FUNCTION now_epoch() RETURNS BIGINT AS 'SELECT epoch(now())'",
			wantErr: false,
		},
		{
			name:    "UDF with VARCHAR parameter",
			sql:     "CREATE FUNCTION upper_case(s VARCHAR) RETURNS VARCHAR AS 'SELECT UPPER(s)'",
			wantErr: false,
		},
		{
			name:    "UDF with DOUBLE return type",
			sql:     "CREATE FUNCTION safe_div(a DOUBLE, b DOUBLE) RETURNS DOUBLE AS 'SELECT CASE WHEN b = 0 THEN NULL ELSE a / b END'",
			wantErr: false,
		},
		{
			name:    "UDF with VARCHAR size parameter",
			sql:     "CREATE FUNCTION truncate_str(s VARCHAR(100)) RETURNS VARCHAR(50) AS 'SELECT LEFT(s, 50)'",
			wantErr: false,
		},
		// Error cases
		{
			name:    "missing function name",
			sql:     "CREATE FUNCTION (x INTEGER) RETURNS INTEGER AS 'SELECT x'",
			wantErr: true,
		},
		{
			name:    "missing parameters",
			sql:     "CREATE FUNCTION add RETURNS INTEGER AS 'SELECT 1'",
			wantErr: true,
		},
		{
			name:    "missing RETURNS clause",
			sql:     "CREATE FUNCTION add(x INTEGER) AS 'SELECT x'",
			wantErr: true,
		},
		{
			name:    "missing AS clause",
			sql:     "CREATE FUNCTION add(x INTEGER) RETURNS INTEGER",
			wantErr: true,
		},
		{
			name:    "missing function body",
			sql:     "CREATE FUNCTION add(x INTEGER) RETURNS INTEGER AS",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, stmt)
			_, ok := stmt.(*CreateFunctionStmt)
			assert.True(t, ok, "expected CreateFunctionStmt, got %T", stmt)
		})
	}
}

func TestParseCreateFunctionAST(t *testing.T) {
	t.Run("simple SQL UDF", func(t *testing.T) {
		stmt, err := Parse(
			"CREATE FUNCTION add(a INTEGER, b INTEGER) RETURNS INTEGER AS 'SELECT a + b'",
		)
		require.NoError(t, err)

		fn, ok := stmt.(*CreateFunctionStmt)
		require.True(t, ok)

		assert.Equal(t, "add", fn.Name)
		assert.Equal(t, "", fn.Schema)
		assert.False(t, fn.OrReplace)
		assert.Len(t, fn.Params, 2)
		assert.Equal(t, "a", fn.Params[0].Name)
		assert.Equal(t, dukdb.TYPE_INTEGER, fn.Params[0].Type)
		assert.Equal(t, "b", fn.Params[1].Name)
		assert.Equal(t, dukdb.TYPE_INTEGER, fn.Params[1].Type)
		assert.Equal(t, dukdb.TYPE_INTEGER, fn.Returns)
		assert.Equal(t, "sql", fn.Language)
		assert.Equal(t, "SELECT a + b", fn.Body)
		assert.Equal(t, VolatilityVolatile, fn.Volatility)
		assert.False(t, fn.Strict)
		assert.False(t, fn.Leakproof)
		assert.Equal(t, "", fn.ParallelSafe)
	})

	t.Run("Python UDF with dollar-quoted body", func(t *testing.T) {
		stmt, err := Parse(
			"CREATE FUNCTION py_len(s VARCHAR) RETURNS INTEGER LANGUAGE python AS $$import sys; return len(s)$$",
		)
		require.NoError(t, err)

		fn, ok := stmt.(*CreateFunctionStmt)
		require.True(t, ok)

		assert.Equal(t, "py_len", fn.Name)
		assert.Len(t, fn.Params, 1)
		assert.Equal(t, "s", fn.Params[0].Name)
		assert.Equal(t, dukdb.TYPE_VARCHAR, fn.Params[0].Type)
		assert.Equal(t, dukdb.TYPE_INTEGER, fn.Returns)
		assert.Equal(t, "python", fn.Language)
		assert.Equal(t, "import sys; return len(s)", fn.Body)
	})

	t.Run("UDF with all attributes", func(t *testing.T) {
		stmt, err := Parse(
			"CREATE OR REPLACE FUNCTION safe_div(a DOUBLE, b DOUBLE) RETURNS DOUBLE LANGUAGE sql IMMUTABLE STRICT LEAKPROOF PARALLEL SAFE AS 'SELECT CASE WHEN b=0 THEN NULL ELSE a/b END'",
		)
		require.NoError(t, err)

		fn, ok := stmt.(*CreateFunctionStmt)
		require.True(t, ok)

		assert.Equal(t, "safe_div", fn.Name)
		assert.True(t, fn.OrReplace)
		assert.Len(t, fn.Params, 2)
		assert.Equal(t, dukdb.TYPE_DOUBLE, fn.Returns)
		assert.Equal(t, "sql", fn.Language)
		assert.Equal(t, VolatilityImmutable, fn.Volatility)
		assert.True(t, fn.Strict)
		assert.True(t, fn.Leakproof)
		assert.Equal(t, "SAFE", fn.ParallelSafe)
	})

	t.Run("UDF with schema qualification", func(t *testing.T) {
		stmt, err := Parse(
			"CREATE FUNCTION myschema.myfunc(x INTEGER) RETURNS INTEGER AS 'SELECT x'",
		)
		require.NoError(t, err)

		fn, ok := stmt.(*CreateFunctionStmt)
		require.True(t, ok)

		assert.Equal(t, "myschema", fn.Schema)
		assert.Equal(t, "myfunc", fn.Name)
	})

	t.Run("UDF with no parameters", func(t *testing.T) {
		stmt, err := Parse("CREATE FUNCTION get_one() RETURNS INTEGER AS 'SELECT 1'")
		require.NoError(t, err)

		fn, ok := stmt.(*CreateFunctionStmt)
		require.True(t, ok)

		assert.Equal(t, "get_one", fn.Name)
		assert.Len(t, fn.Params, 0)
	})

	t.Run("UDF with STABLE volatility", func(t *testing.T) {
		stmt, err := Parse(
			"CREATE FUNCTION read_config(key VARCHAR) RETURNS VARCHAR STABLE AS 'SELECT value FROM config WHERE name = key'",
		)
		require.NoError(t, err)

		fn, ok := stmt.(*CreateFunctionStmt)
		require.True(t, ok)

		assert.Equal(t, VolatilityStable, fn.Volatility)
	})

	t.Run("UDF with VOLATILE volatility", func(t *testing.T) {
		stmt, err := Parse(
			"CREATE FUNCTION rand_val() RETURNS INTEGER VOLATILE AS 'SELECT random()'",
		)
		require.NoError(t, err)

		fn, ok := stmt.(*CreateFunctionStmt)
		require.True(t, ok)

		assert.Equal(t, VolatilityVolatile, fn.Volatility)
	})

	t.Run("UDF with PARALLEL UNSAFE", func(t *testing.T) {
		stmt, err := Parse(
			"CREATE FUNCTION unsafe_op(x INTEGER) RETURNS INTEGER PARALLEL UNSAFE AS 'SELECT x'",
		)
		require.NoError(t, err)

		fn, ok := stmt.(*CreateFunctionStmt)
		require.True(t, ok)

		assert.Equal(t, "UNSAFE", fn.ParallelSafe)
	})

	t.Run("UDF with PARALLEL RESTRICTED", func(t *testing.T) {
		stmt, err := Parse(
			"CREATE FUNCTION restricted_op(x INTEGER) RETURNS INTEGER PARALLEL RESTRICTED AS 'SELECT x'",
		)
		require.NoError(t, err)

		fn, ok := stmt.(*CreateFunctionStmt)
		require.True(t, ok)

		assert.Equal(t, "RESTRICTED", fn.ParallelSafe)
	})

	t.Run("UDF with multi-line dollar-quoted body", func(t *testing.T) {
		sql := `CREATE FUNCTION multiline(x INTEGER) RETURNS INTEGER AS $$
SELECT
  CASE
    WHEN x < 0 THEN -x
    ELSE x
  END
$$`
		stmt, err := Parse(sql)
		require.NoError(t, err)

		fn, ok := stmt.(*CreateFunctionStmt)
		require.True(t, ok)

		assert.Contains(t, fn.Body, "SELECT")
		assert.Contains(t, fn.Body, "CASE")
		assert.Contains(t, fn.Body, "WHEN x < 0 THEN -x")
	})
}

func TestCreateFunctionTableExtractor(t *testing.T) {
	// CREATE FUNCTION should not extract any table references
	// even if the body references tables (the body is opaque)
	stmt, err := Parse(
		"CREATE FUNCTION get_user(id INTEGER) RETURNS VARCHAR AS 'SELECT name FROM users WHERE id = id'",
	)
	require.NoError(t, err)

	fn, ok := stmt.(*CreateFunctionStmt)
	require.True(t, ok)

	te := NewTableExtractor(false)
	fn.Accept(te)

	tables := te.GetTables()
	assert.Empty(t, tables, "CREATE FUNCTION should not extract table references from the body")
}

func TestCreateFunctionStmtType(t *testing.T) {
	stmt, err := Parse("CREATE FUNCTION test(x INTEGER) RETURNS INTEGER AS 'SELECT x'")
	require.NoError(t, err)

	assert.Equal(t, dukdb.STATEMENT_TYPE_CREATE, stmt.Type())
}
