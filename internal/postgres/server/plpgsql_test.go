package server

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockBackendConn implements BackendConnInterface for testing.
type mockBackendConn struct {
	queryResults  []map[string]interface{}
	queryColumns  []string
	executeResult int64
	queryErr      error
	executeErr    error
}

func (m *mockBackendConn) Query(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) ([]map[string]interface{}, []string, error) {
	if m.queryErr != nil {
		return nil, nil, m.queryErr
	}
	return m.queryResults, m.queryColumns, nil
}

func (m *mockBackendConn) Execute(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (int64, error) {
	if m.executeErr != nil {
		return 0, m.executeErr
	}
	return m.executeResult, nil
}

func TestPLpgSQLManager_CreateFunction(t *testing.T) {
	pm := NewPLpgSQLManager()

	fn := &StoredFunction{
		Name:       "add_numbers",
		Parameters: []FunctionParameter{{Name: "a", Type: "INTEGER"}, {Name: "b", Type: "INTEGER"}},
		ReturnType: "INTEGER",
		Language:   "plpgsql",
		Body:       "BEGIN RETURN a + b; END;",
	}

	err := pm.CreateFunction(context.Background(), fn)
	require.NoError(t, err)

	// Verify function was stored
	storedFn, ok := pm.GetFunction("public", "add_numbers")
	assert.True(t, ok)
	assert.Equal(t, "add_numbers", storedFn.Name)
}

func TestPLpgSQLManager_CreateProcedure(t *testing.T) {
	pm := NewPLpgSQLManager()

	proc := &StoredProcedure{
		Name:       "do_something",
		Parameters: []FunctionParameter{{Name: "x", Type: "INTEGER"}},
		Language:   "plpgsql",
		Body:       "BEGIN NULL; END;",
	}

	err := pm.CreateProcedure(context.Background(), proc)
	require.NoError(t, err)

	// Verify procedure was stored
	storedProc, ok := pm.GetProcedure("public", "do_something")
	assert.True(t, ok)
	assert.Equal(t, "do_something", storedProc.Name)
}

func TestPLpgSQLManager_DropFunction(t *testing.T) {
	pm := NewPLpgSQLManager()

	fn := &StoredFunction{
		Name:       "to_drop",
		ReturnType: "VOID",
		Language:   "plpgsql",
		Body:       "BEGIN NULL; END;",
	}

	_ = pm.CreateFunction(context.Background(), fn)

	// Drop the function
	err := pm.DropFunction(context.Background(), "public", "to_drop", nil, false)
	require.NoError(t, err)

	// Verify it's gone
	_, ok := pm.GetFunction("public", "to_drop")
	assert.False(t, ok)

	// Drop non-existent without IF EXISTS
	err = pm.DropFunction(context.Background(), "public", "nonexistent", nil, false)
	assert.Equal(t, ErrFunctionNotFound, err)

	// Drop non-existent with IF EXISTS
	err = pm.DropFunction(context.Background(), "public", "nonexistent", nil, true)
	assert.NoError(t, err)
}

func TestPLpgSQLManager_DropProcedure(t *testing.T) {
	pm := NewPLpgSQLManager()

	proc := &StoredProcedure{
		Name:     "to_drop_proc",
		Language: "plpgsql",
		Body:     "BEGIN NULL; END;",
	}

	_ = pm.CreateProcedure(context.Background(), proc)

	// Drop the procedure
	err := pm.DropProcedure(context.Background(), "public", "to_drop_proc", nil, false)
	require.NoError(t, err)

	// Verify it's gone
	_, ok := pm.GetProcedure("public", "to_drop_proc")
	assert.False(t, ok)
}

func TestParseCreateFunction(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected *StoredFunction
		wantErr  bool
	}{
		{
			name: "simple function",
			sql: `CREATE FUNCTION add(a INTEGER, b INTEGER) RETURNS INTEGER AS $$
				BEGIN
					RETURN a + b;
				END;
			$$ LANGUAGE plpgsql`,
			expected: &StoredFunction{
				Schema:     "public",
				Name:       "add",
				ReturnType: "INTEGER",
				Language:   "plpgsql",
			},
		},
		{
			name: "function with schema",
			sql: `CREATE FUNCTION myschema.multiply(x INT, y INT) RETURNS INT AS $$
				BEGIN RETURN x * y; END;
			$$ LANGUAGE plpgsql`,
			expected: &StoredFunction{
				Schema:     "myschema",
				Name:       "multiply",
				ReturnType: "INT",
				Language:   "plpgsql",
			},
		},
		{
			name: "immutable function",
			sql: `CREATE FUNCTION get_pi() RETURNS DOUBLE PRECISION AS $$
				BEGIN RETURN 3.14159; END;
			$$ LANGUAGE plpgsql IMMUTABLE`,
			expected: &StoredFunction{
				Schema:     "public",
				Name:       "get_pi",
				ReturnType: "DOUBLE",
				Language:   "plpgsql",
				Volatility: VolatilityImmutable,
			},
		},
		{
			name: "strict function",
			sql: `CREATE FUNCTION safe_divide(a INT, b INT) RETURNS INT AS $$
				BEGIN RETURN a / b; END;
			$$ LANGUAGE plpgsql STRICT`,
			expected: &StoredFunction{
				Schema:     "public",
				Name:       "safe_divide",
				ReturnType: "INT",
				Language:   "plpgsql",
				Strict:     true,
			},
		},
		{
			name: "set returning function",
			sql: `CREATE FUNCTION generate_series(start INT, stop INT) RETURNS SETOF INT AS $$
				BEGIN
					FOR i IN start..stop LOOP
						RETURN NEXT i;
					END LOOP;
				END;
			$$ LANGUAGE plpgsql`,
			expected: &StoredFunction{
				Schema:     "public",
				Name:       "generate_series",
				ReturnType: "INT",
				Language:   "plpgsql",
				ReturnsSet: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, err := ParseCreateFunction(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected.Schema, fn.Schema)
			assert.Equal(t, tt.expected.Name, fn.Name)
			assert.Equal(t, tt.expected.ReturnType, fn.ReturnType)
			assert.Equal(t, tt.expected.Language, fn.Language)
			if tt.expected.Volatility != 0 {
				assert.Equal(t, tt.expected.Volatility, fn.Volatility)
			}
			assert.Equal(t, tt.expected.Strict, fn.Strict)
			assert.Equal(t, tt.expected.ReturnsSet, fn.ReturnsSet)
		})
	}
}

func TestParseCreateProcedure(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected *StoredProcedure
		wantErr  bool
	}{
		{
			name: "simple procedure",
			sql: `CREATE PROCEDURE do_work(x INT) AS $$
				BEGIN
					NULL;
				END;
			$$ LANGUAGE plpgsql`,
			expected: &StoredProcedure{
				Schema:   "public",
				Name:     "do_work",
				Language: "plpgsql",
			},
		},
		{
			name: "procedure with schema",
			sql: `CREATE PROCEDURE myschema.cleanup() AS $$
				BEGIN NULL; END;
			$$ LANGUAGE plpgsql`,
			expected: &StoredProcedure{
				Schema:   "myschema",
				Name:     "cleanup",
				Language: "plpgsql",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc, err := ParseCreateProcedure(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected.Schema, proc.Schema)
			assert.Equal(t, tt.expected.Name, proc.Name)
			assert.Equal(t, tt.expected.Language, proc.Language)
		})
	}
}

func TestPLpgSQLParser_SimpleBlock(t *testing.T) {
	body := `
	DECLARE
		x INTEGER := 10;
	BEGIN
		RETURN x;
	END;
	`

	parser := NewPLpgSQLParser(body)
	block, err := parser.Parse()
	require.NoError(t, err)
	require.NotNil(t, block)

	// Check declarations
	require.NotNil(t, block.Declare)
	require.Len(t, block.Declare.Declarations, 1)
	assert.Equal(t, "x", block.Declare.Declarations[0].Name)
	assert.Equal(t, "INTEGER", block.Declare.Declarations[0].DataType)

	// Check body
	require.Len(t, block.Body, 1)
	ret, ok := block.Body[0].(*PLpgSQLReturnStmt)
	require.True(t, ok)
	assert.NotNil(t, ret.Expr)
}

func TestPLpgSQLParser_IfStatement(t *testing.T) {
	body := `
	BEGIN
		IF x > 10 THEN
			y := 1;
		ELSIF x > 5 THEN
			y := 2;
		ELSE
			y := 3;
		END IF;
	END;
	`

	parser := NewPLpgSQLParser(body)
	block, err := parser.Parse()
	require.NoError(t, err)
	require.NotNil(t, block)

	// Check body has IF statement
	require.Len(t, block.Body, 1)
	ifStmt, ok := block.Body[0].(*PLpgSQLIfStmt)
	require.True(t, ok)
	assert.NotNil(t, ifStmt.Condition)
	assert.Len(t, ifStmt.ThenBody, 1)
	assert.Len(t, ifStmt.ElsifClauses, 1)
	assert.Len(t, ifStmt.ElseBody, 1)
}

func TestPLpgSQLParser_WhileLoop(t *testing.T) {
	body := `
	BEGIN
		WHILE i < 10 LOOP
			i := i + 1;
		END LOOP;
	END;
	`

	parser := NewPLpgSQLParser(body)
	block, err := parser.Parse()
	require.NoError(t, err)
	require.NotNil(t, block)

	require.Len(t, block.Body, 1)
	whileStmt, ok := block.Body[0].(*PLpgSQLWhileStmt)
	require.True(t, ok)
	assert.NotNil(t, whileStmt.Condition)
	assert.Len(t, whileStmt.Body, 1)
}

func TestPLpgSQLParser_ForLoop(t *testing.T) {
	body := `
	BEGIN
		FOR i IN 1..10 LOOP
			total := total + i;
		END LOOP;
	END;
	`

	parser := NewPLpgSQLParser(body)
	block, err := parser.Parse()
	require.NoError(t, err)
	require.NotNil(t, block)

	require.Len(t, block.Body, 1)
	forStmt, ok := block.Body[0].(*PLpgSQLForStmt)
	require.True(t, ok)
	assert.Equal(t, "i", forStmt.Variable)
	assert.NotNil(t, forStmt.LowerBound)
	assert.NotNil(t, forStmt.UpperBound)
}

func TestPLpgSQLParser_RaiseStatement(t *testing.T) {
	body := `
	BEGIN
		RAISE NOTICE 'Value is %', x;
		RAISE EXCEPTION 'Error!' USING ERRCODE = '22000';
	END;
	`

	parser := NewPLpgSQLParser(body)
	block, err := parser.Parse()
	require.NoError(t, err)
	require.NotNil(t, block)

	require.Len(t, block.Body, 2)

	notice, ok := block.Body[0].(*PLpgSQLRaiseStmt)
	require.True(t, ok)
	assert.Equal(t, RaiseLevelNotice, notice.Level)
	assert.Equal(t, "Value is %", notice.Message)

	exc, ok := block.Body[1].(*PLpgSQLRaiseStmt)
	require.True(t, ok)
	assert.Equal(t, RaiseLevelException, exc.Level)
	assert.Equal(t, "22000", exc.Options["ERRCODE"])
}

func TestPLpgSQLParser_ExceptionBlock(t *testing.T) {
	body := `
	BEGIN
		x := 1 / y;
	EXCEPTION
		WHEN division_by_zero THEN
			x := 0;
		WHEN OTHERS THEN
			RAISE;
	END;
	`

	parser := NewPLpgSQLParser(body)
	block, err := parser.Parse()
	require.NoError(t, err)
	require.NotNil(t, block)
	require.NotNil(t, block.Exception)
	require.Len(t, block.Exception.Handlers, 2)

	handler1 := block.Exception.Handlers[0]
	assert.Contains(t, handler1.Conditions, "division_by_zero")

	handler2 := block.Exception.Handlers[1]
	assert.Contains(t, handler2.Conditions, "OTHERS")
}

func TestPLpgSQLParser_ExitContinue(t *testing.T) {
	body := `
	BEGIN
		LOOP
			EXIT WHEN i > 10;
			CONTINUE WHEN i = 5;
		END LOOP;
	END;
	`

	parser := NewPLpgSQLParser(body)
	block, err := parser.Parse()
	require.NoError(t, err)
	require.NotNil(t, block)

	require.Len(t, block.Body, 1)
	loopStmt, ok := block.Body[0].(*PLpgSQLLoopStmt)
	require.True(t, ok)
	require.Len(t, loopStmt.Body, 2)

	exitStmt, ok := loopStmt.Body[0].(*PLpgSQLExitStmt)
	require.True(t, ok)
	assert.NotNil(t, exitStmt.Condition)

	continueStmt, ok := loopStmt.Body[1].(*PLpgSQLContinueStmt)
	require.True(t, ok)
	assert.NotNil(t, continueStmt.Condition)
}

func TestPLpgSQLScope(t *testing.T) {
	parent := NewPLpgSQLScope(nil)
	err := parent.Declare("x", "INTEGER", 10, false, false)
	require.NoError(t, err)

	child := NewPLpgSQLScope(parent)
	err = child.Declare("y", "INTEGER", 20, false, false)
	require.NoError(t, err)

	// Child can access parent variable
	v, ok := child.Get("x")
	assert.True(t, ok)
	assert.Equal(t, 10, v.Value)

	// Parent can't access child variable
	_, ok = parent.Get("y")
	assert.False(t, ok)

	// Child can modify parent variable
	err = child.Set("x", 100)
	require.NoError(t, err)
	v, _ = parent.Get("x")
	assert.Equal(t, 100, v.Value)
}

func TestPLpgSQLScope_Constant(t *testing.T) {
	scope := NewPLpgSQLScope(nil)
	err := scope.Declare("PI", "DOUBLE", 3.14, true, false)
	require.NoError(t, err)

	// Can't modify constant
	err = scope.Set("PI", 3.0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CONSTANT")
}

func TestPLpgSQLScope_NotNull(t *testing.T) {
	scope := NewPLpgSQLScope(nil)
	err := scope.Declare("x", "INTEGER", 10, false, true)
	require.NoError(t, err)

	// Can't set to NULL
	err = scope.Set("x", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "null")
}

func TestPLpgSQLExecutor_SimpleFunction(t *testing.T) {
	pm := NewPLpgSQLManager()
	conn := &mockBackendConn{
		queryResults: []map[string]interface{}{{"result": int64(30)}},
		queryColumns: []string{"result"},
	}

	fn := &StoredFunction{
		Name:       "add_numbers",
		Parameters: []FunctionParameter{{Name: "a", Type: "INTEGER"}, {Name: "b", Type: "INTEGER"}},
		ReturnType: "INTEGER",
		Language:   "plpgsql",
		Body: `
		DECLARE
			result INTEGER;
		BEGIN
			result := a + b;
			RETURN result;
		END;
		`,
	}

	err := pm.CreateFunction(context.Background(), fn)
	require.NoError(t, err)

	result, err := pm.ExecuteFunction(
		context.Background(),
		conn,
		"public",
		"add_numbers",
		[]interface{}{10, 20},
	)
	require.NoError(t, err)
	// Result comes from expression evaluation via SQL
	assert.NotNil(t, result)
}

func TestPLpgSQLManager_SQLFunction(t *testing.T) {
	pm := NewPLpgSQLManager()
	conn := &mockBackendConn{
		queryResults: []map[string]interface{}{{"result": int64(30)}},
		queryColumns: []string{"result"},
	}

	fn := &StoredFunction{
		Name:       "sql_add",
		Parameters: []FunctionParameter{{Name: "a", Type: "INTEGER"}, {Name: "b", Type: "INTEGER"}},
		ReturnType: "INTEGER",
		Language:   "sql",
		Body:       "SELECT $1 + $2",
	}

	err := pm.CreateFunction(context.Background(), fn)
	require.NoError(t, err)

	result, err := pm.ExecuteFunction(
		context.Background(),
		conn,
		"public",
		"sql_add",
		[]interface{}{10, 20},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(30), result)
}

func TestPLpgSQLManager_StrictFunction(t *testing.T) {
	pm := NewPLpgSQLManager()
	conn := &mockBackendConn{}

	fn := &StoredFunction{
		Name:       "strict_fn",
		Parameters: []FunctionParameter{{Name: "a", Type: "INTEGER"}},
		ReturnType: "INTEGER",
		Language:   "sql",
		Body:       "SELECT $1 * 2",
		Strict:     true,
	}

	err := pm.CreateFunction(context.Background(), fn)
	require.NoError(t, err)

	// With STRICT, NULL input returns NULL without executing
	result, err := pm.ExecuteFunction(
		context.Background(),
		conn,
		"public",
		"strict_fn",
		[]interface{}{nil},
	)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestPLpgSQLManager_Disable(t *testing.T) {
	pm := NewPLpgSQLManager()
	assert.True(t, pm.IsPLpgSQLSupported())

	pm.Disable()
	assert.False(t, pm.IsPLpgSQLSupported())

	fn := &StoredFunction{Name: "test", Body: "BEGIN END;"}
	err := pm.CreateFunction(context.Background(), fn)
	assert.Equal(t, ErrPLpgSQLNotSupported, err)

	pm.Enable()
	assert.True(t, pm.IsPLpgSQLSupported())

	err = pm.CreateFunction(context.Background(), fn)
	assert.NoError(t, err)
}

func TestPLpgSQLManager_Trigger(t *testing.T) {
	pm := NewPLpgSQLManager()

	trigger := &Trigger{
		Name:     "audit_trigger",
		Table:    "users",
		Timing:   TriggerAfter,
		Events:   TriggerOnInsert | TriggerOnUpdate,
		Level:    TriggerForEachRow,
		Function: "audit_function",
		Enabled:  true,
	}

	err := pm.CreateTrigger(context.Background(), trigger)
	require.NoError(t, err)

	// Get trigger
	stored, ok := pm.GetTrigger("public", "users", "audit_trigger")
	assert.True(t, ok)
	assert.Equal(t, "audit_trigger", stored.Name)
	assert.True(t, stored.Events&TriggerOnInsert != 0)

	// Get triggers for table
	triggers := pm.GetTriggersForTable("public", "users")
	assert.Len(t, triggers, 1)

	// Drop trigger
	err = pm.DropTrigger(context.Background(), "public", "audit_trigger", "users", false)
	require.NoError(t, err)

	_, ok = pm.GetTrigger("public", "users", "audit_trigger")
	assert.False(t, ok)
}

func TestParseParameters(t *testing.T) {
	tests := []struct {
		input    string
		expected []FunctionParameter
	}{
		{
			input:    "",
			expected: nil,
		},
		{
			input:    "a INTEGER",
			expected: []FunctionParameter{{Name: "a", Type: "INTEGER", Mode: ModeIn}},
		},
		{
			input:    "IN a INTEGER",
			expected: []FunctionParameter{{Name: "a", Type: "INTEGER", Mode: ModeIn}},
		},
		{
			input:    "OUT result INTEGER",
			expected: []FunctionParameter{{Name: "result", Type: "INTEGER", Mode: ModeOut}},
		},
		{
			input:    "INOUT x INTEGER",
			expected: []FunctionParameter{{Name: "x", Type: "INTEGER", Mode: ModeInOut}},
		},
		{
			input: "a INTEGER, b INTEGER",
			expected: []FunctionParameter{
				{Name: "a", Type: "INTEGER", Mode: ModeIn},
				{Name: "b", Type: "INTEGER", Mode: ModeIn},
			},
		},
		{
			input: "a INTEGER DEFAULT 0",
			expected: []FunctionParameter{
				{Name: "a", Type: "INTEGER", Mode: ModeIn, DefaultValue: "0"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseParameters(tt.input)
			if tt.expected == nil {
				assert.Nil(t, result)
				return
			}
			require.Len(t, result, len(tt.expected))
			for i, exp := range tt.expected {
				assert.Equal(t, exp.Name, result[i].Name)
				assert.Equal(t, exp.Type, result[i].Type)
				assert.Equal(t, exp.Mode, result[i].Mode)
				if exp.DefaultValue != nil {
					assert.Equal(t, exp.DefaultValue, result[i].DefaultValue)
				}
			}
		})
	}
}
