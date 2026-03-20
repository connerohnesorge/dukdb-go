package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dukdb "github.com/dukdb/dukdb-go"
)

func TestParsePrepare(t *testing.T) {
	stmt, err := Parse("PREPARE myplan AS SELECT * FROM t WHERE id = $1")
	require.NoError(t, err)
	ps, ok := stmt.(*PrepareStmt)
	require.True(t, ok)
	assert.Equal(t, "myplan", ps.Name)
	assert.Equal(t, dukdb.STATEMENT_TYPE_PREPARE, ps.Type())

	inner, ok := ps.Inner.(*SelectStmt)
	require.True(t, ok)
	assert.NotNil(t, inner)
}

func TestParsePrepareInsert(t *testing.T) {
	stmt, err := Parse("PREPARE ins AS INSERT INTO t VALUES ($1, $2)")
	require.NoError(t, err)
	ps, ok := stmt.(*PrepareStmt)
	require.True(t, ok)
	assert.Equal(t, "ins", ps.Name)

	inner, ok := ps.Inner.(*InsertStmt)
	require.True(t, ok)
	assert.Equal(t, "t", inner.Table)
}

func TestParseExecuteNoParams(t *testing.T) {
	stmt, err := Parse("EXECUTE myplan")
	require.NoError(t, err)
	es, ok := stmt.(*ExecuteStmt)
	require.True(t, ok)
	assert.Equal(t, "myplan", es.Name)
	assert.Len(t, es.Params, 0)
	assert.Equal(t, dukdb.STATEMENT_TYPE_EXECUTE, es.Type())
}

func TestParseExecuteWithParams(t *testing.T) {
	stmt, err := Parse("EXECUTE myplan(1, 'hello', 3.14)")
	require.NoError(t, err)
	es, ok := stmt.(*ExecuteStmt)
	require.True(t, ok)
	assert.Equal(t, "myplan", es.Name)
	assert.Len(t, es.Params, 3)
}

func TestParseDeallocateName(t *testing.T) {
	stmt, err := Parse("DEALLOCATE myplan")
	require.NoError(t, err)
	ds, ok := stmt.(*DeallocateStmt)
	require.True(t, ok)
	assert.Equal(t, "myplan", ds.Name)
	assert.False(t, ds.All)
	assert.Equal(t, dukdb.STATEMENT_TYPE_DEALLOCATE, ds.Type())
}

func TestParseDeallocatePrepareName(t *testing.T) {
	stmt, err := Parse("DEALLOCATE PREPARE myplan")
	require.NoError(t, err)
	ds, ok := stmt.(*DeallocateStmt)
	require.True(t, ok)
	assert.Equal(t, "myplan", ds.Name)
	assert.False(t, ds.All)
}

func TestParseDeallocateAll(t *testing.T) {
	stmt, err := Parse("DEALLOCATE ALL")
	require.NoError(t, err)
	ds, ok := stmt.(*DeallocateStmt)
	require.True(t, ok)
	assert.True(t, ds.All)
	assert.Empty(t, ds.Name)
}

func TestParseDeallocatePrepareAll(t *testing.T) {
	stmt, err := Parse("DEALLOCATE PREPARE ALL")
	require.NoError(t, err)
	ds, ok := stmt.(*DeallocateStmt)
	require.True(t, ok)
	assert.True(t, ds.All)
}

func TestParsePrepareMissingName(t *testing.T) {
	_, err := Parse("PREPARE AS SELECT 1")
	require.Error(t, err)
}

func TestParsePrepareMissingAS(t *testing.T) {
	_, err := Parse("PREPARE myplan SELECT 1")
	require.Error(t, err)
}

func TestCountParametersPrepare(t *testing.T) {
	stmt, err := Parse("PREPARE q AS SELECT * FROM t WHERE id = $1 AND name = $2")
	require.NoError(t, err)
	count := CountParameters(stmt)
	assert.Equal(t, 2, count)
}

func TestCountParametersExecute(t *testing.T) {
	stmt, err := Parse("EXECUTE q(1, 2, 3)")
	require.NoError(t, err)
	// EXECUTE params are literal expressions, not $N placeholders
	count := CountParameters(stmt)
	assert.Equal(t, 0, count)
}
