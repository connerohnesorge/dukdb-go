package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseStandaloneValues(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantRows int
		wantCols int
	}{
		{
			name:     "single row single column",
			sql:      "VALUES (1)",
			wantRows: 1,
			wantCols: 1,
		},
		{
			name:     "single row multiple columns",
			sql:      "VALUES (1, 'hello', true)",
			wantRows: 1,
			wantCols: 3,
		},
		{
			name:     "multiple rows",
			sql:      "VALUES (1, 'a'), (2, 'b'), (3, 'c')",
			wantRows: 3,
			wantCols: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			require.NoError(t, err)

			sel, ok := stmt.(*SelectStmt)
			require.True(t, ok, "standalone VALUES should produce a SelectStmt")

			// Standalone VALUES wraps as SELECT * FROM (VALUES ...) AS valueslist
			require.NotNil(t, sel.From)
			require.Len(t, sel.From.Tables, 1)

			ref := sel.From.Tables[0]
			require.NotNil(t, ref.ValuesRef, "should have ValuesRef set")
			assert.Equal(t, "valueslist", ref.Alias)
			assert.Len(t, ref.ValuesRef.Rows, tt.wantRows)

			for i, row := range ref.ValuesRef.Rows {
				assert.Len(t, row, tt.wantCols, "row %d should have %d columns", i, tt.wantCols)
			}
		})
	}
}

func TestParseValuesInFrom(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantRows int
		wantCols int
	}{
		{
			name:     "VALUES in FROM clause",
			sql:      "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t",
			wantRows: 2,
			wantCols: 2,
		},
		{
			name:     "bare VALUES in FROM clause",
			sql:      "SELECT * FROM VALUES (10, 20) AS t",
			wantRows: 1,
			wantCols: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			require.NoError(t, err)

			sel, ok := stmt.(*SelectStmt)
			require.True(t, ok)

			require.NotNil(t, sel.From)
			require.Len(t, sel.From.Tables, 1)

			ref := sel.From.Tables[0]
			require.NotNil(t, ref.ValuesRef, "should have ValuesRef set")
			assert.Len(t, ref.ValuesRef.Rows, tt.wantRows)
			for _, row := range ref.ValuesRef.Rows {
				assert.Len(t, row, tt.wantCols)
			}
		})
	}
}

func TestParseValuesRowCountMismatch(t *testing.T) {
	_, err := Parse("VALUES (1, 2), (3)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "different numbers of columns")
}

func TestParseValuesWithSemicolon(t *testing.T) {
	stmt, err := Parse("VALUES (1, 2);")
	require.NoError(t, err)
	require.NotNil(t, stmt)
}
