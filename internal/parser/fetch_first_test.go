package parser

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFetchFirst tests parsing of SQL standard FETCH FIRST syntax.
func TestFetchFirst(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		wantErr       bool
		errContains   string
		wantLimit     int64
		wantWithTies  bool
		wantOffset    int64
		hasOffset     bool
	}{
		{
			name:      "FETCH FIRST 5 ROWS ONLY",
			sql:       "SELECT * FROM t FETCH FIRST 5 ROWS ONLY",
			wantLimit: 5,
		},
		{
			name:      "FETCH FIRST 1 ROW ONLY",
			sql:       "SELECT * FROM t FETCH FIRST 1 ROW ONLY",
			wantLimit: 1,
		},
		{
			name:      "FETCH NEXT 10 ROWS ONLY",
			sql:       "SELECT * FROM t FETCH NEXT 10 ROWS ONLY",
			wantLimit: 10,
		},
		{
			name:      "FETCH FIRST ROW ONLY (no count defaults to 1)",
			sql:       "SELECT * FROM t FETCH FIRST ROW ONLY",
			wantLimit: 1,
		},
		{
			name:      "FETCH NEXT ROW ONLY (no count defaults to 1)",
			sql:       "SELECT * FROM t FETCH NEXT ROW ONLY",
			wantLimit: 1,
		},
		{
			name:         "FETCH FIRST 3 ROWS WITH TIES",
			sql:          "SELECT * FROM t ORDER BY x FETCH FIRST 3 ROWS WITH TIES",
			wantLimit:    3,
			wantWithTies: true,
		},
		{
			name:         "FETCH NEXT 5 ROWS WITH TIES",
			sql:          "SELECT * FROM t ORDER BY x FETCH NEXT 5 ROWS WITH TIES",
			wantLimit:    5,
			wantWithTies: true,
		},
		{
			name:      "OFFSET with FETCH FIRST",
			sql:       "SELECT * FROM t OFFSET 10 FETCH FIRST 5 ROWS ONLY",
			wantLimit: 5,
			hasOffset: true,
			wantOffset: 10,
		},
		{
			name:      "SQL standard OFFSET N ROWS with FETCH FIRST",
			sql:       "SELECT * FROM t OFFSET 10 ROWS FETCH FIRST 5 ROWS ONLY",
			wantLimit: 5,
			hasOffset: true,
			wantOffset: 10,
		},
		{
			name:      "SQL standard OFFSET N ROW with FETCH FIRST",
			sql:       "SELECT * FROM t OFFSET 3 ROW FETCH FIRST 1 ROW ONLY",
			wantLimit: 1,
			hasOffset: true,
			wantOffset: 3,
		},
		{
			name:        "error: LIMIT and FETCH FIRST both specified",
			sql:         "SELECT * FROM t LIMIT 5 FETCH FIRST 3 ROWS ONLY",
			wantErr:     true,
			errContains: "cannot use both LIMIT and FETCH FIRST",
		},
		{
			name:        "error: FETCH without FIRST or NEXT",
			sql:         "SELECT * FROM t FETCH 5 ROWS ONLY",
			wantErr:     true,
			errContains: "expected FIRST or NEXT after FETCH",
		},
		{
			name:        "error: FETCH FIRST without ROW/ROWS",
			sql:         "SELECT * FROM t FETCH FIRST 5 ONLY",
			wantErr:     true,
			errContains: "expected ROW or ROWS",
		},
		{
			name:        "error: FETCH FIRST without ONLY or WITH TIES",
			sql:         "SELECT * FROM t FETCH FIRST 5 ROWS",
			wantErr:     true,
			errContains: "expected ONLY or WITH TIES",
		},
		{
			name:      "FETCH FIRST with ORDER BY",
			sql:       "SELECT * FROM t ORDER BY x DESC FETCH FIRST 3 ROWS ONLY",
			wantLimit: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}
			require.NoError(t, err)

			sel, ok := stmt.(*SelectStmt)
			require.True(t, ok, "expected SelectStmt")

			// Check limit
			require.NotNil(t, sel.Limit, "expected Limit to be set")
			lit, ok := sel.Limit.(*Literal)
			require.True(t, ok, "expected Limit to be a Literal")
			assert.Equal(t, tt.wantLimit, lit.Value)
			assert.Equal(t, dukdb.TYPE_BIGINT, lit.Type)

			// Check WithTies
			assert.Equal(t, tt.wantWithTies, sel.FetchWithTies)

			// Check offset
			if tt.hasOffset {
				require.NotNil(t, sel.Offset, "expected Offset to be set")
				offLit, ok := sel.Offset.(*Literal)
				require.True(t, ok, "expected Offset to be a Literal")
				assert.Equal(t, tt.wantOffset, offLit.Value)
			}
		})
	}
}

// TestFetchFirstEquivalentToLimit verifies that FETCH FIRST produces the same AST as LIMIT.
func TestFetchFirstEquivalentToLimit(t *testing.T) {
	// Parse LIMIT 5
	limitStmt, err := Parse("SELECT * FROM t LIMIT 5")
	require.NoError(t, err)
	limitSel := limitStmt.(*SelectStmt)

	// Parse FETCH FIRST 5 ROWS ONLY
	fetchStmt, err := Parse("SELECT * FROM t FETCH FIRST 5 ROWS ONLY")
	require.NoError(t, err)
	fetchSel := fetchStmt.(*SelectStmt)

	// Both should have the same limit value
	limitLit := limitSel.Limit.(*Literal)
	fetchLit := fetchSel.Limit.(*Literal)
	assert.Equal(t, limitLit.Value, fetchLit.Value)
	assert.False(t, fetchSel.FetchWithTies)
}

// TestOffsetRowSyntax tests SQL standard OFFSET N ROW/ROWS syntax.
func TestOffsetRowSyntax(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantOffset int64
	}{
		{
			name:       "OFFSET with ROW keyword",
			sql:        "SELECT * FROM t OFFSET 1 ROW",
			wantOffset: 1,
		},
		{
			name:       "OFFSET with ROWS keyword",
			sql:        "SELECT * FROM t OFFSET 5 ROWS",
			wantOffset: 5,
		},
		{
			name:       "OFFSET without ROW/ROWS (existing syntax)",
			sql:        "SELECT * FROM t OFFSET 3",
			wantOffset: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			require.NoError(t, err)

			sel, ok := stmt.(*SelectStmt)
			require.True(t, ok)

			require.NotNil(t, sel.Offset)
			lit, ok := sel.Offset.(*Literal)
			require.True(t, ok)
			assert.Equal(t, tt.wantOffset, lit.Value)
		})
	}
}
