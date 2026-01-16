// Package parser contains PostgreSQL compatibility tests for the dukdb-go parser.
// These tests verify that PostgreSQL-specific syntax extensions are properly parsed.
package parser

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// PostgreSQL Compatibility Test Suite
//
// This file consolidates tests for PostgreSQL-specific syntax features:
// - DISTINCT ON (col1, col2, ...)
// - LIMIT ALL
// - :: type cast operator
// - ILIKE / NOT ILIKE operators
// - GROUP BY ordinal (GROUP BY 1, 2)
// - WITH RECURSIVE
// =============================================================================

// -----------------------------------------------------------------------------
// DISTINCT ON Tests
// -----------------------------------------------------------------------------

func TestPostgresCompatDistinctOn(t *testing.T) {
	tests := []struct {
		name           string
		sql            string
		wantErr        bool
		wantDistinctOn int // number of DISTINCT ON expressions expected
	}{
		{
			name:           "simple DISTINCT ON single column",
			sql:            "SELECT DISTINCT ON (category) * FROM products",
			wantErr:        false,
			wantDistinctOn: 1,
		},
		{
			name:           "DISTINCT ON multiple columns",
			sql:            "SELECT DISTINCT ON (category, brand) name, price FROM products",
			wantErr:        false,
			wantDistinctOn: 2,
		},
		{
			name:           "DISTINCT ON with ORDER BY matching first column",
			sql:            "SELECT DISTINCT ON (category) name, price FROM products ORDER BY category, price DESC",
			wantErr:        false,
			wantDistinctOn: 1,
		},
		{
			name:           "DISTINCT ON with complex expression",
			sql:            "SELECT DISTINCT ON (LOWER(name)) * FROM users",
			wantErr:        false,
			wantDistinctOn: 1,
		},
		{
			name:           "DISTINCT ON with table-qualified column",
			sql:            "SELECT DISTINCT ON (p.category) p.* FROM products p",
			wantErr:        false,
			wantDistinctOn: 1,
		},
		{
			name:           "DISTINCT ON without parentheses fails",
			sql:            "SELECT DISTINCT ON category * FROM products",
			wantErr:        true,
			wantDistinctOn: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)

			selectStmt, ok := stmt.(*SelectStmt)
			require.True(t, ok, "expected SelectStmt")
			assert.Len(t, selectStmt.DistinctOn, tt.wantDistinctOn)
		})
	}
}

func TestPostgresCompatDistinctOnWithOrderBy(t *testing.T) {
	// PostgreSQL semantics: DISTINCT ON expressions should match leading ORDER BY columns.
	// This test verifies the AST structure, not semantic validation.

	t.Run("DISTINCT ON with matching ORDER BY", func(t *testing.T) {
		sql := "SELECT DISTINCT ON (department) employee_name, salary FROM employees ORDER BY department, salary DESC"
		stmt, err := Parse(sql)
		require.NoError(t, err)

		selectStmt := stmt.(*SelectStmt)
		require.Len(t, selectStmt.DistinctOn, 1)
		require.Len(t, selectStmt.OrderBy, 2)

		// Verify DISTINCT ON column
		distinctCol, ok := selectStmt.DistinctOn[0].(*ColumnRef)
		require.True(t, ok)
		assert.Equal(t, "department", distinctCol.Column)

		// Verify ORDER BY first column matches
		orderCol, ok := selectStmt.OrderBy[0].Expr.(*ColumnRef)
		require.True(t, ok)
		assert.Equal(t, "department", orderCol.Column)
	})

	t.Run("DISTINCT ON with multiple columns and ORDER BY", func(t *testing.T) {
		sql := "SELECT DISTINCT ON (region, category) product_name FROM products ORDER BY region, category, price"
		stmt, err := Parse(sql)
		require.NoError(t, err)

		selectStmt := stmt.(*SelectStmt)
		assert.Len(t, selectStmt.DistinctOn, 2)
		assert.Len(t, selectStmt.OrderBy, 3)
	})
}

// -----------------------------------------------------------------------------
// LIMIT ALL Tests
// -----------------------------------------------------------------------------

func TestPostgresCompatLimitAll(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantErr    bool
		wantLimit  bool  // whether Limit should be non-nil
		wantOffset bool  // whether Offset should be non-nil
		offsetVal  int64 // expected offset value
	}{
		{
			name:       "simple LIMIT ALL",
			sql:        "SELECT * FROM t LIMIT ALL",
			wantErr:    false,
			wantLimit:  false,
			wantOffset: false,
		},
		{
			name:       "LIMIT ALL with OFFSET",
			sql:        "SELECT * FROM t LIMIT ALL OFFSET 100",
			wantErr:    false,
			wantLimit:  false,
			wantOffset: true,
			offsetVal:  100,
		},
		{
			name:       "LIMIT ALL in complex query with WHERE",
			sql:        "SELECT id, name FROM users WHERE active = true LIMIT ALL",
			wantErr:    false,
			wantLimit:  false,
			wantOffset: false,
		},
		{
			name:       "LIMIT ALL with ORDER BY and OFFSET",
			sql:        "SELECT * FROM products ORDER BY price DESC LIMIT ALL OFFSET 50",
			wantErr:    false,
			wantLimit:  false,
			wantOffset: true,
			offsetVal:  50,
		},
		{
			name:       "LIMIT ALL in subquery",
			sql:        "SELECT * FROM (SELECT * FROM t LIMIT ALL) AS sub",
			wantErr:    false,
			wantLimit:  false,
			wantOffset: false,
		},
		{
			name:       "LIMIT ALL with GROUP BY and HAVING",
			sql:        "SELECT category, COUNT(*) FROM products GROUP BY category HAVING COUNT(*) > 5 LIMIT ALL",
			wantErr:    false,
			wantLimit:  false,
			wantOffset: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)

			// For subquery tests, check the outer SelectStmt
			selectStmt, ok := stmt.(*SelectStmt)
			require.True(t, ok, "expected SelectStmt")

			// For "LIMIT ALL in subquery", check the outer query which has no limit
			if tt.name == "LIMIT ALL in subquery" {
				// Outer query has no LIMIT clause
				assert.Nil(t, selectStmt.Limit)
				// Check the subquery
				require.NotNil(t, selectStmt.From)
				require.Len(t, selectStmt.From.Tables, 1)
				subquery := selectStmt.From.Tables[0].Subquery
				require.NotNil(t, subquery)
				assert.Nil(t, subquery.Limit, "subquery should have nil Limit for LIMIT ALL")
				return
			}

			if tt.wantLimit {
				assert.NotNil(t, selectStmt.Limit)
			} else {
				assert.Nil(t, selectStmt.Limit, "LIMIT ALL should result in nil Limit")
			}

			if tt.wantOffset {
				require.NotNil(t, selectStmt.Offset)
				offsetLit, ok := selectStmt.Offset.(*Literal)
				require.True(t, ok)
				assert.Equal(t, tt.offsetVal, offsetLit.Value)
			} else {
				assert.Nil(t, selectStmt.Offset)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// :: Type Cast Tests
// -----------------------------------------------------------------------------

func TestPostgresCompatTypeCast(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantErr    bool
		targetType dukdb.Type
	}{
		// Basic casts
		{
			name:       "string to integer",
			sql:        "SELECT '42'::integer",
			wantErr:    false,
			targetType: dukdb.TYPE_INTEGER,
		},
		{
			name:       "string to bigint",
			sql:        "SELECT '9999999999'::bigint",
			wantErr:    false,
			targetType: dukdb.TYPE_BIGINT,
		},
		{
			name:       "string to text",
			sql:        "SELECT 123::text",
			wantErr:    false,
			targetType: dukdb.TYPE_VARCHAR,
		},
		// Parameterized types
		{
			name:       "varchar with length",
			sql:        "SELECT 'hello world'::varchar(100)",
			wantErr:    false,
			targetType: dukdb.TYPE_VARCHAR,
		},
		{
			name:       "numeric with precision and scale",
			sql:        "SELECT 123.456::numeric(10,3)",
			wantErr:    false,
			targetType: dukdb.TYPE_DECIMAL,
		},
		// Chained casts
		{
			name:       "chained cast text to integer",
			sql:        "SELECT '42'::text::integer",
			wantErr:    false,
			targetType: dukdb.TYPE_INTEGER,
		},
		{
			name:       "triple chained cast",
			sql:        "SELECT 42::text::varchar::text",
			wantErr:    false,
			targetType: dukdb.TYPE_VARCHAR,
		},
		// Cast in expressions
		{
			name:       "cast in arithmetic",
			sql:        "SELECT '10'::integer + '20'::integer",
			wantErr:    false,
			targetType: dukdb.TYPE_INTEGER,
		},
		{
			name:       "cast in comparison",
			sql:        "SELECT * FROM t WHERE col::integer > 100",
			wantErr:    false,
			targetType: dukdb.TYPE_INTEGER,
		},
		{
			name:       "cast of function result",
			sql:        "SELECT COALESCE(value, 0)::text FROM t",
			wantErr:    false,
			targetType: dukdb.TYPE_VARCHAR,
		},
		{
			name:       "cast in CASE expression",
			sql:        "SELECT CASE WHEN x > 0 THEN x::text ELSE '0' END FROM t",
			wantErr:    false,
			targetType: dukdb.TYPE_VARCHAR,
		},
		// Date/time casts
		{
			name:       "string to date",
			sql:        "SELECT '2024-01-15'::date",
			wantErr:    false,
			targetType: dukdb.TYPE_DATE,
		},
		{
			name:       "string to timestamp",
			sql:        "SELECT '2024-01-15 10:30:00'::timestamp",
			wantErr:    false,
			targetType: dukdb.TYPE_TIMESTAMP,
		},
		// Error cases
		{
			name:    "missing type after ::",
			sql:     "SELECT 'test'::",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)

			selectStmt, ok := stmt.(*SelectStmt)
			require.True(t, ok, "expected SelectStmt")

			// Verify that a CastExpr with the expected type exists in the AST
			found := findCastExprWithType(selectStmt, tt.targetType)
			assert.True(t, found, "expected CastExpr with type %v", tt.targetType)
		})
	}
}

func TestPostgresCompatTypeCastChaining(t *testing.T) {
	t.Run("verify chained cast structure", func(t *testing.T) {
		sql := "SELECT '123'::text::integer"
		stmt, err := Parse(sql)
		require.NoError(t, err)

		selectStmt := stmt.(*SelectStmt)
		require.Len(t, selectStmt.Columns, 1)

		// The expression should be a CastExpr wrapping another CastExpr
		outerCast, ok := selectStmt.Columns[0].Expr.(*CastExpr)
		require.True(t, ok, "expected outer CastExpr")
		assert.Equal(t, dukdb.TYPE_INTEGER, outerCast.TargetType)

		innerCast, ok := outerCast.Expr.(*CastExpr)
		require.True(t, ok, "expected inner CastExpr")
		assert.Equal(t, dukdb.TYPE_VARCHAR, innerCast.TargetType) // text maps to VARCHAR
	})

	t.Run("cast with complex expression", func(t *testing.T) {
		sql := "SELECT (a + b)::varchar(50) FROM t"
		stmt, err := Parse(sql)
		require.NoError(t, err)

		selectStmt := stmt.(*SelectStmt)
		require.Len(t, selectStmt.Columns, 1)

		castExpr, ok := selectStmt.Columns[0].Expr.(*CastExpr)
		require.True(t, ok, "expected CastExpr")
		assert.Equal(t, dukdb.TYPE_VARCHAR, castExpr.TargetType)

		// Inner expression should be a binary expression
		_, ok = castExpr.Expr.(*BinaryExpr)
		assert.True(t, ok, "expected BinaryExpr inside cast")
	})
}

// -----------------------------------------------------------------------------
// ILIKE Tests
// -----------------------------------------------------------------------------

func TestPostgresCompatILike(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantErr     bool
		wantOp      BinaryOp
		wantPattern string
	}{
		{
			name:        "simple ILIKE",
			sql:         "SELECT * FROM users WHERE name ILIKE '%john%'",
			wantErr:     false,
			wantOp:      OpILike,
			wantPattern: "%john%",
		},
		{
			name:        "ILIKE with prefix pattern",
			sql:         "SELECT * FROM files WHERE path ILIKE '/home/%'",
			wantErr:     false,
			wantOp:      OpILike,
			wantPattern: "/home/%",
		},
		{
			name:        "ILIKE with suffix pattern",
			sql:         "SELECT * FROM emails WHERE address ILIKE '%@EXAMPLE.COM'",
			wantErr:     false,
			wantOp:      OpILike,
			wantPattern: "%@EXAMPLE.COM",
		},
		{
			name:        "NOT ILIKE",
			sql:         "SELECT * FROM products WHERE name NOT ILIKE '%test%'",
			wantErr:     false,
			wantOp:      OpNotILike,
			wantPattern: "%test%",
		},
		{
			name:    "ILIKE with underscore wildcard",
			sql:     "SELECT * FROM codes WHERE code ILIKE 'A_C'",
			wantErr: false,
			wantOp:  OpILike,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)

			selectStmt, ok := stmt.(*SelectStmt)
			require.True(t, ok, "expected SelectStmt")

			binaryExpr, ok := selectStmt.Where.(*BinaryExpr)
			require.True(t, ok, "expected BinaryExpr in WHERE")
			assert.Equal(t, tt.wantOp, binaryExpr.Op)

			if tt.wantPattern != "" {
				lit, ok := binaryExpr.Right.(*Literal)
				require.True(t, ok, "expected Literal on right side")
				assert.Equal(t, tt.wantPattern, lit.Value)
			}
		})
	}
}

func TestPostgresCompatILikeCombined(t *testing.T) {
	t.Run("ILIKE in complex WHERE with AND/OR", func(t *testing.T) {
		sql := "SELECT * FROM t WHERE (name ILIKE 'a%' OR name ILIKE 'b%') AND active = true"
		stmt, err := Parse(sql)
		require.NoError(t, err)

		selectStmt := stmt.(*SelectStmt)
		require.NotNil(t, selectStmt.Where)

		// Top level should be AND
		andExpr, ok := selectStmt.Where.(*BinaryExpr)
		require.True(t, ok)
		assert.Equal(t, OpAnd, andExpr.Op)
	})

	t.Run("ILIKE with type cast", func(t *testing.T) {
		sql := "SELECT * FROM t WHERE col::text ILIKE '%pattern%'"
		stmt, err := Parse(sql)
		require.NoError(t, err)

		selectStmt := stmt.(*SelectStmt)
		binaryExpr, ok := selectStmt.Where.(*BinaryExpr)
		require.True(t, ok)
		assert.Equal(t, OpILike, binaryExpr.Op)

		// Left side should be a cast expression
		castExpr, ok := binaryExpr.Left.(*CastExpr)
		require.True(t, ok, "expected CastExpr on left side")
		assert.Equal(t, dukdb.TYPE_VARCHAR, castExpr.TargetType)
	})
}

// -----------------------------------------------------------------------------
// GROUP BY Ordinal Tests
// -----------------------------------------------------------------------------

func TestPostgresCompatGroupByOrdinal(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		wantN   int // number of GROUP BY expressions
	}{
		{
			name:    "GROUP BY 1",
			sql:     "SELECT category, COUNT(*) FROM products GROUP BY 1",
			wantErr: false,
			wantN:   1,
		},
		{
			name:    "GROUP BY 1, 2",
			sql:     "SELECT category, brand, SUM(price) FROM products GROUP BY 1, 2",
			wantErr: false,
			wantN:   2,
		},
		{
			name:    "GROUP BY mixed ordinal and column name",
			sql:     "SELECT category, brand, SUM(price) FROM products GROUP BY category, 2",
			wantErr: false,
			wantN:   2,
		},
		{
			name:    "GROUP BY 1 with HAVING",
			sql:     "SELECT category, COUNT(*) AS cnt FROM products GROUP BY 1 HAVING COUNT(*) > 10",
			wantErr: false,
			wantN:   1,
		},
		{
			name:    "GROUP BY 1, 2 with ORDER BY",
			sql:     "SELECT region, year, SUM(sales) FROM data GROUP BY 1, 2 ORDER BY 3 DESC",
			wantErr: false,
			wantN:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)

			selectStmt, ok := stmt.(*SelectStmt)
			require.True(t, ok, "expected SelectStmt")
			assert.Len(t, selectStmt.GroupBy, tt.wantN)
		})
	}
}

func TestPostgresCompatGroupByOrdinalWithHaving(t *testing.T) {
	t.Run("GROUP BY ordinal with complex HAVING", func(t *testing.T) {
		sql := "SELECT department, job_title, AVG(salary) FROM employees GROUP BY 1, 2 HAVING AVG(salary) > 50000 AND COUNT(*) >= 5"
		stmt, err := Parse(sql)
		require.NoError(t, err)

		selectStmt := stmt.(*SelectStmt)
		assert.Len(t, selectStmt.GroupBy, 2)
		assert.NotNil(t, selectStmt.Having)

		// HAVING should be an AND expression
		andExpr, ok := selectStmt.Having.(*BinaryExpr)
		require.True(t, ok)
		assert.Equal(t, OpAnd, andExpr.Op)
	})
}

// -----------------------------------------------------------------------------
// WITH RECURSIVE Tests
// -----------------------------------------------------------------------------

func TestPostgresCompatWithRecursive(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		wantErr       bool
		wantRecursive bool
		wantCTECount  int
	}{
		{
			name:          "simple WITH RECURSIVE",
			sql:           "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 10) SELECT * FROM cnt",
			wantErr:       false,
			wantRecursive: true,
			wantCTECount:  1,
		},
		{
			name:          "WITH RECURSIVE for tree traversal",
			sql:           "WITH RECURSIVE tree(id, parent_id, depth) AS (SELECT id, parent_id, 0 FROM nodes WHERE parent_id IS NULL UNION ALL SELECT n.id, n.parent_id, t.depth + 1 FROM nodes n JOIN tree t ON n.parent_id = t.id) SELECT * FROM tree",
			wantErr:       false,
			wantRecursive: true,
			wantCTECount:  1,
		},
		{
			name:          "non-recursive WITH",
			sql:           "WITH tmp AS (SELECT 1 AS x) SELECT * FROM tmp",
			wantErr:       false,
			wantRecursive: false,
			wantCTECount:  1,
		},
		{
			name:          "WITH RECURSIVE with multiple CTEs",
			sql:           "WITH RECURSIVE r1(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r1 WHERE n < 5), r2(m) AS (SELECT n * 2 FROM r1) SELECT * FROM r2",
			wantErr:       false,
			wantRecursive: true,
			wantCTECount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)

			selectStmt, ok := stmt.(*SelectStmt)
			require.True(t, ok, "expected SelectStmt")
			assert.Len(t, selectStmt.CTEs, tt.wantCTECount)

			if tt.wantCTECount > 0 {
				// Check recursive flag on first CTE
				assert.Equal(t, tt.wantRecursive, selectStmt.CTEs[0].Recursive)
			}
		})
	}
}

func TestPostgresCompatWithRecursiveStructure(t *testing.T) {
	t.Run("verify CTE structure", func(t *testing.T) {
		sql := "WITH RECURSIVE factorial(n, f) AS (SELECT 1, 1 UNION ALL SELECT n+1, (n+1)*f FROM factorial WHERE n < 10) SELECT * FROM factorial"
		stmt, err := Parse(sql)
		require.NoError(t, err)

		selectStmt := stmt.(*SelectStmt)
		require.Len(t, selectStmt.CTEs, 1)

		cte := selectStmt.CTEs[0]
		assert.Equal(t, "factorial", cte.Name)
		assert.True(t, cte.Recursive)
		assert.Equal(t, []string{"n", "f"}, cte.Columns)
		assert.NotNil(t, cte.Query)
	})
}

// -----------------------------------------------------------------------------
// Integration Tests - Multiple PostgreSQL Features Combined
// -----------------------------------------------------------------------------

func TestPostgresCompatIntegration(t *testing.T) {
	t.Run("DISTINCT ON with LIMIT ALL", func(t *testing.T) {
		sql := "SELECT DISTINCT ON (category) id, name, price FROM products ORDER BY category, price DESC LIMIT ALL"
		stmt, err := Parse(sql)
		require.NoError(t, err)

		selectStmt := stmt.(*SelectStmt)
		assert.Len(t, selectStmt.DistinctOn, 1)
		assert.Nil(t, selectStmt.Limit, "LIMIT ALL should be nil")
	})

	t.Run("type cast with ILIKE and GROUP BY ordinal", func(t *testing.T) {
		sql := "SELECT category::text, COUNT(*) FROM products WHERE name ILIKE '%widget%' GROUP BY 1"
		stmt, err := Parse(sql)
		require.NoError(t, err)

		selectStmt := stmt.(*SelectStmt)
		// Verify cast in SELECT
		castExpr, ok := selectStmt.Columns[0].Expr.(*CastExpr)
		require.True(t, ok)
		assert.Equal(t, dukdb.TYPE_VARCHAR, castExpr.TargetType)

		// Verify ILIKE in WHERE
		binaryExpr, ok := selectStmt.Where.(*BinaryExpr)
		require.True(t, ok)
		assert.Equal(t, OpILike, binaryExpr.Op)

		// Verify GROUP BY
		assert.Len(t, selectStmt.GroupBy, 1)
	})

	t.Run("WITH RECURSIVE with type cast", func(t *testing.T) {
		sql := "WITH RECURSIVE seq(n, s) AS (SELECT 1, '1'::text UNION ALL SELECT n+1, (n+1)::text FROM seq WHERE n < 5) SELECT * FROM seq"
		stmt, err := Parse(sql)
		require.NoError(t, err)

		selectStmt := stmt.(*SelectStmt)
		require.Len(t, selectStmt.CTEs, 1)
		assert.True(t, selectStmt.CTEs[0].Recursive)
	})

	t.Run("complex query with multiple features", func(t *testing.T) {
		sql := `
			WITH summary AS (
				SELECT
					category,
					brand::text AS brand_name,
					SUM(price)::numeric(10,2) AS total_price
				FROM products
				WHERE name ILIKE '%premium%'
				GROUP BY 1, 2
			)
			SELECT DISTINCT ON (category)
				category,
				brand_name,
				total_price
			FROM summary
			ORDER BY category, total_price DESC
			LIMIT ALL
		`
		stmt, err := Parse(sql)
		require.NoError(t, err)

		selectStmt := stmt.(*SelectStmt)
		assert.Len(t, selectStmt.CTEs, 1)
		assert.Len(t, selectStmt.DistinctOn, 1)
		assert.Nil(t, selectStmt.Limit)
	})
}

// -----------------------------------------------------------------------------
// Edge Cases and Error Handling
// -----------------------------------------------------------------------------

func TestPostgresCompatEdgeCases(t *testing.T) {
	t.Run("empty DISTINCT ON fails", func(t *testing.T) {
		sql := "SELECT DISTINCT ON () * FROM t"
		_, err := Parse(sql)
		assert.Error(t, err)
	})

	t.Run("LIMIT ALL OFFSET 0 is valid", func(t *testing.T) {
		sql := "SELECT * FROM t LIMIT ALL OFFSET 0"
		stmt, err := Parse(sql)
		require.NoError(t, err)

		selectStmt := stmt.(*SelectStmt)
		assert.Nil(t, selectStmt.Limit)
		require.NotNil(t, selectStmt.Offset)
		offsetLit := selectStmt.Offset.(*Literal)
		assert.Equal(t, int64(0), offsetLit.Value)
	})

	t.Run("case insensitive keywords", func(t *testing.T) {
		tests := []string{
			"SELECT * FROM t LIMIT all",
			"SELECT * FROM t limit ALL",
			"SELECT * FROM t LiMiT aLl",
		}
		for _, sql := range tests {
			stmt, err := Parse(sql)
			require.NoError(t, err, "SQL: %s", sql)
			selectStmt := stmt.(*SelectStmt)
			assert.Nil(t, selectStmt.Limit)
		}
	})

	t.Run("ILIKE is case insensitive keyword", func(t *testing.T) {
		tests := []string{
			"SELECT * FROM t WHERE x ilike 'a'",
			"SELECT * FROM t WHERE x ILIKE 'a'",
			"SELECT * FROM t WHERE x IlIkE 'a'",
		}
		for _, sql := range tests {
			stmt, err := Parse(sql)
			require.NoError(t, err, "SQL: %s", sql)
			selectStmt := stmt.(*SelectStmt)
			binaryExpr := selectStmt.Where.(*BinaryExpr)
			assert.Equal(t, OpILike, binaryExpr.Op)
		}
	})

	t.Run("nested type casts", func(t *testing.T) {
		sql := "SELECT ((('123'::text)::integer)::text)::bigint"
		stmt, err := Parse(sql)
		require.NoError(t, err)

		selectStmt := stmt.(*SelectStmt)
		// Should parse without error, outermost cast to bigint
		castExpr, ok := selectStmt.Columns[0].Expr.(*CastExpr)
		require.True(t, ok)
		assert.Equal(t, dukdb.TYPE_BIGINT, castExpr.TargetType)
	})
}

// -----------------------------------------------------------------------------
// INT64 Boundary Value Tests
// -----------------------------------------------------------------------------

func TestPostgresCompatInt64MinMax(t *testing.T) {
	// INT64 min = -9223372036854775808
	// INT64 max = 9223372036854775807
	// The tricky part: 9223372036854775808 > INT64 max, so parsing "-9223372036854775808"
	// as "-" followed by "9223372036854775808" (positive) then negating fails.
	// The parser must handle this by parsing the negative number as a whole.

	tests := []struct {
		name    string
		sql     string
		wantErr bool
		wantVal int64
	}{
		{
			name:    "INT64 min value in SELECT",
			sql:     "SELECT -9223372036854775808",
			wantErr: false,
			wantVal: -9223372036854775808,
		},
		{
			name:    "INT64 max value in SELECT",
			sql:     "SELECT 9223372036854775807",
			wantErr: false,
			wantVal: 9223372036854775807,
		},
		{
			name:    "INT64 min value in expression",
			sql:     "SELECT -9223372036854775808 + 1",
			wantErr: false,
			wantVal: 0, // Just check parsing works
		},
		{
			name:    "negative number near min",
			sql:     "SELECT -9223372036854775807",
			wantErr: false,
			wantVal: -9223372036854775807,
		},
		{
			name:    "INT64 overflow - value below min",
			sql:     "SELECT -9223372036854775809",
			wantErr: true,
			wantVal: 0,
		},
		{
			name:    "INT64 overflow - value above max",
			sql:     "SELECT 9223372036854775808",
			wantErr: true,
			wantVal: 0,
		},
		{
			name:    "INT64 min in comparison",
			sql:     "SELECT * FROM t WHERE x = -9223372036854775808",
			wantErr: false,
			wantVal: -9223372036854775808,
		},
		{
			name:    "INT64 min with parentheses",
			sql:     "SELECT (-9223372036854775808)",
			wantErr: false,
			wantVal: -9223372036854775808,
		},
		{
			name:    "negative float near INT64 min",
			sql:     "SELECT -9223372036854775808.0",
			wantErr: false,
			wantVal: 0, // Will be parsed as float, just check it works
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)

			selectStmt, ok := stmt.(*SelectStmt)
			require.True(t, ok, "expected SelectStmt")

			// For simple SELECT queries, verify the literal value
			if len(selectStmt.Columns) > 0 && tt.wantVal != 0 {
				// Check if the first column is directly a literal
				if lit, ok := selectStmt.Columns[0].Expr.(*Literal); ok {
					if intVal, ok := lit.Value.(int64); ok {
						assert.Equal(t, tt.wantVal, intVal)
					}
				}
			}
		})
	}
}

func TestPostgresCompatInt64MinInInsert(t *testing.T) {
	// Test INT64 min value in INSERT statement
	sql := "INSERT INTO t (x) VALUES (-9223372036854775808)"
	stmt, err := Parse(sql)
	require.NoError(t, err)

	insertStmt, ok := stmt.(*InsertStmt)
	require.True(t, ok, "expected InsertStmt")
	require.Len(t, insertStmt.Values, 1)
	require.Len(t, insertStmt.Values[0], 1)

	lit, ok := insertStmt.Values[0][0].(*Literal)
	require.True(t, ok, "expected Literal")
	assert.Equal(t, int64(-9223372036854775808), lit.Value)
}

func TestPostgresCompatInt64MinInUpdate(t *testing.T) {
	// Test INT64 min value in UPDATE statement
	sql := "UPDATE t SET x = -9223372036854775808 WHERE id = 1"
	stmt, err := Parse(sql)
	require.NoError(t, err)

	updateStmt, ok := stmt.(*UpdateStmt)
	require.True(t, ok, "expected UpdateStmt")
	require.Len(t, updateStmt.Set, 1)

	lit, ok := updateStmt.Set[0].Value.(*Literal)
	require.True(t, ok, "expected Literal")
	assert.Equal(t, int64(-9223372036854775808), lit.Value)
}

// -----------------------------------------------------------------------------
// Comprehensive PostgreSQL Syntax Table-Driven Test
// -----------------------------------------------------------------------------

func TestPostgresCompatComprehensive(t *testing.T) {
	// This table-driven test covers various PostgreSQL-specific syntax combinations
	// to ensure broad compatibility.
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// DISTINCT ON variations
		{"DISTINCT ON basic", "SELECT DISTINCT ON (a) * FROM t", false},
		{"DISTINCT ON with alias", "SELECT DISTINCT ON (a) a AS col1, b FROM t", false},
		{"DISTINCT ON with join", "SELECT DISTINCT ON (t1.id) * FROM t1 JOIN t2 ON t1.id = t2.id", false},

		// LIMIT ALL variations
		{"LIMIT ALL basic", "SELECT * FROM t LIMIT ALL", false},
		{"LIMIT ALL with OFFSET", "SELECT * FROM t LIMIT ALL OFFSET 10", false},
		{"LIMIT ALL after ORDER BY", "SELECT * FROM t ORDER BY id LIMIT ALL", false},

		// :: type cast variations
		{"cast integer", "SELECT 1::integer", false},
		{"cast varchar with size", "SELECT 'test'::varchar(255)", false},
		{"cast in expression", "SELECT a::integer + b::integer FROM t", false},
		{"cast function result", "SELECT NOW()::date", false},

		// ILIKE variations
		{"ILIKE basic", "SELECT * FROM t WHERE a ILIKE 'b'", false},
		{"NOT ILIKE", "SELECT * FROM t WHERE a NOT ILIKE 'b'", false},
		{"ILIKE with wildcards", "SELECT * FROM t WHERE a ILIKE '%b_c%'", false},

		// GROUP BY ordinal variations
		{"GROUP BY 1", "SELECT a FROM t GROUP BY 1", false},
		{"GROUP BY 1, 2, 3", "SELECT a, b, c FROM t GROUP BY 1, 2, 3", false},
		{"GROUP BY ordinal with HAVING", "SELECT a, COUNT(*) FROM t GROUP BY 1 HAVING COUNT(*) > 1", false},

		// WITH RECURSIVE variations
		{"WITH RECURSIVE basic", "WITH RECURSIVE r AS (SELECT 1) SELECT * FROM r", false},
		{"WITH RECURSIVE with columns", "WITH RECURSIVE r(x) AS (SELECT 1) SELECT * FROM r", false},
		{"WITH RECURSIVE with UNION ALL", "WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r WHERE x < 10) SELECT * FROM r", false},

		// Combined features
		{"DISTINCT ON + ORDER BY + LIMIT ALL", "SELECT DISTINCT ON (a) * FROM t ORDER BY a LIMIT ALL", false},
		{"cast + ILIKE", "SELECT * FROM t WHERE a::text ILIKE '%b%'", false},
		{"GROUP BY ordinal + cast", "SELECT a::text, COUNT(*) FROM t GROUP BY 1", false},
		{"WITH + DISTINCT ON", "WITH tmp AS (SELECT * FROM t) SELECT DISTINCT ON (a) * FROM tmp", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// SQL Keyword Typo Detection Tests
// -----------------------------------------------------------------------------

func TestParserKeywordTypoDetection(t *testing.T) {
	// These tests verify that the parser rejects common keyword typos
	// and provides helpful error messages.
	tests := []struct {
		name        string
		sql         string
		wantErr     bool
		errContains string // substring that should appear in error message
	}{
		// FROM typos
		{
			name:        "FORM instead of FROM",
			sql:         "SELECT * FORM users",
			wantErr:     true,
			errContains: "FROM",
		},
		{
			name:        "FOMR instead of FROM",
			sql:         "SELECT * FOMR users",
			wantErr:     true,
			errContains: "FROM",
		},
		{
			name:        "FORM in subquery",
			sql:         "SELECT * FROM (SELECT * FORM t) AS sub",
			wantErr:     true,
			errContains: "FROM",
		},

		// SELECT typos
		{
			name:        "SELCET instead of SELECT",
			sql:         "SELCET * FROM users",
			wantErr:     true,
			errContains: "SELECT",
		},
		{
			name:        "SELEC instead of SELECT",
			sql:         "SELEC * FROM users",
			wantErr:     true,
			errContains: "SELECT",
		},
		{
			name:        "SELET instead of SELECT",
			sql:         "SELET * FROM users",
			wantErr:     true,
			errContains: "SELECT",
		},

		// INSERT typos
		{
			name:        "INSRT instead of INSERT",
			sql:         "INSRT INTO users VALUES (1)",
			wantErr:     true,
			errContains: "INSERT",
		},
		{
			name:        "INSER instead of INSERT",
			sql:         "INSER INTO users VALUES (1)",
			wantErr:     true,
			errContains: "INSERT",
		},

		// INTO typos
		{
			name:        "ITNO instead of INTO",
			sql:         "INSERT ITNO users VALUES (1)",
			wantErr:     true,
			errContains: "INTO",
		},
		{
			name:        "INOT instead of INTO",
			sql:         "INSERT INOT users VALUES (1)",
			wantErr:     true,
			errContains: "INTO",
		},

		// WHERE typos
		{
			name:        "WHER instead of WHERE",
			sql:         "SELECT * FROM users WHER id = 1",
			wantErr:     true,
			errContains: "WHERE",
		},
		{
			name:        "WHRE instead of WHERE",
			sql:         "SELECT * FROM users WHRE id = 1",
			wantErr:     true,
			errContains: "WHERE",
		},
		{
			name:        "WEHRE instead of WHERE",
			sql:         "UPDATE users SET name = 'test' WEHRE id = 1",
			wantErr:     true,
			errContains: "WHERE",
		},

		// UPDATE typos
		{
			name:        "UPATE instead of UPDATE",
			sql:         "UPATE users SET name = 'test'",
			wantErr:     true,
			errContains: "UPDATE",
		},
		{
			name:        "UDPATE instead of UPDATE",
			sql:         "UDPATE users SET name = 'test'",
			wantErr:     true,
			errContains: "UPDATE",
		},

		// DELETE typos
		{
			name:        "DELET instead of DELETE",
			sql:         "DELET FROM users WHERE id = 1",
			wantErr:     true,
			errContains: "DELETE",
		},
		{
			name:        "DELEET instead of DELETE",
			sql:         "DELEET FROM users WHERE id = 1",
			wantErr:     true,
			errContains: "DELETE",
		},

		// VALUES typos
		{
			name:        "VALUEES instead of VALUES",
			sql:         "INSERT INTO users VALUEES (1)",
			wantErr:     true,
			errContains: "VALUES",
		},
		{
			name:        "VALUSE instead of VALUES",
			sql:         "INSERT INTO users VALUSE (1)",
			wantErr:     true,
			errContains: "VALUES",
		},

		// SET typos
		{
			name:        "SE instead of SET",
			sql:         "UPDATE users SE name = 'test' WHERE id = 1",
			wantErr:     true,
			errContains: "SET",
		},
		{
			name:        "SETT instead of SET",
			sql:         "UPDATE users SETT name = 'test' WHERE id = 1",
			wantErr:     true,
			errContains: "SET",
		},

		// ORDER BY typos
		{
			name:        "ORER instead of ORDER",
			sql:         "SELECT * FROM users ORER BY id",
			wantErr:     true,
			errContains: "ORDER",
		},
		{
			name:        "ODER instead of ORDER",
			sql:         "SELECT * FROM users ODER BY id",
			wantErr:     true,
			errContains: "ORDER",
		},

		// GROUP BY typos
		{
			name:        "GROPU instead of GROUP",
			sql:         "SELECT COUNT(*) FROM users GROPU BY name",
			wantErr:     true,
			errContains: "GROUP",
		},
		{
			name:        "GROP instead of GROUP",
			sql:         "SELECT COUNT(*) FROM users GROP BY name",
			wantErr:     true,
			errContains: "GROUP",
		},

		// LIMIT typos
		{
			name:        "LIMT instead of LIMIT",
			sql:         "SELECT * FROM users LIMT 10",
			wantErr:     true,
			errContains: "LIMIT",
		},
		{
			name:        "LMIT instead of LIMIT",
			sql:         "SELECT * FROM users LMIT 10",
			wantErr:     true,
			errContains: "LIMIT",
		},

		// JOIN typos
		{
			name:        "JON instead of JOIN",
			sql:         "SELECT * FROM users u JON orders o ON u.id = o.user_id",
			wantErr:     true,
			errContains: "JOIN",
		},

		// TABLE typos (in CREATE TABLE)
		{
			name:        "TABEL instead of TABLE",
			sql:         "CREATE TABEL users (id INT)",
			wantErr:     true,
			errContains: "TABLE",
		},
		{
			name:        "TABL instead of TABLE",
			sql:         "CREATE TABL users (id INT)",
			wantErr:     true,
			errContains: "TABLE",
		},

		// CREATE typos
		{
			name:        "CRATE instead of CREATE",
			sql:         "CRATE TABLE users (id INT)",
			wantErr:     true,
			errContains: "CREATE",
		},
		{
			name:        "CREAT instead of CREATE",
			sql:         "CREAT TABLE users (id INT)",
			wantErr:     true,
			errContains: "CREATE",
		},

		// DROP typos
		{
			name:        "DORP instead of DROP",
			sql:         "DORP TABLE users",
			wantErr:     true,
			errContains: "DROP",
		},

		// Valid queries (negative tests - should NOT error)
		{
			name:    "valid SELECT FROM",
			sql:     "SELECT * FROM users",
			wantErr: false,
		},
		{
			name:    "valid INSERT INTO VALUES",
			sql:     "INSERT INTO users VALUES (1, 'test')",
			wantErr: false,
		},
		{
			name:    "valid UPDATE SET WHERE",
			sql:     "UPDATE users SET name = 'test' WHERE id = 1",
			wantErr: false,
		},
		{
			name:    "valid DELETE FROM WHERE",
			sql:     "DELETE FROM users WHERE id = 1",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr {
				require.Error(t, err, "expected error for SQL: %s", tt.sql)
				// Verify error message contains the suggested correct keyword
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains,
						"error message should suggest correct keyword for SQL: %s", tt.sql)
				}
			} else {
				assert.NoError(t, err, "unexpected error for SQL: %s", tt.sql)
			}
		})
	}
}
