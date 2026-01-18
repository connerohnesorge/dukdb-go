package planner

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIcebergPlannerExtractPartitionFilters(t *testing.T) {
	planner := NewIcebergPlanner()

	t.Run("nil filter returns empty", func(t *testing.T) {
		filters, residual := planner.ExtractPartitionFilters(nil)
		assert.Nil(t, filters)
		assert.Nil(t, residual)
	})

	t.Run("simple equality filter", func(t *testing.T) {
		// year = 2024
		filter := &binder.BoundBinaryExpr{
			Left:  &binder.BoundColumnRef{Column: "year", ColType: dukdb.TYPE_INTEGER},
			Op:    parser.OpEq,
			Right: &binder.BoundLiteral{Value: int64(2024), ValType: dukdb.TYPE_INTEGER},
		}

		filters, residual := planner.ExtractPartitionFilters(filter)

		require.Len(t, filters, 1)
		assert.Equal(t, "year", filters[0].FieldName)
		assert.Equal(t, "=", filters[0].Operator)
		assert.Equal(t, int64(2024), filters[0].Value)
		assert.Equal(t, "identity", filters[0].Transform)
		assert.Nil(t, residual)
	})

	t.Run("reversed equality filter", func(t *testing.T) {
		// 2024 = year
		filter := &binder.BoundBinaryExpr{
			Left:  &binder.BoundLiteral{Value: int64(2024), ValType: dukdb.TYPE_INTEGER},
			Op:    parser.OpEq,
			Right: &binder.BoundColumnRef{Column: "year", ColType: dukdb.TYPE_INTEGER},
		}

		filters, residual := planner.ExtractPartitionFilters(filter)

		require.Len(t, filters, 1)
		assert.Equal(t, "year", filters[0].FieldName)
		assert.Equal(t, "=", filters[0].Operator)
		assert.Equal(t, int64(2024), filters[0].Value)
		assert.Nil(t, residual)
	})

	t.Run("inequality filter", func(t *testing.T) {
		// status != 'active'
		filter := &binder.BoundBinaryExpr{
			Left:  &binder.BoundColumnRef{Column: "status", ColType: dukdb.TYPE_VARCHAR},
			Op:    parser.OpNe,
			Right: &binder.BoundLiteral{Value: "active", ValType: dukdb.TYPE_VARCHAR},
		}

		filters, residual := planner.ExtractPartitionFilters(filter)

		require.Len(t, filters, 1)
		assert.Equal(t, "status", filters[0].FieldName)
		assert.Equal(t, "!=", filters[0].Operator)
		assert.Equal(t, "active", filters[0].Value)
		assert.Nil(t, residual)
	})

	t.Run("range filter less than", func(t *testing.T) {
		// price < 100.0
		filter := &binder.BoundBinaryExpr{
			Left:  &binder.BoundColumnRef{Column: "price", ColType: dukdb.TYPE_DOUBLE},
			Op:    parser.OpLt,
			Right: &binder.BoundLiteral{Value: 100.0, ValType: dukdb.TYPE_DOUBLE},
		}

		filters, residual := planner.ExtractPartitionFilters(filter)

		require.Len(t, filters, 1)
		assert.Equal(t, "price", filters[0].FieldName)
		assert.Equal(t, "<", filters[0].Operator)
		assert.Equal(t, 100.0, filters[0].Value)
		assert.Nil(t, residual)
	})

	t.Run("range filter greater than or equal", func(t *testing.T) {
		// quantity >= 10
		filter := &binder.BoundBinaryExpr{
			Left:  &binder.BoundColumnRef{Column: "quantity", ColType: dukdb.TYPE_INTEGER},
			Op:    parser.OpGe,
			Right: &binder.BoundLiteral{Value: int64(10), ValType: dukdb.TYPE_INTEGER},
		}

		filters, residual := planner.ExtractPartitionFilters(filter)

		require.Len(t, filters, 1)
		assert.Equal(t, "quantity", filters[0].FieldName)
		assert.Equal(t, ">=", filters[0].Operator)
		assert.Equal(t, int64(10), filters[0].Value)
		assert.Nil(t, residual)
	})

	t.Run("reversed range filter", func(t *testing.T) {
		// 100 > price => price < 100
		filter := &binder.BoundBinaryExpr{
			Left:  &binder.BoundLiteral{Value: 100.0, ValType: dukdb.TYPE_DOUBLE},
			Op:    parser.OpGt,
			Right: &binder.BoundColumnRef{Column: "price", ColType: dukdb.TYPE_DOUBLE},
		}

		filters, residual := planner.ExtractPartitionFilters(filter)

		require.Len(t, filters, 1)
		assert.Equal(t, "price", filters[0].FieldName)
		assert.Equal(t, "<", filters[0].Operator)
		assert.Equal(t, 100.0, filters[0].Value)
		assert.Nil(t, residual)
	})

	t.Run("AND of two partition filters", func(t *testing.T) {
		// year = 2024 AND month = 1
		filter := &binder.BoundBinaryExpr{
			Left: &binder.BoundBinaryExpr{
				Left:  &binder.BoundColumnRef{Column: "year", ColType: dukdb.TYPE_INTEGER},
				Op:    parser.OpEq,
				Right: &binder.BoundLiteral{Value: int64(2024), ValType: dukdb.TYPE_INTEGER},
			},
			Op: parser.OpAnd,
			Right: &binder.BoundBinaryExpr{
				Left:  &binder.BoundColumnRef{Column: "month", ColType: dukdb.TYPE_INTEGER},
				Op:    parser.OpEq,
				Right: &binder.BoundLiteral{Value: int64(1), ValType: dukdb.TYPE_INTEGER},
			},
		}

		filters, residual := planner.ExtractPartitionFilters(filter)

		require.Len(t, filters, 2)
		assert.Equal(t, "year", filters[0].FieldName)
		assert.Equal(t, int64(2024), filters[0].Value)
		assert.Equal(t, "month", filters[1].FieldName)
		assert.Equal(t, int64(1), filters[1].Value)
		assert.Nil(t, residual)
	})

	t.Run("IN list filter", func(t *testing.T) {
		// region IN ('us', 'eu', 'asia')
		filter := &binder.BoundInListExpr{
			Expr: &binder.BoundColumnRef{Column: "region", ColType: dukdb.TYPE_VARCHAR},
			Values: []binder.BoundExpr{
				&binder.BoundLiteral{Value: "us", ValType: dukdb.TYPE_VARCHAR},
				&binder.BoundLiteral{Value: "eu", ValType: dukdb.TYPE_VARCHAR},
				&binder.BoundLiteral{Value: "asia", ValType: dukdb.TYPE_VARCHAR},
			},
			Not: false,
		}

		filters, residual := planner.ExtractPartitionFilters(filter)

		require.Len(t, filters, 1)
		assert.Equal(t, "region", filters[0].FieldName)
		assert.Equal(t, "IN", filters[0].Operator)
		values, ok := filters[0].Value.([]any)
		require.True(t, ok)
		assert.Len(t, values, 3)
		assert.Equal(t, "us", values[0])
		assert.Equal(t, "eu", values[1])
		assert.Equal(t, "asia", values[2])
		assert.Nil(t, residual)
	})

	t.Run("NOT IN list filter", func(t *testing.T) {
		// region NOT IN ('test')
		filter := &binder.BoundInListExpr{
			Expr: &binder.BoundColumnRef{Column: "region", ColType: dukdb.TYPE_VARCHAR},
			Values: []binder.BoundExpr{
				&binder.BoundLiteral{Value: "test", ValType: dukdb.TYPE_VARCHAR},
			},
			Not: true,
		}

		filters, residual := planner.ExtractPartitionFilters(filter)

		require.Len(t, filters, 1)
		assert.Equal(t, "region", filters[0].FieldName)
		assert.Equal(t, "NOT IN", filters[0].Operator)
		assert.Nil(t, residual)
	})

	t.Run("IS NULL filter", func(t *testing.T) {
		filter := &binder.BoundUnaryExpr{
			Op:   parser.OpIsNull,
			Expr: &binder.BoundColumnRef{Column: "optional_field", ColType: dukdb.TYPE_VARCHAR},
		}

		filters, residual := planner.ExtractPartitionFilters(filter)

		require.Len(t, filters, 1)
		assert.Equal(t, "optional_field", filters[0].FieldName)
		assert.Equal(t, "IS NULL", filters[0].Operator)
		assert.Nil(t, filters[0].Value)
		assert.Nil(t, residual)
	})

	t.Run("IS NOT NULL filter", func(t *testing.T) {
		filter := &binder.BoundUnaryExpr{
			Op:   parser.OpIsNotNull,
			Expr: &binder.BoundColumnRef{Column: "required_field", ColType: dukdb.TYPE_VARCHAR},
		}

		filters, residual := planner.ExtractPartitionFilters(filter)

		require.Len(t, filters, 1)
		assert.Equal(t, "required_field", filters[0].FieldName)
		assert.Equal(t, "IS NOT NULL", filters[0].Operator)
		assert.Nil(t, filters[0].Value)
		assert.Nil(t, residual)
	})

	t.Run("non-pushable expression becomes residual", func(t *testing.T) {
		// UPPER(name) = 'ALICE' - function call can't be pushed
		filter := &binder.BoundBinaryExpr{
			Left: &binder.BoundFunctionCall{
				Name: "UPPER",
				Args: []binder.BoundExpr{
					&binder.BoundColumnRef{Column: "name", ColType: dukdb.TYPE_VARCHAR},
				},
				ResType: dukdb.TYPE_VARCHAR,
			},
			Op:    parser.OpEq,
			Right: &binder.BoundLiteral{Value: "ALICE", ValType: dukdb.TYPE_VARCHAR},
		}

		filters, residual := planner.ExtractPartitionFilters(filter)

		assert.Empty(t, filters)
		assert.NotNil(t, residual)
		// The residual should be the original filter
		assert.Equal(t, filter, residual)
	})

	t.Run("mixed pushable and non-pushable", func(t *testing.T) {
		// year = 2024 AND UPPER(name) = 'ALICE'
		pushableFilter := &binder.BoundBinaryExpr{
			Left:  &binder.BoundColumnRef{Column: "year", ColType: dukdb.TYPE_INTEGER},
			Op:    parser.OpEq,
			Right: &binder.BoundLiteral{Value: int64(2024), ValType: dukdb.TYPE_INTEGER},
		}
		nonPushableFilter := &binder.BoundBinaryExpr{
			Left: &binder.BoundFunctionCall{
				Name: "UPPER",
				Args: []binder.BoundExpr{
					&binder.BoundColumnRef{Column: "name", ColType: dukdb.TYPE_VARCHAR},
				},
				ResType: dukdb.TYPE_VARCHAR,
			},
			Op:    parser.OpEq,
			Right: &binder.BoundLiteral{Value: "ALICE", ValType: dukdb.TYPE_VARCHAR},
		}

		filter := &binder.BoundBinaryExpr{
			Left:  pushableFilter,
			Op:    parser.OpAnd,
			Right: nonPushableFilter,
		}

		filters, residual := planner.ExtractPartitionFilters(filter)

		// Should extract the pushable filter
		require.Len(t, filters, 1)
		assert.Equal(t, "year", filters[0].FieldName)
		assert.Equal(t, int64(2024), filters[0].Value)

		// Should have residual for non-pushable
		assert.NotNil(t, residual)
	})
}

func TestIcebergPlannerExtractColumnProjection(t *testing.T) {
	planner := NewIcebergPlanner()

	t.Run("single column reference", func(t *testing.T) {
		columns := []*binder.BoundSelectColumn{
			{
				Expr:  &binder.BoundColumnRef{Column: "id", ColType: dukdb.TYPE_INTEGER},
				Alias: "id",
			},
		}

		result := planner.ExtractColumnProjection(columns)

		require.Len(t, result, 1)
		assert.Contains(t, result, "id")
	})

	t.Run("multiple column references", func(t *testing.T) {
		columns := []*binder.BoundSelectColumn{
			{
				Expr:  &binder.BoundColumnRef{Column: "id", ColType: dukdb.TYPE_INTEGER},
				Alias: "id",
			},
			{
				Expr:  &binder.BoundColumnRef{Column: "name", ColType: dukdb.TYPE_VARCHAR},
				Alias: "name",
			},
			{
				Expr:  &binder.BoundColumnRef{Column: "price", ColType: dukdb.TYPE_DOUBLE},
				Alias: "price",
			},
		}

		result := planner.ExtractColumnProjection(columns)

		require.Len(t, result, 3)
		assert.Contains(t, result, "id")
		assert.Contains(t, result, "name")
		assert.Contains(t, result, "price")
	})

	t.Run("expression with multiple column references", func(t *testing.T) {
		// SELECT a + b
		columns := []*binder.BoundSelectColumn{
			{
				Expr: &binder.BoundBinaryExpr{
					Left:  &binder.BoundColumnRef{Column: "a", ColType: dukdb.TYPE_INTEGER},
					Op:    parser.OpAdd,
					Right: &binder.BoundColumnRef{Column: "b", ColType: dukdb.TYPE_INTEGER},
				},
				Alias: "sum",
			},
		}

		result := planner.ExtractColumnProjection(columns)

		require.Len(t, result, 2)
		assert.Contains(t, result, "a")
		assert.Contains(t, result, "b")
	})

	t.Run("function call with column reference", func(t *testing.T) {
		// SELECT UPPER(name)
		columns := []*binder.BoundSelectColumn{
			{
				Expr: &binder.BoundFunctionCall{
					Name: "UPPER",
					Args: []binder.BoundExpr{
						&binder.BoundColumnRef{Column: "name", ColType: dukdb.TYPE_VARCHAR},
					},
					ResType: dukdb.TYPE_VARCHAR,
				},
				Alias: "upper_name",
			},
		}

		result := planner.ExtractColumnProjection(columns)

		require.Len(t, result, 1)
		assert.Contains(t, result, "name")
	})

	t.Run("duplicate columns are deduplicated", func(t *testing.T) {
		// SELECT id, id + 1
		columns := []*binder.BoundSelectColumn{
			{
				Expr:  &binder.BoundColumnRef{Column: "id", ColType: dukdb.TYPE_INTEGER},
				Alias: "id",
			},
			{
				Expr: &binder.BoundBinaryExpr{
					Left:  &binder.BoundColumnRef{Column: "id", ColType: dukdb.TYPE_INTEGER},
					Op:    parser.OpAdd,
					Right: &binder.BoundLiteral{Value: int64(1), ValType: dukdb.TYPE_INTEGER},
				},
				Alias: "id_plus_one",
			},
		}

		result := planner.ExtractColumnProjection(columns)

		// Should only have 1 unique column
		require.Len(t, result, 1)
		assert.Contains(t, result, "id")
	})

	t.Run("literal only returns empty", func(t *testing.T) {
		// SELECT 42
		columns := []*binder.BoundSelectColumn{
			{
				Expr:  &binder.BoundLiteral{Value: int64(42), ValType: dukdb.TYPE_INTEGER},
				Alias: "answer",
			},
		}

		result := planner.ExtractColumnProjection(columns)

		assert.Empty(t, result)
	})
}

func TestIcebergPlannerExtractFilterColumnReferences(t *testing.T) {
	planner := NewIcebergPlanner()

	t.Run("simple column reference", func(t *testing.T) {
		filter := &binder.BoundBinaryExpr{
			Left:  &binder.BoundColumnRef{Column: "year", ColType: dukdb.TYPE_INTEGER},
			Op:    parser.OpEq,
			Right: &binder.BoundLiteral{Value: int64(2024), ValType: dukdb.TYPE_INTEGER},
		}

		result := planner.ExtractFilterColumnReferences(filter)

		require.Len(t, result, 1)
		assert.Contains(t, result, "year")
	})

	t.Run("multiple column references in AND", func(t *testing.T) {
		filter := &binder.BoundBinaryExpr{
			Left: &binder.BoundBinaryExpr{
				Left:  &binder.BoundColumnRef{Column: "year", ColType: dukdb.TYPE_INTEGER},
				Op:    parser.OpEq,
				Right: &binder.BoundLiteral{Value: int64(2024), ValType: dukdb.TYPE_INTEGER},
			},
			Op: parser.OpAnd,
			Right: &binder.BoundBinaryExpr{
				Left:  &binder.BoundColumnRef{Column: "month", ColType: dukdb.TYPE_INTEGER},
				Op:    parser.OpEq,
				Right: &binder.BoundLiteral{Value: int64(1), ValType: dukdb.TYPE_INTEGER},
			},
		}

		result := planner.ExtractFilterColumnReferences(filter)

		require.Len(t, result, 2)
		assert.Contains(t, result, "year")
		assert.Contains(t, result, "month")
	})

	t.Run("nil filter returns empty", func(t *testing.T) {
		result := planner.ExtractFilterColumnReferences(nil)
		assert.Empty(t, result)
	})
}

func TestLogicalIcebergScan(t *testing.T) {
	t.Run("basic creation", func(t *testing.T) {
		scan := &LogicalIcebergScan{
			TablePath: "/path/to/iceberg/table",
			Alias:     "t",
			Columns:   []string{"id", "name", "value"},
			ColumnTypes: []dukdb.Type{
				dukdb.TYPE_INTEGER,
				dukdb.TYPE_VARCHAR,
				dukdb.TYPE_DOUBLE,
			},
		}

		assert.Equal(t, "/path/to/iceberg/table", scan.TablePath)
		assert.Equal(t, "t", scan.Alias)
		assert.Nil(t, scan.Children())

		cols := scan.OutputColumns()
		require.Len(t, cols, 3)
		assert.Equal(t, "id", cols[0].Column)
		assert.Equal(t, dukdb.TYPE_INTEGER, cols[0].Type)
		assert.Equal(t, "t", cols[0].Table)
	})

	t.Run("with time travel", func(t *testing.T) {
		snapshotID := int64(12345)
		scan := &LogicalIcebergScan{
			TablePath: "/path/to/iceberg/table",
			TimeTravel: &TimeTravelClause{
				Type:       TimeTravelSnapshot,
				SnapshotID: &snapshotID,
			},
		}

		assert.Equal(t, TimeTravelSnapshot, scan.TimeTravel.Type)
		assert.Equal(t, int64(12345), *scan.TimeTravel.SnapshotID)
	})

	t.Run("with timestamp time travel", func(t *testing.T) {
		timestamp := int64(1704067200000) // 2024-01-01 00:00:00 UTC
		scan := &LogicalIcebergScan{
			TablePath: "/path/to/iceberg/table",
			TimeTravel: &TimeTravelClause{
				Type:      TimeTravelTimestamp,
				Timestamp: &timestamp,
			},
		}

		assert.Equal(t, TimeTravelTimestamp, scan.TimeTravel.Type)
		assert.Equal(t, int64(1704067200000), *scan.TimeTravel.Timestamp)
	})
}

func TestPhysicalIcebergScan(t *testing.T) {
	t.Run("basic creation", func(t *testing.T) {
		scan := &PhysicalIcebergScan{
			TablePath: "/path/to/iceberg/table",
			Alias:     "t",
			Columns:   []string{"id", "name"},
			ColumnTypes: []dukdb.Type{
				dukdb.TYPE_INTEGER,
				dukdb.TYPE_VARCHAR,
			},
			PartitionFilters: []PartitionFilter{
				{
					FieldName: "year",
					Operator:  "=",
					Value:     int64(2024),
					Transform: "identity",
				},
			},
			EstimatedRows: 1000,
		}

		assert.Equal(t, "/path/to/iceberg/table", scan.TablePath)
		assert.Nil(t, scan.Children())

		cols := scan.OutputColumns()
		require.Len(t, cols, 2)

		assert.Len(t, scan.PartitionFilters, 1)
		assert.Equal(t, "year", scan.PartitionFilters[0].FieldName)
		assert.Equal(t, int64(1000), scan.EstimatedRows)
	})

	t.Run("with residual filter", func(t *testing.T) {
		residualFilter := &binder.BoundBinaryExpr{
			Left: &binder.BoundFunctionCall{
				Name: "UPPER",
				Args: []binder.BoundExpr{
					&binder.BoundColumnRef{Column: "name", ColType: dukdb.TYPE_VARCHAR},
				},
				ResType: dukdb.TYPE_VARCHAR,
			},
			Op:    parser.OpEq,
			Right: &binder.BoundLiteral{Value: "ALICE", ValType: dukdb.TYPE_VARCHAR},
		}

		scan := &PhysicalIcebergScan{
			TablePath:      "/path/to/iceberg/table",
			ResidualFilter: residualFilter,
		}

		assert.NotNil(t, scan.ResidualFilter)
	})

	t.Run("with column stats", func(t *testing.T) {
		scan := &PhysicalIcebergScan{
			TablePath: "/path/to/iceberg/table",
			ColumnStats: []ColumnStat{
				{
					ColumnName:    "price",
					MinValue:      0.0,
					MaxValue:      1000.0,
					NullCount:     5,
					DistinctCount: 500,
				},
			},
		}

		require.Len(t, scan.ColumnStats, 1)
		assert.Equal(t, "price", scan.ColumnStats[0].ColumnName)
		assert.Equal(t, 0.0, scan.ColumnStats[0].MinValue)
		assert.Equal(t, 1000.0, scan.ColumnStats[0].MaxValue)
	})
}
