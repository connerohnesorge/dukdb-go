package engine

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/optimizer"
)

// TestStatisticsPersistence tests that statistics survive database restart
func TestStatisticsPersistence(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_stats.db")

	// Phase 1: Create database, add data, collect statistics
	{
		engine := NewEngine()

		conn, err := engine.Open(dbPath, nil)
		require.NoError(t, err)
		require.NotNil(t, conn)

		// Create table definition
		tableDef := catalog.NewTableDef(
			"users",
			[]*catalog.ColumnDef{
				catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
				catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
				catalog.NewColumnDef("age", dukdb.TYPE_INTEGER),
			},
		)
		err = engine.Catalog().CreateTable(tableDef)
		require.NoError(t, err)

		// Create table in storage
		table, err := engine.Storage().CreateTable("users", []dukdb.Type{
			dukdb.TYPE_INTEGER,
			dukdb.TYPE_VARCHAR,
			dukdb.TYPE_INTEGER,
		})
		require.NoError(t, err)

		// Insert test data
		err = table.AppendRow([]any{int32(1), "Alice", int32(25)})
		require.NoError(t, err)
		err = table.AppendRow([]any{int32(2), "Bob", int32(30)})
		require.NoError(t, err)
		err = table.AppendRow([]any{int32(3), "Charlie", int32(35)})
		require.NoError(t, err)
		err = table.AppendRow([]any{int32(4), "Diana", int32(28)})
		require.NoError(t, err)

		// Collect statistics (simulating ANALYZE)
		collector := optimizer.NewStatisticsCollector()
		columnNames := []string{"id", "name", "age"}
		columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER}

		dataReader := func(colIdx int) ([]any, error) {
			var values []any
			scanner := table.Scan()
			for chunk := scanner.Next(); chunk != nil; chunk = scanner.Next() {
				for i := 0; i < chunk.Count(); i++ {
					values = append(values, chunk.GetValue(i, colIdx))
				}
			}
			return values, nil
		}

		stats, err := collector.CollectTableStats(
			columnNames,
			columnTypes,
			table.RowCount(),
			dataReader,
		)
		require.NoError(t, err)
		require.NotNil(t, stats)

		// Store statistics in catalog
		tableDef.Statistics = stats

		// Close engine (should persist statistics)
		err = engine.Close()
		require.NoError(t, err)
	}

	// Phase 2: Reopen database and verify statistics are loaded
	{
		engine := NewEngine()

		conn, err := engine.Open(dbPath, nil)
		require.NoError(t, err)
		require.NotNil(t, conn)

		// Verify data is intact
		table, ok := engine.Storage().GetTable("users")
		require.True(t, ok)
		assert.Equal(t, int64(4), table.RowCount())

		// Verify statistics were loaded
		tableDef, ok := engine.Catalog().GetTable("users")
		require.True(t, ok)
		assert.NotNil(t, tableDef.Statistics)
		assert.Equal(t, int64(4), tableDef.Statistics.RowCount)

		// Close engine
		err = engine.Close()
		require.NoError(t, err)
	}
}

// TestStatisticsMultipleTables verifies statistics work with multiple tables
func TestStatisticsMultipleTables(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_stats_multi.db")

	// Phase 1: Create multiple tables with statistics
	{
		engine := NewEngine()

		conn, err := engine.Open(dbPath, nil)
		require.NoError(t, err)
		require.NotNil(t, conn)

		// Create table 1
		tableDef1 := catalog.NewTableDef(
			"table1",
			[]*catalog.ColumnDef{
				catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
				catalog.NewColumnDef("data", dukdb.TYPE_VARCHAR),
			},
		)
		err = engine.Catalog().CreateTable(tableDef1)
		require.NoError(t, err)

		table1, err := engine.Storage().CreateTable("table1", []dukdb.Type{
			dukdb.TYPE_INTEGER,
			dukdb.TYPE_VARCHAR,
		})
		require.NoError(t, err)

		// Insert data into table1
		err = table1.AppendRow([]any{int32(1), "a"})
		require.NoError(t, err)
		err = table1.AppendRow([]any{int32(2), "b"})
		require.NoError(t, err)
		err = table1.AppendRow([]any{int32(3), "c"})
		require.NoError(t, err)

		// Collect statistics for table1
		collector1 := optimizer.NewStatisticsCollector()
		dataReader1 := func(colIdx int) ([]any, error) {
			var values []any
			scanner := table1.Scan()
			for chunk := scanner.Next(); chunk != nil; chunk = scanner.Next() {
				for i := 0; i < chunk.Count(); i++ {
					values = append(values, chunk.GetValue(i, colIdx))
				}
			}
			return values, nil
		}
		stats1, err := collector1.CollectTableStats(
			[]string{"id", "data"},
			[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR},
			table1.RowCount(),
			dataReader1,
		)
		require.NoError(t, err)
		tableDef1.Statistics = stats1

		// Create table 2
		tableDef2 := catalog.NewTableDef(
			"table2",
			[]*catalog.ColumnDef{
				catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
				catalog.NewColumnDef("value", dukdb.TYPE_INTEGER),
			},
		)
		err = engine.Catalog().CreateTable(tableDef2)
		require.NoError(t, err)

		table2, err := engine.Storage().CreateTable("table2", []dukdb.Type{
			dukdb.TYPE_INTEGER,
			dukdb.TYPE_INTEGER,
		})
		require.NoError(t, err)

		// Insert data into table2
		err = table2.AppendRow([]any{int32(1), int32(10)})
		require.NoError(t, err)
		err = table2.AppendRow([]any{int32(2), int32(20)})
		require.NoError(t, err)
		err = table2.AppendRow([]any{int32(3), int32(30)})
		require.NoError(t, err)

		// Collect statistics for table2
		collector2 := optimizer.NewStatisticsCollector()
		dataReader2 := func(colIdx int) ([]any, error) {
			var values []any
			scanner := table2.Scan()
			for chunk := scanner.Next(); chunk != nil; chunk = scanner.Next() {
				for i := 0; i < chunk.Count(); i++ {
					values = append(values, chunk.GetValue(i, colIdx))
				}
			}
			return values, nil
		}
		stats2, err := collector2.CollectTableStats(
			[]string{"id", "value"},
			[]dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER},
			table2.RowCount(),
			dataReader2,
		)
		require.NoError(t, err)
		tableDef2.Statistics = stats2

		// Close engine (should persist both tables' statistics)
		err = engine.Close()
		require.NoError(t, err)
	}

	// Phase 2: Reopen and verify both tables' statistics are available
	{
		engine := NewEngine()

		conn, err := engine.Open(dbPath, nil)
		require.NoError(t, err)
		require.NotNil(t, conn)

		// Verify table1 statistics
		tableDef1, ok := engine.Catalog().GetTable("table1")
		require.True(t, ok)
		assert.NotNil(t, tableDef1.Statistics)
		assert.Equal(t, int64(3), tableDef1.Statistics.RowCount)

		// Verify table2 statistics
		tableDef2, ok := engine.Catalog().GetTable("table2")
		require.True(t, ok)
		assert.NotNil(t, tableDef2.Statistics)
		assert.Equal(t, int64(3), tableDef2.Statistics.RowCount)

		// Close engine
		err = engine.Close()
		require.NoError(t, err)
	}
}
