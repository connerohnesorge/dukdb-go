package optimizer

import (
	dukdb "github.com/dukdb/dukdb-go"
)

// ColumnInfo provides column information needed for statistics.
// This interface is implemented by catalog.ColumnDef.
type ColumnInfo interface {
	// GetName returns the column name.
	GetName() string
	// GetType returns the column type.
	GetType() dukdb.Type
}

// TableInfo provides table information needed for statistics.
// This interface is implemented by catalog.TableDef.
type TableInfo interface {
	// GetStatistics returns the table's statistics, or nil if not analyzed.
	GetStatistics() *TableStatistics
	// GetColumns returns the table's column information.
	GetColumns() []ColumnInfo
	// GetColumnInfo returns a column by name as a ColumnInfo interface.
	GetColumnInfo(name string) (ColumnInfo, bool)
}

// CatalogProvider provides access to catalog information for statistics.
// This interface is implemented by catalog.Catalog.
type CatalogProvider interface {
	// GetTableInfo returns table information for the given schema and table name.
	// Returns nil if the table does not exist.
	GetTableInfo(schema, table string) TableInfo
}

// StatisticsManager provides access to table statistics with sensible defaults.
// It acts as a facade for retrieving statistics from the catalog and provides
// conservative default estimates when no statistics are available.
type StatisticsManager struct {
	catalog CatalogProvider
}

// NewStatisticsManager creates a new StatisticsManager with the given catalog provider.
func NewStatisticsManager(cat CatalogProvider) *StatisticsManager {
	return &StatisticsManager{
		catalog: cat,
	}
}

// GetTableStats returns statistics for a table.
// Returns default statistics if the table has not been analyzed.
func (m *StatisticsManager) GetTableStats(schema, table string) *TableStatistics {
	if m.catalog == nil {
		return m.defaultTableStats(nil)
	}

	tableInfo := m.catalog.GetTableInfo(schema, table)
	if tableInfo == nil {
		return m.defaultTableStats(nil)
	}

	stats := tableInfo.GetStatistics()
	if stats == nil {
		return m.defaultTableStats(tableInfo)
	}

	return stats
}

// GetColumnStats returns statistics for a column.
// Returns default statistics if the column has not been analyzed.
func (m *StatisticsManager) GetColumnStats(schema, table, column string) *ColumnStatistics {
	tableStats := m.GetTableStats(schema, table)
	if tableStats == nil {
		return nil
	}

	colStats := tableStats.GetColumnStats(column)
	if colStats != nil {
		return colStats
	}

	// Return default column stats
	if m.catalog != nil {
		tableInfo := m.catalog.GetTableInfo(schema, table)
		if tableInfo != nil {
			if col, ok := tableInfo.GetColumnInfo(column); ok {
				return m.defaultColumnStats(col.GetName(), col.GetType())
			}
		}
	}

	return nil
}

// defaultTableStats returns conservative default statistics for a table.
func (m *StatisticsManager) defaultTableStats(tableInfo TableInfo) *TableStatistics {
	stats := NewTableStatistics()

	if tableInfo != nil {
		// Add default column statistics for each column
		columns := tableInfo.GetColumns()
		stats.Columns = make([]ColumnStatistics, len(columns))
		for i, col := range columns {
			stats.Columns[i] = *m.defaultColumnStats(col.GetName(), col.GetType())
		}
	}

	return stats
}

// defaultColumnStats returns conservative default statistics for a column.
func (m *StatisticsManager) defaultColumnStats(name string, colType dukdb.Type) *ColumnStatistics {
	return NewColumnStatistics(name, colType)
}
