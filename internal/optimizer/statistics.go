// Package optimizer provides cost-based query optimization for dukdb-go.
// It includes statistics infrastructure, cardinality estimation, cost modeling,
// and join order optimization to improve query execution performance.
package optimizer

import (
	"time"

	dukdb "github.com/dukdb/dukdb-go"
)

// Type width constants in bytes for cost estimation.
const (
	widthByte    int32 = 1
	widthInt16   int32 = 2
	widthInt32   int32 = 4
	widthInt64   int32 = 8
	widthInt128  int32 = 16
	widthVarchar int32 = 32
	widthList    int32 = 40
	widthComplex int32 = 64
	widthDefault int32 = 8
	bytesPerRow  int64 = 100
)

// TableStatistics captures table-level statistics for cost-based optimization.
// Statistics are the foundation of cost-based optimization and must capture
// enough information for accurate cardinality estimation while remaining
// efficient to store and query.
type TableStatistics struct {
	RowCount      int64              // Total rows in table
	PageCount     int64              // Storage pages used
	DataSizeBytes int64              // Total data size in bytes
	LastAnalyzed  time.Time          // When stats were collected
	SampleRate    float64            // Sample rate used (1.0 = full scan)
	Columns       []ColumnStatistics // Per-column stats
}

// ColumnStatistics captures column-level statistics for cardinality estimation.
// Column statistics are essential for selectivity estimation of predicates.
type ColumnStatistics struct {
	ColumnName    string     // Name of the column
	ColumnType    dukdb.Type // DuckDB type of the column
	NullFraction  float64    // Fraction of NULL values (0.0-1.0)
	DistinctCount int64      // Estimated distinct values
	MinValue      any        // Minimum value (type-specific)
	MaxValue      any        // Maximum value (type-specific)
	AvgWidth      int32      // Average value width in bytes
	Histogram     *Histogram // Optional equi-depth histogram
}

// Histogram represents an equi-depth histogram for selectivity estimation.
// Histograms provide accurate selectivity for value distributions,
// especially for skewed data.
type Histogram struct {
	NumBuckets int      // Number of buckets (default 100)
	Buckets    []Bucket // Bucket boundaries and counts
}

// Bucket represents a single bucket in an equi-depth histogram.
// Each bucket contains a fraction of the total values in the column.
type Bucket struct {
	LowerBound    any     // Lower bound (inclusive)
	UpperBound    any     // Upper bound (exclusive for non-last)
	Frequency     float64 // Fraction of values in bucket
	DistinctCount int64   // Distinct values in bucket
}

// DefaultRowCount is the assumed row count when no statistics are available.
const DefaultRowCount = 1000

// DefaultPageCount is the assumed page count when no statistics are available.
const DefaultPageCount = 10

// DefaultDistinctCount is the assumed distinct count when no statistics are available.
const DefaultDistinctCount = 100

// DefaultSelectivity is used when no statistics or specific rules apply.
// This value aligns with DuckDB's proven defaults.
const DefaultSelectivity = 0.2

// DefaultHistogramBuckets is the default number of buckets for histograms.
const DefaultHistogramBuckets = 100

// NewTableStatistics creates a new TableStatistics with sensible defaults.
// The returned statistics represent a table with unknown characteristics,
// using conservative assumptions that work reasonably well in practice.
func NewTableStatistics() *TableStatistics {
	return &TableStatistics{
		RowCount:      DefaultRowCount,
		PageCount:     DefaultPageCount,
		DataSizeBytes: DefaultRowCount * bytesPerRow,
		LastAnalyzed:  time.Time{}, // Zero time indicates never analyzed
		SampleRate:    0.0,         // 0 indicates no sampling done
		Columns:       nil,         // No column statistics by default
	}
}

// GetColumnStats returns the statistics for a column by name.
// Returns nil if no statistics exist for the specified column.
func (ts *TableStatistics) GetColumnStats(columnName string) *ColumnStatistics {
	if ts == nil || ts.Columns == nil {
		return nil
	}
	for i := range ts.Columns {
		if ts.Columns[i].ColumnName == columnName {
			return &ts.Columns[i]
		}
	}

	return nil
}

// HasColumnStats returns true if statistics exist for the specified column.
func (ts *TableStatistics) HasColumnStats(columnName string) bool {
	return ts.GetColumnStats(columnName) != nil
}

// IsAnalyzed returns true if the table has been analyzed.
func (ts *TableStatistics) IsAnalyzed() bool {
	return ts != nil && !ts.LastAnalyzed.IsZero()
}

// NewColumnStatistics creates a new ColumnStatistics with default values.
func NewColumnStatistics(name string, colType dukdb.Type) *ColumnStatistics {
	return &ColumnStatistics{
		ColumnName:    name,
		ColumnType:    colType,
		NullFraction:  0.0,
		DistinctCount: DefaultDistinctCount,
		MinValue:      nil,
		MaxValue:      nil,
		AvgWidth:      EstimateTypeWidth(colType),
		Histogram:     nil,
	}
}

// NewHistogram creates a new Histogram with the specified number of buckets.
func NewHistogram(buckets int) *Histogram {
	numBuckets := buckets
	if numBuckets <= 0 {
		numBuckets = DefaultHistogramBuckets
	}

	return &Histogram{
		NumBuckets: numBuckets,
		Buckets:    make([]Bucket, 0, numBuckets),
	}
}

// EstimateTypeWidth returns the estimated width in bytes for a DuckDB type.
// This is used for memory and I/O cost estimation.
//
//nolint:exhaustive // Default case handles all remaining types
func EstimateTypeWidth(t dukdb.Type) int32 {
	return estimateTypeWidthNumeric(t)
}

// estimateTypeWidthNumeric handles numeric and fixed-size types.
//
//nolint:exhaustive // Default case delegates to estimateTypeWidthVariable
func estimateTypeWidthNumeric(t dukdb.Type) int32 {
	switch t {
	case dukdb.TYPE_BOOLEAN, dukdb.TYPE_TINYINT, dukdb.TYPE_UTINYINT:
		return widthByte
	case dukdb.TYPE_SMALLINT, dukdb.TYPE_USMALLINT:
		return widthInt16
	case dukdb.TYPE_INTEGER, dukdb.TYPE_UINTEGER, dukdb.TYPE_FLOAT, dukdb.TYPE_DATE:
		return widthInt32
	case dukdb.TYPE_BIGINT, dukdb.TYPE_UBIGINT, dukdb.TYPE_DOUBLE,
		dukdb.TYPE_TIME, dukdb.TYPE_TIME_TZ,
		dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP_S, dukdb.TYPE_TIMESTAMP_MS,
		dukdb.TYPE_TIMESTAMP_NS, dukdb.TYPE_TIMESTAMP_TZ:
		return widthInt64
	case dukdb.TYPE_HUGEINT, dukdb.TYPE_UHUGEINT, dukdb.TYPE_UUID,
		dukdb.TYPE_INTERVAL, dukdb.TYPE_DECIMAL:
		return widthInt128
	default:
		return estimateTypeWidthVariable(t)
	}
}

// estimateTypeWidthVariable handles variable-size and complex types.
//
//nolint:exhaustive // Default case handles all remaining/unknown types
func estimateTypeWidthVariable(t dukdb.Type) int32 {
	switch t {
	case dukdb.TYPE_VARCHAR, dukdb.TYPE_BLOB:
		return widthVarchar
	case dukdb.TYPE_LIST:
		return widthList
	case dukdb.TYPE_STRUCT, dukdb.TYPE_MAP, dukdb.TYPE_JSON:
		return widthComplex
	default:
		return widthDefault
	}
}
