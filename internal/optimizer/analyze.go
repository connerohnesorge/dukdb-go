package optimizer

import (
	"math/rand"
	"sort"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
)

// SampleThreshold is the row count above which sampling is used.
const SampleThreshold = 100000

// SampleSize is the number of rows to sample for large tables.
const SampleSize = 10000

// maxDistinctTrack is the maximum number of distinct values to track exactly
// before switching to approximation.
const maxDistinctTrack = 10000

// StatisticsCollector collects statistics for tables.
type StatisticsCollector struct {
	histogramBuckets int
}

// NewStatisticsCollector creates a new StatisticsCollector.
func NewStatisticsCollector() *StatisticsCollector {
	return &StatisticsCollector{
		histogramBuckets: DefaultHistogramBuckets,
	}
}

// SetHistogramBuckets sets the number of histogram buckets.
func (c *StatisticsCollector) SetHistogramBuckets(buckets int) {
	if buckets > 0 {
		c.histogramBuckets = buckets
	}
}

// DataReader provides access to column data for statistics collection.
// The reader function returns all values for a column as []any.
type DataReader func(columnIndex int) ([]any, error)

// CollectTableStats collects statistics for a table using a data reader.
// dataReader provides access to table data for statistics collection.
func (c *StatisticsCollector) CollectTableStats(
	columnNames []string,
	columnTypes []dukdb.Type,
	rowCount int64,
	dataReader DataReader,
) (*TableStatistics, error) {
	stats := &TableStatistics{
		RowCount:      rowCount,
		PageCount:     (rowCount + 999) / 1000, // Rough estimate: 1000 rows per page
		DataSizeBytes: rowCount * bytesPerRow,
		LastAnalyzed:  time.Now(),
		SampleRate:    1.0,
		Columns:       make([]ColumnStatistics, len(columnNames)),
	}

	// Determine if we need to sample
	shouldSample := rowCount > SampleThreshold
	if shouldSample {
		stats.SampleRate = float64(SampleSize) / float64(rowCount)
	}

	// Collect statistics for each column
	for i := range columnNames {
		colStats, err := c.collectColumnStats(
			columnNames[i],
			columnTypes[i],
			rowCount,
			shouldSample,
			func() ([]any, error) {
				return dataReader(i)
			},
		)
		if err != nil {
			return nil, err
		}
		stats.Columns[i] = *colStats
	}

	return stats, nil
}

// collectColumnStats collects statistics for a single column.
func (c *StatisticsCollector) collectColumnStats(
	columnName string,
	columnType dukdb.Type,
	rowCount int64,
	shouldSample bool,
	dataReader func() ([]any, error),
) (*ColumnStatistics, error) {
	values, err := dataReader()
	if err != nil {
		return nil, err
	}

	// Sample if necessary using reservoir sampling
	if shouldSample && len(values) > SampleSize {
		values = reservoirSample(values, SampleSize)
	}

	// Collect basic statistics
	colStats := &ColumnStatistics{
		ColumnName:    columnName,
		ColumnType:    columnType,
		NullFraction:  0.0,
		DistinctCount: 0,
		MinValue:      nil,
		MaxValue:      nil,
		AvgWidth:      EstimateTypeWidth(columnType),
		Histogram:     nil,
	}

	if len(values) == 0 {
		return colStats, nil
	}

	// Count nulls and collect non-null values
	var nonNullValues []any
	nullCount := 0
	for _, v := range values {
		if v == nil {
			nullCount++
		} else {
			nonNullValues = append(nonNullValues, v)
		}
	}

	colStats.NullFraction = float64(nullCount) / float64(len(values))

	if len(nonNullValues) == 0 {
		return colStats, nil
	}

	// Calculate distinct count
	colStats.DistinctCount = c.countDistinct(nonNullValues)

	// Scale distinct count for sampled data
	if shouldSample && len(values) > SampleSize {
		// Use linear scaling for distinct count (rough approximation)
		scaleFactor := float64(rowCount) / float64(len(values))
		// Use a conservative scaling - distinct count doesn't scale linearly
		// but we use a dampened formula
		colStats.DistinctCount = int64(float64(colStats.DistinctCount) * scaleFactor * 0.7)
		if colStats.DistinctCount > rowCount {
			colStats.DistinctCount = rowCount
		}
	}

	// Calculate min/max
	colStats.MinValue, colStats.MaxValue = c.findMinMax(nonNullValues, columnType)

	// Build histogram for appropriate types
	if isHistogramType(columnType) && len(nonNullValues) >= c.histogramBuckets {
		colStats.Histogram = c.buildHistogram(nonNullValues, columnType)
	}

	return colStats, nil
}

// reservoirSample implements reservoir sampling for large datasets.
// It returns a sample of size k from the input slice.
func reservoirSample(values []any, k int) []any {
	if len(values) <= k {
		return values
	}

	// Initialize reservoir with first k elements
	reservoir := make([]any, k)
	copy(reservoir, values[:k])

	// Use a deterministic seed for reproducibility
	rng := rand.New(rand.NewSource(42))

	// Process remaining elements
	for i := k; i < len(values); i++ {
		// Generate random index in [0, i]
		j := rng.Intn(i + 1)
		if j < k {
			reservoir[j] = values[i]
		}
	}

	return reservoir
}

// countDistinct counts the number of distinct values.
// For small cardinalities, it uses exact counting with a map.
// For large cardinalities, it returns the map size (capped at maxDistinctTrack).
func (c *StatisticsCollector) countDistinct(values []any) int64 {
	seen := make(map[any]struct{})
	for _, v := range values {
		if v == nil {
			continue
		}
		// Convert to comparable key
		key := toComparableKey(v)
		if len(seen) >= maxDistinctTrack {
			// Stop tracking exactly, return approximate count
			return int64(maxDistinctTrack)
		}
		seen[key] = struct{}{}
	}
	return int64(len(seen))
}

// toComparableKey converts a value to a comparable map key.
// Slices and maps are not comparable, so we convert them to strings.
func toComparableKey(v any) any {
	switch val := v.(type) {
	case []byte:
		return string(val)
	case []any:
		// For arrays, use a string representation
		return anySliceToString(val)
	default:
		return v
	}
}

// anySliceToString converts a slice of any to a string for comparison.
func anySliceToString(slice []any) string {
	result := "["
	for i, v := range slice {
		if i > 0 {
			result += ","
		}
		result += anyToString(v)
	}
	result += "]"
	return result
}

// anyToString converts any value to a string representation.
func anyToString(v any) string {
	if v == nil {
		return "<nil>"
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return "<complex>"
	}
}

// findMinMax finds the minimum and maximum values in a slice.
func (c *StatisticsCollector) findMinMax(values []any, colType dukdb.Type) (min, max any) {
	if len(values) == 0 {
		return nil, nil
	}

	min = values[0]
	max = values[0]

	for _, v := range values[1:] {
		if v == nil {
			continue
		}
		if compareValues(v, min, colType) < 0 {
			min = v
		}
		if compareValues(v, max, colType) > 0 {
			max = v
		}
	}

	return min, max
}

// compareValues compares two values of the same type.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
//
//nolint:gocyclo // Complex switch is necessary for type handling
func compareValues(a, b any, colType dukdb.Type) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	switch colType {
	case dukdb.TYPE_TINYINT:
		av, bv := toInt8Val(a), toInt8Val(b)
		return compareInt(int64(av), int64(bv))
	case dukdb.TYPE_SMALLINT:
		av, bv := toInt16Val(a), toInt16Val(b)
		return compareInt(int64(av), int64(bv))
	case dukdb.TYPE_INTEGER:
		av, bv := toInt32Val(a), toInt32Val(b)
		return compareInt(int64(av), int64(bv))
	case dukdb.TYPE_BIGINT:
		av, bv := toInt64Val(a), toInt64Val(b)
		return compareInt(av, bv)
	case dukdb.TYPE_UTINYINT:
		av, bv := toUint8Val(a), toUint8Val(b)
		return compareUint(uint64(av), uint64(bv))
	case dukdb.TYPE_USMALLINT:
		av, bv := toUint16Val(a), toUint16Val(b)
		return compareUint(uint64(av), uint64(bv))
	case dukdb.TYPE_UINTEGER:
		av, bv := toUint32Val(a), toUint32Val(b)
		return compareUint(uint64(av), uint64(bv))
	case dukdb.TYPE_UBIGINT:
		av, bv := toUint64Val(a), toUint64Val(b)
		return compareUint(av, bv)
	case dukdb.TYPE_FLOAT:
		av, bv := toFloat32Val(a), toFloat32Val(b)
		return compareFloat(float64(av), float64(bv))
	case dukdb.TYPE_DOUBLE:
		av, bv := toFloat64Val(a), toFloat64Val(b)
		return compareFloat(av, bv)
	case dukdb.TYPE_VARCHAR:
		av, bv := toStringVal(a), toStringVal(b)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case dukdb.TYPE_BOOLEAN:
		av, bv := toBoolVal(a), toBoolVal(b)
		if !av && bv {
			return -1
		}
		if av && !bv {
			return 1
		}
		return 0
	default:
		// For other types, use string comparison as fallback
		return 0
	}
}

func compareInt(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func compareUint(a, b uint64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func compareFloat(a, b float64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// Type conversion helpers

func toInt8Val(v any) int8 {
	switch val := v.(type) {
	case int8:
		return val
	case int:
		return int8(val)
	case int64:
		return int8(val)
	default:
		return 0
	}
}

func toInt16Val(v any) int16 {
	switch val := v.(type) {
	case int16:
		return val
	case int:
		return int16(val)
	case int64:
		return int16(val)
	default:
		return 0
	}
}

func toInt32Val(v any) int32 {
	switch val := v.(type) {
	case int32:
		return val
	case int:
		return int32(val)
	case int64:
		return int32(val)
	default:
		return 0
	}
}

func toInt64Val(v any) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case int32:
		return int64(val)
	case int16:
		return int64(val)
	case int8:
		return int64(val)
	default:
		return 0
	}
}

func toUint8Val(v any) uint8 {
	switch val := v.(type) {
	case uint8:
		return val
	case uint:
		return uint8(val)
	case uint64:
		return uint8(val)
	default:
		return 0
	}
}

func toUint16Val(v any) uint16 {
	switch val := v.(type) {
	case uint16:
		return val
	case uint:
		return uint16(val)
	case uint64:
		return uint16(val)
	default:
		return 0
	}
}

func toUint32Val(v any) uint32 {
	switch val := v.(type) {
	case uint32:
		return val
	case uint:
		return uint32(val)
	case uint64:
		return uint32(val)
	default:
		return 0
	}
}

func toUint64Val(v any) uint64 {
	switch val := v.(type) {
	case uint64:
		return val
	case uint:
		return uint64(val)
	default:
		return 0
	}
}

func toFloat32Val(v any) float32 {
	switch val := v.(type) {
	case float32:
		return val
	case float64:
		return float32(val)
	default:
		return 0
	}
}

func toFloat64Val(v any) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	default:
		return 0
	}
}

func toStringVal(v any) string {
	switch val := v.(type) {
	case string:
		return val
	default:
		return ""
	}
}

func toBoolVal(v any) bool {
	switch val := v.(type) {
	case bool:
		return val
	default:
		return false
	}
}

// isHistogramType returns true if the type supports histograms.
func isHistogramType(colType dukdb.Type) bool {
	switch colType {
	case dukdb.TYPE_TINYINT, dukdb.TYPE_SMALLINT, dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT,
		dukdb.TYPE_UTINYINT, dukdb.TYPE_USMALLINT, dukdb.TYPE_UINTEGER, dukdb.TYPE_UBIGINT,
		dukdb.TYPE_FLOAT, dukdb.TYPE_DOUBLE,
		dukdb.TYPE_VARCHAR, dukdb.TYPE_DATE, dukdb.TYPE_TIMESTAMP:
		return true
	default:
		return false
	}
}

// buildHistogram builds an equi-depth histogram for the values.
func (c *StatisticsCollector) buildHistogram(values []any, colType dukdb.Type) *Histogram {
	if len(values) == 0 {
		return nil
	}

	// Sort values for equi-depth histogram
	sortedValues := make([]any, len(values))
	copy(sortedValues, values)
	sortValues(sortedValues, colType)

	numBuckets := c.histogramBuckets
	if numBuckets > len(sortedValues) {
		numBuckets = len(sortedValues)
	}

	// Calculate bucket size (equi-depth means each bucket has ~equal number of rows)
	bucketSize := len(sortedValues) / numBuckets
	if bucketSize < 1 {
		bucketSize = 1
	}

	histogram := &Histogram{
		NumBuckets: numBuckets,
		Buckets:    make([]Bucket, 0, numBuckets),
	}

	bucketStart := 0
	for i := 0; i < numBuckets && bucketStart < len(sortedValues); i++ {
		bucketEnd := bucketStart + bucketSize
		if i == numBuckets-1 || bucketEnd > len(sortedValues) {
			bucketEnd = len(sortedValues)
		}

		// Get bucket bounds
		lowerBound := sortedValues[bucketStart]
		upperBound := sortedValues[bucketEnd-1]

		// Count distinct values in bucket
		distinctInBucket := countDistinctInRange(sortedValues[bucketStart:bucketEnd])

		bucket := Bucket{
			LowerBound:    lowerBound,
			UpperBound:    upperBound,
			Frequency:     float64(bucketEnd-bucketStart) / float64(len(sortedValues)),
			DistinctCount: int64(distinctInBucket),
		}
		histogram.Buckets = append(histogram.Buckets, bucket)

		bucketStart = bucketEnd
	}

	return histogram
}

// sortValues sorts values in place based on their type.
func sortValues(values []any, colType dukdb.Type) {
	sort.Slice(values, func(i, j int) bool {
		return compareValues(values[i], values[j], colType) < 0
	})
}

// countDistinctInRange counts distinct values in a slice.
func countDistinctInRange(values []any) int {
	seen := make(map[any]struct{})
	for _, v := range values {
		if v != nil {
			key := toComparableKey(v)
			seen[key] = struct{}{}
		}
	}
	return len(seen)
}
