// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file implements filter pushdown for partition and column statistics pruning.
package iceberg

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

// PartitionPredicate represents a predicate for partition pruning.
type PartitionPredicate interface {
	// Evaluate evaluates the predicate against partition values.
	// Returns true if the partition may contain matching data.
	Evaluate(partitionData map[string]any) bool
}

// EqualityPartitionPredicate is a partition predicate for equality checks.
type EqualityPartitionPredicate struct {
	FieldName string
	Value     any
}

// Evaluate checks if the partition value equals the predicate value.
func (p *EqualityPartitionPredicate) Evaluate(partitionData map[string]any) bool {
	if partitionData == nil {
		return true // No partition info, can't prune
	}

	partValue, ok := partitionData[p.FieldName]
	if !ok {
		return true // Field not in partition, can't prune
	}

	return compareValues(partValue, p.Value) == 0
}

// RangePartitionPredicate is a partition predicate for range checks.
type RangePartitionPredicate struct {
	FieldName string
	LowerBound any
	UpperBound any
	LowerInclusive bool
	UpperInclusive bool
}

// Evaluate checks if the partition value falls within the range.
func (p *RangePartitionPredicate) Evaluate(partitionData map[string]any) bool {
	if partitionData == nil {
		return true
	}

	partValue, ok := partitionData[p.FieldName]
	if !ok {
		return true
	}

	// Check lower bound
	if p.LowerBound != nil {
		cmp := compareValues(partValue, p.LowerBound)
		if p.LowerInclusive && cmp < 0 {
			return false
		}
		if !p.LowerInclusive && cmp <= 0 {
			return false
		}
	}

	// Check upper bound
	if p.UpperBound != nil {
		cmp := compareValues(partValue, p.UpperBound)
		if p.UpperInclusive && cmp > 0 {
			return false
		}
		if !p.UpperInclusive && cmp >= 0 {
			return false
		}
	}

	return true
}

// InPartitionPredicate is a partition predicate for IN list checks.
type InPartitionPredicate struct {
	FieldName string
	Values    []any
}

// Evaluate checks if the partition value is in the list.
func (p *InPartitionPredicate) Evaluate(partitionData map[string]any) bool {
	if partitionData == nil {
		return true
	}

	partValue, ok := partitionData[p.FieldName]
	if !ok {
		return true
	}

	for _, v := range p.Values {
		if compareValues(partValue, v) == 0 {
			return true
		}
	}

	return false
}

// NotNullPartitionPredicate is a partition predicate for NOT NULL checks.
type NotNullPartitionPredicate struct {
	FieldName string
}

// Evaluate checks if the partition value is not null.
func (p *NotNullPartitionPredicate) Evaluate(partitionData map[string]any) bool {
	if partitionData == nil {
		return true
	}

	partValue, ok := partitionData[p.FieldName]
	if !ok {
		return true
	}

	return partValue != nil
}

// ColumnStatsPredicate represents a predicate for column statistics pruning.
type ColumnStatsPredicate interface {
	// CanPrune returns true if the file can be pruned based on column statistics.
	CanPrune(file *DataFile, columnID int) bool
}

// EqualityStatsPredicate prunes files where min > value or max < value.
type EqualityStatsPredicate struct {
	Value any
}

// CanPrune returns true if the file definitely doesn't contain the value.
func (p *EqualityStatsPredicate) CanPrune(file *DataFile, columnID int) bool {
	if file.LowerBounds == nil || file.UpperBounds == nil {
		return false // Can't prune without stats
	}

	lowerBytes, hasLower := file.LowerBounds[columnID]
	upperBytes, hasUpper := file.UpperBounds[columnID]

	if !hasLower || !hasUpper {
		return false
	}

	// Decode bounds and compare
	// Note: This is a simplified implementation. Full implementation would
	// need to know the column type for proper decoding.
	lowerValue := decodeBoundValue(lowerBytes)
	upperValue := decodeBoundValue(upperBytes)

	if lowerValue == nil || upperValue == nil {
		return false
	}

	// If value < min or value > max, we can prune
	if compareValues(p.Value, lowerValue) < 0 {
		return true
	}
	if compareValues(p.Value, upperValue) > 0 {
		return true
	}

	return false
}

// RangeStatsPredicate prunes files based on range overlap.
type RangeStatsPredicate struct {
	LowerBound     any
	UpperBound     any
	LowerInclusive bool
	UpperInclusive bool
}

// CanPrune returns true if the file's range doesn't overlap with the predicate range.
func (p *RangeStatsPredicate) CanPrune(file *DataFile, columnID int) bool {
	if file.LowerBounds == nil || file.UpperBounds == nil {
		return false
	}

	lowerBytes, hasLower := file.LowerBounds[columnID]
	upperBytes, hasUpper := file.UpperBounds[columnID]

	if !hasLower || !hasUpper {
		return false
	}

	fileLower := decodeBoundValue(lowerBytes)
	fileUpper := decodeBoundValue(upperBytes)

	if fileLower == nil || fileUpper == nil {
		return false
	}

	// Check if ranges don't overlap
	// File range: [fileLower, fileUpper]
	// Predicate range: [LowerBound, UpperBound]

	// If file max < predicate min, no overlap
	if p.LowerBound != nil {
		cmp := compareValues(fileUpper, p.LowerBound)
		if p.LowerInclusive && cmp < 0 {
			return true
		}
		if !p.LowerInclusive && cmp <= 0 {
			return true
		}
	}

	// If file min > predicate max, no overlap
	if p.UpperBound != nil {
		cmp := compareValues(fileLower, p.UpperBound)
		if p.UpperInclusive && cmp > 0 {
			return true
		}
		if !p.UpperInclusive && cmp >= 0 {
			return true
		}
	}

	return false
}

// NotNullStatsPredicate prunes files where all values are null.
type NotNullStatsPredicate struct{}

// CanPrune returns true if all values in the column are null.
func (p *NotNullStatsPredicate) CanPrune(file *DataFile, columnID int) bool {
	if file.NullValueCounts == nil || file.ValueCounts == nil {
		return false
	}

	nullCount, hasNull := file.NullValueCounts[columnID]
	valueCount, hasValue := file.ValueCounts[columnID]

	if !hasNull || !hasValue {
		return false
	}

	// If all values are null, prune the file
	return nullCount == valueCount && valueCount > 0
}

// IsNullStatsPredicate prunes files where no values are null.
type IsNullStatsPredicate struct{}

// CanPrune returns true if no values in the column are null.
func (p *IsNullStatsPredicate) CanPrune(file *DataFile, columnID int) bool {
	if file.NullValueCounts == nil {
		return false
	}

	nullCount, hasNull := file.NullValueCounts[columnID]
	if !hasNull {
		return false
	}

	// If no nulls exist, prune the file
	return nullCount == 0
}

// decodeBoundValue decodes a byte array to a comparable value.
// This is a simplified implementation that handles common cases.
// Full implementation would need type information from the schema.
func decodeBoundValue(data []byte) any {
	if len(data) == 0 {
		return nil
	}

	// Try to decode as different types based on length
	switch len(data) {
	case 1:
		return int(data[0])
	case 4:
		// Could be int32 or float32
		return int32(binary.LittleEndian.Uint32(data))
	case 8:
		// Could be int64, float64, or date/timestamp
		return int64(binary.LittleEndian.Uint64(data))
	default:
		// Assume string
		return string(data)
	}
}

// ColumnStatsFilter evaluates column statistics filters against a data file.
type ColumnStatsFilter struct {
	predicates map[int][]ColumnStatsPredicate
}

// NewColumnStatsFilter creates a new ColumnStatsFilter.
func NewColumnStatsFilter() *ColumnStatsFilter {
	return &ColumnStatsFilter{
		predicates: make(map[int][]ColumnStatsPredicate),
	}
}

// AddPredicate adds a predicate for a column.
func (f *ColumnStatsFilter) AddPredicate(columnID int, predicate ColumnStatsPredicate) {
	f.predicates[columnID] = append(f.predicates[columnID], predicate)
}

// CanPrune returns true if the file can be pruned based on all predicates.
func (f *ColumnStatsFilter) CanPrune(file *DataFile) bool {
	for columnID, preds := range f.predicates {
		for _, pred := range preds {
			if pred.CanPrune(file, columnID) {
				return true
			}
		}
	}
	return false
}

// FilesMatchingFilter returns files that might match the filter predicates.
func (f *ColumnStatsFilter) FilesMatchingFilter(files []*DataFile) []*DataFile {
	result := make([]*DataFile, 0, len(files))
	for _, file := range files {
		if !f.CanPrune(file) {
			result = append(result, file)
		}
	}
	return result
}

// ManifestPartitionFilter filters manifests based on partition information.
type ManifestPartitionFilter struct {
	predicates []PartitionPredicate
}

// NewManifestPartitionFilter creates a new ManifestPartitionFilter.
func NewManifestPartitionFilter() *ManifestPartitionFilter {
	return &ManifestPartitionFilter{
		predicates: []PartitionPredicate{},
	}
}

// AddPredicate adds a partition predicate.
func (f *ManifestPartitionFilter) AddPredicate(predicate PartitionPredicate) {
	f.predicates = append(f.predicates, predicate)
}

// MayMatch returns true if the manifest may contain matching data.
// This implements the PartitionFilter interface.
func (f *ManifestPartitionFilter) MayMatch(manifest *ManifestFile) bool {
	// Without partition summary in manifest, we can't prune at manifest level
	// This would require reading manifest-level partition bounds
	return true
}

// FilterDataFiles filters data files based on partition predicates.
func (f *ManifestPartitionFilter) FilterDataFiles(files []*DataFile) []*DataFile {
	if len(f.predicates) == 0 {
		return files
	}

	result := make([]*DataFile, 0, len(files))
	for _, file := range files {
		if f.fileMatches(file) {
			result = append(result, file)
		}
	}
	return result
}

// fileMatches checks if a file's partition data matches all predicates.
func (f *ManifestPartitionFilter) fileMatches(file *DataFile) bool {
	for _, pred := range f.predicates {
		if !pred.Evaluate(file.PartitionData) {
			return false
		}
	}
	return true
}

// BuildPartitionFilter creates a partition filter from filter expressions.
func BuildPartitionFilter(exprs []PartitionFilterExpr) *ManifestPartitionFilter {
	filter := NewManifestPartitionFilter()

	for _, expr := range exprs {
		switch expr.Operator {
		case "=", "==":
			filter.AddPredicate(&EqualityPartitionPredicate{
				FieldName: expr.FieldName,
				Value:     expr.Value,
			})
		case "IN":
			if values, ok := expr.Value.([]any); ok {
				filter.AddPredicate(&InPartitionPredicate{
					FieldName: expr.FieldName,
					Values:    values,
				})
			}
		case "<":
			filter.AddPredicate(&RangePartitionPredicate{
				FieldName:      expr.FieldName,
				UpperBound:     expr.Value,
				UpperInclusive: false,
			})
		case "<=":
			filter.AddPredicate(&RangePartitionPredicate{
				FieldName:      expr.FieldName,
				UpperBound:     expr.Value,
				UpperInclusive: true,
			})
		case ">":
			filter.AddPredicate(&RangePartitionPredicate{
				FieldName:      expr.FieldName,
				LowerBound:     expr.Value,
				LowerInclusive: false,
			})
		case ">=":
			filter.AddPredicate(&RangePartitionPredicate{
				FieldName:      expr.FieldName,
				LowerBound:     expr.Value,
				LowerInclusive: true,
			})
		case "IS NOT NULL":
			filter.AddPredicate(&NotNullPartitionPredicate{
				FieldName: expr.FieldName,
			})
		}
	}

	return filter
}

// BuildColumnStatsFilter creates a column stats filter from filter expressions.
func BuildColumnStatsFilter(exprs []ColumnFilterExpr, schema any) *ColumnStatsFilter {
	filter := NewColumnStatsFilter()
	// Column ID resolution would require schema access
	// This is a placeholder for the full implementation
	return filter
}

// BinaryBoundDecoder provides type-aware decoding of binary bounds.
type BinaryBoundDecoder struct{}

// DecodeInt32 decodes a 4-byte little-endian int32.
func (d *BinaryBoundDecoder) DecodeInt32(data []byte) (int32, error) {
	if len(data) != 4 {
		return 0, fmt.Errorf("invalid int32 bound: expected 4 bytes, got %d", len(data))
	}
	return int32(binary.LittleEndian.Uint32(data)), nil
}

// DecodeInt64 decodes an 8-byte little-endian int64.
func (d *BinaryBoundDecoder) DecodeInt64(data []byte) (int64, error) {
	if len(data) != 8 {
		return 0, fmt.Errorf("invalid int64 bound: expected 8 bytes, got %d", len(data))
	}
	return int64(binary.LittleEndian.Uint64(data)), nil
}

// DecodeFloat32 decodes a 4-byte little-endian float32.
func (d *BinaryBoundDecoder) DecodeFloat32(data []byte) (float32, error) {
	if len(data) != 4 {
		return 0, fmt.Errorf("invalid float32 bound: expected 4 bytes, got %d", len(data))
	}
	bits := binary.LittleEndian.Uint32(data)
	return math.Float32frombits(bits), nil
}

// DecodeFloat64 decodes an 8-byte little-endian float64.
func (d *BinaryBoundDecoder) DecodeFloat64(data []byte) (float64, error) {
	if len(data) != 8 {
		return 0, fmt.Errorf("invalid float64 bound: expected 8 bytes, got %d", len(data))
	}
	bits := binary.LittleEndian.Uint64(data)
	return math.Float64frombits(bits), nil
}

// DecodeString decodes a UTF-8 string.
func (d *BinaryBoundDecoder) DecodeString(data []byte) string {
	return string(data)
}

// DecodeBinary returns the raw bytes.
func (d *BinaryBoundDecoder) DecodeBinary(data []byte) []byte {
	result := make([]byte, len(data))
	copy(result, data)
	return result
}

// DecodeDate decodes an int32 representing days since epoch.
func (d *BinaryBoundDecoder) DecodeDate(data []byte) (int32, error) {
	return d.DecodeInt32(data)
}

// DecodeTimestamp decodes an int64 representing microseconds since epoch.
func (d *BinaryBoundDecoder) DecodeTimestamp(data []byte) (int64, error) {
	return d.DecodeInt64(data)
}

// CompareBounds compares two binary bounds.
func CompareBounds(a, b []byte) int {
	return bytes.Compare(a, b)
}
