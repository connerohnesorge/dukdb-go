// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file implements partition specification parsing and transform functions.
package iceberg

import (
	"fmt"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/twmb/murmur3"
)

// PartitionTransform represents an Iceberg partition transform.
type PartitionTransform interface {
	// Name returns the transform name (e.g., "identity", "bucket[16]", "year").
	Name() string
	// Apply applies the transform to a value and returns the partition value.
	Apply(value any) (any, error)
}

// IdentityTransform is the identity partition transform.
type IdentityTransform struct{}

// Name returns "identity".
func (t IdentityTransform) Name() string { return "identity" }

// Apply returns the value unchanged.
func (t IdentityTransform) Apply(value any) (any, error) {
	return value, nil
}

// BucketTransform is the bucket partition transform.
type BucketTransform struct {
	NumBuckets int
}

// Name returns "bucket[N]".
func (t BucketTransform) Name() string {
	return fmt.Sprintf("bucket[%d]", t.NumBuckets)
}

// Apply computes the bucket number for the value using murmur3 hash.
func (t BucketTransform) Apply(value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	hash, err := t.hash(value)
	if err != nil {
		return nil, err
	}

	bucket := (hash & 0x7FFFFFFF) % uint32(t.NumBuckets)

	return int(bucket), nil
}

// hash computes the murmur3 hash for the value.
func (t BucketTransform) hash(value any) (uint32, error) {
	var data []byte

	switch v := value.(type) {
	case int:
		data = int32ToBytes(int32(v))
	case int32:
		data = int32ToBytes(v)
	case int64:
		data = int64ToBytes(v)
	case string:
		data = []byte(v)
	case []byte:
		data = v
	default:
		return 0, fmt.Errorf("%w: bucket transform not supported for type %T", ErrInvalidPartitionTransform, value)
	}

	return murmur3.Sum32(data), nil
}

// TruncateTransform is the truncate partition transform.
type TruncateTransform struct {
	Width int
}

// Name returns "truncate[W]".
func (t TruncateTransform) Name() string {
	return fmt.Sprintf("truncate[%d]", t.Width)
}

// Apply truncates the value to the specified width.
func (t TruncateTransform) Apply(value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	switch v := value.(type) {
	case int:
		return (v / t.Width) * t.Width, nil
	case int32:
		return (v / int32(t.Width)) * int32(t.Width), nil
	case int64:
		return (v / int64(t.Width)) * int64(t.Width), nil
	case string:
		if len(v) <= t.Width {
			return v, nil
		}

		return v[:t.Width], nil
	default:
		return nil, fmt.Errorf("%w: truncate transform not supported for type %T", ErrInvalidPartitionTransform, value)
	}
}

// YearTransform is the year partition transform for dates/timestamps.
type YearTransform struct{}

// Name returns "year".
func (t YearTransform) Name() string { return "year" }

// Apply extracts the year from a date/timestamp value.
// Returns years since 1970 (epoch).
func (t YearTransform) Apply(value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	tm, err := t.toTime(value)
	if err != nil {
		return nil, err
	}

	return tm.Year() - 1970, nil
}

func (t YearTransform) toTime(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case int32:
		// Days since epoch (for date type)
		return time.Unix(int64(v)*86400, 0).UTC(), nil
	case int64:
		// Microseconds since epoch (for timestamp type)
		return time.UnixMicro(v).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("%w: year transform not supported for type %T", ErrInvalidPartitionTransform, value)
	}
}

// MonthTransform is the month partition transform for dates/timestamps.
type MonthTransform struct{}

// Name returns "month".
func (t MonthTransform) Name() string { return "month" }

// Apply extracts the month from a date/timestamp value.
// Returns months since 1970-01 (epoch).
func (t MonthTransform) Apply(value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	tm, err := t.toTime(value)
	if err != nil {
		return nil, err
	}

	yearsSince1970 := tm.Year() - 1970
	monthsSince1970 := yearsSince1970*12 + int(tm.Month()) - 1

	return monthsSince1970, nil
}

func (t MonthTransform) toTime(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case int32:
		return time.Unix(int64(v)*86400, 0).UTC(), nil
	case int64:
		return time.UnixMicro(v).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("%w: month transform not supported for type %T", ErrInvalidPartitionTransform, value)
	}
}

// DayTransform is the day partition transform for dates/timestamps.
type DayTransform struct{}

// Name returns "day".
func (t DayTransform) Name() string { return "day" }

// Apply extracts the day from a date/timestamp value.
// Returns days since 1970-01-01 (epoch).
func (t DayTransform) Apply(value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	switch v := value.(type) {
	case time.Time:
		// Calculate days since epoch
		return int(v.Unix() / 86400), nil
	case int32:
		// Already days since epoch
		return int(v), nil
	case int64:
		// Microseconds since epoch
		return int(v / 86400_000_000), nil
	default:
		return nil, fmt.Errorf("%w: day transform not supported for type %T", ErrInvalidPartitionTransform, value)
	}
}

// HourTransform is the hour partition transform for timestamps.
type HourTransform struct{}

// Name returns "hour".
func (t HourTransform) Name() string { return "hour" }

// Apply extracts the hour from a timestamp value.
// Returns hours since 1970-01-01 00:00:00 (epoch).
func (t HourTransform) Apply(value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	switch v := value.(type) {
	case time.Time:
		return int(v.Unix() / 3600), nil
	case int64:
		// Microseconds since epoch
		return int(v / 3600_000_000), nil
	default:
		return nil, fmt.Errorf("%w: hour transform not supported for type %T", ErrInvalidPartitionTransform, value)
	}
}

// VoidTransform is the void partition transform that produces no partition value.
type VoidTransform struct{}

// Name returns "void".
func (t VoidTransform) Name() string { return "void" }

// Apply always returns nil.
func (t VoidTransform) Apply(value any) (any, error) {
	return nil, nil
}

// ParseTransform creates a PartitionTransform from an iceberg.Transform.
func ParseTransform(transform iceberg.Transform) (PartitionTransform, error) {
	switch t := transform.(type) {
	case iceberg.IdentityTransform:
		return IdentityTransform{}, nil
	case iceberg.BucketTransform:
		return BucketTransform{NumBuckets: t.NumBuckets}, nil
	case iceberg.TruncateTransform:
		return TruncateTransform{Width: t.Width}, nil
	case iceberg.YearTransform:
		return YearTransform{}, nil
	case iceberg.MonthTransform:
		return MonthTransform{}, nil
	case iceberg.DayTransform:
		return DayTransform{}, nil
	case iceberg.HourTransform:
		return HourTransform{}, nil
	case iceberg.VoidTransform:
		return VoidTransform{}, nil
	default:
		return nil, fmt.Errorf("%w: unknown transform type %T", ErrInvalidPartitionTransform, transform)
	}
}

// PartitionField represents a field in a partition spec.
type PartitionField struct {
	// FieldID is the partition field ID.
	FieldID int
	// SourceID is the source column ID in the schema.
	SourceID int
	// Name is the partition field name.
	Name string
	// Transform is the partition transform to apply.
	Transform PartitionTransform
}

// PartitionSpec wraps an Iceberg partition spec with convenience methods.
type PartitionSpec struct {
	// ID is the partition spec ID.
	ID int
	// Fields contains the partition fields.
	Fields []PartitionField

	// raw holds the underlying iceberg-go partition spec.
	raw iceberg.PartitionSpec
}

// NewPartitionSpec creates a PartitionSpec from an iceberg-go PartitionSpec.
func NewPartitionSpec(spec iceberg.PartitionSpec) (*PartitionSpec, error) {
	ps := &PartitionSpec{
		ID:     spec.ID(),
		Fields: make([]PartitionField, 0),
		raw:    spec,
	}

	for field := range spec.Fields() {
		transform, err := ParseTransform(field.Transform)
		if err != nil {
			return nil, err
		}

		ps.Fields = append(ps.Fields, PartitionField{
			FieldID:   field.FieldID,
			SourceID:  field.SourceID,
			Name:      field.Name,
			Transform: transform,
		})
	}

	return ps, nil
}

// Raw returns the underlying iceberg-go partition spec.
func (ps *PartitionSpec) Raw() iceberg.PartitionSpec {
	return ps.raw
}

// IsUnpartitioned returns true if this is an unpartitioned spec.
func (ps *PartitionSpec) IsUnpartitioned() bool {
	return len(ps.Fields) == 0
}

// ComputePartitionValues computes partition values for a row given source column values.
// The values map should contain source column IDs mapped to their values.
func (ps *PartitionSpec) ComputePartitionValues(values map[int]any) (map[string]any, error) {
	result := make(map[string]any)

	for _, field := range ps.Fields {
		sourceValue, ok := values[field.SourceID]
		if !ok {
			result[field.Name] = nil
			continue
		}

		partValue, err := field.Transform.Apply(sourceValue)
		if err != nil {
			return nil, fmt.Errorf("failed to apply transform for field %s: %w", field.Name, err)
		}

		result[field.Name] = partValue
	}

	return result, nil
}

// PartitionEvaluator evaluates partition predicates for pruning.
type PartitionEvaluator struct {
	spec *PartitionSpec
}

// NewPartitionEvaluator creates a new PartitionEvaluator for the given spec.
func NewPartitionEvaluator(spec *PartitionSpec) *PartitionEvaluator {
	return &PartitionEvaluator{spec: spec}
}

// EvaluateEquality evaluates an equality predicate against partition values.
// Returns true if the partition may contain matching data.
func (e *PartitionEvaluator) EvaluateEquality(fieldName string, value any) (bool, error) {
	// Find the partition field
	var field *PartitionField
	for i := range e.spec.Fields {
		if e.spec.Fields[i].Name == fieldName {
			field = &e.spec.Fields[i]
			break
		}
	}

	if field == nil {
		// Field is not a partition column, cannot prune
		return true, nil
	}

	// Compute the partition value for the filter value
	partValue, err := field.Transform.Apply(value)
	if err != nil {
		return true, nil // Cannot evaluate, don't prune
	}

	// This would be compared against manifest partition summaries
	// For now, return true (no pruning)
	_ = partValue

	return true, nil
}

// Helper functions for byte conversion

func int32ToBytes(v int32) []byte {
	return []byte{
		byte(v),
		byte(v >> 8),
		byte(v >> 16),
		byte(v >> 24),
	}
}

func int64ToBytes(v int64) []byte {
	return []byte{
		byte(v),
		byte(v >> 8),
		byte(v >> 16),
		byte(v >> 24),
		byte(v >> 32),
		byte(v >> 40),
		byte(v >> 48),
		byte(v >> 56),
	}
}
