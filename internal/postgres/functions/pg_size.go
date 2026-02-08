package functions

import (
	"fmt"
	"reflect"
	"strconv"
	"time"
)

// PgColumnSize returns the estimated storage size for a value.
// Returns nil when the value is NULL.
type PgColumnSize struct{}

// Evaluate returns the estimated storage size in bytes.
func (*PgColumnSize) Evaluate(value any) *int64 {
	size, ok := sizeFromValue(value)
	if !ok {
		return nil
	}

	return &size
}

// SizeLookup provides optional size lookup functions.
type SizeLookup struct {
	ByName func(name string) (int64, bool)
	ByOID  func(oid uint32) (int64, bool)
}

// PgDatabaseSize returns the size of a database.
// In dukdb-go, we return 0 as we don't track database size.
type PgDatabaseSize struct {
	Lookup SizeLookup
}

// Evaluate returns the database size in bytes.
// In our implementation, this always returns 0.
func (f *PgDatabaseSize) Evaluate(database string) int64 {
	return resolveSizeByName(database, f.Lookup)
}

// EvaluateByOID returns the database size by OID.
func (f *PgDatabaseSize) EvaluateByOID(oid uint32) int64 {
	return resolveSizeByOID(oid, f.Lookup)
}

// PgTableSize returns the size of a table.
type PgTableSize struct {
	Lookup SizeLookup
}

// Evaluate returns the table size in bytes.
func (f *PgTableSize) Evaluate(table string) int64 {
	return resolveSizeByName(table, f.Lookup)
}

// EvaluateByOID returns the table size by OID.
func (f *PgTableSize) EvaluateByOID(oid uint32) int64 {
	return resolveSizeByOID(oid, f.Lookup)
}

// PgRelationSize returns the size of a relation (table, index, etc.).
type PgRelationSize struct {
	Lookup SizeLookup
}

// Evaluate returns the relation size in bytes.
func (f *PgRelationSize) Evaluate(relation string) int64 {
	return resolveSizeByName(relation, f.Lookup)
}

// EvaluateByOID returns the relation size by OID.
func (f *PgRelationSize) EvaluateByOID(oid uint32) int64 {
	return resolveSizeByOID(oid, f.Lookup)
}

// PgTotalRelationSize returns the total size of a relation including indexes.
type PgTotalRelationSize struct {
	Lookup SizeLookup
}

// Evaluate returns the total relation size in bytes.
func (f *PgTotalRelationSize) Evaluate(relation string) int64 {
	return resolveSizeByName(relation, f.Lookup)
}

// EvaluateByOID returns the total relation size by OID.
func (f *PgTotalRelationSize) EvaluateByOID(oid uint32) int64 {
	return resolveSizeByOID(oid, f.Lookup)
}

// PgIndexesSize returns the size of all indexes on a table.
type PgIndexesSize struct {
	Lookup SizeLookup
}

// Evaluate returns the indexes size in bytes.
func (f *PgIndexesSize) Evaluate(table string) int64 {
	return resolveSizeByName(table, f.Lookup)
}

// EvaluateByOID returns the indexes size by OID.
func (f *PgIndexesSize) EvaluateByOID(oid uint32) int64 {
	return resolveSizeByOID(oid, f.Lookup)
}

// PgTablespaceSize returns the size of a tablespace.
type PgTablespaceSize struct {
	Lookup SizeLookup
}

// Evaluate returns the tablespace size in bytes.
func (f *PgTablespaceSize) Evaluate(tablespace string) int64 {
	return resolveSizeByName(tablespace, f.Lookup)
}

// EvaluateByOID returns the tablespace size by OID.
func (f *PgTablespaceSize) EvaluateByOID(oid uint32) int64 {
	return resolveSizeByOID(oid, f.Lookup)
}

// Size unit constants.
const (
	sizeKB = 1024
	sizeMB = sizeKB * 1024
	sizeGB = sizeMB * 1024
	sizeTB = sizeGB * 1024
	sizePB = sizeTB * 1024
)

// PgSizePretty formats a size in bytes as a human-readable string.
type PgSizePretty struct{}

// Evaluate formats size in bytes as human-readable string.
func (*PgSizePretty) Evaluate(size int64) string {
	switch {
	case size >= sizePB:
		return formatSize(float64(size)/float64(sizePB), "PB")
	case size >= sizeTB:
		return formatSize(float64(size)/float64(sizeTB), "TB")
	case size >= sizeGB:
		return formatSize(float64(size)/float64(sizeGB), "GB")
	case size >= sizeMB:
		return formatSize(float64(size)/float64(sizeMB), "MB")
	case size >= sizeKB:
		return formatSize(float64(size)/float64(sizeKB), "kB")
	default:
		return fmt.Sprintf("%d bytes", size)
	}
}

func formatSize(size float64, unit string) string {
	if size == float64(int64(size)) {
		return fmt.Sprintf("%d %s", int64(size), unit)
	}

	return fmt.Sprintf("%.1f %s", size, unit)
}

func resolveSizeByName(name string, lookup SizeLookup) int64 {
	if lookup.ByName == nil {
		return 0
	}
	if size, ok := lookup.ByName(name); ok {
		return size
	}

	return 0
}

func resolveSizeByOID(oid uint32, lookup SizeLookup) int64 {
	if lookup.ByOID == nil {
		return 0
	}
	if size, ok := lookup.ByOID(oid); ok {
		return size
	}

	return 0
}

func sizeFromValue(value any) (int64, bool) {
	val, ok := derefValue(value)
	if !ok {
		return 0, false
	}

	switch v := val.(type) {
	case string:
		return int64(len(v)), true
	case []byte:
		return int64(len(v)), true
	case bool:
		return 1, true
	case int8, uint8:
		return 1, true
	case int16, uint16:
		return 2, true
	case int32, uint32, float32:
		return 4, true
	case int64, uint64, float64:
		return 8, true
	case int:
		return int64(strconv.IntSize / 8), true
	case uint:
		return int64(strconv.IntSize / 8), true
	case time.Time:
		return 8, true
	case time.Duration:
		return 8, true
	case []string:
		return sizeFromStringSlice(v), true
	case []int:
		return int64(len(v)) * int64(strconv.IntSize/8), true
	case []int64:
		return int64(len(v)) * 8, true
	case []int32:
		return int64(len(v)) * 4, true
	case []int16:
		return int64(len(v)) * 2, true
	case []int8:
		return int64(len(v)), true
	case []uint:
		return int64(len(v)) * int64(strconv.IntSize/8), true
	case []uint64:
		return int64(len(v)) * 8, true
	case []uint32:
		return int64(len(v)) * 4, true
	case []uint16:
		return int64(len(v)) * 2, true
	case []float64:
		return int64(len(v)) * 8, true
	case []float32:
		return int64(len(v)) * 4, true
	case []bool:
		return int64(len(v)), true
	default:
		return 0, true
	}
}

func sizeFromStringSlice(values []string) int64 {
	if len(values) == 0 {
		return 0
	}

	var size int64
	for _, v := range values {
		size += int64(len(v))
	}

	return size
}

func derefValue(value any) (any, bool) {
	if value == nil {
		return nil, false
	}

	v := reflect.ValueOf(value)
	if !v.IsValid() {
		return nil, false
	}

	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, false
		}
		return v.Elem().Interface(), true
	}

	return value, true
}
