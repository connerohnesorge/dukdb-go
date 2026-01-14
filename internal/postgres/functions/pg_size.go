package functions

import "fmt"

// PgDatabaseSize returns the size of a database.
// In dukdb-go, we return 0 as we don't track database size.
type PgDatabaseSize struct{}

// Evaluate returns the database size in bytes.
// In our implementation, this always returns 0.
func (*PgDatabaseSize) Evaluate(database string) int64 {
	_ = database // unused
	// TODO: Could potentially sum up file sizes on disk

	return 0
}

// EvaluateByOID returns the database size by OID.
func (*PgDatabaseSize) EvaluateByOID(oid uint32) int64 {
	_ = oid // unused

	return 0
}

// PgTableSize returns the size of a table.
type PgTableSize struct{}

// Evaluate returns the table size in bytes.
func (*PgTableSize) Evaluate(table string) int64 {
	_ = table // unused

	return 0
}

// EvaluateByOID returns the table size by OID.
func (*PgTableSize) EvaluateByOID(oid uint32) int64 {
	_ = oid // unused

	return 0
}

// PgRelationSize returns the size of a relation (table, index, etc.).
type PgRelationSize struct{}

// Evaluate returns the relation size in bytes.
func (*PgRelationSize) Evaluate(relation string) int64 {
	_ = relation // unused

	return 0
}

// EvaluateByOID returns the relation size by OID.
func (*PgRelationSize) EvaluateByOID(oid uint32) int64 {
	_ = oid // unused

	return 0
}

// PgTotalRelationSize returns the total size of a relation including indexes.
type PgTotalRelationSize struct{}

// Evaluate returns the total relation size in bytes.
func (*PgTotalRelationSize) Evaluate(relation string) int64 {
	_ = relation // unused

	return 0
}

// EvaluateByOID returns the total relation size by OID.
func (*PgTotalRelationSize) EvaluateByOID(oid uint32) int64 {
	_ = oid // unused

	return 0
}

// PgIndexesSize returns the size of all indexes on a table.
type PgIndexesSize struct{}

// Evaluate returns the indexes size in bytes.
func (*PgIndexesSize) Evaluate(table string) int64 {
	_ = table // unused

	return 0
}

// EvaluateByOID returns the indexes size by OID.
func (*PgIndexesSize) EvaluateByOID(oid uint32) int64 {
	_ = oid // unused

	return 0
}

// PgTablespaceSize returns the size of a tablespace.
type PgTablespaceSize struct{}

// Evaluate returns the tablespace size in bytes.
func (*PgTablespaceSize) Evaluate(tablespace string) int64 {
	_ = tablespace // unused

	return 0
}

// EvaluateByOID returns the tablespace size by OID.
func (*PgTablespaceSize) EvaluateByOID(oid uint32) int64 {
	_ = oid // unused

	return 0
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
