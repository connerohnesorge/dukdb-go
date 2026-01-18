package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
)

// ErrConstraintViolation is returned when a unique constraint is violated.
var ErrConstraintViolation = errors.New("unique constraint violation")

// ErrIndexCorrupted is returned when an index is detected to be corrupted.
// This can happen when:
// - Index contains RowIDs pointing to non-existent rows
// - Index is out of sync with the underlying table data
// - Index internal data structures are inconsistent
var ErrIndexCorrupted = errors.New("index corrupted")

// IndexCorruptionError provides detailed information about index corruption.
type IndexCorruptionError struct {
	IndexName   string
	TableName   string
	Description string
	StaleRowIDs []RowID // RowIDs in index that don't exist in table
}

func (e *IndexCorruptionError) Error() string {
	return fmt.Sprintf(
		"index %q on table %q is corrupted: %s",
		e.IndexName,
		e.TableName,
		e.Description,
	)
}

func (e *IndexCorruptionError) Unwrap() error {
	return ErrIndexCorrupted
}

// hashKey represents a composite key for indexed columns.
// It uses a hash of the column values for efficient lookup.
type hashKey [32]byte

// makeHashKey creates a hash key from a slice of values.
func makeHashKey(values []any) hashKey {
	h := sha256.New()
	for _, val := range values {
		if val == nil {
			// NULL values get a special marker
			h.Write([]byte{0, 0})

			continue
		}

		// Serialize the value based on type
		// Type prefixes are used to avoid collisions between different semantic types.
		// IMPORTANT: All signed integer types (int8, int16, int32, int64, int) hash the same
		// for SQL semantic equality (e.g., WHERE id = 3 should match whether stored as int32 or int64).
		// Similarly, all unsigned integer types hash the same.
		switch v := val.(type) {
		case bool:
			h.Write([]byte{1}) // Type prefix for bool
			if v {
				h.Write([]byte{1})
			} else {
				h.Write([]byte{0})
			}
		// All signed integers normalize to int64 for consistent hashing
		case int8:
			h.Write([]byte{2}) // Type prefix for signed integers
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(int64(v)))
			h.Write(buf)
		case int16:
			h.Write([]byte{2}) // Type prefix for signed integers
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(int64(v)))
			h.Write(buf)
		case int32:
			h.Write([]byte{2}) // Type prefix for signed integers
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(int64(v)))
			h.Write(buf)
		case int64:
			h.Write([]byte{2}) // Type prefix for signed integers
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(v))
			h.Write(buf)
		case int:
			h.Write([]byte{2}) // Type prefix for signed integers
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(int64(v)))
			h.Write(buf)
		// All unsigned integers normalize to uint64 for consistent hashing
		case uint8:
			h.Write([]byte{3}) // Type prefix for unsigned integers
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(v))
			h.Write(buf)
		case uint16:
			h.Write([]byte{3}) // Type prefix for unsigned integers
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(v))
			h.Write(buf)
		case uint32:
			h.Write([]byte{3}) // Type prefix for unsigned integers
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(v))
			h.Write(buf)
		case uint64:
			h.Write([]byte{3}) // Type prefix for unsigned integers
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, v)
			h.Write(buf)
		case uint:
			h.Write([]byte{3}) // Type prefix for unsigned integers
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(v))
			h.Write(buf)
		// Floating point types remain separate since float32(3.0) != float64(3.0) may have precision differences
		case float32:
			h.Write([]byte{4}) // Type prefix for float32
			buf := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf, math.Float32bits(v))
			h.Write(buf)
		case float64:
			h.Write([]byte{5}) // Type prefix for float64
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, math.Float64bits(v))
			h.Write(buf)
		case string:
			h.Write([]byte{6}) // Type prefix for string
			h.Write([]byte(v))
		case []byte:
			h.Write([]byte{7}) // Type prefix for []byte
			h.Write(v)
		default:
			// Fallback to string representation
			h.Write([]byte{8}) // Type prefix for other
			_, _ = fmt.Fprintf(h, "%v", v)
		}
	}

	var key hashKey
	copy(key[:], h.Sum(nil))

	return key
}

// HashIndex implements a simple hash-based index for equality lookups.
// It maps hash keys (derived from indexed column values) to sets of RowIDs.
type HashIndex struct {
	mu        sync.RWMutex
	Name      string
	TableName string
	Columns   []string
	IsUnique  bool
	entries   map[hashKey][]RowID
}

// NewHashIndex creates a new hash-based index.
func NewHashIndex(name, tableName string, columns []string, isUnique bool) *HashIndex {
	return &HashIndex{
		Name:      name,
		TableName: tableName,
		Columns:   columns,
		IsUnique:  isUnique,
		entries:   make(map[hashKey][]RowID),
	}
}

// Insert adds a key-value pair to the index.
// For unique indexes, it returns an error if the key already exists.
func (idx *HashIndex) Insert(key []any, rowID RowID) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if len(key) != len(idx.Columns) {
		return fmt.Errorf(
			"key length %d does not match column count %d",
			len(key),
			len(idx.Columns),
		)
	}

	hashKey := makeHashKey(key)

	if idx.IsUnique {
		if existing, exists := idx.entries[hashKey]; exists && len(existing) > 0 {
			return ErrConstraintViolation
		}
	}

	idx.entries[hashKey] = append(idx.entries[hashKey], rowID)

	return nil
}

// Delete removes a key-value pair from the index.
func (idx *HashIndex) Delete(key []any, rowID RowID) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if len(key) != len(idx.Columns) {
		return fmt.Errorf(
			"key length %d does not match column count %d",
			len(key),
			len(idx.Columns),
		)
	}

	hashKey := makeHashKey(key)

	existing, exists := idx.entries[hashKey]
	if !exists {
		// Key not found, nothing to delete
		return nil
	}

	// Find and remove the specific RowID
	newRows := make([]RowID, 0, len(existing))
	for _, rid := range existing {
		if rid != rowID {
			newRows = append(newRows, rid)
		}
	}

	if len(newRows) == 0 {
		// No more entries for this key, remove it completely
		delete(idx.entries, hashKey)
	} else {
		idx.entries[hashKey] = newRows
	}

	return nil
}

// Lookup finds all RowIDs associated with the given key.
func (idx *HashIndex) Lookup(key []any) []RowID {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(key) != len(idx.Columns) {
		return nil
	}

	hashKey := makeHashKey(key)
	rowIDs := idx.entries[hashKey]

	// Return a copy to avoid concurrent modification issues
	result := make([]RowID, len(rowIDs))
	copy(result, rowIDs)

	return result
}

// Count returns the number of unique keys in the index.
func (idx *HashIndex) Count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.entries)
}

// Clear removes all entries from the index.
func (idx *HashIndex) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.entries = make(map[hashKey][]RowID)
}

// ValidateAgainstTable checks if the index is consistent with the given table.
// It returns an IndexCorruptionError if any RowIDs in the index point to
// rows that don't exist in the table or have been deleted (tombstoned).
//
// Parameters:
//   - table: The table to validate against
//
// Returns:
//   - error: nil if the index is valid, IndexCorruptionError if corrupted
//
// This method is useful for:
// - Detecting index corruption after system crashes
// - Verifying index integrity after data recovery
// - Diagnosing issues when index scans return unexpected results
func (idx *HashIndex) ValidateAgainstTable(table *Table) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if table == nil {
		return &IndexCorruptionError{
			IndexName:   idx.Name,
			TableName:   idx.TableName,
			Description: "table is nil",
		}
	}

	var staleRowIDs []RowID

	// Check each RowID in the index
	for _, rowIDs := range idx.entries {
		for _, rowID := range rowIDs {
			// Check if the row exists and is not tombstoned
			if !table.ContainsRow(rowID) {
				staleRowIDs = append(staleRowIDs, rowID)
			}
		}
	}

	if len(staleRowIDs) > 0 {
		return &IndexCorruptionError{
			IndexName: idx.Name,
			TableName: idx.TableName,
			Description: fmt.Sprintf(
				"index contains %d stale RowID(s) pointing to non-existent or deleted rows",
				len(staleRowIDs),
			),
			StaleRowIDs: staleRowIDs,
		}
	}

	return nil
}

// LookupWithValidation performs an index lookup and validates that all returned
// RowIDs still exist in the table. It filters out any stale RowIDs and optionally
// returns an error if corruption is detected.
//
// Parameters:
//   - key: The lookup key
//   - table: The table to validate against
//   - reportCorruption: If true, returns an error when stale RowIDs are found
//
// Returns:
//   - []RowID: Valid RowIDs that exist in the table
//   - error: IndexCorruptionError if reportCorruption is true and stale RowIDs found
//
// This method provides a defensive lookup that handles potential index corruption
// gracefully by filtering out invalid RowIDs.
func (idx *HashIndex) LookupWithValidation(
	key []any,
	table *Table,
	reportCorruption bool,
) ([]RowID, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(key) != len(idx.Columns) {
		return nil, nil
	}

	hashKey := makeHashKey(key)
	rowIDs := idx.entries[hashKey]

	if len(rowIDs) == 0 || table == nil {
		return nil, nil
	}

	// Filter to only valid RowIDs
	validRowIDs := make([]RowID, 0, len(rowIDs))
	var staleRowIDs []RowID

	for _, rowID := range rowIDs {
		if table.ContainsRow(rowID) {
			validRowIDs = append(validRowIDs, rowID)
		} else {
			staleRowIDs = append(staleRowIDs, rowID)
		}
	}

	if len(staleRowIDs) > 0 && reportCorruption {
		return validRowIDs, &IndexCorruptionError{
			IndexName:   idx.Name,
			TableName:   idx.TableName,
			Description: fmt.Sprintf("lookup found %d stale RowID(s) for key", len(staleRowIDs)),
			StaleRowIDs: staleRowIDs,
		}
	}

	return validRowIDs, nil
}

// GetAllRowIDs returns all RowIDs stored in the index.
// This is useful for index validation and debugging.
func (idx *HashIndex) GetAllRowIDs() []RowID {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var result []RowID
	for _, rowIDs := range idx.entries {
		result = append(result, rowIDs...)
	}
	return result
}
