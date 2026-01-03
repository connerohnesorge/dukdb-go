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
		// Using a type prefix to avoid collisions between different types with same value
		switch v := val.(type) {
		case bool:
			h.Write([]byte{1}) // Type prefix for bool
			if v {
				h.Write([]byte{1})
			} else {
				h.Write([]byte{0})
			}
		case int8:
			h.Write([]byte{2}) // Type prefix for int8
			h.Write([]byte{byte(v)})
		case int16:
			h.Write([]byte{3}) // Type prefix for int16
			buf := make([]byte, 2)
			binary.LittleEndian.PutUint16(buf, uint16(v))
			h.Write(buf)
		case int32:
			h.Write([]byte{4}) // Type prefix for int32
			buf := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf, uint32(v))
			h.Write(buf)
		case int64:
			h.Write([]byte{5}) // Type prefix for int64
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(v))
			h.Write(buf)
		case uint8:
			h.Write([]byte{6}) // Type prefix for uint8
			h.Write([]byte{v})
		case uint16:
			h.Write([]byte{7}) // Type prefix for uint16
			buf := make([]byte, 2)
			binary.LittleEndian.PutUint16(buf, v)
			h.Write(buf)
		case uint32:
			h.Write([]byte{8}) // Type prefix for uint32
			buf := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf, v)
			h.Write(buf)
		case uint64:
			h.Write([]byte{9}) // Type prefix for uint64
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, v)
			h.Write(buf)
		case float32:
			h.Write([]byte{10}) // Type prefix for float32
			buf := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf, math.Float32bits(v))
			h.Write(buf)
		case float64:
			h.Write([]byte{11}) // Type prefix for float64
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, math.Float64bits(v))
			h.Write(buf)
		case string:
			h.Write([]byte{12}) // Type prefix for string
			h.Write([]byte(v))
		case []byte:
			h.Write([]byte{13}) // Type prefix for []byte
			h.Write(v)
		default:
			// Fallback to string representation
			h.Write([]byte{14}) // Type prefix for other
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
		return fmt.Errorf("key length %d does not match column count %d", len(key), len(idx.Columns))
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
		return fmt.Errorf("key length %d does not match column count %d", len(key), len(idx.Columns))
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
