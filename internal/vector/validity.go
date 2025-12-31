package vector

import "math/bits"

// BitsPerWord is the number of bits in a uint64 (used for validity bitmaps).
const BitsPerWord = 64

// ValidityMask tracks NULL values using a bitmap (1 bit per row).
// Bit = 1 means valid (not NULL), bit = 0 means NULL.
// This abstraction enables future optimizations like RLE compression.
type ValidityMask struct {
	bits     []uint64 // Bitmap: 1 = valid, 0 = NULL
	capacity uint64   // Maximum number of entries this mask can track
}

// NewValidityMask creates a new ValidityMask with the specified capacity.
// All entries are initialized as valid (not NULL) by default.
func NewValidityMask(capacity uint64) *ValidityMask {
	numWords := (capacity + BitsPerWord - 1) / BitsPerWord
	mask := make([]uint64, numWords)
	// Initialize all bits to 1 (valid) by default
	for i := range mask {
		mask[i] = ^uint64(0)
	}

	return &ValidityMask{
		bits:     mask,
		capacity: capacity,
	}
}

// NewValidityMaskEmpty creates a new ValidityMask with all entries marked as NULL.
func NewValidityMaskEmpty(capacity uint64) *ValidityMask {
	numWords := (capacity + BitsPerWord - 1) / BitsPerWord

	return &ValidityMask{
		bits:     make([]uint64, numWords),
		capacity: capacity,
	}
}

// IsValid checks if the entry at the given row index is valid (not NULL).
// Returns true if valid, false if NULL.
// Does not perform bounds checking for performance - caller must ensure row < capacity.
func (v *ValidityMask) IsValid(row uint64) bool {
	wordIdx := row / BitsPerWord
	bitIdx := row % BitsPerWord

	return (v.bits[wordIdx] & (1 << bitIdx)) != 0
}

// SetValid sets the validity of the entry at the given row index.
// If valid is true, marks the entry as valid (not NULL).
// If valid is false, marks the entry as NULL.
// Does not perform bounds checking for performance - caller must ensure row < capacity.
func (v *ValidityMask) SetValid(row uint64, valid bool) {
	wordIdx := row / BitsPerWord
	bitIdx := row % BitsPerWord
	if valid {
		v.bits[wordIdx] |= (1 << bitIdx)
	} else {
		v.bits[wordIdx] &^= (1 << bitIdx)
	}
}

// SetNull marks the entry at the given row index as NULL.
// This is a convenience method equivalent to SetValid(row, false).
func (v *ValidityMask) SetNull(row uint64) {
	wordIdx := row / BitsPerWord
	bitIdx := row % BitsPerWord
	v.bits[wordIdx] &^= (1 << bitIdx)
}

// SetAllValid marks all entries up to count as valid (not NULL).
// This is more efficient than calling SetValid in a loop.
func (v *ValidityMask) SetAllValid(count uint64) {
	if count == 0 {
		return
	}

	// Number of full words to set
	fullWords := count / BitsPerWord
	for i := range fullWords {
		v.bits[i] = ^uint64(0)
	}

	// Remaining bits in the last partial word
	remaining := count % BitsPerWord
	if remaining > 0 && fullWords < uint64(len(v.bits)) {
		// Set only the first 'remaining' bits in this word
		v.bits[fullWords] = (uint64(1) << remaining) - 1
	}
}

// SetAllNull marks all entries as NULL.
// This is more efficient than calling SetNull in a loop.
func (v *ValidityMask) SetAllNull() {
	for i := range v.bits {
		v.bits[i] = 0
	}
}

// CountValid returns the number of valid (non-NULL) entries in the mask.
// Uses efficient bit counting via bits.OnesCount64.
func (v *ValidityMask) CountValid() uint64 {
	if v.capacity == 0 {
		return 0
	}

	count := uint64(0)
	fullWords := v.capacity / BitsPerWord

	// Count bits in full words
	for i := range fullWords {
		count += uint64(bits.OnesCount64(v.bits[i]))
	}

	// Count bits in the partial last word (if any)
	remaining := v.capacity % BitsPerWord
	if remaining > 0 {
		// Create a mask for only the bits we care about
		mask := (uint64(1) << remaining) - 1
		count += uint64(bits.OnesCount64(v.bits[fullWords] & mask))
	}

	return count
}

// CountNull returns the number of NULL entries in the mask.
func (v *ValidityMask) CountNull() uint64 {
	return v.capacity - v.CountValid()
}

// Capacity returns the maximum number of entries this mask can track.
func (v *ValidityMask) Capacity() uint64 {
	return v.capacity
}

// HasNull returns true if any entry is NULL.
// This is more efficient than CountNull() > 0 for early termination.
func (v *ValidityMask) HasNull() bool {
	if v.capacity == 0 {
		return false
	}

	fullWords := v.capacity / BitsPerWord

	// Check full words
	for i := range fullWords {
		if v.bits[i] != ^uint64(0) {
			return true
		}
	}

	// Check the partial last word (if any)
	remaining := v.capacity % BitsPerWord
	if remaining > 0 {
		// Create a mask for only the bits we care about
		mask := (uint64(1) << remaining) - 1
		if (v.bits[fullWords] & mask) != mask {
			return true
		}
	}

	return false
}

// AllValid returns true if all entries are valid (no NULLs).
func (v *ValidityMask) AllValid() bool {
	return !v.HasNull()
}

// Reset clears the mask, marking all entries as NULL.
// The capacity is preserved for reuse.
func (v *ValidityMask) Reset() {
	v.SetAllNull()
}

// Clone creates a deep copy of the validity mask.
func (v *ValidityMask) Clone() *ValidityMask {
	newBits := make([]uint64, len(v.bits))
	copy(newBits, v.bits)

	return &ValidityMask{
		bits:     newBits,
		capacity: v.capacity,
	}
}

// Bits returns the underlying bit slice for direct access.
// This is useful for serialization or low-level operations.
// The returned slice should not be modified directly.
func (v *ValidityMask) Bits() []uint64 {
	return v.bits
}

// SetBits sets the underlying bit slice from external data.
// This is useful for deserialization.
// The input slice is copied to prevent external modification.
func (v *ValidityMask) SetBits(bits []uint64) {
	if len(bits) == 0 {
		return
	}
	// Copy up to the capacity of our mask
	numWords := (v.capacity + BitsPerWord - 1) / BitsPerWord
	toCopy := min(uint64(len(bits)), numWords)
	copy(v.bits[:toCopy], bits[:toCopy])
}

// min returns the minimum of two uint64 values.
func min(a, b uint64) uint64 {
	if a < b {
		return a
	}

	return b
}
