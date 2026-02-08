package array

import "fmt"

// FixedArray represents a fixed-size array value.
type FixedArray struct {
	Values []any
	Size   int
}

// NewFixedArray creates a FixedArray with size validation.
func NewFixedArray(size int, values []any) (FixedArray, error) {
	if size < 0 {
		return FixedArray{}, fmt.Errorf("array size must be non-negative")
	}
	if size != len(values) {
		return FixedArray{}, fmt.Errorf(
			"array size mismatch: expected %d, got %d",
			size,
			len(values),
		)
	}
	copied := make([]any, len(values))
	copy(copied, values)
	return FixedArray{Values: copied, Size: size}, nil
}

// Len returns the number of elements in the array.
func (a FixedArray) Len() int {
	return a.Size
}
