package array

import "fmt"

// Get returns the value at the specified index.
func (a FixedArray) Get(index int) (any, error) {
	if index < 0 || index >= a.Size {
		return nil, fmt.Errorf("index out of range")
	}
	return a.Values[index], nil
}

// Set updates the value at the specified index.
func (a *FixedArray) Set(index int, value any) error {
	if index < 0 || index >= a.Size {
		return fmt.Errorf("index out of range")
	}
	a.Values[index] = value
	return nil
}

// Slice returns a sub-slice of the array values.
func (a FixedArray) Slice(start, end int) ([]any, error) {
	if start < 0 || end < 0 || start > end || end > a.Size {
		return nil, fmt.Errorf("slice bounds out of range")
	}
	out := make([]any, end-start)
	copy(out, a.Values[start:end])
	return out, nil
}
