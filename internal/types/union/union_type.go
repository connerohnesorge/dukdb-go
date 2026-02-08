package uniontype

import "fmt"

// Value represents a tagged union value.
type Value struct {
	Tag   string
	Value any
}

// NewValue creates a union value with validation.
func NewValue(tag string, value any) (Value, error) {
	if tag == "" {
		return Value{}, fmt.Errorf("union tag cannot be empty")
	}
	return Value{Tag: tag, Value: value}, nil
}
