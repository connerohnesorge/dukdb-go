package maptype

import (
	"fmt"
	"reflect"
)

// Value represents a map with validated keys.
type Value struct {
	entries map[any]any
}

// NewValue creates a new map value with key validation.
func NewValue(entries map[any]any) (Value, error) {
	copied := make(map[any]any, len(entries))
	for key, value := range entries {
		if err := validateKey(key); err != nil {
			return Value{}, err
		}
		copied[key] = value
	}
	return Value{entries: copied}, nil
}

// Entries returns a copy of the map entries.
func (v Value) Entries() map[any]any {
	copied := make(map[any]any, len(v.entries))
	for key, value := range v.entries {
		copied[key] = value
	}
	return copied
}

func validateKey(key any) error {
	if key == nil {
		return fmt.Errorf("map key cannot be nil")
	}
	if !reflect.TypeOf(key).Comparable() {
		return fmt.Errorf("map key must be comparable")
	}
	return nil
}
