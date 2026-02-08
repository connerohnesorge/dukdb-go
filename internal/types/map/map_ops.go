package maptype

import "fmt"

// Get returns a value by key.
func (v Value) Get(key any) (any, bool) {
	value, ok := v.entries[key]
	return value, ok
}

// Set updates or inserts a key/value pair.
func (v *Value) Set(key any, value any) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if v.entries == nil {
		v.entries = make(map[any]any)
	}
	v.entries[key] = value
	return nil
}

// Delete removes a key from the map.
func (v *Value) Delete(key any) {
	delete(v.entries, key)
}

// Keys returns the map keys.
func (v Value) Keys() []any {
	keys := make([]any, 0, len(v.entries))
	for key := range v.entries {
		keys = append(keys, key)
	}
	return keys
}

// Values returns the map values.
func (v Value) Values() []any {
	values := make([]any, 0, len(v.entries))
	for _, value := range v.entries {
		values = append(values, value)
	}
	return values
}

// ForEach iterates over entries and returns the first error encountered.
func (v Value) ForEach(fn func(key, value any) error) error {
	for key, value := range v.entries {
		if err := fn(key, value); err != nil {
			return fmt.Errorf("map iteration failed: %w", err)
		}
	}
	return nil
}
