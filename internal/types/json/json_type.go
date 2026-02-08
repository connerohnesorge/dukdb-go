package json

import (
	"encoding/json"
	"fmt"
)

// Value represents validated JSON input with optional parsed cache.
type Value struct {
	raw    []byte
	parsed any
}

// NewValue creates a JSON Value from raw bytes with validation.
func NewValue(raw []byte) (Value, error) {
	if !json.Valid(raw) {
		return Value{}, fmt.Errorf("invalid JSON")
	}
	copied := make([]byte, len(raw))
	copy(copied, raw)
	return Value{raw: copied}, nil
}

// Bytes returns a copy of the raw JSON bytes.
func (v Value) Bytes() []byte {
	copied := make([]byte, len(v.raw))
	copy(copied, v.raw)
	return copied
}

// String returns the raw JSON as a string.
func (v Value) String() string {
	return string(v.raw)
}

// Parse returns the parsed JSON value, caching the result.
func (v *Value) Parse() (any, error) {
	if v.parsed != nil {
		return v.parsed, nil
	}
	var parsed any
	if err := json.Unmarshal(v.raw, &parsed); err != nil {
		return nil, fmt.Errorf("parse JSON: %w", err)
	}
	v.parsed = parsed
	return parsed, nil
}

// MarshalJSON implements json.Marshaler.
func (v Value) MarshalJSON() ([]byte, error) {
	if !json.Valid(v.raw) {
		return nil, fmt.Errorf("invalid JSON")
	}
	return v.Bytes(), nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (v *Value) UnmarshalJSON(data []byte) error {
	parsed, err := NewValue(data)
	if err != nil {
		return err
	}
	v.raw = parsed.raw
	v.parsed = nil
	return nil
}
