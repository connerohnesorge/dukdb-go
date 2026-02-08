package structtype

import "fmt"

// Value represents a struct-like collection of fields.
type Value struct {
	fields map[string]any
	order  []string
}

// NewValue creates a struct value with validation.
func NewValue(fields map[string]any) (Value, error) {
	copied := make(map[string]any, len(fields))
	order := make([]string, 0, len(fields))
	for name, value := range fields {
		if name == "" {
			return Value{}, fmt.Errorf("field name cannot be empty")
		}
		copied[name] = value
		order = append(order, name)
	}
	return Value{fields: copied, order: order}, nil
}

// Fields returns a copy of the field map.
func (v Value) Fields() map[string]any {
	copied := make(map[string]any, len(v.fields))
	for key, value := range v.fields {
		copied[key] = value
	}
	return copied
}

// FieldNames returns the field names in insertion order.
func (v Value) FieldNames() []string {
	names := make([]string, len(v.order))
	copy(names, v.order)
	return names
}
