package structtype

import "fmt"

// Get retrieves a field value.
func (v Value) Get(name string) (any, bool) {
	value, ok := v.fields[name]
	return value, ok
}

// Set updates or inserts a field value.
func (v *Value) Set(name string, value any) error {
	if name == "" {
		return fmt.Errorf("field name cannot be empty")
	}
	if v.fields == nil {
		v.fields = make(map[string]any)
	}
	if _, ok := v.fields[name]; !ok {
		v.order = append(v.order, name)
	}
	v.fields[name] = value
	return nil
}

// Delete removes a field value.
func (v *Value) Delete(name string) {
	delete(v.fields, name)
}
