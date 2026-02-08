package uniontype

import (
	"fmt"
	"reflect"
)

// IsTag returns true if the union tag matches.
func (v Value) IsTag(tag string) bool {
	return v.Tag == tag
}

// As assigns the union value into dest if compatible.
func (v Value) As(dest any) error {
	if dest == nil {
		return fmt.Errorf("destination cannot be nil")
	}
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer")
	}
	target := rv.Elem()
	value := reflect.ValueOf(v.Value)
	if !value.IsValid() {
		target.Set(reflect.Zero(target.Type()))
		return nil
	}
	if value.Type().AssignableTo(target.Type()) {
		target.Set(value)
		return nil
	}
	if value.Type().ConvertibleTo(target.Type()) {
		target.Set(value.Convert(target.Type()))
		return nil
	}
	return fmt.Errorf("cannot assign %s to %s", value.Type(), target.Type())
}
