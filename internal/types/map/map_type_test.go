package maptype

import "testing"

func TestMapValueValidation(t *testing.T) {
	val := Value{}
	if err := val.Set([]int{1}, "bad"); err == nil {
		t.Fatalf("expected invalid key error")
	}
	val, err := NewValue(map[any]any{"a": 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, ok := val.Get("a"); !ok || got != 1 {
		t.Fatalf("unexpected map value: %v", got)
	}
}

func TestMapValueOps(t *testing.T) {
	val, err := NewValue(map[any]any{"a": 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := val.Set("b", 2); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if _, ok := val.Get("b"); !ok {
		t.Fatalf("expected key to exist")
	}
	val.Delete("a")
	if _, ok := val.Get("a"); ok {
		t.Fatalf("expected key to be deleted")
	}
}
