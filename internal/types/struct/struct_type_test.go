package structtype

import "testing"

func TestStructValueValidation(t *testing.T) {
	if _, err := NewValue(map[string]any{"": 1}); err == nil {
		t.Fatalf("expected empty field error")
	}
	val, err := NewValue(map[string]any{"id": 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, ok := val.Get("id"); !ok || got != 1 {
		t.Fatalf("unexpected value: %v", got)
	}
}

func TestStructValueOps(t *testing.T) {
	val, err := NewValue(map[string]any{"name": "dukdb"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := val.Set("version", "1"); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if _, ok := val.Get("version"); !ok {
		t.Fatalf("expected field to exist")
	}
	val.Delete("name")
	if _, ok := val.Get("name"); ok {
		t.Fatalf("expected field to be deleted")
	}
}
