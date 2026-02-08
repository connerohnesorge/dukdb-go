package array

import "testing"

func TestFixedArrayValidation(t *testing.T) {
	if _, err := NewFixedArray(2, []any{1}); err == nil {
		t.Fatalf("expected size mismatch error")
	}
	arr, err := NewFixedArray(2, []any{1, 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if arr.Len() != 2 {
		t.Fatalf("unexpected length: %d", arr.Len())
	}
}

func TestFixedArrayOps(t *testing.T) {
	arr, err := NewFixedArray(2, []any{"a", "b"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, _ := arr.Get(1); v != "b" {
		t.Fatalf("unexpected value: %v", v)
	}
	if err := arr.Set(0, "z"); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	slice, err := arr.Slice(0, 1)
	if err != nil {
		t.Fatalf("slice failed: %v", err)
	}
	if len(slice) != 1 || slice[0] != "z" {
		t.Fatalf("unexpected slice: %v", slice)
	}
}
