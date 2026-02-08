package uniontype

import "testing"

func TestUnionValue(t *testing.T) {
	if _, err := NewValue("", 1); err == nil {
		t.Fatalf("expected empty tag error")
	}

	val, err := NewValue("num", 42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !val.IsTag("num") {
		t.Fatalf("expected tag match")
	}

	var out int
	if err := val.As(&out); err != nil {
		t.Fatalf("As failed: %v", err)
	}
	if out != 42 {
		t.Fatalf("unexpected value: %d", out)
	}
}
