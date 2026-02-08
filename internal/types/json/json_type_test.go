package json

import "testing"

func TestNewValueValidation(t *testing.T) {
	if _, err := NewValue([]byte("{\"ok\":true}")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := NewValue([]byte("{bad")); err == nil {
		t.Fatalf("expected error for invalid JSON")
	}
}

func TestValueParse(t *testing.T) {
	value, err := NewValue([]byte("{\"name\":\"dukdb\"}"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	parsed, err := value.Parse()
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	m, ok := parsed.(map[string]any)
	if !ok {
		t.Fatalf("expected map result")
	}
	if m["name"] != "dukdb" {
		t.Fatalf("unexpected value: %v", m["name"])
	}
}
