package json

import "testing"

func TestParsePath(t *testing.T) {
	path, err := ParsePath("$.user.name")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	segments := path.Segments()
	if len(segments) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(segments))
	}
	if segments[0].Field != "user" || segments[1].Field != "name" {
		t.Fatalf("unexpected segments: %+v", segments)
	}
}

func TestParsePathBracketIndex(t *testing.T) {
	path, err := ParsePath("items[1]")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	segments := path.Segments()
	if len(segments) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(segments))
	}
	if segments[1].Index == nil || *segments[1].Index != 1 {
		t.Fatalf("unexpected index segment: %+v", segments[1])
	}
}
