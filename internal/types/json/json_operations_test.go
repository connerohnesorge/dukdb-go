package json

import "testing"

func TestGetSetDelete(t *testing.T) {
	input := map[string]any{
		"user": map[string]any{
			"name": "dukdb",
			"tags": []any{"go", "db"},
		},
	}

	got, err := Get(input, "user.name")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if got != "dukdb" {
		t.Fatalf("unexpected value: %v", got)
	}

	updated, err := Set(input, "user.name", "duck")
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}
	updatedName, err := Get(updated, "user.name")
	if err != nil {
		t.Fatalf("get after set failed: %v", err)
	}
	if updatedName != "duck" {
		t.Fatalf("unexpected updated value: %v", updatedName)
	}

	deleted, err := Delete(input, "user.tags[0]")
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	deletedTags, err := Get(deleted, "user.tags")
	if err != nil {
		t.Fatalf("get after delete failed: %v", err)
	}
	tags, ok := deletedTags.([]any)
	if !ok || len(tags) != 1 {
		t.Fatalf("unexpected tags after delete: %v", deletedTags)
	}
}
