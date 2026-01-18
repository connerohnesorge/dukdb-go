package engine

import (
	"context"
	"testing"
)

func TestQualifiedColumnsInViewQuery(t *testing.T) {
	eng := NewEngine()
	defer eng.Close()

	conn, err := eng.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer conn.Close()

	ctx := context.Background()

	// Create tables
	_, err = conn.Execute(ctx, "CREATE TABLE users (id INTEGER, name VARCHAR)", nil)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}
	t.Log("Created users table")

	_, err = conn.Execute(
		ctx,
		"CREATE TABLE posts (id INTEGER, user_id INTEGER, title VARCHAR)",
		nil,
	)
	if err != nil {
		t.Fatalf("Failed to create posts table: %v", err)
	}
	t.Log("Created posts table")

	// Create view
	viewSQL := "CREATE VIEW user_posts AS SELECT users.name, posts.title FROM users JOIN posts ON users.id = posts.user_id"
	t.Logf("Executing: %s", viewSQL)
	_, err = conn.Execute(ctx, viewSQL, nil)
	if err != nil {
		t.Fatalf("CREATE VIEW failed with error: %v", err)
	}
	t.Log("CREATE VIEW succeeded")

	// Now query the view
	t.Log("Now querying the view: SELECT * FROM user_posts")
	_, _, err = conn.Query(ctx, "SELECT * FROM user_posts", nil)
	if err != nil {
		t.Fatalf("Query view failed with error: %v", err)
	}
	t.Log("Query view succeeded")
}
