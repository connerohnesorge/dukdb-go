package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStructFieldAccess(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Setup: create table with explicit STRUCT column type
	_, err = db.Exec(`CREATE TABLE people (id INTEGER, info STRUCT(name VARCHAR, age INTEGER))`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO people VALUES (1, struct_pack(name := 'Alice', age := 30))`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO people VALUES (2, struct_pack(name := 'Bob', age := 25))`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO people VALUES (3, NULL)`)
	require.NoError(t, err)

	t.Run("SelectStructField", func(t *testing.T) {
		var name string
		err := db.QueryRow("SELECT info.name FROM people WHERE id = 1").Scan(&name)
		require.NoError(t, err)
		assert.Equal(t, "Alice", name)
	})

	t.Run("SelectMultipleFields", func(t *testing.T) {
		var name string
		var age int
		err := db.QueryRow("SELECT info.name, info.age FROM people WHERE id = 2").Scan(&name, &age)
		require.NoError(t, err)
		assert.Equal(t, "Bob", name)
		assert.Equal(t, 25, age)
	})

	t.Run("NullStructReturnsNull", func(t *testing.T) {
		var name *string
		err := db.QueryRow("SELECT info.name FROM people WHERE id = 3").Scan(&name)
		require.NoError(t, err)
		assert.Nil(t, name)
	})

	t.Run("NonExistentFieldReturnsNull", func(t *testing.T) {
		var val interface{}
		err := db.QueryRow("SELECT info.nonexistent FROM people WHERE id = 1").Scan(&val)
		require.NoError(t, err)
		assert.Nil(t, val)
	})

	t.Run("StructFieldInWhere", func(t *testing.T) {
		var id int
		err := db.QueryRow("SELECT id FROM people WHERE info.age > 28").Scan(&id)
		require.NoError(t, err)
		assert.Equal(t, 1, id)
	})

	t.Run("StructExtractStillWorks", func(t *testing.T) {
		var name string
		err := db.QueryRow("SELECT STRUCT_EXTRACT(info, 'name') FROM people WHERE id = 1").Scan(&name)
		require.NoError(t, err)
		assert.Equal(t, "Alice", name)
	})
}
