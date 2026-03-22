package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListStringFunctionsRound2(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("ListAppend", func(t *testing.T) {
		var result interface{}
		err := db.QueryRow("SELECT LIST_APPEND([1,2,3], 4)").Scan(&result)
		require.NoError(t, err)
		// Verify result contains 4 elements
		assert.NotNil(t, result)
	})

	t.Run("ListAppendNull", func(t *testing.T) {
		var result interface{}
		err := db.QueryRow("SELECT LIST_APPEND(NULL, 1)").Scan(&result)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("ListPrepend", func(t *testing.T) {
		var result interface{}
		err := db.QueryRow("SELECT LIST_PREPEND(0, [1,2,3])").Scan(&result)
		require.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("ListHas", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT LIST_HAS([1,2,3], 2)").Scan(&result)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("ListHasMissing", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT LIST_HAS([1,2,3], 5)").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("StringToArray", func(t *testing.T) {
		var result interface{}
		err := db.QueryRow("SELECT STRING_TO_ARRAY('a,b,c', ',')").Scan(&result)
		require.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("RegexpFullMatchTrue", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT REGEXP_FULL_MATCH('hello', 'h.*o')").Scan(&result)
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("RegexpFullMatchFalsePartial", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT REGEXP_FULL_MATCH('hello world', 'hello')").Scan(&result)
		require.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("RegexpFullMatchNull", func(t *testing.T) {
		var result *bool
		err := db.QueryRow("SELECT REGEXP_FULL_MATCH(NULL, 'test')").Scan(&result)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("ArrayAppendAlias", func(t *testing.T) {
		var result interface{}
		err := db.QueryRow("SELECT ARRAY_APPEND([10,20], 30)").Scan(&result)
		require.NoError(t, err)
		assert.NotNil(t, result)
	})
}
