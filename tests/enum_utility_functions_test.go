package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnumRange(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')")
	require.NoError(t, err)

	var result any
	err = db.QueryRow("SELECT ENUM_RANGE('mood')").Scan(&result)
	require.NoError(t, err)

	// Result should be a slice with 3 elements
	slice, ok := result.([]any)
	require.True(t, ok, "expected []any, got %T", result)
	assert.Equal(t, 3, len(slice))
	assert.Equal(t, "sad", slice[0])
	assert.Equal(t, "ok", slice[1])
	assert.Equal(t, "happy", slice[2])
}

func TestEnumFirst(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')")
	require.NoError(t, err)

	var result string
	err = db.QueryRow("SELECT ENUM_FIRST('mood')").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "sad", result)
}

func TestEnumLast(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')")
	require.NoError(t, err)

	var result string
	err = db.QueryRow("SELECT ENUM_LAST('mood')").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "happy", result)
}

func TestEnumRangeNonExistent(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	var result any
	err = db.QueryRow("SELECT ENUM_RANGE('nonexistent')").Scan(&result)
	assert.Error(t, err)
}

func TestEnumRangeNull(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	var result *string
	err = db.QueryRow("SELECT ENUM_RANGE(NULL)").Scan(&result)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestEnumSingleValue(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TYPE single AS ENUM ('only')")
	require.NoError(t, err)

	var first, last string
	err = db.QueryRow("SELECT ENUM_FIRST('single')").Scan(&first)
	require.NoError(t, err)
	err = db.QueryRow("SELECT ENUM_LAST('single')").Scan(&last)
	require.NoError(t, err)
	assert.Equal(t, "only", first)
	assert.Equal(t, "only", last)
}
