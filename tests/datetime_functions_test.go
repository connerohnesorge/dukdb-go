package tests

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCurrentDateTimeFunctions(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("NowReturnsTimestamp", func(t *testing.T) {
		var result time.Time
		err := db.QueryRow("SELECT NOW()").Scan(&result)
		require.NoError(t, err)
		assert.WithinDuration(t, time.Now(), result, 5*time.Second)
	})

	t.Run("CurrentTimestampBareKeyword", func(t *testing.T) {
		var result time.Time
		err := db.QueryRow("SELECT CURRENT_TIMESTAMP").Scan(&result)
		require.NoError(t, err)
		assert.WithinDuration(t, time.Now(), result, 5*time.Second)
	})

	t.Run("CurrentTimestampWithParens", func(t *testing.T) {
		var result time.Time
		err := db.QueryRow("SELECT CURRENT_TIMESTAMP()").Scan(&result)
		require.NoError(t, err)
		assert.WithinDuration(t, time.Now(), result, 5*time.Second)
	})

	t.Run("CurrentDateBareKeyword", func(t *testing.T) {
		var result interface{}
		err := db.QueryRow("SELECT CURRENT_DATE").Scan(&result)
		require.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("CurrentDateWithParens", func(t *testing.T) {
		var result interface{}
		err := db.QueryRow("SELECT CURRENT_DATE()").Scan(&result)
		require.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("TodayFunction", func(t *testing.T) {
		var result interface{}
		err := db.QueryRow("SELECT TODAY()").Scan(&result)
		require.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("CurrentTimeBareKeyword", func(t *testing.T) {
		var result interface{}
		err := db.QueryRow("SELECT CURRENT_TIME").Scan(&result)
		require.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("NowIsNotNull", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT NOW() IS NOT NULL").Scan(&result)
		require.NoError(t, err)
		assert.True(t, result)
	})
}
