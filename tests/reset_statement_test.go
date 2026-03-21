package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResetStatement(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("ResetTransactionIsolation", func(t *testing.T) {
		// Change from default
		_, err := db.Exec("SET transaction_isolation = 'read committed'")
		require.NoError(t, err)

		// Verify changed
		var val string
		err = db.QueryRow("SHOW transaction_isolation").Scan(&val)
		require.NoError(t, err)
		assert.Equal(t, "READ COMMITTED", val)

		// Reset
		_, err = db.Exec("RESET transaction_isolation")
		require.NoError(t, err)

		// Verify reset to default
		err = db.QueryRow("SHOW transaction_isolation").Scan(&val)
		require.NoError(t, err)
		assert.Equal(t, "SERIALIZABLE", val)
	})

	t.Run("ResetDefaultTransactionIsolation", func(t *testing.T) {
		// Change from default
		_, err := db.Exec("SET default_transaction_isolation = 'repeatable read'")
		require.NoError(t, err)

		// Verify changed
		var val string
		err = db.QueryRow("SHOW default_transaction_isolation").Scan(&val)
		require.NoError(t, err)
		assert.Equal(t, "REPEATABLE READ", val)

		// Reset
		_, err = db.Exec("RESET default_transaction_isolation")
		require.NoError(t, err)

		// Verify reset to default
		err = db.QueryRow("SHOW default_transaction_isolation").Scan(&val)
		require.NoError(t, err)
		assert.Equal(t, "SERIALIZABLE", val)
	})

	t.Run("ResetAll", func(t *testing.T) {
		// Change multiple settings
		_, err := db.Exec("SET transaction_isolation = 'read uncommitted'")
		require.NoError(t, err)

		// Reset all
		_, err = db.Exec("RESET ALL")
		require.NoError(t, err)

		// Verify reset
		var val string
		err = db.QueryRow("SHOW transaction_isolation").Scan(&val)
		require.NoError(t, err)
		assert.Equal(t, "SERIALIZABLE", val)
	})

	t.Run("ResetUnknownVariable", func(t *testing.T) {
		// Should not error (silently accepted like SET)
		_, err := db.Exec("RESET nonexistent_var")
		require.NoError(t, err)
	})
}
