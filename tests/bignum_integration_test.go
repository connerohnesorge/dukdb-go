package tests

import (
	"database/sql"
	"math/big"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	// Import to register the engine backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBignumColumnIntegration tests BIGNUM column operations end-to-end with database/sql.
func TestBignumColumnIntegration(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with BIGNUM column
	_, err = db.Exec(`CREATE TABLE bignum_test (id INTEGER, value BIGNUM)`)
	require.NoError(t, err)

	t.Run("Insert and select small integer as BIGNUM", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO bignum_test VALUES (1, $1)`, "42")
		require.NoError(t, err)

		var id int
		var value string
		err = db.QueryRow(`SELECT id, value FROM bignum_test WHERE id = 1`).Scan(&id, &value)
		require.NoError(t, err)
		assert.Equal(t, 1, id)

		// Verify the value can be parsed as big.Int
		expected := big.NewInt(42)
		actual, ok := new(big.Int).SetString(value, 10)
		require.True(t, ok)
		assert.Equal(t, 0, expected.Cmp(actual))
	})

	t.Run("Insert and select negative BIGNUM", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO bignum_test VALUES (2, $1)`, "-12345678901234567890")
		require.NoError(t, err)

		var id int
		var value string
		err = db.QueryRow(`SELECT id, value FROM bignum_test WHERE id = 2`).Scan(&id, &value)
		require.NoError(t, err)
		assert.Equal(t, 2, id)

		// Verify the value roundtrips correctly
		expected, ok := new(big.Int).SetString("-12345678901234567890", 10)
		require.True(t, ok)
		actual, ok := new(big.Int).SetString(value, 10)
		require.True(t, ok)
		assert.Equal(t, 0, expected.Cmp(actual))
	})

	t.Run("Insert and select value beyond int64 range", func(t *testing.T) {
		// 2^100 is way beyond int64 max (2^63 - 1)
		hugeValue := new(big.Int).Exp(big.NewInt(2), big.NewInt(100), nil)
		_, err := db.Exec(`INSERT INTO bignum_test VALUES (3, $1)`, hugeValue.String())
		require.NoError(t, err)

		var id int
		var value string
		err = db.QueryRow(`SELECT id, value FROM bignum_test WHERE id = 3`).Scan(&id, &value)
		require.NoError(t, err)
		assert.Equal(t, 3, id)

		// Verify the huge value roundtrips correctly
		actual, ok := new(big.Int).SetString(value, 10)
		require.True(t, ok)
		assert.Equal(t, 0, hugeValue.Cmp(actual))
	})

	t.Run("Insert and select very large number string", func(t *testing.T) {
		// 100-digit number
		largeNum := "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
		_, err := db.Exec(`INSERT INTO bignum_test VALUES (4, $1)`, largeNum)
		require.NoError(t, err)

		var id int
		var value string
		err = db.QueryRow(`SELECT id, value FROM bignum_test WHERE id = 4`).Scan(&id, &value)
		require.NoError(t, err)
		assert.Equal(t, 4, id)

		// Verify the value roundtrips correctly
		expected, ok := new(big.Int).SetString(largeNum, 10)
		require.True(t, ok)
		actual, ok := new(big.Int).SetString(value, 10)
		require.True(t, ok)
		assert.Equal(t, 0, expected.Cmp(actual))
	})

	t.Run("Insert and select zero", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO bignum_test VALUES (5, $1)`, "0")
		require.NoError(t, err)

		var id int
		var value string
		err = db.QueryRow(`SELECT id, value FROM bignum_test WHERE id = 5`).Scan(&id, &value)
		require.NoError(t, err)
		assert.Equal(t, 5, id)

		expected := big.NewInt(0)
		actual, ok := new(big.Int).SetString(value, 10)
		require.True(t, ok)
		assert.Equal(t, 0, expected.Cmp(actual))
	})
}

// TestBignumNullHandling tests NULL handling in BIGNUM columns.
func TestBignumNullHandling(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with BIGNUM column
	_, err = db.Exec(`CREATE TABLE bignum_null_test (id INTEGER, value BIGNUM)`)
	require.NoError(t, err)

	t.Run("Insert SQL NULL into BIGNUM column", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO bignum_null_test VALUES (1, NULL)`)
		require.NoError(t, err)

		var id int
		var value sql.NullString
		err = db.QueryRow(`SELECT id, value FROM bignum_null_test WHERE id = 1`).Scan(&id, &value)
		require.NoError(t, err)
		assert.Equal(t, 1, id)
		assert.False(t, value.Valid, "SQL NULL should result in NullString.Valid = false")
	})

	t.Run("Insert SQL NULL using parameter", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO bignum_null_test VALUES (2, $1)`, nil)
		require.NoError(t, err)

		var id int
		var value sql.NullString
		err = db.QueryRow(`SELECT id, value FROM bignum_null_test WHERE id = 2`).Scan(&id, &value)
		require.NoError(t, err)
		assert.Equal(t, 2, id)
		assert.False(t, value.Valid, "SQL NULL via parameter should result in NullString.Valid = false")
	})

	t.Run("Query NULL with IS NULL", func(t *testing.T) {
		rows, err := db.Query(`SELECT id FROM bignum_null_test WHERE value IS NULL ORDER BY id`)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, rows.Close())
		}()

		var ids []int
		for rows.Next() {
			var id int
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())

		assert.Equal(t, []int{1, 2}, ids)
	})

	t.Run("Query non-NULL with IS NOT NULL", func(t *testing.T) {
		// Insert a non-NULL value
		_, err := db.Exec(`INSERT INTO bignum_null_test VALUES (3, $1)`, "12345")
		require.NoError(t, err)

		rows, err := db.Query(`SELECT id FROM bignum_null_test WHERE value IS NOT NULL ORDER BY id`)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, rows.Close())
		}()

		var ids []int
		for rows.Next() {
			var id int
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())

		assert.Equal(t, []int{3}, ids)
	})
}

// TestBignumMultipleRows tests BIGNUM column with multiple rows.
func TestBignumMultipleRows(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with BIGNUM column
	_, err = db.Exec(`CREATE TABLE bignum_multi_test (id INTEGER, value BIGNUM)`)
	require.NoError(t, err)

	// Insert multiple rows with different BIGNUM values
	testData := []struct {
		id    int
		value string
	}{
		{1, "1"},
		{2, "12345678901234567890"},
		{3, "-98765432109876543210"},
		{4, "0"},
		{5, "999999999999999999999999999999999999999999"},
	}

	for _, td := range testData {
		_, err := db.Exec(`INSERT INTO bignum_multi_test VALUES ($1, $2)`, td.id, td.value)
		require.NoError(t, err)
	}

	// Query all rows
	rows, err := db.Query(`SELECT id, value FROM bignum_multi_test ORDER BY id`)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	var results []struct {
		id    int
		value string
	}
	for rows.Next() {
		var id int
		var value string
		require.NoError(t, rows.Scan(&id, &value))
		results = append(results, struct {
			id    int
			value string
		}{id, value})
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 5)

	// Verify each row
	for i, td := range testData {
		assert.Equal(t, td.id, results[i].id)
		// Compare as big.Int values
		expected, ok := new(big.Int).SetString(td.value, 10)
		require.True(t, ok)
		actual, ok := new(big.Int).SetString(results[i].value, 10)
		require.True(t, ok)
		assert.Equal(t, 0, expected.Cmp(actual), "row %d: expected %s, got %s", i, td.value, results[i].value)
	}
}

// TestBignumPreparedStatement tests BIGNUM column with prepared statements.
func TestBignumPreparedStatement(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with BIGNUM column
	_, err = db.Exec(`CREATE TABLE bignum_prep_test (id INTEGER, value BIGNUM)`)
	require.NoError(t, err)

	// Prepare insert statement
	insertStmt, err := db.Prepare(`INSERT INTO bignum_prep_test VALUES ($1, $2)`)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, insertStmt.Close())
	}()

	// Insert multiple rows using prepared statement
	testData := []struct {
		id    int
		value string
	}{
		{1, "111111111111111111111"},
		{2, "222222222222222222222"},
		{3, "333333333333333333333"},
	}

	for _, td := range testData {
		_, err := insertStmt.Exec(td.id, td.value)
		require.NoError(t, err)
	}

	// Prepare select statement
	selectStmt, err := db.Prepare(`SELECT value FROM bignum_prep_test WHERE id = $1`)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, selectStmt.Close())
	}()

	// Query using prepared statement
	for _, td := range testData {
		var value string
		err := selectStmt.QueryRow(td.id).Scan(&value)
		require.NoError(t, err)

		// Compare as big.Int values
		expected, ok := new(big.Int).SetString(td.value, 10)
		require.True(t, ok)
		actual, ok := new(big.Int).SetString(value, 10)
		require.True(t, ok)
		assert.Equal(t, 0, expected.Cmp(actual))
	}
}

// TestBignumTransaction tests BIGNUM column operations within a transaction.
func TestBignumTransaction(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with BIGNUM column
	_, err = db.Exec(`CREATE TABLE bignum_tx_test (id INTEGER, value BIGNUM)`)
	require.NoError(t, err)

	t.Run("Commit transaction", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)

		bigValue := "123456789012345678901234567890"
		_, err = tx.Exec(`INSERT INTO bignum_tx_test VALUES (1, $1)`, bigValue)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		// Verify data persisted
		var value string
		err = db.QueryRow(`SELECT value FROM bignum_tx_test WHERE id = 1`).Scan(&value)
		require.NoError(t, err)

		expected, ok := new(big.Int).SetString(bigValue, 10)
		require.True(t, ok)
		actual, ok := new(big.Int).SetString(value, 10)
		require.True(t, ok)
		assert.Equal(t, 0, expected.Cmp(actual))
	})

	t.Run("Rollback transaction", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)

		_, err = tx.Exec(`INSERT INTO bignum_tx_test VALUES (2, $1)`, "999999999999999999999")
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)

		// Verify data was not persisted
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM bignum_tx_test WHERE id = 2`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})
}

// TestBignumUpdate tests updating BIGNUM column values.
func TestBignumUpdate(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table and insert initial data
	_, err = db.Exec(`CREATE TABLE bignum_update_test (id INTEGER PRIMARY KEY, value BIGNUM)`)
	require.NoError(t, err)

	initialValue := "12345678901234567890"
	_, err = db.Exec(`INSERT INTO bignum_update_test VALUES (1, $1)`, initialValue)
	require.NoError(t, err)

	// Update the BIGNUM data
	newValue := "98765432109876543210"
	result, err := db.Exec(`UPDATE bignum_update_test SET value = $1 WHERE id = 1`, newValue)
	require.NoError(t, err)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)

	// Verify the update
	var value string
	err = db.QueryRow(`SELECT value FROM bignum_update_test WHERE id = 1`).Scan(&value)
	require.NoError(t, err)

	expected, ok := new(big.Int).SetString(newValue, 10)
	require.True(t, ok)
	actual, ok := new(big.Int).SetString(value, 10)
	require.True(t, ok)
	assert.Equal(t, 0, expected.Cmp(actual))
}

// TestBignumDelete tests deleting rows with BIGNUM columns.
func TestBignumDelete(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table and insert data
	_, err = db.Exec(`CREATE TABLE bignum_delete_test (id INTEGER, value BIGNUM)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO bignum_delete_test VALUES (1, $1)`, "11111111111111111111")
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO bignum_delete_test VALUES (2, $1)`, "22222222222222222222")
	require.NoError(t, err)

	// Delete one row
	result, err := db.Exec(`DELETE FROM bignum_delete_test WHERE id = 1`)
	require.NoError(t, err)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)

	// Verify only one row remains
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM bignum_delete_test`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify the correct row remains
	var value string
	err = db.QueryRow(`SELECT value FROM bignum_delete_test WHERE id = 2`).Scan(&value)
	require.NoError(t, err)

	expected, ok := new(big.Int).SetString("22222222222222222222", 10)
	require.True(t, ok)
	actual, ok := new(big.Int).SetString(value, 10)
	require.True(t, ok)
	assert.Equal(t, 0, expected.Cmp(actual))
}

// TestBignumPrecisionEdgeCases tests edge cases for BIGNUM precision.
func TestBignumPrecisionEdgeCases(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with BIGNUM column
	_, err = db.Exec(`CREATE TABLE bignum_precision_test (id INTEGER, value BIGNUM)`)
	require.NoError(t, err)

	t.Run("Maximum int64 value", func(t *testing.T) {
		// int64 max = 9223372036854775807
		maxInt64 := "9223372036854775807"
		_, err := db.Exec(`INSERT INTO bignum_precision_test VALUES (1, $1)`, maxInt64)
		require.NoError(t, err)

		var value string
		err = db.QueryRow(`SELECT value FROM bignum_precision_test WHERE id = 1`).Scan(&value)
		require.NoError(t, err)

		expected, ok := new(big.Int).SetString(maxInt64, 10)
		require.True(t, ok)
		actual, ok := new(big.Int).SetString(value, 10)
		require.True(t, ok)
		assert.Equal(t, 0, expected.Cmp(actual))
	})

	t.Run("Minimum int64 value", func(t *testing.T) {
		// int64 min = -9223372036854775808
		minInt64 := "-9223372036854775808"
		_, err := db.Exec(`INSERT INTO bignum_precision_test VALUES (2, $1)`, minInt64)
		require.NoError(t, err)

		var value string
		err = db.QueryRow(`SELECT value FROM bignum_precision_test WHERE id = 2`).Scan(&value)
		require.NoError(t, err)

		expected, ok := new(big.Int).SetString(minInt64, 10)
		require.True(t, ok)
		actual, ok := new(big.Int).SetString(value, 10)
		require.True(t, ok)
		assert.Equal(t, 0, expected.Cmp(actual))
	})

	t.Run("Value one greater than int64 max", func(t *testing.T) {
		// int64 max + 1 = 9223372036854775808
		beyondInt64Max := "9223372036854775808"
		_, err := db.Exec(`INSERT INTO bignum_precision_test VALUES (3, $1)`, beyondInt64Max)
		require.NoError(t, err)

		var value string
		err = db.QueryRow(`SELECT value FROM bignum_precision_test WHERE id = 3`).Scan(&value)
		require.NoError(t, err)

		expected, ok := new(big.Int).SetString(beyondInt64Max, 10)
		require.True(t, ok)
		actual, ok := new(big.Int).SetString(value, 10)
		require.True(t, ok)
		assert.Equal(t, 0, expected.Cmp(actual))
	})

	t.Run("Value one less than int64 min", func(t *testing.T) {
		// int64 min - 1 = -9223372036854775809
		belowInt64Min := "-9223372036854775809"
		_, err := db.Exec(`INSERT INTO bignum_precision_test VALUES (4, $1)`, belowInt64Min)
		require.NoError(t, err)

		var value string
		err = db.QueryRow(`SELECT value FROM bignum_precision_test WHERE id = 4`).Scan(&value)
		require.NoError(t, err)

		expected, ok := new(big.Int).SetString(belowInt64Min, 10)
		require.True(t, ok)
		actual, ok := new(big.Int).SetString(value, 10)
		require.True(t, ok)
		assert.Equal(t, 0, expected.Cmp(actual))
	})

	t.Run("1000-digit number", func(t *testing.T) {
		// Generate a 1000-digit number
		largeNum := "1"
		for range 999 {
			largeNum += "0"
		}
		_, err := db.Exec(`INSERT INTO bignum_precision_test VALUES (5, $1)`, largeNum)
		require.NoError(t, err)

		var value string
		err = db.QueryRow(`SELECT value FROM bignum_precision_test WHERE id = 5`).Scan(&value)
		require.NoError(t, err)

		expected, ok := new(big.Int).SetString(largeNum, 10)
		require.True(t, ok)
		actual, ok := new(big.Int).SetString(value, 10)
		require.True(t, ok)
		assert.Equal(t, 0, expected.Cmp(actual))
	})
}

// TestBignumArithmetic tests that BIGNUM values maintain precision for arithmetic operations.
// Note: This tests the Go-side arithmetic precision, not SQL-side arithmetic.
func TestBignumArithmetic(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with BIGNUM column
	_, err = db.Exec(`CREATE TABLE bignum_arith_test (id INTEGER, value BIGNUM)`)
	require.NoError(t, err)

	t.Run("Store and retrieve values for external arithmetic", func(t *testing.T) {
		// Store two large numbers
		a := "123456789012345678901234567890"
		b := "987654321098765432109876543210"
		_, err := db.Exec(`INSERT INTO bignum_arith_test VALUES (1, $1)`, a)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO bignum_arith_test VALUES (2, $1)`, b)
		require.NoError(t, err)

		// Retrieve them
		var valueA, valueB string
		err = db.QueryRow(`SELECT value FROM bignum_arith_test WHERE id = 1`).Scan(&valueA)
		require.NoError(t, err)
		err = db.QueryRow(`SELECT value FROM bignum_arith_test WHERE id = 2`).Scan(&valueB)
		require.NoError(t, err)

		// Perform arithmetic in Go
		bigA, ok := new(big.Int).SetString(valueA, 10)
		require.True(t, ok)
		bigB, ok := new(big.Int).SetString(valueB, 10)
		require.True(t, ok)

		// Addition
		sum := new(big.Int).Add(bigA, bigB)
		expectedSum, ok := new(big.Int).SetString("1111111110111111111011111111100", 10)
		require.True(t, ok)
		assert.Equal(t, 0, expectedSum.Cmp(sum))

		// Multiplication
		product := new(big.Int).Mul(bigA, bigB)
		expectedProduct, ok := new(big.Int).SetString("121932631137021795226185032733622923332237463801111263526900", 10)
		require.True(t, ok)
		assert.Equal(t, 0, expectedProduct.Cmp(product))

		// Store the result back
		_, err = db.Exec(`INSERT INTO bignum_arith_test VALUES (3, $1)`, sum.String())
		require.NoError(t, err)

		// Verify it roundtrips
		var storedSum string
		err = db.QueryRow(`SELECT value FROM bignum_arith_test WHERE id = 3`).Scan(&storedSum)
		require.NoError(t, err)

		actualSum, ok := new(big.Int).SetString(storedSum, 10)
		require.True(t, ok)
		assert.Equal(t, 0, sum.Cmp(actualSum))
	})
}
