package executor

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUniqueConstraint_ColumnLevel(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table with column-level UNIQUE
	_, err := executeSQL(t, exec, cat,
		"CREATE TABLE users (id INTEGER PRIMARY KEY, email VARCHAR UNIQUE)")
	require.NoError(t, err)

	// Insert first row
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO users (id, email) VALUES (1, 'alice@test.com')")
	require.NoError(t, err)

	// Insert second row with different email - should succeed
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO users (id, email) VALUES (2, 'bob@test.com')")
	require.NoError(t, err)

	// Insert with duplicate email - should fail
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO users (id, email) VALUES (3, 'alice@test.com')")
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "unique constraint"),
		"expected unique constraint error, got: %s", err.Error())
}

func TestUniqueConstraint_TableLevel(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table with table-level UNIQUE
	_, err := executeSQL(t, exec, cat,
		"CREATE TABLE products (id INTEGER PRIMARY KEY, sku VARCHAR, name VARCHAR, UNIQUE (sku))")
	require.NoError(t, err)

	// Insert first row
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO products (id, sku, name) VALUES (1, 'SKU-001', 'Widget')")
	require.NoError(t, err)

	// Insert with different SKU - should succeed
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO products (id, sku, name) VALUES (2, 'SKU-002', 'Gadget')")
	require.NoError(t, err)

	// Insert with duplicate SKU - should fail
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO products (id, sku, name) VALUES (3, 'SKU-001', 'Thingamajig')")
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "unique constraint"),
		"expected unique constraint error, got: %s", err.Error())
}

func TestUniqueConstraint_CompositeTableLevel(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table with composite UNIQUE constraint
	_, err := executeSQL(t, exec, cat,
		"CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, product_id INTEGER, UNIQUE (customer_id, product_id))")
	require.NoError(t, err)

	// Insert first order
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO orders (id, customer_id, product_id) VALUES (1, 100, 200)")
	require.NoError(t, err)

	// Same customer, different product - should succeed
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO orders (id, customer_id, product_id) VALUES (2, 100, 201)")
	require.NoError(t, err)

	// Different customer, same product - should succeed
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO orders (id, customer_id, product_id) VALUES (3, 101, 200)")
	require.NoError(t, err)

	// Same customer and product - should fail
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO orders (id, customer_id, product_id) VALUES (4, 100, 200)")
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "unique constraint"),
		"expected unique constraint error, got: %s", err.Error())
}

func TestUniqueConstraint_NullAllowed(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table with UNIQUE column
	_, err := executeSQL(t, exec, cat,
		"CREATE TABLE items (id INTEGER PRIMARY KEY, code VARCHAR UNIQUE)")
	require.NoError(t, err)

	// Insert with NULL code - should succeed
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO items (id, code) VALUES (1, NULL)")
	require.NoError(t, err)

	// Insert another NULL code - should succeed (NULLs don't violate UNIQUE per SQL standard)
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO items (id, code) VALUES (2, NULL)")
	require.NoError(t, err)
}

func TestUniqueConstraint_NamedConstraint(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table with named UNIQUE constraint
	_, err := executeSQL(t, exec, cat,
		"CREATE TABLE employees (id INTEGER PRIMARY KEY, badge VARCHAR, CONSTRAINT uq_badge UNIQUE (badge))")
	require.NoError(t, err)

	_, err = executeSQL(t, exec, cat,
		"INSERT INTO employees (id, badge) VALUES (1, 'B001')")
	require.NoError(t, err)

	// Duplicate badge - should fail
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO employees (id, badge) VALUES (2, 'B001')")
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "unique constraint"),
		"expected unique constraint error, got: %s", err.Error())
}

func TestCheckConstraint_ColumnLevel(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table with column-level CHECK
	_, err := executeSQL(t, exec, cat,
		"CREATE TABLE scores (id INTEGER PRIMARY KEY, value INTEGER CHECK (value >= 0))")
	require.NoError(t, err)

	// Insert valid value
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO scores (id, value) VALUES (1, 42)")
	require.NoError(t, err)

	// Insert zero - should succeed
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO scores (id, value) VALUES (2, 0)")
	require.NoError(t, err)

	// Insert negative - should fail
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO scores (id, value) VALUES (3, -1)")
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "CHECK constraint"),
		"expected CHECK constraint error, got: %s", err.Error())
}

func TestCheckConstraint_TableLevel(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table with table-level CHECK
	_, err := executeSQL(t, exec, cat,
		"CREATE TABLE ranges (id INTEGER PRIMARY KEY, low INTEGER, high INTEGER, CHECK (low <= high))")
	require.NoError(t, err)

	// Valid range
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO ranges (id, low, high) VALUES (1, 10, 20)")
	require.NoError(t, err)

	// Equal values - should succeed
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO ranges (id, low, high) VALUES (2, 15, 15)")
	require.NoError(t, err)

	// Invalid range (low > high) - should fail
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO ranges (id, low, high) VALUES (3, 20, 10)")
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "CHECK constraint"),
		"expected CHECK constraint error, got: %s", err.Error())
}

func TestCheckConstraint_NullPasses(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table with CHECK constraint
	_, err := executeSQL(t, exec, cat,
		"CREATE TABLE nullable_check (id INTEGER PRIMARY KEY, value INTEGER CHECK (value > 0))")
	require.NoError(t, err)

	// NULL should pass CHECK per SQL standard
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO nullable_check (id, value) VALUES (1, NULL)")
	require.NoError(t, err)
}

func TestConstraint_ParserPreservation(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create table with both UNIQUE and CHECK constraints
	_, err := executeSQL(t, exec, cat,
		"CREATE TABLE multi_constraint (id INTEGER PRIMARY KEY, email VARCHAR UNIQUE, age INTEGER CHECK (age >= 0))")
	require.NoError(t, err)

	// Verify table was created by inserting a valid row
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO multi_constraint (id, email, age) VALUES (1, 'test@example.com', 25)")
	require.NoError(t, err)

	// Verify UNIQUE enforcement
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO multi_constraint (id, email, age) VALUES (2, 'test@example.com', 30)")
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "unique constraint"),
		"expected unique constraint error, got: %s", err.Error())

	// Verify CHECK enforcement
	_, err = executeSQL(t, exec, cat,
		"INSERT INTO multi_constraint (id, email, age) VALUES (3, 'other@example.com', -1)")
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "CHECK constraint"),
		"expected CHECK constraint error, got: %s", err.Error())
}
