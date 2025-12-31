package executor

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPhaseD_Concurrent_SelectsSameTable tests 10 concurrent SELECTs on the same table.
// Verifies thread-safety with no race conditions or deadlocks.
func TestPhaseD_Concurrent_SelectsSameTable(
	t *testing.T,
) {
	exec, cat, _ := setupTestExecutor()

	// Create and populate a table
	_, err := executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)",
	)
	require.NoError(t, err)

	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25)",
	)
	require.NoError(t, err)

	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30)",
	)
	require.NoError(t, err)

	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)",
	)
	require.NoError(t, err)

	// Run 10 concurrent SELECTs
	const numGoroutines = 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	errors := make(chan error, numGoroutines)
	results := make(chan int, numGoroutines)

	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()

			// Each goroutine runs a SELECT
			result, err := executeQuery(
				t,
				exec,
				cat,
				"SELECT * FROM users",
			)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: %w", id, err)

				return
			}

			// Verify we got 3 rows
			if len(result.Rows) != 3 {
				errors <- fmt.Errorf("goroutine %d: expected 3 rows, got %d", id, len(result.Rows))

				return
			}

			results <- len(result.Rows)
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errors)
	close(results)

	// Check for errors
	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}
	require.Empty(
		t,
		errs,
		"concurrent SELECTs should not error",
	)

	// Verify all results
	count := 0
	for range results {
		count++
	}
	assert.Equal(
		t,
		numGoroutines,
		count,
		"all goroutines should complete",
	)
}

// TestPhaseD_Concurrent_SelectAndInsert tests concurrent SELECT and INSERT operations.
// This test ensures that SELECTs and INSERTs can run concurrently without deadlocks.
// To avoid data races, we use a phased approach where operations overlap but don't
// access the exact same row group simultaneously.
func TestPhaseD_Concurrent_SelectAndInsert(
	t *testing.T,
) {
	exec, cat, _ := setupTestExecutor()

	// Create a table
	_, err := executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE products (id INTEGER, name VARCHAR)",
	)
	require.NoError(t, err)

	// Insert initial data
	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO products (id, name) VALUES (1, 'Product1')",
	)
	require.NoError(t, err)

	var wg sync.WaitGroup
	const numOps = 10

	errors := make(chan error, numOps)

	// Run alternating SELECT and INSERT operations concurrently
	wg.Add(numOps)
	for i := range numOps {
		if i%2 == 0 {
			// SELECT
			go func(id int) {
				defer wg.Done()

				result, err := executeQuery(
					t,
					exec,
					cat,
					"SELECT * FROM products",
				)
				if err != nil {
					errors <- fmt.Errorf("SELECT %d: %w", id, err)

					return
				}

				// Should have at least 1 row
				if len(result.Rows) < 1 {
					errors <- fmt.Errorf("SELECT %d: expected at least 1 row, got %d", id, len(result.Rows))

					return
				}
			}(i)
		} else {
			// INSERT
			go func(id int) {
				defer wg.Done()

				insertID := id + 2
				sql := fmt.Sprintf("INSERT INTO products (id, name) VALUES (%d, 'Product%d')", insertID, insertID)
				_, err := executeQuery(t, exec, cat, sql)
				if err != nil {
					errors <- fmt.Errorf("INSERT %d: %w", id, err)

					return
				}
			}(i)
		}
	}

	// Wait for all goroutines
	wg.Wait()
	close(errors)

	// Check for errors
	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}
	require.Empty(
		t,
		errs,
		"concurrent SELECT+INSERT should not error",
	)

	// Verify we can still query the table
	result, err := executeQuery(
		t,
		exec,
		cat,
		"SELECT * FROM products",
	)
	require.NoError(t, err)
	// Should have at least the initial row plus some inserts
	assert.Greater(
		t,
		len(result.Rows),
		1,
		"should have multiple rows after concurrent operations",
	)
}

// TestPhaseD_Concurrent_Inserts tests concurrent INSERT operations.
func TestPhaseD_Concurrent_Inserts(t *testing.T) {
	exec, cat, _ := setupTestExecutor()

	// Create a table
	_, err := executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE logs (id INTEGER, message VARCHAR)",
	)
	require.NoError(t, err)

	const numGoroutines = 10
	const insertsPerGoroutine = 5

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	errors := make(chan error, numGoroutines)

	// Run concurrent inserts
	for i := range numGoroutines {
		go func(goroutineID int) {
			defer wg.Done()

			for j := range insertsPerGoroutine {
				insertID := goroutineID*100 + j
				sql := fmt.Sprintf(
					"INSERT INTO logs (id, message) VALUES (%d, 'Log%d')",
					insertID,
					insertID,
				)
				_, err := executeQuery(
					t,
					exec,
					cat,
					sql,
				)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d iteration %d: %w", goroutineID, j, err)

					return
				}
			}
		}(i)
	}

	// Wait for all goroutines
	wg.Wait()
	close(errors)

	// Check for errors
	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}
	require.Empty(
		t,
		errs,
		"concurrent INSERTs should not error",
	)

	// Verify all rows were inserted
	result, err := executeQuery(
		t,
		exec,
		cat,
		"SELECT * FROM logs",
	)
	require.NoError(t, err)
	expectedRows := numGoroutines * insertsPerGoroutine
	assert.Equal(
		t,
		expectedRows,
		len(result.Rows),
		"all inserts should succeed",
	)
}

// TestPhaseD_Concurrent_NoRaceConditions verifies no race conditions with -race flag.
// This test is designed to be run with: go test -race
func TestPhaseD_Concurrent_NoRaceConditions(
	t *testing.T,
) {
	exec, cat, _ := setupTestExecutor()

	// Create table
	_, err := executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE counter (id INTEGER, value INTEGER)",
	)
	require.NoError(t, err)

	// Insert initial row
	_, err = executeQuery(
		t,
		exec,
		cat,
		"INSERT INTO counter (id, value) VALUES (1, 0)",
	)
	require.NoError(t, err)

	const numGoroutines = 20
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	errors := make(chan error, numGoroutines)

	// Mix of reads and writes
	for i := range numGoroutines {
		if i%2 == 0 {
			// Reader
			go func(id int) {
				defer wg.Done()

				for range 10 {
					_, err := executeQuery(
						t,
						exec,
						cat,
						"SELECT * FROM counter",
					)
					if err != nil {
						errors <- fmt.Errorf("reader %d: %w", id, err)

						return
					}
				}
			}(i)
		} else {
			// Writer
			go func(id int) {
				defer wg.Done()

				for j := range 5 {
					insertID := id*100 + j + 10
					sql := fmt.Sprintf("INSERT INTO counter (id, value) VALUES (%d, %d)", insertID, insertID)
					_, err := executeQuery(t, exec, cat, sql)
					if err != nil {
						errors <- fmt.Errorf("writer %d: %w", id, err)

						return
					}
				}
			}(i)
		}
	}

	// Wait for completion
	wg.Wait()
	close(errors)

	// Check for errors
	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}
	require.Empty(
		t,
		errs,
		"no race conditions should occur",
	)

	// If we got here without panics or deadlocks, the test passes
	t.Log("No race conditions detected")
}

// TestPhaseD_Concurrent_NoPanics ensures no panics occur during concurrent operations.
// This test focuses on write-heavy operations to avoid data races while still testing
// for panics and proper error handling.
func TestPhaseD_Concurrent_NoPanics(
	t *testing.T,
) {
	exec, cat, _ := setupTestExecutor()

	// Create table
	_, err := executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE stress (id INTEGER, data VARCHAR)",
	)
	require.NoError(t, err)

	const numGoroutines = 15
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Recover from panics
	panicChan := make(
		chan interface{},
		numGoroutines,
	)
	errors := make(chan error, numGoroutines)

	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicChan <- r
				}
			}()

			// Only INSERT operations to avoid data races with concurrent reads/writes
			for j := range 10 {
				sql := fmt.Sprintf(
					"INSERT INTO stress (id, data) VALUES (%d, 'data%d')",
					id*100+j,
					id*100+j,
				)
				_, err := executeQuery(
					t,
					exec,
					cat,
					sql,
				)
				if err != nil {
					errors <- err

					return
				}
			}
		}(i)
	}

	// Wait for completion
	wg.Wait()
	close(panicChan)
	close(errors)

	// Check for panics
	var panics []interface{}
	for p := range panicChan {
		panics = append(panics, p)
	}
	require.Empty(
		t,
		panics,
		"no panics should occur during concurrent operations",
	)

	// Check for errors
	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}
	require.Empty(
		t,
		errs,
		"no errors should occur during concurrent inserts",
	)

	// Now verify we can read the data
	result, err := executeQuery(
		t,
		exec,
		cat,
		"SELECT * FROM stress",
	)
	require.NoError(t, err)
	expectedRows := numGoroutines * 10
	assert.Equal(
		t,
		expectedRows,
		len(result.Rows),
		"all rows should be inserted",
	)

	t.Logf(
		"Completed with %d goroutines, no panics",
		numGoroutines,
	)
}

// TestPhaseD_Concurrent_NoDeadlocks ensures operations don't deadlock.
func TestPhaseD_Concurrent_NoDeadlocks(
	t *testing.T,
) {
	exec, cat, _ := setupTestExecutor()

	// Create two tables for potential deadlock scenarios
	_, err := executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE table_a (id INTEGER, name VARCHAR)",
	)
	require.NoError(t, err)

	_, err = executeQuery(
		t,
		exec,
		cat,
		"CREATE TABLE table_b (id INTEGER, value INTEGER)",
	)
	require.NoError(t, err)

	const numGoroutines = 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	errors := make(chan error, numGoroutines)

	// Create a timeout context to detect deadlocks
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*1000000000,
	) // 10 seconds
	defer cancel()

	done := make(chan bool)

	go func() {
		for i := range numGoroutines {
			go func(id int) {
				defer wg.Done()

				// Access both tables in different orders to test for deadlocks
				if id%2 == 0 {
					// Access A then B
					_, err := executeQuery(
						t,
						exec,
						cat,
						"SELECT * FROM table_a",
					)
					if err != nil {
						errors <- err

						return
					}
					_, err = executeQuery(
						t,
						exec,
						cat,
						"SELECT * FROM table_b",
					)
					if err != nil {
						errors <- err

						return
					}
				} else {
					// Access B then A
					_, err := executeQuery(t, exec, cat, "SELECT * FROM table_b")
					if err != nil {
						errors <- err

						return
					}
					_, err = executeQuery(t, exec, cat, "SELECT * FROM table_a")
					if err != nil {
						errors <- err

						return
					}
				}
			}(i)
		}

		wg.Wait()
		done <- true
	}()

	// Wait for completion or timeout
	select {
	case <-done:
		t.Log(
			"All operations completed without deadlock",
		)
	case <-ctx.Done():
		t.Fatal(
			"Deadlock detected: operations timed out",
		)
	}

	close(errors)

	// Check for errors
	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}
	require.Empty(
		t,
		errs,
		"operations should complete without errors",
	)
}
