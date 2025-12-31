// Package compatibility provides a test framework for verifying dukdb-go
// compatibility with the duckdb-go reference implementation.
package compatibility

import (
	"database/sql"
	"testing"

	"github.com/coder/quartz"

	// Import dukdb driver and engine (engine init() registers the backend)
	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

// CompatibilityTest represents a single compatibility test case.
// Each test should be self-contained: create its own tables, insert data,
// and clean up after itself.
type CompatibilityTest struct {
	// Name is the unique identifier for this test
	Name string
	// Category groups tests (e.g., "sql", "api", "type", "udf", "appender")
	Category string
	// Setup prepares the database for the test (optional)
	Setup func(db *sql.DB) error
	// Test is the actual test function
	Test func(t *testing.T, db *sql.DB)
	// Teardown cleans up after the test (optional)
	Teardown func(db *sql.DB) error
	// SkipDukdb marks this test as skipped for dukdb-go (not yet implemented)
	SkipDukdb bool
	// SkipDuckdb marks this test as skipped for duckdb-go (CGO not available)
	SkipDuckdb bool
}

// DriverAdapter abstracts the difference between dukdb-go and duckdb-go.
// This allows running the same tests against both implementations.
type DriverAdapter interface {
	// Open opens a database connection
	Open(dsn string) (*sql.DB, error)
	// OpenWithConfig opens with explicit configuration
	OpenWithConfig(dsn string, config map[string]string) (*sql.DB, error)
	// Name returns the driver name
	Name() string
	// SupportsArrow returns true if the driver supports Apache Arrow
	SupportsArrow() bool
	// SupportsTableUDF returns true if the driver supports table-valued UDFs
	SupportsTableUDF() bool
	// SupportsScalarUDF returns true if the driver supports scalar UDFs
	SupportsScalarUDF() bool
	// SupportsAggregateUDF returns true if the driver supports aggregate UDFs
	SupportsAggregateUDF() bool
	// WithClock returns a new adapter with clock injection for deterministic testing
	WithClock(clock quartz.Clock) DriverAdapter
}

// dukdbAdapter implements DriverAdapter for dukdb-go
type dukdbAdapter struct {
	clock quartz.Clock
}

// newDukdbAdapter creates a new dukdb adapter
func newDukdbAdapter() *dukdbAdapter {
	return &dukdbAdapter{
		clock: quartz.NewReal(),
	}
}

// Open opens a database connection using dukdb-go
func (a *dukdbAdapter) Open(dsn string) (*sql.DB, error) {
	return sql.Open("dukdb", dsn)
}

// OpenWithConfig opens with explicit configuration
func (a *dukdbAdapter) OpenWithConfig(dsn string, config map[string]string) (*sql.DB, error) {
	// Build DSN with config parameters
	fullDSN := dsn
	if len(config) > 0 {
		fullDSN += "?"
		first := true
		for k, v := range config {
			if !first {
				fullDSN += "&"
			}
			fullDSN += k + "=" + v
			first = false
		}
	}

	return sql.Open("dukdb", fullDSN)
}

// Name returns the driver name
func (a *dukdbAdapter) Name() string {
	return "dukdb"
}

// SupportsArrow returns true - dukdb-go supports Arrow
func (a *dukdbAdapter) SupportsArrow() bool {
	return true
}

// SupportsTableUDF returns true - dukdb-go supports table UDFs
func (a *dukdbAdapter) SupportsTableUDF() bool {
	return true
}

// SupportsScalarUDF returns true - dukdb-go supports scalar UDFs
func (a *dukdbAdapter) SupportsScalarUDF() bool {
	return true
}

// SupportsAggregateUDF returns true - dukdb-go supports aggregate UDFs
func (a *dukdbAdapter) SupportsAggregateUDF() bool {
	return true
}

// WithClock returns a new adapter with clock injection
func (a *dukdbAdapter) WithClock(clock quartz.Clock) DriverAdapter {
	return &dukdbAdapter{
		clock: clock,
	}
}

// TestRunner executes compatibility tests against one or both implementations.
type TestRunner struct {
	adapter DriverAdapter
	clock   quartz.Clock
}

// NewTestRunner creates a new test runner with the given clock.
func NewTestRunner(clock quartz.Clock) *TestRunner {
	adapter := newDukdbAdapter()
	if clock != nil {
		adapter = adapter.WithClock(clock).(*dukdbAdapter)
	}

	return &TestRunner{
		adapter: adapter,
		clock:   clock,
	}
}

// OpenDB opens a new in-memory database for testing.
func (r *TestRunner) OpenDB() (*sql.DB, error) {
	return r.adapter.Open(":memory:")
}

// RunTests executes the given tests against the adapter.
func (r *TestRunner) RunTests(t *testing.T, tests []CompatibilityTest) {
	for _, test := range tests {
		// capture range variable
		t.Run(test.Name, func(t *testing.T) {
			// Skip if needed
			if test.SkipDukdb {
				t.Skip("Test skipped for dukdb-go (not yet implemented)")
			}

			// Open fresh database for each test
			db, err := r.OpenDB()
			if err != nil {
				t.Fatalf("Failed to open database: %v", err)
			}
			defer db.Close()

			// Run setup if provided
			if test.Setup != nil {
				if err := test.Setup(db); err != nil {
					t.Fatalf("Setup failed: %v", err)
				}
			}

			// Run the test
			test.Test(t, db)

			// Run teardown if provided
			if test.Teardown != nil {
				if err := test.Teardown(db); err != nil {
					t.Errorf("Teardown failed: %v", err)
				}
			}
		})
	}
}

// RunTestsParallel executes the given tests in parallel.
func (r *TestRunner) RunTestsParallel(t *testing.T, tests []CompatibilityTest) {
	for _, test := range tests {
		// capture range variable
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			// Skip if needed
			if test.SkipDukdb {
				t.Skip("Test skipped for dukdb-go (not yet implemented)")
			}

			// Open fresh database for each test
			db, err := r.OpenDB()
			if err != nil {
				t.Fatalf("Failed to open database: %v", err)
			}
			defer db.Close()

			// Run setup if provided
			if test.Setup != nil {
				if err := test.Setup(db); err != nil {
					t.Fatalf("Setup failed: %v", err)
				}
			}

			// Run the test
			test.Test(t, db)

			// Run teardown if provided
			if test.Teardown != nil {
				if err := test.Teardown(db); err != nil {
					t.Errorf("Teardown failed: %v", err)
				}
			}
		})
	}
}
