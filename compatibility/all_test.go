package compatibility

import (
	"testing"

	"github.com/coder/quartz"
)

// TestAllCompatibility runs all compatibility tests.
// Use subtest names to run specific categories:
//
//	go test -v -run TestAllCompatibility/SQL ./compatibility/...
//	go test -v -run TestAllCompatibility/Types ./compatibility/...
//	go test -v -run TestAllCompatibility/API ./compatibility/...
//	go test -v -run TestAllCompatibility/Errors ./compatibility/...
//	go test -v -run TestAllCompatibility/Features ./compatibility/...
func TestAllCompatibility(t *testing.T) {
	mClock := quartz.NewMock(t)
	runner := NewTestRunner(mClock)

	t.Run("SQL", func(t *testing.T) {
		runner.RunTests(t, SQLCompatibilityTests)
	})

	t.Run("Types", func(t *testing.T) {
		runner.RunTests(t, TypeCompatibilityTests)
	})

	t.Run("API", func(t *testing.T) {
		runner.RunTests(t, APICompatibilityTests)
	})

	t.Run("Errors", func(t *testing.T) {
		runner.RunTests(
			t,
			ErrorCompatibilityTests,
		)
	})

	t.Run("Features", func(t *testing.T) {
		runner.RunTests(
			t,
			FeatureCompatibilityTests,
		)
	})

	t.Run("DML", func(t *testing.T) {
		runner.RunTests(t, DMLCompatibilityTests)
	})

	t.Run("GetTableNames", func(t *testing.T) {
		runner.RunTests(t, GetTableNamesCompatibilityTests)
	})

	t.Run("Window", func(t *testing.T) {
		runner.RunTests(t, WindowCompatibilityTests)
	})
}

// TestSQLCompatibility runs only SQL compatibility tests.
func TestSQLCompatibility(t *testing.T) {
	runner := NewTestRunner(nil)
	runner.RunTests(t, SQLCompatibilityTests)
}

// TestTypeCompatibility runs only type compatibility tests.
func TestTypeCompatibility(t *testing.T) {
	runner := NewTestRunner(nil)
	runner.RunTests(t, TypeCompatibilityTests)
}

// TestAPICompatibility runs only API compatibility tests.
func TestAPICompatibility(t *testing.T) {
	runner := NewTestRunner(nil)
	runner.RunTests(t, APICompatibilityTests)
}

// TestErrorCompatibility runs only error compatibility tests.
func TestErrorCompatibility(t *testing.T) {
	runner := NewTestRunner(nil)
	runner.RunTests(t, ErrorCompatibilityTests)
}

// TestFeatureCompatibility runs only feature compatibility tests.
func TestFeatureCompatibility(t *testing.T) {
	runner := NewTestRunner(nil)
	runner.RunTests(t, FeatureCompatibilityTests)
}

// TestDMLCompatibility runs only DML (UPDATE/DELETE) compatibility tests.
func TestDMLCompatibility(t *testing.T) {
	runner := NewTestRunner(nil)
	runner.RunTests(t, DMLCompatibilityTests)
}

// TestGetTableNamesCompatibility runs only GetTableNames compatibility tests.
func TestGetTableNamesCompatibility(t *testing.T) {
	runner := NewTestRunner(nil)
	runner.RunTests(t, GetTableNamesCompatibilityTests)
}

// TestQuickCompatibility runs a subset of tests for quick validation.
// Useful for CI or quick local checks.
func TestQuickCompatibility(t *testing.T) {
	runner := NewTestRunner(nil)

	// Quick SQL tests
	quickSQLTests := []CompatibilityTest{
		{
			Name:     "QuickCreateTable",
			Category: "sql",
			Test:     testCreateTable,
		},
		{
			Name:     "QuickInsertValues",
			Category: "sql",
			Test:     testInsertValues,
		},
		{
			Name:     "QuickSelectStar",
			Category: "sql",
			Test:     testSelectStar,
		},
		{
			Name:     "QuickSelectWhere",
			Category: "sql",
			Test:     testSelectWhere,
		},
	}

	// Quick type tests
	quickTypeTests := []CompatibilityTest{
		{
			Name:     "QuickInteger",
			Category: "type",
			Test:     testTypeInteger,
		},
		{
			Name:     "QuickVarchar",
			Category: "type",
			Test:     testTypeVarchar,
		},
		{
			Name:     "QuickBoolean",
			Category: "type",
			Test:     testTypeBoolean,
		},
	}

	// Quick API tests
	quickAPITests := []CompatibilityTest{
		{
			Name:     "QuickOpenClose",
			Category: "api",
			Test:     testOpenClose,
		},
		{
			Name:     "QuickPing",
			Category: "api",
			Test:     testPing,
		},
		{
			Name:     "QuickBeginCommit",
			Category: "api",
			Test:     testBeginCommit,
		},
	}

	t.Run("QuickSQL", func(t *testing.T) {
		runner.RunTests(t, quickSQLTests)
	})

	t.Run("QuickTypes", func(t *testing.T) {
		runner.RunTests(t, quickTypeTests)
	})

	t.Run("QuickAPI", func(t *testing.T) {
		runner.RunTests(t, quickAPITests)
	})
}

// CompatibilityReport generates a summary of all compatibility tests.
type CompatibilityReport struct {
	TotalTests   int
	PassedTests  int
	FailedTests  int
	SkippedTests int
	Categories   map[string]CategoryReport
}

// CategoryReport represents test results for a single category.
type CategoryReport struct {
	Name    string
	Total   int
	Passed  int
	Failed  int
	Skipped int
}

// GetTestCounts returns the total number of tests in each category.
func GetTestCounts() map[string]int {
	return map[string]int{
		"SQL":           len(SQLCompatibilityTests),
		"Types":         len(TypeCompatibilityTests),
		"API":           len(APICompatibilityTests),
		"Errors":        len(ErrorCompatibilityTests),
		"Features":      len(FeatureCompatibilityTests),
		"DML":           len(DMLCompatibilityTests),
		"GetTableNames": len(GetTableNamesCompatibilityTests),
		"Window":        len(WindowCompatibilityTests),
	}
}

// GetTotalTestCount returns the total number of compatibility tests.
func GetTotalTestCount() int {
	counts := GetTestCounts()
	total := 0
	for _, c := range counts {
		total += c
	}

	return total
}
