package duckdb

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFormatDiagnostic_EmptyTable creates an empty table in both dukdb-go and DuckDB CLI,
// then compares the files to identify format differences.
func TestFormatDiagnostic_EmptyTable(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	tmpDir := t.TempDir()
	dukdbPath := filepath.Join(tmpDir, "dukdb.duckdb")
	duckdbPath := filepath.Join(tmpDir, "duckdb.duckdb")

	// Create empty table with dukdb-go
	err := createDukdbTestDB(dukdbPath, false)
	require.NoError(t, err, "Failed to create dukdb-go test DB")

	// Create empty table with DuckDB CLI
	err = createDuckDBCliTestDB(duckdbPath, false)
	require.NoError(t, err, "Failed to create DuckDB CLI test DB")

	// Compare the files
	report, err := CompareDBFiles(dukdbPath, duckdbPath)
	require.NoError(t, err, "Failed to compare files")

	// Log the detailed comparison
	t.Logf("\n=== EMPTY TABLE COMPARISON ===\n%s", report.String())

	// If there are differences, provide hex dumps of problem areas
	if !report.FilesIdentical {
		t.Logf("\n=== HEX DUMPS OF DIFFERENCES ===\n")

		// Group differences by section for hex dumping
		sectionDiffs := make(map[string][]DifferenceLocation)
		for _, diff := range report.Differences {
			sectionDiffs[diff.Section] = append(sectionDiffs[diff.Section], diff)
		}

		for section, diffs := range sectionDiffs {
			if len(diffs) == 0 {
				continue
			}

			// Find the range of differences in this section
			minOffset := diffs[0].Offset
			maxOffset := diffs[0].Offset
			for _, diff := range diffs {
				if diff.Offset < minOffset {
					minOffset = diff.Offset
				}
				if diff.Offset > maxOffset {
					maxOffset = diff.Offset
				}
			}

			// Dump a window around the differences (64 bytes before and after)
			start := minOffset - 64
			if start < 0 {
				start = 0
			}
			length := maxOffset - start + 64

			t.Logf("\n--- Section: %s (offset range: 0x%x - 0x%x) ---", section, minOffset, maxOffset)

			dump1, err := HexDumpRange(dukdbPath, start, length)
			if err != nil {
				t.Logf("Failed to dump dukdb file: %v", err)
			} else {
				t.Logf("\nDukdb-go file (offset 0x%x):\n%s", start, dump1)
			}

			dump2, err := HexDumpRange(duckdbPath, start, length)
			if err != nil {
				t.Logf("Failed to dump DuckDB CLI file: %v", err)
			} else {
				t.Logf("\nDuckDB CLI file (offset 0x%x):\n%s", start, dump2)
			}
		}

		// Also compare headers specifically
		headerComp, err := CompareHeaders(dukdbPath, duckdbPath)
		if err != nil {
			t.Logf("Failed to compare headers: %v", err)
		} else {
			t.Logf("\n=== HEADER COMPARISON ===\n%s", headerComp.String())
		}
	}

	// Summary
	if report.FilesIdentical {
		t.Logf("\nRESULT: Files are IDENTICAL - empty table format matches!")
	} else {
		t.Logf("\nRESULT: Files have %d differences", report.TotalDifferences)
		t.Logf("First difference at offset: 0x%x", report.FirstDifference)
	}
}

// TestFormatDiagnostic_SingleRow creates a table with one row in both systems,
// then compares to identify format differences.
func TestFormatDiagnostic_SingleRow(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	tmpDir := t.TempDir()
	dukdbPath := filepath.Join(tmpDir, "dukdb.duckdb")
	duckdbPath := filepath.Join(tmpDir, "duckdb.duckdb")

	// Create table with one row using dukdb-go
	err := createDukdbTestDB(dukdbPath, true)
	require.NoError(t, err, "Failed to create dukdb-go test DB")

	// Create table with one row using DuckDB CLI
	err = createDuckDBCliTestDB(duckdbPath, true)
	require.NoError(t, err, "Failed to create DuckDB CLI test DB")

	// Compare the files
	report, err := CompareDBFiles(dukdbPath, duckdbPath)
	require.NoError(t, err, "Failed to compare files")

	// Log the detailed comparison
	t.Logf("\n=== SINGLE ROW COMPARISON ===\n%s", report.String())

	// If there are differences, provide hex dumps
	if !report.FilesIdentical {
		t.Logf("\n=== HEX DUMPS OF DIFFERENCES ===\n")

		// Group differences by section
		sectionDiffs := make(map[string][]DifferenceLocation)
		for _, diff := range report.Differences {
			sectionDiffs[diff.Section] = append(sectionDiffs[diff.Section], diff)
		}

		for section, diffs := range sectionDiffs {
			if len(diffs) == 0 {
				continue
			}

			// Find the range
			minOffset := diffs[0].Offset
			maxOffset := diffs[0].Offset
			for _, diff := range diffs {
				if diff.Offset < minOffset {
					minOffset = diff.Offset
				}
				if diff.Offset > maxOffset {
					maxOffset = diff.Offset
				}
			}

			// Dump window
			start := minOffset - 64
			if start < 0 {
				start = 0
			}
			length := maxOffset - start + 128

			t.Logf("\n--- Section: %s (offset range: 0x%x - 0x%x) ---", section, minOffset, maxOffset)

			dump1, err := HexDumpRange(dukdbPath, start, length)
			if err != nil {
				t.Logf("Failed to dump dukdb file: %v", err)
			} else {
				t.Logf("\nDukdb-go file (offset 0x%x):\n%s", start, dump1)
			}

			dump2, err := HexDumpRange(duckdbPath, start, length)
			if err != nil {
				t.Logf("Failed to dump DuckDB CLI file: %v", err)
			} else {
				t.Logf("\nDuckDB CLI file (offset 0x%x):\n%s", start, dump2)
			}
		}

		// Header comparison
		headerComp, err := CompareHeaders(dukdbPath, duckdbPath)
		if err != nil {
			t.Logf("Failed to compare headers: %v", err)
		} else {
			t.Logf("\n=== HEADER COMPARISON ===\n%s", headerComp.String())
		}
	}

	// Summary
	if report.FilesIdentical {
		t.Logf("\nRESULT: Files are IDENTICAL - single row format matches!")
	} else {
		t.Logf("\nRESULT: Files have %d differences", report.TotalDifferences)
		t.Logf("First difference at offset: 0x%x", report.FirstDifference)
	}
}

// TestFormatDiagnostic_MultipleRows creates a table with multiple rows in both systems,
// then compares to identify patterns in format differences.
func TestFormatDiagnostic_MultipleRows(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	tmpDir := t.TempDir()
	dukdbPath := filepath.Join(tmpDir, "dukdb.duckdb")
	duckdbPath := filepath.Join(tmpDir, "duckdb.duckdb")

	// Create table with 10 rows using dukdb-go
	storage, err := CreateDuckDBStorage(dukdbPath, nil)
	require.NoError(t, err)

	tableEntry := NewTableCatalogEntry("test")
	tableEntry.CreateInfo.Schema = "main"
	tableEntry.AddColumn(ColumnDefinition{
		Name: "id",
		Type: TypeInteger,
	})
	tableEntry.AddColumn(ColumnDefinition{
		Name: "name",
		Type: TypeVarchar,
	})

	storage.catalog.AddTable(tableEntry)
	storage.modified = true

	err = storage.Close()
	require.NoError(t, err)

	// Create table with 10 rows using DuckDB CLI
	sql := `
		CREATE TABLE test (id INTEGER, name VARCHAR);
		INSERT INTO test VALUES (1, 'row1'), (2, 'row2'), (3, 'row3'), (4, 'row4'), (5, 'row5'),
		                        (6, 'row6'), (7, 'row7'), (8, 'row8'), (9, 'row9'), (10, 'row10');
	`
	err = runDuckDBSQL(duckdbPath, sql)
	require.NoError(t, err)

	// Compare the files
	report, err := CompareDBFiles(dukdbPath, duckdbPath)
	require.NoError(t, err, "Failed to compare files")

	// Log the detailed comparison
	t.Logf("\n=== MULTIPLE ROWS COMPARISON ===\n%s", report.String())

	// If there are differences, look for patterns
	if !report.FilesIdentical {
		t.Logf("\n=== PATTERN ANALYSIS ===\n")

		// Analyze differences by section
		sectionDiffs := make(map[string][]DifferenceLocation)
		for _, diff := range report.Differences {
			sectionDiffs[diff.Section] = append(sectionDiffs[diff.Section], diff)
		}

		for section, diffs := range sectionDiffs {
			if len(diffs) == 0 {
				continue
			}

			t.Logf("\nSection: %s", section)
			t.Logf("  Total differences: %d", len(diffs))

			// Show first 20 differences in this section
			t.Logf("  First differences:")
			for i, diff := range diffs {
				if i >= 20 {
					t.Logf("  ... and %d more", len(diffs)-20)
					break
				}
				t.Logf("    Offset 0x%08x: dukdb=%02x duckdb=%02x", diff.Offset, diff.Byte1, diff.Byte2)
			}

			// Try to find patterns (e.g., all zeros vs all ones, repeating patterns)
			if len(diffs) > 0 {
				allSameFile1 := true
				allSameFile2 := true
				firstByte1 := diffs[0].Byte1
				firstByte2 := diffs[0].Byte2

				for _, diff := range diffs {
					if diff.Byte1 != firstByte1 {
						allSameFile1 = false
					}
					if diff.Byte2 != firstByte2 {
						allSameFile2 = false
					}
				}

				if allSameFile1 {
					t.Logf("  PATTERN: All dukdb bytes are 0x%02x", firstByte1)
				}
				if allSameFile2 {
					t.Logf("  PATTERN: All DuckDB CLI bytes are 0x%02x", firstByte2)
				}
			}
		}

		// Hex dumps of key sections
		t.Logf("\n=== HEX DUMPS ===\n")

		for section, diffs := range sectionDiffs {
			if len(diffs) == 0 || len(diffs) > 1000 {
				// Skip sections with too many differences
				continue
			}

			minOffset := diffs[0].Offset
			maxOffset := diffs[0].Offset
			for _, diff := range diffs {
				if diff.Offset < minOffset {
					minOffset = diff.Offset
				}
				if diff.Offset > maxOffset {
					maxOffset = diff.Offset
				}
			}

			start := minOffset - 64
			if start < 0 {
				start = 0
			}
			length := maxOffset - start + 128

			t.Logf("\n--- Section: %s ---", section)

			dump1, err := HexDumpRange(dukdbPath, start, length)
			if err == nil {
				t.Logf("\nDukdb-go (0x%x):\n%s", start, dump1)
			}

			dump2, err := HexDumpRange(duckdbPath, start, length)
			if err == nil {
				t.Logf("\nDuckDB CLI (0x%x):\n%s", start, dump2)
			}
		}
	}

	// Summary
	if report.FilesIdentical {
		t.Logf("\nRESULT: Files are IDENTICAL - multiple rows format matches!")
	} else {
		t.Logf("\nRESULT: Files have %d differences across %d sections",
			report.TotalDifferences, len(getSectionNames(report.Differences)))
	}
}

// Helper functions

// createDukdbTestDB creates a test database using dukdb-go.
// If insertRows is true, inserts a single test row (id=1, name='test').
func createDukdbTestDB(path string, insertRows bool) error {
	storage, err := CreateDuckDBStorage(path, nil)
	if err != nil {
		return fmt.Errorf("CreateDuckDBStorage failed: %w", err)
	}

	// Create table
	tableEntry := NewTableCatalogEntry("test")
	tableEntry.CreateInfo.Schema = "main"
	tableEntry.AddColumn(ColumnDefinition{
		Name: "id",
		Type: TypeInteger,
	})
	tableEntry.AddColumn(ColumnDefinition{
		Name: "name",
		Type: TypeVarchar,
	})

	storage.catalog.AddTable(tableEntry)
	storage.modified = true

	// Note: We don't actually insert data rows in this version,
	// because row insertion is not yet implemented in dukdb-go.
	// This test focuses on comparing the catalog/metadata structure.

	if err := storage.Close(); err != nil {
		return fmt.Errorf("Close failed: %w", err)
	}

	return nil
}

// createDuckDBCliTestDB creates a test database using DuckDB CLI.
// If insertRows is true, inserts a single test row (id=1, name='test').
func createDuckDBCliTestDB(path string, insertRows bool) error {
	sql := "CREATE TABLE test (id INTEGER, name VARCHAR);"
	if insertRows {
		sql += " INSERT INTO test VALUES (1, 'test');"
	}

	return runDuckDBSQL(path, sql)
}

// runDuckDBSQL executes SQL in DuckDB CLI.
func runDuckDBSQL(path, sql string) error {
	cmd := exec.Command("duckdb", path, "-c", sql)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("duckdb command failed: %w\nOutput: %s", err, output)
	}
	return nil
}

// getSectionNames returns a list of unique section names from differences.
func getSectionNames(diffs []DifferenceLocation) []string {
	sections := make(map[string]bool)
	for _, diff := range diffs {
		sections[diff.Section] = true
	}

	result := make([]string, 0, len(sections))
	for section := range sections {
		result = append(result, section)
	}
	return result
}
