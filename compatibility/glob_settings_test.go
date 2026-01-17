package compatibility

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go/internal/engine"
)

func TestGlobSettingsSetCommand(t *testing.T) {
	// Open a database connection
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test SET max_files_per_glob
	_, err = db.Exec("SET max_files_per_glob = 50000")
	if err != nil {
		t.Errorf("SET max_files_per_glob failed: %v", err)
	}

	// Verify the value with SHOW
	var value string
	err = db.QueryRow("SHOW max_files_per_glob").Scan(&value)
	if err != nil {
		t.Errorf("SHOW max_files_per_glob failed: %v", err)
	}
	if value != "50000" {
		t.Errorf("SHOW max_files_per_glob = %q, want \"50000\"", value)
	}

	// Test SET file_glob_timeout
	_, err = db.Exec("SET file_glob_timeout = 120")
	if err != nil {
		t.Errorf("SET file_glob_timeout failed: %v", err)
	}

	// Verify the value with SHOW
	err = db.QueryRow("SHOW file_glob_timeout").Scan(&value)
	if err != nil {
		t.Errorf("SHOW file_glob_timeout failed: %v", err)
	}
	if value != "120" {
		t.Errorf("SHOW file_glob_timeout = %q, want \"120\"", value)
	}
}

func TestGlobSettingsSetCommandValidation(t *testing.T) {
	// Open a database connection
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test invalid max_files_per_glob values
	invalidMaxFiles := []string{
		"0",       // Below minimum
		"1000001", // Above maximum
		"-1",      // Negative
	}

	for _, val := range invalidMaxFiles {
		_, err = db.Exec("SET max_files_per_glob = " + val)
		if err == nil {
			t.Errorf("SET max_files_per_glob = %s should have failed", val)
		}
	}

	// Test invalid file_glob_timeout values
	invalidTimeout := []string{
		"0",   // Below minimum
		"601", // Above maximum
		"-1",  // Negative
	}

	for _, val := range invalidTimeout {
		_, err = db.Exec("SET file_glob_timeout = " + val)
		if err == nil {
			t.Errorf("SET file_glob_timeout = %s should have failed", val)
		}
	}
}

func TestGlobSettingsConnectionString(t *testing.T) {
	// Test that glob settings are properly passed through connection string
	db, err := sql.Open("dukdb", ":memory:?max_files_per_glob=25000&file_glob_timeout=90")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Verify max_files_per_glob
	var maxFiles string
	err = db.QueryRow("SHOW max_files_per_glob").Scan(&maxFiles)
	if err != nil {
		t.Errorf("SHOW max_files_per_glob failed: %v", err)
	}
	if maxFiles != "25000" {
		t.Errorf("SHOW max_files_per_glob = %q, want \"25000\"", maxFiles)
	}

	// Verify file_glob_timeout
	var timeout string
	err = db.QueryRow("SHOW file_glob_timeout").Scan(&timeout)
	if err != nil {
		t.Errorf("SHOW file_glob_timeout failed: %v", err)
	}
	if timeout != "90" {
		t.Errorf("SHOW file_glob_timeout = %q, want \"90\"", timeout)
	}
}

func TestGlobSettingsDefaultValues(t *testing.T) {
	// Test that default values are properly set
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Verify default max_files_per_glob (10000)
	var maxFiles string
	err = db.QueryRow("SHOW max_files_per_glob").Scan(&maxFiles)
	if err != nil {
		t.Errorf("SHOW max_files_per_glob failed: %v", err)
	}
	if maxFiles != "10000" {
		t.Errorf("SHOW max_files_per_glob = %q, want \"10000\"", maxFiles)
	}

	// Verify default file_glob_timeout (60)
	var timeout string
	err = db.QueryRow("SHOW file_glob_timeout").Scan(&timeout)
	if err != nil {
		t.Errorf("SHOW file_glob_timeout failed: %v", err)
	}
	if timeout != "60" {
		t.Errorf("SHOW file_glob_timeout = %q, want \"60\"", timeout)
	}
}

func TestGlobSettingsPerConnectionIsolation(t *testing.T) {
	// Test that settings are isolated per connection
	db1, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database 1: %v", err)
	}
	defer func() { _ = db1.Close() }()

	db2, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database 2: %v", err)
	}
	defer func() { _ = db2.Close() }()

	// Set different values on each connection
	_, err = db1.Exec("SET max_files_per_glob = 20000")
	if err != nil {
		t.Errorf("SET max_files_per_glob on db1 failed: %v", err)
	}

	_, err = db2.Exec("SET max_files_per_glob = 30000")
	if err != nil {
		t.Errorf("SET max_files_per_glob on db2 failed: %v", err)
	}

	// Verify they have different values
	var value1, value2 string
	err = db1.QueryRow("SHOW max_files_per_glob").Scan(&value1)
	if err != nil {
		t.Errorf("SHOW max_files_per_glob on db1 failed: %v", err)
	}
	err = db2.QueryRow("SHOW max_files_per_glob").Scan(&value2)
	if err != nil {
		t.Errorf("SHOW max_files_per_glob on db2 failed: %v", err)
	}

	if value1 != "20000" {
		t.Errorf("db1 max_files_per_glob = %q, want \"20000\"", value1)
	}
	if value2 != "30000" {
		t.Errorf("db2 max_files_per_glob = %q, want \"30000\"", value2)
	}
}
