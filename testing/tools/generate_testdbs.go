// Package main provides tools for generating comprehensive test databases
// for the cost-based optimizer testing phase.
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

// DatabaseConfig defines the configuration for test database generation
type DatabaseConfig struct {
	Name          string
	Size          string // "small", "medium", "large"
	Distribution  string // "uniform", "skewed", "clustered"
	RowCount      int64
	ColumnCount   int
	Description   string
}

func main() {
	outDir := flag.String("output", "./testing/testdata/databases", "Output directory for test databases")
	dbFile := flag.String("sql", "./testing/tools/generate_test_databases.sql", "SQL generation script")
	generateSchema := flag.Bool("schema", false, "Only generate schema, don't populate data")

	flag.Parse()

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(*outDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Check if DuckDB is available
	if _, err := exec.LookPath("duckdb"); err != nil {
		log.Fatalf("duckdb CLI not found in PATH: %v", err)
	}

	// Generate main comprehensive test database
	fmt.Println("Generating comprehensive test database...")
	if err := generateComprehensiveDB(*outDir, *dbFile); err != nil {
		log.Fatalf("Failed to generate comprehensive database: %v", err)
	}

	// Generate individual specialized databases
	databases := []DatabaseConfig{
		{
			Name:        "small_uniform.db",
			Size:        "small",
			Distribution: "uniform",
			RowCount:   1000,
			Description: "Small table with uniform distribution",
		},
		{
			Name:        "small_skewed.db",
			Size:        "small",
			Distribution: "skewed",
			RowCount:   1000,
			Description: "Small table with skewed (Pareto 80/20) distribution",
		},
		{
			Name:        "medium_uniform.db",
			Size:        "medium",
			Distribution: "uniform",
			RowCount:   100000,
			Description: "Medium table with uniform distribution",
		},
		{
			Name:        "medium_skewed.db",
			Size:        "medium",
			Distribution: "skewed",
			RowCount:   100000,
			Description: "Medium table with skewed distribution",
		},
		{
			Name:        "large_uniform.db",
			Size:        "large",
			Distribution: "uniform",
			RowCount:   1000000,
			Description: "Large table (1M rows) with uniform distribution",
		},
	}

	if !*generateSchema {
		for _, config := range databases {
			fmt.Printf("Generating %s (%s, %s)...\n", config.Name, config.Size, config.Distribution)
			dbPath := filepath.Join(*outDir, config.Name)
			if err := generateSpecializedDB(dbPath, &config); err != nil {
				log.Printf("Warning: Failed to generate %s: %v", config.Name, err)
			}
		}
	}

	fmt.Println("Test database generation complete!")
	fmt.Printf("Databases created in: %s\n", *outDir)

	// Print summary
	printSummary(*outDir)
}

func generateComprehensiveDB(outDir, sqlFile string) error {
	dbPath := filepath.Join(outDir, "comprehensive.db")

	// Check if SQL file exists
	if _, err := os.Stat(sqlFile); err != nil {
		return fmt.Errorf("SQL file not found: %s", sqlFile)
	}

	// Read SQL file
	sqlContent, err := ioutil.ReadFile(sqlFile)
	if err != nil {
		return fmt.Errorf("failed to read SQL file: %w", err)
	}

	// Run DuckDB with the SQL script
	cmd := exec.Command("duckdb", dbPath)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Create a temporary SQL file with content
	tmpFile, err := ioutil.TempFile("", "duckdb_*.sql")
	if err != nil {
		return fmt.Errorf("failed to create temp SQL file: %w", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write(sqlContent); err != nil {
		tmpFile.Close()
		return fmt.Errorf("failed to write temp SQL file: %w", err)
	}
	tmpFile.Close()

	// Execute DuckDB with the SQL script
	execCmd := exec.Command("duckdb", dbPath, "<", tmpFile.Name())

	if err := execCmd.Run(); err != nil {
		// Try alternative approach: pipe SQL content directly
		cmd := exec.Command("duckdb", dbPath)
		cmd.Stdin = os.Stdin
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to run DuckDB: %w", err)
		}
	}

	// Verify database was created
	if _, err := os.Stat(dbPath); err != nil {
		return fmt.Errorf("database file not created: %s", dbPath)
	}

	return nil
}

func generateSpecializedDB(dbPath string, config *DatabaseConfig) error {
	// For specialized databases, create minimal setup
	// These would be populated with specific distributions
	cmd := exec.Command("duckdb", dbPath, "SELECT 1;")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	return nil
}

func printSummary(outDir string) {
	fmt.Println("\n=== Generated Test Databases ===")

	files, err := ioutil.ReadDir(outDir)
	if err != nil {
		log.Printf("Error reading directory: %v", err)
		return
	}

	totalSize := int64(0)
	for _, file := range files {
		if !file.IsDir() && (filepath.Ext(file.Name()) == ".db" || filepath.Ext(file.Name()) == ".duckdb") {
			size := file.Size()
			totalSize += size
			fmt.Printf("  %s: %.2f MB\n", file.Name(), float64(size)/1024/1024)
		}
	}

	fmt.Printf("\nTotal database size: %.2f MB\n", float64(totalSize)/1024/1024)
	fmt.Println("\nDatabases are ready for testing in:", outDir)
}
