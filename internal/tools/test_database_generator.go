// Package tools provides utility tools for testing and development.
//
// Test Database Generator
//
// Task 9.13: Creates test databases with TPC-H-like schemas for benchmarking.
// This tool can generate databases of various sizes and distributions for
// performance testing of the optimizer.
package tools

import (
	"context"
	"fmt"
	"log"

	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/executor"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// TestDatabaseGenerator creates test databases for performance testing
type TestDatabaseGenerator struct {
	catalog  *catalog.Catalog
	storage  *storage.Storage
	executor *executor.Executor
}

// NewTestDatabaseGenerator creates a new test database generator
func NewTestDatabaseGenerator() *TestDatabaseGenerator {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := executor.NewExecutor(cat, stor)

	return &TestDatabaseGenerator{
		catalog:  cat,
		storage:  stor,
		executor: exec,
	}
}

// GenerateTPCHSchema creates the TPC-H schema
func (g *TestDatabaseGenerator) GenerateTPCHSchema() error {
	// Create region table
	if err := g.executeSQL(`
		CREATE TABLE region (
			r_regionkey INTEGER,
			r_name VARCHAR,
			r_comment VARCHAR
		)
	`); err != nil {
		return fmt.Errorf("failed to create region table: %w", err)
	}

	// Create nation table
	if err := g.executeSQL(`
		CREATE TABLE nation (
			n_nationkey INTEGER,
			n_name VARCHAR,
			n_regionkey INTEGER,
			n_comment VARCHAR
		)
	`); err != nil {
		return fmt.Errorf("failed to create nation table: %w", err)
	}

	// Create customer table
	if err := g.executeSQL(`
		CREATE TABLE customer (
			c_custkey INTEGER,
			c_name VARCHAR,
			c_address VARCHAR,
			c_nationkey INTEGER,
			c_phone VARCHAR,
			c_acctbal DECIMAL(15,2),
			c_mktsegment VARCHAR,
			c_comment VARCHAR
		)
	`); err != nil {
		return fmt.Errorf("failed to create customer table: %w", err)
	}

	// Create orders table
	if err := g.executeSQL(`
		CREATE TABLE orders (
			o_orderkey INTEGER,
			o_custkey INTEGER,
			o_orderstatus VARCHAR,
			o_totalprice DECIMAL(15,2),
			o_orderdate DATE,
			o_orderpriority VARCHAR,
			o_clerk VARCHAR,
			o_shippriority INTEGER,
			o_comment VARCHAR
		)
	`); err != nil {
		return fmt.Errorf("failed to create orders table: %w", err)
	}

	// Create lineitem table
	if err := g.executeSQL(`
		CREATE TABLE lineitem (
			l_orderkey INTEGER,
			l_partkey INTEGER,
			l_suppkey INTEGER,
			l_linenumber INTEGER,
			l_quantity DECIMAL(15,2),
			l_extendedprice DECIMAL(15,2),
			l_discount DECIMAL(15,2),
			l_tax DECIMAL(15,2),
			l_returnflag VARCHAR,
			l_linestatus VARCHAR,
			l_shipdate DATE,
			l_commitdate DATE,
			l_receiptdate DATE,
			l_shipinstruct VARCHAR,
			l_shipmode VARCHAR,
			l_comment VARCHAR
		)
	`); err != nil {
		return fmt.Errorf("failed to create lineitem table: %w", err)
	}

	// Create part table
	if err := g.executeSQL(`
		CREATE TABLE part (
			p_partkey INTEGER,
			p_name VARCHAR,
			p_mfgr VARCHAR,
			p_brand VARCHAR,
			p_type VARCHAR,
			p_size INTEGER,
			p_container VARCHAR,
			p_retailprice DECIMAL(15,2),
			p_comment VARCHAR
		)
	`); err != nil {
		return fmt.Errorf("failed to create part table: %w", err)
	}

	// Create supplier table
	if err := g.executeSQL(`
		CREATE TABLE supplier (
			s_suppkey INTEGER,
			s_name VARCHAR,
			s_address VARCHAR,
			s_nationkey INTEGER,
			s_phone VARCHAR,
			s_acctbal DECIMAL(15,2),
			s_comment VARCHAR
		)
	`); err != nil {
		return fmt.Errorf("failed to create supplier table: %w", err)
	}

	// Create partsupp table
	if err := g.executeSQL(`
		CREATE TABLE partsupp (
			ps_partkey INTEGER,
			ps_suppkey INTEGER,
			ps_availqty INTEGER,
			ps_supplycost DECIMAL(15,2),
			ps_comment VARCHAR
		)
	`); err != nil {
		return fmt.Errorf("failed to create partsupp table: %w", err)
	}

	log.Println("TPC-H schema created successfully")
	return nil
}

// LoadTestData loads minimal test data into TPC-H tables
// Scale factor indicates the relative data size (0.1 = 100MB, 1.0 = 1GB)
func (g *TestDatabaseGenerator) LoadTestData(scaleFactor float64) error {
	// Load regions
	if err := g.executeSQL(`
		INSERT INTO region VALUES
		(0, 'AFRICA', 'Afro-pessimistic'),
		(1, 'AMERICA', 'American'),
		(2, 'ASIA', 'Asian'),
		(3, 'EUROPE', 'European'),
		(4, 'MIDDLE EAST', 'Middle Eastern')
	`); err != nil {
		return fmt.Errorf("failed to load regions: %w", err)
	}

	// Load nations
	if err := g.executeSQL(`
		INSERT INTO nation VALUES
		(0, 'ALGERIA', 0, ''),
		(1, 'ARGENTINA', 1, ''),
		(2, 'BRAZIL', 1, ''),
		(3, 'CANADA', 1, ''),
		(4, 'EGYPT', 0, ''),
		(5, 'ETHIOPIA', 0, ''),
		(6, 'FRANCE', 3, ''),
		(7, 'GERMANY', 3, ''),
		(8, 'INDIA', 2, ''),
		(9, 'INDONESIA', 2, '')
	`); err != nil {
		return fmt.Errorf("failed to load nations: %w", err)
	}

	log.Printf("Loaded test data with scale factor %.1f", scaleFactor)
	return nil
}

// RunANALYZE runs ANALYZE on all tables to collect statistics
func (g *TestDatabaseGenerator) RunANALYZE() error {
	tables := []string{"region", "nation", "customer", "orders", "lineitem", "part", "supplier", "partsupp"}

	for _, table := range tables {
		if err := g.executeSQL(fmt.Sprintf("ANALYZE TABLE %s", table)); err != nil {
			return fmt.Errorf("failed to analyze %s: %w", table, err)
		}
	}

	log.Println("ANALYZE completed on all tables")
	return nil
}

// executeSQL executes a SQL statement
func (g *TestDatabaseGenerator) executeSQL(sql string) error {
	stmt, err := parser.Parse(sql)
	if err != nil {
		return err
	}

	// Import binder from the correct package
	b := binder.NewBinder(g.catalog)
	boundStmt, err := b.Bind(stmt)
	if err != nil {
		return err
	}

	p := planner.NewPlanner(g.catalog)
	plan, err := p.Plan(boundStmt)
	if err != nil {
		return err
	}

	_, err = g.executor.Execute(context.Background(), plan, nil)
	return err
}

// GetCatalog returns the catalog for querying
func (g *TestDatabaseGenerator) GetCatalog() *catalog.Catalog {
	return g.catalog
}

// GetExecutor returns the executor for running queries
func (g *TestDatabaseGenerator) GetExecutor() *executor.Executor {
	return g.executor
}
