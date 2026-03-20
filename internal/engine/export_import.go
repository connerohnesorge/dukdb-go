package engine

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
)

// handleExportDatabase exports the entire database to a directory.
// It creates:
//   - schema.sql: DDL statements to recreate all schemas, tables, and views
//   - <table>.csv: CSV data files for each table
//   - load.sql: COPY FROM statements to reload the data
func (c *EngineConn) handleExportDatabase(stmt *parser.ExportDatabaseStmt) (int64, error) {
	// Create export directory
	if err := os.MkdirAll(stmt.Path, 0o755); err != nil {
		return 0, fmt.Errorf("failed to create export directory: %v", err)
	}

	// Generate schema.sql
	var schemaSql strings.Builder

	// Get all schemas
	schemas := c.engine.catalog.ListSchemas()
	// Sort schemas for deterministic output
	sort.Slice(schemas, func(i, j int) bool {
		return schemas[i].Name() < schemas[j].Name()
	})
	for _, schema := range schemas {
		schemaName := schema.Name()
		if schemaName != "main" && schemaName != "" {
			schemaSql.WriteString(
				fmt.Sprintf("CREATE SCHEMA %s;\n", schemaName),
			)
		}
	}

	// Collect all tables from all schemas and generate DDL
	type tableInfo struct {
		schema   string
		tableDef *catalog.TableDef
	}
	var allTables []tableInfo

	for _, schema := range schemas {
		schemaName := schema.Name()
		tables := schema.ListTables()
		// Sort tables for deterministic output
		sort.Slice(tables, func(i, j int) bool {
			return tables[i].Name < tables[j].Name
		})
		for _, table := range tables {
			allTables = append(allTables, tableInfo{
				schema:   schemaName,
				tableDef: table,
			})
			schemaSql.WriteString(generateCreateTableSQL(table))
			schemaSql.WriteString("\n")
		}
	}

	// Get all views from all schemas and generate DDL
	for _, schema := range schemas {
		views := schema.ListViews()
		// Sort views for deterministic output
		sort.Slice(views, func(i, j int) bool {
			return views[i].Name < views[j].Name
		})
		for _, view := range views {
			qualifiedName := view.Name
			if view.Schema != "" && view.Schema != "main" {
				qualifiedName = view.Schema + "." + view.Name
			}
			schemaSql.WriteString(
				fmt.Sprintf("CREATE VIEW %s AS %s;\n", qualifiedName, view.Query),
			)
		}
	}

	// Write schema.sql
	schemaPath := filepath.Join(stmt.Path, "schema.sql")
	if err := os.WriteFile(schemaPath, []byte(schemaSql.String()), 0o644); err != nil {
		return 0, fmt.Errorf("failed to write schema.sql: %v", err)
	}

	// Export table data and generate load.sql
	var loadSql strings.Builder
	for _, tinfo := range allTables {
		dataFile := tinfo.tableDef.Name + ".csv"
		dataPath := filepath.Join(stmt.Path, dataFile)

		if err := c.exportTableToCSV(tinfo.tableDef, dataPath); err != nil {
			return 0, fmt.Errorf("failed to export table %s: %v", tinfo.tableDef.Name, err)
		}

		tableName := tinfo.tableDef.Name
		if tinfo.schema != "" && tinfo.schema != "main" {
			tableName = tinfo.schema + "." + tableName
		}
		loadSql.WriteString(
			fmt.Sprintf(
				"COPY %s FROM '%s' (FORMAT CSV, HEADER true);\n",
				tableName,
				dataPath,
			),
		)
	}

	loadPath := filepath.Join(stmt.Path, "load.sql")
	if err := os.WriteFile(loadPath, []byte(loadSql.String()), 0o644); err != nil {
		return 0, fmt.Errorf("failed to write load.sql: %v", err)
	}

	return 0, nil
}

// exportTableToCSV exports a table's data to a CSV file.
func (c *EngineConn) exportTableToCSV(tableDef *catalog.TableDef, path string) error {
	colNames := tableDef.ColumnNames()
	colTypes := tableDef.ColumnTypes()

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	w := csv.NewWriter(f)
	defer w.Flush()

	// Write header
	if err := w.Write(colNames); err != nil {
		return err
	}

	// Get the table from storage and scan it
	table, ok := c.engine.storage.GetTable(tableDef.Name)
	if !ok {
		// Table exists in catalog but not in storage (empty table)
		// Header-only CSV is fine
		return nil
	}

	scanner := table.Scan()

	for {
		chunk := scanner.Next()
		if chunk == nil {
			break
		}

		for row := 0; row < chunk.Count(); row++ {
			record := make([]string, len(colNames))
			for col := 0; col < len(colNames); col++ {
				val := chunk.GetValue(row, col)
				record[col] = formatCSVValue(val, colTypes[col])
			}
			if err := w.Write(record); err != nil {
				return err
			}
		}
	}

	return nil
}

// handleImportDatabase imports a database from a previously exported directory.
// It reads schema.sql to create tables/views, then load.sql to load data.
func (c *EngineConn) handleImportDatabase(
	ctx context.Context,
	stmt *parser.ImportDatabaseStmt,
) (int64, error) {
	// Read and execute schema.sql
	schemaPath := filepath.Join(stmt.Path, "schema.sql")
	schemaSQL, err := os.ReadFile(schemaPath)
	if err != nil {
		return 0, fmt.Errorf("failed to read schema.sql: %v", err)
	}

	// Execute each statement in schema.sql
	for _, sqlStmt := range splitSQLStatements(string(schemaSQL)) {
		sqlStmt = strings.TrimSpace(sqlStmt)
		if sqlStmt == "" {
			continue
		}
		parsed, err := parser.Parse(sqlStmt)
		if err != nil {
			return 0, fmt.Errorf("failed to parse schema.sql statement %q: %v", sqlStmt, err)
		}
		if _, err := c.executeInnerStmt(ctx, parsed, nil); err != nil {
			return 0, fmt.Errorf("failed to execute schema.sql statement %q: %v", sqlStmt, err)
		}
	}

	// Read and execute load.sql
	loadPath := filepath.Join(stmt.Path, "load.sql")
	loadSQL, err := os.ReadFile(loadPath)
	if err != nil {
		return 0, fmt.Errorf("failed to read load.sql: %v", err)
	}

	for _, sqlStmt := range splitSQLStatements(string(loadSQL)) {
		sqlStmt = strings.TrimSpace(sqlStmt)
		if sqlStmt == "" {
			continue
		}
		parsed, err := parser.Parse(sqlStmt)
		if err != nil {
			return 0, fmt.Errorf("failed to parse load.sql statement %q: %v", sqlStmt, err)
		}
		if _, err := c.executeInnerStmt(ctx, parsed, nil); err != nil {
			return 0, fmt.Errorf("failed to execute load.sql statement %q: %v", sqlStmt, err)
		}
	}

	return 0, nil
}

// splitSQLStatements splits a string containing multiple SQL statements on semicolons.
// It handles basic cases and ignores semicolons inside string literals.
func splitSQLStatements(sql string) []string {
	var stmts []string
	var current strings.Builder
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		if inString {
			current.WriteByte(ch)
			if ch == stringChar {
				// Check for escaped quote (doubled)
				if i+1 < len(sql) && sql[i+1] == stringChar {
					current.WriteByte(sql[i+1])
					i++
				} else {
					inString = false
				}
			}
			continue
		}

		if ch == '\'' || ch == '"' {
			inString = true
			stringChar = ch
			current.WriteByte(ch)
			continue
		}

		if ch == ';' {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				stmts = append(stmts, stmt)
			}
			current.Reset()
			continue
		}

		current.WriteByte(ch)
	}

	// Handle last statement without trailing semicolon
	stmt := strings.TrimSpace(current.String())
	if stmt != "" {
		stmts = append(stmts, stmt)
	}

	return stmts
}

// generateCreateTableSQL generates a CREATE TABLE statement from a catalog.TableDef.
func generateCreateTableSQL(table *catalog.TableDef) string {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	if table.Schema != "" && table.Schema != "main" {
		sb.WriteString(table.Schema)
		sb.WriteString(".")
	}
	sb.WriteString(table.Name)
	sb.WriteString(" (")

	for i, col := range table.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(col.Name)
		sb.WriteString(" ")
		sb.WriteString(parser.GetTypeName(col.Type))
		if !col.Nullable {
			sb.WriteString(" NOT NULL")
		}
	}

	// Primary key
	if len(table.PrimaryKey) > 0 {
		sb.WriteString(", PRIMARY KEY (")
		for i, pkIdx := range table.PrimaryKey {
			if i > 0 {
				sb.WriteString(", ")
			}
			if pkIdx < len(table.Columns) {
				sb.WriteString(table.Columns[pkIdx].Name)
			}
		}
		sb.WriteString(")")
	}

	sb.WriteString(");")
	return sb.String()
}

// formatCSVValue formats a value for CSV output.
func formatCSVValue(val any, _ dukdb.Type) string {
	if val == nil {
		return ""
	}
	return fmt.Sprintf("%v", val)
}
