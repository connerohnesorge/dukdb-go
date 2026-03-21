package engine

import (
	"context"
	"encoding/csv"
	"fmt"
	"math"
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
//   - schema.sql: DDL statements to recreate all schemas, tables, views, sequences, and indexes
//   - <table>.<ext>: Data files for each table (CSV, Parquet, or JSON)
//   - load.sql: COPY FROM statements to reload the data
func (c *EngineConn) handleExportDatabase(stmt *parser.ExportDatabaseStmt) (int64, error) {
	// Create export directory
	if err := os.MkdirAll(stmt.Path, 0o755); err != nil {
		return 0, fmt.Errorf("failed to create export directory: %v", err)
	}

	// Determine export format
	format := "CSV"
	if opts := stmt.Options; opts != nil {
		if f, ok := opts["FORMAT"]; ok {
			format = strings.ToUpper(f)
		}
	}

	var ext string
	switch format {
	case "CSV":
		ext = "csv"
	case "PARQUET":
		ext = "parquet"
	case "JSON":
		ext = "json"
	default:
		return 0, fmt.Errorf("unsupported export format: %s", format)
	}

	// Generate schema.sql with ordered phases:
	// 1. CREATE SCHEMA
	// 2. CREATE SEQUENCE
	// 3. CREATE TABLE
	// 4. CREATE VIEW
	// 5. CREATE INDEX
	var schemaSql strings.Builder

	// Get all schemas
	schemas := c.engine.catalog.ListSchemas()
	// Sort schemas for deterministic output
	sort.Slice(schemas, func(i, j int) bool {
		return schemas[i].Name() < schemas[j].Name()
	})

	// Phase 1: CREATE SCHEMA statements
	for _, schema := range schemas {
		schemaName := schema.Name()
		if schemaName != "main" && schemaName != "" {
			schemaSql.WriteString(
				fmt.Sprintf("CREATE SCHEMA %s;\n", schemaName),
			)
		}
	}

	// Phase 2: CREATE SEQUENCE statements
	for _, schema := range schemas {
		sequences := schema.ListSequences()
		// Sort sequences for deterministic output
		sort.Slice(sequences, func(i, j int) bool {
			return sequences[i].Name < sequences[j].Name
		})
		for _, seq := range sequences {
			schemaSql.WriteString(generateCreateSequenceSQL(seq))
			schemaSql.WriteString("\n")
		}
	}

	// Phase 3: CREATE TABLE statements
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

	// Phase 4: CREATE VIEW statements
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

	// Phase 5: CREATE INDEX statements
	for _, schema := range schemas {
		indexes := schema.ListIndexes()
		// Sort indexes for deterministic output
		sort.Slice(indexes, func(i, j int) bool {
			return indexes[i].Name < indexes[j].Name
		})
		for _, idx := range indexes {
			// Skip primary key indexes to avoid duplicate PK declarations
			if idx.IsPrimary {
				continue
			}
			schemaSql.WriteString(generateCreateIndexSQL(idx))
			schemaSql.WriteString("\n")
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
		// Multi-schema data file naming
		dataFile := tinfo.tableDef.Name + "." + ext
		if tinfo.schema != "" && tinfo.schema != "main" {
			dataFile = tinfo.schema + "_" + tinfo.tableDef.Name + "." + ext
		}
		dataPath := filepath.Join(stmt.Path, dataFile)

		tableName := tinfo.tableDef.Name
		if tinfo.schema != "" && tinfo.schema != "main" {
			tableName = tinfo.schema + "." + tableName
		}

		switch format {
		case "CSV":
			if err := c.exportTableToCSV(tinfo.tableDef, dataPath); err != nil {
				return 0, fmt.Errorf("failed to export table %s: %v", tinfo.tableDef.Name, err)
			}
			loadSql.WriteString(
				fmt.Sprintf(
					"COPY %s FROM '%s' (FORMAT CSV, HEADER true);\n",
					tableName,
					dataPath,
				),
			)
		case "PARQUET":
			if err := c.exportTableViaCopy(tableName, dataPath, "PARQUET"); err != nil {
				return 0, fmt.Errorf("failed to export table %s: %v", tinfo.tableDef.Name, err)
			}
			loadSql.WriteString(
				fmt.Sprintf(
					"COPY %s FROM '%s' (FORMAT PARQUET);\n",
					tableName,
					dataPath,
				),
			)
		case "JSON":
			if err := c.exportTableViaCopy(tableName, dataPath, "JSON"); err != nil {
				return 0, fmt.Errorf("failed to export table %s: %v", tinfo.tableDef.Name, err)
			}
			loadSql.WriteString(
				fmt.Sprintf(
					"COPY %s FROM '%s' (FORMAT JSON);\n",
					tableName,
					dataPath,
				),
			)
		}
	}

	loadPath := filepath.Join(stmt.Path, "load.sql")
	if err := os.WriteFile(loadPath, []byte(loadSql.String()), 0o644); err != nil {
		return 0, fmt.Errorf("failed to write load.sql: %v", err)
	}

	return 0, nil
}

// exportTableViaCopy exports a table using a COPY TO statement for non-CSV formats.
func (c *EngineConn) exportTableViaCopy(tableName, path, format string) error {
	query := fmt.Sprintf("COPY %s TO '%s' (FORMAT %s)", tableName, path, format)
	parsed, err := parser.Parse(query)
	if err != nil {
		return err
	}
	ctx := context.Background()
	_, err = c.executeInnerStmt(ctx, parsed, nil)
	return err
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
		if col.HasDefault {
			sb.WriteString(" DEFAULT ")
			sb.WriteString(formatDefaultValue(col.DefaultValue))
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

// generateCreateSequenceSQL generates a CREATE SEQUENCE statement from a catalog.SequenceDef.
func generateCreateSequenceSQL(seq *catalog.SequenceDef) string {
	var sb strings.Builder
	sb.WriteString("CREATE SEQUENCE ")
	if seq.Schema != "" && seq.Schema != "main" {
		sb.WriteString(seq.Schema)
		sb.WriteString(".")
	}
	sb.WriteString(seq.Name)
	if seq.StartWith != 1 {
		sb.WriteString(fmt.Sprintf(" START WITH %d", seq.StartWith))
	}
	if seq.IncrementBy != 1 {
		sb.WriteString(fmt.Sprintf(" INCREMENT BY %d", seq.IncrementBy))
	}
	if seq.MinValue != math.MinInt64 {
		sb.WriteString(fmt.Sprintf(" MINVALUE %d", seq.MinValue))
	}
	if seq.MaxValue != math.MaxInt64 {
		sb.WriteString(fmt.Sprintf(" MAXVALUE %d", seq.MaxValue))
	}
	if seq.IsCycle {
		sb.WriteString(" CYCLE")
	}
	sb.WriteString(";")
	return sb.String()
}

// generateCreateIndexSQL generates a CREATE INDEX statement from a catalog.IndexDef.
func generateCreateIndexSQL(idx *catalog.IndexDef) string {
	var sb strings.Builder
	sb.WriteString("CREATE ")
	if idx.IsUnique {
		sb.WriteString("UNIQUE ")
	}
	sb.WriteString("INDEX ")
	sb.WriteString(idx.Name)
	sb.WriteString(" ON ")
	if idx.Schema != "" && idx.Schema != "main" {
		sb.WriteString(idx.Schema)
		sb.WriteString(".")
	}
	sb.WriteString(idx.Table)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(idx.Columns, ", "))
	sb.WriteString(");")
	return sb.String()
}

// formatDefaultValue formats a default value as a SQL literal.
func formatDefaultValue(val any) string {
	if val == nil {
		return "NULL"
	}
	switch v := val.(type) {
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// formatCSVValue formats a value for CSV output.
func formatCSVValue(val any, _ dukdb.Type) string {
	if val == nil {
		return ""
	}
	return fmt.Sprintf("%v", val)
}
