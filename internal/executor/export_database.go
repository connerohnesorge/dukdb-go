package executor

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// executeExportDatabase exports the entire database to a directory.
// It creates schema.sql (DDL), data files for each table, and load.sql (COPY FROM statements).
func (e *Executor) executeExportDatabase(
	ctx *ExecutionContext,
	plan *planner.PhysicalExportDatabase,
) (*ExecutionResult, error) {
	if e.sqlExecFunc == nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "EXPORT DATABASE requires SQL execution capability (sqlExecFunc not set)",
		}
	}

	dir := plan.Path
	format := "CSV"
	if f, ok := plan.Options["FORMAT"]; ok {
		format = strings.ToUpper(f)
	}

	// Create directory
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("failed to create export directory: %v", err),
		}
	}

	// Determine file extension
	ext := "csv"
	switch format {
	case "PARQUET":
		ext = "parquet"
	case "JSON":
		ext = "json"
	case "CSV":
		// default
	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("unsupported export format: %s", format),
		}
	}

	var schemaSQLBuilder strings.Builder
	var loadSQLBuilder strings.Builder

	// Get all schemas
	schemas := e.catalog.ListSchemas()
	sort.Slice(schemas, func(i, j int) bool {
		return schemas[i].Name() < schemas[j].Name()
	})

	// Phase 1: CREATE SCHEMA statements
	for _, schema := range schemas {
		schemaName := schema.Name()
		if schemaName != "main" && schemaName != "" {
			schemaSQLBuilder.WriteString(
				fmt.Sprintf("CREATE SCHEMA %s;\n", schemaName),
			)
		}
	}

	// Phase 2: CREATE SEQUENCE statements
	for _, schema := range schemas {
		sequences := schema.ListSequences()
		sort.Slice(sequences, func(i, j int) bool {
			return sequences[i].Name < sequences[j].Name
		})
		for _, seq := range sequences {
			schemaSQLBuilder.WriteString(exportGenerateCreateSequenceSQL(seq))
			schemaSQLBuilder.WriteString("\n")
		}
	}

	// Phase 3: CREATE TABLE statements and data export
	type tableInfo struct {
		schema   string
		tableDef *catalog.TableDef
	}
	var allTables []tableInfo

	for _, schema := range schemas {
		schemaName := schema.Name()
		tables := schema.ListTables()
		sort.Slice(tables, func(i, j int) bool {
			return tables[i].Name < tables[j].Name
		})
		for _, table := range tables {
			allTables = append(allTables, tableInfo{
				schema:   schemaName,
				tableDef: table,
			})
			schemaSQLBuilder.WriteString(exportGenerateCreateTableSQL(table))
			schemaSQLBuilder.WriteString("\n")
		}
	}

	// Export data for each table
	for _, tinfo := range allTables {
		dataFile := tinfo.tableDef.Name + "." + ext
		if tinfo.schema != "" && tinfo.schema != "main" {
			dataFile = tinfo.schema + "_" + tinfo.tableDef.Name + "." + ext
		}
		dataPath := filepath.Join(dir, dataFile)

		tableName := tinfo.tableDef.Name
		if tinfo.schema != "" && tinfo.schema != "main" {
			tableName = tinfo.schema + "." + tableName
		}

		// Build COPY TO statement
		var copySQL string
		switch format {
		case "CSV":
			copySQL = fmt.Sprintf("COPY %s TO '%s' (FORMAT CSV, HEADER true)", tableName, dataPath)
		default:
			copySQL = fmt.Sprintf("COPY %s TO '%s' (FORMAT %s)", tableName, dataPath, format)
		}

		if err := e.sqlExecFunc(ctx.Context, copySQL); err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("failed to export table %s: %v", tinfo.tableDef.Name, err),
			}
		}

		// Add COPY FROM to load.sql
		var loadLine string
		switch format {
		case "CSV":
			loadLine = fmt.Sprintf("COPY %s FROM '%s' (FORMAT CSV, HEADER true);\n", tableName, dataPath)
		default:
			loadLine = fmt.Sprintf("COPY %s FROM '%s' (FORMAT %s);\n", tableName, dataPath, format)
		}
		loadSQLBuilder.WriteString(loadLine)
	}

	// Phase 4: CREATE VIEW statements
	for _, schema := range schemas {
		views := schema.ListViews()
		sort.Slice(views, func(i, j int) bool {
			return views[i].Name < views[j].Name
		})
		for _, view := range views {
			qualifiedName := view.Name
			if view.Schema != "" && view.Schema != "main" {
				qualifiedName = view.Schema + "." + view.Name
			}
			schemaSQLBuilder.WriteString(
				fmt.Sprintf("CREATE VIEW %s AS %s;\n", qualifiedName, view.Query),
			)
		}
	}

	// Phase 5: CREATE INDEX statements
	for _, schema := range schemas {
		indexes := schema.ListIndexes()
		sort.Slice(indexes, func(i, j int) bool {
			return indexes[i].Name < indexes[j].Name
		})
		for _, idx := range indexes {
			if idx.IsPrimary {
				continue
			}
			schemaSQLBuilder.WriteString(exportGenerateCreateIndexSQL(idx))
			schemaSQLBuilder.WriteString("\n")
		}
	}

	// Write schema.sql
	schemaPath := filepath.Join(dir, "schema.sql")
	if err := os.WriteFile(schemaPath, []byte(schemaSQLBuilder.String()), 0o644); err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("failed to write schema.sql: %v", err),
		}
	}

	// Write load.sql
	loadPath := filepath.Join(dir, "load.sql")
	if err := os.WriteFile(loadPath, []byte(loadSQLBuilder.String()), 0o644); err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("failed to write load.sql: %v", err),
		}
	}

	return &ExecutionResult{}, nil
}

// exportGenerateCreateTableSQL generates a CREATE TABLE statement from a catalog.TableDef.
func exportGenerateCreateTableSQL(table *catalog.TableDef) string {
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
			sb.WriteString(exportFormatDefaultValue(col.DefaultValue))
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

// exportGenerateCreateSequenceSQL generates a CREATE SEQUENCE statement.
func exportGenerateCreateSequenceSQL(seq *catalog.SequenceDef) string {
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

// exportGenerateCreateIndexSQL generates a CREATE INDEX statement.
func exportGenerateCreateIndexSQL(idx *catalog.IndexDef) string {
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

// exportFormatDefaultValue formats a default value as a SQL literal.
func exportFormatDefaultValue(val any) string {
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
