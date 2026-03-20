package binder

import (
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/parser"
)

// Known pragma names and their categories
var pragmaCategories = map[string]PragmaType{
	// Info pragmas
	"database_size":   PragmaTypeInfo,
	"table_info":      PragmaTypeInfo,
	"storage_info":    PragmaTypeInfo,
	"show_tables":     PragmaTypeInfo,
	"show_tables_all": PragmaTypeInfo,
	"tables":          PragmaTypeInfo,
	"functions":       PragmaTypeInfo,
	"version":         PragmaTypeInfo,
	"database_list":   PragmaTypeInfo,
	"collations":      PragmaTypeInfo,
	"show":            PragmaTypeInfo,

	// Config pragmas
	"memory_limit":         PragmaTypeConfig,
	"max_memory":           PragmaTypeConfig,
	"threads":              PragmaTypeConfig,
	"worker_threads":       PragmaTypeConfig,
	"default_null_order":   PragmaTypeConfig,
	"temp_directory":       PragmaTypeConfig,
	"checkpoint_threshold": PragmaTypeConfig,

	// FTS pragmas
	"create_fts_index": PragmaTypeConfig,
	"drop_fts_index":   PragmaTypeConfig,

	// Profiling pragmas
	"enable_profiling":     PragmaTypeProfiling,
	"disable_profiling":    PragmaTypeProfiling,
	"profiling_mode":       PragmaTypeProfiling,
	"profiling_output":     PragmaTypeProfiling,
	"enable_progress_bar":  PragmaTypeProfiling,
	"disable_progress_bar": PragmaTypeProfiling,
}

// bindPragma binds a PRAGMA statement.
func (b *Binder) bindPragma(stmt *parser.PragmaStmt) (*BoundPragmaStmt, error) {
	bound := &BoundPragmaStmt{
		Name: strings.ToLower(stmt.Name),
	}

	// Determine pragma type
	if pt, ok := pragmaCategories[bound.Name]; ok {
		bound.PragmaType = pt
	} else {
		// Unknown pragmas default to Info
		bound.PragmaType = PragmaTypeInfo
	}

	// Bind arguments if present
	if len(stmt.Args) > 0 {
		bound.Args = make([]BoundExpr, len(stmt.Args))
		for i, arg := range stmt.Args {
			boundArg, err := b.bindExpr(arg, dukdb.TYPE_ANY)
			if err != nil {
				return nil, err
			}
			bound.Args[i] = boundArg
		}
	}

	// Bind value if present (SET PRAGMA name = value)
	if stmt.Value != nil {
		val, err := b.bindExpr(stmt.Value, dukdb.TYPE_ANY)
		if err != nil {
			return nil, err
		}
		bound.Value = val
	}

	return bound, nil
}

// bindExplain binds an EXPLAIN statement.
func (b *Binder) bindExplain(stmt *parser.ExplainStmt) (*BoundExplainStmt, error) {
	bound := &BoundExplainStmt{
		Analyze: stmt.Analyze,
	}

	// Bind the underlying query
	boundQuery, err := b.Bind(stmt.Query)
	if err != nil {
		return nil, err
	}
	bound.Query = boundQuery

	return bound, nil
}

// bindVacuum binds a VACUUM statement.
func (b *Binder) bindVacuum(stmt *parser.VacuumStmt) (*BoundVacuumStmt, error) {
	bound := &BoundVacuumStmt{
		Schema:    stmt.Schema,
		TableName: stmt.TableName,
	}

	// If table is specified, resolve it
	if stmt.TableName != "" {
		schema := stmt.Schema
		if schema == "" {
			schema = "main"
		}

		tableDef, ok := b.catalog.GetTableInSchema(schema, stmt.TableName)
		if !ok {
			return nil, b.errorf("table '%s.%s' not found", schema, stmt.TableName)
		}
		bound.TableDef = tableDef
		bound.Schema = schema
	}

	return bound, nil
}

// bindAnalyze binds an ANALYZE statement.
func (b *Binder) bindAnalyze(stmt *parser.AnalyzeStmt) (*BoundAnalyzeStmt, error) {
	bound := &BoundAnalyzeStmt{
		Schema:    stmt.Schema,
		TableName: stmt.TableName,
	}

	// If table is specified, resolve it
	if stmt.TableName != "" {
		schema := stmt.Schema
		if schema == "" {
			schema = "main"
		}

		tableDef, ok := b.catalog.GetTableInSchema(schema, stmt.TableName)
		if !ok {
			return nil, b.errorf("table '%s.%s' not found", schema, stmt.TableName)
		}
		bound.TableDef = tableDef
		bound.Schema = schema
	}

	return bound, nil
}

// bindCheckpoint binds a CHECKPOINT statement.
func (b *Binder) bindCheckpoint(stmt *parser.CheckpointStmt) (*BoundCheckpointStmt, error) {
	return &BoundCheckpointStmt{
		Database: stmt.Database,
		Force:    stmt.Force,
	}, nil
}
