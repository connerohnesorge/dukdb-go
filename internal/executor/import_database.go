package executor

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// executeImportDatabase imports a previously exported database from a directory.
// It reads and executes schema.sql (DDL) followed by load.sql (COPY FROM statements).
func (e *Executor) executeImportDatabase(
	ctx *ExecutionContext,
	plan *planner.PhysicalImportDatabase,
) (*ExecutionResult, error) {
	if e.sqlExecFunc == nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "IMPORT DATABASE requires SQL execution capability (sqlExecFunc not set)",
		}
	}

	dir := plan.Path

	// Validate directory exists
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("import directory does not exist: %s", dir),
		}
	}

	// Read and execute schema.sql
	schemaPath := filepath.Join(dir, "schema.sql")
	schemaBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("failed to read schema.sql: %v", err),
		}
	}

	schemaSQL := string(schemaBytes)
	if schemaSQL != "" {
		if err := e.executeMultiSQL(ctx, schemaSQL); err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("import schema.sql failed: %v", err),
			}
		}
	}

	// Read and execute load.sql
	loadPath := filepath.Join(dir, "load.sql")
	loadBytes, err := os.ReadFile(loadPath)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("failed to read load.sql: %v", err),
		}
	}

	loadSQL := string(loadBytes)
	if loadSQL != "" {
		if err := e.executeMultiSQL(ctx, loadSQL); err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("import load.sql failed: %v", err),
			}
		}
	}

	return &ExecutionResult{}, nil
}

// executeMultiSQL splits SQL by semicolons (handling string literals) and executes each statement.
func (e *Executor) executeMultiSQL(ctx *ExecutionContext, sql string) error {
	stmts := importSplitSQLStatements(sql)
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if err := e.sqlExecFunc(ctx.Context, stmt); err != nil {
			return fmt.Errorf("failed to execute: %s: %w", stmt, err)
		}
	}
	return nil
}

// importSplitSQLStatements splits a string containing multiple SQL statements on semicolons.
// It handles basic cases and ignores semicolons inside string literals.
func importSplitSQLStatements(sql string) []string {
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
