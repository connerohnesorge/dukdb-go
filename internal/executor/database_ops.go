package executor

import (
	"path/filepath"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// executeAttach handles an ATTACH DATABASE statement.
// It creates a new in-memory catalog and storage for the attached database,
// derives an alias from the path if not specified, and registers the database
// with the DatabaseManager.
func (e *Executor) executeAttach(
	_ *ExecutionContext,
	plan *planner.PhysicalAttach,
) (*ExecutionResult, error) {
	if e.dbManager == nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "database manager not available",
		}
	}

	alias := plan.Alias
	if alias == "" {
		// Derive alias from filename without extension
		base := filepath.Base(plan.Path)
		alias = strings.TrimSuffix(base, filepath.Ext(base))
		if alias == "" || alias == ":memory:" {
			alias = "db"
		}
	}

	// Create in-memory catalog and storage for the attached database.
	// File-backed databases would need actual file I/O (future work).
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()

	if err := e.dbManager.Attach(alias, plan.Path, plan.ReadOnly, cat, stor); err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  err.Error(),
		}
	}

	return &ExecutionResult{}, nil
}

// executeDetach handles a DETACH DATABASE statement.
// It removes a previously attached database from the DatabaseManager.
func (e *Executor) executeDetach(
	_ *ExecutionContext,
	plan *planner.PhysicalDetach,
) (*ExecutionResult, error) {
	if e.dbManager == nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "database manager not available",
		}
	}

	if err := e.dbManager.Detach(plan.Name, plan.IfExists); err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  err.Error(),
		}
	}

	return &ExecutionResult{}, nil
}

// executeUse handles a USE statement.
// It sets the active database on the DatabaseManager.
func (e *Executor) executeUse(
	_ *ExecutionContext,
	plan *planner.PhysicalUse,
) (*ExecutionResult, error) {
	if e.dbManager == nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "database manager not available",
		}
	}

	if err := e.dbManager.Use(plan.Database); err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  err.Error(),
		}
	}

	return &ExecutionResult{}, nil
}

// executeCreateDatabase handles a CREATE DATABASE statement.
// It creates a new in-memory database and registers it with the DatabaseManager.
func (e *Executor) executeCreateDatabase(
	_ *ExecutionContext,
	plan *planner.PhysicalCreateDatabase,
) (*ExecutionResult, error) {
	if e.dbManager == nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "database manager not available",
		}
	}

	if err := e.dbManager.CreateDatabase(plan.Name, plan.IfNotExists); err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  err.Error(),
		}
	}

	return &ExecutionResult{}, nil
}

// executeDropDatabase handles a DROP DATABASE statement.
// It removes a database from the DatabaseManager.
func (e *Executor) executeDropDatabase(
	_ *ExecutionContext,
	plan *planner.PhysicalDropDatabase,
) (*ExecutionResult, error) {
	if e.dbManager == nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "database manager not available",
		}
	}

	if err := e.dbManager.DropDatabase(plan.Name, plan.IfExists); err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  err.Error(),
		}
	}

	return &ExecutionResult{}, nil
}
