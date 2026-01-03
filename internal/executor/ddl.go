package executor

import (
	"fmt"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/wal"
)

// DDL Execution Functions

// executeCreateView executes a CREATE VIEW statement.
func (e *Executor) executeCreateView(
	ctx *ExecutionContext,
	plan *planner.PhysicalCreateView,
) (*ExecutionResult, error) {
	// Check if view already exists
	_, exists := e.catalog.GetViewInSchema(plan.Schema, plan.View)
	if exists {
		if plan.IfNotExists {
			return &ExecutionResult{RowsAffected: 0}, nil
		}
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  fmt.Sprintf("view %s.%s already exists", plan.Schema, plan.View),
		}
	}

	// Extract table dependencies from the view query
	tableDeps := extractTableDependencies(plan.QueryText)

	// Create view definition with table dependencies
	viewDef := catalog.NewViewDefWithDependencies(plan.View, plan.Schema, plan.QueryText, tableDeps)

	// Add to catalog
	if err := e.catalog.CreateViewInSchema(plan.Schema, viewDef); err != nil {
		return nil, err
	}

	// Write WAL entry
	if e.wal != nil {
		entry := &wal.CreateViewEntry{
			Schema: plan.Schema,
			Name:   plan.View,
			Query:  plan.QueryText,
		}
		if err := e.wal.WriteEntry(entry); err != nil {
			// Rollback catalog change
			_ = e.catalog.DropViewInSchema(plan.Schema, plan.View)
			return nil, fmt.Errorf("WAL append failed: %w", err)
		}
	}

	return &ExecutionResult{RowsAffected: 0}, nil
}

// extractTableDependencies parses the view query and extracts the tables it depends on.
func extractTableDependencies(queryText string) []string {
	// Parse the query to get the AST
	stmt, err := parser.Parse(queryText)
	if err != nil {
		// If parsing fails, return empty dependencies
		return nil
	}

	// Get the SelectStmt from the parsed result
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	// Use TableExtractor to find all table references
	extractor := parser.NewTableExtractor(false) // false = unqualified names
	extractor.VisitSelectStmt(selectStmt)

	return extractor.GetTables()
}

// executeDropView executes a DROP VIEW statement.
func (e *Executor) executeDropView(
	ctx *ExecutionContext,
	plan *planner.PhysicalDropView,
) (*ExecutionResult, error) {
	// Check if view exists
	_, exists := e.catalog.GetViewInSchema(plan.Schema, plan.View)
	if !exists {
		if plan.IfExists {
			return &ExecutionResult{RowsAffected: 0}, nil
		}
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  fmt.Sprintf("view %s.%s does not exist", plan.Schema, plan.View),
		}
	}

	// Drop from catalog
	if err := e.catalog.DropViewInSchema(plan.Schema, plan.View); err != nil {
		return nil, err
	}

	// Write WAL entry
	if e.wal != nil {
		entry := &wal.DropViewEntry{
			Schema: plan.Schema,
			Name:   plan.View,
		}
		if err := e.wal.WriteEntry(entry); err != nil {
			return nil, fmt.Errorf("WAL append failed: %w", err)
		}
	}

	return &ExecutionResult{RowsAffected: 0}, nil
}

// executeCreateIndex executes a CREATE INDEX statement.
func (e *Executor) executeCreateIndex(
	ctx *ExecutionContext,
	plan *planner.PhysicalCreateIndex,
) (*ExecutionResult, error) {
	// Check if index already exists
	_, exists := e.catalog.GetIndexInSchema(plan.Schema, plan.Index)
	if exists {
		if plan.IfNotExists {
			return &ExecutionResult{RowsAffected: 0}, nil
		}
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  fmt.Sprintf("index %s.%s already exists", plan.Schema, plan.Index),
		}
	}

	// Validate columns exist in the table
	if plan.TableDef != nil {
		for _, colName := range plan.Columns {
			found := false
			for _, col := range plan.TableDef.Columns {
				if col.Name == colName {
					found = true
					break
				}
			}
			if !found {
				return nil, &dukdb.Error{
					Type: dukdb.ErrorTypeCatalog,
					Msg:  fmt.Sprintf("column %s does not exist in table %s", colName, plan.Table),
				}
			}
		}
	}

	// Create index definition
	indexDef := catalog.NewIndexDef(plan.Index, plan.Schema, plan.Table, plan.Columns, plan.IsUnique)

	// Add to catalog
	if err := e.catalog.CreateIndexInSchema(plan.Schema, indexDef); err != nil {
		return nil, err
	}

	// Write WAL entry
	if e.wal != nil {
		entry := &wal.CreateIndexEntry{
			Schema:   plan.Schema,
			Table:    plan.Table,
			Name:     plan.Index,
			Columns:  plan.Columns,
			IsUnique: plan.IsUnique,
		}
		if err := e.wal.WriteEntry(entry); err != nil {
			// Rollback catalog change
			_ = e.catalog.DropIndexInSchema(plan.Schema, plan.Index)
			return nil, fmt.Errorf("WAL append failed: %w", err)
		}
	}

	return &ExecutionResult{RowsAffected: 0}, nil
}

// executeDropIndex executes a DROP INDEX statement.
func (e *Executor) executeDropIndex(
	ctx *ExecutionContext,
	plan *planner.PhysicalDropIndex,
) (*ExecutionResult, error) {
	// Check if index exists
	_, exists := e.catalog.GetIndexInSchema(plan.Schema, plan.Index)
	if !exists {
		if plan.IfExists {
			return &ExecutionResult{RowsAffected: 0}, nil
		}
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  fmt.Sprintf("index %s.%s does not exist", plan.Schema, plan.Index),
		}
	}

	// Drop from catalog
	if err := e.catalog.DropIndexInSchema(plan.Schema, plan.Index); err != nil {
		return nil, err
	}

	// Write WAL entry
	if e.wal != nil {
		entry := &wal.DropIndexEntry{
			Schema: plan.Schema,
			Name:   plan.Index,
		}
		if err := e.wal.WriteEntry(entry); err != nil {
			return nil, fmt.Errorf("WAL append failed: %w", err)
		}
	}

	return &ExecutionResult{RowsAffected: 0}, nil
}

// executeCreateSequence executes a CREATE SEQUENCE statement.
func (e *Executor) executeCreateSequence(
	ctx *ExecutionContext,
	plan *planner.PhysicalCreateSequence,
) (*ExecutionResult, error) {
	// Check if sequence already exists
	_, exists := e.catalog.GetSequenceInSchema(plan.Schema, plan.Sequence)
	if exists {
		if plan.IfNotExists {
			return &ExecutionResult{RowsAffected: 0}, nil
		}
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  fmt.Sprintf("sequence %s.%s already exists", plan.Schema, plan.Sequence),
		}
	}

	// Create sequence definition
	seqDef := catalog.NewSequenceDef(plan.Sequence, plan.Schema)
	seqDef.StartWith = plan.StartWith
	seqDef.CurrentVal = plan.StartWith
	seqDef.IncrementBy = plan.IncrementBy

	if plan.MinValue != nil {
		seqDef.MinValue = *plan.MinValue
	}
	if plan.MaxValue != nil {
		seqDef.MaxValue = *plan.MaxValue
	}
	seqDef.IsCycle = plan.IsCycle

	// Add to catalog
	if err := e.catalog.CreateSequenceInSchema(plan.Schema, seqDef); err != nil {
		return nil, err
	}

	// Write WAL entry
	if e.wal != nil {
		minVal := seqDef.MinValue
		if plan.MinValue != nil {
			minVal = *plan.MinValue
		}
		maxVal := seqDef.MaxValue
		if plan.MaxValue != nil {
			maxVal = *plan.MaxValue
		}
		entry := &wal.CreateSequenceEntry{
			Schema:      plan.Schema,
			Name:        plan.Sequence,
			StartWith:   plan.StartWith,
			IncrementBy: plan.IncrementBy,
			MinValue:    minVal,
			MaxValue:    maxVal,
			IsCycle:     plan.IsCycle,
		}
		if err := e.wal.WriteEntry(entry); err != nil {
			// Rollback catalog change
			_ = e.catalog.DropSequenceInSchema(plan.Schema, plan.Sequence)
			return nil, fmt.Errorf("WAL append failed: %w", err)
		}
	}

	return &ExecutionResult{RowsAffected: 0}, nil
}

// executeDropSequence executes a DROP SEQUENCE statement.
func (e *Executor) executeDropSequence(
	ctx *ExecutionContext,
	plan *planner.PhysicalDropSequence,
) (*ExecutionResult, error) {
	// Check if sequence exists
	_, exists := e.catalog.GetSequenceInSchema(plan.Schema, plan.Sequence)
	if !exists {
		if plan.IfExists {
			return &ExecutionResult{RowsAffected: 0}, nil
		}
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  fmt.Sprintf("sequence %s.%s does not exist", plan.Schema, plan.Sequence),
		}
	}

	// Drop from catalog
	if err := e.catalog.DropSequenceInSchema(plan.Schema, plan.Sequence); err != nil {
		return nil, err
	}

	// Write WAL entry
	if e.wal != nil {
		entry := &wal.DropSequenceEntry{
			Schema: plan.Schema,
			Name:   plan.Sequence,
		}
		if err := e.wal.WriteEntry(entry); err != nil {
			return nil, fmt.Errorf("WAL append failed: %w", err)
		}
	}

	return &ExecutionResult{RowsAffected: 0}, nil
}

// executeCreateSchema executes a CREATE SCHEMA statement.
func (e *Executor) executeCreateSchema(
	ctx *ExecutionContext,
	plan *planner.PhysicalCreateSchema,
) (*ExecutionResult, error) {
	// Create schema (with IF NOT EXISTS handling)
	var err error
	if plan.IfNotExists {
		_, err = e.catalog.CreateSchemaIfNotExists(plan.Schema, true)
	} else {
		_, err = e.catalog.CreateSchema(plan.Schema)
	}

	if err != nil {
		return nil, err
	}

	// Write WAL entry
	if e.wal != nil {
		entry := &wal.CreateSchemaEntry{
			Name: plan.Schema,
		}
		if err := e.wal.WriteEntry(entry); err != nil {
			// Rollback catalog change
			_ = e.catalog.DropSchema(plan.Schema)
			return nil, fmt.Errorf("WAL append failed: %w", err)
		}
	}

	return &ExecutionResult{RowsAffected: 0}, nil
}

// executeDropSchema executes a DROP SCHEMA statement.
func (e *Executor) executeDropSchema(
	ctx *ExecutionContext,
	plan *planner.PhysicalDropSchema,
) (*ExecutionResult, error) {
	// Drop schema with IF EXISTS and CASCADE support
	if err := e.catalog.DropSchemaIfExists(plan.Schema, plan.IfExists, plan.Cascade); err != nil {
		return nil, err
	}

	// Write WAL entry
	if e.wal != nil {
		entry := &wal.DropSchemaEntry{
			Name: plan.Schema,
		}
		if err := e.wal.WriteEntry(entry); err != nil {
			return nil, fmt.Errorf("WAL append failed: %w", err)
		}
	}

	return &ExecutionResult{RowsAffected: 0}, nil
}

// executeAlterTable executes an ALTER TABLE statement.
func (e *Executor) executeAlterTable(
	ctx *ExecutionContext,
	plan *planner.PhysicalAlterTable,
) (*ExecutionResult, error) {
	// Get table definition
	tableDef, exists := e.catalog.GetTableInSchema(plan.Schema, plan.Table)
	if !exists {
		if plan.IfExists {
			// IF EXISTS - silently succeed
			return &ExecutionResult{RowsAffected: 0}, nil
		}
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  fmt.Sprintf("table %s.%s does not exist", plan.Schema, plan.Table),
		}
	}

	// Perform the alteration based on operation type
	switch parser.AlterTableOp(plan.Operation) {
	case parser.AlterTableRenameTo:
		// Check if new name already exists
		_, exists := e.catalog.GetTableInSchema(plan.Schema, plan.NewTableName)
		if exists {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeCatalog,
				Msg:  fmt.Sprintf("table %s.%s already exists", plan.Schema, plan.NewTableName),
			}
		}

		// Update storage (rename the physical table) if it exists
		if _, exists := e.storage.GetTable(plan.Table); exists {
			if err := e.storage.RenameTable(plan.Table, plan.NewTableName); err != nil {
				return nil, err
			}
		}

		// Drop old table from catalog
		if err := e.catalog.DropTableInSchema(plan.Schema, plan.Table); err != nil {
			return nil, err
		}

		// Add with new name
		tableDef.Name = plan.NewTableName
		if err := e.catalog.CreateTableInSchema(plan.Schema, tableDef); err != nil {
			return nil, err
		}

		// Write WAL entry
		if e.wal != nil {
			entry := &wal.AlterTableEntry{
				Schema:       plan.Schema,
				Table:        plan.Table,
				Operation:    uint8(parser.AlterTableRenameTo),
				NewTableName: plan.NewTableName,
			}
			if err := e.wal.WriteEntry(entry); err != nil {
				return nil, fmt.Errorf("WAL append failed: %w", err)
			}
		}

	case parser.AlterTableRenameColumn:
		// Rename column using TableDef method (updates columnIndex)
		if err := tableDef.RenameColumn(plan.OldColumn, plan.NewColumn); err != nil {
			return nil, err
		}

		// Update any indexes that reference this column
		indexes := e.catalog.GetIndexesForTable(plan.Schema, plan.Table)
		for _, idx := range indexes {
			for i, colName := range idx.Columns {
				if colName == plan.OldColumn {
					idx.Columns[i] = plan.NewColumn
				}
			}
		}

		// Write WAL entry
		if e.wal != nil {
			entry := &wal.AlterTableEntry{
				Schema:    plan.Schema,
				Table:     plan.Table,
				Operation: uint8(parser.AlterTableRenameColumn),
				OldColumn: plan.OldColumn,
				NewColumn: plan.NewColumn,
			}
			if err := e.wal.WriteEntry(entry); err != nil {
				return nil, fmt.Errorf("WAL append failed: %w", err)
			}
		}

	case parser.AlterTableDropColumn:
		// Check if any indexes reference this column
		indexes := e.catalog.GetIndexesForTable(plan.Schema, plan.Table)
		for _, idx := range indexes {
			for _, colName := range idx.Columns {
				if colName == plan.DropColumn {
					return nil, &dukdb.Error{
						Type: dukdb.ErrorTypeCatalog,
						Msg: fmt.Sprintf("cannot drop column %s: referenced by index %s",
							plan.DropColumn, idx.Name),
					}
				}
			}
		}

		// Get column index before dropping
		colIdx, ok := tableDef.GetColumnIndex(plan.DropColumn)
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeCatalog,
				Msg:  fmt.Sprintf("column %s does not exist in table %s", plan.DropColumn, plan.Table),
			}
		}

		// Drop column from storage (updates physical data)
		if storageTable, ok := e.storage.GetTable(plan.Table); ok {
			if err := storageTable.DropColumn(colIdx); err != nil {
				return nil, err
			}
		}

		// Drop column using TableDef method (updates columnIndex and PK)
		if err := tableDef.DropColumn(plan.DropColumn); err != nil {
			return nil, err
		}

		// Write WAL entry
		if e.wal != nil {
			entry := &wal.AlterTableEntry{
				Schema:    plan.Schema,
				Table:     plan.Table,
				Operation: uint8(parser.AlterTableDropColumn),
				Column:    plan.DropColumn,
			}
			if err := e.wal.WriteEntry(entry); err != nil {
				return nil, fmt.Errorf("WAL append failed: %w", err)
			}
		}

	case parser.AlterTableAddColumn:
		// Add column to storage (updates physical data structure)
		if storageTable, ok := e.storage.GetTable(plan.Table); ok {
			storageTable.AddColumn(plan.AddColumn.Type)
		}

		// Add column using TableDef method (updates columnIndex)
		if err := tableDef.AddColumn(plan.AddColumn); err != nil {
			return nil, err
		}

		// Write WAL entry
		if e.wal != nil {
			entry := &wal.AlterTableEntry{
				Schema:    plan.Schema,
				Table:     plan.Table,
				Operation: uint8(parser.AlterTableAddColumn),
				Column:    plan.AddColumn.Name,
			}
			if err := e.wal.WriteEntry(entry); err != nil {
				return nil, fmt.Errorf("WAL append failed: %w", err)
			}
		}

	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("unsupported ALTER TABLE operation: %d", plan.Operation),
		}
	}

	return &ExecutionResult{RowsAffected: 0}, nil
}
