package executor

import (
	"fmt"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
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

	// Validate columns exist in the table and get column indices
	var colIndices []int
	if plan.TableDef != nil {
		for _, colName := range plan.Columns {
			found := false
			for i, col := range plan.TableDef.Columns {
				if strings.EqualFold(col.Name, colName) {
					found = true
					colIndices = append(colIndices, i)
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

	// Create index definition in catalog
	indexDef := catalog.NewIndexDef(plan.Index, plan.Schema, plan.Table, plan.Columns, plan.IsUnique)

	// Add to catalog
	if err := e.catalog.CreateIndexInSchema(plan.Schema, indexDef); err != nil {
		return nil, err
	}

	// Create the physical HashIndex structure
	hashIndex := storage.NewHashIndex(plan.Index, plan.Table, plan.Columns, plan.IsUnique)

	// Populate the index with existing table data
	if err := e.populateIndex(plan.Table, hashIndex, colIndices); err != nil {
		// Rollback catalog change
		_ = e.catalog.DropIndexInSchema(plan.Schema, plan.Index)
		return nil, fmt.Errorf("failed to populate index: %w", err)
	}

	// Store the index in storage
	if err := e.storage.CreateIndex(plan.Schema, hashIndex); err != nil {
		// Rollback catalog change
		_ = e.catalog.DropIndexInSchema(plan.Schema, plan.Index)
		return nil, fmt.Errorf("failed to store index: %w", err)
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
			// Rollback storage and catalog changes
			_ = e.storage.DropIndex(plan.Schema, plan.Index)
			_ = e.catalog.DropIndexInSchema(plan.Schema, plan.Index)
			return nil, fmt.Errorf("WAL append failed: %w", err)
		}
	}

	return &ExecutionResult{RowsAffected: 0}, nil
}

// populateIndex populates an index with existing rows from a table.
func (e *Executor) populateIndex(tableName string, index *storage.HashIndex, colIndices []int) error {
	// Get the table from storage
	table, ok := e.storage.GetTable(tableName)
	if !ok {
		// Table might be empty or not exist, which is fine for index creation
		return nil
	}

	// Create a scanner to iterate through the table
	scanner := table.Scan()

	// Iterate through all chunks
	for {
		chunk := scanner.Next()
		if chunk == nil {
			break
		}

		// Insert each row into the index
		rowCount := chunk.Count()
		for row := 0; row < rowCount; row++ {
			// Build the key from the indexed columns
			key := make([]any, len(colIndices))
			for i, colIdx := range colIndices {
				key[i] = chunk.GetValue(row, colIdx)
			}

			// Get the RowID for this row in the chunk
			rowID := scanner.GetRowID(row)
			if rowID == nil {
				// Skip rows without RowID (shouldn't happen, but be safe)
				continue
			}

			// Insert into index
			if err := index.Insert(key, *rowID); err != nil {
				return fmt.Errorf("failed to insert row %d into index: %w", row, err)
			}
		}
	}

	return nil
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

	// Drop from storage (ignore errors since index might not have been stored)
	_ = e.storage.DropIndex(plan.Schema, plan.Index)

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

// ---------- Secret DDL Execution Functions ----------

// executeCreateSecret executes a CREATE SECRET statement.
func (e *Executor) executeCreateSecret(
	ctx *ExecutionContext,
	plan *planner.PhysicalCreateSecret,
) (*ExecutionResult, error) {
	// Check if secret manager is available
	if e.secretManager == nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "secret manager not configured",
		}
	}

	// Check if secret already exists
	if e.secretManager.Exists(ctx.Context, plan.Name) {
		if plan.IfNotExists {
			return &ExecutionResult{RowsAffected: 0}, nil
		}
		if plan.OrReplace {
			// Delete the existing secret first
			if err := e.secretManager.Delete(ctx.Context, plan.Name); err != nil {
				return nil, &dukdb.Error{
					Type: dukdb.ErrorTypeCatalog,
					Msg:  fmt.Sprintf("failed to replace secret %s: %v", plan.Name, err),
				}
			}
		} else {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeCatalog,
				Msg:  fmt.Sprintf("secret %s already exists", plan.Name),
			}
		}
	}

	// Create the secret
	if err := e.secretManager.Create(
		ctx.Context,
		plan.Name,
		plan.SecretType,
		plan.Provider,
		plan.Scope,
		plan.Persistent,
		plan.Options,
	); err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  fmt.Sprintf("failed to create secret %s: %v", plan.Name, err),
		}
	}

	return &ExecutionResult{RowsAffected: 0}, nil
}

// executeDropSecret executes a DROP SECRET statement.
func (e *Executor) executeDropSecret(
	ctx *ExecutionContext,
	plan *planner.PhysicalDropSecret,
) (*ExecutionResult, error) {
	// Check if secret manager is available
	if e.secretManager == nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "secret manager not configured",
		}
	}

	// Check if secret exists
	if !e.secretManager.Exists(ctx.Context, plan.Name) {
		if plan.IfExists {
			return &ExecutionResult{RowsAffected: 0}, nil
		}
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  fmt.Sprintf("secret %s does not exist", plan.Name),
		}
	}

	// Delete the secret
	if err := e.secretManager.Delete(ctx.Context, plan.Name); err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  fmt.Sprintf("failed to drop secret %s: %v", plan.Name, err),
		}
	}

	return &ExecutionResult{RowsAffected: 0}, nil
}

// executeAlterSecret executes an ALTER SECRET statement.
func (e *Executor) executeAlterSecret(
	ctx *ExecutionContext,
	plan *planner.PhysicalAlterSecret,
) (*ExecutionResult, error) {
	// Check if secret manager is available
	if e.secretManager == nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "secret manager not configured",
		}
	}

	// Check if secret exists
	if !e.secretManager.Exists(ctx.Context, plan.Name) {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  fmt.Sprintf("secret %s does not exist", plan.Name),
		}
	}

	// Update the secret
	if err := e.secretManager.Update(ctx.Context, plan.Name, plan.Options); err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  fmt.Sprintf("failed to alter secret %s: %v", plan.Name, err),
		}
	}

	return &ExecutionResult{RowsAffected: 0}, nil
}
