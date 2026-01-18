package executor

import (
	"context"
	"testing"

	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	planpkg "github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestDDLExecution(t *testing.T) {
	// Setup catalog, storage, and executor
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := context.Background()

	t.Run("CreateAndDropView", func(t *testing.T) {
		// Create a test table first
		tableDef := catalog.NewTableDef("test_table", []*catalog.ColumnDef{
			{Name: "id", Type: 0},   // INTEGER
			{Name: "name", Type: 3}, // VARCHAR
		})
		err := cat.CreateTableInSchema("main", tableDef)
		require.NoError(t, err)

		// Create view
		createViewPlan := &planpkg.PhysicalCreateView{
			Schema:      "main",
			View:        "test_view",
			IfNotExists: false,
			QueryText:   "SELECT * FROM test_table",
		}

		result, err := exec.executeCreateView(&ExecutionContext{Context: ctx}, createViewPlan)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, int64(0), result.RowsAffected)

		// Verify view was created
		view, exists := cat.GetViewInSchema("main", "test_view")
		require.True(t, exists)
		require.Equal(t, "test_view", view.Name)
		require.Equal(t, "SELECT * FROM test_table", view.Query)

		// Test IF NOT EXISTS
		result, err = exec.executeCreateView(
			&ExecutionContext{Context: ctx},
			&planpkg.PhysicalCreateView{
				Schema:      "main",
				View:        "test_view",
				IfNotExists: true,
				QueryText:   "SELECT * FROM test_table",
			},
		)
		require.NoError(t, err)
		require.Equal(t, int64(0), result.RowsAffected)

		// Test duplicate without IF NOT EXISTS
		_, err = exec.executeCreateView(
			&ExecutionContext{Context: ctx},
			&planpkg.PhysicalCreateView{
				Schema:      "main",
				View:        "test_view",
				IfNotExists: false,
				QueryText:   "SELECT * FROM test_table",
			},
		)
		require.Error(t, err)

		// Drop view
		dropViewPlan := &planpkg.PhysicalDropView{
			Schema:   "main",
			View:     "test_view",
			IfExists: false,
		}

		result, err = exec.executeDropView(&ExecutionContext{Context: ctx}, dropViewPlan)
		require.NoError(t, err)
		require.Equal(t, int64(0), result.RowsAffected)

		// Verify view was dropped
		_, exists = cat.GetViewInSchema("main", "test_view")
		require.False(t, exists)

		// Test IF EXISTS
		result, err = exec.executeDropView(
			&ExecutionContext{Context: ctx},
			&planpkg.PhysicalDropView{
				Schema:   "main",
				View:     "test_view",
				IfExists: true,
			},
		)
		require.NoError(t, err)
		require.Equal(t, int64(0), result.RowsAffected)

		// Test drop non-existent view
		_, err = exec.executeDropView(&ExecutionContext{Context: ctx}, &planpkg.PhysicalDropView{
			Schema:   "main",
			View:     "test_view",
			IfExists: false,
		})
		require.Error(t, err)
	})

	t.Run("CreateAndDropIndex", func(t *testing.T) {
		// Create a test table first
		tableDef := catalog.NewTableDef("indexed_table", []*catalog.ColumnDef{
			{Name: "id", Type: 0},    // INTEGER
			{Name: "email", Type: 3}, // VARCHAR
		})
		err := cat.CreateTableInSchema("main", tableDef)
		require.NoError(t, err)

		// Create index
		createIndexPlan := &planpkg.PhysicalCreateIndex{
			Schema:      "main",
			Table:       "indexed_table",
			Index:       "idx_email",
			IfNotExists: false,
			Columns:     []string{"email"},
			IsUnique:    true,
			TableDef:    tableDef,
		}

		result, err := exec.executeCreateIndex(&ExecutionContext{Context: ctx}, createIndexPlan)
		require.NoError(t, err)
		require.Equal(t, int64(0), result.RowsAffected)

		// Verify index was created
		index, exists := cat.GetIndexInSchema("main", "idx_email")
		require.True(t, exists)
		require.Equal(t, "idx_email", index.Name)
		require.Equal(t, "indexed_table", index.Table)
		require.Equal(t, []string{"email"}, index.Columns)
		require.True(t, index.IsUnique)

		// Test IF NOT EXISTS
		result, err = exec.executeCreateIndex(
			&ExecutionContext{Context: ctx},
			&planpkg.PhysicalCreateIndex{
				Schema:      "main",
				Table:       "indexed_table",
				Index:       "idx_email",
				IfNotExists: true,
				Columns:     []string{"email"},
				IsUnique:    true,
				TableDef:    tableDef,
			},
		)
		require.NoError(t, err)

		// Drop index
		dropIndexPlan := &planpkg.PhysicalDropIndex{
			Schema:   "main",
			Index:    "idx_email",
			IfExists: false,
		}

		result, err = exec.executeDropIndex(&ExecutionContext{Context: ctx}, dropIndexPlan)
		require.NoError(t, err)
		require.Equal(t, int64(0), result.RowsAffected)

		// Verify index was dropped
		_, exists = cat.GetIndexInSchema("main", "idx_email")
		require.False(t, exists)
	})

	t.Run("CreateAndDropSequence", func(t *testing.T) {
		// Create sequence
		createSeqPlan := &planpkg.PhysicalCreateSequence{
			Schema:      "main",
			Sequence:    "test_seq",
			IfNotExists: false,
			StartWith:   100,
			IncrementBy: 5,
			MinValue:    nil,
			MaxValue:    nil,
			IsCycle:     false,
		}

		result, err := exec.executeCreateSequence(&ExecutionContext{Context: ctx}, createSeqPlan)
		require.NoError(t, err)
		require.Equal(t, int64(0), result.RowsAffected)

		// Verify sequence was created
		seq, exists := cat.GetSequenceInSchema("main", "test_seq")
		require.True(t, exists)
		require.Equal(t, "test_seq", seq.Name)
		require.Equal(t, int64(100), seq.StartWith)
		require.Equal(t, int64(100), seq.CurrentVal)
		require.Equal(t, int64(5), seq.IncrementBy)

		// Test IF NOT EXISTS
		result, err = exec.executeCreateSequence(
			&ExecutionContext{Context: ctx},
			&planpkg.PhysicalCreateSequence{
				Schema:      "main",
				Sequence:    "test_seq",
				IfNotExists: true,
				StartWith:   100,
				IncrementBy: 5,
			},
		)
		require.NoError(t, err)

		// Drop sequence
		dropSeqPlan := &planpkg.PhysicalDropSequence{
			Schema:   "main",
			Sequence: "test_seq",
			IfExists: false,
		}

		result, err = exec.executeDropSequence(&ExecutionContext{Context: ctx}, dropSeqPlan)
		require.NoError(t, err)
		require.Equal(t, int64(0), result.RowsAffected)

		// Verify sequence was dropped
		_, exists = cat.GetSequenceInSchema("main", "test_seq")
		require.False(t, exists)
	})

	t.Run("CreateAndDropSchema", func(t *testing.T) {
		// Create schema
		createSchemaPlan := &planpkg.PhysicalCreateSchema{
			Schema:      "test_schema",
			IfNotExists: false,
		}

		result, err := exec.executeCreateSchema(&ExecutionContext{Context: ctx}, createSchemaPlan)
		require.NoError(t, err)
		require.Equal(t, int64(0), result.RowsAffected)

		// Verify schema was created
		_, exists := cat.GetSchema("test_schema")
		require.True(t, exists)

		// Test IF NOT EXISTS
		result, err = exec.executeCreateSchema(
			&ExecutionContext{Context: ctx},
			&planpkg.PhysicalCreateSchema{
				Schema:      "test_schema",
				IfNotExists: true,
			},
		)
		require.NoError(t, err)

		// Drop schema
		dropSchemaPlan := &planpkg.PhysicalDropSchema{
			Schema:   "test_schema",
			IfExists: false,
			Cascade:  false,
		}

		result, err = exec.executeDropSchema(&ExecutionContext{Context: ctx}, dropSchemaPlan)
		require.NoError(t, err)
		require.Equal(t, int64(0), result.RowsAffected)

		// Verify schema was dropped
		_, exists = cat.GetSchema("test_schema")
		require.False(t, exists)

		// Test dropping main schema (should fail)
		_, err = exec.executeDropSchema(
			&ExecutionContext{Context: ctx},
			&planpkg.PhysicalDropSchema{
				Schema:   "main",
				IfExists: false,
				Cascade:  false,
			},
		)
		require.Error(t, err)
	})

	t.Run("AlterTableRename", func(t *testing.T) {
		// Create a test table
		tableDef := catalog.NewTableDef("old_name", []*catalog.ColumnDef{
			{Name: "id", Type: 0},
		})
		err := cat.CreateTableInSchema("main", tableDef)
		require.NoError(t, err)

		// Rename table
		alterPlan := &planpkg.PhysicalAlterTable{
			Schema:       "main",
			Table:        "old_name",
			TableDef:     tableDef,
			Operation:    int(parser.AlterTableRenameTo),
			NewTableName: "new_name",
		}

		result, err := exec.executeAlterTable(&ExecutionContext{Context: ctx}, alterPlan)
		require.NoError(t, err)
		require.Equal(t, int64(0), result.RowsAffected)

		// Verify old name doesn't exist
		_, exists := cat.GetTableInSchema("main", "old_name")
		require.False(t, exists)

		// Verify new name exists
		_, exists = cat.GetTableInSchema("main", "new_name")
		require.True(t, exists)
	})

	t.Run("AlterTableRenameColumn", func(t *testing.T) {
		// Create a test table
		tableDef := catalog.NewTableDef("rename_col_table", []*catalog.ColumnDef{
			{Name: "old_col", Type: 0},
			{Name: "other", Type: 3},
		})
		err := cat.CreateTableInSchema("main", tableDef)
		require.NoError(t, err)

		// Get the table def for alteration
		td, _ := cat.GetTableInSchema("main", "rename_col_table")

		// Rename column
		alterPlan := &planpkg.PhysicalAlterTable{
			Schema:    "main",
			Table:     "rename_col_table",
			TableDef:  td,
			Operation: int(parser.AlterTableRenameColumn),
			OldColumn: "old_col",
			NewColumn: "new_col",
		}

		result, err := exec.executeAlterTable(&ExecutionContext{Context: ctx}, alterPlan)
		require.NoError(t, err)
		require.Equal(t, int64(0), result.RowsAffected)

		// Verify column was renamed
		td, _ = cat.GetTableInSchema("main", "rename_col_table")
		require.Equal(t, "new_col", td.Columns[0].Name)
		require.Equal(t, "other", td.Columns[1].Name)
	})

	t.Run("AlterTableDropColumn", func(t *testing.T) {
		// Create a test table
		tableDef := catalog.NewTableDef("drop_col_table", []*catalog.ColumnDef{
			{Name: "id", Type: 0},
			{Name: "to_drop", Type: 3},
		})
		err := cat.CreateTableInSchema("main", tableDef)
		require.NoError(t, err)

		// Get the table def for alteration
		td, _ := cat.GetTableInSchema("main", "drop_col_table")

		// Drop column
		alterPlan := &planpkg.PhysicalAlterTable{
			Schema:     "main",
			Table:      "drop_col_table",
			TableDef:   td,
			Operation:  int(parser.AlterTableDropColumn),
			DropColumn: "to_drop",
		}

		result, err := exec.executeAlterTable(&ExecutionContext{Context: ctx}, alterPlan)
		require.NoError(t, err)
		require.Equal(t, int64(0), result.RowsAffected)

		// Verify column was dropped
		td, _ = cat.GetTableInSchema("main", "drop_col_table")
		require.Equal(t, 1, len(td.Columns))
		require.Equal(t, "id", td.Columns[0].Name)
	})
}

func TestDDLPlanning(t *testing.T) {
	cat := catalog.NewCatalog()
	planner := planpkg.NewPlanner(cat)

	t.Run("PlanCreateView", func(t *testing.T) {
		stmt := &binder.BoundCreateViewStmt{
			Schema:      "main",
			View:        "my_view",
			IfNotExists: false,
			QueryText:   "SELECT 1",
		}

		plan, err := planner.Plan(stmt)
		require.NoError(t, err)
		require.NotNil(t, plan)

		physPlan, ok := plan.(*planpkg.PhysicalCreateView)
		require.True(t, ok)
		require.Equal(t, "main", physPlan.Schema)
		require.Equal(t, "my_view", physPlan.View)
		require.Equal(t, "SELECT 1", physPlan.QueryText)
	})

	t.Run("PlanDropView", func(t *testing.T) {
		stmt := &binder.BoundDropViewStmt{
			Schema:   "main",
			View:     "my_view",
			IfExists: true,
		}

		plan, err := planner.Plan(stmt)
		require.NoError(t, err)
		require.NotNil(t, plan)

		physPlan, ok := plan.(*planpkg.PhysicalDropView)
		require.True(t, ok)
		require.Equal(t, "main", physPlan.Schema)
		require.Equal(t, "my_view", physPlan.View)
		require.True(t, physPlan.IfExists)
	})

	t.Run("PlanCreateIndex", func(t *testing.T) {
		stmt := &binder.BoundCreateIndexStmt{
			Schema:      "main",
			Table:       "users",
			Index:       "idx_email",
			IfNotExists: false,
			Columns:     []string{"email"},
			IsUnique:    true,
		}

		plan, err := planner.Plan(stmt)
		require.NoError(t, err)
		require.NotNil(t, plan)

		physPlan, ok := plan.(*planpkg.PhysicalCreateIndex)
		require.True(t, ok)
		require.Equal(t, "main", physPlan.Schema)
		require.Equal(t, "users", physPlan.Table)
		require.Equal(t, "idx_email", physPlan.Index)
		require.True(t, physPlan.IsUnique)
	})

	t.Run("PlanCreateSequence", func(t *testing.T) {
		stmt := &binder.BoundCreateSequenceStmt{
			Schema:      "main",
			Sequence:    "my_seq",
			IfNotExists: false,
			StartWith:   1,
			IncrementBy: 1,
		}

		plan, err := planner.Plan(stmt)
		require.NoError(t, err)
		require.NotNil(t, plan)

		physPlan, ok := plan.(*planpkg.PhysicalCreateSequence)
		require.True(t, ok)
		require.Equal(t, "main", physPlan.Schema)
		require.Equal(t, "my_seq", physPlan.Sequence)
		require.Equal(t, int64(1), physPlan.StartWith)
		require.Equal(t, int64(1), physPlan.IncrementBy)
	})

	t.Run("PlanCreateSchema", func(t *testing.T) {
		stmt := &binder.BoundCreateSchemaStmt{
			Schema:      "new_schema",
			IfNotExists: false,
		}

		plan, err := planner.Plan(stmt)
		require.NoError(t, err)
		require.NotNil(t, plan)

		physPlan, ok := plan.(*planpkg.PhysicalCreateSchema)
		require.True(t, ok)
		require.Equal(t, "new_schema", physPlan.Schema)
	})

	t.Run("PlanAlterTable", func(t *testing.T) {
		stmt := &binder.BoundAlterTableStmt{
			Schema:       "main",
			Table:        "users",
			Operation:    parser.AlterTableRenameTo,
			NewTableName: "customers",
		}

		plan, err := planner.Plan(stmt)
		require.NoError(t, err)
		require.NotNil(t, plan)

		physPlan, ok := plan.(*planpkg.PhysicalAlterTable)
		require.True(t, ok)
		require.Equal(t, "main", physPlan.Schema)
		require.Equal(t, "users", physPlan.Table)
		require.Equal(t, int(parser.AlterTableRenameTo), physPlan.Operation)
		require.Equal(t, "customers", physPlan.NewTableName)
	})
}

func TestSequenceFunctions(t *testing.T) {
	// Setup catalog, storage, and executor
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	binderInst := binder.NewBinder(cat)

	t.Run("NEXTVAL returns incrementing values", func(t *testing.T) {
		// Create a sequence
		seq := catalog.NewSequenceDef("test_seq", "main")
		seq.StartWith = 10
		seq.IncrementBy = 2
		seq.CurrentVal = 10

		schema, _ := cat.GetSchema("main")
		err := schema.CreateSequence(seq)
		require.NoError(t, err)

		// Parse and bind NEXTVAL('test_seq')
		stmt, err := parser.Parse("SELECT nextval('test_seq')")
		require.NoError(t, err)

		selectStmt, ok := stmt.(*parser.SelectStmt)
		require.True(t, ok)

		// Bind the SELECT statement
		boundStmt, err := binderInst.Bind(selectStmt)
		require.NoError(t, err)

		// Verify NEXTVAL was bound correctly
		boundSelect, ok := boundStmt.(*binder.BoundSelectStmt)
		require.True(t, ok)
		require.Len(t, boundSelect.Columns, 1)

		seqCall, ok := boundSelect.Columns[0].Expr.(*binder.BoundSequenceCall)
		require.True(t, ok)
		require.Equal(t, "NEXTVAL", seqCall.FunctionName)
		require.Equal(t, "main", seqCall.SchemaName)
		require.Equal(t, "test_seq", seqCall.SequenceName)

		// Execute NEXTVAL multiple times and verify incrementing values
		// First call returns START WITH value (10), then increments
		ctx := &ExecutionContext{Context: context.Background()}

		val1, err := exec.evaluateSequenceCall(ctx, seqCall)
		require.NoError(t, err)
		require.Equal(t, int64(10), val1) // First call returns start value

		val2, err := exec.evaluateSequenceCall(ctx, seqCall)
		require.NoError(t, err)
		require.Equal(t, int64(12), val2) // 10 + 2

		val3, err := exec.evaluateSequenceCall(ctx, seqCall)
		require.NoError(t, err)
		require.Equal(t, int64(14), val3) // 12 + 2
	})

	t.Run("CURRVAL returns current value without incrementing", func(t *testing.T) {
		// Create a sequence
		seq := catalog.NewSequenceDef("curr_test_seq", "main")
		seq.StartWith = 100
		seq.IncrementBy = 1
		seq.CurrentVal = 100

		schema, _ := cat.GetSchema("main")
		err := schema.CreateSequence(seq)
		require.NoError(t, err)

		ctx := &ExecutionContext{Context: context.Background()}

		// First call NEXTVAL to set LastVal
		nextvalStmt, err := parser.Parse("SELECT nextval('curr_test_seq')")
		require.NoError(t, err)
		nextvalBound, err := binderInst.Bind(nextvalStmt)
		require.NoError(t, err)
		nextvalCall := nextvalBound.(*binder.BoundSelectStmt).Columns[0].Expr.(*binder.BoundSequenceCall)
		val, err := exec.evaluateSequenceCall(ctx, nextvalCall)
		require.NoError(t, err)
		require.Equal(t, int64(100), val)

		// Parse and bind CURRVAL('curr_test_seq')
		stmt, err := parser.Parse("SELECT currval('curr_test_seq')")
		require.NoError(t, err)

		selectStmt, ok := stmt.(*parser.SelectStmt)
		require.True(t, ok)

		boundStmt, err := binderInst.Bind(selectStmt)
		require.NoError(t, err)

		boundSelect, ok := boundStmt.(*binder.BoundSelectStmt)
		require.True(t, ok)
		require.Len(t, boundSelect.Columns, 1)

		seqCall, ok := boundSelect.Columns[0].Expr.(*binder.BoundSequenceCall)
		require.True(t, ok)
		require.Equal(t, "CURRVAL", seqCall.FunctionName)

		// Execute CURRVAL multiple times and verify it returns the last nextval value (100)
		val1, err := exec.evaluateSequenceCall(ctx, seqCall)
		require.NoError(t, err)
		require.Equal(t, int64(100), val1)

		val2, err := exec.evaluateSequenceCall(ctx, seqCall)
		require.NoError(t, err)
		require.Equal(t, int64(100), val2) // Same value

		val3, err := exec.evaluateSequenceCall(ctx, seqCall)
		require.NoError(t, err)
		require.Equal(t, int64(100), val3) // Same value
	})

	t.Run("NEXTVAL and CURRVAL together", func(t *testing.T) {
		// Create a sequence
		seq := catalog.NewSequenceDef("mixed_seq", "main")
		seq.StartWith = 1
		seq.IncrementBy = 1
		seq.CurrentVal = 1

		schema, _ := cat.GetSchema("main")
		err := schema.CreateSequence(seq)
		require.NoError(t, err)

		ctx := &ExecutionContext{Context: context.Background()}

		// Bind NEXTVAL
		stmt, err := parser.Parse("SELECT nextval('mixed_seq')")
		require.NoError(t, err)
		boundStmt, err := binderInst.Bind(stmt)
		require.NoError(t, err)
		nextvalCall := boundStmt.(*binder.BoundSelectStmt).Columns[0].Expr.(*binder.BoundSequenceCall)

		// Bind CURRVAL
		stmt2, err := parser.Parse("SELECT currval('mixed_seq')")
		require.NoError(t, err)
		boundStmt2, err := binderInst.Bind(stmt2)
		require.NoError(t, err)
		currvalCall := boundStmt2.(*binder.BoundSelectStmt).Columns[0].Expr.(*binder.BoundSequenceCall)

		// NEXTVAL returns START WITH value first (1)
		val, err := exec.evaluateSequenceCall(ctx, nextvalCall)
		require.NoError(t, err)
		require.Equal(t, int64(1), val)

		// CURRVAL returns last nextval value (1)
		val, err = exec.evaluateSequenceCall(ctx, currvalCall)
		require.NoError(t, err)
		require.Equal(t, int64(1), val)

		// CURRVAL again - still same
		val, err = exec.evaluateSequenceCall(ctx, currvalCall)
		require.NoError(t, err)
		require.Equal(t, int64(1), val)

		// NEXTVAL increments and returns next value (2)
		val, err = exec.evaluateSequenceCall(ctx, nextvalCall)
		require.NoError(t, err)
		require.Equal(t, int64(2), val)

		// CURRVAL returns new current (2)
		val, err = exec.evaluateSequenceCall(ctx, currvalCall)
		require.NoError(t, err)
		require.Equal(t, int64(2), val)
	})

	t.Run("Error on non-existent sequence", func(t *testing.T) {
		// Parse and bind NEXTVAL for non-existent sequence
		stmt, err := parser.Parse("SELECT nextval('nonexistent_seq')")
		require.NoError(t, err)

		selectStmt, ok := stmt.(*parser.SelectStmt)
		require.True(t, ok)

		// Binding should fail because sequence doesn't exist
		_, err = binderInst.Bind(selectStmt)
		require.Error(t, err)
		require.Contains(t, err.Error(), "sequence not found")
	})

	t.Run("Qualified sequence name", func(t *testing.T) {
		// Create a sequence in main schema
		seq := catalog.NewSequenceDef("qualified_seq", "main")
		seq.StartWith = 50
		seq.IncrementBy = 1
		seq.CurrentVal = 50

		schema, _ := cat.GetSchema("main")
		err := schema.CreateSequence(seq)
		require.NoError(t, err)

		// Parse and bind NEXTVAL('main.qualified_seq')
		stmt, err := parser.Parse("SELECT nextval('main.qualified_seq')")
		require.NoError(t, err)

		selectStmt, ok := stmt.(*parser.SelectStmt)
		require.True(t, ok)

		boundStmt, err := binderInst.Bind(selectStmt)
		require.NoError(t, err)

		boundSelect, ok := boundStmt.(*binder.BoundSelectStmt)
		require.True(t, ok)

		seqCall, ok := boundSelect.Columns[0].Expr.(*binder.BoundSequenceCall)
		require.True(t, ok)
		require.Equal(t, "main", seqCall.SchemaName)
		require.Equal(t, "qualified_seq", seqCall.SequenceName)

		ctx := &ExecutionContext{Context: context.Background()}
		val, err := exec.evaluateSequenceCall(ctx, seqCall)
		require.NoError(t, err)
		require.Equal(t, int64(50), val) // First call returns START WITH value
	})

	t.Run("Sequence in INSERT statement", func(t *testing.T) {
		// Create a table
		tableDef := catalog.NewTableDef("test_table", []*catalog.ColumnDef{
			{Name: "id", Type: 1},   // BIGINT
			{Name: "name", Type: 3}, // VARCHAR
		})
		err := cat.CreateTableInSchema("main", tableDef)
		require.NoError(t, err)

		// Create a sequence
		seq := catalog.NewSequenceDef("insert_seq", "main")
		seq.StartWith = 1
		seq.IncrementBy = 1
		seq.CurrentVal = 1

		schema, _ := cat.GetSchema("main")
		err = schema.CreateSequence(seq)
		require.NoError(t, err)

		// Parse INSERT with NEXTVAL
		stmt, err := parser.Parse(
			"INSERT INTO test_table (id, name) VALUES (nextval('insert_seq'), 'Alice')",
		)
		require.NoError(t, err)

		insertStmt, ok := stmt.(*parser.InsertStmt)
		require.True(t, ok)

		// Bind the INSERT statement
		boundStmt, err := binderInst.Bind(insertStmt)
		require.NoError(t, err)

		boundInsert, ok := boundStmt.(*binder.BoundInsertStmt)
		require.True(t, ok)

		// Verify the first value is a sequence call
		require.Len(t, boundInsert.Values, 1)    // One row
		require.Len(t, boundInsert.Values[0], 2) // Two columns

		seqCall, ok := boundInsert.Values[0][0].(*binder.BoundSequenceCall)
		require.True(t, ok)
		require.Equal(t, "NEXTVAL", seqCall.FunctionName)
		require.Equal(t, "insert_seq", seqCall.SequenceName)

		// Evaluate the sequence call
		ctx := &ExecutionContext{Context: context.Background()}
		val, err := exec.evaluateSequenceCall(ctx, seqCall)
		require.NoError(t, err)
		require.Equal(t, int64(1), val) // First nextval returns START WITH value (1)
	})

	t.Run("Invalid sequence function argument count", func(t *testing.T) {
		// NEXTVAL with no arguments should fail
		stmt, err := parser.Parse("SELECT nextval()")
		require.NoError(t, err)

		selectStmt, ok := stmt.(*parser.SelectStmt)
		require.True(t, ok)

		_, err = binderInst.Bind(selectStmt)
		require.Error(t, err)
		require.Contains(t, err.Error(), "requires exactly 1 argument")

		// NEXTVAL with multiple arguments should fail
		stmt2, err := parser.Parse("SELECT nextval('seq1', 'seq2')")
		require.NoError(t, err)

		selectStmt2, ok := stmt2.(*parser.SelectStmt)
		require.True(t, ok)

		_, err = binderInst.Bind(selectStmt2)
		require.Error(t, err)
		require.Contains(t, err.Error(), "requires exactly 1 argument")
	})

	t.Run("Invalid sequence function argument type", func(t *testing.T) {
		// NEXTVAL with non-string literal should fail
		stmt, err := parser.Parse("SELECT nextval(123)")
		require.NoError(t, err)

		selectStmt, ok := stmt.(*parser.SelectStmt)
		require.True(t, ok)

		_, err = binderInst.Bind(selectStmt)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be a string literal")
	})
}
