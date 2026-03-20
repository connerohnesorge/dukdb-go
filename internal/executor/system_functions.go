package executor

import (
	"github.com/dukdb/dukdb-go/internal/metadata"
	"github.com/dukdb/dukdb-go/internal/planner"
)

func (e *Executor) executeDuckDBSettings(
	ctx *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	settings := metadata.GetSettings(ctx.conn)
	rows := make([]map[string]any, 0, len(settings))
	for _, setting := range settings {
		rows = append(rows, map[string]any{
			"name":        setting.Name,
			"value":       setting.Value,
			"description": setting.Description,
			"input_type":  setting.InputType,
			"scope":       setting.Scope,
		})
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: metadata.ColumnNames(metadata.DuckDBSettingsColumns()),
	}, nil
}

func (e *Executor) executeDuckDBTables(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	tables := metadata.GetTables(e.catalog, e.storage, metadata.DefaultDatabaseName)
	rows := make([]map[string]any, 0, len(tables))
	for _, table := range tables {
		rows = append(rows, map[string]any{
			"database_name":   table.DatabaseName,
			"schema_name":     table.SchemaName,
			"table_name":      table.TableName,
			"table_type":      table.TableType,
			"row_count":       table.RowCount,
			"estimated_size":  table.EstimatedSize,
			"column_count":    int64(table.ColumnCount),
			"has_primary_key": table.HasPrimaryKey,
		})
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: metadata.ColumnNames(metadata.DuckDBTablesColumns()),
	}, nil
}

func (e *Executor) executeDuckDBColumns(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	columns := metadata.GetColumns(e.catalog, metadata.DefaultDatabaseName)
	rows := make([]map[string]any, 0, len(columns))
	for _, column := range columns {
		rows = append(rows, map[string]any{
			"database_name":  column.DatabaseName,
			"schema_name":    column.SchemaName,
			"table_name":     column.TableName,
			"column_name":    column.ColumnName,
			"column_index":   int64(column.ColumnIndex),
			"data_type":      column.DataType,
			"is_nullable":    column.IsNullable,
			"column_default": column.ColumnDefault,
		})
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: metadata.ColumnNames(metadata.DuckDBColumnsColumns()),
	}, nil
}

func (e *Executor) executeDuckDBViews(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	views := metadata.GetViews(e.catalog, metadata.DefaultDatabaseName)
	rows := make([]map[string]any, 0, len(views))
	for _, view := range views {
		rows = append(rows, map[string]any{
			"database_name":   view.DatabaseName,
			"schema_name":     view.SchemaName,
			"view_name":       view.ViewName,
			"view_definition": view.ViewDefinition,
		})
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: metadata.ColumnNames(metadata.DuckDBViewsColumns()),
	}, nil
}

func (e *Executor) executeDuckDBFunctions(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	functions := metadata.GetFunctions(nil, metadata.DefaultDatabaseName)
	rows := make([]map[string]any, 0, len(functions))
	for _, fn := range functions {
		rows = append(rows, map[string]any{
			"database_name": fn.DatabaseName,
			"schema_name":   fn.SchemaName,
			"function_name": fn.FunctionName,
			"function_type": fn.FunctionType,
			"parameters":    fn.Parameters,
			"return_type":   fn.ReturnType,
			"description":   fn.Description,
		})
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: metadata.ColumnNames(metadata.DuckDBFunctionsColumns()),
	}, nil
}

func (e *Executor) executeDuckDBConstraints(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	constraints := metadata.GetConstraints(e.catalog, metadata.DefaultDatabaseName)
	rows := make([]map[string]any, 0, len(constraints))
	for _, constraint := range constraints {
		rows = append(rows, map[string]any{
			"database_name":      constraint.DatabaseName,
			"schema_name":        constraint.SchemaName,
			"table_name":         constraint.TableName,
			"constraint_name":    constraint.ConstraintName,
			"constraint_type":    constraint.ConstraintType,
			"constraint_columns": constraint.ConstraintColumn,
		})
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: metadata.ColumnNames(metadata.DuckDBConstraintsColumns()),
	}, nil
}

func (e *Executor) executeDuckDBIndexes(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	indexes := metadata.GetIndexes(e.catalog, metadata.DefaultDatabaseName)
	rows := make([]map[string]any, 0, len(indexes))
	for _, idx := range indexes {
		rows = append(rows, map[string]any{
			"database_name": idx.DatabaseName,
			"schema_name":   idx.SchemaName,
			"table_name":    idx.TableName,
			"index_name":    idx.IndexName,
			"is_unique":     idx.IsUnique,
			"is_primary":    idx.IsPrimary,
			"index_columns": idx.IndexColumns,
		})
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: metadata.ColumnNames(metadata.DuckDBIndexesColumns()),
	}, nil
}

func (e *Executor) executeDuckDBDatabases(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	databases := metadata.GetDatabases(e.storage, metadata.DefaultDatabaseName)
	rows := make([]map[string]any, 0, len(databases))
	for _, db := range databases {
		rows = append(rows, map[string]any{
			"database_name": db.DatabaseName,
			"database_oid":  db.DatabaseOID,
			"database_size": db.DatabaseSize,
			"database_type": db.DatabaseType,
		})
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: metadata.ColumnNames(metadata.DuckDBDatabasesColumns()),
	}, nil
}

func (e *Executor) executeDuckDBSequences(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	sequences := metadata.GetSequences(e.catalog, metadata.DefaultDatabaseName)
	rows := make([]map[string]any, 0, len(sequences))
	for _, seq := range sequences {
		rows = append(rows, map[string]any{
			"database_name": seq.DatabaseName,
			"schema_name":   seq.SchemaName,
			"sequence_name": seq.SequenceName,
			"start_value":   seq.StartValue,
			"increment_by":  seq.IncrementBy,
			"min_value":     seq.MinValue,
			"max_value":     seq.MaxValue,
			"is_cycle":      seq.IsCycle,
			"current_value": seq.CurrentValue,
		})
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: metadata.ColumnNames(metadata.DuckDBSequencesColumns()),
	}, nil
}

func (e *Executor) executeDuckDBDependencies(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	dependencies := metadata.GetDependencies(e.catalog, metadata.DefaultDatabaseName)
	rows := make([]map[string]any, 0, len(dependencies))
	for _, dep := range dependencies {
		rows = append(rows, map[string]any{
			"database_name":   dep.DatabaseName,
			"schema_name":     dep.SchemaName,
			"object_name":     dep.ObjectName,
			"object_type":     dep.ObjectType,
			"dependency_name": dep.DependencyName,
			"dependency_type": dep.DependencyType,
		})
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: metadata.ColumnNames(metadata.DuckDBDependenciesColumns()),
	}, nil
}

func (e *Executor) executeDuckDBOptimizers(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	optimizers := metadata.GetOptimizers()
	rows := make([]map[string]any, 0, len(optimizers))
	for _, optimizer := range optimizers {
		rows = append(rows, map[string]any{
			"name":        optimizer.Name,
			"description": optimizer.Description,
			"value":       optimizer.Value,
		})
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: metadata.ColumnNames(metadata.DuckDBOptimizersColumns()),
	}, nil
}

func (e *Executor) executeDuckDBKeywords(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	keywords := metadata.GetKeywords()
	rows := make([]map[string]any, 0, len(keywords))
	for _, keyword := range keywords {
		rows = append(rows, map[string]any{
			"keyword":  keyword.Keyword,
			"category": keyword.Category,
			"reserved": keyword.Reserved,
		})
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: metadata.ColumnNames(metadata.DuckDBKeywordsColumns()),
	}, nil
}

func (e *Executor) executeDuckDBExtensions(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	var rows []map[string]any

	// Use extension registry if available
	if e.extRegistry != nil {
		exts := e.extRegistry.ListExtensions()
		rows = make([]map[string]any, 0, len(exts))
		for _, ext := range exts {
			rows = append(rows, map[string]any{
				"extension_name": ext.Name,
				"loaded":         ext.Loaded,
				"installed":      ext.Installed,
				"description":    ext.Description,
			})
		}
	} else {
		rows = []map[string]any{}
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: metadata.ColumnNames(metadata.DuckDBExtensionsColumns()),
	}, nil
}

func (e *Executor) executeDuckDBMemoryUsage(
	ctx *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	usage := metadata.GetMemoryUsage(ctx.conn)
	rows := make([]map[string]any, 0, len(usage))
	for _, item := range usage {
		rows = append(rows, map[string]any{
			"memory_usage":  item.MemoryUsage,
			"max_memory":    item.MaxMemory,
			"system_memory": item.SystemMemory,
		})
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: metadata.ColumnNames(metadata.DuckDBMemoryUsageColumns()),
	}, nil
}

func (e *Executor) executeDuckDBTempDirectory(
	ctx *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	temp := metadata.GetTempDirectory(ctx.conn)
	rows := make([]map[string]any, 0, len(temp))
	for _, item := range temp {
		rows = append(rows, map[string]any{
			"temp_directory": item.TempDirectory,
		})
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: metadata.ColumnNames(metadata.DuckDBTempDirectoryColumns()),
	}, nil
}
