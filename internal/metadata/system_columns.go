package metadata

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
)

// DuckDBSettingsColumns returns column definitions for duckdb_settings().
func DuckDBSettingsColumns() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("value", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("description", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("input_type", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("scope", dukdb.TYPE_VARCHAR),
	}
}

// DuckDBTablesColumns returns column definitions for duckdb_tables().
func DuckDBTablesColumns() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("database_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("schema_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("table_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("table_type", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("row_count", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("estimated_size", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("column_count", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("has_primary_key", dukdb.TYPE_BOOLEAN),
	}
}

// DuckDBColumnsColumns returns column definitions for duckdb_columns().
func DuckDBColumnsColumns() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("database_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("schema_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("table_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("column_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("column_index", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("data_type", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("is_nullable", dukdb.TYPE_BOOLEAN),
		catalog.NewColumnDef("column_default", dukdb.TYPE_VARCHAR),
	}
}

// DuckDBViewsColumns returns column definitions for duckdb_views().
func DuckDBViewsColumns() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("database_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("schema_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("view_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("view_definition", dukdb.TYPE_VARCHAR),
	}
}

// DuckDBFunctionsColumns returns column definitions for duckdb_functions().
func DuckDBFunctionsColumns() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("database_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("schema_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("function_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("function_type", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("parameters", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("return_type", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("description", dukdb.TYPE_VARCHAR),
	}
}

// DuckDBConstraintsColumns returns column definitions for duckdb_constraints().
func DuckDBConstraintsColumns() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("database_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("schema_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("table_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("constraint_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("constraint_type", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("constraint_columns", dukdb.TYPE_VARCHAR),
	}
}

// DuckDBIndexesColumns returns column definitions for duckdb_indexes().
func DuckDBIndexesColumns() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("database_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("schema_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("table_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("index_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("is_unique", dukdb.TYPE_BOOLEAN),
		catalog.NewColumnDef("is_primary", dukdb.TYPE_BOOLEAN),
		catalog.NewColumnDef("index_columns", dukdb.TYPE_VARCHAR),
	}
}

// DuckDBDatabasesColumns returns column definitions for duckdb_databases().
func DuckDBDatabasesColumns() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("database_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("database_oid", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("database_size", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("database_type", dukdb.TYPE_VARCHAR),
	}
}

// DuckDBSequencesColumns returns column definitions for duckdb_sequences().
func DuckDBSequencesColumns() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("database_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("schema_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("sequence_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("start_value", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("increment_by", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("min_value", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("max_value", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("is_cycle", dukdb.TYPE_BOOLEAN),
		catalog.NewColumnDef("current_value", dukdb.TYPE_BIGINT),
	}
}

// DuckDBDependenciesColumns returns column definitions for duckdb_dependencies().
func DuckDBDependenciesColumns() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("database_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("schema_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("object_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("object_type", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("dependency_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("dependency_type", dukdb.TYPE_VARCHAR),
	}
}

// DuckDBOptimizersColumns returns column definitions for duckdb_optimizers().
func DuckDBOptimizersColumns() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("description", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("value", dukdb.TYPE_VARCHAR),
	}
}

// DuckDBKeywordsColumns returns column definitions for duckdb_keywords().
func DuckDBKeywordsColumns() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("keyword", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("category", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("reserved", dukdb.TYPE_BOOLEAN),
	}
}

// DuckDBExtensionsColumns returns column definitions for duckdb_extensions().
func DuckDBExtensionsColumns() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("extension_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("loaded", dukdb.TYPE_BOOLEAN),
		catalog.NewColumnDef("installed", dukdb.TYPE_BOOLEAN),
		catalog.NewColumnDef("description", dukdb.TYPE_VARCHAR),
	}
}

// DuckDBMemoryUsageColumns returns column definitions for duckdb_memory_usage().
func DuckDBMemoryUsageColumns() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("memory_usage", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("max_memory", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("system_memory", dukdb.TYPE_BIGINT),
	}
}

// DuckDBTempDirectoryColumns returns column definitions for duckdb_temp_directory().
func DuckDBTempDirectoryColumns() []*catalog.ColumnDef {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("temp_directory", dukdb.TYPE_VARCHAR),
	}
}
