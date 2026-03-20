package executor

import (
	"fmt"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// dukdbTypeToOid returns a PostgreSQL-compatible OID for a dukdb type.
// These OIDs match PostgreSQL's built-in type OIDs for common types.
func dukdbTypeToOid(t dukdb.Type) int64 {
	switch t {
	case dukdb.TYPE_BOOLEAN:
		return 16
	case dukdb.TYPE_SMALLINT:
		return 21
	case dukdb.TYPE_INTEGER:
		return 23
	case dukdb.TYPE_BIGINT:
		return 20
	case dukdb.TYPE_FLOAT:
		return 700
	case dukdb.TYPE_DOUBLE:
		return 701
	case dukdb.TYPE_VARCHAR:
		return 1043
	case dukdb.TYPE_DATE:
		return 1082
	case dukdb.TYPE_TIMESTAMP:
		return 1114
	case dukdb.TYPE_BLOB:
		return 17
	default:
		return 1043 // default to varchar OID
	}
}


func (e *Executor) executePgCatalogNamespace(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	columns := []string{"oid", "nspname", "nspowner"}

	rows := make([]map[string]any, 0)
	oid := int64(1)
	for _, schema := range e.catalog.ListSchemas() {
		rows = append(rows, map[string]any{
			"oid":      oid,
			"nspname":  schema.Name(),
			"nspowner": int64(1),
		})
		oid++
	}

	return &ExecutionResult{Columns: columns, Rows: rows}, nil
}

func (e *Executor) executePgCatalogClass(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	columns := []string{"oid", "relname", "relnamespace", "relkind", "relowner", "reltuples"}

	rows := make([]map[string]any, 0)
	oid := int64(1)
	nsOid := int64(1)

	for _, schema := range e.catalog.ListSchemas() {
		// Tables
		for _, table := range schema.ListTables() {
			rows = append(rows, map[string]any{
				"oid":          oid,
				"relname":      table.Name,
				"relnamespace": nsOid,
				"relkind":      "r",
				"relowner":     int64(1),
				"reltuples":    float32(-1),
			})
			oid++
		}

		// Views
		for _, view := range schema.ListViews() {
			rows = append(rows, map[string]any{
				"oid":          oid,
				"relname":      view.Name,
				"relnamespace": nsOid,
				"relkind":      "v",
				"relowner":     int64(1),
				"reltuples":    float32(0),
			})
			oid++
		}

		// Indexes
		for _, idx := range schema.ListIndexes() {
			rows = append(rows, map[string]any{
				"oid":          oid,
				"relname":      idx.Name,
				"relnamespace": nsOid,
				"relkind":      "i",
				"relowner":     int64(1),
				"reltuples":    float32(-1),
			})
			oid++
		}

		nsOid++
	}

	return &ExecutionResult{Columns: columns, Rows: rows}, nil
}

func (e *Executor) executePgCatalogAttribute(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	columns := []string{"attrelid", "attname", "atttypid", "attnum", "attnotnull", "attisdropped"}

	rows := make([]map[string]any, 0)
	tableOid := int64(1)

	for _, schema := range e.catalog.ListSchemas() {
		for _, table := range schema.ListTables() {
			for i, col := range table.Columns {
				rows = append(rows, map[string]any{
					"attrelid":    tableOid,
					"attname":     col.Name,
					"atttypid":    dukdbTypeToOid(col.Type),
					"attnum":      int64(i + 1),
					"attnotnull":  !col.Nullable,
					"attisdropped": false,
				})
			}
			tableOid++
		}
		// Skip OIDs for views and indexes to match pg_class ordering
		for range schema.ListViews() {
			tableOid++
		}
		for range schema.ListIndexes() {
			tableOid++
		}
	}

	return &ExecutionResult{Columns: columns, Rows: rows}, nil
}

func (e *Executor) executePgCatalogType(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	columns := []string{"oid", "typname", "typnamespace", "typlen", "typtype"}

	// Return the built-in type mappings
	typeEntries := []struct {
		oid     int64
		name    string
		typlen  int64
		typtype string
	}{
		{16, "bool", 1, "b"},
		{17, "bytea", -1, "b"},
		{20, "int8", 8, "b"},
		{21, "int2", 2, "b"},
		{23, "int4", 4, "b"},
		{25, "text", -1, "b"},
		{700, "float4", 4, "b"},
		{701, "float8", 8, "b"},
		{1043, "varchar", -1, "b"},
		{1082, "date", 4, "b"},
		{1114, "timestamp", 8, "b"},
		{1186, "interval", 16, "b"},
	}

	rows := make([]map[string]any, 0, len(typeEntries))
	for _, te := range typeEntries {
		rows = append(rows, map[string]any{
			"oid":          te.oid,
			"typname":      te.name,
			"typnamespace": int64(1), // pg_catalog namespace
			"typlen":       te.typlen,
			"typtype":      te.typtype,
		})
	}

	return &ExecutionResult{Columns: columns, Rows: rows}, nil
}

func (e *Executor) executePgCatalogTables(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	columns := []string{"schemaname", "tablename", "tableowner", "hasindexes", "hasrules", "hastriggers"}

	rows := make([]map[string]any, 0)

	for _, schema := range e.catalog.ListSchemas() {
		schemaName := schema.Name()
		indexes := schema.ListIndexes()

		for _, table := range schema.ListTables() {
			hasIndexes := false
			for _, idx := range indexes {
				if idx.Table == table.Name {
					hasIndexes = true
					break
				}
			}
			rows = append(rows, map[string]any{
				"schemaname":  schemaName,
				"tablename":   table.Name,
				"tableowner":  "dukdb",
				"hasindexes":  hasIndexes,
				"hasrules":    false,
				"hastriggers": false,
			})
		}
	}

	return &ExecutionResult{Columns: columns, Rows: rows}, nil
}

func (e *Executor) executePgCatalogViews(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	columns := []string{"schemaname", "viewname", "viewowner", "definition"}

	rows := make([]map[string]any, 0)

	for _, schema := range e.catalog.ListSchemas() {
		schemaName := schema.Name()
		for _, view := range schema.ListViews() {
			rows = append(rows, map[string]any{
				"schemaname": schemaName,
				"viewname":   view.Name,
				"viewowner":  "dukdb",
				"definition": view.Query,
			})
		}
	}

	return &ExecutionResult{Columns: columns, Rows: rows}, nil
}

func (e *Executor) executePgCatalogIndex(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	columns := []string{"indexrelid", "indrelid", "indnatts", "indisunique", "indisprimary"}

	rows := make([]map[string]any, 0)

	// Build a table name -> OID mapping consistent with pg_class
	tableOids := make(map[string]int64)
	oid := int64(1)
	indexOids := make(map[string]int64)

	for _, schema := range e.catalog.ListSchemas() {
		for _, table := range schema.ListTables() {
			tableOids[table.Name] = oid
			oid++
		}
		for range schema.ListViews() {
			oid++
		}
		for _, idx := range schema.ListIndexes() {
			indexOids[idx.Name] = oid
			oid++
		}
	}

	for _, schema := range e.catalog.ListSchemas() {
		for _, idx := range schema.ListIndexes() {
			indexOid := indexOids[idx.Name]
			tableOid := tableOids[idx.Table]
			rows = append(rows, map[string]any{
				"indexrelid":   indexOid,
				"indrelid":     tableOid,
				"indnatts":     int64(len(idx.Columns)),
				"indisunique":  idx.IsUnique,
				"indisprimary": idx.IsPrimary,
			})
		}
	}

	return &ExecutionResult{Columns: columns, Rows: rows}, nil
}

func (e *Executor) executePgCatalogConstraint(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	columns := []string{"oid", "conname", "connamespace", "contype", "conrelid"}

	rows := make([]map[string]any, 0)
	constraintOid := int64(1)
	tableOid := int64(1)
	nsOid := int64(1)

	for _, schema := range e.catalog.ListSchemas() {
		for _, table := range schema.ListTables() {
			// Primary key constraint
			if table.HasPrimaryKey() {
				rows = append(rows, map[string]any{
					"oid":          constraintOid,
					"conname":      table.Name + "_pkey",
					"connamespace": nsOid,
					"contype":      "p", // primary key
					"conrelid":     tableOid,
				})
				constraintOid++
			}

			// Other constraints
			for i, c := range table.Constraints {
				switch ct := c.(type) {
				case *catalog.UniqueConstraintDef:
					conName := ct.Name
					if conName == "" {
						conName = fmt.Sprintf("%s_unique_%d", table.Name, i)
					}
					rows = append(rows, map[string]any{
						"oid":          constraintOid,
						"conname":      conName,
						"connamespace": nsOid,
						"contype":      "u",
						"conrelid":     tableOid,
					})
					constraintOid++
				case *catalog.CheckConstraintDef:
					conName := ct.Name
					if conName == "" {
						conName = fmt.Sprintf("%s_check_%d", table.Name, i)
					}
					rows = append(rows, map[string]any{
						"oid":          constraintOid,
						"conname":      conName,
						"connamespace": nsOid,
						"contype":      "c",
						"conrelid":     tableOid,
					})
					constraintOid++
				}
			}
			tableOid++
		}
		nsOid++
	}

	return &ExecutionResult{Columns: columns, Rows: rows}, nil
}

func (e *Executor) executePgCatalogDatabase(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	columns := []string{"oid", "datname", "datdba", "encoding", "datcollate"}

	rows := []map[string]any{
		{
			"oid":        int64(1),
			"datname":    "memory",
			"datdba":     int64(1),
			"encoding":   int64(6), // UTF8
			"datcollate": "en_US.UTF-8",
		},
	}

	return &ExecutionResult{Columns: columns, Rows: rows}, nil
}

func (e *Executor) executePgCatalogSettings(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	columns := []string{"name", "setting", "description"}

	rows := []map[string]any{
		{
			"name":        "server_version",
			"setting":     "15.0",
			"description": "PostgreSQL-compatible version string",
		},
		{
			"name":        "server_encoding",
			"setting":     "UTF8",
			"description": "Server (database) character set encoding",
		},
		{
			"name":        "client_encoding",
			"setting":     "UTF8",
			"description": "Client character set encoding",
		},
	}

	return &ExecutionResult{Columns: columns, Rows: rows}, nil
}

func (e *Executor) executePgCatalogRoles(
	_ *ExecutionContext,
	_ *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	columns := []string{"oid", "rolname", "rolsuper", "rolcreatedb"}

	rows := []map[string]any{
		{
			"oid":         int64(1),
			"rolname":     "dukdb",
			"rolsuper":    true,
			"rolcreatedb": true,
		},
	}

	return &ExecutionResult{Columns: columns, Rows: rows}, nil
}
