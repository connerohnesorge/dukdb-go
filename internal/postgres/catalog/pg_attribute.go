package catalog

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/postgres/types"
)

// pg_attribute columns - PostgreSQL column/attribute catalog
// Reference: https://www.postgresql.org/docs/current/catalog-pg-attribute.html
var pgAttributeColumns = []string{
	"attrelid",      // OID of the table this column belongs to
	"attname",       // Column name
	"atttypid",      // Data type OID
	"attstattarget", // Statistics target
	"attlen",        // Copy of pg_type.typlen
	"attnum",        // Column number (1-indexed, 0 for system columns)
	"attndims",      // Number of dimensions for array types
	"attcacheoff",   // Always -1 in storage
	"atttypmod",     // Type modifier (e.g., varchar(n))
	"attbyval",      // Passed by value
	"attalign",      // Alignment requirement
	"attstorage",    // Storage strategy
	"attcompression", // Compression method
	"attnotnull",    // NOT NULL constraint
	"atthasdef",     // Has a default value
	"atthasmissing", // Has missing value
	"attidentity",   // Identity column type
	"attgenerated",  // Generated column type
	"attisdropped",  // Column is dropped
	"attislocal",    // Defined locally
	"attinhcount",   // Number of inheritance ancestors
	"attcollation",  // Collation OID
	"attacl",        // Access privileges
	"attoptions",    // Options
	"attfdwoptions", // Foreign data wrapper options
	"attmissingval", // Missing value
}

// queryPgAttribute returns data for pg_catalog.pg_attribute.
func (pg *PgCatalog) queryPgAttribute(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: pgAttributeColumns,
		Rows:    make([]map[string]any, 0),
	}

	// Iterate over all schemas
	schemas := pg.catalog.ListSchemas()
	for _, schema := range schemas {
		// Get columns from tables
		tables := pg.catalog.ListTablesInSchema(schema.Name())
		for _, table := range tables {
			tableOID := generateOID("table:" + schema.Name() + "." + table.Name)

			for i, col := range table.Columns {
				typeOID := dukdbTypeToOID(col.Type)
				typeLen := getTypeLen(col.Type)

				row := map[string]any{
					"attrelid":       tableOID,
					"attname":        col.Name,
					"atttypid":       int64(typeOID),
					"attstattarget":  int64(-1),
					"attlen":         typeLen,
					"attnum":         int64(i + 1), // 1-indexed
					"attndims":       int64(0),     // No array support yet
					"attcacheoff":    int64(-1),
					"atttypmod":      int64(-1),
					"attbyval":       isPassByValue(col.Type),
					"attalign":       getAlignment(col.Type),
					"attstorage":     getStorageType(col.Type),
					"attcompression": "",
					"attnotnull":     !col.Nullable,
					"atthasdef":      col.HasDefault,
					"atthasmissing":  false,
					"attidentity":    "",
					"attgenerated":   "",
					"attisdropped":   false,
					"attislocal":     true,
					"attinhcount":    int64(0),
					"attcollation":   int64(0),
					"attacl":         nil,
					"attoptions":     nil,
					"attfdwoptions":  nil,
					"attmissingval":  nil,
				}

				if matchesFilters(row, filters) {
					result.Rows = append(result.Rows, row)
				}
			}
		}
	}

	return result
}

// dukdbTypeToOID maps dukdb types to PostgreSQL OIDs.
func dukdbTypeToOID(typ dukdb.Type) uint32 {
	switch typ {
	case dukdb.TYPE_BOOLEAN:
		return types.OID_BOOL
	case dukdb.TYPE_TINYINT, dukdb.TYPE_SMALLINT, dukdb.TYPE_UTINYINT:
		return types.OID_INT2
	case dukdb.TYPE_INTEGER, dukdb.TYPE_USMALLINT:
		return types.OID_INT4
	case dukdb.TYPE_BIGINT, dukdb.TYPE_UINTEGER:
		return types.OID_INT8
	case dukdb.TYPE_UBIGINT, dukdb.TYPE_HUGEINT:
		return types.OID_NUMERIC
	case dukdb.TYPE_FLOAT:
		return types.OID_FLOAT4
	case dukdb.TYPE_DOUBLE:
		return types.OID_FLOAT8
	case dukdb.TYPE_VARCHAR:
		return types.OID_VARCHAR
	case dukdb.TYPE_BLOB:
		return types.OID_BYTEA
	case dukdb.TYPE_DATE:
		return types.OID_DATE
	case dukdb.TYPE_TIME:
		return types.OID_TIME
	case dukdb.TYPE_TIMESTAMP:
		return types.OID_TIMESTAMP
	case dukdb.TYPE_TIMESTAMP_TZ:
		return types.OID_TIMESTAMPTZ
	case dukdb.TYPE_INTERVAL:
		return types.OID_INTERVAL
	case dukdb.TYPE_DECIMAL:
		return types.OID_NUMERIC
	case dukdb.TYPE_UUID:
		return types.OID_UUID
	case dukdb.TYPE_JSON:
		return types.OID_JSON
	default:
		return types.OID_TEXT
	}
}

// getTypeLen returns the byte length for fixed-size types, -1 for variable.
func getTypeLen(typ dukdb.Type) int64 {
	switch typ {
	case dukdb.TYPE_BOOLEAN:
		return 1
	case dukdb.TYPE_TINYINT, dukdb.TYPE_UTINYINT:
		return 1
	case dukdb.TYPE_SMALLINT, dukdb.TYPE_USMALLINT:
		return 2
	case dukdb.TYPE_INTEGER, dukdb.TYPE_UINTEGER:
		return 4
	case dukdb.TYPE_BIGINT, dukdb.TYPE_UBIGINT:
		return 8
	case dukdb.TYPE_FLOAT:
		return 4
	case dukdb.TYPE_DOUBLE:
		return 8
	case dukdb.TYPE_DATE:
		return 4
	case dukdb.TYPE_TIME:
		return 8
	case dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP_TZ:
		return 8
	case dukdb.TYPE_UUID:
		return 16
	default:
		return -1 // Variable length
	}
}

// isPassByValue returns true if the type is passed by value.
func isPassByValue(typ dukdb.Type) bool {
	switch typ {
	case dukdb.TYPE_BOOLEAN,
		dukdb.TYPE_TINYINT, dukdb.TYPE_UTINYINT,
		dukdb.TYPE_SMALLINT, dukdb.TYPE_USMALLINT,
		dukdb.TYPE_INTEGER, dukdb.TYPE_UINTEGER,
		dukdb.TYPE_BIGINT, dukdb.TYPE_UBIGINT,
		dukdb.TYPE_FLOAT, dukdb.TYPE_DOUBLE:
		return true
	default:
		return false
	}
}

// getAlignment returns the alignment character for the type.
func getAlignment(typ dukdb.Type) string {
	switch typ {
	case dukdb.TYPE_BOOLEAN, dukdb.TYPE_TINYINT, dukdb.TYPE_UTINYINT:
		return "c" // char alignment
	case dukdb.TYPE_SMALLINT, dukdb.TYPE_USMALLINT:
		return "s" // short alignment
	case dukdb.TYPE_INTEGER, dukdb.TYPE_UINTEGER, dukdb.TYPE_FLOAT:
		return "i" // int alignment
	default:
		return "d" // double alignment
	}
}

// getStorageType returns the storage type character.
func getStorageType(typ dukdb.Type) string {
	switch typ {
	case dukdb.TYPE_VARCHAR, dukdb.TYPE_BLOB, dukdb.TYPE_JSON:
		return "x" // extended (TOAST-able)
	default:
		return "p" // plain
	}
}
