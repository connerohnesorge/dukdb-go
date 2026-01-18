package catalog

import (
	"github.com/dukdb/dukdb-go/internal/catalog"
)

// pg_index columns - PostgreSQL index catalog
// Reference: https://www.postgresql.org/docs/current/catalog-pg-index.html
var pgIndexColumns = []string{
	"indexrelid",          // OID of pg_class entry for this index
	"indrelid",            // OID of pg_class entry for the table
	"indnatts",            // Total number of columns in index
	"indnkeyatts",         // Number of key columns in index
	"indisunique",         // Is this a unique index
	"indnullsnotdistinct", // Are nulls treated as not distinct
	"indisprimary",        // Is this the primary key index
	"indisexclusion",      // Is this an exclusion constraint index
	"indimmediate",        // Is constraint checked immediately on insert
	"indisclustered",      // Is table clustered on this index
	"indisvalid",          // Is index valid for queries
	"indcheckxmin",        // Must wait for xmin to be old
	"indisready",          // Is index ready for inserts
	"indislive",           // Is index alive (not being dropped)
	"indisreplident",      // Is index the replica identity
	"indkey",              // Array of column numbers
	"indcollation",        // Array of collation OIDs
	"indclass",            // Array of operator class OIDs
	"indoption",           // Array of per-column flags
	"indexprs",            // Expression trees for expression index columns
	"indpred",             // Partial index predicate
}

// queryPgIndex returns data for pg_catalog.pg_index.
func (pg *PgCatalog) queryPgIndex(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: pgIndexColumns,
		Rows:    make([]map[string]any, 0),
	}

	// Iterate over all schemas
	schemas := pg.catalog.ListSchemas()
	for _, schema := range schemas {
		// Get all tables
		tables := pg.catalog.ListTablesInSchema(schema.Name())
		for _, table := range tables {
			tableOID := generateOID("table:" + schema.Name() + "." + table.Name)

			// Get indexes for this table
			indexes := pg.catalog.GetIndexesForTable(schema.Name(), table.Name)
			for _, idx := range indexes {
				indexOID := generateOID("index:" + schema.Name() + "." + idx.Name)
				numCols := int64(len(idx.Columns))

				// Build indkey array (column numbers)
				indkey := buildIndKeyFromTable(table, idx.Columns)

				row := map[string]any{
					"indexrelid":          indexOID,
					"indrelid":            tableOID,
					"indnatts":            numCols,
					"indnkeyatts":         numCols,
					"indisunique":         idx.IsUnique,
					"indnullsnotdistinct": false,
					"indisprimary":        idx.IsPrimary,
					"indisexclusion":      false,
					"indimmediate":        true,
					"indisclustered":      false,
					"indisvalid":          true,
					"indcheckxmin":        false,
					"indisready":          true,
					"indislive":           true,
					"indisreplident":      false,
					"indkey":              indkey,
					"indcollation":        nil, // Would need collation OIDs
					"indclass":            nil, // Would need operator class OIDs
					"indoption":           nil,
					"indexprs":            nil,
					"indpred":             nil,
				}

				if matchesFilters(row, filters) {
					result.Rows = append(result.Rows, row)
				}
			}
		}
	}

	return result
}

// buildIndKeyFromTable builds the indkey array (column numbers) for an index.
// Uses the table's column list to find column positions by name.
func buildIndKeyFromTable(table *catalog.TableDef, columns []string) string {
	// For simplicity, we'll return a space-separated string of column positions
	// In PostgreSQL this is actually an int2vector type
	result := ""
	for i, colName := range columns {
		if i > 0 {
			result += " "
		}
		// Find column position (1-indexed)
		if idx, found := table.GetColumnIndex(colName); found {
			result += intToString(int64(idx + 1))
		} else {
			result += "0" // Unknown column
		}
	}
	return result
}
