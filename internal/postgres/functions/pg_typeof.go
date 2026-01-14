package functions

import (
	"github.com/dukdb/dukdb-go/internal/postgres/types"
)

// PgTypeof returns the PostgreSQL type name for a value.
// This is used by ORMs and tools for type introspection.
type PgTypeof struct{}

// Name returns the function name.
func (*PgTypeof) Name() string {
	return "pg_typeof"
}

// Evaluate returns the PostgreSQL type name for the given value and OID.
func (*PgTypeof) Evaluate(value any, oid uint32) string {
	_ = value // value not needed when OID is provided

	return types.GetDefaultMapper().GetTypeName(oid)
}

// EvaluateFromDuckDBType returns the PostgreSQL type name for a DuckDB type.
func (*PgTypeof) EvaluateFromDuckDBType(duckDBType string) string {
	oid := types.MapDuckDBToPostgresOID(duckDBType)

	return types.GetDefaultMapper().GetTypeName(oid)
}
