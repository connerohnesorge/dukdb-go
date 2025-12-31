package binder

import (
	dukdb "github.com/dukdb/dukdb-go"
)

// BoundStatement represents a statement that has been bound to the catalog.
type BoundStatement interface {
	boundStmtNode()
	Type() dukdb.StmtType
}

// BoundExpr represents an expression that has been bound to the catalog.
type BoundExpr interface {
	boundExprNode()
	ResultType() dukdb.Type
}

// ScalarUDFResolver is the interface for resolving scalar UDFs.
// This is used to decouple the binder from the dukdb package.
type ScalarUDFResolver interface {
	// LookupScalarUDF looks up a scalar UDF by name and argument types.
	// Returns the UDF info (opaque), result type, and whether it was found.
	LookupScalarUDF(
		name string,
		argTypes []dukdb.Type,
	) (udfInfo any, resultType dukdb.Type, found bool)
	// BindScalarUDF calls the ScalarBinder callback if present.
	// Returns the bind context to be used during execution.
	// For volatile functions, this returns nil to prevent caching.
	BindScalarUDF(
		udfInfo any,
		args []dukdb.ScalarUDFArg,
	) (bindCtx any, err error)
	// IsVolatile returns true if the UDF is marked as volatile.
	// Volatile functions produce different results each invocation and cannot be cached.
	IsVolatile(udfInfo any) bool
}
