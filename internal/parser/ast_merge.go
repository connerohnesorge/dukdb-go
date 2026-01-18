// Package parser provides SQL parsing for the native Go DuckDB implementation.
package parser

import (
	dukdb "github.com/dukdb/dukdb-go"
)

// ---------- MERGE INTO Statement Types ----------

// MergeActionType represents the type of action in a MERGE clause.
type MergeActionType int

const (
	MergeActionUpdate    MergeActionType = iota // UPDATE existing row
	MergeActionDelete                           // DELETE matched row
	MergeActionInsert                           // INSERT new row
	MergeActionDoNothing                        // DO NOTHING
)

// MergeAction represents an action to perform when a MERGE condition matches.
// Each action specifies what to do when rows match (or don't match) the join condition.
//
// Example:
//
//	WHEN MATCHED AND target.status = 'active' THEN
//	    UPDATE SET value = source.value, updated_at = NOW()
//	WHEN NOT MATCHED THEN
//	    INSERT (id, value) VALUES (source.id, source.value)
type MergeAction struct {
	// Type specifies the action type (UPDATE, DELETE, INSERT, DO NOTHING).
	Type MergeActionType
	// Cond is an optional additional condition (AND ...) for the action.
	// This allows multiple actions for the same match condition with different filters.
	Cond Expr
	// Update contains the SET clauses for UPDATE actions.
	// Example: SET value = source.value, updated_at = NOW()
	Update []SetClause
	// Insert contains the column/value pairs for INSERT actions.
	// Example: INSERT (id, value) VALUES (source.id, source.value)
	Insert []SetClause
}

// MergeStmt represents a MERGE INTO statement.
// MERGE combines INSERT, UPDATE, and DELETE operations into a single statement
// based on a join condition between a target and source table.
// Supports the optional RETURNING clause to return values from affected rows.
//
// Example:
//
//	MERGE INTO target AS t
//	USING source AS s
//	ON t.id = s.id
//	WHEN MATCHED THEN
//	    UPDATE SET t.value = s.value
//	WHEN NOT MATCHED THEN
//	    INSERT (id, value) VALUES (s.id, s.value)
//	RETURNING *;
type MergeStmt struct {
	// Schema is the optional schema name for the target table.
	Schema string
	// Into is the target table reference (the table being modified).
	Into TableRef
	// Using is the source table reference (table, subquery, or table function).
	Using TableRef
	// On is the join condition between target and source.
	On Expr
	// WhenMatched contains actions to execute when a row matches.
	// Multiple WHEN MATCHED clauses are allowed with different conditions.
	WhenMatched []MergeAction
	// WhenNotMatched contains actions to execute when a source row has no match.
	// This is also known as WHEN NOT MATCHED BY TARGET.
	WhenNotMatched []MergeAction
	// WhenNotMatchedBySource contains actions for target rows with no source match.
	// This is also known as WHEN NOT MATCHED BY SOURCE.
	WhenNotMatchedBySource []MergeAction
	// Returning specifies columns to return after the merge operation.
	// If non-empty, the MERGE becomes a query that returns the specified columns
	// from the affected (inserted, updated, or deleted) rows.
	// Use Star=true in SelectColumn for RETURNING *.
	Returning []SelectColumn
}

func (*MergeStmt) stmtNode() {}

// Type returns the statement type for MergeStmt.
// Returns STATEMENT_TYPE_MERGE_INTO as this is a MERGE INTO operation.
func (*MergeStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_MERGE_INTO }

// Accept implements the Visitor pattern for MergeStmt.
func (s *MergeStmt) Accept(v Visitor) {
	v.VisitMergeStmt(s)
}
