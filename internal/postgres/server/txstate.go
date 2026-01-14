// Package server provides PostgreSQL wire protocol server functionality.
// This file implements transaction state tracking and notice handling.
package server

// TransactionState represents the current state of a PostgreSQL transaction.
// This corresponds to the ReadyForQuery message's transaction status indicator.
type TransactionState byte

const (
	// TxStateIdle indicates the server is not in a transaction block.
	// The backend is ready to accept a new query.
	TxStateIdle TransactionState = 'I'

	// TxStateInTx indicates the server is in a transaction block.
	// Commands will be buffered until COMMIT or ROLLBACK.
	TxStateInTx TransactionState = 'T'

	// TxStateFailed indicates the server is in a failed transaction block.
	// All commands will be rejected until ROLLBACK is issued.
	TxStateFailed TransactionState = 'E'
)

// String returns a human-readable description of the transaction state.
func (ts TransactionState) String() string {
	switch ts {
	case TxStateIdle:
		return "idle"
	case TxStateInTx:
		return "in transaction"
	case TxStateFailed:
		return "failed transaction"
	default:
		return "unknown"
	}
}

// IsValid checks if the transaction state is a valid PostgreSQL state.
func (ts TransactionState) IsValid() bool {
	return ts == TxStateIdle || ts == TxStateInTx || ts == TxStateFailed
}

// Notice represents a PostgreSQL notice or warning message.
// These are non-error messages sent from the server to the client
// to provide information about query execution.
type Notice struct {
	// Severity indicates the importance of the notice.
	// Standard values: WARNING, NOTICE, DEBUG, INFO, LOG
	Severity string

	// Code is the SQLSTATE code for the notice.
	// For notices, this is typically "00000" (successful completion)
	// or a warning code starting with "01".
	Code string

	// Message is the primary notice message.
	Message string

	// Detail provides additional detail about the notice (optional).
	Detail string

	// Hint provides a suggestion related to the notice (optional).
	Hint string

	// Position is the cursor position in the query where the notice applies (optional).
	// A value of 0 means the position is not available.
	Position int

	// Where indicates the context in which the notice was generated (optional).
	Where string
}

// NewNotice creates a new Notice with the given severity and message.
func NewNotice(severity, message string) *Notice {
	code := CodeSuccessfulCompletion
	if severity == SeverityWarning {
		code = CodeWarning
	}
	return &Notice{
		Severity: severity,
		Code:     code,
		Message:  message,
	}
}

// NewWarning creates a new warning Notice.
func NewWarning(message string) *Notice {
	return NewNotice(SeverityWarning, message)
}

// NewInfo creates a new informational Notice.
func NewInfo(message string) *Notice {
	return NewNotice(SeverityInfo, message)
}

// NewDebug creates a new debug Notice.
func NewDebug(message string) *Notice {
	return NewNotice(SeverityDebug, message)
}

// WithDetail sets the detail field of the notice.
func (n *Notice) WithDetail(detail string) *Notice {
	n.Detail = detail
	return n
}

// WithHint sets the hint field of the notice.
func (n *Notice) WithHint(hint string) *Notice {
	n.Hint = hint
	return n
}

// WithPosition sets the position field of the notice.
func (n *Notice) WithPosition(position int) *Notice {
	n.Position = position
	return n
}

// WithWhere sets the where field of the notice.
func (n *Notice) WithWhere(where string) *Notice {
	n.Where = where
	return n
}

// WithCode sets the SQLSTATE code of the notice.
func (n *Notice) WithCode(code string) *Notice {
	n.Code = code
	return n
}

// GetTransactionState returns the current transaction state for a session.
// This is used to determine the value to send in ReadyForQuery messages.
func GetTransactionState(session *Session) TransactionState {
	if session == nil {
		return TxStateIdle
	}

	if !session.InTransaction() {
		return TxStateIdle
	}

	if session.IsTransactionAborted() {
		return TxStateFailed
	}

	return TxStateInTx
}

// ErrTransactionAborted is the error returned when a command is executed
// in a failed transaction block. PostgreSQL requires all commands to fail
// until ROLLBACK is issued.
const ErrTransactionAbortedMessage = "current transaction is aborted, commands ignored until end of transaction block"

// NewErrTransactionAborted creates a new PgError for aborted transaction state.
func NewErrTransactionAborted() *PgError {
	return NewPgError(CodeInvalidTransactionState, ErrTransactionAbortedMessage).
		WithHint("Use ROLLBACK to end the failed transaction block and start a new one.")
}

// EmptyQueryTag is the command tag returned for empty queries.
// PostgreSQL returns an EmptyQueryResponse message for queries that are
// empty or contain only whitespace/comments.
const EmptyQueryTag = ""
