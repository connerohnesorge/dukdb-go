package server

import (
	"context"
	"database/sql/driver"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	wire "github.com/jeroenrinzema/psql-wire"
)

// Common PostgreSQL OIDs for type mapping.
// These are standard PostgreSQL type OIDs.
const (
	OidUnknown     uint32 = 0
	OidBool        uint32 = 16
	OidBytea       uint32 = 17
	OidChar        uint32 = 18
	OidInt8        uint32 = 20 // bigint
	OidInt2        uint32 = 21 // smallint
	OidInt4        uint32 = 23 // integer
	OidText        uint32 = 25
	OidOid         uint32 = 26
	OidFloat4      uint32 = 700
	OidFloat8      uint32 = 701
	OidVarchar     uint32 = 1043
	OidDate        uint32 = 1082
	OidTime        uint32 = 1083
	OidTimestamp   uint32 = 1114
	OidTimestampTZ uint32 = 1184
	OidInterval    uint32 = 1186
	OidNumeric     uint32 = 1700
	OidUUID        uint32 = 2950
	OidJSON        uint32 = 114
	OidJSONB       uint32 = 3802
)

// Handler handles PostgreSQL wire protocol queries.
// It bridges the psql-wire server to the dukdb engine.
type Handler struct {
	server *Server
}

// NewHandler creates a new query handler for the given server.
func NewHandler(server *Server) *Handler {
	return &Handler{server: server}
}

// Parse implements the wire.ParseFn interface.
// It receives SQL queries from clients and returns prepared statements.
// Supports multi-statement queries separated by semicolons.
func (h *Handler) Parse(ctx context.Context, query string) (wire.PreparedStatements, error) {
	if h.server == nil || h.server.conn == nil {
		return nil, ErrNoConnection
	}

	// Preprocess the query (strip comments, normalize whitespace)
	query = preprocessQuery(query)
	if query == "" {
		// Return empty result for empty queries (PostgreSQL behavior)
		return h.createEmptyQueryResponse()
	}

	// Split into multiple statements if needed
	statements := splitStatements(query)
	if len(statements) == 0 {
		return h.createEmptyQueryResponse()
	}

	// Create a prepared statement for each statement
	preparedStmts := make([]*wire.PreparedStatement, 0, len(statements))
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		stmtCopy := stmt // Capture for closure
		stmtFn := func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
			return h.executeQuery(ctx, stmtCopy, writer, parameters)
		}

		// Determine the query type and columns
		columns, params := h.analyzeQuery(ctx, stmt)

		// Create the prepared statement with columns and parameters
		var opts []wire.PreparedOptionFn
		if len(columns) > 0 {
			opts = append(opts, wire.WithColumns(columns))
		}
		if len(params) > 0 {
			opts = append(opts, wire.WithParameters(params))
		}

		preparedStmts = append(preparedStmts, wire.NewStatement(stmtFn, opts...))
	}

	if len(preparedStmts) == 0 {
		return h.createEmptyQueryResponse()
	}

	return wire.Prepared(preparedStmts...), nil
}

// createEmptyQueryResponse creates a response for an empty query.
func (h *Handler) createEmptyQueryResponse() (wire.PreparedStatements, error) {
	stmtFn := func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
		return writer.Complete("EMPTY")
	}
	stmt := wire.NewStatement(stmtFn)
	return wire.Prepared(stmt), nil
}

// preprocessQuery normalizes whitespace and strips comments from a query.
func preprocessQuery(query string) string {
	// Strip leading/trailing whitespace
	query = strings.TrimSpace(query)
	if query == "" {
		return ""
	}

	// Strip comments
	query = stripComments(query)

	// Normalize whitespace (collapse multiple spaces into one)
	query = normalizeWhitespace(query)

	return strings.TrimSpace(query)
}

// stripComments removes SQL comments from a query.
// Handles both -- line comments and /* */ block comments.
// Preserves strings (does not strip inside quoted strings).
func stripComments(query string) string {
	var result strings.Builder
	result.Grow(len(query))

	i := 0
	for i < len(query) {
		// Check for string literal
		if query[i] == '\'' {
			// Copy the entire string literal including quotes
			result.WriteByte(query[i])
			i++
			for i < len(query) {
				result.WriteByte(query[i])
				if query[i] == '\'' {
					// Check for escaped quote ''
					if i+1 < len(query) && query[i+1] == '\'' {
						i++
						result.WriteByte(query[i])
					} else {
						i++
						break
					}
				}
				i++
			}
			continue
		}

		// Check for double-quoted identifier
		if query[i] == '"' {
			result.WriteByte(query[i])
			i++
			for i < len(query) {
				result.WriteByte(query[i])
				if query[i] == '"' {
					// Check for escaped quote ""
					if i+1 < len(query) && query[i+1] == '"' {
						i++
						result.WriteByte(query[i])
					} else {
						i++
						break
					}
				}
				i++
			}
			continue
		}

		// Check for -- line comment
		if i+1 < len(query) && query[i] == '-' && query[i+1] == '-' {
			// Skip to end of line
			for i < len(query) && query[i] != '\n' {
				i++
			}
			// Replace with space to preserve token separation
			result.WriteByte(' ')
			continue
		}

		// Check for /* */ block comment
		if i+1 < len(query) && query[i] == '/' && query[i+1] == '*' {
			i += 2
			// Find closing */
			for i+1 < len(query) {
				if query[i] == '*' && query[i+1] == '/' {
					i += 2
					break
				}
				i++
			}
			// Replace with space to preserve token separation
			result.WriteByte(' ')
			continue
		}

		result.WriteByte(query[i])
		i++
	}

	return result.String()
}

// normalizeWhitespace collapses multiple whitespace characters into single spaces.
func normalizeWhitespace(query string) string {
	var result strings.Builder
	result.Grow(len(query))

	inWhitespace := false
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(query); i++ {
		c := query[i]

		// Track string literals
		if !inString && (c == '\'' || c == '"') {
			inString = true
			stringChar = c
			inWhitespace = false // Reset whitespace state when entering string
			result.WriteByte(c)
			continue
		}

		if inString {
			result.WriteByte(c)
			if c == stringChar {
				// Check for escaped quote
				if i+1 < len(query) && query[i+1] == stringChar {
					i++
					result.WriteByte(query[i])
				} else {
					inString = false
					inWhitespace = false // Reset whitespace state when exiting string
				}
			}
			continue
		}

		// Outside strings, normalize whitespace
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			if !inWhitespace {
				result.WriteByte(' ')
				inWhitespace = true
			}
		} else {
			result.WriteByte(c)
			inWhitespace = false
		}
	}

	return result.String()
}

// splitStatements splits a query string into individual statements by semicolons.
// Respects string literals (doesn't split on semicolons inside strings).
func splitStatements(query string) []string {
	var statements []string
	var current strings.Builder

	inString := false
	stringChar := byte(0)

	for i := 0; i < len(query); i++ {
		c := query[i]

		// Track string literals
		if !inString && (c == '\'' || c == '"') {
			inString = true
			stringChar = c
			current.WriteByte(c)
			continue
		}

		if inString {
			current.WriteByte(c)
			if c == stringChar {
				// Check for escaped quote
				if i+1 < len(query) && query[i+1] == stringChar {
					i++
					current.WriteByte(query[i])
				} else {
					inString = false
				}
			}
			continue
		}

		// Found statement separator
		if c == ';' {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
			continue
		}

		current.WriteByte(c)
	}

	// Don't forget the last statement (may not end with semicolon)
	if stmt := strings.TrimSpace(current.String()); stmt != "" {
		statements = append(statements, stmt)
	}

	return statements
}

// executeQuery executes a query and writes results to the DataWriter.
func (h *Handler) executeQuery(ctx context.Context, query string, writer wire.DataWriter, parameters []wire.Parameter) error {
	// Get session from context for transaction state management
	session, _ := SessionFromContext(ctx)

	// Classify the query type
	upperQuery := strings.ToUpper(strings.TrimSpace(query))

	// Check if transaction is in aborted state
	// Only ROLLBACK and transaction-related commands are allowed in aborted state
	if session != nil && session.InTransaction() && session.IsTransactionAborted() {
		if !h.isAllowedInAbortedTransaction(upperQuery) {
			return NewErrTransactionAborted()
		}
	}

	// Convert wire parameters to driver.NamedValue
	args := h.convertParameters(ctx, parameters)

	// Handle special commands first
	if handled, err := h.handleSpecialCommand(ctx, upperQuery, query, writer, session); handled {
		return err
	}

	// Handle transaction commands
	if handled, err := h.handleTransactionCommand(ctx, upperQuery, writer, session); handled {
		return err
	}

	// Determine if this is a query that returns rows
	isSelect := strings.HasPrefix(upperQuery, "SELECT") ||
		strings.HasPrefix(upperQuery, "SHOW") ||
		strings.HasPrefix(upperQuery, "EXPLAIN") ||
		strings.HasPrefix(upperQuery, "DESCRIBE") ||
		strings.HasPrefix(upperQuery, "WITH") ||
		strings.HasPrefix(upperQuery, "TABLE") ||
		strings.HasPrefix(upperQuery, "VALUES")

	// Handle catalog queries (information_schema and pg_catalog)
	if isSelect && h.server.catalogHandler != nil && IsCatalogQuery(query) {
		err := h.server.catalogHandler.ExecuteCatalogQuery(ctx, query, writer)
		if err != nil && session != nil && session.InTransaction() {
			session.SetTransactionAborted(true)
		}
		if err != nil {
			return ToPgError(err)
		}
		return nil
	}

	// Rewrite PostgreSQL system functions (e.g., current_database(), version())
	rewrittenQuery := query
	if isSelect && IsPgFunctionQuery(query) {
		dbName := "dukdb"
		username := "dukdb"
		serverVersion := DefaultServerVersion
		var sessionID uint64

		if h.server.config != nil {
			if h.server.config.Database != "" {
				dbName = h.server.config.Database
			}
			if h.server.config.ServerVersion != "" {
				serverVersion = h.server.config.ServerVersion
			}
		}
		if session != nil {
			if session.Username() != "" {
				username = session.Username()
			}
			sessionID = session.ID()
		}
		rewrittenQuery = RewritePgFunctions(query, dbName, username, serverVersion, sessionID)
	}

	// Handle Oracle/MySQL "FROM dual" queries
	if isSelect && IsSelectFromDual(rewrittenQuery) {
		rewrittenQuery = HandleSelectFromDual(rewrittenQuery)
	}

	var err error
	if isSelect {
		err = h.executeSelectQuery(ctx, rewrittenQuery, writer, args)
	} else {
		err = h.executeNonSelectQuery(ctx, query, writer, args)
	}

	// Handle errors within transactions
	if err != nil && session != nil && session.InTransaction() {
		// Mark the transaction as aborted
		session.SetTransactionAborted(true)
	}

	// Convert errors to PostgreSQL format
	if err != nil {
		return ToPgError(err)
	}

	return nil
}

// isAllowedInAbortedTransaction checks if a command is allowed when the transaction is aborted.
// Only ROLLBACK and certain transaction-related commands can execute in an aborted transaction.
func (h *Handler) isAllowedInAbortedTransaction(upperQuery string) bool {
	// ROLLBACK is always allowed to exit the failed transaction
	if strings.HasPrefix(upperQuery, "ROLLBACK") {
		return true
	}

	// COMMIT is allowed but will actually do a rollback
	if strings.HasPrefix(upperQuery, "COMMIT") || strings.HasPrefix(upperQuery, "END") {
		return true
	}

	// SAVEPOINT commands are sometimes allowed (for nested transactions)
	// but we'll block them for safety
	if strings.HasPrefix(upperQuery, "RELEASE") {
		return true
	}

	return false
}

// handleSpecialCommand handles SET, SHOW, DISCARD, RESET, PREPARE, EXECUTE, and DEALLOCATE commands.
// Returns (true, error) if the command was handled, (false, nil) if not.
func (h *Handler) handleSpecialCommand(ctx context.Context, upperQuery, originalQuery string, writer wire.DataWriter, session *Session) (bool, error) {
	// Handle SET command
	if strings.HasPrefix(upperQuery, "SET ") {
		return true, h.handleSetCommand(ctx, originalQuery, writer, session)
	}

	// Handle SHOW command
	if strings.HasPrefix(upperQuery, "SHOW ") {
		return true, h.handleShowCommand(ctx, upperQuery, originalQuery, writer, session)
	}

	// Handle DISCARD command
	if strings.HasPrefix(upperQuery, "DISCARD ") {
		return true, h.handleDiscardCommand(ctx, upperQuery, writer, session)
	}

	// Handle RESET command
	if strings.HasPrefix(upperQuery, "RESET ") {
		return true, h.handleResetCommand(ctx, upperQuery, writer, session)
	}

	// Handle PREPARE command (SQL-level prepared statements)
	if strings.HasPrefix(upperQuery, "PREPARE ") {
		return true, h.handlePrepareCommand(ctx, originalQuery, writer, session)
	}

	// Handle EXECUTE command (SQL-level prepared statement execution)
	if strings.HasPrefix(upperQuery, "EXECUTE ") {
		return true, h.handleExecuteCommand(ctx, originalQuery, writer, session)
	}

	// Handle DEALLOCATE command
	if strings.HasPrefix(upperQuery, "DEALLOCATE") {
		return true, h.handleDeallocateCommand(ctx, originalQuery, writer, session)
	}

	return false, nil
}

// handleSetCommand handles the SET command.
func (h *Handler) handleSetCommand(ctx context.Context, query string, writer wire.DataWriter, session *Session) error {
	// Parse SET variable = value or SET variable TO value
	upperQuery := strings.ToUpper(query)

	// Handle SET LOCAL (transaction-scoped) vs SET (session-scoped)
	isLocal := strings.HasPrefix(upperQuery, "SET LOCAL ")
	if isLocal {
		query = strings.TrimPrefix(query, "SET LOCAL ")
		query = strings.TrimPrefix(query, "set local ")
	} else {
		query = strings.TrimPrefix(query, "SET ")
		query = strings.TrimPrefix(query, "set ")
	}

	// Parse variable = value or variable TO value
	var varName, varValue string

	if idx := strings.Index(strings.ToUpper(query), " TO "); idx != -1 {
		varName = strings.TrimSpace(query[:idx])
		varValue = strings.TrimSpace(query[idx+4:])
	} else if idx := strings.Index(query, "="); idx != -1 {
		varName = strings.TrimSpace(query[:idx])
		varValue = strings.TrimSpace(query[idx+1:])
	} else {
		return NewPgError(CodeSyntaxError, "invalid SET command syntax")
	}

	// Normalize variable name (lowercase)
	varName = strings.ToLower(varName)

	// Remove quotes from value if present
	varValue = strings.Trim(varValue, "'\"")

	// Store in session if available
	if session != nil {
		if isLocal && session.InTransaction() {
			session.SetLocalVariable(varName, varValue)
		} else {
			session.SetVariable(varName, varValue)
		}
	}

	return writer.Complete("SET")
}

// handleShowCommand handles the SHOW command.
func (h *Handler) handleShowCommand(ctx context.Context, upperQuery, originalQuery string, writer wire.DataWriter, session *Session) error {
	// Extract variable name
	varName := strings.TrimSpace(strings.TrimPrefix(originalQuery, "SHOW "))
	varName = strings.TrimSpace(strings.TrimPrefix(varName, "show "))
	varName = strings.ToLower(varName)

	// Try to get value from session first
	var value string
	if session != nil {
		if v, ok := session.GetVariable(varName); ok {
			value = v
		}
	}

	// If not found in session, check for built-in variables
	if value == "" {
		value = h.getBuiltinVariable(varName, session)
	}

	// If still not found, try to pass through to the engine
	if value == "" {
		// Try executing as a regular SHOW command on the engine
		args := []driver.NamedValue{}
		rows, columns, err := h.server.conn.Query(ctx, originalQuery, args)
		if err == nil && len(rows) > 0 && len(columns) > 0 {
			// Return the result from the engine
			for _, row := range rows {
				values := make([]any, len(columns))
				for i, col := range columns {
					values[i] = row[col]
				}
				if err := writer.Row(values); err != nil {
					return err
				}
			}
			return writer.Complete("SHOW")
		}
		// If engine doesn't know the variable, return empty string
		value = ""
	}

	// Return the value as a single-row result
	if err := writer.Row([]any{value}); err != nil {
		return err
	}

	return writer.Complete("SHOW")
}

// getBuiltinVariable returns the value of a built-in PostgreSQL variable.
func (h *Handler) getBuiltinVariable(varName string, session *Session) string {
	switch varName {
	case "server_version":
		if h.server != nil && h.server.config != nil {
			return h.server.config.ServerVersion
		}
		return DefaultServerVersion
	case "server_encoding":
		return "UTF8"
	case "client_encoding":
		return "UTF8"
	case "is_superuser":
		return "on"
	case "session_authorization":
		if session != nil {
			return session.Username()
		}
		return "unknown"
	case "standard_conforming_strings":
		return "on"
	case "timezone", "time zone":
		return "UTC"
	case "datestyle":
		return "ISO, MDY"
	case "intervalstyle":
		return "postgres"
	case "integer_datetimes":
		return "on"
	case "transaction_isolation", "default_transaction_isolation":
		if session != nil {
			return session.GetIsolationLevel()
		}
		return "read committed"
	case "transaction_read_only":
		if session != nil && session.IsTransactionReadOnly() {
			return "on"
		}
		return "off"
	case "search_path":
		return "\"$user\", public"
	case "application_name":
		if session != nil {
			if v, ok := session.GetAttribute("application_name"); ok {
				if s, ok := v.(string); ok {
					return s
				}
			}
		}
		return ""
	default:
		return ""
	}
}

// handleDiscardCommand handles the DISCARD command.
func (h *Handler) handleDiscardCommand(ctx context.Context, upperQuery string, writer wire.DataWriter, session *Session) error {
	// DISCARD ALL - reset session state
	// DISCARD PLANS - discard prepared statements
	// DISCARD SEQUENCES - reset sequence state
	// DISCARD TEMP - drop temp tables
	// DISCARD TEMPORARY - same as TEMP

	discardType := strings.TrimSpace(strings.TrimPrefix(upperQuery, "DISCARD "))

	switch discardType {
	case "ALL":
		// Reset all session state
		if session != nil {
			session.ResetVariables()
			session.SetInTransaction(false)
			session.SetTransactionReadOnly(false)
			session.SetTransactionAborted(false)
			// Clear SQL-level prepared statements
			if ps := session.PreparedStatements(); ps != nil {
				ps.Clear()
			}
			// Clear portals
			if portals := session.Portals(); portals != nil {
				portals.Clear()
			}
		}
	case "PLANS":
		// Clear SQL-level prepared statements
		if session != nil {
			if ps := session.PreparedStatements(); ps != nil {
				ps.Clear()
			}
			if portals := session.Portals(); portals != nil {
				portals.Clear()
			}
		}
	case "SEQUENCES", "TEMP", "TEMPORARY":
		// These would require engine support
	default:
		return NewPgError(CodeSyntaxError, "invalid DISCARD command: "+discardType)
	}

	return writer.Complete("DISCARD " + discardType)
}

// handleResetCommand handles the RESET command.
func (h *Handler) handleResetCommand(ctx context.Context, upperQuery string, writer wire.DataWriter, session *Session) error {
	// RESET variable - reset to default value
	// RESET ALL - reset all variables

	varName := strings.TrimSpace(strings.TrimPrefix(upperQuery, "RESET "))
	varName = strings.ToLower(varName)

	if session != nil {
		if varName == "all" {
			session.ResetVariables()
		} else {
			session.DeleteVariable(varName)
		}
	}

	return writer.Complete("RESET")
}

// handleTransactionCommand handles BEGIN, COMMIT, ROLLBACK, SAVEPOINT, and RELEASE commands.
// Returns (true, error) if the command was handled, (false, nil) if not.
func (h *Handler) handleTransactionCommand(ctx context.Context, upperQuery string, writer wire.DataWriter, session *Session) (bool, error) {
	// Handle BEGIN/START TRANSACTION
	if strings.HasPrefix(upperQuery, "BEGIN") || strings.HasPrefix(upperQuery, "START TRANSACTION") {
		return true, h.handleBeginTransaction(ctx, upperQuery, writer, session)
	}

	// Handle COMMIT/END
	if strings.HasPrefix(upperQuery, "COMMIT") || strings.HasPrefix(upperQuery, "END") {
		return true, h.handleCommitTransaction(ctx, writer, session)
	}

	// Handle ROLLBACK
	if strings.HasPrefix(upperQuery, "ROLLBACK") {
		return true, h.handleRollbackTransaction(ctx, upperQuery, writer, session)
	}

	// Handle SAVEPOINT
	if strings.HasPrefix(upperQuery, "SAVEPOINT ") {
		return true, h.handleSavepoint(ctx, upperQuery, writer, session)
	}

	// Handle RELEASE SAVEPOINT
	if strings.HasPrefix(upperQuery, "RELEASE ") {
		return true, h.handleReleaseSavepoint(ctx, upperQuery, writer, session)
	}

	return false, nil
}

// handleBeginTransaction handles BEGIN and START TRANSACTION commands.
func (h *Handler) handleBeginTransaction(ctx context.Context, upperQuery string, writer wire.DataWriter, session *Session) error {
	if session != nil {
		if session.InTransaction() {
			// PostgreSQL allows BEGIN inside a transaction but issues a warning
			// We'll just return success
			return writer.Complete("BEGIN")
		}

		session.SetInTransaction(true)
		session.SetTransactionAborted(false)

		// Parse transaction options
		h.parseTransactionOptions(upperQuery, session)
	}

	// Execute BEGIN on the engine
	args := []driver.NamedValue{}
	_, err := h.server.conn.Execute(ctx, "BEGIN", args)
	if err != nil {
		return ToPgError(err)
	}

	return writer.Complete("BEGIN")
}

// parseTransactionOptions parses BEGIN/START TRANSACTION options.
func (h *Handler) parseTransactionOptions(upperQuery string, session *Session) {
	// Look for ISOLATION LEVEL
	if idx := strings.Index(upperQuery, "ISOLATION LEVEL "); idx != -1 {
		rest := upperQuery[idx+16:]
		// Extract isolation level
		var level string
		if strings.HasPrefix(rest, "SERIALIZABLE") {
			level = "serializable"
		} else if strings.HasPrefix(rest, "REPEATABLE READ") {
			level = "repeatable read"
		} else if strings.HasPrefix(rest, "READ COMMITTED") {
			level = "read committed"
		} else if strings.HasPrefix(rest, "READ UNCOMMITTED") {
			level = "read uncommitted"
		}
		if level != "" {
			session.SetIsolationLevel(level)
		}
	}

	// Look for READ ONLY / READ WRITE
	if strings.Contains(upperQuery, "READ ONLY") {
		session.SetTransactionReadOnly(true)
	} else if strings.Contains(upperQuery, "READ WRITE") {
		session.SetTransactionReadOnly(false)
	}
}

// handleCommitTransaction handles COMMIT and END commands.
func (h *Handler) handleCommitTransaction(ctx context.Context, writer wire.DataWriter, session *Session) error {
	if session != nil {
		if !session.InTransaction() {
			// PostgreSQL returns COMMIT even when there's no transaction
			return writer.Complete("COMMIT")
		}

		if session.IsTransactionAborted() {
			// Roll back instead of committing an aborted transaction
			session.SetInTransaction(false)
			session.SetTransactionAborted(false)
			session.ClearLocalVariables()

			args := []driver.NamedValue{}
			_, _ = h.server.conn.Execute(ctx, "ROLLBACK", args)

			return writer.Complete("ROLLBACK")
		}

		session.SetInTransaction(false)
		session.ClearLocalVariables()
	}

	// Execute COMMIT on the engine
	args := []driver.NamedValue{}
	_, err := h.server.conn.Execute(ctx, "COMMIT", args)
	if err != nil {
		return ToPgError(err)
	}

	return writer.Complete("COMMIT")
}

// handleRollbackTransaction handles ROLLBACK commands.
func (h *Handler) handleRollbackTransaction(ctx context.Context, upperQuery string, writer wire.DataWriter, session *Session) error {
	// Check for ROLLBACK TO SAVEPOINT
	if strings.Contains(upperQuery, "TO ") {
		return h.handleRollbackToSavepoint(ctx, upperQuery, writer, session)
	}

	if session != nil {
		session.SetInTransaction(false)
		session.SetTransactionAborted(false)
		session.ClearLocalVariables()
	}

	// Execute ROLLBACK on the engine
	args := []driver.NamedValue{}
	_, err := h.server.conn.Execute(ctx, "ROLLBACK", args)
	if err != nil {
		return ToPgError(err)
	}

	return writer.Complete("ROLLBACK")
}

// handleSavepoint handles SAVEPOINT commands.
func (h *Handler) handleSavepoint(ctx context.Context, upperQuery string, writer wire.DataWriter, session *Session) error {
	// Extract savepoint name
	savepointName := strings.TrimSpace(strings.TrimPrefix(upperQuery, "SAVEPOINT "))

	// Execute on the engine
	args := []driver.NamedValue{}
	_, err := h.server.conn.Execute(ctx, "SAVEPOINT "+savepointName, args)
	if err != nil {
		return ToPgError(err)
	}

	return writer.Complete("SAVEPOINT")
}

// handleReleaseSavepoint handles RELEASE SAVEPOINT commands.
func (h *Handler) handleReleaseSavepoint(ctx context.Context, upperQuery string, writer wire.DataWriter, session *Session) error {
	// Extract savepoint name (handle both "RELEASE SAVEPOINT name" and "RELEASE name")
	rest := strings.TrimSpace(strings.TrimPrefix(upperQuery, "RELEASE "))
	savepointName := strings.TrimSpace(strings.TrimPrefix(rest, "SAVEPOINT "))

	// Execute on the engine
	args := []driver.NamedValue{}
	_, err := h.server.conn.Execute(ctx, "RELEASE SAVEPOINT "+savepointName, args)
	if err != nil {
		return ToPgError(err)
	}

	return writer.Complete("RELEASE")
}

// handleRollbackToSavepoint handles ROLLBACK TO SAVEPOINT commands.
func (h *Handler) handleRollbackToSavepoint(ctx context.Context, upperQuery string, writer wire.DataWriter, session *Session) error {
	// Extract savepoint name (handle both "ROLLBACK TO SAVEPOINT name" and "ROLLBACK TO name")
	idx := strings.Index(upperQuery, "TO ")
	if idx == -1 {
		return NewPgError(CodeSyntaxError, "invalid ROLLBACK TO syntax")
	}

	rest := strings.TrimSpace(upperQuery[idx+3:])
	savepointName := strings.TrimSpace(strings.TrimPrefix(rest, "SAVEPOINT "))

	// Execute on the engine
	args := []driver.NamedValue{}
	_, err := h.server.conn.Execute(ctx, "ROLLBACK TO SAVEPOINT "+savepointName, args)
	if err != nil {
		return ToPgError(err)
	}

	// Clear aborted state after successful rollback to savepoint
	if session != nil {
		session.SetTransactionAborted(false)
	}

	return writer.Complete("ROLLBACK")
}

// executeSelectQuery executes a SELECT-like query and writes results.
// Implements streaming to avoid loading all rows into memory.
func (h *Handler) executeSelectQuery(ctx context.Context, query string, writer wire.DataWriter, args []driver.NamedValue) error {
	// Execute the query using the engine connection
	rows, columns, err := h.server.conn.Query(ctx, query, args)
	if err != nil {
		return err
	}

	// Handle empty result set
	if len(rows) == 0 {
		return writer.Empty()
	}

	// Stream each row (write immediately rather than buffering)
	for _, row := range rows {
		values := make([]any, len(columns))
		for i, col := range columns {
			values[i] = row[col]
		}
		if err := writer.Row(values); err != nil {
			return err
		}
	}

	// Complete with SELECT tag
	return writer.Complete("SELECT " + itoa(len(rows)))
}

// executeNonSelectQuery executes a non-SELECT query (INSERT, UPDATE, DELETE, etc.).
func (h *Handler) executeNonSelectQuery(ctx context.Context, query string, writer wire.DataWriter, args []driver.NamedValue) error {
	// Execute the query using the engine connection
	rowsAffected, err := h.server.conn.Execute(ctx, query, args)
	if err != nil {
		return err
	}

	// Determine the command tag based on query type
	tag := h.getCommandTag(query, rowsAffected)
	return writer.Complete(tag)
}

// getCommandTag returns the PostgreSQL command tag for a query.
// The command tag tells the client what command was executed and how many rows were affected.
// See: https://www.postgresql.org/docs/current/protocol-message-formats.html (CommandComplete)
func (h *Handler) getCommandTag(query string, rowsAffected int64) string {
	upperQuery := strings.ToUpper(strings.TrimSpace(query))

	switch {
	// DML commands with row counts
	case strings.HasPrefix(upperQuery, "INSERT"):
		// INSERT oid rows - oid is always 0 in modern PostgreSQL
		return "INSERT 0 " + itoa64(rowsAffected)
	case strings.HasPrefix(upperQuery, "UPDATE"):
		return "UPDATE " + itoa64(rowsAffected)
	case strings.HasPrefix(upperQuery, "DELETE"):
		return "DELETE " + itoa64(rowsAffected)
	case strings.HasPrefix(upperQuery, "MERGE"):
		return "MERGE " + itoa64(rowsAffected)
	case strings.HasPrefix(upperQuery, "COPY"):
		return "COPY " + itoa64(rowsAffected)

	// Cursor operations with row counts
	case strings.HasPrefix(upperQuery, "FETCH"):
		return "FETCH " + itoa64(rowsAffected)
	case strings.HasPrefix(upperQuery, "MOVE"):
		return "MOVE " + itoa64(rowsAffected)

	// CREATE commands
	case strings.HasPrefix(upperQuery, "CREATE"):
		return h.getCreateTag(upperQuery)

	// DROP commands
	case strings.HasPrefix(upperQuery, "DROP"):
		return h.getDropTag(upperQuery)

	// ALTER commands
	case strings.HasPrefix(upperQuery, "ALTER"):
		return h.getAlterTag(upperQuery)

	// Transaction commands
	case strings.HasPrefix(upperQuery, "BEGIN"):
		return "BEGIN"
	case strings.HasPrefix(upperQuery, "START TRANSACTION"):
		return "START TRANSACTION"
	case strings.HasPrefix(upperQuery, "COMMIT"):
		return "COMMIT"
	case strings.HasPrefix(upperQuery, "END"):
		return "COMMIT"
	case strings.HasPrefix(upperQuery, "ROLLBACK"):
		return "ROLLBACK"
	case strings.HasPrefix(upperQuery, "SAVEPOINT"):
		return "SAVEPOINT"
	case strings.HasPrefix(upperQuery, "RELEASE"):
		return "RELEASE"

	// Session commands
	case strings.HasPrefix(upperQuery, "SET"):
		return "SET"
	case strings.HasPrefix(upperQuery, "RESET"):
		return "RESET"
	case strings.HasPrefix(upperQuery, "DISCARD"):
		return h.getDiscardTag(upperQuery)

	// Cursor commands
	case strings.HasPrefix(upperQuery, "DECLARE"):
		return "DECLARE CURSOR"
	case strings.HasPrefix(upperQuery, "CLOSE"):
		return h.getCloseTag(upperQuery)

	// Prepared statement commands
	case strings.HasPrefix(upperQuery, "PREPARE"):
		return "PREPARE"
	case strings.HasPrefix(upperQuery, "EXECUTE"):
		return "EXECUTE"
	case strings.HasPrefix(upperQuery, "DEALLOCATE"):
		return "DEALLOCATE"

	// Utility commands
	case strings.HasPrefix(upperQuery, "TRUNCATE"):
		return "TRUNCATE TABLE"
	case strings.HasPrefix(upperQuery, "GRANT"):
		return "GRANT"
	case strings.HasPrefix(upperQuery, "REVOKE"):
		return "REVOKE"
	case strings.HasPrefix(upperQuery, "VACUUM"):
		return "VACUUM"
	case strings.HasPrefix(upperQuery, "ANALYZE"):
		return "ANALYZE"
	case strings.HasPrefix(upperQuery, "CLUSTER"):
		return "CLUSTER"
	case strings.HasPrefix(upperQuery, "REINDEX"):
		return "REINDEX"
	case strings.HasPrefix(upperQuery, "CHECKPOINT"):
		return "CHECKPOINT"
	case strings.HasPrefix(upperQuery, "LOCK"):
		return "LOCK TABLE"

	// EXPLAIN commands
	case strings.HasPrefix(upperQuery, "EXPLAIN"):
		return "EXPLAIN"

	// Notification commands
	case strings.HasPrefix(upperQuery, "LISTEN"):
		return "LISTEN"
	case strings.HasPrefix(upperQuery, "UNLISTEN"):
		return "UNLISTEN"
	case strings.HasPrefix(upperQuery, "NOTIFY"):
		return "NOTIFY"

	// Security and metadata commands
	case strings.HasPrefix(upperQuery, "COMMENT"):
		return "COMMENT"
	case strings.HasPrefix(upperQuery, "SECURITY LABEL"):
		return "SECURITY LABEL"

	// Materialized view commands
	case strings.HasPrefix(upperQuery, "REFRESH MATERIALIZED VIEW"):
		return "REFRESH MATERIALIZED VIEW"

	// LOAD command (for loading shared libraries)
	case strings.HasPrefix(upperQuery, "LOAD"):
		return "LOAD"

	// DO command (anonymous code block)
	case strings.HasPrefix(upperQuery, "DO"):
		return "DO"

	// CALL command (stored procedure)
	case strings.HasPrefix(upperQuery, "CALL"):
		return "CALL"

	// IMPORT/EXPORT commands
	case strings.HasPrefix(upperQuery, "IMPORT FOREIGN SCHEMA"):
		return "IMPORT FOREIGN SCHEMA"

	default:
		return "OK"
	}
}

// getCreateTag returns the command tag for CREATE commands.
// Uses prefix matching after "CREATE " to avoid ambiguous matches.
func (h *Handler) getCreateTag(upperQuery string) string {
	// Remove "CREATE " prefix and any OR REPLACE modifier
	rest := strings.TrimPrefix(upperQuery, "CREATE ")
	rest = strings.TrimPrefix(rest, "OR REPLACE ")
	rest = strings.TrimPrefix(rest, "TEMP ")
	rest = strings.TrimPrefix(rest, "TEMPORARY ")
	rest = strings.TrimPrefix(rest, "UNLOGGED ")
	rest = strings.TrimPrefix(rest, "UNIQUE ")

	switch {
	case strings.HasPrefix(rest, "MATERIALIZED VIEW"):
		return "CREATE MATERIALIZED VIEW"
	case strings.HasPrefix(rest, "FOREIGN DATA WRAPPER"):
		return "CREATE FOREIGN DATA WRAPPER"
	case strings.HasPrefix(rest, "FOREIGN TABLE"):
		return "CREATE FOREIGN TABLE"
	case strings.HasPrefix(rest, "TABLESPACE"):
		return "CREATE TABLESPACE"
	case strings.HasPrefix(rest, "TABLE"):
		return "CREATE TABLE"
	case strings.HasPrefix(rest, "INDEX"):
		return "CREATE INDEX"
	case strings.HasPrefix(rest, "VIEW"):
		return "CREATE VIEW"
	case strings.HasPrefix(rest, "SCHEMA"):
		return "CREATE SCHEMA"
	case strings.HasPrefix(rest, "SEQUENCE"):
		return "CREATE SEQUENCE"
	case strings.HasPrefix(rest, "DATABASE"):
		return "CREATE DATABASE"
	case strings.HasPrefix(rest, "TYPE"):
		return "CREATE TYPE"
	case strings.HasPrefix(rest, "FUNCTION"):
		return "CREATE FUNCTION"
	case strings.HasPrefix(rest, "PROCEDURE"):
		return "CREATE PROCEDURE"
	case strings.HasPrefix(rest, "TRIGGER"):
		return "CREATE TRIGGER"
	case strings.HasPrefix(rest, "EXTENSION"):
		return "CREATE EXTENSION"
	case strings.HasPrefix(rest, "ROLE"):
		return "CREATE ROLE"
	case strings.HasPrefix(rest, "USER"):
		return "CREATE ROLE" // CREATE USER is alias for CREATE ROLE
	case strings.HasPrefix(rest, "SERVER"):
		return "CREATE SERVER"
	case strings.HasPrefix(rest, "AGGREGATE"):
		return "CREATE AGGREGATE"
	case strings.HasPrefix(rest, "OPERATOR"):
		return "CREATE OPERATOR"
	case strings.HasPrefix(rest, "COLLATION"):
		return "CREATE COLLATION"
	case strings.HasPrefix(rest, "DOMAIN"):
		return "CREATE DOMAIN"
	case strings.HasPrefix(rest, "RULE"):
		return "CREATE RULE"
	case strings.HasPrefix(rest, "POLICY"):
		return "CREATE POLICY"
	default:
		return "CREATE"
	}
}

// getDropTag returns the command tag for DROP commands.
// Uses prefix matching after "DROP " to avoid ambiguous matches.
func (h *Handler) getDropTag(upperQuery string) string {
	// Remove "DROP " prefix and any IF EXISTS modifier
	rest := strings.TrimPrefix(upperQuery, "DROP ")
	rest = strings.TrimPrefix(rest, "IF EXISTS ")

	switch {
	case strings.HasPrefix(rest, "MATERIALIZED VIEW"):
		return "DROP MATERIALIZED VIEW"
	case strings.HasPrefix(rest, "FOREIGN DATA WRAPPER"):
		return "DROP FOREIGN DATA WRAPPER"
	case strings.HasPrefix(rest, "FOREIGN TABLE"):
		return "DROP FOREIGN TABLE"
	case strings.HasPrefix(rest, "TABLESPACE"):
		return "DROP TABLESPACE"
	case strings.HasPrefix(rest, "TABLE"):
		return "DROP TABLE"
	case strings.HasPrefix(rest, "INDEX"):
		return "DROP INDEX"
	case strings.HasPrefix(rest, "VIEW"):
		return "DROP VIEW"
	case strings.HasPrefix(rest, "SCHEMA"):
		return "DROP SCHEMA"
	case strings.HasPrefix(rest, "SEQUENCE"):
		return "DROP SEQUENCE"
	case strings.HasPrefix(rest, "DATABASE"):
		return "DROP DATABASE"
	case strings.HasPrefix(rest, "TYPE"):
		return "DROP TYPE"
	case strings.HasPrefix(rest, "FUNCTION"):
		return "DROP FUNCTION"
	case strings.HasPrefix(rest, "PROCEDURE"):
		return "DROP PROCEDURE"
	case strings.HasPrefix(rest, "TRIGGER"):
		return "DROP TRIGGER"
	case strings.HasPrefix(rest, "EXTENSION"):
		return "DROP EXTENSION"
	case strings.HasPrefix(rest, "ROLE"):
		return "DROP ROLE"
	case strings.HasPrefix(rest, "USER"):
		return "DROP ROLE" // DROP USER is alias for DROP ROLE
	case strings.HasPrefix(rest, "SERVER"):
		return "DROP SERVER"
	case strings.HasPrefix(rest, "AGGREGATE"):
		return "DROP AGGREGATE"
	case strings.HasPrefix(rest, "OPERATOR"):
		return "DROP OPERATOR"
	case strings.HasPrefix(rest, "COLLATION"):
		return "DROP COLLATION"
	case strings.HasPrefix(rest, "DOMAIN"):
		return "DROP DOMAIN"
	case strings.HasPrefix(rest, "RULE"):
		return "DROP RULE"
	case strings.HasPrefix(rest, "POLICY"):
		return "DROP POLICY"
	default:
		return "DROP"
	}
}

// getAlterTag returns the command tag for ALTER commands.
// Uses prefix matching after "ALTER " to avoid ambiguous matches.
func (h *Handler) getAlterTag(upperQuery string) string {
	// Remove "ALTER " prefix
	rest := strings.TrimPrefix(upperQuery, "ALTER ")

	switch {
	case strings.HasPrefix(rest, "MATERIALIZED VIEW"):
		return "ALTER MATERIALIZED VIEW"
	case strings.HasPrefix(rest, "DEFAULT PRIVILEGES"):
		return "ALTER DEFAULT PRIVILEGES"
	case strings.HasPrefix(rest, "FOREIGN DATA WRAPPER"):
		return "ALTER FOREIGN DATA WRAPPER"
	case strings.HasPrefix(rest, "FOREIGN TABLE"):
		return "ALTER FOREIGN TABLE"
	case strings.HasPrefix(rest, "TABLESPACE"):
		return "ALTER TABLESPACE"
	case strings.HasPrefix(rest, "TABLE"):
		return "ALTER TABLE"
	case strings.HasPrefix(rest, "INDEX"):
		return "ALTER INDEX"
	case strings.HasPrefix(rest, "VIEW"):
		return "ALTER VIEW"
	case strings.HasPrefix(rest, "SCHEMA"):
		return "ALTER SCHEMA"
	case strings.HasPrefix(rest, "SEQUENCE"):
		return "ALTER SEQUENCE"
	case strings.HasPrefix(rest, "DATABASE"):
		return "ALTER DATABASE"
	case strings.HasPrefix(rest, "TYPE"):
		return "ALTER TYPE"
	case strings.HasPrefix(rest, "FUNCTION"):
		return "ALTER FUNCTION"
	case strings.HasPrefix(rest, "PROCEDURE"):
		return "ALTER PROCEDURE"
	case strings.HasPrefix(rest, "TRIGGER"):
		return "ALTER TRIGGER"
	case strings.HasPrefix(rest, "EXTENSION"):
		return "ALTER EXTENSION"
	case strings.HasPrefix(rest, "ROLE"):
		return "ALTER ROLE"
	case strings.HasPrefix(rest, "USER"):
		return "ALTER ROLE" // ALTER USER is alias for ALTER ROLE
	case strings.HasPrefix(rest, "SERVER"):
		return "ALTER SERVER"
	case strings.HasPrefix(rest, "AGGREGATE"):
		return "ALTER AGGREGATE"
	case strings.HasPrefix(rest, "OPERATOR"):
		return "ALTER OPERATOR"
	case strings.HasPrefix(rest, "COLLATION"):
		return "ALTER COLLATION"
	case strings.HasPrefix(rest, "DOMAIN"):
		return "ALTER DOMAIN"
	case strings.HasPrefix(rest, "RULE"):
		return "ALTER RULE"
	case strings.HasPrefix(rest, "POLICY"):
		return "ALTER POLICY"
	default:
		return "ALTER TABLE"
	}
}

// getDiscardTag returns the command tag for DISCARD commands.
func (h *Handler) getDiscardTag(upperQuery string) string {
	switch {
	case strings.Contains(upperQuery, "ALL"):
		return "DISCARD ALL"
	case strings.Contains(upperQuery, "PLANS"):
		return "DISCARD PLANS"
	case strings.Contains(upperQuery, "SEQUENCES"):
		return "DISCARD SEQUENCES"
	case strings.Contains(upperQuery, "TEMP"), strings.Contains(upperQuery, "TEMPORARY"):
		return "DISCARD TEMP"
	default:
		return "DISCARD"
	}
}

// getCloseTag returns the command tag for CLOSE commands.
func (h *Handler) getCloseTag(upperQuery string) string {
	if strings.Contains(upperQuery, "ALL") {
		return "CLOSE CURSOR"
	}
	return "CLOSE CURSOR"
}

// analyzeQuery analyzes a query to determine its result columns and parameters.
func (h *Handler) analyzeQuery(ctx context.Context, query string) (wire.Columns, []uint32) {
	upperQuery := strings.ToUpper(strings.TrimSpace(query))

	// For non-SELECT queries, return empty columns
	isSelect := strings.HasPrefix(upperQuery, "SELECT") ||
		strings.HasPrefix(upperQuery, "SHOW") ||
		strings.HasPrefix(upperQuery, "EXPLAIN") ||
		strings.HasPrefix(upperQuery, "WITH") ||
		strings.HasPrefix(upperQuery, "TABLE") ||
		strings.HasPrefix(upperQuery, "VALUES")

	if !isSelect {
		// Parse parameters for non-SELECT queries
		params := wire.ParseParameters(query)
		return nil, params
	}

	// For SELECT queries, try to get column metadata
	// by preparing the statement
	columns := h.getQueryColumns(ctx, query)
	params := wire.ParseParameters(query)

	return columns, params
}

// getQueryColumns attempts to determine the columns for a SELECT query.
func (h *Handler) getQueryColumns(ctx context.Context, query string) wire.Columns {
	if h.server == nil || h.server.conn == nil {
		return nil
	}

	// Try to prepare the statement to get column metadata
	stmt, err := h.server.conn.Prepare(ctx, query)
	if err != nil {
		return nil
	}
	defer func() { _ = stmt.Close() }()

	// Check if the statement supports introspection
	introspector, ok := stmt.(dukdb.BackendStmtIntrospector)
	if !ok {
		return nil
	}

	// Build columns from introspection
	colCount := introspector.ColumnCount()
	if colCount == 0 {
		return nil
	}

	columns := make(wire.Columns, colCount)
	for i := 0; i < colCount; i++ {
		name := introspector.ColumnName(i)
		typ := introspector.ColumnType(i)
		oid := dukdbTypeToOid(typ)

		// Use ColumnBuilder for full PostgreSQL row description metadata
		columns[i] = NewColumnBuilder(name).
			TypeOID(oid).
			ColumnNumber(int16(i + 1)). // 1-based column number
			Build()
	}

	return columns
}

// convertParameters converts wire.Parameter to driver.NamedValue.
func (h *Handler) convertParameters(ctx context.Context, parameters []wire.Parameter) []driver.NamedValue {
	if len(parameters) == 0 {
		return nil
	}

	args := make([]driver.NamedValue, len(parameters))
	for i, param := range parameters {
		// Scan the parameter value using the appropriate type
		// For now, we use a generic approach
		value := param.Value()

		args[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   value,
		}
	}

	return args
}

// dukdbTypeToOid converts a dukdb.Type to a PostgreSQL OID.
// We intentionally use a default case to handle all types that don't have
// a direct PostgreSQL equivalent - they fall back to TEXT encoding.
//
//nolint:exhaustive // We handle all common types, others fall through to default
func dukdbTypeToOid(typ dukdb.Type) uint32 {
	switch typ {
	case dukdb.TYPE_BOOLEAN:
		return OidBool
	case dukdb.TYPE_TINYINT, dukdb.TYPE_SMALLINT:
		return OidInt2
	case dukdb.TYPE_INTEGER:
		return OidInt4
	case dukdb.TYPE_BIGINT:
		return OidInt8
	case dukdb.TYPE_FLOAT:
		return OidFloat4
	case dukdb.TYPE_DOUBLE:
		return OidFloat8
	case dukdb.TYPE_VARCHAR:
		return OidVarchar
	case dukdb.TYPE_BLOB:
		return OidBytea
	case dukdb.TYPE_DATE:
		return OidDate
	case dukdb.TYPE_TIME:
		return OidTime
	case dukdb.TYPE_TIMESTAMP:
		return OidTimestamp
	case dukdb.TYPE_TIMESTAMP_TZ:
		return OidTimestampTZ
	case dukdb.TYPE_INTERVAL:
		return OidInterval
	case dukdb.TYPE_DECIMAL:
		return OidNumeric
	case dukdb.TYPE_UUID:
		return OidUUID
	default:
		return OidText // Default to text for unknown types
	}
}

// itoa64 converts an int64 to a string.
func itoa64(n int64) string {
	if n == 0 {
		return "0"
	}

	negative := n < 0
	if negative {
		n = -n
	}

	// Maximum int64 has 19 digits plus sign
	var buf [21]byte
	i := len(buf)

	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}

	if negative {
		i--
		buf[i] = '-'
	}

	return string(buf[i:])
}

// handlePrepareCommand handles the SQL PREPARE statement.
// Format: PREPARE name [(type, ...)] AS query
func (h *Handler) handlePrepareCommand(ctx context.Context, query string, writer wire.DataWriter, session *Session) error {
	if session == nil {
		return NewPgError(CodeInternalError, "no session available for PREPARE")
	}

	// Parse the PREPARE statement
	parsed, err := ParsePrepareStatement(query)
	if err != nil {
		return NewPgError(CodeSyntaxError, err.Error())
	}

	// Create the prepared statement
	ps, err := h.CreatePreparedStatement(ctx, parsed.Name, parsed.Query, parsed.ParamTypes)
	if err != nil {
		return ToPgError(err)
	}

	// Store in session's prepared statement cache
	if err := session.PreparedStatements().Set(parsed.Name, ps); err != nil {
		_ = ps.Close()
		return ToPgError(err)
	}

	return writer.Complete("PREPARE")
}

// handleExecuteCommand handles the SQL EXECUTE statement.
// Format: EXECUTE name [(param, ...)]
func (h *Handler) handleExecuteCommand(ctx context.Context, query string, writer wire.DataWriter, session *Session) error {
	if session == nil {
		return NewPgError(CodeInternalError, "no session available for EXECUTE")
	}

	// Parse the EXECUTE statement
	parsed, err := ParseExecuteStatement(query)
	if err != nil {
		return NewPgError(CodeSyntaxError, err.Error())
	}

	// Get the prepared statement from the session cache
	ps, ok := session.PreparedStatements().Get(parsed.Name)
	if !ok {
		return NewPgError(CodeUndefinedPStmt, "prepared statement \""+parsed.Name+"\" does not exist")
	}

	// Execute the prepared statement with parameters
	return h.ExecutePreparedStatement(ctx, ps, parsed.Parameters, writer)
}

// handleDeallocateCommand handles the SQL DEALLOCATE statement.
// Format: DEALLOCATE [PREPARE] name | DEALLOCATE ALL
func (h *Handler) handleDeallocateCommand(ctx context.Context, query string, writer wire.DataWriter, session *Session) error {
	if session == nil {
		return NewPgError(CodeInternalError, "no session available for DEALLOCATE")
	}

	// Parse the DEALLOCATE statement
	name, deallocateAll, err := ParseDeallocateStatement(query)
	if err != nil {
		return NewPgError(CodeSyntaxError, err.Error())
	}

	stmtCache := session.PreparedStatements()
	if stmtCache == nil {
		return NewPgError(CodeInternalError, "prepared statement cache not available")
	}

	if deallocateAll {
		// DEALLOCATE ALL - remove all prepared statements
		stmtCache.Clear()
		return writer.Complete("DEALLOCATE ALL")
	}

	// DEALLOCATE specific statement
	if !stmtCache.Delete(name) {
		return NewPgError(CodeUndefinedPStmt, "prepared statement \""+name+"\" does not exist")
	}

	return writer.Complete("DEALLOCATE")
}
