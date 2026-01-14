package server

import (
	"context"
	"database/sql/driver"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"sync"

	dukdb "github.com/dukdb/dukdb-go"
	wire "github.com/jeroenrinzema/psql-wire"
)

// PreparedStatement represents a prepared statement stored in the session.
// It wraps the underlying backend statement with additional PostgreSQL metadata.
type PreparedStatement struct {
	Name       string          // Statement name (empty for unnamed)
	Query      string          // Original SQL query
	ParamTypes []uint32        // Parameter OIDs (PostgreSQL type identifiers)
	Columns    wire.Columns    // Result columns
	Stmt       dukdb.BackendStmt // Underlying prepared statement
}

// Close closes the prepared statement and releases resources.
func (ps *PreparedStatement) Close() error {
	if ps.Stmt != nil {
		return ps.Stmt.Close()
	}
	return nil
}

// Portal represents a bound prepared statement with parameters.
// In PostgreSQL protocol, a portal is created by binding parameters to a prepared statement.
type Portal struct {
	Name       string
	Statement  *PreparedStatement
	Parameters []driver.NamedValue
	Executed   bool
}

// PreparedStmtCache manages prepared statements for a session.
// It provides thread-safe storage and retrieval of prepared statements.
type PreparedStmtCache struct {
	mu         sync.RWMutex
	statements map[string]*PreparedStatement
}

// NewPreparedStmtCache creates a new prepared statement cache.
func NewPreparedStmtCache() *PreparedStmtCache {
	return &PreparedStmtCache{
		statements: make(map[string]*PreparedStatement),
	}
}

// Set stores a prepared statement with the given name.
// If a statement with the same name exists, it is closed and replaced.
func (c *PreparedStmtCache) Set(name string, stmt *PreparedStatement) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Close existing statement if present
	if existing, ok := c.statements[name]; ok {
		_ = existing.Close()
	}

	c.statements[name] = stmt
	return nil
}

// Get retrieves a prepared statement by name.
// Returns nil, false if not found.
func (c *PreparedStmtCache) Get(name string) (*PreparedStatement, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stmt, ok := c.statements[name]
	return stmt, ok
}

// Delete removes a prepared statement by name.
// Returns true if the statement existed and was removed.
func (c *PreparedStmtCache) Delete(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if stmt, ok := c.statements[name]; ok {
		_ = stmt.Close()
		delete(c.statements, name)
		return true
	}
	return false
}

// Clear removes all prepared statements.
func (c *PreparedStmtCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, stmt := range c.statements {
		_ = stmt.Close()
	}
	c.statements = make(map[string]*PreparedStatement)
}

// Names returns the names of all prepared statements.
func (c *PreparedStmtCache) Names() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.statements))
	for name := range c.statements {
		names = append(names, name)
	}
	return names
}

// Count returns the number of prepared statements.
func (c *PreparedStmtCache) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.statements)
}

// Close closes all prepared statements and clears the cache.
func (c *PreparedStmtCache) Close() error {
	c.Clear()
	return nil
}

// PortalCache manages portals (bound prepared statements) for a session.
type PortalCache struct {
	mu      sync.RWMutex
	portals map[string]*Portal
}

// NewPortalCache creates a new portal cache.
func NewPortalCache() *PortalCache {
	return &PortalCache{
		portals: make(map[string]*Portal),
	}
}

// Set stores a portal with the given name.
func (c *PortalCache) Set(name string, portal *Portal) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.portals[name] = portal
}

// Get retrieves a portal by name.
func (c *PortalCache) Get(name string) (*Portal, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	portal, ok := c.portals[name]
	return portal, ok
}

// Delete removes a portal by name.
func (c *PortalCache) Delete(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.portals[name]; ok {
		delete(c.portals, name)
		return true
	}
	return false
}

// Clear removes all portals.
func (c *PortalCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.portals = make(map[string]*Portal)
}

// Close closes all portals and clears the cache.
func (c *PortalCache) Close() error {
	c.Clear()
	return nil
}

// ParsedPrepareStatement contains parsed information from a PREPARE SQL statement.
type ParsedPrepareStatement struct {
	Name       string
	ParamTypes []string
	Query      string
}

// ParsePrepareStatement parses a SQL PREPARE statement.
// Format: PREPARE name [(type, ...)] AS query
func ParsePrepareStatement(sql string) (*ParsedPrepareStatement, error) {
	// Match: PREPARE name [(types)] AS query
	// The types part is optional
	pattern := regexp.MustCompile(`(?i)^\s*PREPARE\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:\(([^)]*)\))?\s+AS\s+(.+)$`)
	matches := pattern.FindStringSubmatch(sql)
	if matches == nil {
		return nil, errors.New("invalid PREPARE statement syntax")
	}

	result := &ParsedPrepareStatement{
		Name:  matches[1],
		Query: strings.TrimSpace(matches[3]),
	}

	// Parse parameter types if present
	if matches[2] != "" {
		typeStrings := strings.Split(matches[2], ",")
		result.ParamTypes = make([]string, len(typeStrings))
		for i, t := range typeStrings {
			result.ParamTypes[i] = strings.TrimSpace(t)
		}
	}

	return result, nil
}

// ParsedExecuteStatement contains parsed information from an EXECUTE SQL statement.
type ParsedExecuteStatement struct {
	Name       string
	Parameters []string
}

// ParseExecuteStatement parses a SQL EXECUTE statement.
// Format: EXECUTE name [(param, ...)]
func ParseExecuteStatement(sql string) (*ParsedExecuteStatement, error) {
	// Match: EXECUTE name - capture the name first
	pattern := regexp.MustCompile(`(?i)^\s*EXECUTE\s+([a-zA-Z_][a-zA-Z0-9_]*)`)
	matches := pattern.FindStringSubmatch(sql)
	if matches == nil {
		return nil, errors.New("invalid EXECUTE statement syntax")
	}

	result := &ParsedExecuteStatement{
		Name: matches[1],
	}

	// Look for the parameter list by finding the opening parenthesis
	rest := sql[len(matches[0]):]
	rest = strings.TrimSpace(rest)

	if strings.HasPrefix(rest, "(") {
		// Find the matching closing parenthesis, accounting for nesting
		params := extractParenContent(rest)
		if params != "" {
			result.Parameters = parseParameterList(params)
		}
	}

	return result, nil
}

// extractParenContent extracts content from balanced parentheses.
// Given "(a, b, (c, d))", returns "a, b, (c, d)".
func extractParenContent(s string) string {
	if !strings.HasPrefix(s, "(") {
		return ""
	}

	depth := 0
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(s); i++ {
		c := s[i]

		// Handle string literals
		if !inString && (c == '\'' || c == '"') {
			inString = true
			stringChar = c
			continue
		}

		if inString {
			if c == stringChar {
				// Check for escaped quote
				if i+1 < len(s) && s[i+1] == stringChar {
					i++
				} else {
					inString = false
				}
			}
			continue
		}

		switch c {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				// Found the matching closing paren
				return s[1:i]
			}
		}
	}

	// No matching closing paren found
	return ""
}

// parseParameterList parses a comma-separated parameter list.
// Handles quoted strings and nested parentheses.
func parseParameterList(params string) []string {
	var result []string
	var current strings.Builder
	depth := 0
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(params); i++ {
		c := params[i]

		// Handle string literals
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
				if i+1 < len(params) && params[i+1] == stringChar {
					i++
					current.WriteByte(params[i])
				} else {
					inString = false
				}
			}
			continue
		}

		// Handle nested parentheses
		switch c {
		case '(':
			depth++
			current.WriteByte(c)
		case ')':
			depth--
			current.WriteByte(c)
		case ',':
			if depth == 0 {
				// Parameter separator at top level
				result = append(result, strings.TrimSpace(current.String()))
				current.Reset()
			} else {
				current.WriteByte(c)
			}
		default:
			current.WriteByte(c)
		}
	}

	// Don't forget the last parameter
	if s := strings.TrimSpace(current.String()); s != "" {
		result = append(result, s)
	}

	return result
}

// ParseDeallocateStatement parses a SQL DEALLOCATE statement.
// Format: DEALLOCATE [PREPARE] name | DEALLOCATE ALL
func ParseDeallocateStatement(sql string) (name string, deallocateAll bool, err error) {
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))

	// Check for DEALLOCATE ALL
	if upperSQL == "DEALLOCATE ALL" || upperSQL == "DEALLOCATE PREPARE ALL" {
		return "", true, nil
	}

	// Match: DEALLOCATE [PREPARE] name
	pattern := regexp.MustCompile(`(?i)^\s*DEALLOCATE\s+(?:PREPARE\s+)?([a-zA-Z_][a-zA-Z0-9_]*)`)
	matches := pattern.FindStringSubmatch(sql)
	if matches == nil {
		return "", false, errors.New("invalid DEALLOCATE statement syntax")
	}

	return matches[1], false, nil
}

// InferParameterTypes attempts to infer parameter types from a query.
// It analyzes the query context to determine likely types for each $N parameter.
func InferParameterTypes(query string) []uint32 {
	// Find all parameter references
	params := wire.ParseParameters(query)
	if len(params) == 0 {
		return nil
	}

	// Try to infer types from context
	result := make([]uint32, len(params))
	for i := range result {
		result[i] = OidUnknown // Default to unknown
	}

	// Simple type inference based on query patterns
	upperQuery := strings.ToUpper(query)

	// Look for common patterns
	patterns := []struct {
		pattern *regexp.Regexp
		oid     uint32
	}{
		// Integer comparisons
		{regexp.MustCompile(`(?i)\bid\s*=\s*\$(\d+)`), OidInt4},
		{regexp.MustCompile(`(?i)_id\s*=\s*\$(\d+)`), OidInt4},
		{regexp.MustCompile(`(?i)LIMIT\s+\$(\d+)`), OidInt8},
		{regexp.MustCompile(`(?i)OFFSET\s+\$(\d+)`), OidInt8},

		// Boolean patterns
		{regexp.MustCompile(`(?i)WHERE\s+\$(\d+)`), OidBool},
		{regexp.MustCompile(`(?i)AND\s+\$(\d+)`), OidBool},
		{regexp.MustCompile(`(?i)OR\s+\$(\d+)`), OidBool},

		// Text patterns
		{regexp.MustCompile(`(?i)LIKE\s+\$(\d+)`), OidText},
		{regexp.MustCompile(`(?i)ILIKE\s+\$(\d+)`), OidText},
		{regexp.MustCompile(`(?i)~\s*\$(\d+)`), OidText},

		// IN clauses - typically the same type as the column
		{regexp.MustCompile(`(?i)IN\s*\(\s*\$(\d+)`), OidUnknown},
	}

	for _, p := range patterns {
		matches := p.pattern.FindAllStringSubmatch(upperQuery, -1)
		for _, match := range matches {
			if len(match) > 1 {
				paramNum, err := strconv.Atoi(match[1])
				if err == nil && paramNum > 0 && paramNum <= len(result) {
					result[paramNum-1] = p.oid
				}
			}
		}
	}

	return result
}

// TypeNameToOid converts a PostgreSQL type name to its OID.
func TypeNameToOid(typeName string) uint32 {
	normalizedName := strings.ToLower(strings.TrimSpace(typeName))

	switch normalizedName {
	case "boolean", "bool":
		return OidBool
	case "bytea":
		return OidBytea
	case "char", "character":
		return OidChar
	case "smallint", "int2":
		return OidInt2
	case "integer", "int", "int4":
		return OidInt4
	case "bigint", "int8":
		return OidInt8
	case "real", "float4":
		return OidFloat4
	case "double precision", "float8", "double":
		return OidFloat8
	case "text":
		return OidText
	case "varchar", "character varying":
		return OidVarchar
	case "date":
		return OidDate
	case "time", "time without time zone":
		return OidTime
	case "timestamp", "timestamp without time zone":
		return OidTimestamp
	case "timestamptz", "timestamp with time zone":
		return OidTimestampTZ
	case "interval":
		return OidInterval
	case "numeric", "decimal":
		return OidNumeric
	case "uuid":
		return OidUUID
	case "json":
		return OidJSON
	case "jsonb":
		return OidJSONB
	case "oid":
		return OidOid
	default:
		return OidUnknown
	}
}

// OidToTypeName converts a PostgreSQL OID to its type name.
func OidToTypeName(oid uint32) string {
	switch oid {
	case OidBool:
		return "boolean"
	case OidBytea:
		return "bytea"
	case OidChar:
		return "char"
	case OidInt2:
		return "smallint"
	case OidInt4:
		return "integer"
	case OidInt8:
		return "bigint"
	case OidFloat4:
		return "real"
	case OidFloat8:
		return "double precision"
	case OidText:
		return "text"
	case OidVarchar:
		return "varchar"
	case OidDate:
		return "date"
	case OidTime:
		return "time"
	case OidTimestamp:
		return "timestamp"
	case OidTimestampTZ:
		return "timestamptz"
	case OidInterval:
		return "interval"
	case OidNumeric:
		return "numeric"
	case OidUUID:
		return "uuid"
	case OidJSON:
		return "json"
	case OidJSONB:
		return "jsonb"
	case OidOid:
		return "oid"
	default:
		return "unknown"
	}
}

// CreatePreparedStatement creates a PreparedStatement from a query.
func (h *Handler) CreatePreparedStatement(ctx context.Context, name, query string, paramTypeNames []string) (*PreparedStatement, error) {
	if h.server == nil || h.server.conn == nil {
		return nil, ErrNoConnection
	}

	// Prepare the statement using the backend
	stmt, err := h.server.conn.Prepare(ctx, query)
	if err != nil {
		return nil, err
	}

	// Build parameter types
	var paramTypes []uint32
	if len(paramTypeNames) > 0 {
		// Use explicit type names provided in PREPARE statement
		paramTypes = make([]uint32, len(paramTypeNames))
		for i, typeName := range paramTypeNames {
			paramTypes[i] = TypeNameToOid(typeName)
		}
	} else {
		// Infer types from the statement
		paramTypes = InferParameterTypes(query)
	}

	// Get column information
	var columns wire.Columns
	if introspector, ok := stmt.(dukdb.BackendStmtIntrospector); ok {
		colCount := introspector.ColumnCount()
		if colCount > 0 {
			columns = make(wire.Columns, colCount)
			for i := range colCount {
				columns[i] = wire.Column{
					Name: introspector.ColumnName(i),
					Oid:  dukdbTypeToOid(introspector.ColumnType(i)),
				}
			}
		}
	}

	return &PreparedStatement{
		Name:       name,
		Query:      query,
		ParamTypes: paramTypes,
		Columns:    columns,
		Stmt:       stmt,
	}, nil
}

// ExecutePreparedStatement executes a prepared statement with the given parameters.
func (h *Handler) ExecutePreparedStatement(ctx context.Context, ps *PreparedStatement, params []string, writer wire.DataWriter) error {
	if ps.Stmt == nil {
		return errors.New("prepared statement has no underlying statement")
	}

	// Convert string parameters to driver.NamedValue
	args := make([]driver.NamedValue, len(params))
	for i, param := range params {
		args[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   param,
		}
	}

	// Determine if this is a SELECT-like query
	upperQuery := strings.ToUpper(strings.TrimSpace(ps.Query))
	isSelect := strings.HasPrefix(upperQuery, "SELECT") ||
		strings.HasPrefix(upperQuery, "SHOW") ||
		strings.HasPrefix(upperQuery, "EXPLAIN") ||
		strings.HasPrefix(upperQuery, "WITH") ||
		strings.HasPrefix(upperQuery, "TABLE") ||
		strings.HasPrefix(upperQuery, "VALUES")

	if isSelect {
		// Execute as a query
		rows, columns, err := ps.Stmt.Query(ctx, args)
		if err != nil {
			return err
		}

		// Handle empty result set
		if len(rows) == 0 {
			return writer.Empty()
		}

		// Write rows
		for _, row := range rows {
			values := make([]any, len(columns))
			for i, col := range columns {
				values[i] = row[col]
			}
			if err := writer.Row(values); err != nil {
				return err
			}
		}

		return writer.Complete("SELECT " + itoa(len(rows)))
	}

	// Execute as a non-query
	rowsAffected, err := ps.Stmt.Execute(ctx, args)
	if err != nil {
		return err
	}

	tag := h.getCommandTag(ps.Query, rowsAffected)
	return writer.Complete(tag)
}
