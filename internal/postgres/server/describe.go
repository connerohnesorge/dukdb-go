// Package server provides PostgreSQL wire protocol server functionality.
//
// This file documents the DESCRIBE message support for statement introspection
// in the extended query protocol.
//
// DESCRIBE Message Protocol
//
// The PostgreSQL DESCRIBE message ('D') allows clients to introspect prepared
// statements and portals without executing them. This is essential for clients
// that use the extended query protocol (like pgx, libpq, and JDBC drivers).
//
// # Message Types
//
// DESCRIBE has two variants:
//
//  1. Describe Statement ('S'): Returns ParameterDescription + RowDescription
//     - ParameterDescription: Lists the OIDs of parameters ($1, $2, etc.)
//     - RowDescription: Describes the result columns
//
//  2. Describe Portal ('P'): Returns RowDescription only
//     - RowDescription: Describes the result columns with format codes
//
// # Implementation
//
// The psql-wire library handles DESCRIBE messages through its handleDescribe
// method. Our implementation ensures:
//
//  1. Parameter types are inferred or explicitly declared
//  2. Column types are derived from query introspection
//  3. Type OIDs follow PostgreSQL conventions
//
// # Type Inference
//
// When clients don't provide explicit parameter types, we infer types from
// query context using pattern matching:
//
//   - WHERE id = $1 -> INT4 (integer)
//   - WHERE name LIKE $1 -> TEXT
//   - LIMIT $1 -> INT8 (bigint)
//   - OFFSET $1 -> INT8 (bigint)
//
// # Explicit Type Declaration
//
// Clients can use the PREPARE statement with explicit types:
//
//	PREPARE stmt (integer, text) AS SELECT * FROM users WHERE id = $1 AND name = $2
//
// The declared types are converted to OIDs and stored in the PreparedStatement.
//
// # Column Description
//
// For SELECT queries, column metadata is obtained by preparing the statement
// and introspecting the result columns. Each column includes:
//
//   - Name: Column name
//   - TableOID: OID of the source table (0 for computed columns)
//   - AttrNo: Attribute number in the table
//   - TypeOID: PostgreSQL type OID
//   - TypeSize: Size in bytes (-1 for variable length)
//   - TypeModifier: Type-specific modifier (e.g., VARCHAR length)
//   - Format: 0 for text, 1 for binary
//
// # Wire Protocol Messages
//
// Parse Request:
//
//	'P' + length + name + query + param_count + param_types[]
//
// ParameterDescription Response:
//
//	't' + length + param_count + param_oids[]
//
// RowDescription Response:
//
//	'T' + length + field_count + (field_info)*
//
// NoData Response (for non-returning queries):
//
//	'n' + length
//
// # Example Flow
//
//	Client                          Server
//	   |                               |
//	   |------- Parse --------------->|  (name="", query="SELECT $1::int")
//	   |<------ ParseComplete --------|
//	   |                               |
//	   |------- Describe 'S' "" ----->|  (describe unnamed statement)
//	   |<------ ParameterDescription --|  (param_count=1, oid=23[int4])
//	   |<------ RowDescription --------|  (col_count=1, name="int4", oid=23)
//	   |                               |
//	   |------- Bind ---------------->|  (portal="", stmt="", params=[42])
//	   |<------ BindComplete ---------|
//	   |                               |
//	   |------- Describe 'P' "" ----->|  (describe unnamed portal)
//	   |<------ RowDescription --------|  (with format codes from Bind)
//	   |                               |
//	   |------- Execute ------------->|  (portal="", max_rows=0)
//	   |<------ DataRow ---------------|  [42]
//	   |<------ CommandComplete -------|  "SELECT 1"
//	   |                               |
//	   |------- Sync ---------------->|
//	   |<------ ReadyForQuery ---------|
package server

import (
	"regexp"
	"strconv"
	"strings"

	wire "github.com/jeroenrinzema/psql-wire"
)

// EnhancedParameterInference provides additional parameter type inference
// beyond the basic InferParameterTypes function.
//
// This is used when creating PreparedStatements to provide better type
// information for DESCRIBE responses.
func EnhancedParameterInference(query string) []uint32 {
	// Use the existing InferParameterTypes first
	params := InferParameterTypes(query)
	if params == nil {
		return nil
	}

	// Apply additional inference patterns
	enhanceWithCastPatterns(query, params)
	enhanceWithComparisonPatterns(query, params)
	enhanceWithFunctionPatterns(query, params)

	return params
}

// enhanceWithCastPatterns looks for explicit cast patterns like $1::int
func enhanceWithCastPatterns(query string, params []uint32) {
	// Pattern: $N::type or CAST($N AS type)
	castPatterns := []struct {
		pattern *regexp.Regexp
		oid     uint32
	}{
		// Direct casts: $1::integer, $1::int4, etc.
		{regexp.MustCompile(`\$(\d+)::(?:integer|int4|int)\b`), OidInt4},
		{regexp.MustCompile(`\$(\d+)::(?:bigint|int8)\b`), OidInt8},
		{regexp.MustCompile(`\$(\d+)::(?:smallint|int2)\b`), OidInt2},
		{regexp.MustCompile(`\$(\d+)::(?:text|varchar)\b`), OidText},
		{regexp.MustCompile(`\$(\d+)::(?:boolean|bool)\b`), OidBool},
		{regexp.MustCompile(`\$(\d+)::(?:real|float4)\b`), OidFloat4},
		{regexp.MustCompile(`\$(\d+)::(?:double precision|float8)\b`), OidFloat8},
		{regexp.MustCompile(`\$(\d+)::(?:numeric|decimal)\b`), OidNumeric},
		{regexp.MustCompile(`\$(\d+)::date\b`), OidDate},
		{regexp.MustCompile(`\$(\d+)::(?:timestamp|timestamptz)\b`), OidTimestamp},
		{regexp.MustCompile(`\$(\d+)::uuid\b`), OidUUID},
		{regexp.MustCompile(`\$(\d+)::(?:json|jsonb)\b`), OidJSON},
		{regexp.MustCompile(`\$(\d+)::bytea\b`), OidBytea},
	}

	lowerQuery := strings.ToLower(query)

	for _, p := range castPatterns {
		matches := p.pattern.FindAllStringSubmatch(lowerQuery, -1)
		for _, match := range matches {
			if len(match) > 1 {
				paramNum, err := strconv.Atoi(match[1])
				if err == nil && paramNum > 0 && paramNum <= len(params) {
					// Explicit casts take precedence
					params[paramNum-1] = p.oid
				}
			}
		}
	}
}

// enhanceWithComparisonPatterns looks for comparison patterns with known columns
func enhanceWithComparisonPatterns(query string, params []uint32) {
	// Pattern: column_name = $N where column_name suggests type
	columnPatterns := []struct {
		pattern *regexp.Regexp
		oid     uint32
	}{
		// Common ID columns (usually integers)
		{regexp.MustCompile(`(?i)(?:^|\s)(?:id|pk|fk)\s*=\s*\$(\d+)`), OidInt8},
		{regexp.MustCompile(`(?i)(?:^|\s)\w+_id\s*=\s*\$(\d+)`), OidInt8},

		// Common count/quantity columns (usually integers)
		{regexp.MustCompile(`(?i)(?:count|quantity|amount|num|number)\s*[<>=]+\s*\$(\d+)`), OidInt8},

		// Common name/text columns
		{regexp.MustCompile(`(?i)(?:name|title|description|label|text)\s*=\s*\$(\d+)`), OidText},

		// Date columns
		{regexp.MustCompile(`(?i)(?:date|created|updated|modified)_?(?:at|on)?\s*[<>=]+\s*\$(\d+)`), OidTimestampTZ},

		// Boolean columns
		{regexp.MustCompile(`(?i)(?:is_|has_|can_|should_|enable|active|visible|deleted)\w*\s*=\s*\$(\d+)`), OidBool},

		// Price/money columns (usually numeric)
		{regexp.MustCompile(`(?i)(?:price|cost|amount|total|balance)\s*[<>=]+\s*\$(\d+)`), OidNumeric},
	}

	for _, p := range columnPatterns {
		matches := p.pattern.FindAllStringSubmatch(query, -1)
		for _, match := range matches {
			if len(match) > 1 {
				paramNum, err := strconv.Atoi(match[1])
				if err == nil && paramNum > 0 && paramNum <= len(params) {
					// Only update if still unknown
					if params[paramNum-1] == OidUnknown {
						params[paramNum-1] = p.oid
					}
				}
			}
		}
	}
}

// enhanceWithFunctionPatterns looks for function call patterns
func enhanceWithFunctionPatterns(query string, params []uint32) {
	// Pattern: function($N) where function suggests type
	functionPatterns := []struct {
		pattern *regexp.Regexp
		oid     uint32
	}{
		// String functions
		{regexp.MustCompile(`(?i)(?:length|char_length|upper|lower|trim|ltrim|rtrim)\s*\(\s*\$(\d+)`), OidText},

		// Numeric functions
		{regexp.MustCompile(`(?i)(?:abs|round|ceil|floor|sqrt)\s*\(\s*\$(\d+)`), OidNumeric},

		// Date/Time functions
		{regexp.MustCompile(`(?i)(?:date_part|extract)\s*\([^,]+,\s*\$(\d+)`), OidTimestampTZ},
		{regexp.MustCompile(`(?i)(?:age|date_trunc)\s*\([^,]+,\s*\$(\d+)`), OidTimestampTZ},

		// Array functions
		{regexp.MustCompile(`(?i)array_length\s*\(\s*\$(\d+)`), OidUnknown}, // Array type, unknown element

		// JSON functions
		{regexp.MustCompile(`(?i)(?:json_extract|jsonb_extract|json_array_length)\s*\(\s*\$(\d+)`), OidJSONB},
	}

	for _, p := range functionPatterns {
		matches := p.pattern.FindAllStringSubmatch(query, -1)
		for _, match := range matches {
			if len(match) > 1 {
				paramNum, err := strconv.Atoi(match[1])
				if err == nil && paramNum > 0 && paramNum <= len(params) {
					// Only update if still unknown
					if params[paramNum-1] == OidUnknown {
						params[paramNum-1] = p.oid
					}
				}
			}
		}
	}
}

// GetStatementDescription returns the description info for a prepared statement.
// This is used by the wire protocol handler to respond to DESCRIBE Statement messages.
type StatementDescription struct {
	// ParameterOIDs contains the OIDs of the statement parameters.
	// This is returned in the ParameterDescription message.
	ParameterOIDs []uint32

	// Columns describes the result columns.
	// This is returned in the RowDescription message.
	Columns wire.Columns
}

// GetStatementDescription retrieves description info for a named prepared statement.
func (h *Handler) GetStatementDescription(stmtName string) (*StatementDescription, error) {
	if h.server == nil {
		return nil, ErrNoConnection
	}

	// For the extended query protocol, statements are managed by psql-wire
	// The description is automatically handled when Parse creates the statement
	// with columns and parameters set via wire.WithColumns and wire.WithParameters

	return nil, nil // psql-wire handles this internally
}

// GetPortalDescription returns the description info for a bound portal.
// This is used by the wire protocol handler to respond to DESCRIBE Portal messages.
type PortalDescription struct {
	// Columns describes the result columns with format codes applied.
	// This is returned in the RowDescription message.
	Columns wire.Columns

	// FormatCodes contains the format codes for each result column.
	// 0 = text, 1 = binary
	FormatCodes []int16
}

// GetPortalDescription retrieves description info for a named portal.
func (h *Handler) GetPortalDescription(portalName string) (*PortalDescription, error) {
	if h.server == nil {
		return nil, ErrNoConnection
	}

	// For the extended query protocol, portals are managed by psql-wire
	// The description is automatically handled when Bind creates the portal

	return nil, nil // psql-wire handles this internally
}
