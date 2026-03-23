package dukdb

import (
	"strings"
)

// detectStatementType detects the type of a SQL statement using simple keyword extraction.
// This is a lightweight function that extracts the first keyword (after stripping
// comments and whitespace) and maps it to a StmtType.
//
// This function handles:
//   - Leading whitespace
//   - Single-line comments (-- and //)
//   - Multi-line comments (/* */)
//   - Case-insensitive keyword matching
func detectStatementType(sql string) StmtType {
	keyword := extractFirstKeyword(sql)
	if keyword == "" {
		return STATEMENT_TYPE_INVALID
	}

	return keywordToStmtType(keyword)
}

// extractFirstKeyword extracts the first SQL keyword from the query,
// skipping leading comments and whitespace.
func extractFirstKeyword(sql string) string {
	i := 0
	n := len(sql)

	for i < n {
		// Skip whitespace
		if isWhitespaceChar(sql[i]) {
			i++

			continue
		}

		// Skip single-line comments (-- ...)
		if i+1 < n && sql[i] == '-' &&
			sql[i+1] == '-' {
			i += 2
			for i < n && sql[i] != '\n' {
				i++
			}

			continue
		}

		// Skip C-style single-line comments (// ...)
		if i+1 < n && sql[i] == '/' &&
			sql[i+1] == '/' {
			i += 2
			for i < n && sql[i] != '\n' {
				i++
			}

			continue
		}

		// Skip multi-line comments (/* ... */)
		if i+1 < n && sql[i] == '/' &&
			sql[i+1] == '*' {
			i += 2
			for i+1 < n {
				if sql[i] == '*' &&
					sql[i+1] == '/' {
					i += 2

					break
				}
				i++
			}

			continue
		}

		// Found start of keyword
		break
	}

	if i >= n {
		return ""
	}

	// Extract the keyword (contiguous alphanumeric characters)
	start := i
	for i < n && isAlphaNumericChar(sql[i]) {
		i++
	}

	return strings.ToUpper(sql[start:i])
}

// keywordToStmtType maps a SQL keyword to a StmtType.
func keywordToStmtType(keyword string) StmtType {
	switch keyword {
	case "SELECT", "WITH":
		return STATEMENT_TYPE_SELECT
	case "INSERT":
		return STATEMENT_TYPE_INSERT
	case "UPDATE":
		return STATEMENT_TYPE_UPDATE
	case "DELETE", "TRUNCATE":
		return STATEMENT_TYPE_DELETE
	case "CREATE":
		return STATEMENT_TYPE_CREATE
	case "DROP":
		return STATEMENT_TYPE_DROP
	case "ALTER":
		return STATEMENT_TYPE_ALTER
	case "EXPLAIN":
		return STATEMENT_TYPE_EXPLAIN
	case "PREPARE":
		return STATEMENT_TYPE_PREPARE
	case "EXECUTE":
		return STATEMENT_TYPE_EXECUTE
	case "BEGIN",
		"START",
		"COMMIT",
		"ROLLBACK",
		"END":
		return STATEMENT_TYPE_TRANSACTION
	case "COPY":
		return STATEMENT_TYPE_COPY
	case "ANALYZE":
		return STATEMENT_TYPE_ANALYZE
	case "SET":
		return STATEMENT_TYPE_SET
	case "PRAGMA":
		return STATEMENT_TYPE_PRAGMA
	case "VACUUM":
		return STATEMENT_TYPE_VACUUM
	case "CALL":
		return STATEMENT_TYPE_CALL
	case "LOAD":
		return STATEMENT_TYPE_LOAD
	case "EXPORT":
		return STATEMENT_TYPE_EXPORT
	case "IMPORT":
		return STATEMENT_TYPE_COPY_DATABASE
	case "ATTACH":
		return STATEMENT_TYPE_ATTACH
	case "DETACH":
		return STATEMENT_TYPE_DETACH
	case "MERGE":
		return STATEMENT_TYPE_MERGE_INTO
	default:
		return STATEMENT_TYPE_INVALID
	}
}

// isWhitespaceChar returns true if c is a whitespace character.
func isWhitespaceChar(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' ||
		c == '\r'
}

// isAlphaNumericChar returns true if c is an alphanumeric character or underscore.
func isAlphaNumericChar(c byte) bool {
	return (c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') ||
		c == '_'
}
