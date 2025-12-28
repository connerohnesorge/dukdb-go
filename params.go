package dukdb

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// placeholder represents a placeholder in a SQL query.
type placeholder struct {
	// start is the byte offset where the placeholder begins in the query.
	start int
	// end is the byte offset where the placeholder ends in the query.
	end int
	// name is the placeholder name (without $ or @).
	// For positional: "1", "2", etc.
	// For named: the actual name.
	name string
	// isPositional indicates whether this is a positional ($1) or named (@name) placeholder.
	isPositional bool
}

// positionalPlaceholderRegex matches $1, $2, etc. (1-based indexing).
var positionalPlaceholderRegex = regexp.MustCompile(
	`\$(\d+)`,
)

// namedPlaceholderRegex matches @name placeholders.
var namedPlaceholderRegex = regexp.MustCompile(
	`@([a-zA-Z_][a-zA-Z0-9_]*)`,
)

// extractPositionalPlaceholders extracts positional placeholders ($1, $2, etc.) from a query.
// It skips placeholders that appear inside string literals (single-quoted).
func extractPositionalPlaceholders(
	query string,
) []placeholder {
	var placeholders []placeholder

	// Find all potential matches
	matches := positionalPlaceholderRegex.FindAllStringSubmatchIndex(
		query,
		-1,
	)
	if matches == nil {
		return placeholders
	}

	for _, match := range matches {
		start := match[0]
		end := match[1]
		nameStart := match[2]
		nameEnd := match[3]

		// Check if this placeholder is inside a string literal
		if isInsideStringLiteral(query, start) {
			continue
		}

		name := query[nameStart:nameEnd]
		placeholders = append(
			placeholders,
			placeholder{
				start:        start,
				end:          end,
				name:         name,
				isPositional: true,
			},
		)
	}

	return placeholders
}

// extractNamedPlaceholders extracts named placeholders (@name) from a query.
// It skips placeholders that appear inside string literals (single-quoted).
func extractNamedPlaceholders(
	query string,
) []placeholder {
	var placeholders []placeholder

	// Find all potential matches
	matches := namedPlaceholderRegex.FindAllStringSubmatchIndex(
		query,
		-1,
	)
	if matches == nil {
		return placeholders
	}

	for _, match := range matches {
		start := match[0]
		end := match[1]
		nameStart := match[2]
		nameEnd := match[3]

		// Check if this placeholder is inside a string literal
		if isInsideStringLiteral(query, start) {
			continue
		}

		name := query[nameStart:nameEnd]
		placeholders = append(
			placeholders,
			placeholder{
				start:        start,
				end:          end,
				name:         name,
				isPositional: false,
			},
		)
	}

	return placeholders
}

// isInsideStringLiteral checks if the given position is inside a SQL string literal.
// SQL string literals are enclosed in single quotes, with ” representing an escaped quote.
func isInsideStringLiteral(
	query string,
	pos int,
) bool {
	inString := false
	i := 0

	for i < pos && i < len(query) {
		if query[i] == '\'' {
			// Check for escaped quote ('')
			if i+1 < len(query) &&
				query[i+1] == '\'' {
				// Skip the escaped quote
				i += 2
				continue
			}
			inString = !inString
		}
		i++
	}

	return inString
}

// BindParams binds the given arguments to the query placeholders and returns
// the query with all placeholders replaced by literal values.
// It supports both positional ($1, $2, etc. - 1-based) and named (@name) placeholders,
// but mixing the two styles is not allowed.
func BindParams(
	query string,
	args []driver.NamedValue,
) (string, error) {
	// Extract both types of placeholders
	positional := extractPositionalPlaceholders(
		query,
	)
	named := extractNamedPlaceholders(query)

	// Check for mixing placeholder styles
	if len(positional) > 0 && len(named) > 0 {
		return "", &Error{
			Type: ErrorTypeInvalidInput,
			Msg:  "cannot mix positional ($1) and named (@name) placeholders",
		}
	}

	// No placeholders - return query as-is
	if len(positional) == 0 && len(named) == 0 {
		return query, nil
	}

	// Handle positional placeholders
	if len(positional) > 0 {
		return bindPositionalParams(
			query,
			positional,
			args,
		)
	}

	// Handle named placeholders
	return bindNamedParams(query, named, args)
}

// bindPositionalParams binds positional placeholders ($1, $2, etc.) to arguments.
func bindPositionalParams(
	query string,
	placeholders []placeholder,
	args []driver.NamedValue,
) (string, error) {
	// Build a map from ordinal to formatted value
	valueMap := make(map[int]string)

	for _, arg := range args {
		formatted, err := FormatValue(arg.Value)
		if err != nil {
			return "", err
		}
		valueMap[arg.Ordinal] = formatted
	}

	// Sort placeholders by position in reverse order so we can replace from end to beginning
	// without invalidating positions
	sorted := make(
		[]placeholder,
		len(placeholders),
	)
	copy(sorted, placeholders)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].start > sorted[j].start
	})

	result := query
	for _, p := range sorted {
		ordinal, err := strconv.Atoi(p.name)
		if err != nil {
			return "", &Error{
				Type: ErrorTypeInvalidInput,
				Msg: fmt.Sprintf(
					"invalid positional placeholder: $%s",
					p.name,
				),
			}
		}

		val, ok := valueMap[ordinal]
		if !ok {
			return "", &Error{
				Type: ErrorTypeInvalidInput,
				Msg: fmt.Sprintf(
					"missing argument for placeholder $%d",
					ordinal,
				),
			}
		}

		result = result[:p.start] + val + result[p.end:]
	}

	return result, nil
}

// bindNamedParams binds named placeholders (@name) to arguments.
func bindNamedParams(
	query string,
	placeholders []placeholder,
	args []driver.NamedValue,
) (string, error) {
	// Build a map from name to formatted value
	valueMap := make(map[string]string)

	for _, arg := range args {
		if arg.Name == "" {
			continue
		}
		formatted, err := FormatValue(arg.Value)
		if err != nil {
			return "", err
		}
		valueMap[arg.Name] = formatted
	}

	// Sort placeholders by position in reverse order
	sorted := make(
		[]placeholder,
		len(placeholders),
	)
	copy(sorted, placeholders)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].start > sorted[j].start
	})

	result := query
	for _, p := range sorted {
		val, ok := valueMap[p.name]
		if !ok {
			return "", &Error{
				Type: ErrorTypeInvalidInput,
				Msg: fmt.Sprintf(
					"missing argument for placeholder @%s",
					p.name,
				),
			}
		}

		result = result[:p.start] + val + result[p.end:]
	}

	return result, nil
}

// FormatValue formats a Go value as a SQL literal string.
// This function handles all DuckDB-supported types.
func FormatValue(v any) (string, error) {
	if v == nil {
		return "NULL", nil
	}

	switch val := v.(type) {
	case nil:
		return "NULL", nil

	case bool:
		if val {
			return "TRUE", nil
		}
		return "FALSE", nil

	case int:
		return strconv.FormatInt(int64(val), 10), nil
	case int8:
		return strconv.FormatInt(int64(val), 10), nil
	case int16:
		return strconv.FormatInt(int64(val), 10), nil
	case int32:
		return strconv.FormatInt(int64(val), 10), nil
	case int64:
		return strconv.FormatInt(val, 10), nil

	case uint:
		return strconv.FormatUint(uint64(val), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(val), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(val), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(val), 10), nil
	case uint64:
		return strconv.FormatUint(val, 10), nil

	case float32:
		return formatFloat(float64(val))
	case float64:
		return formatFloat(val)

	case string:
		return formatString(val), nil

	case []byte:
		return formatBlob(val), nil

	case time.Time:
		return formatTimestamp(val), nil

	case UUID:
		return formatString(val.String()), nil
	case *UUID:
		if val == nil {
			return "NULL", nil
		}
		return formatString(val.String()), nil

	case Decimal:
		return val.String(), nil
	case *Decimal:
		if val == nil {
			return "NULL", nil
		}
		return val.String(), nil

	case Interval:
		return formatInterval(val), nil
	case *Interval:
		if val == nil {
			return "NULL", nil
		}
		return formatInterval(*val), nil

	case *big.Int:
		if val == nil {
			return "NULL", nil
		}
		return val.String(), nil

	case driver.Valuer:
		// Handle types that implement driver.Valuer
		dv, err := val.Value()
		if err != nil {
			return "", err
		}
		return FormatValue(dv)

	default:
		return "", &Error{
			Type: ErrorTypeInvalidType,
			Msg:  fmt.Sprintf("unsupported type %T", v),
		}
	}
}

// formatString formats a string value as a SQL string literal.
// It escapes single quotes by doubling them to prevent SQL injection.
func formatString(s string) string {
	// Escape single quotes by doubling them
	escaped := strings.ReplaceAll(s, "'", "''")
	return "'" + escaped + "'"
}

// formatBlob formats a byte slice as a SQL blob literal.
// Uses the X'...' hex format.
func formatBlob(b []byte) string {
	return "X'" + strings.ToUpper(
		hex.EncodeToString(b),
	) + "'"
}

// formatFloat formats a float64 as a SQL literal.
// Returns an error for Infinity and NaN values which are not supported.
func formatFloat(f float64) (string, error) {
	if math.IsInf(f, 0) {
		return "", &Error{
			Type: ErrorTypeInvalid,
			Msg:  "infinity not supported in SQL literals",
		}
	}
	if math.IsNaN(f) {
		return "", &Error{
			Type: ErrorTypeInvalid,
			Msg:  "NaN not supported in SQL literals",
		}
	}
	return strconv.FormatFloat(
		f,
		'g',
		-1,
		64,
	), nil
}

// formatTimestamp formats a time.Time as a SQL timestamp literal.
func formatTimestamp(t time.Time) string {
	// Use ISO 8601 format that DuckDB understands
	return "'" + t.UTC().
		Format("2006-01-02 15:04:05.999999") +
		"'"
}

// formatInterval formats an Interval as a SQL interval literal.
func formatInterval(i Interval) string {
	return fmt.Sprintf(
		"INTERVAL '%d months %d days %d microseconds'",
		i.Months,
		i.Days,
		i.Micros,
	)
}
