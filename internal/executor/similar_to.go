package executor

import (
	"fmt"
	"regexp"
	"strings"
)

// matchSimilarTo evaluates a SQL SIMILAR TO pattern match.
// It converts the SQL regex pattern to a Go regexp and matches against the value.
func matchSimilarTo(value, pattern string, escape rune) (any, error) {
	if value == "" && pattern == "" {
		return true, nil
	}

	goPattern, err := sqlRegexToGoRegex(pattern, escape)
	if err != nil {
		return nil, err
	}

	re, err := regexp.Compile(goPattern)
	if err != nil {
		return nil, fmt.Errorf("SIMILAR TO: invalid pattern: %v", err)
	}

	return re.MatchString(value), nil
}

// sqlRegexToGoRegex converts a SQL SIMILAR TO pattern to a Go regexp pattern.
// SQL SIMILAR TO patterns use:
//   - % for zero or more characters (like .* in regexp)
//   - _ for exactly one character (like . in regexp)
//   - | for alternation
//   - () for grouping
//   - [] for character classes (with ! for negation instead of ^)
//   - An escape character to escape special characters
func sqlRegexToGoRegex(pattern string, escape rune) (string, error) {
	var buf strings.Builder
	buf.WriteString("^")

	runes := []rune(pattern)
	for i := 0; i < len(runes); i++ {
		ch := runes[i]

		// Handle escape character
		if ch == escape {
			i++
			if i >= len(runes) {
				return "", fmt.Errorf("SIMILAR TO pattern ends with escape character")
			}
			buf.WriteString(regexp.QuoteMeta(string(runes[i])))
			continue
		}

		switch ch {
		case '%':
			buf.WriteString(".*")
		case '_':
			buf.WriteString(".")
		case '|':
			buf.WriteRune('|')
		case '(':
			buf.WriteRune('(')
		case ')':
			buf.WriteRune(')')
		case '[':
			buf.WriteRune('[')
			i++
			// SQL uses ! for negation in character classes, Go uses ^
			if i < len(runes) && runes[i] == '!' {
				buf.WriteRune('^')
				i++
			}
			for i < len(runes) && runes[i] != ']' {
				buf.WriteRune(runes[i])
				i++
			}
			if i < len(runes) {
				buf.WriteRune(']')
			}
		default:
			buf.WriteString(regexp.QuoteMeta(string(ch)))
		}
	}

	buf.WriteString("$")
	return buf.String(), nil
}
