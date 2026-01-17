package executor

import (
	"strings"
	"unicode/utf8"

	dukdb "github.com/dukdb/dukdb-go"
)

// Concatenation and Splitting

func concatWSValue(separator any, args ...any) (any, error) {
	// NULL separator returns NULL
	if separator == nil {
		return nil, nil
	}

	sep := toString(separator)
	parts := make([]string, 0, len(args))

	// CONCAT_WS skips NULL values in concatenation (exception to NULL propagation rule)
	for _, arg := range args {
		if arg == nil {
			continue // Skip NULL values, don't propagate
		}
		parts = append(parts, toString(arg))
	}

	return strings.Join(parts, sep), nil
}

// STRING_SPLIT uses LITERAL separator (not regex pattern).
// For regex-based splitting, use REGEXP_SPLIT_TO_ARRAY.
func stringSplitValue(str any, separator any) (any, error) {
	// NULL check FIRST before any processing
	if str == nil || separator == nil {
		return nil, nil
	}

	s := toString(str)
	sep := toString(separator)

	if sep == "" {
		// Split into individual characters (use runes for UTF-8 safety)
		chars := make([]string, 0, utf8.RuneCountInString(s))
		for _, ch := range s {
			chars = append(chars, string(ch))
		}
		return chars, nil
	}

	return strings.Split(s, sep), nil
}

// Padding Functions

func lpadValue(str any, length any, fill any) (any, error) {
	// NULL check FIRST before any processing
	if str == nil || length == nil {
		return nil, nil
	}

	s := toString(str)
	targetLen := int(toInt64Value(length))
	fillStr := " "
	if fill != nil {
		fillStr = toString(fill)
	}

	// Use rune slices for UTF-8 safety
	runes := []rune(s)
	currentLen := len(runes)

	if currentLen >= targetLen {
		// Truncate to target length
		return string(runes[:targetLen]), nil
	}

	padLen := targetLen - currentLen
	if fillStr == "" {
		return s, nil
	}

	// Repeat fill string to cover padding length
	fillRunes := []rune(fillStr)
	fillLen := len(fillRunes)
	padding := make([]rune, 0, padLen)
	for len(padding) < padLen {
		if len(padding)+fillLen <= padLen {
			padding = append(padding, fillRunes...)
		} else {
			padding = append(padding, fillRunes[:padLen-len(padding)]...)
		}
	}

	return string(padding) + s, nil
}

func rpadValue(str any, length any, fill any) (any, error) {
	// NULL check FIRST before any processing
	if str == nil || length == nil {
		return nil, nil
	}

	s := toString(str)
	targetLen := int(toInt64Value(length))
	fillStr := " "
	if fill != nil {
		fillStr = toString(fill)
	}

	// Use rune slices for UTF-8 safety
	runes := []rune(s)
	currentLen := len(runes)

	if currentLen >= targetLen {
		// Truncate to target length
		return string(runes[:targetLen]), nil
	}

	padLen := targetLen - currentLen
	if fillStr == "" {
		return s, nil
	}

	// Repeat fill string to cover padding length
	fillRunes := []rune(fillStr)
	fillLen := len(fillRunes)
	padding := make([]rune, 0, padLen)
	for len(padding) < padLen {
		if len(padding)+fillLen <= padLen {
			padding = append(padding, fillRunes...)
		} else {
			padding = append(padding, fillRunes[:padLen-len(padding)]...)
		}
	}

	return s + string(padding), nil
}

// String Manipulation

func reverseValue(str any) (any, error) {
	// NULL check FIRST before any processing
	if str == nil {
		return nil, nil
	}

	s := toString(str)
	// Use rune slices for UTF-8 safety
	runes := []rune(s)

	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}

	return string(runes), nil
}

func repeatValue(str any, count any) (any, error) {
	// NULL check FIRST before any processing
	if str == nil || count == nil {
		return nil, nil
	}

	s := toString(str)
	n := int(toInt64Value(count))

	if n < 0 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "REPEAT count must be non-negative",
		}
	}

	return strings.Repeat(s, n), nil
}

func leftValue(str any, count any) (any, error) {
	// NULL check FIRST before any processing
	if str == nil || count == nil {
		return nil, nil
	}

	s := toString(str)
	n := int(toInt64Value(count))

	// Use rune slices for UTF-8 safety
	runes := []rune(s)

	if n < 0 {
		return "", nil
	}
	if n >= len(runes) {
		return s, nil
	}

	return string(runes[:n]), nil
}

func rightValue(str any, count any) (any, error) {
	// NULL check FIRST before any processing
	if str == nil || count == nil {
		return nil, nil
	}

	s := toString(str)
	n := int(toInt64Value(count))

	// Use rune slices for UTF-8 safety
	runes := []rune(s)

	if n < 0 {
		return "", nil
	}
	if n >= len(runes) {
		return s, nil
	}

	return string(runes[len(runes)-n:]), nil
}

// Note: SQL syntax is POSITION(substring IN string), but Go implementation
// takes parameters as positionValue(substring, str) for consistency with other functions.
func positionValue(substring any, str any) (any, error) {
	// NULL check FIRST before any processing
	if substring == nil || str == nil {
		return nil, nil
	}

	sub := toString(substring)
	s := toString(str)

	// Find position using rune-based indexing
	idx := strings.Index(s, sub)
	if idx == -1 {
		return int64(0), nil // SQL uses 1-based indexing, 0 means not found
	}

	// Convert byte index to rune index (character position)
	runePos := utf8.RuneCountInString(s[:idx])
	return int64(runePos + 1), nil // Convert to 1-based
}

func containsValue(str any, substring any) (any, error) {
	// NULL check FIRST before any processing
	if str == nil || substring == nil {
		return nil, nil
	}

	s := toString(str)
	sub := toString(substring)

	return strings.Contains(s, sub), nil
}

func prefixValue(str any, prefix any) (any, error) {
	// NULL check FIRST before any processing
	if str == nil || prefix == nil {
		return nil, nil
	}

	s := toString(str)
	pre := toString(prefix)

	return strings.HasPrefix(s, pre), nil
}

func suffixValue(str any, suffix any) (any, error) {
	// NULL check FIRST before any processing
	if str == nil || suffix == nil {
		return nil, nil
	}

	s := toString(str)
	suf := toString(suffix)

	return strings.HasSuffix(s, suf), nil
}

// Encoding Functions

func asciiValue(char any) (any, error) {
	// NULL check FIRST before any processing
	if char == nil {
		return nil, nil
	}

	s := toString(char)
	if len(s) == 0 {
		return int64(0), nil
	}

	return int64(s[0]), nil
}

func chrValue(code any) (any, error) {
	// NULL check FIRST before any processing
	if code == nil {
		return nil, nil
	}

	c := toInt64Value(code)

	if c < 0 || c > 127 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "CHR code must be in ASCII range [0, 127]",
		}
	}

	return string(rune(c)), nil
}

func unicodeValue(char any) (any, error) {
	// NULL check FIRST before any processing
	if char == nil {
		return nil, nil
	}

	s := toString(char)
	if len(s) == 0 {
		return int64(0), nil
	}

	// Get first rune (use runes for Unicode support)
	runes := []rune(s)
	return int64(runes[0]), nil
}
