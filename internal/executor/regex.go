package executor

import (
	"fmt"
	"regexp"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
)

// Regular Expression Functions
// DuckDB uses RE2 regex engine. Go's regexp package implements RE2.
// This provides compatibility with DuckDB's regex behavior.

func regexpMatchesValue(str any, pattern any) (any, error) {
	// NULL check FIRST before any processing
	if str == nil || pattern == nil {
		return nil, nil
	}

	s := toString(str)
	p := toString(pattern)

	re, err := regexp.Compile(p)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("Invalid regular expression: %s", err.Error()),
		}
	}

	return re.MatchString(s), nil
}

func regexpReplaceValue(str any, pattern any, replacement any, flags any) (any, error) {
	// NULL check FIRST before any processing
	if str == nil || pattern == nil || replacement == nil {
		return nil, nil
	}

	s := toString(str)
	p := toString(pattern)
	r := toString(replacement)
	f := ""
	if flags != nil {
		f = toString(flags)
	}

	re, err := regexp.Compile(p)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("Invalid regular expression: %s", err.Error()),
		}
	}

	// Handle flags
	if strings.Contains(f, "g") {
		// Global replacement (all matches)
		return re.ReplaceAllString(s, r), nil
	}

	// Default: Replace first match only
	match := re.FindStringIndex(s)
	if match == nil {
		return s, nil
	}

	return s[:match[0]] + re.ReplaceAllString(s[match[0]:match[1]], r) + s[match[1]:], nil
}

func regexpExtractValue(str any, pattern any, group any) (any, error) {
	// NULL check FIRST before any processing
	if str == nil || pattern == nil {
		return nil, nil
	}

	s := toString(str)
	p := toString(pattern)
	g := int64(0)
	if group != nil {
		g = toInt64Value(group)
	}

	re, err := regexp.Compile(p)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("Invalid regular expression: %s", err.Error()),
		}
	}

	matches := re.FindStringSubmatch(s)
	if matches == nil || int(g) >= len(matches) {
		return nil, nil // Return NULL if no match or group doesn't exist
	}

	return matches[g], nil
}

func regexpExtractAllValue(str any, pattern any, group any) (any, error) {
	// NULL check FIRST before any processing
	if str == nil || pattern == nil {
		return nil, nil
	}

	s := toString(str)
	p := toString(pattern)
	g := int64(0)
	if group != nil {
		g = toInt64Value(group)
	}

	re, err := regexp.Compile(p)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("Invalid regular expression: %s", err.Error()),
		}
	}

	allMatches := re.FindAllStringSubmatch(s, -1)
	if allMatches == nil {
		return []string{}, nil // Return empty array
	}

	result := make([]string, 0, len(allMatches))
	for _, matches := range allMatches {
		if int(g) < len(matches) {
			result = append(result, matches[g])
		}
	}

	return result, nil
}

func regexpSplitToArrayValue(str any, pattern any) (any, error) {
	// NULL check FIRST before any processing
	if str == nil || pattern == nil {
		return nil, nil
	}

	s := toString(str)
	p := toString(pattern)

	re, err := regexp.Compile(p)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("Invalid regular expression: %s", err.Error()),
		}
	}

	return re.Split(s, -1), nil
}
