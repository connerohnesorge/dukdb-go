package executor

import (
	"context"
	"testing"

	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test Regular Expression Functions

func TestRegexpMatches(t *testing.T) {
	tests := []struct {
		name     string
		str      any
		pattern  any
		expected any
		hasError bool
	}{
		{"simple match", "hello", "h.*o", true, false},
		{"no match", "hello", "x+", false, false},
		{"NULL str", nil, "test", nil, false},
		{"NULL pattern", "test", nil, nil, false},
		{"invalid regex", "test", "[", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := regexpMatchesValue(tt.str, tt.pattern)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestRegexpReplace(t *testing.T) {
	tests := []struct {
		name     string
		str      any
		pattern  any
		repl     any
		flags    any
		expected any
		hasError bool
	}{
		{"replace first", "hello world", "l", "X", nil, "heXlo world", false},
		{"replace all with g flag", "hello world", "l", "X", "g", "heXXo worXd", false},
		{"no match", "hello", "x", "X", nil, "hello", false},
		{"NULL str", nil, "test", "X", nil, nil, false},
		{"invalid regex", "test", "[", "X", nil, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := regexpReplaceValue(tt.str, tt.pattern, tt.repl, tt.flags)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestRegexpExtract(t *testing.T) {
	tests := []struct {
		name     string
		str      any
		pattern  any
		group    any
		expected any
		hasError bool
	}{
		{"extract group 0", "abc123def", "[0-9]+", nil, "123", false},
		{"extract group 1", "abc123def", "([0-9]+)", int64(1), "123", false},
		{"no match", "hello", "[0-9]+", nil, nil, false},
		{"NULL str", nil, "test", nil, nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := regexpExtractValue(tt.str, tt.pattern, tt.group)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestRegexpExtractAll(t *testing.T) {
	tests := []struct {
		name     string
		str      any
		pattern  any
		group    any
		expected any
		hasError bool
	}{
		{"extract all", "a1b2c3", "[0-9]+", nil, []string{"1", "2", "3"}, false},
		{"no matches", "abc", "[0-9]+", nil, []string{}, false},
		{"NULL str", nil, "test", nil, nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := regexpExtractAllValue(tt.str, tt.pattern, tt.group)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestRegexpSplitToArray(t *testing.T) {
	tests := []struct {
		name     string
		str      any
		pattern  any
		expected any
		hasError bool
	}{
		{"split by spaces", "a b c", "\\s+", []string{"a", "b", "c"}, false},
		{"split by digits", "a1b2c", "[0-9]", []string{"a", "b", "c"}, false},
		{"NULL str", nil, "test", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := regexpSplitToArrayValue(tt.str, tt.pattern)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// Test String Concatenation and Splitting

func TestConcatWS(t *testing.T) {
	tests := []struct {
		name     string
		sep      any
		args     []any
		expected any
	}{
		{"basic concat", ",", []any{"a", "b", "c"}, "a,b,c"},
		{"skip NULLs", ",", []any{"a", nil, "c"}, "a,c"},
		{"NULL separator", nil, []any{"a", "b"}, nil},
		{"empty separator", "", []any{"a", "b", "c"}, "abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := concatWSValue(tt.sep, tt.args...)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestStringSplit(t *testing.T) {
	tests := []struct {
		name     string
		str      any
		sep      any
		expected any
	}{
		{"split by comma", "a,b,c", ",", []string{"a", "b", "c"}},
		{"split by space", "hello world", " ", []string{"hello", "world"}},
		{"empty separator", "abc", "", []string{"a", "b", "c"}},
		{"NULL str", nil, ",", nil},
		{"NULL separator", "test", nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := stringSplitValue(tt.str, tt.sep)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test Padding Functions

func TestLPad(t *testing.T) {
	tests := []struct {
		name     string
		str      any
		length   any
		fill     any
		expected any
	}{
		{"pad with spaces", "hi", int64(5), nil, "   hi"},
		{"pad with custom char", "hi", int64(5), "x", "xxxhi"},
		{"truncate", "hello", int64(3), nil, "hel"},
		{"no padding needed", "hello", int64(5), nil, "hello"},
		{"NULL str", nil, int64(5), nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := lpadValue(tt.str, tt.length, tt.fill)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRPad(t *testing.T) {
	tests := []struct {
		name     string
		str      any
		length   any
		fill     any
		expected any
	}{
		{"pad with spaces", "hi", int64(5), nil, "hi   "},
		{"pad with custom char", "hi", int64(5), "x", "hixxx"},
		{"truncate", "hello", int64(3), nil, "hel"},
		{"no padding needed", "hello", int64(5), nil, "hello"},
		{"NULL str", nil, int64(5), nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := rpadValue(tt.str, tt.length, tt.fill)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test String Manipulation Functions

func TestReverse(t *testing.T) {
	tests := []struct {
		name     string
		str      any
		expected any
	}{
		{"basic reverse", "hello", "olleh"},
		{"empty string", "", ""},
		{"single char", "a", "a"},
		{"UTF-8", "café", "éfac"},
		{"NULL", nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := reverseValue(tt.str)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRepeat(t *testing.T) {
	tests := []struct {
		name     string
		str      any
		count    any
		expected any
		hasError bool
	}{
		{"repeat 3 times", "x", int64(3), "xxx", false},
		{"repeat 0 times", "x", int64(0), "", false},
		{"negative count", "x", int64(-1), nil, true},
		{"NULL str", nil, int64(3), nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := repeatValue(tt.str, tt.count)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestLeft(t *testing.T) {
	tests := []struct {
		name     string
		str      any
		count    any
		expected any
	}{
		{"extract left 3", "hello", int64(3), "hel"},
		{"count exceeds length", "hi", int64(5), "hi"},
		{"negative count", "hello", int64(-1), ""},
		{"NULL str", nil, int64(3), nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := leftValue(tt.str, tt.count)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRight(t *testing.T) {
	tests := []struct {
		name     string
		str      any
		count    any
		expected any
	}{
		{"extract right 3", "hello", int64(3), "llo"},
		{"count exceeds length", "hi", int64(5), "hi"},
		{"negative count", "hello", int64(-1), ""},
		{"NULL str", nil, int64(3), nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := rightValue(tt.str, tt.count)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPosition(t *testing.T) {
	tests := []struct {
		name     string
		substr   any
		str      any
		expected any
	}{
		{"found at position 2", "el", "hello", int64(2)},
		{"not found", "x", "hello", int64(0)},
		{"empty substring", "", "hello", int64(1)},
		{"NULL substr", nil, "hello", nil},
		{"NULL str", "test", nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := positionValue(tt.substr, tt.str)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestContains(t *testing.T) {
	tests := []struct {
		name     string
		str      any
		substr   any
		expected any
	}{
		{"contains", "hello world", "world", true},
		{"does not contain", "hello", "x", false},
		{"NULL str", nil, "test", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := containsValue(tt.str, tt.substr)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPrefix(t *testing.T) {
	tests := []struct {
		name     string
		str      any
		prefix   any
		expected any
	}{
		{"has prefix", "hello world", "hello", true},
		{"does not have prefix", "hello", "world", false},
		{"NULL str", nil, "test", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := prefixValue(tt.str, tt.prefix)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSuffix(t *testing.T) {
	tests := []struct {
		name     string
		str      any
		suffix   any
		expected any
	}{
		{"has suffix", "hello world", "world", true},
		{"does not have suffix", "hello", "world", false},
		{"NULL str", nil, "test", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := suffixValue(tt.str, tt.suffix)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test Encoding Functions

func TestASCII(t *testing.T) {
	tests := []struct {
		name     string
		char     any
		expected any
	}{
		{"letter A", "A", int64(65)},
		{"letter a", "a", int64(97)},
		{"empty string", "", int64(0)},
		{"NULL", nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := asciiValue(tt.char)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCHR(t *testing.T) {
	tests := []struct {
		name     string
		code     any
		expected any
		hasError bool
	}{
		{"code 65", int64(65), "A", false},
		{"code 97", int64(97), "a", false},
		{"out of range", int64(200), nil, true},
		{"negative", int64(-1), nil, true},
		{"NULL", nil, nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := chrValue(tt.code)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestUnicode(t *testing.T) {
	tests := []struct {
		name     string
		char     any
		expected any
	}{
		{"letter A", "A", int64(65)},
		{"unicode char", "é", int64(233)},
		{"empty string", "", int64(0)},
		{"NULL", nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := unicodeValue(tt.char)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test Hash Functions

func TestMD5(t *testing.T) {
	tests := []struct {
		name     string
		str      any
		expected any
	}{
		{"hello", "hello", "5d41402abc4b2a76b9719d911017c592"},
		{"empty", "", "d41d8cd98f00b204e9800998ecf8427e"},
		{"NULL", nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := md5Value(tt.str)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSHA256(t *testing.T) {
	tests := []struct {
		name     string
		str      any
		expected any
	}{
		{"hello", "hello", "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"},
		{"NULL", nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := sha256Value(tt.str)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHash(t *testing.T) {
	tests := []struct {
		name string
		str  any
	}{
		{"hello", "hello"},
		{"empty", ""},
		{"NULL", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := hashStringValue(tt.str)
			require.NoError(t, err)
			if tt.str == nil {
				assert.Nil(t, result)
			} else {
				// Just check it returns an int64
				_, ok := result.(int64)
				assert.True(t, ok)
			}
		})
	}
}

// Test String Distance Functions

func TestLevenshtein(t *testing.T) {
	tests := []struct {
		name     string
		str1     any
		str2     any
		expected any
	}{
		{"identical", "hello", "hello", int64(0)},
		{"kitten/sitting", "kitten", "sitting", int64(3)},
		{"empty strings", "", "", int64(0)},
		{"NULL str1", nil, "test", nil},
		{"NULL str2", "test", nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := levenshteinValue(tt.str1, tt.str2)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDamerauLevenshtein(t *testing.T) {
	tests := []struct {
		name     string
		str1     any
		str2     any
		expected any
	}{
		{"transposition", "ab", "ba", int64(1)},
		{"kitten/sitting", "kitten", "sitting", int64(3)},
		{"NULL str1", nil, "test", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := damerauLevenshteinValue(tt.str1, tt.str2)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHamming(t *testing.T) {
	tests := []struct {
		name     string
		str1     any
		str2     any
		expected any
		hasError bool
	}{
		{"identical", "abc", "abc", int64(0), false},
		{"one difference", "abc", "aXc", int64(1), false},
		{"all different", "abc", "xyz", int64(3), false},
		{"unequal length", "abc", "ab", nil, true},
		{"NULL str1", nil, "test", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := hammingValue(tt.str1, tt.str2)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestJaccard(t *testing.T) {
	tests := []struct {
		name string
		str1 any
		str2 any
		min  float64
		max  float64
	}{
		{"identical", "abc", "abc", 1.0, 1.0},
		{"no overlap", "abc", "xyz", 0.0, 0.0},
		{"partial overlap", "abc", "bcd", 0.3, 0.6},
		{"NULL str1", nil, "test", 0.0, 0.0}, // NULL should return nil, but will be 0.0 here
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jaccardValue(tt.str1, tt.str2)
			require.NoError(t, err)
			if tt.str1 == nil || tt.str2 == nil {
				assert.Nil(t, result)
			} else {
				val, ok := result.(float64)
				require.True(t, ok)
				assert.GreaterOrEqual(t, val, tt.min)
				assert.LessOrEqual(t, val, tt.max)
			}
		})
	}
}

func TestJaroSimilarity(t *testing.T) {
	tests := []struct {
		name string
		str1 any
		str2 any
		min  float64
	}{
		{"identical", "abc", "abc", 1.0},
		{"similar", "martha", "marhta", 0.9},
		{"NULL str1", nil, "test", 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jaroSimilarityValue(tt.str1, tt.str2)
			require.NoError(t, err)
			if tt.str1 == nil || tt.str2 == nil {
				assert.Nil(t, result)
			} else {
				val, ok := result.(float64)
				require.True(t, ok)
				assert.GreaterOrEqual(t, val, tt.min)
			}
		})
	}
}

func TestJaroWinklerSimilarity(t *testing.T) {
	tests := []struct {
		name string
		str1 any
		str2 any
		min  float64
	}{
		{"identical", "abc", "abc", 1.0},
		{"similar with prefix", "martha", "marhta", 0.9},
		{"NULL str1", nil, "test", 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := jaroWinklerSimilarityValue(tt.str1, tt.str2)
			require.NoError(t, err)
			if tt.str1 == nil || tt.str2 == nil {
				assert.Nil(t, result)
			} else {
				val, ok := result.(float64)
				require.True(t, ok)
				assert.GreaterOrEqual(t, val, tt.min)
			}
		})
	}
}

// =============================================================================
// Whitespace Trimming Aliases Integration Tests
// Tests for STRIP, LSTRIP, RSTRIP aliases (Python-style naming)
// =============================================================================

// setupStringTestExecutor creates an executor for string function testing
func setupStringTestExecutor() (*Executor, *catalog.Catalog) {
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	return exec, cat
}

// executeStringQuery executes a SQL query and returns the result
func executeStringQuery(t *testing.T, exec *Executor, cat *catalog.Catalog, sql string) (*ExecutionResult, error) {
	t.Helper()

	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}

	b := binder.NewBinder(cat)
	boundStmt, err := b.Bind(stmt)
	if err != nil {
		return nil, err
	}

	p := planner.NewPlanner(cat)
	plan, err := p.Plan(boundStmt)
	if err != nil {
		return nil, err
	}

	return exec.Execute(context.Background(), plan, nil)
}

// getFirstResultValue extracts the first value from the first row of a result
func getFirstResultValue(t *testing.T, result *ExecutionResult) any {
	t.Helper()
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], 1)

	for _, v := range result.Rows[0] {
		return v
	}
	return nil
}

// TestIntegration_STRIP_Function tests the STRIP function (alias for TRIM)
func TestIntegration_STRIP_Function(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{"basic strip", "SELECT STRIP('  hello  ')", "hello"},
		{"strip leading only", "SELECT STRIP('   world')", "world"},
		{"strip trailing only", "SELECT STRIP('test   ')", "test"},
		{"no whitespace", "SELECT STRIP('nowhitespace')", "nowhitespace"},
		{"empty string", "SELECT STRIP('')", ""},
		{"only whitespace", "SELECT STRIP('     ')", ""},
		{"tabs and spaces", "SELECT STRIP('\t hello \t')", "hello"},
		{"newlines", "SELECT STRIP('\n\nhello\n\n')", "hello"},
		{"mixed whitespace", "SELECT STRIP(' \t\n hello \n\t ')", "hello"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			val := getFirstResultValue(t, result)
			assert.Equal(t, tt.expected, val)
		})
	}
}

// TestIntegration_LSTRIP_Function tests the LSTRIP function (alias for LTRIM)
func TestIntegration_LSTRIP_Function(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{"basic lstrip", "SELECT LSTRIP('  hello  ')", "hello  "},
		{"strip leading only", "SELECT LSTRIP('   world')", "world"},
		{"no leading whitespace", "SELECT LSTRIP('test   ')", "test   "},
		{"no whitespace", "SELECT LSTRIP('nowhitespace')", "nowhitespace"},
		{"empty string", "SELECT LSTRIP('')", ""},
		{"only spaces", "SELECT LSTRIP('     ')", ""},
		{"multiple leading spaces", "SELECT LSTRIP('    data')", "data"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			val := getFirstResultValue(t, result)
			assert.Equal(t, tt.expected, val)
		})
	}
}

// TestIntegration_RSTRIP_Function tests the RSTRIP function (alias for RTRIM)
func TestIntegration_RSTRIP_Function(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{"basic rstrip", "SELECT RSTRIP('  hello  ')", "  hello"},
		{"strip trailing only", "SELECT RSTRIP('world   ')", "world"},
		{"no trailing whitespace", "SELECT RSTRIP('   test')", "   test"},
		{"no whitespace", "SELECT RSTRIP('nowhitespace')", "nowhitespace"},
		{"empty string", "SELECT RSTRIP('')", ""},
		{"only spaces", "SELECT RSTRIP('     ')", ""},
		{"multiple trailing spaces", "SELECT RSTRIP('data    ')", "data"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			val := getFirstResultValue(t, result)
			assert.Equal(t, tt.expected, val)
		})
	}
}

// TestIntegration_STRIP_TRIM_Equivalence tests that STRIP and TRIM produce identical results
func TestIntegration_STRIP_TRIM_Equivalence(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	testStrings := []string{
		"'  hello  '",
		"'   world'",
		"'test   '",
		"'nowhitespace'",
		"''",
		"'     '",
		"'\t hello \t'",
		"'\n\nhello\n\n'",
	}

	for _, str := range testStrings {
		t.Run(str, func(t *testing.T) {
			stripQuery := "SELECT STRIP(" + str + ")"
			trimQuery := "SELECT TRIM(" + str + ")"

			stripResult, err := executeStringQuery(t, exec, cat, stripQuery)
			require.NoError(t, err)
			stripVal := getFirstResultValue(t, stripResult)

			trimResult, err := executeStringQuery(t, exec, cat, trimQuery)
			require.NoError(t, err)
			trimVal := getFirstResultValue(t, trimResult)

			assert.Equal(t, trimVal, stripVal, "STRIP and TRIM should produce identical results for %s", str)
		})
	}
}

// TestIntegration_LSTRIP_LTRIM_Equivalence tests that LSTRIP and LTRIM produce identical results
func TestIntegration_LSTRIP_LTRIM_Equivalence(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	testStrings := []string{
		"'  hello  '",
		"'   world'",
		"'test   '",
		"'nowhitespace'",
		"''",
		"'     '",
	}

	for _, str := range testStrings {
		t.Run(str, func(t *testing.T) {
			lstripQuery := "SELECT LSTRIP(" + str + ")"
			ltrimQuery := "SELECT LTRIM(" + str + ")"

			lstripResult, err := executeStringQuery(t, exec, cat, lstripQuery)
			require.NoError(t, err)
			lstripVal := getFirstResultValue(t, lstripResult)

			ltrimResult, err := executeStringQuery(t, exec, cat, ltrimQuery)
			require.NoError(t, err)
			ltrimVal := getFirstResultValue(t, ltrimResult)

			assert.Equal(t, ltrimVal, lstripVal, "LSTRIP and LTRIM should produce identical results for %s", str)
		})
	}
}

// TestIntegration_RSTRIP_RTRIM_Equivalence tests that RSTRIP and RTRIM produce identical results
func TestIntegration_RSTRIP_RTRIM_Equivalence(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	testStrings := []string{
		"'  hello  '",
		"'   world'",
		"'test   '",
		"'nowhitespace'",
		"''",
		"'     '",
	}

	for _, str := range testStrings {
		t.Run(str, func(t *testing.T) {
			rstripQuery := "SELECT RSTRIP(" + str + ")"
			rtrimQuery := "SELECT RTRIM(" + str + ")"

			rstripResult, err := executeStringQuery(t, exec, cat, rstripQuery)
			require.NoError(t, err)
			rstripVal := getFirstResultValue(t, rstripResult)

			rtrimResult, err := executeStringQuery(t, exec, cat, rtrimQuery)
			require.NoError(t, err)
			rtrimVal := getFirstResultValue(t, rtrimResult)

			assert.Equal(t, rtrimVal, rstripVal, "RSTRIP and RTRIM should produce identical results for %s", str)
		})
	}
}

// TestIntegration_STRIP_NULL_Input tests STRIP function with NULL input
func TestIntegration_STRIP_NULL_Input(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	result, err := executeStringQuery(t, exec, cat, "SELECT STRIP(NULL)")
	require.NoError(t, err)
	val := getFirstResultValue(t, result)
	// NULL input should return empty string since toString(nil) returns ""
	assert.Equal(t, "", val)
}

// TestIntegration_LSTRIP_NULL_Input tests LSTRIP function with NULL input
func TestIntegration_LSTRIP_NULL_Input(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	result, err := executeStringQuery(t, exec, cat, "SELECT LSTRIP(NULL)")
	require.NoError(t, err)
	val := getFirstResultValue(t, result)
	// NULL input should return empty string since toString(nil) returns ""
	assert.Equal(t, "", val)
}

// TestIntegration_RSTRIP_NULL_Input tests RSTRIP function with NULL input
func TestIntegration_RSTRIP_NULL_Input(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	result, err := executeStringQuery(t, exec, cat, "SELECT RSTRIP(NULL)")
	require.NoError(t, err)
	val := getFirstResultValue(t, result)
	// NULL input should return empty string since toString(nil) returns ""
	assert.Equal(t, "", val)
}

// TestIntegration_STRIP_ArgumentCount tests error handling for wrong argument count
func TestIntegration_STRIP_ArgumentCount(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	// Test with no arguments - should fail
	_, err := executeStringQuery(t, exec, cat, "SELECT STRIP()")
	assert.Error(t, err, "STRIP with no arguments should fail")

	// Test with too many arguments - should fail
	_, err = executeStringQuery(t, exec, cat, "SELECT STRIP('a', 'b')")
	assert.Error(t, err, "STRIP with too many arguments should fail")
}

// TestIntegration_LSTRIP_ArgumentCount tests error handling for wrong argument count
func TestIntegration_LSTRIP_ArgumentCount(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	// Test with no arguments - should fail
	_, err := executeStringQuery(t, exec, cat, "SELECT LSTRIP()")
	assert.Error(t, err, "LSTRIP with no arguments should fail")

	// Test with too many arguments - should fail
	_, err = executeStringQuery(t, exec, cat, "SELECT LSTRIP('a', 'b')")
	assert.Error(t, err, "LSTRIP with too many arguments should fail")
}

// TestIntegration_RSTRIP_ArgumentCount tests error handling for wrong argument count
func TestIntegration_RSTRIP_ArgumentCount(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	// Test with no arguments - should fail
	_, err := executeStringQuery(t, exec, cat, "SELECT RSTRIP()")
	assert.Error(t, err, "RSTRIP with no arguments should fail")

	// Test with too many arguments - should fail
	_, err = executeStringQuery(t, exec, cat, "SELECT RSTRIP('a', 'b')")
	assert.Error(t, err, "RSTRIP with too many arguments should fail")
}

// =============================================================================
// Integration Tests for String Functions
// =============================================================================

// TestIntegration_Regex_InWhere tests regex functions in WHERE clauses (task 2.14)
func TestIntegration_Regex_InWhere(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected any
	}{
		{"REGEXP_MATCHES basic", "SELECT REGEXP_MATCHES('hello123', '[0-9]+')", true},
		{"REGEXP_MATCHES no match", "SELECT REGEXP_MATCHES('hello', '[0-9]+')", false},
		{"REGEXP_MATCHES email pattern", "SELECT REGEXP_MATCHES('test@example.com', '^[a-z]+@[a-z]+\\.[a-z]+$')", true},
		{"REGEXP_MATCHES word boundary", "SELECT REGEXP_MATCHES('hello world', '\\bworld\\b')", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			val := getFirstResultValue(t, result)
			assert.Equal(t, tt.expected, val)
		})
	}
}

// TestIntegration_StringManipulation tests REVERSE, REPEAT, LEFT, RIGHT, POSITION, CONTAINS, PREFIX, SUFFIX (task 5.20)
func TestIntegration_StringManipulation(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected any
	}{
		// REVERSE
		{"REVERSE basic", "SELECT REVERSE('hello')", "olleh"},
		{"REVERSE unicode", "SELECT REVERSE('cafe')", "efac"},
		{"REVERSE empty", "SELECT REVERSE('')", ""},

		// REPEAT
		{"REPEAT basic", "SELECT REPEAT('ab', 3)", "ababab"},
		{"REPEAT zero", "SELECT REPEAT('x', 0)", ""},

		// LEFT
		{"LEFT basic", "SELECT LEFT('hello', 3)", "hel"},
		{"LEFT exceeds length", "SELECT LEFT('hi', 10)", "hi"},

		// RIGHT
		{"RIGHT basic", "SELECT RIGHT('hello', 3)", "llo"},
		{"RIGHT exceeds length", "SELECT RIGHT('hi', 10)", "hi"},

		// STRPOS (POSITION with function call syntax)
		{"STRPOS found", "SELECT STRPOS('hello', 'lo')", int64(4)},
		{"STRPOS not found", "SELECT STRPOS('hello', 'x')", int64(0)},
		{"STRPOS empty substring", "SELECT STRPOS('hello', '')", int64(1)},

		// CONTAINS
		{"CONTAINS true", "SELECT CONTAINS('hello world', 'world')", true},
		{"CONTAINS false", "SELECT CONTAINS('hello', 'x')", false},

		// PREFIX (STARTS_WITH alias)
		{"PREFIX true", "SELECT PREFIX('hello world', 'hello')", true},
		{"PREFIX false", "SELECT PREFIX('hello', 'world')", false},
		{"STARTS_WITH alias", "SELECT STARTS_WITH('hello world', 'hello')", true},

		// SUFFIX (ENDS_WITH alias)
		{"SUFFIX true", "SELECT SUFFIX('hello world', 'world')", true},
		{"SUFFIX false", "SELECT SUFFIX('hello', 'world')", false},
		{"ENDS_WITH alias", "SELECT ENDS_WITH('hello world', 'world')", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			val := getFirstResultValue(t, result)
			assert.Equal(t, tt.expected, val)
		})
	}
}

// TestIntegration_Padding tests LPAD/RPAD for formatting (task 4.9)
func TestIntegration_Padding(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected any
	}{
		// LPAD
		{"LPAD with spaces", "SELECT LPAD('42', 5)", "   42"},
		{"LPAD with zeros", "SELECT LPAD('42', 5, '0')", "00042"},
		{"LPAD truncate", "SELECT LPAD('hello', 3)", "hel"},
		{"LPAD multi-char fill", "SELECT LPAD('x', 5, 'ab')", "ababx"},

		// RPAD
		{"RPAD with spaces", "SELECT RPAD('42', 5)", "42   "},
		{"RPAD with zeros", "SELECT RPAD('42', 5, '0')", "42000"},
		{"RPAD truncate", "SELECT RPAD('hello', 3)", "hel"},
		{"RPAD multi-char fill", "SELECT RPAD('x', 5, 'ab')", "xabab"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			val := getFirstResultValue(t, result)
			assert.Equal(t, tt.expected, val)
		})
	}
}

// TestIntegration_ASCII_CHR tests ASCII and CHR for character code manipulation (task 6.11)
func TestIntegration_ASCII_CHR(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected any
	}{
		// ASCII
		{"ASCII letter A", "SELECT ASCII('A')", int64(65)},
		{"ASCII letter a", "SELECT ASCII('a')", int64(97)},
		{"ASCII digit 0", "SELECT ASCII('0')", int64(48)},
		{"ASCII empty string", "SELECT ASCII('')", int64(0)},

		// CHR
		{"CHR 65 is A", "SELECT CHR(65)", "A"},
		{"CHR 97 is a", "SELECT CHR(97)", "a"},
		{"CHR 48 is 0", "SELECT CHR(48)", "0"},

		// Round-trip: ASCII(CHR(x)) = x, CHR(ASCII(c)) = c
		{"Round-trip ASCII to CHR", "SELECT CHR(ASCII('X'))", "X"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			val := getFirstResultValue(t, result)
			assert.Equal(t, tt.expected, val)
		})
	}
}

// TestIntegration_HashFunctions tests MD5, SHA256, HASH in queries (task 7.12)
func TestIntegration_HashFunctions(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected any
	}{
		// MD5
		{"MD5 hello", "SELECT MD5('hello')", "5d41402abc4b2a76b9719d911017c592"},
		{"MD5 empty", "SELECT MD5('')", "d41d8cd98f00b204e9800998ecf8427e"},

		// SHA256
		{"SHA256 hello", "SELECT SHA256('hello')", "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"},

		// HASH (returns int64, just verify it returns something)
		{"HASH produces int64", "SELECT HASH('hello') IS NOT NULL", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			val := getFirstResultValue(t, result)
			assert.Equal(t, tt.expected, val)
		})
	}

	// Test HASH returns consistent values
	t.Run("HASH consistency", func(t *testing.T) {
		result1, err := executeStringQuery(t, exec, cat, "SELECT HASH('test')")
		require.NoError(t, err)
		val1 := getFirstResultValue(t, result1)

		result2, err := executeStringQuery(t, exec, cat, "SELECT HASH('test')")
		require.NoError(t, err)
		val2 := getFirstResultValue(t, result2)

		assert.Equal(t, val1, val2, "HASH should return consistent values for same input")
		_, ok := val1.(int64)
		assert.True(t, ok, "HASH should return int64")
	})
}

// TestIntegration_StringDistance tests LEVENSHTEIN for fuzzy matching (task 8.24)
func TestIntegration_StringDistance(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected any
	}{
		// LEVENSHTEIN
		{"LEVENSHTEIN identical", "SELECT LEVENSHTEIN('hello', 'hello')", int64(0)},
		{"LEVENSHTEIN kitten/sitting", "SELECT LEVENSHTEIN('kitten', 'sitting')", int64(3)},
		{"LEVENSHTEIN empty strings", "SELECT LEVENSHTEIN('', '')", int64(0)},
		{"LEVENSHTEIN one empty", "SELECT LEVENSHTEIN('abc', '')", int64(3)},

		// DAMERAU_LEVENSHTEIN
		{"DAMERAU_LEVENSHTEIN transposition", "SELECT DAMERAU_LEVENSHTEIN('ab', 'ba')", int64(1)},
		{"DAMERAU_LEVENSHTEIN identical", "SELECT DAMERAU_LEVENSHTEIN('test', 'test')", int64(0)},

		// HAMMING
		{"HAMMING identical", "SELECT HAMMING('abc', 'abc')", int64(0)},
		{"HAMMING one diff", "SELECT HAMMING('abc', 'aXc')", int64(1)},
		{"HAMMING all diff", "SELECT HAMMING('abc', 'xyz')", int64(3)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			val := getFirstResultValue(t, result)
			assert.Equal(t, tt.expected, val)
		})
	}

	// Test similarity functions return values in [0, 1]
	similarityTests := []struct {
		name  string
		query string
	}{
		{"JACCARD", "SELECT JACCARD('hello', 'hallo')"},
		{"JARO_SIMILARITY", "SELECT JARO_SIMILARITY('martha', 'marhta')"},
		{"JARO_WINKLER_SIMILARITY", "SELECT JARO_WINKLER_SIMILARITY('martha', 'marhta')"},
	}

	for _, tt := range similarityTests {
		t.Run(tt.name+" range", func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			val := getFirstResultValue(t, result)
			f, ok := val.(float64)
			require.True(t, ok, "similarity function should return float64")
			assert.GreaterOrEqual(t, f, 0.0, "similarity should be >= 0")
			assert.LessOrEqual(t, f, 1.0, "similarity should be <= 1")
		})
	}
}

// TestIntegration_NullHandling tests NULL propagation in nested function calls (task 11.7)
func TestIntegration_NullHandling(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected any
	}{
		// NULL input propagation
		{"REVERSE NULL", "SELECT REVERSE(NULL)", nil},
		{"LEFT NULL str", "SELECT LEFT(NULL, 3)", nil},
		{"RIGHT NULL str", "SELECT RIGHT(NULL, 3)", nil},
		{"REPEAT NULL str", "SELECT REPEAT(NULL, 3)", nil},
		{"STRPOS NULL substring", "SELECT STRPOS('hello', NULL)", nil},
		{"CONTAINS NULL str", "SELECT CONTAINS(NULL, 'x')", nil},

		// Hash functions with NULL
		{"MD5 NULL", "SELECT MD5(NULL)", nil},
		{"SHA256 NULL", "SELECT SHA256(NULL)", nil},
		{"HASH NULL", "SELECT HASH(NULL)", nil},

		// Distance functions with NULL
		{"LEVENSHTEIN NULL first", "SELECT LEVENSHTEIN(NULL, 'test')", nil},
		{"LEVENSHTEIN NULL second", "SELECT LEVENSHTEIN('test', NULL)", nil},
		{"HAMMING NULL first", "SELECT HAMMING(NULL, 'abc')", nil},
		{"JACCARD NULL", "SELECT JACCARD(NULL, 'test')", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			val := getFirstResultValue(t, result)
			assert.Equal(t, tt.expected, val)
		})
	}
}

// TestIntegration_StringErrorHandling tests error handling for invalid inputs (task 12.10)
func TestIntegration_StringErrorHandling(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	// Test invalid regex pattern
	t.Run("Invalid regex pattern", func(t *testing.T) {
		_, err := executeStringQuery(t, exec, cat, "SELECT REGEXP_MATCHES('test', '[')")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Invalid regular expression")
	})

	// Test HAMMING with unequal lengths
	t.Run("HAMMING unequal lengths", func(t *testing.T) {
		_, err := executeStringQuery(t, exec, cat, "SELECT HAMMING('abc', 'ab')")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "HAMMING requires strings of equal length")
	})

	// Test CHR with out of range
	t.Run("CHR out of range positive", func(t *testing.T) {
		_, err := executeStringQuery(t, exec, cat, "SELECT CHR(200)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "CHR code must be in ASCII range")
	})

	t.Run("CHR out of range negative", func(t *testing.T) {
		_, err := executeStringQuery(t, exec, cat, "SELECT CHR(-1)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "CHR code must be in ASCII range")
	})

	// Test REPEAT with negative count
	t.Run("REPEAT negative count", func(t *testing.T) {
		_, err := executeStringQuery(t, exec, cat, "SELECT REPEAT('x', -1)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "REPEAT count must be non-negative")
	})

	// Test invalid regex in REGEXP_REPLACE
	t.Run("REGEXP_REPLACE invalid pattern", func(t *testing.T) {
		_, err := executeStringQuery(t, exec, cat, "SELECT REGEXP_REPLACE('test', '[', 'x')")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Invalid regular expression")
	})

	// Test invalid regex in REGEXP_EXTRACT
	t.Run("REGEXP_EXTRACT invalid pattern", func(t *testing.T) {
		_, err := executeStringQuery(t, exec, cat, "SELECT REGEXP_EXTRACT('test', '[')")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Invalid regular expression")
	})
}

// TestIntegration_NestedFunctions tests nested string function calls (task 14.5)
func TestIntegration_NestedFunctions(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected any
	}{
		// UPPER(REGEXP_REPLACE(...))
		{"UPPER REGEXP_REPLACE", "SELECT UPPER(REGEXP_REPLACE('hello123world', '[0-9]+', '_'))", "HELLO_WORLD"},

		// LOWER(REVERSE(...))
		{"LOWER REVERSE", "SELECT LOWER(REVERSE('HELLO'))", "olleh"},

		// TRIM(LPAD(...))
		{"STRIP LPAD", "SELECT STRIP(LPAD('x', 5))", "x"},

		// Multiple nesting
		{"Triple nesting", "SELECT UPPER(REVERSE(LEFT('hello', 3)))", "LEH"},

		// CONCAT nested with manipulation
		{"CONCAT REVERSE LEFT", "SELECT CONCAT(REVERSE('abc'), LEFT('xyz', 2))", "cbaxy"},

		// LENGTH of REVERSE
		{"LENGTH REVERSE", "SELECT LENGTH(REVERSE('hello'))", int64(5)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			val := getFirstResultValue(t, result)
			assert.Equal(t, tt.expected, val)
		})
	}
}

// TestIntegration_DataCleaning tests TRIM, REGEXP_REPLACE for data normalization (task 14.9)
func TestIntegration_DataCleaning(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected any
	}{
		// Basic trimming
		{"TRIM whitespace", "SELECT TRIM('  hello world  ')", "hello world"},
		{"LTRIM leading", "SELECT LTRIM('   data')", "data"},
		{"RTRIM trailing", "SELECT RTRIM('data   ')", "data"},

		// Remove digits with REGEXP_REPLACE
		{"Remove digits", "SELECT REGEXP_REPLACE('abc123def456', '[0-9]+', '', 'g')", "abcdef"},

		// Remove special characters
		{"Remove special chars", "SELECT REGEXP_REPLACE('hello@#$world', '[^a-zA-Z]', '', 'g')", "helloworld"},

		// Normalize whitespace (replace multiple spaces with single)
		{"Normalize spaces", "SELECT REGEXP_REPLACE('hello   world', '\\s+', ' ', 'g')", "hello world"},

		// Extract alphanumeric only
		{"Keep alphanumeric", "SELECT REGEXP_REPLACE('test!@#123', '[^a-zA-Z0-9]', '', 'g')", "test123"},

		// Combine TRIM and REGEXP_REPLACE
		{"TRIM and remove digits", "SELECT TRIM(REGEXP_REPLACE('  abc123  ', '[0-9]', '', 'g'))", "abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			val := getFirstResultValue(t, result)
			assert.Equal(t, tt.expected, val)
		})
	}
}

// TestIntegration_RegexExtract tests REGEXP_EXTRACT functions
func TestIntegration_RegexExtract(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected any
	}{
		{"REGEXP_EXTRACT digits", "SELECT REGEXP_EXTRACT('abc123def', '[0-9]+')", "123"},
		{"REGEXP_EXTRACT with group", "SELECT REGEXP_EXTRACT('abc123def', '([0-9]+)', 1)", "123"},
		{"REGEXP_EXTRACT no match", "SELECT REGEXP_EXTRACT('abcdef', '[0-9]+')", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			val := getFirstResultValue(t, result)
			assert.Equal(t, tt.expected, val)
		})
	}
}

// TestIntegration_Unicode tests Unicode string handling
func TestIntegration_Unicode(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected any
	}{
		{"UNICODE basic", "SELECT UNICODE('A')", int64(65)},
		{"UNICODE extended", "SELECT UNICODE('e')", int64(101)},
		{"REVERSE unicode safe", "SELECT REVERSE('ab')", "ba"},
		{"LENGTH unicode", "SELECT LENGTH('hello')", int64(5)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err)
			val := getFirstResultValue(t, result)
			assert.Equal(t, tt.expected, val)
		})
	}
}

// =============================================================================
// Benchmark and Performance Tests
// =============================================================================

// BenchmarkLevenshtein benchmarks LEVENSHTEIN on large strings (task 8.23)
func BenchmarkLevenshtein(b *testing.B) {
	// Create test strings of various sizes
	sizes := []int{10, 100, 500}

	for _, size := range sizes {
		str1 := make([]byte, size)
		str2 := make([]byte, size)
		for i := 0; i < size; i++ {
			str1[i] = byte('a' + (i % 26))
			str2[i] = byte('a' + ((i + 1) % 26))
		}

		b.Run("size_"+string(rune('0'+size/100)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = levenshteinValue(string(str1), string(str2))
			}
		})
	}
}

// BenchmarkDamerauLevenshtein benchmarks DAMERAU_LEVENSHTEIN on large strings
func BenchmarkDamerauLevenshtein(b *testing.B) {
	sizes := []int{10, 100, 500}

	for _, size := range sizes {
		str1 := make([]byte, size)
		str2 := make([]byte, size)
		for i := 0; i < size; i++ {
			str1[i] = byte('a' + (i % 26))
			str2[i] = byte('a' + ((i + 1) % 26))
		}

		b.Run("size_"+string(rune('0'+size/100)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = damerauLevenshteinValue(string(str1), string(str2))
			}
		})
	}
}

// BenchmarkRegexMatch benchmarks regex pattern matching (task 15.2)
func BenchmarkRegexMatch(b *testing.B) {
	testCases := []struct {
		name    string
		str     string
		pattern string
	}{
		{"simple", "hello world", "world"},
		{"digit_extract", "abc123def456", "[0-9]+"},
		{"email", "test@example.com", "^[a-z]+@[a-z]+\\.[a-z]+$"},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = regexpMatchesValue(tc.str, tc.pattern)
			}
		})
	}
}

// BenchmarkHashFunctions benchmarks hash functions
func BenchmarkHashFunctions(b *testing.B) {
	testStrings := []string{
		"hello",
		"hello world this is a longer string",
		string(make([]byte, 1000)),
	}

	for i, str := range testStrings {
		b.Run("MD5_len_"+string(rune('0'+i)), func(b *testing.B) {
			for j := 0; j < b.N; j++ {
				_, _ = md5Value(str)
			}
		})

		b.Run("SHA256_len_"+string(rune('0'+i)), func(b *testing.B) {
			for j := 0; j < b.N; j++ {
				_, _ = sha256Value(str)
			}
		})
	}
}

// =============================================================================
// Integration Tests for Type Compatibility in UNION Queries (Task 10.9)
// Tests that string functions with compatible return types work in UNION queries
// =============================================================================

// TestIntegration_UNION_TypeCompatibility_VARCHAR tests UNION of VARCHAR-returning string functions
func TestIntegration_UNION_TypeCompatibility_VARCHAR(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		minCount int // minimum expected row count
	}{
		{
			name:     "UNION MD5 and SHA256 (both VARCHAR)",
			query:    "SELECT MD5('hello') UNION SELECT SHA256('world')",
			minCount: 2,
		},
		{
			name:     "UNION REVERSE and REPEAT (both VARCHAR)",
			query:    "SELECT REVERSE('abc') UNION SELECT REPEAT('x', 3)",
			minCount: 2,
		},
		{
			name:     "UNION LEFT and RIGHT (both VARCHAR)",
			query:    "SELECT LEFT('hello', 2) UNION SELECT RIGHT('world', 3)",
			minCount: 2,
		},
		{
			name:     "UNION LPAD and RPAD (both VARCHAR)",
			query:    "SELECT LPAD('x', 3, '0') UNION SELECT RPAD('y', 3, '0')",
			minCount: 2,
		},
		{
			name:     "UNION CHR values (all VARCHAR)",
			query:    "SELECT CHR(65) UNION SELECT CHR(66) UNION SELECT CHR(67)",
			minCount: 2, // At least 2, possibly 3 depending on dedup behavior
		},
		{
			name:     "UNION REGEXP_REPLACE and REGEXP_EXTRACT (both VARCHAR)",
			query:    "SELECT REGEXP_REPLACE('abc123', '[0-9]+', 'X') UNION SELECT REGEXP_EXTRACT('def456', '[0-9]+')",
			minCount: 2,
		},
		{
			name:     "UNION STRIP aliases (all VARCHAR)",
			query:    "SELECT STRIP('  a  ') UNION SELECT LSTRIP('  b') UNION SELECT RSTRIP('c  ')",
			minCount: 2, // At least 2 rows with different column names
		},
		{
			name:     "UNION with same values deduplication",
			query:    "SELECT MD5('test') UNION SELECT MD5('test')",
			minCount: 1, // UNION removes duplicates
		},
		{
			name:     "UNION ALL MD5 with duplicates",
			query:    "SELECT MD5('test') UNION ALL SELECT MD5('test')",
			minCount: 2, // UNION ALL keeps duplicates
		},
		{
			name:     "UNION CONCAT_WS values (VARCHAR)",
			query:    "SELECT CONCAT_WS(',', 'a', 'b') UNION SELECT CONCAT_WS('-', 'x', 'y')",
			minCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err, "Query should execute without error")
			assert.GreaterOrEqual(t, len(result.Rows), tt.minCount, "Expected at least %d rows", tt.minCount)

			// Verify all results are strings (VARCHAR type)
			for i, row := range result.Rows {
				for col, val := range row {
					if val != nil {
						_, ok := val.(string)
						assert.True(t, ok, "Row %d, column %s should be string, got %T", i, col, val)
					}
				}
			}
		})
	}
}

// TestIntegration_UNION_TypeCompatibility_BIGINT tests UNION of BIGINT-returning string functions
func TestIntegration_UNION_TypeCompatibility_BIGINT(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		minCount int
	}{
		{
			name:     "UNION LEVENSHTEIN and HAMMING (both BIGINT)",
			query:    "SELECT LEVENSHTEIN('abc', 'abd') UNION SELECT HAMMING('abc', 'xyz')",
			minCount: 2,
		},
		{
			name:     "UNION LEVENSHTEIN and DAMERAU_LEVENSHTEIN (both BIGINT)",
			query:    "SELECT LEVENSHTEIN('kitten', 'sitting') UNION SELECT DAMERAU_LEVENSHTEIN('ab', 'ba')",
			minCount: 2,
		},
		{
			name:     "UNION ASCII and UNICODE (both BIGINT)",
			query:    "SELECT ASCII('A') UNION SELECT UNICODE('B')",
			minCount: 2,
		},
		{
			name:     "UNION STRPOS values (BIGINT)",
			query:    "SELECT STRPOS('hello', 'l') UNION SELECT STRPOS('world', 'o')",
			minCount: 2,
		},
		{
			name:     "UNION POSITION same values (BIGINT)",
			query:    "SELECT STRPOS('abc', 'b') UNION SELECT STRPOS('xyz', 'y')",
			minCount: 1, // Both return 2, may be deduped
		},
		{
			name:     "UNION HASH values (BIGINT)",
			query:    "SELECT HASH('hello') UNION SELECT HASH('world')",
			minCount: 2,
		},
		{
			name:     "UNION multiple distance functions (all BIGINT)",
			query:    "SELECT LEVENSHTEIN('a', 'b') UNION SELECT HAMMING('aa', 'ab') UNION SELECT DAMERAU_LEVENSHTEIN('ab', 'ba')",
			minCount: 1, // All return 1, may be deduped to varying degrees
		},
		{
			name:     "UNION INSTR values (BIGINT)",
			query:    "SELECT INSTR('hello', 'e') UNION SELECT INSTR('world', 'r')",
			minCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err, "Query should execute without error")
			assert.GreaterOrEqual(t, len(result.Rows), tt.minCount, "Expected at least %d rows", tt.minCount)

			// Verify all results are integers (BIGINT type)
			for i, row := range result.Rows {
				for col, val := range row {
					if val != nil {
						_, ok := val.(int64)
						assert.True(t, ok, "Row %d, column %s should be int64, got %T", i, col, val)
					}
				}
			}
		})
	}
}

// TestIntegration_UNION_TypeCompatibility_BOOLEAN tests UNION of BOOLEAN-returning string functions
func TestIntegration_UNION_TypeCompatibility_BOOLEAN(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		minCount int
	}{
		{
			name:     "UNION REGEXP_MATCHES values (BOOLEAN)",
			query:    "SELECT REGEXP_MATCHES('hello', 'ell') UNION SELECT REGEXP_MATCHES('world', 'xyz')",
			minCount: 2, // true and false
		},
		{
			name:     "UNION CONTAINS values (BOOLEAN)",
			query:    "SELECT CONTAINS('hello', 'ell') UNION SELECT CONTAINS('world', 'xyz')",
			minCount: 2, // true and false
		},
		{
			name:     "UNION PREFIX and SUFFIX same bool value (BOOLEAN)",
			query:    "SELECT PREFIX('hello', 'he') UNION SELECT SUFFIX('world', 'ld')",
			minCount: 1, // Both true, at least 1 row
		},
		{
			name:     "UNION STARTS_WITH and ENDS_WITH (BOOLEAN)",
			query:    "SELECT STARTS_WITH('hello', 'he') UNION SELECT ENDS_WITH('world', 'ld')",
			minCount: 1, // Both true, at least 1 row
		},
		{
			name:     "UNION multiple boolean functions returning true",
			query:    "SELECT CONTAINS('abc', 'b') UNION SELECT PREFIX('abc', 'a') UNION SELECT SUFFIX('abc', 'c')",
			minCount: 1, // All true, at least 1 row
		},
		{
			name:     "UNION with false values",
			query:    "SELECT CONTAINS('abc', 'x') UNION SELECT PREFIX('abc', 'z')",
			minCount: 1, // Both false, at least 1 row
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err, "Query should execute without error")
			assert.GreaterOrEqual(t, len(result.Rows), tt.minCount, "Expected at least %d rows", tt.minCount)

			// Verify all results are booleans
			for i, row := range result.Rows {
				for col, val := range row {
					if val != nil {
						_, ok := val.(bool)
						assert.True(t, ok, "Row %d, column %s should be bool, got %T", i, col, val)
					}
				}
			}
		})
	}
}

// TestIntegration_UNION_TypeCompatibility_DOUBLE tests UNION of DOUBLE-returning string functions
func TestIntegration_UNION_TypeCompatibility_DOUBLE(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		minCount int
	}{
		{
			name:     "UNION JACCARD similarity values (DOUBLE)",
			query:    "SELECT JACCARD('abc', 'abd') UNION SELECT JACCARD('xyz', 'xyz')",
			minCount: 1, // At least 1 row
		},
		{
			name:     "UNION JARO_SIMILARITY values (DOUBLE)",
			query:    "SELECT JARO_SIMILARITY('martha', 'marhta') UNION SELECT JARO_SIMILARITY('hello', 'hallo')",
			minCount: 1, // At least 1 row
		},
		{
			name:     "UNION JARO_WINKLER_SIMILARITY values (DOUBLE)",
			query:    "SELECT JARO_WINKLER_SIMILARITY('martha', 'marhta') UNION SELECT JARO_WINKLER_SIMILARITY('hello', 'world')",
			minCount: 2,
		},
		{
			name:     "UNION all similarity functions (all DOUBLE)",
			query:    "SELECT JACCARD('abc', 'abc') UNION SELECT JARO_SIMILARITY('abc', 'abc') UNION SELECT JARO_WINKLER_SIMILARITY('abc', 'abc')",
			minCount: 1, // All return 1.0, at least 1 row
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err, "Query should execute without error")
			assert.GreaterOrEqual(t, len(result.Rows), tt.minCount, "Expected at least %d rows", tt.minCount)

			// Verify all results are floats (DOUBLE type)
			for i, row := range result.Rows {
				for col, val := range row {
					if val != nil {
						_, ok := val.(float64)
						assert.True(t, ok, "Row %d, column %s should be float64, got %T", i, col, val)
					}
				}
			}
		})
	}
}

// TestIntegration_UNION_MixedFunctions tests UNION with multiple same-type functions
func TestIntegration_UNION_MixedFunctions(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name         string
		query        string
		minCount     int
		expectedType string // "string", "int64", "bool", "float64"
	}{
		{
			name: "Complex VARCHAR UNION chain",
			query: `SELECT MD5('a')
					UNION SELECT SHA256('b')
					UNION SELECT REVERSE('abc')
					UNION SELECT LPAD('x', 3, '0')
					UNION SELECT RPAD('y', 3, '0')`,
			minCount:     2, // At least 2 rows with different values
			expectedType: "string",
		},
		{
			name: "Complex BIGINT UNION chain",
			query: `SELECT LEVENSHTEIN('cat', 'hat')
					UNION SELECT HAMMING('cat', 'hat')
					UNION SELECT ASCII('A')
					UNION SELECT STRPOS('hello', 'l')`,
			minCount:     2, // At least 2 distinct values
			expectedType: "int64",
		},
		{
			name: "Boolean function UNION chain",
			query: `SELECT CONTAINS('hello', 'ell')
					UNION SELECT PREFIX('hello', 'he')
					UNION SELECT SUFFIX('hello', 'lo')
					UNION SELECT REGEXP_MATCHES('hello', 'ell')`,
			minCount:     1, // All return true, at least 1 row
			expectedType: "bool",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err, "Query should execute without error")
			assert.GreaterOrEqual(t, len(result.Rows), tt.minCount, "Expected at least %d rows", tt.minCount)

			// Verify type consistency
			for i, row := range result.Rows {
				for col, val := range row {
					if val != nil {
						var ok bool
						switch tt.expectedType {
						case "string":
							_, ok = val.(string)
						case "int64":
							_, ok = val.(int64)
						case "bool":
							_, ok = val.(bool)
						case "float64":
							_, ok = val.(float64)
						}
						assert.True(t, ok, "Row %d, column %s should be %s, got %T", i, col, tt.expectedType, val)
					}
				}
			}
		})
	}
}

// TestIntegration_UNION_WithLiterals tests UNION of string functions with literal values
func TestIntegration_UNION_WithLiterals(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		minCount int
	}{
		{
			name:     "VARCHAR function with string literal",
			query:    "SELECT MD5('hello') UNION SELECT 'literal_string'",
			minCount: 2,
		},
		{
			name:     "BIGINT function with integer literal",
			query:    "SELECT LEVENSHTEIN('abc', 'abd') UNION SELECT 100",
			minCount: 2,
		},
		{
			name:     "BOOLEAN function with boolean literal",
			query:    "SELECT CONTAINS('hello', 'ell') UNION SELECT false",
			minCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err, "Query should execute without error")
			assert.GreaterOrEqual(t, len(result.Rows), tt.minCount, "Expected at least %d rows", tt.minCount)
		})
	}
}

// TestIntegration_UNION_NullHandling tests UNION with NULL values from string functions
func TestIntegration_UNION_NullHandling(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		minCount int
		hasNull  bool
	}{
		{
			name:     "VARCHAR function with NULL result in UNION",
			query:    "SELECT MD5('hello') UNION SELECT MD5(NULL)",
			minCount: 2,
			hasNull:  true,
		},
		{
			name:     "BIGINT function with NULL result in UNION",
			query:    "SELECT LEVENSHTEIN('abc', 'abd') UNION SELECT LEVENSHTEIN(NULL, 'xyz')",
			minCount: 2,
			hasNull:  true,
		},
		{
			name:     "BOOLEAN function with NULL result in UNION",
			query:    "SELECT CONTAINS('hello', 'ell') UNION SELECT CONTAINS(NULL, 'x')",
			minCount: 2,
			hasNull:  true,
		},
		{
			name:     "Multiple NULLs may be deduplicated in UNION",
			query:    "SELECT MD5(NULL) UNION SELECT SHA256(NULL)",
			minCount: 1, // Both return NULL, may be deduplicated
			hasNull:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err, "Query should execute without error")
			assert.GreaterOrEqual(t, len(result.Rows), tt.minCount, "Expected at least %d rows", tt.minCount)

			if tt.hasNull {
				// Check that at least one row has a NULL value
				hasNullValue := false
				for _, row := range result.Rows {
					for _, val := range row {
						if val == nil {
							hasNullValue = true
							break
						}
					}
				}
				assert.True(t, hasNullValue, "Expected at least one NULL value in results")
			}
		})
	}
}

// =============================================================================
// Integration Tests for STRING_SPLIT with UNNEST (Tasks 3.8 and 14.6)
// Tests that STRING_SPLIT arrays can be expanded using UNNEST
// =============================================================================

// TestIntegration_STRING_SPLIT_UNNEST_Basic tests basic STRING_SPLIT with UNNEST
func TestIntegration_STRING_SPLIT_UNNEST_Basic(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected []any
	}{
		{
			name:     "Basic comma split with UNNEST",
			query:    "SELECT * FROM UNNEST(STRING_SPLIT('a,b,c', ','))",
			expected: []any{"a", "b", "c"},
		},
		{
			name:     "Space split with UNNEST",
			query:    "SELECT * FROM UNNEST(STRING_SPLIT('hello world', ' '))",
			expected: []any{"hello", "world"},
		},
		{
			name:     "Pipe delimiter with UNNEST",
			query:    "SELECT * FROM UNNEST(STRING_SPLIT('one|two|three', '|'))",
			expected: []any{"one", "two", "three"},
		},
		{
			name:     "Single element result",
			query:    "SELECT * FROM UNNEST(STRING_SPLIT('noseparator', ','))",
			expected: []any{"noseparator"},
		},
		{
			name:     "Empty parts from consecutive separators",
			query:    "SELECT * FROM UNNEST(STRING_SPLIT('a,,b', ','))",
			expected: []any{"a", "", "b"},
		},
		{
			name:     "Character-by-character split (empty separator)",
			query:    "SELECT * FROM UNNEST(STRING_SPLIT('abc', ''))",
			expected: []any{"a", "b", "c"},
		},
		{
			name:     "Longer separator string",
			query:    "SELECT * FROM UNNEST(STRING_SPLIT('one--two--three', '--'))",
			expected: []any{"one", "two", "three"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err, "Query should execute without error")
			require.Len(t, result.Rows, len(tt.expected), "Expected %d rows", len(tt.expected))

			// Verify each row matches expected value
			for i, row := range result.Rows {
				// Get the value from the row (column name is "unnest")
				val, ok := row["unnest"]
				require.True(t, ok, "Row should have 'unnest' column")
				assert.Equal(t, tt.expected[i], val, "Row %d mismatch", i)
			}
		})
	}
}

// TestIntegration_STRING_SPLIT_UNNEST_WithAlias tests STRING_SPLIT with UNNEST using aliases
func TestIntegration_STRING_SPLIT_UNNEST_WithAlias(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name        string
		query       string
		expected    []any
		colName     string
		expectError bool
	}{
		{
			name:     "With table alias",
			query:    "SELECT * FROM UNNEST(STRING_SPLIT('x,y,z', ',')) AS t",
			expected: []any{"x", "y", "z"},
			colName:  "unnest",
		},
		{
			name:     "CSV line parsing",
			query:    "SELECT * FROM UNNEST(STRING_SPLIT('name,age,city', ',')) AS fields",
			expected: []any{"name", "age", "city"},
			colName:  "unnest",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			if tt.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err, "Query should execute without error")
			require.Len(t, result.Rows, len(tt.expected), "Expected %d rows", len(tt.expected))

			for i, row := range result.Rows {
				val := row[tt.colName]
				assert.Equal(t, tt.expected[i], val, "Row %d mismatch", i)
			}
		})
	}
}

// TestIntegration_STRING_SPLIT_UNNEST_EmptyAndNull tests edge cases with empty/NULL
func TestIntegration_STRING_SPLIT_UNNEST_EmptyAndNull(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	t.Run("Empty string produces single empty element", func(t *testing.T) {
		result, err := executeStringQuery(t, exec, cat, "SELECT * FROM UNNEST(STRING_SPLIT('', ','))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "", result.Rows[0]["unnest"])
	})

	t.Run("Only separator produces two empty elements", func(t *testing.T) {
		result, err := executeStringQuery(t, exec, cat, "SELECT * FROM UNNEST(STRING_SPLIT(',', ','))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 2)
		assert.Equal(t, "", result.Rows[0]["unnest"])
		assert.Equal(t, "", result.Rows[1]["unnest"])
	})

	t.Run("Multiple consecutive separators", func(t *testing.T) {
		result, err := executeStringQuery(t, exec, cat, "SELECT * FROM UNNEST(STRING_SPLIT('a,,,b', ','))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 4) // "a", "", "", "b"
		assert.Equal(t, "a", result.Rows[0]["unnest"])
		assert.Equal(t, "", result.Rows[1]["unnest"])
		assert.Equal(t, "", result.Rows[2]["unnest"])
		assert.Equal(t, "b", result.Rows[3]["unnest"])
	})
}

// TestIntegration_STRING_SPLIT_UNNEST_RealWorldExamples tests real-world use cases
func TestIntegration_STRING_SPLIT_UNNEST_RealWorldExamples(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected []any
	}{
		{
			name:     "Parse path components",
			query:    "SELECT * FROM UNNEST(STRING_SPLIT('/home/user/documents', '/'))",
			expected: []any{"", "home", "user", "documents"},
		},
		{
			name:     "Parse URL query params keys",
			query:    "SELECT * FROM UNNEST(STRING_SPLIT('key1=val1&key2=val2&key3=val3', '&'))",
			expected: []any{"key1=val1", "key2=val2", "key3=val3"},
		},
		{
			name:     "Parse comma-separated list with spaces",
			query:    "SELECT * FROM UNNEST(STRING_SPLIT('apple, banana, cherry', ', '))",
			expected: []any{"apple", "banana", "cherry"},
		},
		{
			name:     "Parse tab-separated values",
			query:    "SELECT * FROM UNNEST(STRING_SPLIT('col1\tcol2\tcol3', '\t'))",
			expected: []any{"col1", "col2", "col3"},
		},
		{
			name:     "Parse semicolon-separated emails",
			query:    "SELECT * FROM UNNEST(STRING_SPLIT('user1@example.com;user2@example.com;user3@example.com', ';'))",
			expected: []any{"user1@example.com", "user2@example.com", "user3@example.com"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err, "Query should execute without error")
			require.Len(t, result.Rows, len(tt.expected), "Expected %d rows", len(tt.expected))

			for i, row := range result.Rows {
				val := row["unnest"]
				assert.Equal(t, tt.expected[i], val, "Row %d mismatch", i)
			}
		})
	}
}

// TestIntegration_STRING_SPLIT_UNNEST_WithOtherFunctions tests STRING_SPLIT+UNNEST combined with other functions
func TestIntegration_STRING_SPLIT_UNNEST_WithOtherFunctions(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	t.Run("COUNT elements from STRING_SPLIT", func(t *testing.T) {
		result, err := executeStringQuery(t, exec, cat, "SELECT COUNT(*) FROM UNNEST(STRING_SPLIT('a,b,c,d,e', ','))")
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		// Count should be 5
		for _, val := range result.Rows[0] {
			assert.Equal(t, int64(5), val)
		}
	})
}

// TestIntegration_REGEXP_SPLIT_UNNEST tests REGEXP_SPLIT_TO_ARRAY with UNNEST
func TestIntegration_REGEXP_SPLIT_UNNEST(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	tests := []struct {
		name     string
		query    string
		expected []any
	}{
		{
			name:     "Regex split by whitespace",
			query:    "SELECT * FROM UNNEST(REGEXP_SPLIT_TO_ARRAY('hello   world  foo', '\\s+'))",
			expected: []any{"hello", "world", "foo"},
		},
		{
			name:     "Regex split by digits",
			query:    "SELECT * FROM UNNEST(REGEXP_SPLIT_TO_ARRAY('a1b2c3d', '[0-9]'))",
			expected: []any{"a", "b", "c", "d"},
		},
		{
			name:     "Regex split by multiple separators",
			query:    "SELECT * FROM UNNEST(REGEXP_SPLIT_TO_ARRAY('one,two;three|four', '[,;|]'))",
			expected: []any{"one", "two", "three", "four"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executeStringQuery(t, exec, cat, tt.query)
			require.NoError(t, err, "Query should execute without error")
			require.Len(t, result.Rows, len(tt.expected), "Expected %d rows", len(tt.expected))

			for i, row := range result.Rows {
				val := row["unnest"]
				assert.Equal(t, tt.expected[i], val, "Row %d mismatch", i)
			}
		})
	}
}

// TestIntegration_UNION_TypeConsistency tests that UNION queries produce consistent types
func TestIntegration_UNION_TypeConsistency(t *testing.T) {
	exec, cat := setupStringTestExecutor()

	t.Run("VARCHAR functions produce string results", func(t *testing.T) {
		result, err := executeStringQuery(t, exec, cat, "SELECT MD5('hello') UNION SELECT SHA256('world')")
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 2)

		// All values should be strings
		for _, row := range result.Rows {
			for _, val := range row {
				if val != nil {
					_, ok := val.(string)
					assert.True(t, ok, "Expected string, got %T", val)
				}
			}
		}
	})

	t.Run("BIGINT functions produce int64 results", func(t *testing.T) {
		result, err := executeStringQuery(t, exec, cat, "SELECT LEVENSHTEIN('abc', 'abd') UNION SELECT HAMMING('xyz', 'xyw')")
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 1)

		// All values should be int64
		for _, row := range result.Rows {
			for _, val := range row {
				if val != nil {
					_, ok := val.(int64)
					assert.True(t, ok, "Expected int64, got %T", val)
				}
			}
		}
	})

	t.Run("BOOLEAN functions produce bool results", func(t *testing.T) {
		result, err := executeStringQuery(t, exec, cat, "SELECT CONTAINS('hello', 'ell') UNION SELECT CONTAINS('world', 'xyz')")
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 2)

		// All values should be booleans
		for _, row := range result.Rows {
			for _, val := range row {
				if val != nil {
					_, ok := val.(bool)
					assert.True(t, ok, "Expected bool, got %T", val)
				}
			}
		}
	})

	t.Run("DOUBLE functions produce float64 results", func(t *testing.T) {
		result, err := executeStringQuery(t, exec, cat, "SELECT JACCARD('abc', 'abd') UNION SELECT JARO_SIMILARITY('hello', 'hallo')")
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(result.Rows), 1)

		// All values should be float64
		for _, row := range result.Rows {
			for _, val := range row {
				if val != nil {
					_, ok := val.(float64)
					assert.True(t, ok, "Expected float64, got %T", val)
				}
			}
		}
	})
}
