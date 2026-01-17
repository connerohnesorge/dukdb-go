package filesystem

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContainsGlobPattern(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		// Patterns with wildcards
		{"asterisk", "*.csv", true},
		{"double asterisk", "**/*.csv", true},
		{"question mark", "file?.txt", true},
		{"bracket", "file[0-9].txt", true},
		{"multiple wildcards", "data/**/*.parquet", true},
		{"asterisk in middle", "data/*.csv", true},

		// Patterns without wildcards
		{"literal path", "data/file.csv", false},
		{"empty string", "", false},
		{"simple filename", "file.txt", false},
		{"path with dots", "data.2024/file.csv", false},

		// Escaped wildcards (should not count as glob patterns)
		{"escaped asterisk", `file\*.txt`, false},
		{"escaped question", `file\?.txt`, false},
		{"escaped bracket", `file\[0\].txt`, false},
		{"mixed escaped", `data/file\*.csv`, false},

		// Partial escapes
		{"partial escape asterisk", `file\*data*.csv`, true},
		{"bracket after escape", `data\*/[abc].txt`, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ContainsGlobPattern(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValidateGlobPattern(t *testing.T) {
	tests := []struct {
		name        string
		pattern     string
		expectError bool
		errorType   error
	}{
		// Valid patterns
		{"simple asterisk", "*.csv", false, nil},
		{"single double asterisk", "data/**/*.csv", false, nil},
		{"question mark", "file?.txt", false, nil},
		{"bracket", "file[0-9].txt", false, nil},
		{"negated bracket", "file[!abc].txt", false, nil},
		{"range bracket", "file[a-z].txt", false, nil},
		{"complex pattern", "data/**/year=*/month=*/*.parquet", false, nil},
		{"literal path", "data/file.csv", false, nil},

		// Invalid patterns
		{"empty pattern", "", true, ErrInvalidGlobPattern},
		{"multiple double asterisk", "data/**/**/file.csv", true, ErrMultipleRecursiveWildcards},
		{"unclosed bracket", "file[abc.txt", true, ErrInvalidGlobPattern},
		{"unclosed bracket end", "data/[", true, ErrInvalidGlobPattern},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateGlobPattern(tt.pattern)
			if tt.expectError {
				assert.Error(t, err)
				if tt.errorType != nil {
					assert.ErrorIs(t, err, tt.errorType)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestParsePatternSegments(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		expected []PatternSegment
	}{
		{
			name:    "simple path",
			pattern: "data/file.csv",
			expected: []PatternSegment{
				{Type: SegmentLiteral, Value: "data"},
				{Type: SegmentLiteral, Value: "file.csv"},
			},
		},
		{
			name:    "asterisk wildcard",
			pattern: "data/*.csv",
			expected: []PatternSegment{
				{Type: SegmentLiteral, Value: "data"},
				{Type: SegmentWildcard, Value: "*.csv"},
			},
		},
		{
			name:    "double asterisk",
			pattern: "data/**/*.csv",
			expected: []PatternSegment{
				{Type: SegmentLiteral, Value: "data"},
				{Type: SegmentRecursive, Value: "**"},
				{Type: SegmentWildcard, Value: "*.csv"},
			},
		},
		{
			name:    "question mark",
			pattern: "data/file?.txt",
			expected: []PatternSegment{
				{Type: SegmentLiteral, Value: "data"},
				{Type: SegmentWildcard, Value: "file?.txt"},
			},
		},
		{
			name:    "bracket",
			pattern: "data/file[0-9].txt",
			expected: []PatternSegment{
				{Type: SegmentLiteral, Value: "data"},
				{Type: SegmentWildcard, Value: "file[0-9].txt"},
			},
		},
		{
			name:    "complex hive pattern",
			pattern: "data/**/year=*/month=*/*.parquet",
			expected: []PatternSegment{
				{Type: SegmentLiteral, Value: "data"},
				{Type: SegmentRecursive, Value: "**"},
				{Type: SegmentWildcard, Value: "year=*"},
				{Type: SegmentWildcard, Value: "month=*"},
				{Type: SegmentWildcard, Value: "*.parquet"},
			},
		},
		{
			name:    "leading slash",
			pattern: "/data/file.csv",
			expected: []PatternSegment{
				{Type: SegmentLiteral, Value: "/data"},
				{Type: SegmentLiteral, Value: "file.csv"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParsePatternSegments(tt.pattern)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractPrefix(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		expected string
	}{
		{"simple asterisk", "data/*.csv", "data/"},
		{"nested asterisk", "data/2024/01/*.csv", "data/2024/01/"},
		{"double asterisk", "data/**/*.csv", "data/"},
		{"question mark", "data/file?.csv", "data/"},
		{"bracket", "data/[0-9]/*.csv", "data/"},
		{"asterisk at start", "*.csv", ""},
		{"double asterisk at start", "**/*.csv", ""},
		{"no wildcard", "data/file.csv", "data/file.csv/"},
		{"empty pattern", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractPrefix(tt.pattern)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMatchGlob(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		input    string
		expected bool
	}{
		// Asterisk tests
		{"asterisk matches any", "*.csv", "data.csv", true},
		{"asterisk matches empty", "*.csv", ".csv", true},
		{"asterisk matches long", "*.csv", "very_long_file_name.csv", true},
		{"asterisk no match", "*.csv", "data.txt", false},
		{"asterisk in middle", "data*.csv", "data123.csv", true},
		{"asterisk prefix", "data*", "data", true},
		{"asterisk prefix match", "data*", "data123", true},
		{"asterisk prefix no match", "data*", "dat", false},
		{"multiple asterisks", "*data*", "mydata123", true},
		{"multiple asterisks 2", "*data*", "data", true},
		{"multiple asterisks no match", "*data*", "dta", false},

		// Question mark tests
		{"question matches one", "file?.txt", "file1.txt", true},
		{"question matches letter", "file?.txt", "filea.txt", true},
		{"question no match", "file?.txt", "file12.txt", false},
		{"question no match short", "file?.txt", "file.txt", false},
		{"multiple questions", "file??.txt", "file12.txt", true},
		{"multiple questions no match", "file??.txt", "file1.txt", false},

		// Character class tests
		{"bracket single char", "file[0-9].txt", "file5.txt", true},
		{"bracket range", "file[a-z].txt", "filem.txt", true},
		{"bracket no match", "file[0-9].txt", "filea.txt", false},
		{"bracket set", "file[abc].txt", "fileb.txt", true},
		{"bracket set no match", "file[abc].txt", "filed.txt", false},

		// Negated bracket tests
		{"negated bracket", "file[!0-9].txt", "filea.txt", true},
		{"negated bracket no match", "file[!0-9].txt", "file5.txt", false},
		{"negated bracket set", "file[!abc].txt", "filed.txt", true},
		{"negated bracket set no match", "file[!abc].txt", "filea.txt", false},

		// Escape tests
		{"escaped asterisk", `file\*.txt`, "file*.txt", true},
		{"escaped asterisk no match", `file\*.txt`, "fileX.txt", false},
		{"escaped question", `file\?.txt`, "file?.txt", true},
		{"escaped bracket", `file\[0\].txt`, "file[0].txt", true},

		// Edge cases
		{"empty pattern empty string", "", "", true},
		{"empty pattern non-empty", "", "x", false},
		{"non-empty pattern empty", "x", "", false},
		{"exact match", "file.txt", "file.txt", true},
		{"exact no match", "file.txt", "file.csv", false},

		// Complex patterns
		{"complex pattern 1", "data_*.csv", "data_2024.csv", true},
		{"complex pattern 2", "file[0-9][a-z].txt", "file5x.txt", true},
		{"complex pattern 3", "*[0-9].csv", "data123.csv", true},
		{"complex pattern 4", "prefix*suffix", "prefixmiddlesuffix", true},
		{"complex pattern 5", "prefix*suffix", "prefixsuffix", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchGlob(tt.pattern, tt.input)
			assert.Equal(t, tt.expected, result, "pattern=%q input=%q", tt.pattern, tt.input)
		})
	}
}

func TestMatchPattern(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		path     string
		expected bool
	}{
		// Basic path matching
		{"exact match", "data/file.csv", "data/file.csv", true},
		{"exact no match", "data/file.csv", "data/file.txt", false},

		// Single asterisk
		{"asterisk in filename", "data/*.csv", "data/file.csv", true},
		{"asterisk in filename 2", "data/*.csv", "data/another.csv", true},
		{"asterisk in filename no match", "data/*.csv", "other/file.csv", false},
		{"asterisk in dir", "*/file.csv", "data/file.csv", true},

		// Double asterisk
		{"double asterisk", "data/**/*.csv", "data/file.csv", true},
		{"double asterisk nested", "data/**/*.csv", "data/sub/file.csv", true},
		{"double asterisk deep", "data/**/*.csv", "data/a/b/c/file.csv", true},
		{"double asterisk no match", "data/**/*.csv", "other/file.csv", false},
		{"double asterisk at start", "**/*.csv", "data/file.csv", true},
		{"double asterisk at start deep", "**/*.csv", "a/b/c/file.csv", true},

		// Question mark
		{"question in filename", "data/file?.csv", "data/file1.csv", true},
		{"question no match", "data/file?.csv", "data/file12.csv", false},

		// Character classes
		{"bracket in filename", "data/file[0-9].csv", "data/file5.csv", true},
		{"bracket no match", "data/file[0-9].csv", "data/filea.csv", false},

		// Hive partitioning patterns
		{"hive pattern", "data/year=*/month=*/*.parquet", "data/year=2024/month=01/data.parquet", true},
		{"hive pattern no match", "data/year=*/month=*/*.parquet", "data/year=2024/data.parquet", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := MatchPattern(tt.pattern, tt.path)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result, "pattern=%q path=%q", tt.pattern, tt.path)
		})
	}
}

func TestMatchPatternInvalid(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		path    string
	}{
		{"empty pattern", "", "data/file.csv"},
		{"multiple double asterisk", "data/**/**/file.csv", "data/a/b/file.csv"},
		{"unclosed bracket", "data/file[abc.csv", "data/fileb.csv"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := MatchPattern(tt.pattern, tt.path)
			assert.Error(t, err)
		})
	}
}

// TestGlobMatcherWithLocalFS tests the GlobMatcher with a real local filesystem.
func TestGlobMatcherWithLocalFS(t *testing.T) {
	// Create a temporary directory structure for testing
	tempDir := t.TempDir()

	// Create test directory structure:
	// tempDir/
	//   data/
	//     file1.csv
	//     file2.csv
	//     file3.txt
	//     subdir/
	//       file4.csv
	//       file5.parquet
	//     year=2024/
	//       month=01/
	//         data.csv
	//       month=02/
	//         data.csv
	//     year=2023/
	//       month=12/
	//         data.csv

	dirs := []string{
		"data",
		"data/subdir",
		"data/year=2024/month=01",
		"data/year=2024/month=02",
		"data/year=2023/month=12",
	}

	for _, dir := range dirs {
		err := os.MkdirAll(filepath.Join(tempDir, dir), 0o755)
		require.NoError(t, err)
	}

	files := []string{
		"data/file1.csv",
		"data/file2.csv",
		"data/file3.txt",
		"data/subdir/file4.csv",
		"data/subdir/file5.parquet",
		"data/year=2024/month=01/data.csv",
		"data/year=2024/month=02/data.csv",
		"data/year=2023/month=12/data.csv",
	}

	for _, file := range files {
		err := os.WriteFile(filepath.Join(tempDir, file), []byte("test"), 0o644)
		require.NoError(t, err)
	}

	fs := NewLocalFileSystem(tempDir)
	gm := NewGlobMatcher(fs)

	tests := []struct {
		name     string
		pattern  string
		expected []string
	}{
		{
			name:    "simple asterisk",
			pattern: "data/*.csv",
			expected: []string{
				"data/file1.csv",
				"data/file2.csv",
			},
		},
		{
			name:    "all csv files recursive",
			pattern: "data/**/*.csv",
			expected: []string{
				"data/file1.csv",
				"data/file2.csv",
				"data/subdir/file4.csv",
				"data/year=2023/month=12/data.csv",
				"data/year=2024/month=01/data.csv",
				"data/year=2024/month=02/data.csv",
			},
		},
		{
			name:    "question mark",
			pattern: "data/file?.csv",
			expected: []string{
				"data/file1.csv",
				"data/file2.csv",
			},
		},
		{
			name:    "bracket range",
			pattern: "data/file[1-2].csv",
			expected: []string{
				"data/file1.csv",
				"data/file2.csv",
			},
		},
		{
			name:    "bracket single",
			pattern: "data/file[1].csv",
			expected: []string{
				"data/file1.csv",
			},
		},
		{
			name:    "negated bracket",
			pattern: "data/file[!3].csv",
			expected: []string{
				"data/file1.csv",
				"data/file2.csv",
			},
		},
		{
			name:    "hive partitioning",
			pattern: "data/year=*/month=*/data.csv",
			expected: []string{
				"data/year=2023/month=12/data.csv",
				"data/year=2024/month=01/data.csv",
				"data/year=2024/month=02/data.csv",
			},
		},
		{
			name:    "specific year hive",
			pattern: "data/year=2024/month=*/*.csv",
			expected: []string{
				"data/year=2024/month=01/data.csv",
				"data/year=2024/month=02/data.csv",
			},
		},
		{
			name:     "no match",
			pattern:  "data/*.json",
			expected: nil,
		},
		{
			name:    "literal path",
			pattern: "data/file1.csv",
			expected: []string{
				"data/file1.csv",
			},
		},
		{
			name:    "parquet files",
			pattern: "data/**/*.parquet",
			expected: []string{
				"data/subdir/file5.parquet",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := gm.Match(tt.pattern)
			require.NoError(t, err)

			// Sort expected for comparison
			expected := make([]string, len(tt.expected))
			copy(expected, tt.expected)
			sort.Strings(expected)

			// Handle nil vs empty slice comparison
			if len(expected) == 0 && len(result) == 0 {
				return // Both empty, test passes
			}
			assert.Equal(t, expected, result)
		})
	}
}

func TestGlobMatcherMatchMultiple(t *testing.T) {
	// Create a temporary directory structure
	tempDir := t.TempDir()

	dirs := []string{
		"data",
		"backup",
	}

	for _, dir := range dirs {
		err := os.MkdirAll(filepath.Join(tempDir, dir), 0o755)
		require.NoError(t, err)
	}

	files := []string{
		"data/file1.csv",
		"data/file2.csv",
		"backup/file3.csv",
		"backup/file4.csv",
	}

	for _, file := range files {
		err := os.WriteFile(filepath.Join(tempDir, file), []byte("test"), 0o644)
		require.NoError(t, err)
	}

	fs := NewLocalFileSystem(tempDir)
	gm := NewGlobMatcher(fs)

	tests := []struct {
		name     string
		patterns []string
		expected []string
	}{
		{
			name:     "multiple patterns",
			patterns: []string{"data/*.csv", "backup/*.csv"},
			expected: []string{
				"backup/file3.csv",
				"backup/file4.csv",
				"data/file1.csv",
				"data/file2.csv",
			},
		},
		{
			name:     "overlapping patterns",
			patterns: []string{"data/*.csv", "data/file1.csv"},
			expected: []string{
				"data/file1.csv",
				"data/file2.csv",
			},
		},
		{
			name:     "single pattern",
			patterns: []string{"data/*.csv"},
			expected: []string{
				"data/file1.csv",
				"data/file2.csv",
			},
		},
		{
			name:     "no matches",
			patterns: []string{"*.json", "*.xml"},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := gm.MatchMultiple(tt.patterns)
			require.NoError(t, err)

			expected := make([]string, len(tt.expected))
			copy(expected, tt.expected)
			sort.Strings(expected)

			// Handle nil vs empty slice comparison
			if len(expected) == 0 && len(result) == 0 {
				return // Both empty, test passes
			}
			assert.Equal(t, expected, result)
		})
	}
}

func TestGlobMatcherErrors(t *testing.T) {
	tempDir := t.TempDir()
	fs := NewLocalFileSystem(tempDir)
	gm := NewGlobMatcher(fs)

	tests := []struct {
		name    string
		pattern string
	}{
		{"empty pattern", ""},
		{"multiple double asterisk", "data/**/**/file.csv"},
		{"unclosed bracket", "data/file[abc.csv"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := gm.Match(tt.pattern)
			assert.Error(t, err)
		})
	}
}

func TestMatchCharClass(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		char     rune
		matched  bool
		rest     string
		ok       bool
	}{
		{"simple set match", "[abc]", 'b', true, "", true},
		{"simple set no match", "[abc]", 'd', false, "", true},
		{"range match", "[a-z]", 'm', true, "", true},
		{"range no match", "[a-z]", '5', false, "", true},
		{"negated match", "[!abc]", 'd', true, "", true},
		{"negated no match", "[!abc]", 'a', false, "", true},
		{"multiple ranges", "[a-zA-Z]", 'M', true, "", true},
		{"digit range", "[0-9]", '5', true, "", true},
		{"mixed set and range", "[a-z0]", '0', true, "", true},
		{"closing bracket in set", "[]abc]", ']', true, "", true},
		{"escaped in class", `[a\-z]`, '-', true, "", true},

		// With remaining pattern
		{"with rest", "[abc]xyz", 'a', true, "xyz", true},

		// Invalid patterns
		{"not a class", "abc", 'a', false, "abc", false},
		{"unclosed", "[abc", 'a', false, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matched, rest, ok := matchCharClass(tt.pattern, tt.char)
			assert.Equal(t, tt.matched, matched, "matched mismatch")
			assert.Equal(t, tt.rest, rest, "rest mismatch")
			assert.Equal(t, tt.ok, ok, "ok mismatch")
		})
	}
}

// Benchmark tests
func BenchmarkContainsGlobPattern(b *testing.B) {
	patterns := []string{
		"data/file.csv",
		"data/*.csv",
		"data/**/*.parquet",
		"data/file[0-9].txt",
		`data/file\*.txt`,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, p := range patterns {
			ContainsGlobPattern(p)
		}
	}
}

func BenchmarkMatchGlob(b *testing.B) {
	testCases := []struct {
		pattern string
		input   string
	}{
		{"*.csv", "data.csv"},
		{"data*.csv", "data_2024_01_15.csv"},
		{"file[0-9][0-9].txt", "file42.txt"},
		{"prefix*middle*suffix", "prefix_abc_middle_xyz_suffix"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, tc := range testCases {
			matchGlob(tc.pattern, tc.input)
		}
	}
}

func BenchmarkMatchPattern(b *testing.B) {
	testCases := []struct {
		pattern string
		path    string
	}{
		{"data/*.csv", "data/file.csv"},
		{"data/**/*.parquet", "data/year=2024/month=01/data.parquet"},
		{"**/year=*/month=*/*.csv", "data/archive/year=2024/month=01/data.csv"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, tc := range testCases {
			_, _ = MatchPattern(tc.pattern, tc.path)
		}
	}
}

func BenchmarkExtractPrefix(b *testing.B) {
	patterns := []string{
		"data/2024/01/*.csv",
		"data/**/*.parquet",
		"s3://bucket/data/year=2024/**/*.csv",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, p := range patterns {
			ExtractPrefix(p)
		}
	}
}

func BenchmarkGlobMatcher(b *testing.B) {
	// Create a test directory structure
	tempDir := b.TempDir()

	// Create 100 files
	for i := 0; i < 100; i++ {
		dir := filepath.Join(tempDir, "data")
		_ = os.MkdirAll(dir, 0o755)
		path := filepath.Join(dir, filepath.Base(tempDir)+"_"+string(rune('0'+i%10))+".csv")
		_ = os.WriteFile(path, []byte("test"), 0o644)
	}

	fs := NewLocalFileSystem(tempDir)
	gm := NewGlobMatcher(fs)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = gm.Match("data/*.csv")
	}
}

// TestLocalFileSystem_Glob tests the FileSystem.Glob method on LocalFileSystem.
func TestLocalFileSystem_Glob(t *testing.T) {
	// Create a temporary directory structure for testing
	tempDir := t.TempDir()

	// Create test directory structure:
	// tempDir/
	//   data/
	//     file1.csv
	//     file2.csv
	//     file3.txt
	//     subdir/
	//       file4.csv
	//       file5.parquet

	dirs := []string{
		"data",
		"data/subdir",
	}

	for _, dir := range dirs {
		err := os.MkdirAll(filepath.Join(tempDir, dir), 0o755)
		require.NoError(t, err)
	}

	files := []string{
		"data/file1.csv",
		"data/file2.csv",
		"data/file3.txt",
		"data/subdir/file4.csv",
		"data/subdir/file5.parquet",
	}

	for _, file := range files {
		err := os.WriteFile(filepath.Join(tempDir, file), []byte("test"), 0o644)
		require.NoError(t, err)
	}

	// Use LocalFileSystem with basePath (follows existing test patterns)
	fs := NewLocalFileSystem(tempDir)

	tests := []struct {
		name     string
		pattern  string
		expected []string
	}{
		{
			name:    "simple asterisk",
			pattern: "data/*.csv",
			expected: []string{
				"data/file1.csv",
				"data/file2.csv",
			},
		},
		{
			name:    "all csv files recursive",
			pattern: "data/**/*.csv",
			expected: []string{
				"data/file1.csv",
				"data/file2.csv",
				"data/subdir/file4.csv",
			},
		},
		{
			name:    "question mark",
			pattern: "data/file?.csv",
			expected: []string{
				"data/file1.csv",
				"data/file2.csv",
			},
		},
		{
			name:     "no match",
			pattern:  "data/*.json",
			expected: nil,
		},
		{
			name:    "literal path",
			pattern: "data/file1.csv",
			expected: []string{
				"data/file1.csv",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := fs.Glob(tt.pattern)
			require.NoError(t, err)

			// Sort expected for comparison
			expected := make([]string, len(tt.expected))
			copy(expected, tt.expected)
			sort.Strings(expected)

			// Handle nil vs empty slice comparison
			if len(expected) == 0 && len(result) == 0 {
				return // Both empty, test passes
			}
			assert.Equal(t, expected, result)
		})
	}
}

// TestLocalFileSystem_SupportsGlob tests that LocalFileSystem reports native glob support.
func TestLocalFileSystem_SupportsGlob(t *testing.T) {
	fs := NewLocalFileSystem("")
	assert.True(t, fs.SupportsGlob())
}

// TestFallbackGlob tests the FallbackGlob function used by filesystems without native glob.
func TestFallbackGlob(t *testing.T) {
	// Create a temporary directory structure
	tempDir := t.TempDir()

	dirs := []string{
		"data",
		"data/subdir",
	}

	for _, dir := range dirs {
		err := os.MkdirAll(filepath.Join(tempDir, dir), 0o755)
		require.NoError(t, err)
	}

	files := []string{
		"data/file1.csv",
		"data/file2.csv",
		"data/subdir/file3.csv",
	}

	for _, file := range files {
		err := os.WriteFile(filepath.Join(tempDir, file), []byte("test"), 0o644)
		require.NoError(t, err)
	}

	fs := NewLocalFileSystem(tempDir)
	pattern := "data/*.csv"

	// Test FallbackGlob works the same as Glob
	result1, err := fs.Glob(pattern)
	require.NoError(t, err)

	result2, err := FallbackGlob(fs, pattern)
	require.NoError(t, err)

	assert.Equal(t, result1, result2)
	assert.Len(t, result1, 2, "Should find 2 CSV files")
}

// TestFileSystem_GlobInterface tests that all FileSystem implementations have Glob and SupportsGlob.
func TestFileSystem_GlobInterface(t *testing.T) {
	// Test that LocalFileSystem implements Glob interface correctly
	tempDir := t.TempDir()
	localFS := NewLocalFileSystem(tempDir)

	// Verify the interface methods exist and can be called
	var fs FileSystem = localFS
	supportsGlob := fs.SupportsGlob()
	assert.True(t, supportsGlob, "LocalFileSystem should support glob")

	// Create a test file
	testFile := filepath.Join(tempDir, "test.csv")
	err := os.WriteFile(testFile, []byte("test"), 0o644)
	require.NoError(t, err)

	// Test glob with relative pattern (relative to basePath)
	result, err := fs.Glob("*.csv")
	require.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, "test.csv", result[0])
}

// TestFileSystem_GlobInvalidPattern tests error handling for invalid patterns.
func TestFileSystem_GlobInvalidPattern(t *testing.T) {
	tempDir := t.TempDir()
	fs := NewLocalFileSystem(tempDir)

	tests := []struct {
		name    string
		pattern string
	}{
		{"empty pattern", ""},
		{"multiple double asterisk", "data/**/**/file.csv"},
		{"unclosed bracket", "data/file[abc.csv"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := fs.Glob(tt.pattern)
			assert.Error(t, err)
		})
	}
}

// TestHTTPFileSystem_GlobNotSupported tests that HTTPFileSystem returns error for glob.
func TestHTTPFileSystem_GlobNotSupported(t *testing.T) {
	ctx := context.Background()
	fs, err := NewHTTPFileSystem(ctx, DefaultHTTPConfig())
	require.NoError(t, err)

	// Verify SupportsGlob returns false
	assert.False(t, fs.SupportsGlob())

	// Verify Glob returns ErrNotSupported
	_, err = fs.Glob("*.csv")
	assert.ErrorIs(t, err, ErrNotSupported)
}

// TestCloudFileSystem_SupportsGlob tests that cloud filesystems report native glob support.
// Note: These tests verify the interface but do not test actual cloud operations.
// For actual cloud glob tests, see the integration tests with LocalStack/emulators.
func TestCloudFileSystem_SupportsGlob(t *testing.T) {
	// Test S3 SupportsGlob
	t.Run("S3FileSystem", func(t *testing.T) {
		config := DefaultS3Config()
		config.Endpoint = "localhost:9000" // Fake endpoint for testing

		// Note: NewS3FileSystem may fail if it tries to connect to the endpoint,
		// but we only need to test that the method exists and returns true.
		// We use NewS3FileSystemWithClient to avoid connection issues.
		fs := &S3FileSystem{config: config}
		assert.True(t, fs.SupportsGlob(), "S3FileSystem should support native glob")
	})

	// Test GCS SupportsGlob
	t.Run("GCSFileSystem", func(t *testing.T) {
		fs := &GCSFileSystem{}
		assert.True(t, fs.SupportsGlob(), "GCSFileSystem should support native glob")
	})

	// Test Azure SupportsGlob
	t.Run("AzureFileSystem", func(t *testing.T) {
		fs := &AzureFileSystem{}
		assert.True(t, fs.SupportsGlob(), "AzureFileSystem should support native glob")
	})
}

// TestCloudGlob_InvalidPattern tests that cloud filesystems properly validate patterns.
func TestCloudGlob_InvalidPattern(t *testing.T) {
	invalidPatterns := []struct {
		name    string
		pattern string
	}{
		{"empty pattern", ""},
		{"multiple double asterisk", "s3://bucket/data/**/**/file.csv"},
		{"unclosed bracket", "gs://bucket/data/file[abc.csv"},
	}

	// Test each invalid pattern for S3
	t.Run("S3FileSystem", func(t *testing.T) {
		config := DefaultS3Config()
		config.Endpoint = "localhost:9000"
		fs := &S3FileSystem{config: config}

		for _, tt := range invalidPatterns {
			t.Run(tt.name, func(t *testing.T) {
				_, err := fs.Glob(tt.pattern)
				assert.Error(t, err, "S3FileSystem should return error for invalid pattern: %s", tt.pattern)
			})
		}
	})

	// Test each invalid pattern for GCS
	t.Run("GCSFileSystem", func(t *testing.T) {
		fs := &GCSFileSystem{}

		for _, tt := range invalidPatterns {
			t.Run(tt.name, func(t *testing.T) {
				// GCS requires a bucket, so adjust patterns
				pattern := tt.pattern
				if pattern == "" {
					// Skip empty pattern test - it's already empty
				} else if !strings.Contains(pattern, "gs://") && !strings.Contains(pattern, "gcs://") {
					pattern = "gs://bucket/" + pattern
				}
				_, err := fs.Glob(pattern)
				assert.Error(t, err, "GCSFileSystem should return error for invalid pattern: %s", tt.pattern)
			})
		}
	})

	// Test each invalid pattern for Azure
	t.Run("AzureFileSystem", func(t *testing.T) {
		fs := &AzureFileSystem{}

		for _, tt := range invalidPatterns {
			t.Run(tt.name, func(t *testing.T) {
				// Azure requires a container, so adjust patterns
				pattern := tt.pattern
				if pattern == "" {
					// Skip empty pattern test - it's already empty
				} else if !strings.Contains(pattern, "azure://") && !strings.Contains(pattern, "az://") {
					pattern = "azure://container/" + pattern
				}
				_, err := fs.Glob(pattern)
				assert.Error(t, err, "AzureFileSystem should return error for invalid pattern: %s", tt.pattern)
			})
		}
	})
}
