package filesystem

import (
	"os"
	"path/filepath"
	"testing"
)

// createTestFiles creates n files in the specified directory for benchmarking.
// Files are created with minimal content to avoid I/O overhead during setup.
func createTestFiles(t testing.TB, dir string, pattern string, count int) []string {
	paths := make([]string, count)
	for i := 0; i < count; i++ {
		filename := filepath.Join(dir, pattern)
		// Replace %d placeholder with file number
		filename = filepath.Join(dir, replacePattern(pattern, i))
		paths[i] = filename

		// Create parent directory if needed
		parentDir := filepath.Dir(filename)
		if err := os.MkdirAll(parentDir, 0o755); err != nil {
			t.Fatalf("failed to create directory %s: %v", parentDir, err)
		}

		// Create empty file
		if err := os.WriteFile(filename, []byte{}, 0o644); err != nil {
			t.Fatalf("failed to create file %s: %v", filename, err)
		}
	}
	return paths
}

// replacePattern replaces %d with the index value in a pattern string.
func replacePattern(pattern string, idx int) string {
	result := make([]byte, 0, len(pattern)+10)
	for i := 0; i < len(pattern); i++ {
		if i < len(pattern)-1 && pattern[i] == '%' && pattern[i+1] == 'd' {
			// Replace %d with the index
			num := idx
			if num == 0 {
				result = append(result, '0')
			} else {
				digits := make([]byte, 0, 10)
				for num > 0 {
					digits = append(digits, byte('0'+num%10))
					num /= 10
				}
				// Reverse digits
				for j := len(digits) - 1; j >= 0; j-- {
					result = append(result, digits[j])
				}
			}
			i++ // Skip the 'd'
		} else {
			result = append(result, pattern[i])
		}
	}
	return string(result)
}

// createNestedTestFiles creates files in a nested directory structure for recursive glob testing.
func createNestedTestFiles(t testing.TB, baseDir string, depth, filesPerLevel int) int {
	totalFiles := 0
	var createLevel func(dir string, currentDepth int)
	createLevel = func(dir string, currentDepth int) {
		if currentDepth > depth {
			return
		}

		// Create files at this level
		for i := 0; i < filesPerLevel; i++ {
			filename := filepath.Join(dir, replacePattern("file_%d.csv", i))
			if err := os.WriteFile(filename, []byte{}, 0o644); err != nil {
				t.Fatalf("failed to create file %s: %v", filename, err)
			}
			totalFiles++
		}

		// Create subdirectories
		for i := 0; i < 3; i++ { // 3 subdirectories per level
			subdir := filepath.Join(dir, replacePattern("dir_%d", i))
			if err := os.MkdirAll(subdir, 0o755); err != nil {
				t.Fatalf("failed to create directory %s: %v", subdir, err)
			}
			createLevel(subdir, currentDepth+1)
		}
	}

	createLevel(baseDir, 0)
	return totalFiles
}

// BenchmarkGlobLocalFS1000 benchmarks glob matching with 1,000 files.
func BenchmarkGlobLocalFS1000(b *testing.B) {
	tempDir := b.TempDir()
	dataDir := filepath.Join(tempDir, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		b.Fatalf("failed to create data directory: %v", err)
	}

	// Create 1,000 CSV files
	createTestFiles(b, dataDir, "file_%d.csv", 1000)

	fs := NewLocalFileSystem(tempDir)
	gm := NewGlobMatcher(fs)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		matches, err := gm.Match("data/*.csv")
		if err != nil {
			b.Fatalf("glob failed: %v", err)
		}
		if len(matches) != 1000 {
			b.Fatalf("expected 1000 matches, got %d", len(matches))
		}
	}

	b.ReportMetric(float64(1000)/b.Elapsed().Seconds()*float64(b.N), "files/sec")
}

// BenchmarkGlobLocalFS10000 benchmarks glob matching with 10,000 files.
// This test is marked as potentially slow and should be skipped in short mode.
func BenchmarkGlobLocalFS10000(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping 10,000 files benchmark in short mode")
	}

	tempDir := b.TempDir()
	dataDir := filepath.Join(tempDir, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		b.Fatalf("failed to create data directory: %v", err)
	}

	// Create 10,000 CSV files
	createTestFiles(b, dataDir, "file_%d.csv", 10000)

	fs := NewLocalFileSystem(tempDir)
	gm := NewGlobMatcher(fs)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		matches, err := gm.Match("data/*.csv")
		if err != nil {
			b.Fatalf("glob failed: %v", err)
		}
		if len(matches) != 10000 {
			b.Fatalf("expected 10000 matches, got %d", len(matches))
		}
	}

	b.ReportMetric(float64(10000)/b.Elapsed().Seconds()*float64(b.N), "files/sec")
}

// BenchmarkGlobRecursive benchmarks recursive glob patterns (**/*.csv).
func BenchmarkGlobRecursive(b *testing.B) {
	tempDir := b.TempDir()
	dataDir := filepath.Join(tempDir, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		b.Fatalf("failed to create data directory: %v", err)
	}

	// Create nested directory structure with files
	// depth=3, filesPerLevel=5 gives approximately 5 + 3*5 + 9*5 + 27*5 = 200 files
	totalFiles := createNestedTestFiles(b, dataDir, 3, 5)

	fs := NewLocalFileSystem(tempDir)
	gm := NewGlobMatcher(fs)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		matches, err := gm.Match("data/**/*.csv")
		if err != nil {
			b.Fatalf("glob failed: %v", err)
		}
		if len(matches) == 0 {
			b.Fatalf("expected matches, got 0")
		}
	}

	b.ReportMetric(float64(totalFiles)/b.Elapsed().Seconds()*float64(b.N), "files/sec")
}

// BenchmarkGlobRecursiveDeep benchmarks deeply nested recursive glob patterns.
func BenchmarkGlobRecursiveDeep(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping deep recursive benchmark in short mode")
	}

	tempDir := b.TempDir()
	dataDir := filepath.Join(tempDir, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		b.Fatalf("failed to create data directory: %v", err)
	}

	// Create deeper nested structure
	totalFiles := createNestedTestFiles(b, dataDir, 4, 10)

	fs := NewLocalFileSystem(tempDir)
	gm := NewGlobMatcher(fs)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		matches, err := gm.Match("data/**/*.csv")
		if err != nil {
			b.Fatalf("glob failed: %v", err)
		}
		if len(matches) == 0 {
			b.Fatalf("expected matches, got 0")
		}
	}

	b.ReportMetric(float64(totalFiles)/b.Elapsed().Seconds()*float64(b.N), "files/sec")
}

// BenchmarkPrefixExtraction benchmarks the ExtractPrefix function.
func BenchmarkPrefixExtraction(b *testing.B) {
	patterns := []string{
		"data/2024/01/*.csv",
		"data/**/*.parquet",
		"s3://bucket/data/year=2024/**/*.csv",
		"gs://bucket/prefix/subprefix/deep/nested/path/*.json",
		"azure://container/very/long/prefix/path/to/data/*.parquet",
		"**/data.csv",
		"data/file.csv",
		"data/year=*/month=*/day=*/*.csv",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, p := range patterns {
			ExtractPrefix(p)
		}
	}

	b.ReportMetric(float64(len(patterns)), "patterns/op")
}

// BenchmarkPatternMatching benchmarks the matchGlob function with various patterns.
func BenchmarkPatternMatching(b *testing.B) {
	testCases := []struct {
		name    string
		pattern string
		input   string
	}{
		{"simple_asterisk", "*.csv", "data.csv"},
		{"prefix_asterisk", "data*.csv", "data_2024_01_15.csv"},
		{"character_class", "file[0-9][0-9].txt", "file42.txt"},
		{"multiple_asterisks", "prefix*middle*suffix", "prefix_abc_middle_xyz_suffix"},
		{"question_mark", "file?.csv", "file1.csv"},
		{"negated_bracket", "file[!abc].txt", "filex.txt"},
		{"complex_pattern", "data_[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9].csv", "data_2024-01-15.csv"},
		{"long_prefix", "very_long_prefix_data_2024_*.csv", "very_long_prefix_data_2024_01.csv"},
		{"escaped_chars", `file\*.txt`, "file*.txt"},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, tc := range testCases {
			matchGlob(tc.pattern, tc.input)
		}
	}

	b.ReportMetric(float64(len(testCases)), "patterns/op")
}

// BenchmarkPatternMatchingSimple benchmarks simple asterisk patterns.
func BenchmarkPatternMatchingSimple(b *testing.B) {
	pattern := "*.csv"
	inputs := []string{
		"data.csv",
		"file123.csv",
		"very_long_filename_with_lots_of_characters.csv",
		"a.csv",
		"test_data_2024_01_15_export.csv",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, input := range inputs {
			matchGlob(pattern, input)
		}
	}
}

// BenchmarkPatternMatchingComplex benchmarks complex character class patterns.
func BenchmarkPatternMatchingComplex(b *testing.B) {
	pattern := "[a-zA-Z][0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]_*.csv"
	inputs := []string{
		"X2024-01-15_data.csv",
		"Y2023-12-31_export.csv",
		"A1999-06-15_backup.csv",
		"Z2025-01-01_test.csv",
		"M2020-07-04_report.csv",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, input := range inputs {
			matchGlob(pattern, input)
		}
	}
}

// BenchmarkMatchPatternRecursive benchmarks the MatchPattern function with recursive patterns.
func BenchmarkMatchPatternRecursive(b *testing.B) {
	testCases := []struct {
		pattern string
		path    string
	}{
		{"data/**/*.csv", "data/file.csv"},
		{"data/**/*.csv", "data/subdir/file.csv"},
		{"data/**/*.csv", "data/a/b/c/file.csv"},
		{"**/year=*/month=*/*.csv", "data/year=2024/month=01/file.csv"},
		{"**/year=*/month=*/*.csv", "archive/backup/year=2023/month=12/data.csv"},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, tc := range testCases {
			_, _ = MatchPattern(tc.pattern, tc.path)
		}
	}

	b.ReportMetric(float64(len(testCases)), "patterns/op")
}

// BenchmarkValidateGlobPattern benchmarks glob pattern validation.
func BenchmarkValidateGlobPattern(b *testing.B) {
	patterns := []string{
		"data/*.csv",
		"data/**/*.parquet",
		"file[0-9].txt",
		"file[!abc].txt",
		"data/file?.csv",
		`file\*.txt`,
		"data/year=*/month=*/*.csv",
		"data/2024/01/15/*.csv",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, p := range patterns {
			_ = ValidateGlobPattern(p)
		}
	}

	b.ReportMetric(float64(len(patterns)), "patterns/op")
}

// BenchmarkContainsGlobPatternLarge benchmarks ContainsGlobPattern with many paths.
func BenchmarkContainsGlobPatternLarge(b *testing.B) {
	// Generate a mix of glob and non-glob paths
	paths := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		if i%10 == 0 {
			paths[i] = replacePattern("data/*.csv", i)
		} else {
			paths[i] = replacePattern("data/file_%d.csv", i)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, p := range paths {
			ContainsGlobPattern(p)
		}
	}

	b.ReportMetric(float64(len(paths)), "paths/op")
}

// BenchmarkParsePatternSegments benchmarks pattern segment parsing.
func BenchmarkParsePatternSegments(b *testing.B) {
	patterns := []string{
		"data/file.csv",
		"data/*.csv",
		"data/**/*.parquet",
		"data/year=*/month=*/day=*/*.csv",
		"/absolute/path/to/data/**/*.json",
		"s3://bucket/prefix/data/**/*.parquet",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, p := range patterns {
			ParsePatternSegments(p)
		}
	}

	b.ReportMetric(float64(len(patterns)), "patterns/op")
}

// BenchmarkGlobMatchMultiple benchmarks matching multiple patterns.
func BenchmarkGlobMatchMultiple(b *testing.B) {
	tempDir := b.TempDir()

	// Create multiple data directories
	dirs := []string{"data", "backup", "archive"}
	for _, dir := range dirs {
		dirPath := filepath.Join(tempDir, dir)
		if err := os.MkdirAll(dirPath, 0o755); err != nil {
			b.Fatalf("failed to create directory: %v", err)
		}
		createTestFiles(b, dirPath, "file_%d.csv", 100)
	}

	fs := NewLocalFileSystem(tempDir)
	gm := NewGlobMatcher(fs)

	patterns := []string{"data/*.csv", "backup/*.csv", "archive/*.csv"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		matches, err := gm.MatchMultiple(patterns)
		if err != nil {
			b.Fatalf("glob failed: %v", err)
		}
		if len(matches) != 300 {
			b.Fatalf("expected 300 matches, got %d", len(matches))
		}
	}

	b.ReportMetric(float64(300)/b.Elapsed().Seconds()*float64(b.N), "files/sec")
}

// BenchmarkGlobWithHivePartitions benchmarks glob with Hive-style partitioning.
func BenchmarkGlobWithHivePartitions(b *testing.B) {
	tempDir := b.TempDir()
	dataDir := filepath.Join(tempDir, "data")

	// Create Hive-style partition structure
	years := []string{"2023", "2024"}
	months := []string{"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"}

	totalFiles := 0
	for _, year := range years {
		for _, month := range months {
			partDir := filepath.Join(dataDir, "year="+year, "month="+month)
			if err := os.MkdirAll(partDir, 0o755); err != nil {
				b.Fatalf("failed to create directory: %v", err)
			}
			// Create 5 files per partition
			for i := 0; i < 5; i++ {
				filename := filepath.Join(partDir, replacePattern("data_%d.csv", i))
				if err := os.WriteFile(filename, []byte{}, 0o644); err != nil {
					b.Fatalf("failed to create file: %v", err)
				}
				totalFiles++
			}
		}
	}

	fs := NewLocalFileSystem(tempDir)
	gm := NewGlobMatcher(fs)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		matches, err := gm.Match("data/year=*/month=*/*.csv")
		if err != nil {
			b.Fatalf("glob failed: %v", err)
		}
		if len(matches) != totalFiles {
			b.Fatalf("expected %d matches, got %d", totalFiles, len(matches))
		}
	}

	b.ReportMetric(float64(totalFiles)/b.Elapsed().Seconds()*float64(b.N), "files/sec")
}
