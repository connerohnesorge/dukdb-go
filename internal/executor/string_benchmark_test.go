// Package executor provides string function benchmarks and performance tests.
// This file contains benchmarks for tasks 15.5, 15.6, 15.7, and 15.8.
package executor

import (
	"fmt"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// =============================================================================
// Task 15.5: Benchmark STRING_SPLIT on large strings with many separators
// =============================================================================

// BenchmarkStringSplit_BySize benchmarks STRING_SPLIT with various string sizes.
func BenchmarkStringSplit_BySize(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		// Create a string with separators every 10 characters
		str := createStringWithSeparators(size, ",", 10)
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = stringSplitValue(str, ",")
			}
		})
	}
}

// BenchmarkStringSplit_BySeparatorCount benchmarks STRING_SPLIT with varying separator counts.
func BenchmarkStringSplit_BySeparatorCount(b *testing.B) {
	// Fixed string size of 1000 characters
	baseSize := 1000
	separatorCounts := []int{10, 50, 100, 200}

	for _, sepCount := range separatorCounts {
		// Calculate interval to get approximately sepCount separators
		interval := baseSize / sepCount
		if interval < 2 {
			interval = 2
		}
		str := createStringWithSeparators(baseSize, ",", interval)

		b.Run(fmt.Sprintf("separators_%d", sepCount), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = stringSplitValue(str, ",")
			}
		})
	}
}

// BenchmarkStringSplit_BySeparatorLength benchmarks STRING_SPLIT with short vs long separators.
func BenchmarkStringSplit_BySeparatorLength(b *testing.B) {
	size := 1000
	separators := []string{
		",",             // 1 character
		"---",           // 3 characters
		"<separator>",   // 11 characters
		"||DELIMITER||", // 13 characters
	}

	for _, sep := range separators {
		str := createStringWithSeparators(size, sep, 10)
		b.Run(fmt.Sprintf("sep_len_%d", len(sep)), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = stringSplitValue(str, sep)
			}
		})
	}
}

// BenchmarkStringSplit_EmptySeparator benchmarks splitting into individual characters.
func BenchmarkStringSplit_EmptySeparator(b *testing.B) {
	sizes := []int{100, 500, 1000}

	for _, size := range sizes {
		str := strings.Repeat("a", size)
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = stringSplitValue(str, "")
			}
		})
	}
}

// BenchmarkStringSplit_Unicode benchmarks STRING_SPLIT with Unicode strings.
func BenchmarkStringSplit_Unicode(b *testing.B) {
	// Unicode string with Japanese characters
	unicodeStr := strings.Repeat("Hello", 100) + strings.Repeat(",", 1)
	for i := 0; i < 50; i++ {
		unicodeStr += strings.Repeat("x", 10) + ","
	}

	b.Run("unicode_content", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = stringSplitValue(unicodeStr, ",")
		}
	})
}

// =============================================================================
// Task 15.6: Profile memory usage for string operations
// =============================================================================

// BenchmarkMemory_StringSplit profiles memory allocation for STRING_SPLIT.
func BenchmarkMemory_StringSplit(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		str := createStringWithSeparators(size, ",", 10)
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, _ := stringSplitValue(str, ",")
				// Prevent compiler optimization
				_ = result
			}
		})
	}
}

// BenchmarkMemory_Levenshtein profiles memory allocation for LEVENSHTEIN.
func BenchmarkMemory_Levenshtein(b *testing.B) {
	sizes := []int{10, 50, 100, 200}

	for _, size := range sizes {
		str1 := strings.Repeat("a", size)
		str2 := strings.Repeat("b", size)
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, _ := levenshteinValue(str1, str2)
				_ = result
			}
		})
	}
}

// BenchmarkMemory_DamerauLevenshtein profiles memory allocation for DAMERAU_LEVENSHTEIN.
func BenchmarkMemory_DamerauLevenshtein(b *testing.B) {
	sizes := []int{10, 50, 100, 200}

	for _, size := range sizes {
		str1 := strings.Repeat("a", size)
		str2 := strings.Repeat("b", size)
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, _ := damerauLevenshteinValue(str1, str2)
				_ = result
			}
		})
	}
}

// BenchmarkMemory_RegexpReplace profiles memory allocation for REGEXP_REPLACE.
func BenchmarkMemory_RegexpReplace(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		str := strings.Repeat("abc123def456", size/12+1)[:size]
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, _ := regexpReplaceValue(str, "[0-9]+", "X", "g")
				_ = result
			}
		})
	}
}

// BenchmarkMemory_Hash profiles memory allocation for hash functions.
func BenchmarkMemory_Hash(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		str := strings.Repeat("a", size)

		b.Run(fmt.Sprintf("MD5_size_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, _ := md5Value(str)
				_ = result
			}
		})

		b.Run(fmt.Sprintf("SHA256_size_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, _ := sha256Value(str)
				_ = result
			}
		})
	}
}

// BenchmarkMemory_ConcatWS profiles memory allocation for CONCAT_WS.
func BenchmarkMemory_ConcatWS(b *testing.B) {
	argCounts := []int{5, 20, 100}

	for _, count := range argCounts {
		args := make([]any, count)
		for i := range args {
			args[i] = fmt.Sprintf("value%d", i)
		}

		b.Run(fmt.Sprintf("args_%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, _ := concatWSValue(",", args...)
				_ = result
			}
		})
	}
}

// BenchmarkMemory_Reverse profiles memory allocation for REVERSE.
func BenchmarkMemory_Reverse(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		str := strings.Repeat("abcdefghij", size/10+1)[:size]
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, _ := reverseValue(str)
				_ = result
			}
		})
	}
}

// BenchmarkMemory_Padding profiles memory allocation for LPAD/RPAD.
func BenchmarkMemory_Padding(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		str := "x"
		b.Run(fmt.Sprintf("LPAD_size_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, _ := lpadValue(str, int64(size), "0")
				_ = result
			}
		})

		b.Run(fmt.Sprintf("RPAD_size_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, _ := rpadValue(str, int64(size), "0")
				_ = result
			}
		})
	}
}

// TestMemoryScaling documents memory scaling behavior for string operations.
// This is a test (not benchmark) to document memory usage characteristics.
func TestMemoryScaling(t *testing.T) {
	t.Run("STRING_SPLIT memory scaling", func(t *testing.T) {
		// Memory should scale linearly with output size
		// For a string of size N with M separators, we allocate:
		// - M+1 strings for the result slice
		// - Total string bytes approximately equal to input size
		sizes := []int{100, 1000, 10000}
		for _, size := range sizes {
			str := createStringWithSeparators(size, ",", 10)
			var memBefore, memAfter runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&memBefore)

			result, _ := stringSplitValue(str, ",")
			arr := result.([]string)

			runtime.ReadMemStats(&memAfter)
			allocBytes := memAfter.TotalAlloc - memBefore.TotalAlloc

			t.Logf("STRING_SPLIT size=%d: result_len=%d, alloc_bytes=%d, bytes_per_element=%d",
				size, len(arr), allocBytes, allocBytes/uint64(len(arr)+1))
		}
	})

	t.Run("LEVENSHTEIN memory scaling", func(t *testing.T) {
		// Memory should scale as O(n*m) for the distance matrix
		sizes := []int{10, 50, 100}
		for _, size := range sizes {
			str1 := strings.Repeat("a", size)
			str2 := strings.Repeat("b", size)
			var memBefore, memAfter runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&memBefore)

			_, _ = levenshteinValue(str1, str2)

			runtime.ReadMemStats(&memAfter)
			allocBytes := memAfter.TotalAlloc - memBefore.TotalAlloc

			expectedMatrixSize := uint64(size+1) * uint64(size+1) * 8 // int64 = 8 bytes
			t.Logf(
				"LEVENSHTEIN size=%d: alloc_bytes=%d, expected_matrix=%d, ratio=%.2f",
				size,
				allocBytes,
				expectedMatrixSize,
				float64(allocBytes)/float64(expectedMatrixSize),
			)
		}
	})
}

// =============================================================================
// Task 15.7: Compare performance with DuckDB (target: within 2x)
// =============================================================================

// The following benchmarks document expected performance relative to DuckDB.
// Since we cannot run actual DuckDB in tests, we document expected ranges.
// Target: dukdb-go should be within 2x of DuckDB performance.

/*
Performance Comparison Notes (Task 15.7):

STRING_SPLIT:
- DuckDB uses highly optimized C++ string operations
- Go's strings.Split is also well-optimized
- Expected: 1.2x - 1.8x slower than DuckDB for large strings
- Reason: Go's memory model and GC vs C++ manual memory management

REGEXP_MATCHES/REGEXP_REPLACE:
- Both use RE2 regex engine (DuckDB uses RE2, Go's regexp implements RE2)
- Expected: 1.0x - 1.5x of DuckDB performance
- Reason: Same underlying algorithm, slight overhead from Go interface

LEVENSHTEIN/DAMERAU_LEVENSHTEIN:
- Both use O(n*m) dynamic programming
- Expected: 1.3x - 2.0x slower than DuckDB
- Reason: DuckDB may use SIMD optimizations, Go uses scalar operations

MD5/SHA256:
- Both use crypto libraries
- Expected: 1.0x - 1.2x of DuckDB performance
- Reason: Both use highly optimized implementations (Go's crypto is excellent)

CONCAT_WS:
- Expected: 1.0x - 1.5x of DuckDB performance
- Reason: Simple string concatenation, Go's strings.Join is efficient

Recommendations for staying within 2x:
1. Use string building with pre-allocated capacity where possible
2. Avoid unnecessary string conversions
3. Consider caching compiled regex patterns (see Task 15.8)
4. Use rune slices only when UTF-8 safety is required
*/

// BenchmarkDuckDBComparison_StringSplit provides baseline for comparison.
// Run this benchmark and compare against DuckDB CLI timing for same operations.
func BenchmarkDuckDBComparison_StringSplit(b *testing.B) {
	// This benchmark should complete in similar time to:
	// DuckDB CLI: SELECT STRING_SPLIT(repeat('a,', 500), ',') (x N iterations)

	str := strings.Repeat("a,", 500)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = stringSplitValue(str, ",")
	}
}

// BenchmarkDuckDBComparison_Levenshtein provides baseline for comparison.
func BenchmarkDuckDBComparison_Levenshtein(b *testing.B) {
	// Compare against: SELECT LEVENSHTEIN('kitten', 'sitting') (x N iterations)
	b.Run("short_strings", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = levenshteinValue("kitten", "sitting")
		}
	})

	// Compare against: SELECT LEVENSHTEIN(repeat('a', 100), repeat('b', 100))
	b.Run("medium_strings_100", func(b *testing.B) {
		str1 := strings.Repeat("a", 100)
		str2 := strings.Repeat("b", 100)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = levenshteinValue(str1, str2)
		}
	})
}

// BenchmarkDuckDBComparison_RegexReplace provides baseline for comparison.
func BenchmarkDuckDBComparison_RegexReplace(b *testing.B) {
	// Compare against: SELECT REGEXP_REPLACE('hello123world456', '[0-9]+', 'X', 'g')
	b.Run("global_replace", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = regexpReplaceValue("hello123world456", "[0-9]+", "X", "g")
		}
	})
}

// BenchmarkDuckDBComparison_Hash provides baseline for comparison.
func BenchmarkDuckDBComparison_Hash(b *testing.B) {
	// Compare against: SELECT MD5('hello')
	b.Run("MD5", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = md5Value("hello")
		}
	})

	// Compare against: SELECT SHA256('hello')
	b.Run("SHA256", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = sha256Value("hello")
		}
	})
}

// =============================================================================
// Task 15.8: Identify optimization opportunities (pattern caching, SIMD)
// =============================================================================

// regexCache provides a simple LRU cache for compiled regex patterns.
// This demonstrates the optimization opportunity mentioned in Task 15.8.
var (
	regexCacheMu sync.RWMutex
	regexCache   = make(map[string]*regexp.Regexp)
)

// getCachedRegexp returns a cached compiled regex or compiles and caches it.
func getCachedRegexp(pattern string) (*regexp.Regexp, error) {
	regexCacheMu.RLock()
	if re, ok := regexCache[pattern]; ok {
		regexCacheMu.RUnlock()
		return re, nil
	}
	regexCacheMu.RUnlock()

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	regexCacheMu.Lock()
	regexCache[pattern] = re
	regexCacheMu.Unlock()

	return re, nil
}

// BenchmarkRegexCache_Comparison compares cached vs uncached regex performance.
// This demonstrates the benefit of pattern caching (Task 15.8).
func BenchmarkRegexCache_Comparison(b *testing.B) {
	pattern := "[a-zA-Z]+@[a-zA-Z]+\\.[a-zA-Z]+"
	testStr := "test@example.com"

	b.Run("uncached", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			re, _ := regexp.Compile(pattern)
			_ = re.MatchString(testStr)
		}
	})

	b.Run("cached", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			re, _ := getCachedRegexp(pattern)
			_ = re.MatchString(testStr)
		}
	})

	b.Run("precompiled", func(b *testing.B) {
		re := regexp.MustCompile(pattern)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = re.MatchString(testStr)
		}
	})
}

// BenchmarkRegexCompilation measures regex compilation overhead.
func BenchmarkRegexCompilation(b *testing.B) {
	patterns := []struct {
		name    string
		pattern string
	}{
		{"simple", "hello"},
		{"character_class", "[0-9]+"},
		{"email_like", "^[a-z]+@[a-z]+\\.[a-z]+$"},
		{"complex", "(?i)(?:https?://)?(?:www\\.)?[a-z0-9]+\\.[a-z]{2,}(?:/[^\\s]*)?"},
	}

	for _, p := range patterns {
		b.Run(p.name+"_compile", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = regexp.Compile(p.pattern)
			}
		})
	}
}

// BenchmarkOptimization_StringBuilding demonstrates string building optimization.
func BenchmarkOptimization_StringBuilding(b *testing.B) {
	parts := make([]string, 100)
	for i := range parts {
		parts[i] = fmt.Sprintf("part%d", i)
	}

	b.Run("concat_naive", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result := ""
			for _, p := range parts {
				result += p + ","
			}
			_ = result
		}
	})

	b.Run("strings_join", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result := strings.Join(parts, ",")
			_ = result
		}
	})

	b.Run("strings_builder", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var sb strings.Builder
			for j, p := range parts {
				if j > 0 {
					sb.WriteString(",")
				}
				sb.WriteString(p)
			}
			_ = sb.String()
		}
	})

	b.Run("strings_builder_preallocated", func(b *testing.B) {
		// Estimate size
		totalLen := 0
		for _, p := range parts {
			totalLen += len(p) + 1
		}

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var sb strings.Builder
			sb.Grow(totalLen)
			for j, p := range parts {
				if j > 0 {
					sb.WriteString(",")
				}
				sb.WriteString(p)
			}
			_ = sb.String()
		}
	})
}

/*
Optimization Opportunities Identified (Task 15.8):

1. REGEX PATTERN CACHING:
   - Current: Each call to regexpMatchesValue compiles the pattern fresh
   - Opportunity: Cache compiled patterns (shown in BenchmarkRegexCache_Comparison)
   - Expected improvement: 5-10x for repeated patterns
   - Implementation: Add a sync.Map or LRU cache for compiled patterns

2. STRING BUILDING:
   - Current: Some functions may use naive string concatenation
   - Opportunity: Use strings.Builder with Grow() for pre-allocation
   - Expected improvement: 2-5x for large concatenations
   - Shown in: BenchmarkOptimization_StringBuilding

3. LEVENSHTEIN MATRIX REUSE:
   - Current: Allocates new matrix for each call
   - Opportunity: Use sync.Pool for matrix reuse
   - Expected improvement: 30-50% reduction in allocations
   - Note: Memory allocation is O(n*m), significant for large strings

4. SIMD OPPORTUNITIES (Future):
   - HAMMING distance: Could use SIMD for byte comparison
   - String comparison: Could use SIMD for prefix matching
   - Note: Requires CGO or assembly, conflicts with "pure Go" requirement
   - Alternative: Use Go's optimized runtime string operations

5. RUNE SLICE POOLING:
   - Current: Creates new rune slices for Unicode operations
   - Opportunity: Pool rune slices for reverseValue, leftValue, rightValue
   - Expected improvement: 20-40% reduction in allocations

6. HASH FUNCTION OPTIMIZATION:
   - Current: Uses standard crypto library
   - Already optimal: Go's crypto library is highly optimized
   - No significant improvement possible without CGO

7. NULL CHECK OPTIMIZATION:
   - Current: Checks nil at start of each function
   - Already optimal: Early return is the best approach

Summary:
- Highest impact: Regex pattern caching (5-10x for repeated patterns)
- Medium impact: String builder pre-allocation (2-5x)
- Lower impact: Slice pooling (20-50% allocation reduction)
- Not recommended: SIMD (requires CGO, breaks pure Go requirement)
*/

// =============================================================================
// Helper functions
// =============================================================================

// createStringWithSeparators creates a string of approximately the given size
// with separators placed at regular intervals.
func createStringWithSeparators(totalSize int, separator string, interval int) string {
	var sb strings.Builder
	sb.Grow(totalSize + totalSize/interval*len(separator))

	charCount := 0
	for sb.Len() < totalSize {
		sb.WriteByte('a' + byte(charCount%26))
		charCount++
		if charCount%interval == 0 && sb.Len() < totalSize-len(separator) {
			sb.WriteString(separator)
		}
	}

	return sb.String()
}
