# Design: Essential String Functions Implementation

## Critical Implementation Requirements

### NULL Handling

**ALL string functions must check for NULL inputs and return NULL immediately:**

```go
func exampleStringFunction(str any) (any, error) {
	// NULL check FIRST before any processing
	if str == nil {
		return nil, nil
	}

	s := toString(str)
	// ... rest of function logic
}
```

**Exception:** `CONCAT_WS` skips NULL values in concatenation but does not propagate NULL:

```go
func concatWSValue(separator any, args ...any) (any, error) {
	if separator == nil {
		return nil, nil // NULL separator returns NULL
	}

	sep := toString(separator)
	parts := make([]string, 0, len(args))

	for _, arg := range args {
		if arg == nil {
			continue // Skip NULL values, don't propagate
		}
		parts = append(parts, toString(arg))
	}

	return strings.Join(parts, sep), nil
}
```

### UTF-8 Safety

**All string operations must use rune slices for character-based operations, not byte indexing:**

**Problem:** Go's string indexing uses bytes, DuckDB uses Unicode characters.

**Bad (byte-based):**
```go
func leftValue(str any, count any) (any, error) {
	s := toString(str)
	n := int(toInt64(count))
	if n >= len(s) {
		return s, nil
	}
	return s[:n], nil // WRONG: byte index, breaks for "café"
}
```

**Good (character-based):**
```go
func leftValue(str any, count any) (any, error) {
	if str == nil {
		return nil, nil
	}

	s := toString(str)
	n := int(toInt64(count))

	runes := []rune(s) // Convert to runes FIRST
	if n < 0 {
		return "", nil
	}
	if n >= len(runes) {
		return s, nil
	}
	return string(runes[:n]), nil // Use rune slice
}
```

**Functions requiring rune conversion:**
- `leftValue`, `rightValue` - Extract N characters
- `lpadValue`, `rpadValue` - Pad to character length
- `hammingValue` - Compare character lengths
- `levenshteinValue`, `damerauLevenshteinValue` - Character-based edit distance
- All length comparisons should use `utf8.RuneCountInString()` or `len([]rune(s))`

## Implementation Details

**Note**: The code examples below illustrate core algorithm logic. All production implementations MUST:
1. Include NULL checks as documented in "NULL Handling" section above (lines 1-41)
2. Use rune-based operations for UTF-8 safety as documented in "UTF-8 Safety" section above (lines 43-88)
3. Follow DuckDB v1.4.3 exact behavior for edge cases

### 1. Regular Expression Engine

**Location**: `internal/executor/regex.go` (NEW)

```go
package executor

import (
	"regexp"
)

// DuckDB uses RE2 regex engine. Go's regexp package implements RE2.
// This provides compatibility with DuckDB's regex behavior.

// Regular Expression Functions

func regexpMatchesValue(str any, pattern any) (any, error) {
	s := toString(str)
	p := toString(pattern)

	re, err := regexp.Compile(p)
	if err != nil {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  fmt.Sprintf("Invalid regular expression: %s", err.Error()),
		}
	}

	return re.MatchString(s), nil
}

func regexpReplaceValue(str any, pattern any, replacement any, flags any) (any, error) {
	s := toString(str)
	p := toString(pattern)
	r := toString(replacement)
	f := ""
	if flags != nil {
		f = toString(flags)
	}

	re, err := regexp.Compile(p)
	if err != nil {
		return nil, &Error{
			Type: ErrorTypeExecutor,
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
	s := toString(str)
	p := toString(pattern)
	g := int64(0)
	if group != nil {
		g = toInt64(group)
	}

	re, err := regexp.Compile(p)
	if err != nil {
		return nil, &Error{
			Type: ErrorTypeExecutor,
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
	s := toString(str)
	p := toString(pattern)
	g := int64(0)
	if group != nil {
		g = toInt64(group)
	}

	re, err := regexp.Compile(p)
	if err != nil {
		return nil, &Error{
			Type: ErrorTypeExecutor,
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
	s := toString(str)
	p := toString(pattern)

	re, err := regexp.Compile(p)
	if err != nil {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  fmt.Sprintf("Invalid regular expression: %s", err.Error()),
		}
	}

	return re.Split(s, -1), nil
}
```

### 2. String Manipulation Functions

**Location**: `internal/executor/string.go` (NEW)

```go
package executor

import (
	"strings"
	"unicode"
)

// Concatenation and Splitting

func concatWSValue(separator any, args ...any) (any, error) {
	sep := toString(separator)
	parts := make([]string, 0, len(args))

	for _, arg := range args {
		if arg == nil {
			continue // Skip NULL values
		}
		parts = append(parts, toString(arg))
	}

	return strings.Join(parts, sep), nil
}

// STRING_SPLIT uses LITERAL separator (not regex pattern).
// For regex-based splitting, use REGEXP_SPLIT_TO_ARRAY.
func stringSplitValue(str any, separator any) (any, error) {
	s := toString(str)
	sep := toString(separator)

	if sep == "" {
		// Split into individual characters
		chars := make([]string, 0, len(s))
		for _, ch := range s {
			chars = append(chars, string(ch))
		}
		return chars, nil
	}

	return strings.Split(s, sep), nil
}

// Padding Functions

func lpadValue(str any, length any, fill any) (any, error) {
	s := toString(str)
	targetLen := int(toInt64(length))
	fillStr := " "
	if fill != nil {
		fillStr = toString(fill)
	}

	if len(s) >= targetLen {
		return s[:targetLen], nil
	}

	padLen := targetLen - len(s)
	if fillStr == "" {
		return s, nil
	}

	// Repeat fill string to cover padding length
	padding := strings.Repeat(fillStr, (padLen/len(fillStr))+1)
	return padding[:padLen] + s, nil
}

func rpadValue(str any, length any, fill any) (any, error) {
	s := toString(str)
	targetLen := int(toInt64(length))
	fillStr := " "
	if fill != nil {
		fillStr = toString(fill)
	}

	if len(s) >= targetLen {
		return s[:targetLen], nil
	}

	padLen := targetLen - len(s)
	if fillStr == "" {
		return s, nil
	}

	// Repeat fill string to cover padding length
	padding := strings.Repeat(fillStr, (padLen/len(fillStr))+1)
	return s + padding[:padLen], nil
}

// String Manipulation

func reverseValue(str any) (any, error) {
	s := toString(str)
	runes := []rune(s)

	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}

	return string(runes), nil
}

func repeatValue(str any, count any) (any, error) {
	s := toString(str)
	n := int(toInt64(count))

	if n < 0 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "REPEAT count must be non-negative",
		}
	}

	return strings.Repeat(s, n), nil
}

func leftValue(str any, count any) (any, error) {
	s := toString(str)
	n := int(toInt64(count))

	if n < 0 {
		return "", nil
	}
	if n >= len(s) {
		return s, nil
	}

	return s[:n], nil
}

func rightValue(str any, count any) (any, error) {
	s := toString(str)
	n := int(toInt64(count))

	if n < 0 {
		return "", nil
	}
	if n >= len(s) {
		return s, nil
	}

	return s[len(s)-n:], nil
}

// Note: SQL syntax is POSITION(substring IN string), but Go implementation
// takes parameters as positionValue(substring, str) for consistency with other functions.
func positionValue(substring any, str any) (any, error) {
	sub := toString(substring)
	s := toString(str)

	idx := strings.Index(s, sub)
	if idx == -1 {
		return int64(0), nil // SQL uses 1-based indexing, 0 means not found
	}

	return int64(idx + 1), nil // Convert to 1-based
}

func containsValue(str any, substring any) (any, error) {
	s := toString(str)
	sub := toString(substring)

	return strings.Contains(s, sub), nil
}

func prefixValue(str any, prefix any) (any, error) {
	s := toString(str)
	pre := toString(prefix)

	return strings.HasPrefix(s, pre), nil
}

func suffixValue(str any, suffix any) (any, error) {
	s := toString(str)
	suf := toString(suffix)

	return strings.HasSuffix(s, suf), nil
}

// Encoding Functions

func asciiValue(char any) (any, error) {
	s := toString(char)
	if len(s) == 0 {
		return int64(0), nil
	}

	return int64(s[0]), nil
}

func chrValue(code any) (any, error) {
	c := toInt64(code)

	if c < 0 || c > 127 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "CHR code must be in ASCII range [0, 127]",
		}
	}

	return string(rune(c)), nil
}

func unicodeValue(char any) (any, error) {
	s := toString(char)
	if len(s) == 0 {
		return int64(0), nil
	}

	// Get first rune
	runes := []rune(s)
	return int64(runes[0]), nil
}
```

### 3. Cryptographic Hash Functions

**Location**: `internal/executor/hash.go` (NEW)

```go
package executor

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"hash/fnv"
)

// Hash Functions

func md5Value(str any) (any, error) {
	s := toString(str)
	hash := md5.Sum([]byte(s))
	return hex.EncodeToString(hash[:]), nil
}

func sha256Value(str any) (any, error) {
	s := toString(str)
	hash := sha256.Sum256([]byte(s))
	return hex.EncodeToString(hash[:]), nil
}

func hashValue(str any) (any, error) {
	s := toString(str)

	// Use FNV-1a hash (64-bit) as DuckDB's default hash
	h := fnv.New64a()
	h.Write([]byte(s))

	return int64(h.Sum64()), nil
}
```

### 4. String Distance Functions

**Location**: `internal/executor/string_distance.go` (NEW)

```go
package executor

import (
	"math"
)

// Levenshtein edit distance (dynamic programming)
func levenshteinValue(str1 any, str2 any) (any, error) {
	s1 := toString(str1)
	s2 := toString(str2)

	len1 := len(s1)
	len2 := len(s2)

	// Create matrix
	matrix := make([][]int, len1+1)
	for i := range matrix {
		matrix[i] = make([]int, len2+1)
	}

	// Initialize first row and column
	for i := 0; i <= len1; i++ {
		matrix[i][0] = i
	}
	for j := 0; j <= len2; j++ {
		matrix[0][j] = j
	}

	// Fill matrix
	for i := 1; i <= len1; i++ {
		for j := 1; j <= len2; j++ {
			cost := 1
			if s1[i-1] == s2[j-1] {
				cost = 0
			}

			matrix[i][j] = min(
				min(matrix[i-1][j]+1, matrix[i][j-1]+1), // deletion, insertion
				matrix[i-1][j-1]+cost,                     // substitution
			)
		}
	}

	return int64(matrix[len1][len2]), nil
}

// Damerau-Levenshtein edit distance (with transpositions)
func damerauLevenshteinValue(str1 any, str2 any) (any, error) {
	s1 := toString(str1)
	s2 := toString(str2)

	len1 := len(s1)
	len2 := len(s2)

	// Create matrix with extra row/column for transpositions
	maxDist := len1 + len2
	matrix := make([][]int, len1+2)
	for i := range matrix {
		matrix[i] = make([]int, len2+2)
	}

	matrix[0][0] = maxDist
	for i := 0; i <= len1; i++ {
		matrix[i+1][0] = maxDist
		matrix[i+1][1] = i
	}
	for j := 0; j <= len2; j++ {
		matrix[0][j+1] = maxDist
		matrix[1][j+1] = j
	}

	// Track last occurrence of each character
	lastMatch := make(map[byte]int)

	// Fill matrix
	for i := 1; i <= len1; i++ {
		lastMatchCol := 0
		for j := 1; j <= len2; j++ {
			lastMatchRow := lastMatch[s2[j-1]]
			cost := 1
			if s1[i-1] == s2[j-1] {
				cost = 0
				lastMatchCol = j
			}

			matrix[i+1][j+1] = min(
				min(
					matrix[i][j]+cost,              // substitution
					matrix[i+1][j]+1,               // insertion
				),
				min(
					matrix[i][j+1]+1,               // deletion
					matrix[lastMatchRow][lastMatchCol]+((i-lastMatchRow-1)+1+(j-lastMatchCol-1)), // transposition
				),
			)
		}
		lastMatch[s1[i-1]] = i
	}

	return int64(matrix[len1+1][len2+1]), nil
}

// Hamming distance (only for equal-length strings)
func hammingValue(str1 any, str2 any) (any, error) {
	s1 := toString(str1)
	s2 := toString(str2)

	if len(s1) != len(s2) {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "HAMMING requires strings of equal length",
		}
	}

	distance := 0
	for i := 0; i < len(s1); i++ {
		if s1[i] != s2[i] {
			distance++
		}
	}

	return int64(distance), nil
}

// Jaccard similarity coefficient
func jaccardValue(str1 any, str2 any) (any, error) {
	s1 := toString(str1)
	s2 := toString(str2)

	// Convert to character sets
	set1 := make(map[rune]bool)
	set2 := make(map[rune]bool)

	for _, ch := range s1 {
		set1[ch] = true
	}
	for _, ch := range s2 {
		set2[ch] = true
	}

	// Calculate intersection and union
	intersection := 0
	for ch := range set1 {
		if set2[ch] {
			intersection++
		}
	}

	union := len(set1) + len(set2) - intersection

	if union == 0 {
		return float64(1.0), nil
	}

	return float64(intersection) / float64(union), nil
}

// Jaro similarity
func jaroSimilarityValue(str1 any, str2 any) (any, error) {
	s1 := toString(str1)
	s2 := toString(str2)

	if s1 == s2 {
		return float64(1.0), nil
	}

	len1 := len(s1)
	len2 := len(s2)

	if len1 == 0 || len2 == 0 {
		return float64(0.0), nil
	}

	matchDistance := max(len1, len2)/2 - 1
	if matchDistance < 1 {
		matchDistance = 1
	}

	s1Matches := make([]bool, len1)
	s2Matches := make([]bool, len2)

	matches := 0
	transpositions := 0

	// Find matches
	for i := 0; i < len1; i++ {
		start := max(0, i-matchDistance)
		end := min(i+matchDistance+1, len2)

		for j := start; j < end; j++ {
			if s2Matches[j] || s1[i] != s2[j] {
				continue
			}
			s1Matches[i] = true
			s2Matches[j] = true
			matches++
			break
		}
	}

	if matches == 0 {
		return float64(0.0), nil
	}

	// Count transpositions
	k := 0
	for i := 0; i < len1; i++ {
		if !s1Matches[i] {
			continue
		}
		for !s2Matches[k] {
			k++
		}
		if s1[i] != s2[k] {
			transpositions++
		}
		k++
	}

	similarity := (float64(matches)/float64(len1) +
		float64(matches)/float64(len2) +
		(float64(matches)-float64(transpositions)/2.0)/float64(matches)) / 3.0

	return similarity, nil
}

// Jaro-Winkler similarity (with prefix bonus)
func jaroWinklerSimilarityValue(str1 any, str2 any) (any, error) {
	jaroSim, err := jaroSimilarityValue(str1, str2)
	if err != nil {
		return nil, err
	}

	jaro := jaroSim.(float64)

	s1 := toString(str1)
	s2 := toString(str2)

	// Find common prefix length (up to 4 chars)
	prefixLen := 0
	maxPrefix := min(min(len(s1), len(s2)), 4)
	for i := 0; i < maxPrefix; i++ {
		if s1[i] == s2[i] {
			prefixLen++
		} else {
			break
		}
	}

	// Apply Winkler modification
	p := 0.1 // scaling factor
	similarity := jaro + float64(prefixLen)*p*(1.0-jaro)

	return similarity, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
```

### 5. Expression Evaluator Integration

**Location**: `internal/executor/expr.go`

```go
// Add to evaluateFunction switch statement

// Regular Expression Functions
case "REGEXP_MATCHES":
	if len(args) != 2 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "REGEXP_MATCHES requires 2 arguments",
		}
	}
	return regexpMatchesValue(args[0], args[1])

case "REGEXP_REPLACE":
	if len(args) < 3 || len(args) > 4 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "REGEXP_REPLACE requires 3 or 4 arguments",
		}
	}
	flags := interface{}(nil)
	if len(args) == 4 {
		flags = args[3]
	}
	return regexpReplaceValue(args[0], args[1], args[2], flags)

case "REGEXP_EXTRACT":
	if len(args) < 2 || len(args) > 3 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "REGEXP_EXTRACT requires 2 or 3 arguments",
		}
	}
	group := interface{}(nil)
	if len(args) == 3 {
		group = args[2]
	}
	return regexpExtractValue(args[0], args[1], group)

case "REGEXP_EXTRACT_ALL":
	if len(args) < 2 || len(args) > 3 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "REGEXP_EXTRACT_ALL requires 2 or 3 arguments",
		}
	}
	group := interface{}(nil)
	if len(args) == 3 {
		group = args[2]
	}
	return regexpExtractAllValue(args[0], args[1], group)

case "REGEXP_SPLIT_TO_ARRAY":
	if len(args) != 2 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "REGEXP_SPLIT_TO_ARRAY requires 2 arguments",
		}
	}
	return regexpSplitToArrayValue(args[0], args[1])

// Concatenation and Splitting
case "CONCAT_WS":
	if len(args) < 2 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "CONCAT_WS requires at least 2 arguments",
		}
	}
	return concatWSValue(args[0], args[1:]...)

case "STRING_SPLIT":
	if len(args) != 2 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "STRING_SPLIT requires 2 arguments",
		}
	}
	return stringSplitValue(args[0], args[1])

case "STRING_SPLIT_REGEX":
	if len(args) != 2 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "STRING_SPLIT_REGEX requires 2 arguments",
		}
	}
	return regexpSplitToArrayValue(args[0], args[1])

// Padding Functions
case "LPAD":
	if len(args) < 2 || len(args) > 3 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "LPAD requires 2 or 3 arguments",
		}
	}
	fill := interface{}(nil)
	if len(args) == 3 {
		fill = args[2]
	}
	return lpadValue(args[0], args[1], fill)

case "RPAD":
	if len(args) < 2 || len(args) > 3 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "RPAD requires 2 or 3 arguments",
		}
	}
	fill := interface{}(nil)
	if len(args) == 3 {
		fill = args[2]
	}
	return rpadValue(args[0], args[1], fill)

// String Manipulation
case "REVERSE":
	if len(args) != 1 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "REVERSE requires 1 argument",
		}
	}
	return reverseValue(args[0])

case "REPEAT":
	if len(args) != 2 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "REPEAT requires 2 arguments",
		}
	}
	return repeatValue(args[0], args[1])

case "LEFT":
	if len(args) != 2 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "LEFT requires 2 arguments",
		}
	}
	return leftValue(args[0], args[1])

case "RIGHT":
	if len(args) != 2 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "RIGHT requires 2 arguments",
		}
	}
	return rightValue(args[0], args[1])

case "POSITION":
	if len(args) != 2 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "POSITION requires 2 arguments",
		}
	}
	return positionValue(args[0], args[1])

case "STRPOS", "INSTR":
	if len(args) != 2 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "STRPOS requires 2 arguments",
		}
	}
	return positionValue(args[1], args[0]) // Note: reversed argument order

case "CONTAINS":
	if len(args) != 2 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "CONTAINS requires 2 arguments",
		}
	}
	return containsValue(args[0], args[1])

case "PREFIX", "STARTS_WITH":
	if len(args) != 2 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "PREFIX requires 2 arguments",
		}
	}
	return prefixValue(args[0], args[1])

case "SUFFIX", "ENDS_WITH":
	if len(args) != 2 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "SUFFIX requires 2 arguments",
		}
	}
	return suffixValue(args[0], args[1])

// Encoding Functions
case "ASCII":
	if len(args) != 1 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "ASCII requires 1 argument",
		}
	}
	return asciiValue(args[0])

case "CHR":
	if len(args) != 1 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "CHR requires 1 argument",
		}
	}
	return chrValue(args[0])

case "UNICODE":
	if len(args) != 1 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "UNICODE requires 1 argument",
		}
	}
	return unicodeValue(args[0])

// Hash Functions
case "MD5":
	if len(args) != 1 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "MD5 requires 1 argument",
		}
	}
	return md5Value(args[0])

case "SHA256":
	if len(args) != 1 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "SHA256 requires 1 argument",
		}
	}
	return sha256Value(args[0])

case "HASH":
	if len(args) != 1 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "HASH requires 1 argument",
		}
	}
	return hashValue(args[0])

// String Distance Functions
case "LEVENSHTEIN":
	if len(args) != 2 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "LEVENSHTEIN requires 2 arguments",
		}
	}
	return levenshteinValue(args[0], args[1])

case "DAMERAU_LEVENSHTEIN":
	if len(args) != 2 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "DAMERAU_LEVENSHTEIN requires 2 arguments",
		}
	}
	return damerauLevenshteinValue(args[0], args[1])

case "HAMMING":
	if len(args) != 2 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "HAMMING requires 2 arguments",
		}
	}
	return hammingValue(args[0], args[1])

case "JACCARD":
	if len(args) != 2 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "JACCARD requires 2 arguments",
		}
	}
	return jaccardValue(args[0], args[1])

case "JARO_SIMILARITY":
	if len(args) != 2 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "JARO_SIMILARITY requires 2 arguments",
		}
	}
	return jaroSimilarityValue(args[0], args[1])

case "JARO_WINKLER_SIMILARITY":
	if len(args) != 2 {
		return nil, &Error{
			Type: ErrorTypeExecutor,
			Msg:  "JARO_WINKLER_SIMILARITY requires 2 arguments",
		}
	}
	return jaroWinklerSimilarityValue(args[0], args[1])

// Aliases for existing functions
case "STRIP":
	return evalTrim(args) // Alias for TRIM

case "LSTRIP":
	return evalLTrim(args) // Alias for LTRIM

case "RSTRIP":
	return evalRTrim(args) // Alias for RTRIM
```

### 6. Type Inference

**Location**: `internal/binder/type_inference.go`

```go
// String functions return VARCHAR except where noted

func inferStringFunctionType(fnName string, argTypes []Type) Type {
	switch fnName {
	// Boolean-returning functions
	case "REGEXP_MATCHES", "CONTAINS", "PREFIX", "SUFFIX", "STARTS_WITH", "ENDS_WITH":
		return TypeBoolean

	// Integer-returning functions
	case "POSITION", "STRPOS", "INSTR", "ASCII", "UNICODE", "LEVENSHTEIN", "HAMMING":
		return TypeBigint

	// Array-returning functions
	case "REGEXP_EXTRACT_ALL", "REGEXP_SPLIT_TO_ARRAY", "STRING_SPLIT", "STRING_SPLIT_REGEX":
		return TypeList // Array of VARCHAR

	// Float-returning functions
	case "JACCARD", "JARO_SIMILARITY", "JARO_WINKLER_SIMILARITY":
		return TypeDouble

	// Hash functions return signed BIGINT
	case "HASH":
		return TypeBigint

	// Hash functions that return hex strings
	case "MD5", "SHA1", "SHA256", "SHA384", "SHA512":
		return TypeVarchar

	default:
		// Most string functions return VARCHAR
		return TypeVarchar
	}
}
```

### 7. NULL Handling

All string functions follow DuckDB's NULL propagation rule:
- If any input is NULL → output is NULL
- Exception: CONCAT_WS skips NULL values in concatenation

```go
// Add to each string function (except CONCAT_WS)

if value == nil {
	return nil, nil  // NULL in → NULL out
}
```

## Context

**Problem**: Users cannot perform basic text processing operations in SQL queries without workarounds.

**Constraints**:
- Must maintain pure Go (no cgo)
- Must match DuckDB v1.4.3 behavior exactly
- Must use Go's regexp package (implements RE2, matching DuckDB)
- Performance-critical for text analytics queries

**Stakeholders**:
- Data analysts performing log parsing and ETL
- Application developers handling text data
- Users migrating from DuckDB CLI
- Security teams using hash functions for data integrity

## Goals / Non-Goals

**Goals**:
- ✅ Full parity with DuckDB v1.4.3 string functions (50+ functions)
- ✅ RE2-compatible regex matching (Go's regexp package)
- ✅ Cryptographic hash functions (MD5, SHA family)
- ✅ String distance/similarity functions
- ✅ NULL propagation compatibility
- ✅ Type inference matching DuckDB behavior

**Non-Goals**:
- ❌ PCRE regex support (DuckDB uses RE2, not PCRE)
- ❌ Locale-specific case conversion (use Unicode-aware case folding)
- ❌ Custom hash functions beyond standard library
- ❌ Performance optimization beyond Go stdlib (defer to future)

## Decisions

### Decision 1: Regex Engine Choice
**Choice**: Use Go's `regexp` package (RE2 implementation)
**Rationale**:
- DuckDB uses RE2 regex engine
- Go's regexp package implements RE2 syntax
- No external dependencies required
- Performance is good for most use cases

**Alternatives Considered**:
- External RE2 library: Requires cgo (violates pure Go constraint)
- PCRE library: Different syntax than DuckDB (incompatible)

### Decision 2: Hash Function Implementation
**Choice**: Use Go's `crypto/*` packages for cryptographic hashes
**Rationale**:
- Standard library provides MD5, SHA1, SHA256, SHA384, SHA512
- Well-tested, secure implementations
- No external dependencies
- Matches DuckDB output format (hex-encoded strings)

**Alternatives Considered**:
- Custom hash implementations: Reinventing the wheel, error-prone
- Third-party libraries: Unnecessary dependencies

### Decision 3: String Distance Algorithms
**Choice**: Implement Levenshtein, Hamming, Jaccard, Jaro, Jaro-Winkler from scratch
**Rationale**:
- Algorithms are well-defined and straightforward
- Pure Go implementations exist as reference
- No suitable stdlib or lightweight library
- Performance is acceptable for typical use cases

**Alternatives Considered**:
- External libraries: Most are abandoned or heavyweight
- Skip these functions: Reduces DuckDB compatibility

### Decision 4: REGEXP_REPLACE Default Behavior
**Choice**: Replace first match only by default, use 'g' flag for global replacement
**Rationale**:
- Matches DuckDB v1.4.3 behavior exactly
- Aligns with common regex replacement semantics
- Allows fine-grained control via flags parameter

**Alternatives Considered**:
- Always replace all matches: Inconsistent with DuckDB
- Require explicit flag always: More verbose API

## Risks / Trade-offs

### Risk 1: Regex Pattern Incompatibilities
**Risk**: Some edge-case regex patterns may behave slightly differently between Go's RE2 and DuckDB's RE2
**Mitigation**:
- Comprehensive test suite comparing outputs
- Document any known pattern differences
- Accept differences if both are valid RE2 behavior

### Risk 2: String Distance Performance
**Risk**: Levenshtein distance is O(n*m) and can be slow for large strings
**Mitigation**:
- Use standard dynamic programming algorithm (already optimized)
- Document performance characteristics
- Defer advanced optimizations (bounded edit distance) to future work

### Trade-off: Simplicity vs Feature Completeness
**Choice**: Implement basic FORMAT function with {} placeholders, not full printf-style formatting initially
**Rationale**:
- Get core functionality working first
- Full printf formatting is complex and error-prone
- Can enhance later if users need it
- Most use cases covered by simple {} placeholders

## Migration Plan

**No migration needed** - this is additive functionality.

**Compatibility**:
- Existing queries work unchanged
- New string functions are opt-in
- No breaking changes to existing behavior

**Rollout**:
1. Phase 1: Regex functions (REGEXP_MATCHES, REGEXP_REPLACE, REGEXP_EXTRACT, REGEXP_EXTRACT_ALL, REGEXP_SPLIT_TO_ARRAY)
2. Phase 2: String manipulation (CONCAT_WS, STRING_SPLIT, LPAD, RPAD, CENTER, REVERSE, REPEAT, LEFT, RIGHT)
3. Phase 3: Hash functions (MD5, SHA1, SHA256, SHA384, SHA512, HASH)
4. Phase 4: String distance and similarity functions
5. Phase 5: Encoding and formatting functions

## Open Questions

1. **Q**: Should REGEXP_REPLACE support all RE2 flags or just 'g' (global)?
   **A**: Start with 'g' flag only, add more flags if users request them

2. **Q**: How to handle invalid UTF-8 in string functions?
   **A**: Follow Go's default behavior (replacement character U+FFFD), match DuckDB

3. **Q**: Should LEVENSHTEIN have a maximum string length limit to prevent performance issues?
   **A**: No hard limit initially, document O(n*m) complexity, consider adding limit if abuse occurs

4. **Q**: FORMAT function: full printf-style or simple {} placeholders?
   **A**: Start with simple {} placeholders (indexed and sequential), enhance later if needed

## Performance Considerations

**Benchmarks to Add**:
- REGEXP_MATCHES on various pattern complexities
- REGEXP_REPLACE with and without 'g' flag
- LEVENSHTEIN on strings of varying lengths (10, 100, 1000 chars)
- Hash functions (MD5, SHA256, SHA512) throughput
- STRING_SPLIT on large strings with many delimiters

**Expected Performance**:
- Regex functions: ~1-50µs per operation (depends on pattern complexity)
- Hash functions: ~1-10µs per operation (depends on string length)
- String manipulation: ~100-500ns per operation (simple operations)
- LEVENSHTEIN: ~10µs for 100-char strings, ~1ms for 1000-char strings

**Optimization Opportunities** (future):
- Regex pattern caching (compile once, use many times)
- Vectorized string operations for DataChunk processing
- Bounded Levenshtein distance (early termination)
- SIMD acceleration for hash functions
