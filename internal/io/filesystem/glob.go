// Package filesystem provides a pluggable filesystem interface for cloud and local storage.
package filesystem

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"
)

// Constants used throughout the glob matching.
const (
	pathSeparator       = "/"
	currentDir          = "."
	recursiveWildcard   = "**"
	hiddenFilePrefix    = "."
)

// ErrInvalidGlobPattern is returned when a glob pattern is invalid.
var ErrInvalidGlobPattern = errors.New("invalid glob pattern")

// ErrMultipleRecursiveWildcards is returned when a pattern contains multiple ** wildcards.
var ErrMultipleRecursiveWildcards = errors.New("pattern cannot contain multiple '**' wildcards")

// ErrNoFilesMatch is returned when no files match a glob pattern.
var ErrNoFilesMatch = errors.New("no files match pattern")

// GlobMatcher handles pattern matching and file expansion.
type GlobMatcher struct {
	fs FileSystem
}

// NewGlobMatcher creates a new GlobMatcher with the given filesystem.
func NewGlobMatcher(fs FileSystem) *GlobMatcher {
	return &GlobMatcher{fs: fs}
}

// Match expands a glob pattern into a list of file paths.
// Returns files sorted alphabetically by default.
func (gm *GlobMatcher) Match(pattern string) ([]string, error) {
	// Validate the pattern
	if err := ValidateGlobPattern(pattern); err != nil {
		return nil, err
	}

	// Normalize path separators
	pattern = normalizePath(pattern)

	// If no glob characters, return the pattern as-is if it exists
	if !ContainsGlobPattern(pattern) {
		exists, err := gm.fs.Exists(pattern)
		if err != nil {
			return nil, fmt.Errorf("checking path existence: %w", err)
		}
		if exists {
			return []string{pattern}, nil
		}
		return nil, nil
	}

	// Parse pattern into segments
	segments := ParsePatternSegments(pattern)

	// Find matches using recursive expansion
	matches, err := gm.expandPattern(segments, "")
	if err != nil {
		return nil, err
	}

	// Sort results alphabetically
	sort.Strings(matches)

	return matches, nil
}

// MatchMultiple handles an array of patterns or files.
// Each pattern can contain glob wildcards. Results are combined and deduplicated.
func (gm *GlobMatcher) MatchMultiple(patterns []string) ([]string, error) {
	seen := make(map[string]struct{})
	var results []string

	for _, pattern := range patterns {
		matches, err := gm.Match(pattern)
		if err != nil {
			return nil, fmt.Errorf("pattern %q: %w", pattern, err)
		}

		for _, match := range matches {
			if _, ok := seen[match]; !ok {
				seen[match] = struct{}{}
				results = append(results, match)
			}
		}
	}

	// Sort results alphabetically
	sort.Strings(results)

	return results, nil
}

// expandPattern recursively expands pattern segments into matching paths.
func (gm *GlobMatcher) expandPattern(segments []PatternSegment, basePath string) ([]string, error) {
	if len(segments) == 0 {
		// Base case: no more segments, check if basePath is a file
		if basePath == "" {
			return nil, nil
		}
		info, err := gm.fs.Stat(basePath)
		if err != nil {
			return nil, nil // Path does not exist
		}
		if !info.IsDir() {
			return []string{basePath}, nil
		}
		return nil, nil
	}

	segment := segments[0]
	remaining := segments[1:]

	switch segment.Type {
	case SegmentLiteral:
		// Navigate to literal directory/file
		newPath := joinPath(basePath, segment.Value)
		return gm.expandPattern(remaining, newPath)

	case SegmentWildcard:
		// Expand single-level wildcard
		return gm.expandWildcard(segment.Value, basePath, remaining)

	case SegmentRecursive:
		// Expand recursive wildcard **
		return gm.expandRecursive(basePath, remaining)

	default:
		return nil, fmt.Errorf("unknown segment type: %d", segment.Type)
	}
}

// expandWildcard expands a wildcard pattern (* or ? or [...]) at the current directory level.
func (gm *GlobMatcher) expandWildcard(pattern, basePath string, remaining []PatternSegment) ([]string, error) {
	dirPath := basePath
	if dirPath == "" {
		dirPath = currentDir
	}

	entries, err := gm.fs.ReadDir(dirPath)
	if err != nil {
		return nil, nil // Directory does not exist or cannot be read
	}

	var results []string

	for _, entry := range entries {
		name := entry.Name()

		// Skip hidden files unless pattern explicitly matches them
		if strings.HasPrefix(name, hiddenFilePrefix) && !strings.HasPrefix(pattern, hiddenFilePrefix) {
			continue
		}

		if matchSegment(pattern, name) {
			newPath := joinPath(basePath, name)

			if len(remaining) == 0 {
				// No more segments - this is a final match
				info, err := entry.Info()
				if err != nil {
					continue
				}
				if !info.IsDir() {
					results = append(results, newPath)
				}
			} else {
				// More segments to process
				matches, err := gm.expandPattern(remaining, newPath)
				if err != nil {
					return nil, err
				}
				results = append(results, matches...)
			}
		}
	}

	return results, nil
}

// expandRecursive expands ** to match zero or more directories.
func (gm *GlobMatcher) expandRecursive(basePath string, remaining []PatternSegment) ([]string, error) {
	var results []string

	// First, try matching with zero directories (** matches nothing)
	matches, err := gm.expandPattern(remaining, basePath)
	if err != nil {
		return nil, err
	}
	results = append(results, matches...)

	// Then, recursively walk directories
	dirPath := basePath
	if dirPath == "" {
		dirPath = currentDir
	}

	err = gm.walkRecursive(dirPath, basePath, remaining, &results)
	if err != nil {
		return nil, err
	}

	return results, nil
}

// walkRecursive walks a directory tree recursively and expands remaining pattern segments.
func (gm *GlobMatcher) walkRecursive(
	dirPath, basePath string,
	remaining []PatternSegment,
	results *[]string,
) error {
	entries, err := gm.fs.ReadDir(dirPath)
	if err != nil {
		return nil // Directory cannot be read, skip silently
	}

	for _, entry := range entries {
		name := entry.Name()

		// Skip hidden directories
		if strings.HasPrefix(name, hiddenFilePrefix) {
			continue
		}

		newPath := joinPath(basePath, name)

		if entry.IsDir() {
			// Check for symlinks to avoid infinite loops
			info, err := entry.Info()
			if err != nil {
				continue
			}
			if info.Mode()&ModeSymlink != 0 {
				continue // Skip symlinks to avoid infinite loops
			}

			// Try matching remaining pattern from this directory
			matches, err := gm.expandPattern(remaining, newPath)
			if err != nil {
				return err
			}
			*results = append(*results, matches...)

			// Continue walking deeper
			err = gm.walkRecursive(joinPath(dirPath, name), newPath, remaining, results)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// SegmentType represents the type of a pattern segment.
type SegmentType int

const (
	// SegmentLiteral is a literal path segment with no wildcards.
	SegmentLiteral SegmentType = iota
	// SegmentWildcard is a segment with *, ?, or [...] wildcards.
	SegmentWildcard
	// SegmentRecursive is the ** recursive wildcard.
	SegmentRecursive
)

// PatternSegment represents a parsed segment of a glob pattern.
type PatternSegment struct {
	Type  SegmentType
	Value string
}

// ParsePatternSegments parses a glob pattern into segments.
// For absolute paths (starting with /), the first segment's Value will include the root.
func ParsePatternSegments(pattern string) []PatternSegment {
	parts := strings.Split(pattern, pathSeparator)
	segments := make([]PatternSegment, 0, len(parts))

	// Track if this is an absolute path
	isAbsolute := strings.HasPrefix(pattern, pathSeparator)
	firstNonEmpty := true

	for _, part := range parts {
		if part == "" {
			continue
		}

		// For the first segment of an absolute path, prepend /
		value := part
		if isAbsolute && firstNonEmpty {
			value = pathSeparator + part
			firstNonEmpty = false
		} else {
			firstNonEmpty = false
		}

		if part == recursiveWildcard {
			segments = append(segments, PatternSegment{
				Type:  SegmentRecursive,
				Value: part,
			})
		} else if containsWildcardChars(part) {
			segments = append(segments, PatternSegment{
				Type:  SegmentWildcard,
				Value: part,
			})
		} else {
			segments = append(segments, PatternSegment{
				Type:  SegmentLiteral,
				Value: value,
			})
		}
	}

	return segments
}

// containsWildcardChars checks if a string contains unescaped wildcard characters.
func containsWildcardChars(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++ // Skip escaped character
			continue
		}
		if s[i] == '*' || s[i] == '?' || s[i] == '[' {
			return true
		}
	}
	return false
}

// ContainsGlobPattern detects if a string contains glob wildcards.
// Returns true if the string contains *, **, ?, or [...] (unescaped).
func ContainsGlobPattern(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++ // Skip escaped character
			continue
		}
		switch s[i] {
		case '*', '?', '[':
			return true
		}
	}
	return false
}

// ValidateGlobPattern validates a glob pattern.
// Returns an error if the pattern is invalid (e.g., multiple ** wildcards).
func ValidateGlobPattern(pattern string) error {
	if pattern == "" {
		return fmt.Errorf("%w: empty pattern", ErrInvalidGlobPattern)
	}

	// Check for multiple ** wildcards
	segments := strings.Split(pattern, pathSeparator)
	recursiveCount := 0
	for _, seg := range segments {
		if seg == recursiveWildcard {
			recursiveCount++
			if recursiveCount > 1 {
				return ErrMultipleRecursiveWildcards
			}
		}
	}

	// Validate bracket expressions
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == '\\' && i+1 < len(pattern) {
			i++ // Skip escaped character
			continue
		}
		if pattern[i] == '[' {
			// Find closing bracket
			j := i + 1
			if j < len(pattern) && pattern[j] == '!' {
				j++
			}
			if j < len(pattern) && pattern[j] == ']' {
				j++
			}
			for j < len(pattern) && pattern[j] != ']' {
				if pattern[j] == '\\' && j+1 < len(pattern) {
					j++
				}
				j++
			}
			if j >= len(pattern) {
				return fmt.Errorf("%w: unclosed bracket expression", ErrInvalidGlobPattern)
			}
			i = j
		}
	}

	return nil
}

// ExtractPrefix extracts the literal prefix before any wildcards.
// This is useful for cloud storage optimization (e.g., S3 prefix filtering).
// Example: "data/2024/**/*.parquet" -> "data/2024/"
func ExtractPrefix(pattern string) string {
	pattern = normalizePath(pattern)
	segments := strings.Split(pattern, pathSeparator)

	var prefix strings.Builder
	for _, seg := range segments {
		if containsWildcardChars(seg) || seg == recursiveWildcard {
			break
		}
		if prefix.Len() > 0 {
			prefix.WriteString(pathSeparator)
		}
		prefix.WriteString(seg)
	}

	result := prefix.String()
	if result != "" && !strings.HasSuffix(result, pathSeparator) {
		result += pathSeparator
	}

	return result
}

// matchSegment matches a filename against a glob pattern segment.
// Supports *, ?, [abc], [a-z], [!abc], and escape sequences.
func matchSegment(pattern, name string) bool {
	return matchGlob(pattern, name)
}

// matchGlob matches a string against a glob pattern.
// Pattern syntax:
//   - * matches any sequence of non-separator characters
//   - ? matches exactly one character
//   - [abc] matches one character from set
//   - [a-z] matches one character from range
//   - [!abc] negates character class
//   - \ escapes the next character
func matchGlob(pattern, s string) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		case '*':
			// Skip consecutive asterisks
			for len(pattern) > 0 && pattern[0] == '*' {
				pattern = pattern[1:]
			}
			// * at end matches everything
			if len(pattern) == 0 {
				return true
			}
			// Try matching rest of pattern at each position
			for i := 0; i <= len(s); i++ {
				if matchGlob(pattern, s[i:]) {
					return true
				}
			}
			return false

		case '?':
			// ? must match exactly one character
			if len(s) == 0 {
				return false
			}
			_, size := utf8.DecodeRuneInString(s)
			s = s[size:]
			pattern = pattern[1:]

		case '[':
			// Character class
			if len(s) == 0 {
				return false
			}
			c, size := utf8.DecodeRuneInString(s)
			s = s[size:]

			matched, newPattern, ok := matchCharClass(pattern, c)
			if !ok {
				return false
			}
			if !matched {
				return false
			}
			pattern = newPattern

		case '\\':
			// Escape sequence
			if len(pattern) > 1 {
				pattern = pattern[1:]
			}
			if len(s) == 0 || s[0] != pattern[0] {
				return false
			}
			s = s[1:]
			pattern = pattern[1:]

		default:
			// Literal character match
			if len(s) == 0 {
				return false
			}
			pc, pSize := utf8.DecodeRuneInString(pattern)
			sc, sSize := utf8.DecodeRuneInString(s)
			if pc != sc {
				return false
			}
			pattern = pattern[pSize:]
			s = s[sSize:]
		}
	}

	return len(s) == 0
}

// matchCharClass matches a character against a character class pattern.
// Returns (matched, remaining pattern, ok).
// ok is false if the pattern is malformed.
func matchCharClass(pattern string, c rune) (matched bool, rest string, ok bool) {
	if len(pattern) == 0 || pattern[0] != '[' {
		return false, pattern, false
	}
	pattern = pattern[1:]

	// Check for negation
	negated := false
	if len(pattern) > 0 && pattern[0] == '!' {
		negated = true
		pattern = pattern[1:]
	}

	// Empty class is invalid, but handle special case []] and [!]]
	if len(pattern) > 0 && pattern[0] == ']' {
		// ] at start is literal
		if c == ']' {
			matched = true
		}
		pattern = pattern[1:]
	}

	// Process character class
	for len(pattern) > 0 && pattern[0] != ']' {
		if pattern[0] == '\\' && len(pattern) > 1 {
			// Escaped character
			pattern = pattern[1:]
			if rune(pattern[0]) == c {
				matched = true
			}
			pattern = pattern[1:]
			continue
		}

		// Check for range (a-z)
		if len(pattern) >= 3 && pattern[1] == '-' && pattern[2] != ']' {
			lo := rune(pattern[0])
			hi := rune(pattern[2])
			if lo <= c && c <= hi {
				matched = true
			}
			pattern = pattern[3:]
			continue
		}

		// Single character
		if rune(pattern[0]) == c {
			matched = true
		}
		pattern = pattern[1:]
	}

	// Must end with ]
	if len(pattern) == 0 || pattern[0] != ']' {
		return false, pattern, false
	}
	pattern = pattern[1:]

	if negated {
		matched = !matched
	}

	return matched, pattern, true
}

// normalizePath normalizes path separators to forward slashes.
func normalizePath(path string) string {
	return filepath.ToSlash(path)
}

// joinPath joins path components with forward slashes.
func joinPath(base, elem string) string {
	if base == "" {
		return elem
	}
	return base + pathSeparator + elem
}

// MatchPattern checks if a path matches a glob pattern.
// This is a standalone function for use without a filesystem.
func MatchPattern(pattern, path string) (bool, error) {
	if err := ValidateGlobPattern(pattern); err != nil {
		return false, err
	}

	pattern = normalizePath(pattern)
	path = normalizePath(path)

	patternParts := strings.Split(pattern, pathSeparator)
	pathParts := strings.Split(path, pathSeparator)

	return matchParts(patternParts, pathParts), nil
}

// matchParts matches path parts against pattern parts recursively.
func matchParts(patternParts, pathParts []string) bool {
	pi := 0 // pattern index
	si := 0 // path index

	for pi < len(patternParts) {
		if patternParts[pi] == recursiveWildcard {
			// ** matches zero or more path segments
			pi++

			// ** at end matches everything
			if pi >= len(patternParts) {
				return true
			}

			// Try matching remaining pattern at each position
			for si <= len(pathParts) {
				if matchParts(patternParts[pi:], pathParts[si:]) {
					return true
				}
				si++
			}
			return false
		}

		// Non-** segment must match
		if si >= len(pathParts) {
			return false
		}

		if !matchGlob(patternParts[pi], pathParts[si]) {
			return false
		}

		pi++
		si++
	}

	return si == len(pathParts)
}
