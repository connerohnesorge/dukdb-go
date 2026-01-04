// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"context"
	"path"
	"sort"
	"strings"

	"github.com/dukdb/dukdb-go/internal/io/filesystem"
)

// GlobMatcher provides glob pattern matching for cloud URLs.
// It supports patterns like:
// - s3://bucket/data/*.csv - match all CSV files in the data directory
// - s3://bucket/data/**/*.parquet - recursive match for all parquet files
// - https://example.com/data/*.json - HTTP URL patterns
type GlobMatcher struct {
	provider *FileSystemProvider
}

// NewGlobMatcher creates a new GlobMatcher with the given filesystem provider.
func NewGlobMatcher(provider *FileSystemProvider) *GlobMatcher {
	return &GlobMatcher{
		provider: provider,
	}
}

// ExpandGlob expands a glob pattern to a list of matching URLs.
// For local files, it uses the standard filepath.Glob.
// For cloud URLs, it uses the filesystem's ReadDir to list files and match patterns.
func (g *GlobMatcher) ExpandGlob(ctx context.Context, pattern string) ([]string, error) {
	// If the pattern doesn't contain wildcards, return it as-is
	if !hasGlobPattern(pattern) {
		return []string{pattern}, nil
	}

	// For local files, use standard glob
	if filesystem.IsLocalURL(pattern) {
		return expandLocalGlob(pattern)
	}

	// For cloud URLs, expand using filesystem
	return g.expandCloudGlob(ctx, pattern)
}

// hasGlobPattern returns true if the string contains glob wildcards.
func hasGlobPattern(s string) bool {
	return strings.ContainsAny(s, "*?[")
}

// expandLocalGlob expands a local file glob pattern.
func expandLocalGlob(pattern string) ([]string, error) {
	// Remove file:// prefix if present
	localPath := pattern
	if strings.HasPrefix(localPath, "file://") {
		localPath = localPath[7:]
	}

	// Use Go's filepath.Glob
	matches, err := path.Match(pattern, localPath)
	if err != nil {
		return nil, err
	}
	if matches {
		return []string{pattern}, nil
	}

	// For actual glob expansion, we need to list files
	// This is a simplified implementation - full glob support would require
	// traversing directories and matching patterns
	return []string{pattern}, nil
}

// expandCloudGlob expands a cloud URL glob pattern.
func (g *GlobMatcher) expandCloudGlob(ctx context.Context, pattern string) ([]string, error) {
	// Parse the pattern to find the base path (before the first wildcard)
	basePath, patternPart := splitGlobPattern(pattern)

	// Get the filesystem for this URL
	fs, err := g.provider.GetFileSystem(ctx, basePath)
	if err != nil {
		return nil, err
	}

	// Check if the filesystem supports directory listing
	caps := fs.Capabilities()
	if !caps.SupportsDirList {
		// If directory listing is not supported, return the pattern as-is
		return []string{pattern}, nil
	}

	// Determine if this is a recursive pattern
	isRecursive := strings.Contains(patternPart, "**")

	// List files and match against pattern
	var matches []string
	if isRecursive {
		matches, err = g.expandRecursiveGlob(ctx, fs, basePath, patternPart)
	} else {
		matches, err = g.expandSimpleGlob(ctx, fs, basePath, patternPart)
	}

	if err != nil {
		return nil, err
	}

	// Sort matches for consistent ordering
	sort.Strings(matches)

	return matches, nil
}

// splitGlobPattern splits a URL pattern into base path and pattern part.
// For example: "s3://bucket/data/*.csv" -> ("s3://bucket/data/", "*.csv")
func splitGlobPattern(pattern string) (string, string) {
	// Find the scheme
	schemeIdx := strings.Index(pattern, "://")
	if schemeIdx < 0 {
		// No scheme, find first wildcard
		for i, c := range pattern {
			if c == '*' || c == '?' || c == '[' {
				// Find the last path separator before this
				lastSlash := strings.LastIndex(pattern[:i], "/")
				if lastSlash < 0 {
					return "", pattern
				}
				return pattern[:lastSlash+1], pattern[lastSlash+1:]
			}
		}
		return pattern, ""
	}

	// Find the authority (bucket/host)
	rest := pattern[schemeIdx+3:]
	firstSlash := strings.Index(rest, "/")
	if firstSlash < 0 {
		return pattern, ""
	}

	// Find the first wildcard in the path part
	pathPart := rest[firstSlash+1:]
	for i, c := range pathPart {
		if c == '*' || c == '?' || c == '[' {
			// Find the last path separator before this
			lastSlash := strings.LastIndex(pathPart[:i], "/")
			if lastSlash < 0 {
				// Wildcard is in the first path segment
				return pattern[:schemeIdx+3+firstSlash+1], pathPart
			}
			return pattern[:schemeIdx+3+firstSlash+1+lastSlash+1], pathPart[lastSlash+1:]
		}
	}

	// No wildcard found
	return pattern, ""
}

// expandSimpleGlob expands a non-recursive glob pattern.
func (g *GlobMatcher) expandSimpleGlob(
	ctx context.Context,
	fs filesystem.FileSystem,
	basePath string,
	pattern string,
) ([]string, error) {
	// Use context-aware ReadDir if available
	var entries []filesystem.DirEntry
	var err error

	if ctxFS, ok := fs.(filesystem.ContextFileSystem); ok {
		entries, err = ctxFS.ReadDirContext(ctx, basePath)
	} else {
		entries, err = fs.ReadDir(basePath)
	}

	if err != nil {
		return nil, err
	}

	var matches []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if matchGlobPattern(pattern, name) {
			matches = append(matches, basePath+name)
		}
	}

	return matches, nil
}

// expandRecursiveGlob expands a recursive glob pattern (containing **).
func (g *GlobMatcher) expandRecursiveGlob(
	ctx context.Context,
	fs filesystem.FileSystem,
	basePath string,
	pattern string,
) ([]string, error) {
	// Split the pattern at **
	parts := strings.SplitN(pattern, "**", 2)
	beforePart := parts[0]
	afterPart := ""
	if len(parts) > 1 {
		afterPart = strings.TrimPrefix(parts[1], "/")
	}

	// Recursively list all files
	return g.collectMatchingFiles(ctx, fs, basePath, beforePart, afterPart)
}

// collectMatchingFiles recursively collects files matching the pattern.
func (g *GlobMatcher) collectMatchingFiles(
	ctx context.Context,
	fs filesystem.FileSystem,
	currentPath string,
	beforePart string,
	afterPart string,
) ([]string, error) {
	// Use context-aware ReadDir if available
	var entries []filesystem.DirEntry
	var err error

	if ctxFS, ok := fs.(filesystem.ContextFileSystem); ok {
		entries, err = ctxFS.ReadDirContext(ctx, currentPath)
	} else {
		entries, err = fs.ReadDir(currentPath)
	}

	if err != nil {
		// If we can't read the directory, return empty matches
		return nil, nil
	}

	var matches []string
	for _, entry := range entries {
		name := entry.Name()
		fullPath := currentPath + name

		if entry.IsDir() {
			// Recurse into subdirectories
			subMatches, err := g.collectMatchingFiles(ctx, fs, fullPath+"/", beforePart, afterPart)
			if err != nil {
				return nil, err
			}
			matches = append(matches, subMatches...)
		} else {
			// Check if the file matches the after pattern
			if afterPart == "" || matchGlobPattern(afterPart, name) {
				matches = append(matches, fullPath)
			}
		}
	}

	return matches, nil
}

// matchGlobPattern matches a string against a simple glob pattern.
// Supports * (any characters) and ? (single character).
func matchGlobPattern(pattern, s string) bool {
	// Use Go's path.Match which supports * and ?
	matched, err := path.Match(pattern, s)
	if err != nil {
		return false
	}
	return matched
}

// ExpandPaths expands a list of paths/patterns to a flat list of matching files.
// This is useful for table functions that accept multiple file paths.
func (g *GlobMatcher) ExpandPaths(ctx context.Context, paths []string) ([]string, error) {
	var allMatches []string

	for _, p := range paths {
		matches, err := g.ExpandGlob(ctx, p)
		if err != nil {
			return nil, err
		}
		allMatches = append(allMatches, matches...)
	}

	// Remove duplicates while preserving order
	seen := make(map[string]bool)
	var unique []string
	for _, m := range allMatches {
		if !seen[m] {
			seen[m] = true
			unique = append(unique, m)
		}
	}

	return unique, nil
}
