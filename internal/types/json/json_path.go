package json

import (
	"fmt"
	"strconv"
)

// PathSegment represents a single segment of a JSON path.
type PathSegment struct {
	Field string
	Index *int
}

// Path represents a parsed JSON path expression.
type Path struct {
	segments []PathSegment
}

// Segments returns a copy of the path segments.
func (p Path) Segments() []PathSegment {
	out := make([]PathSegment, len(p.segments))
	copy(out, p.segments)
	return out
}

// ParsePath parses a JSON path expression.
func ParsePath(path string) (Path, error) {
	if path == "" {
		return Path{}, fmt.Errorf("path cannot be empty")
	}
	i := 0
	if path[0] == '$' {
		i++
	}

	segments := []PathSegment{}
	for i < len(path) {
		switch path[i] {
		case '.':
			i++
			field, next, err := parseField(path, i)
			if err != nil {
				return Path{}, err
			}
			segments = append(segments, PathSegment{Field: field})
			i = next
		case '[':
			seg, next, err := parseBracket(path, i)
			if err != nil {
				return Path{}, err
			}
			segments = append(segments, seg)
			i = next
		default:
			field, next, err := parseField(path, i)
			if err != nil {
				return Path{}, err
			}
			segments = append(segments, PathSegment{Field: field})
			i = next
		}
	}

	if len(segments) == 0 {
		return Path{}, fmt.Errorf("path contains no segments")
	}
	return Path{segments: segments}, nil
}

func parseField(path string, start int) (string, int, error) {
	if start >= len(path) {
		return "", start, fmt.Errorf("expected field name")
	}
	if path[start] == '"' {
		end := start + 1
		for end < len(path) && path[end] != '"' {
			end++
		}
		if end >= len(path) {
			return "", start, fmt.Errorf("unterminated quoted field")
		}
		return path[start+1 : end], end + 1, nil
	}

	end := start
	for end < len(path) {
		ch := path[end]
		if ch == '.' || ch == '[' || ch == ']' {
			break
		}
		end++
	}
	if end == start {
		return "", start, fmt.Errorf("expected field name")
	}
	return path[start:end], end, nil
}

func parseBracket(path string, start int) (PathSegment, int, error) {
	if path[start] != '[' {
		return PathSegment{}, start, fmt.Errorf("expected [")
	}
	i := start + 1
	if i >= len(path) {
		return PathSegment{}, start, fmt.Errorf("unterminated bracket")
	}

	if path[i] == '"' {
		field, next, err := parseField(path, i)
		if err != nil {
			return PathSegment{}, start, err
		}
		if next >= len(path) || path[next] != ']' {
			return PathSegment{}, start, fmt.Errorf("unterminated bracket")
		}
		return PathSegment{Field: field}, next + 1, nil
	}

	end := i
	for end < len(path) && path[end] != ']' {
		end++
	}
	if end >= len(path) {
		return PathSegment{}, start, fmt.Errorf("unterminated bracket")
	}

	idx, err := strconv.Atoi(path[i:end])
	if err != nil {
		return PathSegment{}, start, fmt.Errorf("invalid array index")
	}
	return PathSegment{Index: &idx}, end + 1, nil
}
