package json

import (
	"encoding/json"
	"fmt"
)

// Get extracts a value from JSON input by path.
func Get(input any, path string) (any, error) {
	root, err := normalizeInput(input)
	if err != nil {
		return nil, err
	}
	parsedPath, err := ParsePath(path)
	if err != nil {
		return nil, err
	}
	return getAt(root, parsedPath.segments)
}

// Set updates a JSON value at the specified path and returns a new tree.
func Set(input any, path string, value any) (any, error) {
	root, err := normalizeInput(input)
	if err != nil {
		return nil, err
	}
	parsedPath, err := ParsePath(path)
	if err != nil {
		return nil, err
	}
	return setAt(root, parsedPath.segments, value)
}

// Delete removes a value at the specified path and returns a new tree.
func Delete(input any, path string) (any, error) {
	root, err := normalizeInput(input)
	if err != nil {
		return nil, err
	}
	parsedPath, err := ParsePath(path)
	if err != nil {
		return nil, err
	}
	return deleteAt(root, parsedPath.segments)
}

func normalizeInput(input any) (any, error) {
	switch v := input.(type) {
	case Value:
		return v.Parse()
	case *Value:
		return v.Parse()
	case []byte:
		var out any
		if err := json.Unmarshal(v, &out); err != nil {
			return nil, fmt.Errorf("parse JSON: %w", err)
		}
		return out, nil
	case string:
		var out any
		if err := json.Unmarshal([]byte(v), &out); err != nil {
			return nil, fmt.Errorf("parse JSON: %w", err)
		}
		return out, nil
	default:
		return v, nil
	}
}

func getAt(node any, segments []PathSegment) (any, error) {
	if len(segments) == 0 {
		return node, nil
	}
	seg := segments[0]
	switch {
	case seg.Index != nil:
		arr, ok := node.([]any)
		if !ok {
			return nil, fmt.Errorf("expected array")
		}
		if *seg.Index < 0 || *seg.Index >= len(arr) {
			return nil, fmt.Errorf("index out of range")
		}
		return getAt(arr[*seg.Index], segments[1:])
	default:
		m, ok := node.(map[string]any)
		if !ok {
			if anyMap, ok := node.(map[any]any); ok {
				if v, ok := anyMap[seg.Field]; ok {
					return getAt(v, segments[1:])
				}
				return nil, fmt.Errorf("field not found")
			}
			return nil, fmt.Errorf("expected object")
		}
		child, ok := m[seg.Field]
		if !ok {
			return nil, fmt.Errorf("field not found")
		}
		return getAt(child, segments[1:])
	}
}

func setAt(node any, segments []PathSegment, value any) (any, error) {
	if len(segments) == 0 {
		return value, nil
	}
	seg := segments[0]
	switch {
	case seg.Index != nil:
		arr, ok := node.([]any)
		if !ok {
			return nil, fmt.Errorf("expected array")
		}
		if *seg.Index < 0 || *seg.Index >= len(arr) {
			return nil, fmt.Errorf("index out of range")
		}
		copied := copySlice(arr)
		child, err := setAt(copied[*seg.Index], segments[1:], value)
		if err != nil {
			return nil, err
		}
		copied[*seg.Index] = child
		return copied, nil
	default:
		m, ok := node.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected object")
		}
		copied := copyMap(m)
		child, ok := copied[seg.Field]
		if !ok {
			child = map[string]any{}
		}
		updated, err := setAt(child, segments[1:], value)
		if err != nil {
			return nil, err
		}
		copied[seg.Field] = updated
		return copied, nil
	}
}

func deleteAt(node any, segments []PathSegment) (any, error) {
	if len(segments) == 0 {
		return node, nil
	}
	seg := segments[0]
	if len(segments) == 1 {
		switch {
		case seg.Index != nil:
			arr, ok := node.([]any)
			if !ok {
				return nil, fmt.Errorf("expected array")
			}
			if *seg.Index < 0 || *seg.Index >= len(arr) {
				return nil, fmt.Errorf("index out of range")
			}
			copied := copySlice(arr)
			return append(copied[:*seg.Index], copied[*seg.Index+1:]...), nil
		default:
			m, ok := node.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("expected object")
			}
			copied := copyMap(m)
			delete(copied, seg.Field)
			return copied, nil
		}
	}

	switch {
	case seg.Index != nil:
		arr, ok := node.([]any)
		if !ok {
			return nil, fmt.Errorf("expected array")
		}
		if *seg.Index < 0 || *seg.Index >= len(arr) {
			return nil, fmt.Errorf("index out of range")
		}
		copied := copySlice(arr)
		updated, err := deleteAt(copied[*seg.Index], segments[1:])
		if err != nil {
			return nil, err
		}
		copied[*seg.Index] = updated
		return copied, nil
	default:
		m, ok := node.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected object")
		}
		copied := copyMap(m)
		child, ok := copied[seg.Field]
		if !ok {
			return nil, fmt.Errorf("field not found")
		}
		updated, err := deleteAt(child, segments[1:])
		if err != nil {
			return nil, err
		}
		copied[seg.Field] = updated
		return copied, nil
	}
}

func copyMap(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func copySlice(src []any) []any {
	dst := make([]any, len(src))
	copy(dst, src)
	return dst
}
