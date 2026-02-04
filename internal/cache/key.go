package cache

import (
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
)

// ParameterMode controls how query parameters contribute to cache keys.
type ParameterMode string

const (
	ParameterModeExact     ParameterMode = "exact"
	ParameterModeStructure ParameterMode = "structure"
)

// BuildCacheKey constructs a deterministic cache key from query text and parameters.
func BuildCacheKey(query string, args []driver.NamedValue, mode ParameterMode) (string, error) {
	normalized := normalizeQuery(query)
	paramFingerprint, err := fingerprintArgs(args, mode)
	if err != nil {
		return "", err
	}

	payload := strings.Join([]string{normalized, string(mode), paramFingerprint}, "|")
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:]), nil
}

func normalizeQuery(query string) string {
	fields := strings.Fields(query)
	return strings.Join(fields, " ")
}

func fingerprintArgs(args []driver.NamedValue, mode ParameterMode) (string, error) {
	if len(args) == 0 {
		return "", nil
	}

	positional := make([]driver.NamedValue, 0, len(args))
	named := make([]driver.NamedValue, 0, len(args))

	for _, arg := range args {
		if arg.Name != "" {
			named = append(named, arg)
		} else {
			positional = append(positional, arg)
		}
	}

	sort.Slice(positional, func(i, j int) bool {
		return positional[i].Ordinal < positional[j].Ordinal
	})
	sort.Slice(named, func(i, j int) bool {
		return named[i].Name < named[j].Name
	})

	parts := make([]string, 0, len(args))
	for _, arg := range positional {
		fragment, err := formatArgFingerprint(fmt.Sprintf("$%d", arg.Ordinal), arg.Value, mode)
		if err != nil {
			return "", err
		}
		parts = append(parts, fragment)
	}
	for _, arg := range named {
		fragment, err := formatArgFingerprint("@"+arg.Name, arg.Value, mode)
		if err != nil {
			return "", err
		}
		parts = append(parts, fragment)
	}

	return strings.Join(parts, ";"), nil
}

func formatArgFingerprint(label string, value any, mode ParameterMode) (string, error) {
	if mode == ParameterModeStructure {
		if value == nil {
			return fmt.Sprintf("%s:<nil>", label), nil
		}
		return fmt.Sprintf("%s:%T", label, value), nil
	}

	formatted, err := dukdb.FormatValue(value)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s=%s", label, formatted), nil
}
