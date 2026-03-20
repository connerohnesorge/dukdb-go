package executor

import (
	"fmt"
	"strings"
	"unicode/utf8"

	dukdb "github.com/dukdb/dukdb-go"
)

// formatString implements DuckDB-compatible printf-style string formatting.
// Supports: %s, %d, %f, %e, %g, %x, %o, %c, %%, width, precision, flags (-, 0, +).
func formatString(format string, args []any) (string, error) {
	var result strings.Builder
	argIdx := 0
	i := 0

	for i < len(format) {
		if format[i] != '%' {
			r, size := utf8.DecodeRuneInString(format[i:])
			result.WriteRune(r)
			i += size

			continue
		}

		i++ // skip %

		if i >= len(format) {
			return "", &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "FORMAT: incomplete format specifier",
			}
		}

		// Handle %%
		if format[i] == '%' {
			result.WriteByte('%')
			i++

			continue
		}

		// Collect the full format specifier: flags, width, precision, verb
		specStart := i - 1 // include the %

		// Flags: -, +, 0, space
		for i < len(format) && (format[i] == '-' || format[i] == '+' || format[i] == '0' || format[i] == ' ') {
			i++
		}

		// Width
		for i < len(format) && format[i] >= '0' && format[i] <= '9' {
			i++
		}

		// Precision
		if i < len(format) && format[i] == '.' {
			i++
			for i < len(format) && format[i] >= '0' && format[i] <= '9' {
				i++
			}
		}

		if i >= len(format) {
			return "", &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "FORMAT: incomplete format specifier",
			}
		}

		verb := format[i]
		i++

		if argIdx >= len(args) {
			return "", &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "FORMAT: not enough arguments for format string",
			}
		}

		arg := args[argIdx]
		argIdx++

		if arg == nil {
			result.WriteString("NULL")

			continue
		}

		goFmt := format[specStart:i]

		switch verb {
		case 's':
			result.WriteString(fmt.Sprintf(goFmt, toString(arg)))
		case 'd', 'x', 'o':
			v, _ := toInt64(arg)
			result.WriteString(fmt.Sprintf(goFmt, v))
		case 'f', 'e', 'g':
			v, _ := toFloat64(arg)
			result.WriteString(fmt.Sprintf(goFmt, v))
		case 'c':
			v, _ := toInt64(arg)
			r := rune(v)
			result.WriteRune(r)
		default:
			return "", &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("FORMAT: unsupported format specifier '%%%c'", verb),
			}
		}
	}

	return result.String(), nil
}
