// Package server provides PostgreSQL wire protocol compatibility for dukdb-go.
// This file implements full parameter binding for prepared statements with proper
// type conversion based on PostgreSQL OIDs.
package server

import (
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	wire "github.com/jeroenrinzema/psql-wire"
)

// ParameterBinder handles conversion of wire protocol parameters to driver values.
// It supports both text and binary format parameters and properly handles all
// standard PostgreSQL data types.
type ParameterBinder struct {
	// paramTypes holds the expected OIDs for each parameter position.
	// If nil or empty, types are inferred from the parameter values.
	paramTypes []uint32
}

// NewParameterBinder creates a new ParameterBinder with the given parameter types.
func NewParameterBinder(paramTypes []uint32) *ParameterBinder {
	return &ParameterBinder{
		paramTypes: paramTypes,
	}
}

// BindParameters converts wire.Parameter values to driver.NamedValue using
// the appropriate type conversion based on parameter OIDs and format codes.
func (pb *ParameterBinder) BindParameters(params []wire.Parameter) ([]driver.NamedValue, error) {
	if len(params) == 0 {
		return nil, nil
	}

	result := make([]driver.NamedValue, len(params))
	for i, param := range params {
		value, err := pb.bindParameter(i, param)
		if err != nil {
			return nil, fmt.Errorf("parameter $%d: %w", i+1, err)
		}
		result[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   value,
		}
	}

	return result, nil
}

// bindParameter converts a single wire.Parameter to its Go value.
func (pb *ParameterBinder) bindParameter(index int, param wire.Parameter) (any, error) {
	// Get the raw value bytes
	rawValue := param.Value()

	// Handle NULL values (nil or empty bytes typically indicates NULL)
	if rawValue == nil {
		return nil, nil
	}

	// Determine the target OID
	oid := pb.getParameterOid(index)

	// Handle based on format code
	if param.Format() == wire.BinaryFormat {
		return pb.bindBinaryParameter(oid, rawValue)
	}

	// Text format
	return pb.bindTextParameter(oid, rawValue)
}

// getParameterOid returns the OID for the parameter at the given index.
func (pb *ParameterBinder) getParameterOid(index int) uint32 {
	if pb.paramTypes != nil && index < len(pb.paramTypes) {
		return pb.paramTypes[index]
	}
	return OidUnknown
}

// bindTextParameter converts a text format parameter to its Go value.
func (pb *ParameterBinder) bindTextParameter(oid uint32, data []byte) (any, error) {
	if data == nil {
		return nil, nil
	}

	text := string(data)

	// For string types, empty string is valid and should be returned
	// For other types, empty string typically means NULL
	if len(data) == 0 {
		switch oid {
		case OidText, OidVarchar, OidChar, OidJSON, OidJSONB:
			return "", nil
		case OidBytea:
			return []byte{}, nil
		default:
			return nil, nil
		}
	}

	switch oid {
	case OidBool:
		return parseTextBool(text)
	case OidInt2:
		return parseTextInt16(text)
	case OidInt4:
		return parseTextInt32(text)
	case OidInt8:
		return parseTextInt64(text)
	case OidFloat4:
		return parseTextFloat32(text)
	case OidFloat8:
		return parseTextFloat64(text)
	case OidNumeric:
		return parseTextNumeric(text)
	case OidText, OidVarchar, OidChar:
		return text, nil
	case OidBytea:
		return parseTextBytea(text)
	case OidDate:
		return parseTextDate(text)
	case OidTime:
		return parseTextTime(text)
	case OidTimestamp:
		return parseTextTimestamp(text)
	case OidTimestampTZ:
		return parseTextTimestampTZ(text)
	case OidInterval:
		return parseTextInterval(text)
	case OidUUID:
		return parseTextUUID(text)
	case OidJSON, OidJSONB:
		return text, nil // Keep JSON as string
	case OidOid:
		return parseTextUint32(text)
	case OidUnknown:
		// For unknown types, try to infer from the value
		return inferTextValue(text)
	default:
		// Default to string for unhandled types
		return text, nil
	}
}

// bindBinaryParameter converts a binary format parameter to its Go value.
func (pb *ParameterBinder) bindBinaryParameter(oid uint32, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}

	switch oid {
	case OidBool:
		return parseBinaryBool(data)
	case OidInt2:
		return parseBinaryInt16(data)
	case OidInt4:
		return parseBinaryInt32(data)
	case OidInt8:
		return parseBinaryInt64(data)
	case OidFloat4:
		return parseBinaryFloat32(data)
	case OidFloat8:
		return parseBinaryFloat64(data)
	case OidText, OidVarchar, OidChar:
		return string(data), nil
	case OidBytea:
		// Return a copy of the byte slice
		result := make([]byte, len(data))
		copy(result, data)
		return result, nil
	case OidDate:
		return parseBinaryDate(data)
	case OidTime:
		return parseBinaryTime(data)
	case OidTimestamp:
		return parseBinaryTimestamp(data)
	case OidTimestampTZ:
		return parseBinaryTimestampTZ(data)
	case OidInterval:
		return parseBinaryInterval(data)
	case OidNumeric:
		return parseBinaryNumeric(data)
	case OidUUID:
		return parseBinaryUUID(data)
	case OidJSON, OidJSONB:
		// JSON in binary format is still UTF-8 text
		return string(data), nil
	case OidOid:
		return parseBinaryUint32(data)
	case OidUnknown:
		// For unknown OID, try to infer type from binary data length
		return inferBinaryValue(data)
	default:
		// For other unknown binary types, return raw bytes
		result := make([]byte, len(data))
		copy(result, data)
		return result, nil
	}
}

// Text format parsers

func parseTextBool(text string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(text)) {
	case "t", "true", "y", "yes", "on", "1":
		return true, nil
	case "f", "false", "n", "no", "off", "0":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean value: %q", text)
	}
}

func parseTextInt16(text string) (int16, error) {
	v, err := strconv.ParseInt(strings.TrimSpace(text), 10, 16)
	if err != nil {
		return 0, fmt.Errorf("invalid smallint value: %w", err)
	}
	return int16(v), nil
}

func parseTextInt32(text string) (int32, error) {
	v, err := strconv.ParseInt(strings.TrimSpace(text), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid integer value: %w", err)
	}
	return int32(v), nil
}

func parseTextInt64(text string) (int64, error) {
	v, err := strconv.ParseInt(strings.TrimSpace(text), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid bigint value: %w", err)
	}
	return v, nil
}

func parseTextFloat32(text string) (float32, error) {
	v, err := strconv.ParseFloat(strings.TrimSpace(text), 32)
	if err != nil {
		return 0, fmt.Errorf("invalid real value: %w", err)
	}
	return float32(v), nil
}

func parseTextFloat64(text string) (float64, error) {
	v, err := strconv.ParseFloat(strings.TrimSpace(text), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid double precision value: %w", err)
	}
	return v, nil
}

func parseTextNumeric(text string) (string, error) {
	// For numeric/decimal, keep as string to preserve precision
	// The engine will handle parsing
	return strings.TrimSpace(text), nil
}

func parseTextUint32(text string) (uint32, error) {
	v, err := strconv.ParseUint(strings.TrimSpace(text), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid oid value: %w", err)
	}
	return uint32(v), nil
}

func parseTextBytea(text string) ([]byte, error) {
	// PostgreSQL bytea can be in hex format (\x...) or escape format
	text = strings.TrimSpace(text)

	if strings.HasPrefix(text, "\\x") || strings.HasPrefix(text, "\\X") {
		// Hex format
		hexStr := text[2:]
		return decodeHex(hexStr)
	}

	// Escape format or raw bytes
	return decodeEscapeBytea(text)
}

func decodeHex(hexStr string) ([]byte, error) {
	if len(hexStr)%2 != 0 {
		return nil, errors.New("invalid hex string length")
	}

	result := make([]byte, len(hexStr)/2)
	for i := 0; i < len(hexStr); i += 2 {
		b, err := strconv.ParseUint(hexStr[i:i+2], 16, 8)
		if err != nil {
			return nil, fmt.Errorf("invalid hex digit: %w", err)
		}
		result[i/2] = byte(b)
	}
	return result, nil
}

func decodeEscapeBytea(text string) ([]byte, error) {
	var result []byte
	i := 0
	for i < len(text) {
		if text[i] == '\\' && i+1 < len(text) {
			if text[i+1] == '\\' {
				// Escaped backslash
				result = append(result, '\\')
				i += 2
			} else if i+3 < len(text) && isOctalDigit(text[i+1]) {
				// Octal escape \NNN
				val, err := strconv.ParseUint(text[i+1:i+4], 8, 8)
				if err != nil {
					return nil, fmt.Errorf("invalid octal escape: %w", err)
				}
				result = append(result, byte(val))
				i += 4
			} else {
				result = append(result, text[i])
				i++
			}
		} else {
			result = append(result, text[i])
			i++
		}
	}
	return result, nil
}

func isOctalDigit(c byte) bool {
	return c >= '0' && c <= '7'
}

// PostgreSQL epoch is 2000-01-01 00:00:00 UTC
var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

func parseTextDate(text string) (time.Time, error) {
	text = strings.TrimSpace(text)

	// Try common date formats
	formats := []string{
		"2006-01-02",
		"01/02/2006",
		"02-Jan-2006",
		"2006/01/02",
		"January 2, 2006",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, text); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("invalid date value: %q", text)
}

func parseTextTime(text string) (time.Time, error) {
	text = strings.TrimSpace(text)

	// Try common time formats
	formats := []string{
		"15:04:05",
		"15:04:05.000000",
		"15:04:05.000",
		"15:04",
		"3:04:05 PM",
		"3:04 PM",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, text); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("invalid time value: %q", text)
}

func parseTextTimestamp(text string) (time.Time, error) {
	text = strings.TrimSpace(text)

	// Try common timestamp formats (without timezone)
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.000000",
		"2006-01-02 15:04:05.000",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05.000000",
		"2006-01-02T15:04:05.000",
		"2006-01-02",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, text); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("invalid timestamp value: %q", text)
}

func parseTextTimestampTZ(text string) (time.Time, error) {
	text = strings.TrimSpace(text)

	// Try common timestamp with timezone formats
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02 15:04:05-07",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05.000000-07",
		"2006-01-02 15:04:05.000000-07:00",
		"2006-01-02T15:04:05-07:00",
		"2006-01-02T15:04:05.000000-07:00",
		"2006-01-02 15:04:05 MST",
		"2006-01-02 15:04:05",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, text); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("invalid timestamptz value: %q", text)
}

func parseTextInterval(text string) (string, error) {
	// Intervals are complex; keep as string and let engine parse
	return strings.TrimSpace(text), nil
}

func parseTextUUID(text string) (string, error) {
	text = strings.TrimSpace(text)

	// Validate UUID format (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
	if len(text) != 36 {
		// Try without dashes
		if len(text) == 32 {
			// Add dashes
			return text[0:8] + "-" + text[8:12] + "-" + text[12:16] + "-" + text[16:20] + "-" + text[20:32], nil
		}
		return "", fmt.Errorf("invalid UUID length: %d", len(text))
	}

	// Validate dashes are in correct positions
	if text[8] != '-' || text[13] != '-' || text[18] != '-' || text[23] != '-' {
		return "", fmt.Errorf("invalid UUID format: %q", text)
	}

	return text, nil
}

// inferTextValue attempts to infer the Go type from a text value.
func inferTextValue(text string) (any, error) {
	text = strings.TrimSpace(text)

	// Check for NULL
	if strings.EqualFold(text, "null") {
		return nil, nil
	}

	// Check for boolean
	lower := strings.ToLower(text)
	if lower == "true" || lower == "t" || lower == "yes" || lower == "y" || lower == "on" {
		return true, nil
	}
	if lower == "false" || lower == "f" || lower == "no" || lower == "n" || lower == "off" {
		return false, nil
	}

	// Check for integer
	if v, err := strconv.ParseInt(text, 10, 64); err == nil {
		// Return appropriate sized integer
		if v >= math.MinInt32 && v <= math.MaxInt32 {
			return int32(v), nil
		}
		return v, nil
	}

	// Check for float
	if v, err := strconv.ParseFloat(text, 64); err == nil {
		return v, nil
	}

	// Default to string
	return text, nil
}

// Binary format parsers

// inferBinaryValue attempts to infer the Go type from binary data when OID is unknown.
// This is used when the client sends binary format parameters but no type information.
func inferBinaryValue(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// Try to infer based on common binary sizes used by PostgreSQL
	switch len(data) {
	case 1:
		// Could be a boolean or a small integer
		// Assume boolean since it's most common for 1-byte binary
		return data[0] != 0, nil
	case 2:
		// Most likely int16
		return int16(binary.BigEndian.Uint16(data)), nil
	case 4:
		// Most likely int32
		return int32(binary.BigEndian.Uint32(data)), nil
	case 8:
		// Could be int64 or float64 or timestamp
		// Assume int64 as it's the most common
		return int64(binary.BigEndian.Uint64(data)), nil
	case 16:
		// Could be UUID
		return parseBinaryUUID(data)
	default:
		// For other sizes, try to interpret as string (UTF-8 text)
		// This handles text/varchar that was sent in binary format
		return string(data), nil
	}
}

func parseBinaryBool(data []byte) (bool, error) {
	if len(data) != 1 {
		return false, errors.New("invalid binary boolean length")
	}
	return data[0] != 0, nil
}

func parseBinaryInt16(data []byte) (int16, error) {
	if len(data) != 2 {
		return 0, errors.New("invalid binary int16 length")
	}
	return int16(binary.BigEndian.Uint16(data)), nil
}

func parseBinaryInt32(data []byte) (int32, error) {
	if len(data) != 4 {
		return 0, errors.New("invalid binary int32 length")
	}
	return int32(binary.BigEndian.Uint32(data)), nil
}

func parseBinaryInt64(data []byte) (int64, error) {
	if len(data) != 8 {
		return 0, errors.New("invalid binary int64 length")
	}
	return int64(binary.BigEndian.Uint64(data)), nil
}

func parseBinaryFloat32(data []byte) (float32, error) {
	if len(data) != 4 {
		return 0, errors.New("invalid binary float32 length")
	}
	bits := binary.BigEndian.Uint32(data)
	return math.Float32frombits(bits), nil
}

func parseBinaryFloat64(data []byte) (float64, error) {
	if len(data) != 8 {
		return 0, errors.New("invalid binary float64 length")
	}
	bits := binary.BigEndian.Uint64(data)
	return math.Float64frombits(bits), nil
}

func parseBinaryUint32(data []byte) (uint32, error) {
	if len(data) != 4 {
		return 0, errors.New("invalid binary uint32 length")
	}
	return binary.BigEndian.Uint32(data), nil
}

func parseBinaryDate(data []byte) (time.Time, error) {
	if len(data) != 4 {
		return time.Time{}, errors.New("invalid binary date length")
	}
	// PostgreSQL stores date as days since 2000-01-01
	days := int32(binary.BigEndian.Uint32(data))
	return pgEpoch.AddDate(0, 0, int(days)), nil
}

func parseBinaryTimestamp(data []byte) (time.Time, error) {
	if len(data) != 8 {
		return time.Time{}, errors.New("invalid binary timestamp length")
	}
	// PostgreSQL stores timestamp as microseconds since 2000-01-01
	microseconds := int64(binary.BigEndian.Uint64(data))
	return pgEpoch.Add(time.Duration(microseconds) * time.Microsecond), nil
}

func parseBinaryTimestampTZ(data []byte) (time.Time, error) {
	if len(data) != 8 {
		return time.Time{}, errors.New("invalid binary timestamptz length")
	}
	// PostgreSQL stores timestamptz as microseconds since 2000-01-01 in UTC
	microseconds := int64(binary.BigEndian.Uint64(data))
	return pgEpoch.Add(time.Duration(microseconds) * time.Microsecond).UTC(), nil
}

func parseBinaryUUID(data []byte) (string, error) {
	if len(data) != 16 {
		return "", errors.New("invalid binary UUID length")
	}
	// Format as UUID string with dashes
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		data[0:4], data[4:6], data[6:8], data[8:10], data[10:16]), nil
}

// parseBinaryTime parses a PostgreSQL binary time value.
// PostgreSQL stores time as microseconds since midnight (int64).
func parseBinaryTime(data []byte) (time.Time, error) {
	if len(data) != 8 {
		return time.Time{}, errors.New("invalid binary time length")
	}
	// PostgreSQL stores time as microseconds since midnight
	microseconds := int64(binary.BigEndian.Uint64(data))

	// Create a time.Time with just the time component (date is zero value)
	// Use a reference date (epoch) and add the time component
	hours := microseconds / (3600 * 1e6)
	microseconds -= hours * 3600 * 1e6
	minutes := microseconds / (60 * 1e6)
	microseconds -= minutes * 60 * 1e6
	seconds := microseconds / 1e6
	microseconds -= seconds * 1e6
	nanos := microseconds * 1000

	return time.Date(0, 1, 1, int(hours), int(minutes), int(seconds), int(nanos), time.UTC), nil
}

// parseBinaryInterval parses a PostgreSQL binary interval value.
// PostgreSQL stores interval as: 8 bytes microseconds + 4 bytes days + 4 bytes months
func parseBinaryInterval(data []byte) (string, error) {
	if len(data) != 16 {
		return "", errors.New("invalid binary interval length")
	}

	// PostgreSQL interval binary format:
	// - 8 bytes: time in microseconds (int64)
	// - 4 bytes: days (int32)
	// - 4 bytes: months (int32)
	microseconds := int64(binary.BigEndian.Uint64(data[0:8]))
	days := int32(binary.BigEndian.Uint32(data[8:12]))
	months := int32(binary.BigEndian.Uint32(data[12:16]))

	// Convert to PostgreSQL interval string format
	var parts []string

	// Years and months
	if months != 0 {
		years := months / 12
		remainingMonths := months % 12
		if years != 0 {
			parts = append(parts, fmt.Sprintf("%d year", years))
			if years != 1 && years != -1 {
				parts[len(parts)-1] += "s"
			}
		}
		if remainingMonths != 0 {
			parts = append(parts, fmt.Sprintf("%d mon", remainingMonths))
			if remainingMonths != 1 && remainingMonths != -1 {
				parts[len(parts)-1] += "s"
			}
		}
	}

	// Days
	if days != 0 {
		parts = append(parts, fmt.Sprintf("%d day", days))
		if days != 1 && days != -1 {
			parts[len(parts)-1] += "s"
		}
	}

	// Time component
	if microseconds != 0 {
		negative := microseconds < 0
		if negative {
			microseconds = -microseconds
		}

		hours := microseconds / (3600 * 1e6)
		microseconds -= hours * 3600 * 1e6
		minutes := microseconds / (60 * 1e6)
		microseconds -= minutes * 60 * 1e6
		seconds := microseconds / 1e6
		microseconds -= seconds * 1e6

		timeStr := fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
		if microseconds > 0 {
			timeStr += fmt.Sprintf(".%06d", microseconds)
			// Trim trailing zeros
			timeStr = strings.TrimRight(timeStr, "0")
		}
		if negative {
			timeStr = "-" + timeStr
		}
		parts = append(parts, timeStr)
	}

	if len(parts) == 0 {
		return "00:00:00", nil
	}

	return strings.Join(parts, " "), nil
}

// parseBinaryNumeric parses a PostgreSQL binary numeric value.
// PostgreSQL numeric binary format is complex:
// - 2 bytes: ndigits (number of digits, not counting leading/trailing zeros)
// - 2 bytes: weight (weight of first digit)
// - 2 bytes: sign (0 = positive, 0x4000 = negative, 0xC000 = NaN)
// - 2 bytes: dscale (display scale, number of decimal places to display)
// - ndigits * 2 bytes: digits in base 10000
func parseBinaryNumeric(data []byte) (string, error) {
	if len(data) < 8 {
		return "", errors.New("invalid binary numeric length")
	}

	ndigits := int(binary.BigEndian.Uint16(data[0:2]))
	weight := int16(binary.BigEndian.Uint16(data[2:4]))
	sign := binary.BigEndian.Uint16(data[4:6])
	dscale := int(binary.BigEndian.Uint16(data[6:8]))

	// Validate length
	expectedLen := 8 + ndigits*2
	if len(data) != expectedLen {
		return "", fmt.Errorf(
			"invalid binary numeric length: expected %d, got %d",
			expectedLen,
			len(data),
		)
	}

	// Handle special values
	const (
		numericPositive = 0x0000
		numericNegative = 0x4000
		numericNaN      = 0xC000
	)

	if sign == numericNaN {
		return "NaN", nil
	}

	// Handle zero (no digits)
	if ndigits == 0 {
		if dscale > 0 {
			return "0." + strings.Repeat("0", dscale), nil
		}
		return "0", nil
	}

	// Read the digits (each digit is a base-10000 value)
	digits := make([]int16, ndigits)
	for i := 0; i < ndigits; i++ {
		offset := 8 + i*2
		digits[i] = int16(binary.BigEndian.Uint16(data[offset : offset+2]))
	}

	// Build the number string
	var result strings.Builder

	// Add sign
	if sign == numericNegative {
		result.WriteByte('-')
	}

	// The weight represents the power of 10000 for the first digit
	// For example, weight=1 means the first digit is multiplied by 10000^1
	// weight=-1 means the first digit is multiplied by 10000^-1 (i.e., 0.0001 to 0.9999)

	// Calculate decimal point position
	// Each digit represents 4 decimal digits
	// decimalPos is the number of digits before the decimal point
	decimalPos := (int(weight) + 1) * 4

	// Build digits as a string
	var digitStr strings.Builder
	for _, d := range digits {
		digitStr.WriteString(fmt.Sprintf("%04d", d))
	}
	numStr := digitStr.String()

	// Trim leading zeros from the first digit group only if it's before decimal
	if len(numStr) > 0 && decimalPos > 0 {
		// Find first non-zero in the first group
		firstGroup := numStr[:4]
		firstGroupTrimmed := strings.TrimLeft(firstGroup, "0")
		if firstGroupTrimmed == "" {
			firstGroupTrimmed = "0"
		}
		numStr = firstGroupTrimmed + numStr[4:]
		decimalPos = decimalPos - (4 - len(firstGroupTrimmed))
	}

	if decimalPos <= 0 {
		// Number is less than 1
		result.WriteString("0.")
		// Add leading zeros after decimal point
		for i := decimalPos; i < 0; i++ {
			result.WriteByte('0')
		}
		result.WriteString(numStr)
	} else if decimalPos >= len(numStr) {
		// Number is an integer with possible trailing zeros
		result.WriteString(numStr)
		// Add trailing zeros if needed
		for i := len(numStr); i < decimalPos; i++ {
			result.WriteByte('0')
		}
	} else {
		// Number has both integer and fractional parts
		result.WriteString(numStr[:decimalPos])
		result.WriteByte('.')
		result.WriteString(numStr[decimalPos:])
	}

	// Get the result and apply display scale
	numericStr := result.String()

	// Find decimal position in result
	dotIdx := strings.Index(numericStr, ".")
	if dotIdx == -1 {
		// No decimal point - add one if needed
		if dscale > 0 {
			numericStr += "." + strings.Repeat("0", dscale)
		}
	} else {
		// Has decimal point - adjust fractional part
		fracPart := numericStr[dotIdx+1:]
		if len(fracPart) < dscale {
			// Pad with zeros
			numericStr += strings.Repeat("0", dscale-len(fracPart))
		} else if len(fracPart) > dscale && dscale > 0 {
			// Truncate (though this shouldn't happen with valid data)
			numericStr = numericStr[:dotIdx+1+dscale]
		}
	}

	// Remove trailing zeros after decimal point if dscale allows
	if dscale == 0 && strings.Contains(numericStr, ".") {
		numericStr = strings.TrimRight(numericStr, "0")
		numericStr = strings.TrimRight(numericStr, ".")
	}

	return numericStr, nil
}

// ConvertStringParams converts string parameters to driver.NamedValue using
// type inference or the provided parameter types.
func ConvertStringParams(params []string, paramTypes []uint32) ([]driver.NamedValue, error) {
	if len(params) == 0 {
		return nil, nil
	}

	result := make([]driver.NamedValue, len(params))
	for i, param := range params {
		var value any
		var err error

		// Get OID if available
		oid := OidUnknown
		if paramTypes != nil && i < len(paramTypes) {
			oid = paramTypes[i]
		}

		// Convert based on OID
		if oid != OidUnknown {
			binder := NewParameterBinder([]uint32{oid})
			value, err = binder.bindTextParameter(oid, []byte(param))
		} else {
			// Infer type from string value
			value, err = inferTextValue(param)
		}

		if err != nil {
			return nil, fmt.Errorf("parameter $%d: %w", i+1, err)
		}

		result[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   value,
		}
	}

	return result, nil
}

// StripQuotes removes surrounding single or double quotes from a parameter string.
func StripQuotes(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 {
		if (s[0] == '\'' && s[len(s)-1] == '\'') ||
			(s[0] == '"' && s[len(s)-1] == '"') {
			return s[1 : len(s)-1]
		}
	}
	return s
}
