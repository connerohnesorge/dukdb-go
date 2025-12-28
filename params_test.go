package dukdb

import (
	"database/sql/driver"
	"math"
	"math/big"
	"testing"
	"time"
)

// TestExtractPositionalPlaceholders tests positional placeholder extraction ($1, $2, etc.)
func TestExtractPositionalPlaceholders(
	t *testing.T,
) {
	tests := []struct {
		name     string
		query    string
		expected []placeholder
	}{
		{
			name:     "no placeholders",
			query:    "SELECT * FROM users",
			expected: nil,
		},
		{
			name:  "single positional placeholder",
			query: "SELECT * FROM users WHERE id = $1",
			expected: []placeholder{
				{
					start:        31,
					end:          33,
					name:         "1",
					isPositional: true,
				},
			},
		},
		{
			name:  "multiple positional placeholders",
			query: "SELECT * FROM users WHERE id = $1 AND name = $2",
			expected: []placeholder{
				{
					start:        31,
					end:          33,
					name:         "1",
					isPositional: true,
				},
				{
					start:        45,
					end:          47,
					name:         "2",
					isPositional: true,
				},
			},
		},
		{
			name:     "placeholder inside string literal - skipped",
			query:    "SELECT * FROM users WHERE name = 'test$1'",
			expected: nil,
		},
		{
			name:  "placeholder outside string literal",
			query: "SELECT * FROM users WHERE name = 'test' AND id = $1",
			expected: []placeholder{
				{
					start:        49,
					end:          51,
					name:         "1",
					isPositional: true,
				},
			},
		},
		{
			name:     "placeholder inside escaped string",
			query:    "SELECT * FROM users WHERE name = 'it''s $1 test'",
			expected: nil,
		},
		{
			name:  "multi-digit placeholder",
			query: "SELECT * FROM users WHERE id = $10 AND name = $12",
			expected: []placeholder{
				{
					start:        31,
					end:          34,
					name:         "10",
					isPositional: true,
				},
				{
					start:        46,
					end:          49,
					name:         "12",
					isPositional: true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractPositionalPlaceholders(
				tt.query,
			)

			if len(result) != len(tt.expected) {
				t.Fatalf(
					"expected %d placeholders, got %d",
					len(tt.expected),
					len(result),
				)
			}

			for i, p := range result {
				if p.start != tt.expected[i].start {
					t.Errorf(
						"placeholder %d: expected start %d, got %d",
						i,
						tt.expected[i].start,
						p.start,
					)
				}
				if p.end != tt.expected[i].end {
					t.Errorf(
						"placeholder %d: expected end %d, got %d",
						i,
						tt.expected[i].end,
						p.end,
					)
				}
				if p.name != tt.expected[i].name {
					t.Errorf(
						"placeholder %d: expected name %q, got %q",
						i,
						tt.expected[i].name,
						p.name,
					)
				}
				if p.isPositional != tt.expected[i].isPositional {
					t.Errorf(
						"placeholder %d: expected isPositional %v, got %v",
						i,
						tt.expected[i].isPositional,
						p.isPositional,
					)
				}
			}
		})
	}
}

// TestExtractNamedPlaceholders tests named placeholder extraction (@name)
func TestExtractNamedPlaceholders(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected []placeholder
	}{
		{
			name:     "no placeholders",
			query:    "SELECT * FROM users",
			expected: nil,
		},
		{
			name:  "single named placeholder",
			query: "SELECT * FROM users WHERE id = @userId",
			expected: []placeholder{
				{
					start:        31,
					end:          38,
					name:         "userId",
					isPositional: false,
				},
			},
		},
		{
			name:  "multiple named placeholders",
			query: "SELECT * FROM users WHERE id = @userId AND name = @userName",
			expected: []placeholder{
				{
					start:        31,
					end:          38,
					name:         "userId",
					isPositional: false,
				},
				{
					start:        50,
					end:          59,
					name:         "userName",
					isPositional: false,
				},
			},
		},
		{
			name:     "placeholder inside string literal - skipped",
			query:    "SELECT * FROM users WHERE name = 'test@email'",
			expected: nil,
		},
		{
			name:  "placeholder with underscore",
			query: "SELECT * FROM users WHERE id = @user_id",
			expected: []placeholder{
				{
					start:        31,
					end:          39,
					name:         "user_id",
					isPositional: false,
				},
			},
		},
		{
			name:  "placeholder with numbers",
			query: "SELECT * FROM users WHERE id = @user123",
			expected: []placeholder{
				{
					start:        31,
					end:          39,
					name:         "user123",
					isPositional: false,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractNamedPlaceholders(
				tt.query,
			)

			if len(result) != len(tt.expected) {
				t.Fatalf(
					"expected %d placeholders, got %d",
					len(tt.expected),
					len(result),
				)
			}

			for i, p := range result {
				if p.start != tt.expected[i].start {
					t.Errorf(
						"placeholder %d: expected start %d, got %d",
						i,
						tt.expected[i].start,
						p.start,
					)
				}
				if p.end != tt.expected[i].end {
					t.Errorf(
						"placeholder %d: expected end %d, got %d",
						i,
						tt.expected[i].end,
						p.end,
					)
				}
				if p.name != tt.expected[i].name {
					t.Errorf(
						"placeholder %d: expected name %q, got %q",
						i,
						tt.expected[i].name,
						p.name,
					)
				}
				if p.isPositional != tt.expected[i].isPositional {
					t.Errorf(
						"placeholder %d: expected isPositional %v, got %v",
						i,
						tt.expected[i].isPositional,
						p.isPositional,
					)
				}
			}
		})
	}
}

// TestBindParams tests the BindParams function
func TestBindParams(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		args        []driver.NamedValue
		expected    string
		expectError bool
	}{
		{
			name:     "no placeholders",
			query:    "SELECT * FROM users",
			args:     nil,
			expected: "SELECT * FROM users",
		},
		{
			name:  "single positional placeholder",
			query: "SELECT * FROM users WHERE id = $1",
			args: []driver.NamedValue{
				{Ordinal: 1, Value: 42},
			},
			expected: "SELECT * FROM users WHERE id = 42",
		},
		{
			name:  "multiple positional placeholders",
			query: "SELECT * FROM users WHERE id = $1 AND name = $2",
			args: []driver.NamedValue{
				{Ordinal: 1, Value: 42},
				{Ordinal: 2, Value: "John"},
			},
			expected: "SELECT * FROM users WHERE id = 42 AND name = 'John'",
		},
		{
			name:  "single named placeholder",
			query: "SELECT * FROM users WHERE id = @userId",
			args: []driver.NamedValue{
				{Name: "userId", Value: 42},
			},
			expected: "SELECT * FROM users WHERE id = 42",
		},
		{
			name:  "multiple named placeholders",
			query: "SELECT * FROM users WHERE id = @userId AND name = @userName",
			args: []driver.NamedValue{
				{Name: "userId", Value: 42},
				{Name: "userName", Value: "John"},
			},
			expected: "SELECT * FROM users WHERE id = 42 AND name = 'John'",
		},
		{
			name:  "mixed placeholders - error",
			query: "SELECT * FROM users WHERE id = $1 AND name = @userName",
			args: []driver.NamedValue{
				{Ordinal: 1, Value: 42},
				{Name: "userName", Value: "John"},
			},
			expectError: true,
		},
		{
			name:  "missing positional argument",
			query: "SELECT * FROM users WHERE id = $1 AND name = $2",
			args: []driver.NamedValue{
				{Ordinal: 1, Value: 42},
			},
			expectError: true,
		},
		{
			name:  "missing named argument",
			query: "SELECT * FROM users WHERE id = @userId AND name = @userName",
			args: []driver.NamedValue{
				{Name: "userId", Value: 42},
			},
			expectError: true,
		},
		{
			name:  "null value",
			query: "INSERT INTO users (id, name) VALUES ($1, $2)",
			args: []driver.NamedValue{
				{Ordinal: 1, Value: 42},
				{Ordinal: 2, Value: nil},
			},
			expected: "INSERT INTO users (id, name) VALUES (42, NULL)",
		},
		{
			name:  "boolean true",
			query: "INSERT INTO users (active) VALUES ($1)",
			args: []driver.NamedValue{
				{Ordinal: 1, Value: true},
			},
			expected: "INSERT INTO users (active) VALUES (TRUE)",
		},
		{
			name:  "boolean false",
			query: "INSERT INTO users (active) VALUES ($1)",
			args: []driver.NamedValue{
				{Ordinal: 1, Value: false},
			},
			expected: "INSERT INTO users (active) VALUES (FALSE)",
		},
		{
			name:  "same placeholder used multiple times",
			query: "SELECT * FROM users WHERE id = $1 OR parent_id = $1",
			args: []driver.NamedValue{
				{Ordinal: 1, Value: 42},
			},
			expected: "SELECT * FROM users WHERE id = 42 OR parent_id = 42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := BindParams(
				tt.query,
				tt.args,
			)

			if tt.expectError {
				if err == nil {
					t.Errorf(
						"expected error, got nil",
					)
				}
				return
			}

			if err != nil {
				t.Fatalf(
					"unexpected error: %v",
					err,
				)
			}

			if result != tt.expected {
				t.Errorf(
					"expected %q, got %q",
					tt.expected,
					result,
				)
			}
		})
	}
}

// TestFormatValue tests the FormatValue function
func TestFormatValue(t *testing.T) {
	tests := []struct {
		name        string
		value       any
		expected    string
		expectError bool
	}{
		// Nil
		{
			name:     "nil",
			value:    nil,
			expected: "NULL",
		},

		// Booleans
		{
			name:     "bool true",
			value:    true,
			expected: "TRUE",
		},
		{
			name:     "bool false",
			value:    false,
			expected: "FALSE",
		},

		// Integers
		{
			name:     "int",
			value:    int(42),
			expected: "42",
		},
		{
			name:     "int8",
			value:    int8(42),
			expected: "42",
		},
		{
			name:     "int16",
			value:    int16(42),
			expected: "42",
		},
		{
			name:     "int32",
			value:    int32(42),
			expected: "42",
		},
		{
			name:     "int64",
			value:    int64(42),
			expected: "42",
		},
		{
			name:     "negative int",
			value:    int(-42),
			expected: "-42",
		},

		// Unsigned integers
		{
			name:     "uint",
			value:    uint(42),
			expected: "42",
		},
		{
			name:     "uint8",
			value:    uint8(42),
			expected: "42",
		},
		{
			name:     "uint16",
			value:    uint16(42),
			expected: "42",
		},
		{
			name:     "uint32",
			value:    uint32(42),
			expected: "42",
		},
		{
			name:     "uint64",
			value:    uint64(42),
			expected: "42",
		},

		// Floats
		{
			name:     "float32",
			value:    float32(3.14),
			expected: "3.140000104904175",
		},
		{
			name:     "float64",
			value:    float64(3.14),
			expected: "3.14",
		},
		{
			name:        "float infinity",
			value:       math.Inf(1),
			expectError: true,
		},
		{
			name:        "float -infinity",
			value:       math.Inf(-1),
			expectError: true,
		},
		{
			name:        "float NaN",
			value:       math.NaN(),
			expectError: true,
		},

		// Strings
		{
			name:     "string",
			value:    "hello",
			expected: "'hello'",
		},
		{
			name:     "string with quote",
			value:    "it's",
			expected: "'it''s'",
		},
		{
			name:     "empty string",
			value:    "",
			expected: "''",
		},

		// Bytes
		{
			name: "bytes",
			value: []byte{
				0xDE,
				0xAD,
				0xBE,
				0xEF,
			},
			expected: "X'DEADBEEF'",
		},
		{
			name:     "empty bytes",
			value:    []byte{},
			expected: "X''",
		},

		// Big integers
		{
			name:     "big.Int",
			value:    big.NewInt(123456789),
			expected: "123456789",
		},
		{
			name:     "nil big.Int",
			value:    (*big.Int)(nil),
			expected: "NULL",
		},

		// Interval
		{
			name: "Interval",
			value: Interval{
				Months: 1,
				Days:   2,
				Micros: 3000000,
			},
			expected: "INTERVAL '1 months 2 days 3000000 microseconds'",
		},

		// UUID
		{
			name:     "nil UUID pointer",
			value:    (*UUID)(nil),
			expected: "NULL",
		},

		// Decimal
		{
			name:     "nil Decimal pointer",
			value:    (*Decimal)(nil),
			expected: "NULL",
		},

		// Unsupported type
		{
			name:        "unsupported type",
			value:       struct{}{},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := FormatValue(tt.value)

			if tt.expectError {
				if err == nil {
					t.Errorf(
						"expected error, got nil",
					)
				}
				return
			}

			if err != nil {
				t.Fatalf(
					"unexpected error: %v",
					err,
				)
			}

			if result != tt.expected {
				t.Errorf(
					"expected %q, got %q",
					tt.expected,
					result,
				)
			}
		})
	}
}

// TestFormatValueTime tests time formatting
func TestFormatValueTime(t *testing.T) {
	// Test a specific time
	testTime := time.Date(
		2023,
		6,
		15,
		14,
		30,
		45,
		123456000,
		time.UTC,
	)
	result, err := FormatValue(testTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "'2023-06-15 14:30:45.123456'"
	if result != expected {
		t.Errorf(
			"expected %q, got %q",
			expected,
			result,
		)
	}
}

// TestFormatValueUUID tests UUID formatting
func TestFormatValueUUID(t *testing.T) {
	var uuid UUID
	// Set up a test UUID (same bytes as "12345678-1234-5678-1234-567812345678")
	copy(
		uuid[:],
		[]byte{
			0x12,
			0x34,
			0x56,
			0x78,
			0x12,
			0x34,
			0x56,
			0x78,
			0x12,
			0x34,
			0x56,
			0x78,
			0x12,
			0x34,
			0x56,
			0x78,
		},
	)

	result, err := FormatValue(uuid)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "'12345678-1234-5678-1234-567812345678'"
	if result != expected {
		t.Errorf(
			"expected %q, got %q",
			expected,
			result,
		)
	}
}

// TestFormatValueDecimal tests Decimal formatting
func TestFormatValueDecimal(t *testing.T) {
	dec := Decimal{
		Width: 10,
		Scale: 2,
		Value: big.NewInt(12345),
	}

	result, err := FormatValue(dec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "123.45"
	if result != expected {
		t.Errorf(
			"expected %q, got %q",
			expected,
			result,
		)
	}
}

// TestSQLInjectionPrevention tests that SQL injection attempts are properly escaped
func TestSQLInjectionPrevention(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "single quote injection",
			input:    "'; DROP TABLE users; --",
			expected: "'''; DROP TABLE users; --'",
		},
		{
			name:     "multiple single quotes",
			input:    "it's Bob's table",
			expected: "'it''s Bob''s table'",
		},
		{
			name:     "unicode characters",
			input:    "hello\u0000world",
			expected: "'hello\u0000world'",
		},
		{
			name:     "backslash",
			input:    "path\\to\\file",
			expected: "'path\\to\\file'",
		},
		{
			name:     "comment injection",
			input:    "value /* comment */ --",
			expected: "'value /* comment */ --'",
		},
		{
			name:     "nested quotes",
			input:    "a''b",
			expected: "'a''''b'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatString(tt.input)
			if result != tt.expected {
				t.Errorf(
					"expected %q, got %q",
					tt.expected,
					result,
				)
			}
		})
	}
}

// TestIsInsideStringLiteral tests the string literal detection
func TestIsInsideStringLiteral(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		pos      int
		expected bool
	}{
		{
			name:     "before string",
			query:    "SELECT 'hello' FROM t",
			pos:      0,
			expected: false,
		},
		{
			name:     "inside string",
			query:    "SELECT 'hello' FROM t",
			pos:      10,
			expected: true,
		},
		{
			name:     "after string",
			query:    "SELECT 'hello' FROM t",
			pos:      16,
			expected: false,
		},
		{
			name:     "inside escaped quote",
			query:    "SELECT 'it''s' FROM t",
			pos:      11,
			expected: true,
		},
		{
			name:     "after escaped quote",
			query:    "SELECT 'it''s' FROM t",
			pos:      16,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isInsideStringLiteral(
				tt.query,
				tt.pos,
			)
			if result != tt.expected {
				t.Errorf(
					"expected %v, got %v",
					tt.expected,
					result,
				)
			}
		})
	}
}
