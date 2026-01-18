package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCopyCommand(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantErr  bool
		expected *CopyCommand
	}{
		{
			name:  "COPY table TO STDOUT basic",
			query: "COPY users TO STDOUT",
			expected: &CopyCommand{
				IsFrom:      false,
				Table:       "users",
				Destination: "STDOUT",
				Options: &CopyOptions{
					Format:    CopyFormatText,
					Header:    false,
					Delimiter: '\t',
					Null:      "\\N",
					Quote:     '"',
					Escape:    '"',
				},
			},
		},
		{
			name:  "COPY table FROM STDIN basic",
			query: "COPY users FROM STDIN",
			expected: &CopyCommand{
				IsFrom:      true,
				Table:       "users",
				Destination: "STDIN",
				Options: &CopyOptions{
					Format:    CopyFormatText,
					Header:    false,
					Delimiter: '\t',
					Null:      "\\N",
					Quote:     '"',
					Escape:    '"',
				},
			},
		},
		{
			name:  "COPY with CSV format",
			query: "COPY users TO STDOUT (FORMAT csv)",
			expected: &CopyCommand{
				IsFrom:      false,
				Table:       "users",
				Destination: "STDOUT",
				Options: &CopyOptions{
					Format:    CopyFormatCSV,
					Header:    false,
					Delimiter: ',',
					Null:      "",
					Quote:     '"',
					Escape:    '"',
				},
			},
		},
		{
			name:  "COPY with CSV format and header",
			query: "COPY users TO STDOUT (FORMAT csv, HEADER)",
			expected: &CopyCommand{
				IsFrom:      false,
				Table:       "users",
				Destination: "STDOUT",
				Options: &CopyOptions{
					Format:    CopyFormatCSV,
					Header:    true,
					Delimiter: ',',
					Null:      "",
					Quote:     '"',
					Escape:    '"',
				},
			},
		},
		{
			name:  "COPY with binary format",
			query: "COPY users TO STDOUT (FORMAT binary)",
			expected: &CopyCommand{
				IsFrom:      false,
				Table:       "users",
				Destination: "STDOUT",
				Options: &CopyOptions{
					Format:    CopyFormatBinary,
					Header:    false,
					Delimiter: '\t',
					Null:      "\\N",
					Quote:     '"',
					Escape:    '"',
				},
			},
		},
		{
			name:  "COPY with column list",
			query: "COPY users (id, name, email) TO STDOUT",
			expected: &CopyCommand{
				IsFrom:      false,
				Table:       "users",
				Destination: "STDOUT",
				Options: &CopyOptions{
					Format:    CopyFormatText,
					Header:    false,
					Delimiter: '\t',
					Null:      "\\N",
					Quote:     '"',
					Escape:    '"',
					Columns:   []string{"id", "name", "email"},
				},
			},
		},
		{
			name:  "COPY (SELECT) TO STDOUT",
			query: "COPY (SELECT id, name FROM users WHERE active = true) TO STDOUT",
			expected: &CopyCommand{
				IsFrom:      false,
				Query:       "SELECT id, name FROM users WHERE active = true",
				Destination: "STDOUT",
				Options: &CopyOptions{
					Format:    CopyFormatText,
					Header:    false,
					Delimiter: '\t',
					Null:      "\\N",
					Quote:     '"',
					Escape:    '"',
				},
			},
		},
		{
			name:  "COPY with custom delimiter",
			query: "COPY users TO STDOUT (FORMAT csv, DELIMITER '|')",
			expected: &CopyCommand{
				IsFrom:      false,
				Table:       "users",
				Destination: "STDOUT",
				Options: &CopyOptions{
					Format:    CopyFormatCSV,
					Header:    false,
					Delimiter: '|',
					Null:      "",
					Quote:     '"',
					Escape:    '"',
				},
			},
		},
		{
			name:  "COPY with custom NULL string",
			query: "COPY users TO STDOUT (FORMAT csv, NULL 'NULL')",
			expected: &CopyCommand{
				IsFrom:      false,
				Table:       "users",
				Destination: "STDOUT",
				Options: &CopyOptions{
					Format:    CopyFormatCSV,
					Header:    false,
					Delimiter: ',',
					Null:      "NULL",
					Quote:     '"',
					Escape:    '"',
				},
			},
		},
		{
			name:  "COPY WITH syntax",
			query: "COPY users TO STDOUT WITH (FORMAT csv, HEADER true)",
			expected: &CopyCommand{
				IsFrom:      false,
				Table:       "users",
				Destination: "STDOUT",
				Options: &CopyOptions{
					Format:    CopyFormatCSV,
					Header:    true,
					Delimiter: ',',
					Null:      "",
					Quote:     '"',
					Escape:    '"',
				},
			},
		},
		{
			name:    "COPY to file (not supported)",
			query:   "COPY users TO '/tmp/users.csv'",
			wantErr: true,
		},
		{
			name:    "Not a COPY command",
			query:   "SELECT * FROM users",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := ParseCopyCommand(tt.query)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, cmd)

			assert.Equal(t, tt.expected.IsFrom, cmd.IsFrom, "IsFrom mismatch")
			assert.Equal(t, tt.expected.Table, cmd.Table, "Table mismatch")
			assert.Equal(t, tt.expected.Query, cmd.Query, "Query mismatch")
			assert.Equal(t, tt.expected.Destination, cmd.Destination, "Destination mismatch")

			assert.Equal(t, tt.expected.Options.Format, cmd.Options.Format, "Format mismatch")
			assert.Equal(t, tt.expected.Options.Header, cmd.Options.Header, "Header mismatch")
			assert.Equal(
				t,
				tt.expected.Options.Delimiter,
				cmd.Options.Delimiter,
				"Delimiter mismatch",
			)
			assert.Equal(t, tt.expected.Options.Null, cmd.Options.Null, "Null mismatch")

			if len(tt.expected.Options.Columns) > 0 {
				assert.Equal(
					t,
					tt.expected.Options.Columns,
					cmd.Options.Columns,
					"Columns mismatch",
				)
			}
		})
	}
}

func TestIsCopyCommand(t *testing.T) {
	tests := []struct {
		query    string
		expected bool
	}{
		{"COPY users TO STDOUT", true},
		{"copy users to stdout", true},
		{"COPY users FROM STDIN", true},
		{"  COPY users TO STDOUT", true},
		{"SELECT * FROM users", false},
		{"INSERT INTO users VALUES (1)", false},
		{"COPYCAT", false}, // Should not match partial words
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			result := IsCopyCommand(tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsCopyToStdout(t *testing.T) {
	tests := []struct {
		query    string
		expected bool
	}{
		{"COPY users TO STDOUT", true},
		{"COPY users TO STDOUT (FORMAT csv)", true},
		{"COPY (SELECT * FROM users) TO STDOUT", true},
		{"COPY users FROM STDIN", false},
		{"SELECT * FROM users", false},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			result := IsCopyToStdout(tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsCopyFromStdin(t *testing.T) {
	tests := []struct {
		query    string
		expected bool
	}{
		{"COPY users FROM STDIN", true},
		{"COPY users FROM STDIN (FORMAT csv)", true},
		{"COPY users (id, name) FROM STDIN", true},
		{"COPY users TO STDOUT", false},
		{"SELECT * FROM users", false},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			result := IsCopyFromStdin(tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatValueForCopy(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected string
	}{
		{"string", "hello", "hello"},
		{"int", 42, "42"},
		{"int64", int64(1234567890), "1234567890"},
		{"float64", 3.14159, "3.14159"},
		{"bool true", true, "t"},
		{"bool false", false, "f"},
		{"bytes", []byte("world"), "world"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatValueForCopy(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCopyOptionsDefaults(t *testing.T) {
	opts := DefaultCopyOptions()

	assert.Equal(t, CopyFormatText, opts.Format)
	assert.Equal(t, byte('\t'), opts.Delimiter)
	assert.Equal(t, "\\N", opts.Null)
	assert.Equal(t, byte('"'), opts.Quote)
	assert.Equal(t, byte('"'), opts.Escape)
	assert.False(t, opts.Header)
}

func TestParseColumnList(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"id, name, email", []string{"id", "name", "email"}},
		{"id,name,email", []string{"id", "name", "email"}},
		{"  id  ,  name  ", []string{"id", "name"}},
		{"single", []string{"single"}},
		{"", []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseColumnList(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCopyOutWriter(t *testing.T) {
	t.Run("text format", func(t *testing.T) {
		opts := &CopyOptions{
			Format:    CopyFormatText,
			Delimiter: '\t',
			Null:      "\\N",
		}
		// Create a mock writer (we just test buffer operations)
		w := NewCopyOutWriter(nil, opts)

		// Write some rows
		err := w.WriteRow([]any{"hello", 42, true})
		require.NoError(t, err)

		err = w.WriteRow([]any{nil, 0, false})
		require.NoError(t, err)

		data := w.GetData()
		assert.Contains(t, string(data), "hello\t42\tt")
		assert.Contains(t, string(data), "\\N\t0\tf")
	})

	t.Run("csv format", func(t *testing.T) {
		opts := &CopyOptions{
			Format:    CopyFormatCSV,
			Delimiter: ',',
			Null:      "",
		}
		w := NewCopyOutWriter(nil, opts)

		// Write header
		err := w.WriteHeader([]string{"name", "age", "active"})
		require.NoError(t, err)

		// Write row
		err = w.WriteRow([]any{"Alice", 30, true})
		require.NoError(t, err)

		data := w.GetData()
		assert.Contains(t, string(data), "name,age,active")
		assert.Contains(t, string(data), "Alice,30,t")
	})
}

func TestCopyLargeDataset(t *testing.T) {
	// Test that COPY formatting handles large datasets efficiently
	opts := &CopyOptions{
		Format:    CopyFormatCSV,
		Delimiter: ',',
		Null:      "",
	}
	w := NewCopyOutWriter(nil, opts)

	// Generate a large number of rows
	numRows := 10000
	for i := 0; i < numRows; i++ {
		err := w.WriteRow([]any{
			i,
			"user_" + formatValueForCopy(i),
			"email_" + formatValueForCopy(i) + "@example.com",
			i%2 == 0,
		})
		require.NoError(t, err)
	}

	data := w.GetData()
	// Verify data was written
	assert.Greater(t, len(data), 0)

	// Count lines (should be at least numRows)
	lineCount := 0
	for _, b := range data {
		if b == '\n' {
			lineCount++
		}
	}
	assert.Equal(t, numRows, lineCount, "should have %d lines", numRows)
}

func BenchmarkCopyFormatText(b *testing.B) {
	opts := &CopyOptions{
		Format:    CopyFormatText,
		Delimiter: '\t',
		Null:      "\\N",
	}

	for i := 0; i < b.N; i++ {
		w := NewCopyOutWriter(nil, opts)
		for j := 0; j < 1000; j++ {
			_ = w.WriteRow([]any{j, "user", "email@example.com", true})
		}
	}
}

func BenchmarkCopyFormatCSV(b *testing.B) {
	opts := &CopyOptions{
		Format:    CopyFormatCSV,
		Delimiter: ',',
		Null:      "",
	}

	for i := 0; i < b.N; i++ {
		w := NewCopyOutWriter(nil, opts)
		for j := 0; j < 1000; j++ {
			_ = w.WriteRow([]any{j, "user", "email@example.com", true})
		}
	}
}
