package dukdb

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
)

// DefaultAppenderThreshold is the default number of rows buffered before auto-flush.
const DefaultAppenderThreshold = 1024

// Appender provides efficient bulk data loading via batched INSERTs.
// It buffers rows in memory and flushes them as multi-row INSERT statements.
// Appender is thread-safe and can be used from multiple goroutines.
type Appender struct {
	conn      *Conn    // Database connection
	catalog   string   // Database catalog (defaults to "memory")
	schema    string   // Schema name (defaults to "main")
	table     string   // Table name
	columns   []string // Column names in order
	colTypes  []Type   // Column types (for reference, validation is deferred)
	buffer    [][]any  // Buffered rows
	threshold int      // Auto-flush threshold
	closed    bool     // Whether the appender has been closed
	mu        sync.Mutex
}

// NewAppenderFromConn creates a new Appender for the specified table.
// Uses "memory" as the catalog and the provided schema.
// The schema parameter specifies the schema name (use "main" for default schema).
// Returns ErrorTypeCatalog if the table does not exist.
func NewAppenderFromConn(
	conn *Conn,
	schema, table string,
) (*Appender, error) {
	return NewAppender(conn, "", schema, table)
}

// NewAppender creates a new Appender for the specified catalog, schema, and table.
// If catalog is empty, "memory" is used as the default.
// If schema is empty, "main" is used as the default.
// Returns ErrorTypeCatalog if the table does not exist.
func NewAppender(
	conn *Conn,
	catalog, schema, table string,
) (*Appender, error) {
	return NewAppenderWithThreshold(
		conn,
		catalog,
		schema,
		table,
		DefaultAppenderThreshold,
	)
}

// NewAppenderWithThreshold creates a new Appender with a custom auto-flush threshold.
// The threshold must be >= 1. When the buffer reaches this size, it automatically flushes.
// Returns ErrorTypeInvalid if threshold < 1.
// Returns ErrorTypeCatalog if the table does not exist.
func NewAppenderWithThreshold(
	conn *Conn,
	catalog, schema, table string,
	threshold int,
) (*Appender, error) {
	if threshold < 1 {
		return nil, &Error{
			Type: ErrorTypeInvalid,
			Msg:  "threshold must be >= 1",
		}
	}

	// Apply defaults
	if catalog == "" {
		catalog = "memory"
	}
	if schema == "" {
		schema = "main"
	}

	// Query table metadata to get column names and types
	columns, colTypes, err := getTableColumns(
		conn,
		catalog,
		schema,
		table,
	)
	if err != nil {
		return nil, err
	}

	return &Appender{
		conn:      conn,
		catalog:   catalog,
		schema:    schema,
		table:     table,
		columns:   columns,
		colTypes:  colTypes,
		buffer:    make([][]any, 0, threshold),
		threshold: threshold,
		closed:    false,
	}, nil
}

// getTableColumns retrieves the column names and types for a table using duckdb_columns().
// Returns ErrorTypeCatalog if the table does not exist.
func getTableColumns(
	conn *Conn,
	catalog, schema, table string,
) ([]string, []Type, error) {
	// Query the table metadata using duckdb_columns()
	query := `SELECT column_name, data_type
FROM duckdb_columns()
WHERE database_name = $1 AND schema_name = $2 AND table_name = $3
ORDER BY column_index`

	// Convert args to driver.NamedValue
	args := []driver.NamedValue{
		{Ordinal: 1, Value: catalog},
		{Ordinal: 2, Value: schema},
		{Ordinal: 3, Value: table},
	}

	driverRows, err := conn.QueryContext(
		context.Background(),
		query,
		args,
	)
	if err != nil {
		return nil, nil, err
	}
	defer driverRows.Close()

	var columns []string
	var colTypes []Type

	// Iterate through the rows using driver.Rows interface
	dest := make([]driver.Value, 2)
	for {
		err := driverRows.Next(dest)
		if err != nil {
			if err == io.EOF ||
				err.Error() == "EOF" {
				break
			}
			return nil, nil, err
		}
		colName, _ := dest[0].(string)
		dataType, _ := dest[1].(string)
		columns = append(columns, colName)
		colTypes = append(
			colTypes,
			parseDataType(dataType),
		)
	}

	if len(columns) == 0 {
		return nil, nil, &Error{
			Type: ErrorTypeCatalog,
			Msg: fmt.Sprintf(
				"table '%s.%s' not found",
				schema,
				table,
			),
		}
	}

	return columns, colTypes, nil
}

// parseDataType converts a DuckDB data type string to a Type constant.
// This is a best-effort mapping for common types.
func parseDataType(dataType string) Type {
	// Normalize the type name
	upperType := strings.ToUpper(
		strings.TrimSpace(dataType),
	)

	// Handle parameterized types by extracting base type
	if idx := strings.Index(upperType, "("); idx != -1 {
		upperType = upperType[:idx]
	}

	switch upperType {
	case "BOOLEAN", "BOOL":
		return TYPE_BOOLEAN
	case "TINYINT", "INT1":
		return TYPE_TINYINT
	case "SMALLINT", "INT2", "SHORT":
		return TYPE_SMALLINT
	case "INTEGER", "INT4", "INT", "SIGNED":
		return TYPE_INTEGER
	case "BIGINT", "INT8", "LONG":
		return TYPE_BIGINT
	case "UTINYINT":
		return TYPE_UTINYINT
	case "USMALLINT":
		return TYPE_USMALLINT
	case "UINTEGER":
		return TYPE_UINTEGER
	case "UBIGINT":
		return TYPE_UBIGINT
	case "FLOAT", "FLOAT4", "REAL":
		return TYPE_FLOAT
	case "DOUBLE", "FLOAT8":
		return TYPE_DOUBLE
	case "TIMESTAMP":
		return TYPE_TIMESTAMP
	case "TIMESTAMP_S":
		return TYPE_TIMESTAMP_S
	case "TIMESTAMP_MS":
		return TYPE_TIMESTAMP_MS
	case "TIMESTAMP_NS":
		return TYPE_TIMESTAMP_NS
	case "TIMESTAMPTZ",
		"TIMESTAMP WITH TIME ZONE":
		return TYPE_TIMESTAMP_TZ
	case "DATE":
		return TYPE_DATE
	case "TIME":
		return TYPE_TIME
	case "TIMETZ", "TIME WITH TIME ZONE":
		return TYPE_TIME_TZ
	case "INTERVAL":
		return TYPE_INTERVAL
	case "HUGEINT":
		return TYPE_HUGEINT
	case "UHUGEINT":
		return TYPE_UHUGEINT
	case "VARCHAR",
		"CHAR",
		"BPCHAR",
		"TEXT",
		"STRING":
		return TYPE_VARCHAR
	case "BLOB", "BYTEA", "BINARY", "VARBINARY":
		return TYPE_BLOB
	case "DECIMAL", "NUMERIC":
		return TYPE_DECIMAL
	case "UUID":
		return TYPE_UUID
	case "LIST":
		return TYPE_LIST
	case "STRUCT":
		return TYPE_STRUCT
	case "MAP":
		return TYPE_MAP
	case "ARRAY":
		return TYPE_ARRAY
	case "UNION":
		return TYPE_UNION
	case "ENUM":
		return TYPE_ENUM
	case "BIT", "BITSTRING":
		return TYPE_BIT
	default:
		return TYPE_INVALID
	}
}

// AppendRow adds a row to the buffer. If the buffer reaches the threshold,
// it automatically flushes the data.
// Returns ErrorTypeClosed if the appender is closed.
// Returns ErrorTypeInvalid if the number of values doesn't match the column count.
func (a *Appender) AppendRow(
	values ...any,
) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return &Error{
			Type: ErrorTypeClosed,
			Msg:  "appender is closed",
		}
	}

	if len(values) != len(a.columns) {
		return &Error{
			Type: ErrorTypeInvalid,
			Msg: fmt.Sprintf(
				"expected %d columns, got %d",
				len(a.columns),
				len(values),
			),
		}
	}

	// Auto-flush at threshold
	if len(a.buffer) >= a.threshold {
		if err := a.flushLocked(); err != nil {
			return err
		}
	}

	// Make a copy of values to avoid external modification
	row := make([]any, len(values))
	copy(row, values)
	a.buffer = append(a.buffer, row)
	return nil
}

// Flush writes all buffered rows to the database.
// On error, the buffer is preserved for retry.
// Returns ErrorTypeClosed if the appender is closed.
func (a *Appender) Flush() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.flushLocked()
}

// flushLocked performs the actual flush. Caller must hold the mutex.
func (a *Appender) flushLocked() error {
	if a.closed {
		return &Error{
			Type: ErrorTypeClosed,
			Msg:  "appender is closed",
		}
	}

	if len(a.buffer) == 0 {
		return nil // Nothing to flush
	}

	insert, err := a.buildInsert()
	if err != nil {
		// Buffer preserved for retry
		return err
	}

	_, err = a.conn.ExecContext(
		context.Background(),
		insert,
		nil,
	)
	if err != nil {
		// Buffer preserved for retry
		return err
	}

	// Clear buffer only on success
	a.buffer = a.buffer[:0]
	return nil
}

// buildInsert constructs a multi-row INSERT statement from the buffer.
func (a *Appender) buildInsert() (string, error) {
	var sb strings.Builder

	sb.WriteString("INSERT INTO ")
	sb.WriteString(a.qualifiedTableName())
	sb.WriteString(" (")
	for i, col := range a.columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(quoteIdentifier(col))
	}
	sb.WriteString(") VALUES ")

	for i, row := range a.buffer {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(")
		for j, val := range row {
			if j > 0 {
				sb.WriteString(", ")
			}
			formatted, err := FormatValue(val)
			if err != nil {
				return "", err
			}
			sb.WriteString(formatted)
		}
		sb.WriteString(")")
	}

	return sb.String(), nil
}

// qualifiedTableName returns the fully qualified table name (schema.table).
func (a *Appender) qualifiedTableName() string {
	return quoteIdentifier(
		a.schema,
	) + "." + quoteIdentifier(
		a.table,
	)
}

// quoteIdentifier quotes a SQL identifier to prevent SQL injection.
func quoteIdentifier(name string) string {
	// Double any embedded double quotes
	escaped := strings.ReplaceAll(
		name,
		"\"",
		"\"\"",
	)
	return "\"" + escaped + "\""
}

// Close flushes any remaining buffered data and marks the appender as closed.
// After Close, no more rows can be appended.
// Returns ErrorTypeClosed if already closed.
func (a *Appender) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return &Error{
			Type: ErrorTypeClosed,
			Msg:  "appender is already closed",
		}
	}

	// Flush any remaining data
	if err := a.flushLocked(); err != nil {
		// Still mark as closed, but return the error
		a.closed = true
		return err
	}

	a.closed = true
	return nil
}

// Columns returns the column names for the table.
func (a *Appender) Columns() []string {
	return a.columns
}

// ColumnTypes returns the column types for the table.
func (a *Appender) ColumnTypes() []Type {
	return a.colTypes
}

// BufferSize returns the current number of rows in the buffer.
func (a *Appender) BufferSize() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.buffer)
}

// Threshold returns the auto-flush threshold.
func (a *Appender) Threshold() int {
	return a.threshold
}

// IsClosed returns whether the appender has been closed.
func (a *Appender) IsClosed() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.closed
}
