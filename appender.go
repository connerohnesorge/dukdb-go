package dukdb

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/coder/quartz"
)

// DefaultAppenderThreshold is the default number of rows buffered before auto-flush.
const DefaultAppenderThreshold = 1024

// Appender provides efficient bulk data loading via batched INSERTs.
// For table appenders, it uses DataChunk-based columnar buffering for performance.
// For query appenders, it uses row-based buffering with SQL generation.
// Appender is thread-safe and can be used from multiple goroutines.
//
// There are two modes of operation:
//   - Table appender: Created via NewAppender, uses DataChunk-based buffering
//   - Query appender: Created via NewQueryAppender, uses SQL-based buffering
type Appender struct {
	conn      *Conn    // Database connection
	catalog   string   // Database catalog (defaults to "memory")
	schema    string   // Schema name (defaults to "main")
	table     string   // Table name
	columns   []string // Column names in order
	colTypes  []Type   // Column types (for reference, validation is deferred)
	threshold int      // Auto-flush threshold
	closed    bool     // Whether the appender has been closed
	mu        sync.Mutex

	// DataChunk-based buffering for table appenders
	currentChunk   *DataChunk // Current DataChunk for columnar buffering
	currentSize    int        // Number of rows in currentChunk
	columnTypeInfo []TypeInfo // Column TypeInfo for DataChunk creation

	// Row-based buffering for query appenders (and fallback)
	buffer [][]any // Buffered rows (used by query appenders)

	// Query appender fields (only set when created via NewQueryAppender)
	isQueryAppender bool       // True if this is a query appender
	query           string     // The SQL query to execute (INSERT, UPDATE, DELETE, MERGE INTO)
	tempTableName   string     // Name of the temporary table for batched data
	queryColTypes   []TypeInfo // Column types for the temp table
	queryColNames   []string   // Column names for the temp table
	tempTableExists bool       // Whether the temp table has been created
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

	// Try to get TypeInfo for DataChunk-based buffering
	typeInfos, _, typeInfoErr := conn.backendConn.GetTableTypeInfos(schema, table)

	// Create the appender
	appender := &Appender{
		conn:      conn,
		catalog:   catalog,
		schema:    schema,
		table:     table,
		columns:   columns,
		colTypes:  colTypes,
		threshold: threshold,
		closed:    false,
	}

	// If we got TypeInfo, use DataChunk-based buffering
	if typeInfoErr == nil && len(typeInfos) > 0 {
		appender.columnTypeInfo = typeInfos
		chunk, chunkErr := NewDataChunk(typeInfos)
		if chunkErr == nil {
			appender.currentChunk = chunk
			appender.currentSize = 0
		} else {
			// Fallback to row-based buffering
			appender.buffer = make([][]any, 0, threshold)
		}
	} else {
		// Fallback to row-based buffering
		appender.buffer = make([][]any, 0, threshold)
	}

	return appender, nil
}

// NewQueryAppender creates a query Appender that executes a custom query with batched rows.
// The batched rows are treated as a temporary table.
//
// Parameters:
//   - driverConn: The database connection (must be a *Conn)
//   - query: The SQL query to execute (INSERT, DELETE, UPDATE, or MERGE INTO)
//   - table: The name of the temporary table for batched data (defaults to "appended_data" if empty)
//   - colTypes: The column types for the temporary table
//   - colNames: The column names for the temporary table (defaults to "col1", "col2", ... if empty)
//
// Returns ErrorTypeInvalid if:
//   - query is empty
//   - colTypes is empty
//   - colNames length doesn't match colTypes length (when colNames is not empty)
func NewQueryAppender(
	driverConn driver.Conn,
	query, table string,
	colTypes []TypeInfo,
	colNames []string,
) (*Appender, error) {
	return NewQueryAppenderWithThreshold(
		driverConn,
		query,
		table,
		colTypes,
		colNames,
		DefaultAppenderThreshold,
	)
}

// NewQueryAppenderWithThreshold creates a query Appender with a custom auto-flush threshold.
// See NewQueryAppender for parameter documentation.
func NewQueryAppenderWithThreshold(
	driverConn driver.Conn,
	query, table string,
	colTypes []TypeInfo,
	colNames []string,
	threshold int,
) (*Appender, error) {
	// Extract *Conn from driver.Conn
	conn, ok := driverConn.(*Conn)
	if !ok {
		return nil, &Error{
			Type: ErrorTypeConnection,
			Msg:  "invalid connection type: expected *Conn",
		}
	}

	// Validate threshold
	if threshold < 1 {
		return nil, &Error{
			Type: ErrorTypeInvalid,
			Msg:  "threshold must be >= 1",
		}
	}

	// Validate query
	if query == "" {
		return nil, &Error{
			Type: ErrorTypeInvalid,
			Msg:  "query cannot be empty",
		}
	}

	// Validate colTypes
	if len(colTypes) == 0 {
		return nil, &Error{
			Type: ErrorTypeInvalid,
			Msg:  "column types cannot be empty",
		}
	}

	// Validate colNames length if provided
	if len(colNames) != 0 && len(colNames) != len(colTypes) {
		return nil, &Error{
			Type: ErrorTypeInvalid,
			Msg: fmt.Sprintf(
				"column names length (%d) must match column types length (%d)",
				len(colNames),
				len(colTypes),
			),
		}
	}

	// Apply default table name
	if table == "" {
		table = "appended_data"
	}

	// Generate default column names if not provided
	columns := colNames
	if len(columns) == 0 {
		columns = make([]string, len(colTypes))
		for i := range colTypes {
			columns[i] = fmt.Sprintf("col%d", i+1)
		}
	}

	return &Appender{
		conn:            conn,
		catalog:         "memory",
		schema:          "main",
		table:           table, // Used as temp table name
		columns:         columns,
		colTypes:        nil, // Not used for query appender
		buffer:          make([][]any, 0, threshold),
		threshold:       threshold,
		closed:          false,
		isQueryAppender: true,
		query:           query,
		tempTableName:   table,
		queryColTypes:   colTypes,
		queryColNames:   columns,
		tempTableExists: false,
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
	defer func() {
		_ = driverRows.Close()
	}()

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

// createTempTable creates the temporary table for query appender mode.
func (a *Appender) createTempTable() error {
	if a.tempTableExists {
		return nil
	}

	var sb strings.Builder
	sb.WriteString("CREATE TEMP TABLE ")
	sb.WriteString(quoteIdentifier(a.tempTableName))
	sb.WriteString(" (")
	for i, colName := range a.queryColNames {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(quoteIdentifier(colName))
		sb.WriteString(" ")
		sb.WriteString(a.queryColTypes[i].SQLType())
	}
	sb.WriteString(")")

	_, err := a.conn.ExecContext(
		context.Background(),
		sb.String(),
		nil,
	)
	if err != nil {
		return err
	}

	a.tempTableExists = true
	return nil
}

// truncateTempTable clears all data from the temporary table.
func (a *Appender) truncateTempTable() error {
	if !a.tempTableExists {
		return nil
	}

	query := "DELETE FROM " + quoteIdentifier(a.tempTableName)
	_, err := a.conn.ExecContext(
		context.Background(),
		query,
		nil,
	)
	return err
}

// dropTempTable removes the temporary table.
func (a *Appender) dropTempTable() error {
	if !a.tempTableExists {
		return nil
	}

	query := "DROP TABLE IF EXISTS " + quoteIdentifier(a.tempTableName)
	_, err := a.conn.ExecContext(
		context.Background(),
		query,
		nil,
	)
	if err == nil {
		a.tempTableExists = false
	}
	return err
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

	// Use DataChunk-based buffering for table appenders
	if a.currentChunk != nil && !a.isQueryAppender {
		return a.appendRowToChunk(values)
	}

	// Use row-based buffering for query appenders (and fallback)
	return a.appendRowToBuffer(values)
}

// appendRowToChunk appends a row to the DataChunk (for table appenders).
func (a *Appender) appendRowToChunk(values []any) error {
	// Calculate effective threshold (minimum of user threshold and VectorSize)
	effectiveThreshold := a.threshold
	if VectorSize < effectiveThreshold {
		effectiveThreshold = VectorSize
	}

	// Auto-flush at threshold
	if a.currentSize >= effectiveThreshold {
		if err := a.flushLocked(); err != nil {
			return err
		}
	}

	// Set values in the DataChunk
	for i, val := range values {
		if err := a.currentChunk.SetValue(i, a.currentSize, val); err != nil {
			return err
		}
	}
	a.currentSize++

	return nil
}

// appendRowToBuffer appends a row to the row-based buffer (for query appenders).
func (a *Appender) appendRowToBuffer(values []any) error {
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

	if a.isQueryAppender {
		if len(a.buffer) == 0 {
			return nil // Nothing to flush
		}
		return a.flushQueryAppender()
	}

	// Table appenders: use DataChunk-based flush if available
	if a.currentChunk != nil {
		if a.currentSize == 0 {
			return nil // Nothing to flush
		}
		return a.flushTableAppenderDataChunk()
	}

	// Fallback to row-based flush
	if len(a.buffer) == 0 {
		return nil // Nothing to flush
	}
	return a.flushTableAppender()
}

// flushTableAppender performs a direct INSERT flush for table appender mode.
// This is the fallback path when DataChunk-based buffering is not available.
func (a *Appender) flushTableAppender() error {
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

// flushTableAppenderDataChunk performs a direct DataChunk flush for table appender mode.
// This is the optimized path that bypasses SQL parsing.
func (a *Appender) flushTableAppenderDataChunk() error {
	if a.currentSize == 0 {
		return nil
	}

	// Set the chunk size to the actual number of rows
	if err := a.currentChunk.SetSize(a.currentSize); err != nil {
		return err
	}

	// Append the chunk directly to storage via the backend
	_, err := a.conn.backendConn.AppendDataChunk(
		context.Background(),
		a.schema,
		a.table,
		a.currentChunk,
	)
	if err != nil {
		// Chunk preserved for retry
		return err
	}

	// Reset chunk for reuse
	a.currentChunk.reset()
	a.currentSize = 0
	return nil
}

// flushQueryAppender performs a three-phase flush for query appender mode:
// 1. Create/truncate temp table
// 2. Insert buffered data into temp table
// 3. Execute user query
func (a *Appender) flushQueryAppender() error {
	// Phase 1: Create temp table if it doesn't exist
	if err := a.createTempTable(); err != nil {
		return err
	}

	// Phase 1b: Clear any existing data from temp table
	if err := a.truncateTempTable(); err != nil {
		return err
	}

	// Phase 2: Insert buffered data into temp table
	insert, err := a.buildTempTableInsert()
	if err != nil {
		return err
	}

	_, err = a.conn.ExecContext(
		context.Background(),
		insert,
		nil,
	)
	if err != nil {
		return err
	}

	// Phase 3: Execute the user query
	_, err = a.conn.ExecContext(
		context.Background(),
		a.query,
		nil,
	)
	if err != nil {
		return err
	}

	// Clear buffer only on success
	a.buffer = a.buffer[:0]
	return nil
}

// buildTempTableInsert constructs a multi-row INSERT statement for the temp table.
func (a *Appender) buildTempTableInsert() (string, error) {
	var sb strings.Builder

	sb.WriteString("INSERT INTO ")
	sb.WriteString(quoteIdentifier(a.tempTableName))
	sb.WriteString(" (")
	for i, col := range a.queryColNames {
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
	flushErr := a.flushLocked()

	// For query appenders, drop the temp table (cleanup)
	var dropErr error
	if a.isQueryAppender {
		dropErr = a.dropTempTable()
	}

	a.closed = true

	// Return first error encountered
	if flushErr != nil {
		return flushErr
	}
	return dropErr
}

// AppenderContext provides context and clock for flush operations.
// Used for deterministic testing with mock clocks.
type AppenderContext struct {
	ctx   context.Context
	clock quartz.Clock
}

// NewAppenderContext creates an AppenderContext with the given context and clock.
// If clock is nil, the real system clock is used.
func NewAppenderContext(ctx context.Context, clock quartz.Clock) AppenderContext {
	if clock == nil {
		clock = quartz.NewReal()
	}
	return AppenderContext{
		ctx:   ctx,
		clock: clock,
	}
}

// FlushWithContext writes all buffered rows to the database with deadline checking.
// Uses the injected clock for deadline comparison to enable deterministic testing.
// On error, the buffer is preserved for retry.
// Returns context.DeadlineExceeded if the deadline has passed.
// Returns ErrorTypeClosed if the appender is closed.
func (a *Appender) FlushWithContext(appCtx AppenderContext) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return &Error{
			Type: ErrorTypeClosed,
			Msg:  "appender is closed",
		}
	}

	// Check deadline using the injected clock
	if deadline, ok := appCtx.ctx.Deadline(); ok {
		if appCtx.clock.Until(deadline) <= 0 {
			return context.DeadlineExceeded
		}
	}

	return a.flushLockedWithContext(appCtx.ctx)
}

// flushLockedWithContext performs the actual flush with context. Caller must hold the mutex.
func (a *Appender) flushLockedWithContext(ctx context.Context) error {
	if a.isQueryAppender {
		if len(a.buffer) == 0 {
			return nil // Nothing to flush
		}
		return a.flushQueryAppenderWithContext(ctx)
	}

	// Table appenders: use DataChunk-based flush if available
	if a.currentChunk != nil {
		if a.currentSize == 0 {
			return nil // Nothing to flush
		}
		return a.flushTableAppenderDataChunkWithContext(ctx)
	}

	// Fallback to row-based flush
	if len(a.buffer) == 0 {
		return nil // Nothing to flush
	}
	return a.flushTableAppenderWithContext(ctx)
}

// flushTableAppenderWithContext performs a direct INSERT flush with context.
// This is the fallback path when DataChunk-based buffering is not available.
func (a *Appender) flushTableAppenderWithContext(ctx context.Context) error {
	insert, err := a.buildInsert()
	if err != nil {
		return err
	}

	_, err = a.conn.ExecContext(ctx, insert, nil)
	if err != nil {
		return err
	}

	a.buffer = a.buffer[:0]
	return nil
}

// flushTableAppenderDataChunkWithContext performs a direct DataChunk flush with context.
// This is the optimized path that bypasses SQL parsing.
func (a *Appender) flushTableAppenderDataChunkWithContext(ctx context.Context) error {
	if a.currentSize == 0 {
		return nil
	}

	// Set the chunk size to the actual number of rows
	if err := a.currentChunk.SetSize(a.currentSize); err != nil {
		return err
	}

	// Append the chunk directly to storage via the backend
	_, err := a.conn.backendConn.AppendDataChunk(
		ctx,
		a.schema,
		a.table,
		a.currentChunk,
	)
	if err != nil {
		// Chunk preserved for retry
		return err
	}

	// Reset chunk for reuse
	a.currentChunk.reset()
	a.currentSize = 0
	return nil
}

// flushQueryAppenderWithContext performs a three-phase flush with context.
func (a *Appender) flushQueryAppenderWithContext(ctx context.Context) error {
	// Phase 1: Create temp table if it doesn't exist
	if err := a.createTempTableWithContext(ctx); err != nil {
		return err
	}

	// Phase 1b: Clear any existing data from temp table
	if err := a.truncateTempTableWithContext(ctx); err != nil {
		return err
	}

	// Phase 2: Insert buffered data into temp table
	insert, err := a.buildTempTableInsert()
	if err != nil {
		return err
	}

	_, err = a.conn.ExecContext(ctx, insert, nil)
	if err != nil {
		return err
	}

	// Phase 3: Execute the user query
	_, err = a.conn.ExecContext(ctx, a.query, nil)
	if err != nil {
		return err
	}

	a.buffer = a.buffer[:0]
	return nil
}

// createTempTableWithContext creates the temp table with context.
func (a *Appender) createTempTableWithContext(ctx context.Context) error {
	if a.tempTableExists {
		return nil
	}

	var sb strings.Builder
	sb.WriteString("CREATE TEMP TABLE ")
	sb.WriteString(quoteIdentifier(a.tempTableName))
	sb.WriteString(" (")
	for i, colName := range a.queryColNames {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(quoteIdentifier(colName))
		sb.WriteString(" ")
		sb.WriteString(a.queryColTypes[i].SQLType())
	}
	sb.WriteString(")")

	_, err := a.conn.ExecContext(ctx, sb.String(), nil)
	if err != nil {
		return err
	}

	a.tempTableExists = true
	return nil
}

// truncateTempTableWithContext clears temp table data with context.
func (a *Appender) truncateTempTableWithContext(ctx context.Context) error {
	if !a.tempTableExists {
		return nil
	}

	query := "DELETE FROM " + quoteIdentifier(a.tempTableName)
	_, err := a.conn.ExecContext(ctx, query, nil)
	return err
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

	// DataChunk-based buffering for table appenders
	if a.currentChunk != nil && !a.isQueryAppender {
		return a.currentSize
	}

	// Row-based buffering for query appenders (and fallback)
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
