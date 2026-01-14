# Wire Protocol Integration Design for PostgreSQL Compatibility

## Overview

This specification defines the integration architecture for PostgreSQL wire protocol support in dukdb-go using the `jeroenrinzema/psql-wire` library. The wire protocol server enables PostgreSQL-compatible clients (psql, pgx, JDBC, ORMs) to connect to and query a dukdb-go database using the PostgreSQL network protocol.

### Design Goals

- Enable PostgreSQL clients to connect transparently
- Support both Simple Query and Extended Query protocols
- Maintain full compatibility with existing `database/sql` driver
- Provide session isolation between concurrent connections
- Support concurrent server mode alongside native driver access
- Minimal overhead for query processing
- Graceful error handling with PostgreSQL error codes

---

## Architecture Overview

### High-Level Architecture

```
PostgreSQL Clients (psql, pgx, JDBC, ORMs, etc.)
        |
        | TCP/TLS (PostgreSQL wire protocol)
        v
+------------------------------------------+
|        PostgreSQL Wire Server            |
|  (github.com/jeroenrinzema/psql-wire)    |
+------------------------------------------+
        |
        | Query Handler Interface
        v
+------------------------------------------+
|          Session Manager                  |
|  - Session-to-Connection mapping         |
|  - Prepared statement cache              |
|  - Transaction state                     |
+------------------------------------------+
        |
        | BackendConn Interface
        v
+------------------------------------------+
|        dukdb-go Engine                   |
|  - Parser -> Binder -> Planner           |
|  - Executor -> Storage                   |
+------------------------------------------+
```

### Package Structure

```
internal/postgres/
    server/
        server.go           # PostgresServer main entry point
        config.go           # Server configuration options
        handler.go          # Query handler implementation
        session.go          # Session management
        prepared.go         # Prepared statement management
        transaction.go      # Transaction state management
        auth.go             # Authentication handlers
        tls.go              # TLS configuration
    wire/
        result_writer.go    # Result formatting for wire protocol
        field_desc.go       # Field description builder
        error.go            # PostgreSQL error code mapping
        codec.go            # Value encoding/decoding
    types/
        oid.go              # PostgreSQL OID constants (from type-mapping spec)
        mapping.go          # Type mapping (from type-mapping spec)
        conversion.go       # Value conversion
```

---

## Server Configuration

### ServerConfig Structure

```go
// ServerConfig holds configuration for the PostgreSQL wire protocol server.
type ServerConfig struct {
    // ListenAddr is the address to listen on (e.g., "127.0.0.1:5432", ":5432").
    ListenAddr string

    // DatabasePath is the path to the dukdb database.
    // Use ":memory:" for in-memory database shared across connections.
    DatabasePath string

    // DatabaseConfig is the optional dukdb.Config for the database.
    DatabaseConfig *dukdb.Config

    // MaxConnections limits concurrent connections (0 = unlimited).
    MaxConnections int

    // Authentication configures the authentication method.
    Authentication AuthConfig

    // TLS configures TLS/SSL settings.
    TLS *TLSConfig

    // ReadTimeout is the timeout for reading client messages.
    ReadTimeout time.Duration

    // WriteTimeout is the timeout for writing responses.
    WriteTimeout time.Duration

    // IdleTimeout is the timeout for idle connections.
    IdleTimeout time.Duration

    // Logger is an optional logger for server events.
    Logger *slog.Logger

    // PostgresCompatMode enables PostgreSQL function aliases and type mapping.
    // Should generally be true when using the wire protocol server.
    PostgresCompatMode bool

    // Parameters are initial server parameters sent during startup.
    Parameters map[string]string
}

// AuthConfig configures authentication for the server.
type AuthConfig struct {
    // Method is the authentication method.
    // Supported: "trust", "password", "md5", "scram-sha-256"
    Method string

    // PasswordCallback is called to verify passwords.
    // Not used for "trust" method.
    PasswordCallback func(username, password string) bool

    // Users is a map of username to password for simple auth.
    // Used when PasswordCallback is nil.
    Users map[string]string
}

// TLSConfig configures TLS for the server.
type TLSConfig struct {
    // CertFile is the path to the server certificate.
    CertFile string

    // KeyFile is the path to the server private key.
    KeyFile string

    // ClientAuth configures client certificate requirements.
    // Options: "none", "request", "require", "verify"
    ClientAuth string

    // CAFile is the path to the CA certificate for client verification.
    CAFile string
}
```

### Default Server Parameters

```go
// DefaultServerParameters returns initial parameters sent during startup.
func DefaultServerParameters() map[string]string {
    return map[string]string{
        "server_version":           "16.0 (dukdb-go)",
        "server_encoding":          "UTF8",
        "client_encoding":          "UTF8",
        "DateStyle":                "ISO, MDY",
        "IntervalStyle":            "postgres",
        "TimeZone":                 "UTC",
        "integer_datetimes":        "on",
        "standard_conforming_strings": "on",
        "application_name":         "",
        "is_superuser":             "on",
        "session_authorization":    "dukdb",
    }
}
```

---

## Connection Lifecycle

### Startup Flow

```
Client                          Server
   |                               |
   |------- SSL Request ---------->|  (optional)
   |<------ SSL Response ---------|  ('S' accept, 'N' reject)
   |                               |
   |------- Startup Message ------>|  (version, user, database, options)
   |                               |
   |                               |  [Create Session]
   |                               |  [Open BackendConn]
   |                               |
   |<------ Authentication --------|  (method-specific exchange)
   |------- Password/Auth -------->|
   |<------ AuthenticationOk ------|
   |                               |
   |<------ ParameterStatus -------|  (multiple, server parameters)
   |<------ BackendKeyData --------|  (cancel key)
   |<------ ReadyForQuery ---------|  ('I' idle)
   |                               |
```

### Session Creation

```go
// handleStartup processes the startup message and creates a session.
func (s *Server) handleStartup(ctx context.Context, startup *pgproto3.StartupMessage) error {
    // Extract connection parameters
    user := startup.Parameters["user"]
    database := startup.Parameters["database"]
    options := startup.Parameters["options"]

    // Create a new session
    session := &Session{
        ID:              generateSessionID(),
        Username:        user,
        Database:        database,
        StartTime:       time.Now(),
        PreparedStmts:   make(map[string]*PreparedStatement),
        Portals:         make(map[string]*Portal),
        TransactionState: TransactionIdle,
        Parameters:      make(map[string]string),
    }

    // Copy default parameters
    for k, v := range s.config.Parameters {
        session.Parameters[k] = v
    }

    // Apply client options
    if appName, ok := startup.Parameters["application_name"]; ok {
        session.Parameters["application_name"] = appName
    }

    // Open backend connection
    conn, err := s.backend.Open(s.config.DatabasePath, s.config.DatabaseConfig)
    if err != nil {
        return s.sendError(ctx, err)
    }
    session.BackendConn = conn

    // Store session in context
    wire.StoreSession(ctx, session)

    return nil
}
```

### Authentication Flow

```go
// handleAuthentication performs authentication based on configured method.
func (s *Server) handleAuthentication(ctx context.Context, session *Session) error {
    switch s.config.Authentication.Method {
    case "trust":
        // No authentication required
        return s.sendAuthenticationOk(ctx)

    case "password":
        // Clear text password
        if err := s.sendAuthenticationCleartext(ctx); err != nil {
            return err
        }
        return s.handlePasswordResponse(ctx, session, false)

    case "md5":
        // MD5 password with salt
        salt := generateSalt()
        if err := s.sendAuthenticationMD5(ctx, salt); err != nil {
            return err
        }
        return s.handleMD5Response(ctx, session, salt)

    case "scram-sha-256":
        // SCRAM-SHA-256 authentication
        return s.handleSCRAMAuth(ctx, session)

    default:
        return fmt.Errorf("unsupported authentication method: %s", s.config.Authentication.Method)
    }
}
```

### Connection Termination

```go
// handleTerminate cleans up when a client disconnects.
func (s *Server) handleTerminate(ctx context.Context) error {
    session := wire.SessionFromContext(ctx)
    if session == nil {
        return nil
    }

    // Roll back any active transaction
    if session.TransactionState != TransactionIdle {
        if _, err := session.BackendConn.Execute(ctx, "ROLLBACK", nil); err != nil {
            s.log.Warn("failed to rollback on disconnect", "err", err)
        }
    }

    // Close prepared statements
    for _, stmt := range session.PreparedStmts {
        if err := stmt.BackendStmt.Close(); err != nil {
            s.log.Warn("failed to close prepared statement", "name", stmt.Name, "err", err)
        }
    }

    // Close backend connection
    if err := session.BackendConn.Close(); err != nil {
        s.log.Warn("failed to close backend connection", "err", err)
    }

    return nil
}
```

---

## Query Execution Flow

### Simple Query Protocol

The simple query protocol handles a single text query string that may contain multiple statements.

```
Client                          Server
   |                               |
   |------- Query Message -------->|  (query string)
   |                               |
   |                               |  [Parse SQL]
   |                               |  [Plan & Execute]
   |                               |
   |<------ RowDescription --------|  (for SELECT)
   |<------ DataRow --------------|  (multiple)
   |<------ CommandComplete ------|
   |<------ ReadyForQuery --------|
   |                               |
```

```go
// handleSimpleQuery processes a simple query message.
func (s *Server) handleSimpleQuery(ctx context.Context, query string) error {
    session := wire.SessionFromContext(ctx)
    writer := wire.DataWriterFromContext(ctx)

    // Handle empty query
    if strings.TrimSpace(query) == "" {
        return s.sendEmptyQueryResponse(ctx)
    }

    // Execute query against backend
    // The query may contain multiple statements separated by semicolons
    // psql-wire handles statement splitting internally

    results, columns, err := session.BackendConn.Query(ctx, query, nil)
    if err != nil {
        return s.sendError(ctx, err)
    }

    // For non-SELECT statements, results will be empty
    if len(columns) == 0 {
        // This was an INSERT/UPDATE/DELETE/DDL statement
        return s.sendCommandComplete(ctx, determineCommandTag(query))
    }

    // Send row description
    fields := s.buildFieldDescriptions(columns, results)
    if err := writer.Define(fields); err != nil {
        return err
    }

    // Send data rows
    for _, row := range results {
        values := make([]any, len(columns))
        for i, col := range columns {
            values[i] = row[col]
        }
        if err := writer.Row(values); err != nil {
            return err
        }
    }

    // Send command complete
    return writer.Complete(fmt.Sprintf("SELECT %d", len(results)))
}
```

### Extended Query Protocol

The extended query protocol separates parsing, binding, and execution for prepared statements.

```
Client                          Server
   |                               |
   |------- Parse --------------->|  (name, query, param types)
   |------- Bind ---------------->|  (portal, stmt, params)
   |------- Describe ------------>|  (statement or portal)
   |------- Execute ------------->|  (portal, max rows)
   |------- Sync ---------------->|  (end of batch)
   |                               |
   |<------ ParseComplete --------|
   |<------ BindComplete ---------|
   |<------ RowDescription -------|
   |<------ DataRow --------------|  (multiple)
   |<------ CommandComplete ------|
   |<------ ReadyForQuery --------|
   |                               |
```

#### Parse Message Handler

```go
// handleParse processes a Parse message (prepare statement).
func (s *Server) handleParse(ctx context.Context, msg *pgproto3.Parse) error {
    session := wire.SessionFromContext(ctx)

    // Prepare the statement via backend
    backendStmt, err := session.BackendConn.Prepare(ctx, msg.Query)
    if err != nil {
        return s.sendError(ctx, err)
    }

    // Store in session's prepared statements
    stmt := &PreparedStatement{
        Name:        msg.Name,
        Query:       msg.Query,
        BackendStmt: backendStmt,
        ParamOIDs:   msg.ParameterOIDs,
    }

    // Unnamed statement (empty name) is special - always replaced
    if msg.Name == "" {
        if old, ok := session.PreparedStmts[""]; ok {
            _ = old.BackendStmt.Close()
        }
    } else if _, ok := session.PreparedStmts[msg.Name]; ok {
        // Named statement already exists - error in PostgreSQL
        return s.sendError(ctx, fmt.Errorf("prepared statement %q already exists", msg.Name))
    }

    session.PreparedStmts[msg.Name] = stmt

    return s.sendParseComplete(ctx)
}
```

#### Bind Message Handler

```go
// handleBind processes a Bind message (bind parameters to prepared statement).
func (s *Server) handleBind(ctx context.Context, msg *pgproto3.Bind) error {
    session := wire.SessionFromContext(ctx)

    // Find the prepared statement
    stmt, ok := session.PreparedStmts[msg.PreparedStatement]
    if !ok {
        return s.sendError(ctx, fmt.Errorf("prepared statement %q does not exist", msg.PreparedStatement))
    }

    // Decode parameter values
    params, err := s.decodeBindParameters(msg, stmt)
    if err != nil {
        return s.sendError(ctx, err)
    }

    // Create portal
    portal := &Portal{
        Name:       msg.DestinationPortal,
        Statement:  stmt,
        Parameters: params,
        ResultFormats: msg.ResultFormatCodes,
    }

    // Unnamed portal (empty name) is special - always replaced
    if msg.DestinationPortal == "" {
        delete(session.Portals, "")
    } else if _, ok := session.Portals[msg.DestinationPortal]; ok {
        return s.sendError(ctx, fmt.Errorf("portal %q already exists", msg.DestinationPortal))
    }

    session.Portals[msg.DestinationPortal] = portal

    return s.sendBindComplete(ctx)
}
```

#### Execute Message Handler

```go
// handleExecute processes an Execute message (run prepared statement).
func (s *Server) handleExecute(ctx context.Context, msg *pgproto3.Execute) error {
    session := wire.SessionFromContext(ctx)
    writer := wire.DataWriterFromContext(ctx)

    // Find the portal
    portal, ok := session.Portals[msg.Portal]
    if !ok {
        return s.sendError(ctx, fmt.Errorf("portal %q does not exist", msg.Portal))
    }

    // Convert parameters to driver.NamedValue
    args := make([]driver.NamedValue, len(portal.Parameters))
    for i, p := range portal.Parameters {
        args[i] = driver.NamedValue{
            Ordinal: i + 1,
            Value:   p,
        }
    }

    // Execute via backend statement
    results, columns, err := portal.Statement.BackendStmt.Query(ctx, args)
    if err != nil {
        return s.sendError(ctx, err)
    }

    // Handle row limit
    maxRows := int(msg.MaxRows)
    if maxRows > 0 && maxRows < len(results) {
        results = results[:maxRows]
        portal.Suspended = true
    }

    // Send results
    if len(columns) > 0 {
        // Already sent RowDescription during Describe
        for _, row := range results {
            values := make([]any, len(columns))
            for i, col := range columns {
                values[i] = row[col]
            }
            if err := writer.Row(values); err != nil {
                return err
            }
        }
    }

    // Send appropriate completion
    if portal.Suspended {
        return s.sendPortalSuspended(ctx)
    }

    return writer.Complete(fmt.Sprintf("SELECT %d", len(results)))
}
```

---

## Session Management

### Session Structure

```go
// Session represents a client connection session.
type Session struct {
    // ID is a unique identifier for this session.
    ID uint64

    // Username is the authenticated username.
    Username string

    // Database is the connected database name.
    Database string

    // StartTime is when the session was created.
    StartTime time.Time

    // BackendConn is the underlying dukdb connection.
    BackendConn dukdb.BackendConn

    // PreparedStmts holds named prepared statements.
    PreparedStmts map[string]*PreparedStatement

    // Portals holds bound portals.
    Portals map[string]*Portal

    // TransactionState tracks transaction status.
    TransactionState TransactionState

    // TransactionIsolation is the current transaction isolation level.
    TransactionIsolation string

    // Parameters holds session-level settings.
    Parameters map[string]string

    // CancelKey is the key for query cancellation.
    CancelKey uint32

    // mu protects concurrent access.
    mu sync.RWMutex
}

// TransactionState represents the transaction status.
type TransactionState int

const (
    // TransactionIdle - not in a transaction block.
    TransactionIdle TransactionState = iota

    // TransactionInProgress - in a transaction block.
    TransactionInProgress

    // TransactionFailed - transaction failed, waiting for ROLLBACK.
    TransactionFailed
)

// PreparedStatement holds a parsed prepared statement.
type PreparedStatement struct {
    Name        string
    Query       string
    BackendStmt dukdb.BackendStmt
    ParamOIDs   []uint32
    Columns     []FieldDescription
}

// Portal holds a bound prepared statement with parameters.
type Portal struct {
    Name          string
    Statement     *PreparedStatement
    Parameters    []any
    ResultFormats []int16
    Suspended     bool
    RowsReturned  int
}
```

### Session-to-Connection Mapping

Each wire protocol session maps to a single dukdb BackendConn. The mapping is 1:1 to ensure:

1. Transaction isolation between sessions
2. Proper prepared statement lifecycle
3. Clean session cleanup on disconnect

```go
// SessionManager manages active sessions.
type SessionManager struct {
    sessions map[uint64]*Session
    backend  dukdb.Backend
    mu       sync.RWMutex
}

// CreateSession creates a new session with a dedicated backend connection.
func (sm *SessionManager) CreateSession(
    username string,
    database string,
    dbPath string,
    config *dukdb.Config,
) (*Session, error) {
    sm.mu.Lock()
    defer sm.mu.Unlock()

    // Open a dedicated backend connection
    conn, err := sm.backend.Open(dbPath, config)
    if err != nil {
        return nil, fmt.Errorf("failed to open backend connection: %w", err)
    }

    session := &Session{
        ID:               generateSessionID(),
        Username:         username,
        Database:         database,
        StartTime:        time.Now(),
        BackendConn:      conn,
        PreparedStmts:    make(map[string]*PreparedStatement),
        Portals:          make(map[string]*Portal),
        TransactionState: TransactionIdle,
        Parameters:       make(map[string]string),
        CancelKey:        generateCancelKey(),
    }

    sm.sessions[session.ID] = session

    return session, nil
}

// CloseSession closes a session and its backend connection.
func (sm *SessionManager) CloseSession(id uint64) error {
    sm.mu.Lock()
    defer sm.mu.Unlock()

    session, ok := sm.sessions[id]
    if !ok {
        return nil
    }

    // Close all prepared statements
    for _, stmt := range session.PreparedStmts {
        _ = stmt.BackendStmt.Close()
    }

    // Close backend connection
    if err := session.BackendConn.Close(); err != nil {
        return err
    }

    delete(sm.sessions, id)

    return nil
}
```

---

## Transaction Handling

### Transaction State Machine

```
                    BEGIN
   [Idle] ----------------------> [InProgress]
     ^                                |
     |     COMMIT                     | (query error)
     +--------------------------------+
     |                                |
     |     ROLLBACK                   v
     +--------------------------- [Failed]
```

### Transaction Commands

```go
// handleTransactionCommand processes BEGIN, COMMIT, ROLLBACK.
func (s *Server) handleTransactionCommand(ctx context.Context, cmd string) error {
    session := wire.SessionFromContext(ctx)

    switch strings.ToUpper(cmd) {
    case "BEGIN", "BEGIN TRANSACTION", "START TRANSACTION":
        return s.handleBegin(ctx, session, cmd)

    case "COMMIT", "END", "END TRANSACTION":
        return s.handleCommit(ctx, session)

    case "ROLLBACK", "ABORT":
        return s.handleRollback(ctx, session)

    default:
        // Check for isolation level specification
        if strings.HasPrefix(strings.ToUpper(cmd), "BEGIN TRANSACTION ISOLATION LEVEL") ||
           strings.HasPrefix(strings.ToUpper(cmd), "START TRANSACTION ISOLATION LEVEL") {
            return s.handleBeginWithIsolation(ctx, session, cmd)
        }
        // Check for SAVEPOINT commands
        if strings.HasPrefix(strings.ToUpper(cmd), "SAVEPOINT ") {
            return s.handleSavepoint(ctx, session, cmd)
        }
        if strings.HasPrefix(strings.ToUpper(cmd), "ROLLBACK TO ") {
            return s.handleRollbackToSavepoint(ctx, session, cmd)
        }
        if strings.HasPrefix(strings.ToUpper(cmd), "RELEASE ") {
            return s.handleReleaseSavepoint(ctx, session, cmd)
        }
    }

    return fmt.Errorf("unknown transaction command: %s", cmd)
}

// handleBegin starts a new transaction.
func (s *Server) handleBegin(ctx context.Context, session *Session, cmd string) error {
    session.mu.Lock()
    defer session.mu.Unlock()

    if session.TransactionState != TransactionIdle {
        // PostgreSQL warns but allows BEGIN in a transaction
        s.log.Warn("BEGIN in active transaction")
    }

    _, err := session.BackendConn.Execute(ctx, cmd, nil)
    if err != nil {
        return err
    }

    session.TransactionState = TransactionInProgress

    return nil
}

// handleCommit commits the current transaction.
func (s *Server) handleCommit(ctx context.Context, session *Session) error {
    session.mu.Lock()
    defer session.mu.Unlock()

    if session.TransactionState == TransactionIdle {
        // PostgreSQL warns but succeeds
        s.log.Warn("COMMIT with no active transaction")
        return nil
    }

    if session.TransactionState == TransactionFailed {
        // Cannot commit a failed transaction
        _, _ = session.BackendConn.Execute(ctx, "ROLLBACK", nil)
        session.TransactionState = TransactionIdle
        return fmt.Errorf("current transaction is aborted, commands ignored until end of transaction block")
    }

    _, err := session.BackendConn.Execute(ctx, "COMMIT", nil)
    session.TransactionState = TransactionIdle

    return err
}

// handleRollback rolls back the current transaction.
func (s *Server) handleRollback(ctx context.Context, session *Session) error {
    session.mu.Lock()
    defer session.mu.Unlock()

    if session.TransactionState == TransactionIdle {
        // PostgreSQL warns but succeeds
        s.log.Warn("ROLLBACK with no active transaction")
        return nil
    }

    _, err := session.BackendConn.Execute(ctx, "ROLLBACK", nil)
    session.TransactionState = TransactionIdle

    return err
}
```

### Transaction Error Handling

When a query fails inside a transaction, PostgreSQL marks the transaction as failed and rejects all further commands until ROLLBACK.

```go
// executeInTransaction wraps query execution with transaction state management.
func (s *Server) executeInTransaction(ctx context.Context, session *Session, query string, args []driver.NamedValue) ([]map[string]any, []string, error) {
    // Check if transaction is in failed state
    if session.TransactionState == TransactionFailed {
        return nil, nil, fmt.Errorf("current transaction is aborted, commands ignored until end of transaction block")
    }

    results, columns, err := session.BackendConn.Query(ctx, query, args)
    if err != nil {
        // Mark transaction as failed if we're in a transaction
        if session.TransactionState == TransactionInProgress {
            session.TransactionState = TransactionFailed
        }
        return nil, nil, err
    }

    return results, columns, nil
}
```

### ReadyForQuery Transaction Indicator

```go
// sendReadyForQuery sends ReadyForQuery with appropriate transaction status.
func (s *Server) sendReadyForQuery(ctx context.Context) error {
    session := wire.SessionFromContext(ctx)

    var status byte
    switch session.TransactionState {
    case TransactionIdle:
        status = 'I' // Idle (not in transaction)
    case TransactionInProgress:
        status = 'T' // In transaction block
    case TransactionFailed:
        status = 'E' // In failed transaction block
    }

    return wire.WriteReadyForQuery(ctx, status)
}
```

---

## Result Formatting

### Field Description Builder

```go
// FieldDescription describes a result column for wire protocol.
type FieldDescription struct {
    Name            string // Column name
    TableOID        uint32 // OID of source table (0 if computed)
    ColumnIndex     int16  // Column number in table (0 if computed)
    TypeOID         uint32 // PostgreSQL type OID
    TypeSize        int16  // Type size (-1 for variable length)
    TypeModifier    int32  // Type modifier (e.g., varchar length)
    Format          int16  // 0 = text, 1 = binary
}

// buildFieldDescriptions creates field descriptions from query result columns.
func (s *Server) buildFieldDescriptions(columns []string, results []map[string]any) []FieldDescription {
    fields := make([]FieldDescription, len(columns))

    for i, col := range columns {
        // Infer type from first row's values
        var duckType dukdb.Type
        if len(results) > 0 {
            duckType = inferDuckDBType(results[0][col])
        } else {
            duckType = dukdb.TYPE_VARCHAR // Default for empty results
        }

        // Map DuckDB type to PostgreSQL OID
        pgOID := types.DuckDBTypeToPostgresOID(duckType)

        fields[i] = FieldDescription{
            Name:         col,
            TableOID:     0, // Not from a specific table
            ColumnIndex:  0,
            TypeOID:      pgOID,
            TypeSize:     types.PostgresTypeSize(pgOID),
            TypeModifier: -1,
            Format:       0, // Text format
        }
    }

    return fields
}
```

### Value Encoding

```go
// encodeValue converts a DuckDB value to PostgreSQL wire format.
func encodeValue(value any, format int16) ([]byte, error) {
    if value == nil {
        return nil, nil // NULL is encoded as nil
    }

    if format == 1 {
        return encodeBinaryValue(value)
    }

    return encodeTextValue(value)
}

// encodeTextValue converts a value to PostgreSQL text format.
func encodeTextValue(value any) ([]byte, error) {
    switch v := value.(type) {
    case bool:
        if v {
            return []byte("t"), nil
        }
        return []byte("f"), nil

    case int8, int16, int32, int64, int:
        return []byte(fmt.Sprintf("%d", v)), nil

    case uint8, uint16, uint32, uint64, uint:
        return []byte(fmt.Sprintf("%d", v)), nil

    case float32:
        return []byte(strconv.FormatFloat(float64(v), 'g', -1, 32)), nil

    case float64:
        return []byte(strconv.FormatFloat(v, 'g', -1, 64)), nil

    case string:
        return []byte(v), nil

    case []byte:
        // Bytea hex format: \xHEXDIGITS
        return []byte("\\x" + hex.EncodeToString(v)), nil

    case time.Time:
        // Timestamp format: YYYY-MM-DD HH:MM:SS.ffffff
        return []byte(v.Format("2006-01-02 15:04:05.999999")), nil

    case [16]byte:
        // UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        return []byte(formatUUID(v)), nil

    case []any:
        // Array format: {elem1,elem2,...}
        return encodeArrayText(v)

    case map[string]any:
        // JSON object
        data, err := json.Marshal(v)
        return data, err

    default:
        // Fallback to string representation
        return []byte(fmt.Sprintf("%v", v)), nil
    }
}
```

---

## Error Handling

### PostgreSQL Error Codes

```go
// Error code constants following PostgreSQL SQLSTATE conventions.
const (
    // Class 00 - Successful Completion
    ErrCodeSuccessfulCompletion = "00000"

    // Class 01 - Warning
    ErrCodeWarning = "01000"

    // Class 02 - No Data
    ErrCodeNoData = "02000"

    // Class 03 - SQL Statement Not Yet Complete
    ErrCodeSQLStatementNotYetComplete = "03000"

    // Class 08 - Connection Exception
    ErrCodeConnectionException = "08000"
    ErrCodeConnectionFailure   = "08006"

    // Class 22 - Data Exception
    ErrCodeDataException        = "22000"
    ErrCodeDivisionByZero       = "22012"
    ErrCodeInvalidTextRepresentation = "22P02"
    ErrCodeNumericValueOutOfRange = "22003"

    // Class 23 - Integrity Constraint Violation
    ErrCodeIntegrityConstraintViolation = "23000"
    ErrCodeNotNullViolation             = "23502"
    ErrCodeForeignKeyViolation          = "23503"
    ErrCodeUniqueViolation              = "23505"
    ErrCodeCheckViolation               = "23514"

    // Class 25 - Invalid Transaction State
    ErrCodeInvalidTransactionState = "25000"
    ErrCodeActiveSQLTransaction    = "25001"
    ErrCodeNoActiveSQLTransaction  = "25P01"

    // Class 40 - Transaction Rollback
    ErrCodeTransactionRollback       = "40000"
    ErrCodeSerializationFailure      = "40001"
    ErrCodeDeadlockDetected          = "40P01"

    // Class 42 - Syntax Error or Access Rule Violation
    ErrCodeSyntaxError            = "42601"
    ErrCodeUndefinedColumn        = "42703"
    ErrCodeUndefinedTable         = "42P01"
    ErrCodeDuplicateColumn        = "42701"
    ErrCodeDuplicateTable         = "42P07"
    ErrCodeAmbiguousColumn        = "42702"

    // Class 53 - Insufficient Resources
    ErrCodeOutOfMemory        = "53200"
    ErrCodeDiskFull           = "53100"

    // Class 55 - Object Not In Prerequisite State
    ErrCodeObjectNotInPrerequisiteState = "55000"

    // Class 57 - Operator Intervention
    ErrCodeQueryCanceled = "57014"

    // Class XX - Internal Error
    ErrCodeInternalError = "XX000"
)
```

### Error Mapping

```go
// mapDukDBError maps a dukdb-go error to PostgreSQL error response.
func mapDukDBError(err error) *pgproto3.ErrorResponse {
    // Check for specific error types
    var dukErr *dukdb.Error
    if errors.As(err, &dukErr) {
        return mapDukDBErrorType(dukErr)
    }

    // Check for common error patterns in error messages
    msg := err.Error()

    switch {
    case strings.Contains(msg, "syntax error"):
        return &pgproto3.ErrorResponse{
            Severity: "ERROR",
            Code:     ErrCodeSyntaxError,
            Message:  msg,
        }

    case strings.Contains(msg, "table") && strings.Contains(msg, "does not exist"):
        return &pgproto3.ErrorResponse{
            Severity: "ERROR",
            Code:     ErrCodeUndefinedTable,
            Message:  msg,
        }

    case strings.Contains(msg, "column") && strings.Contains(msg, "does not exist"):
        return &pgproto3.ErrorResponse{
            Severity: "ERROR",
            Code:     ErrCodeUndefinedColumn,
            Message:  msg,
        }

    case strings.Contains(msg, "unique constraint") || strings.Contains(msg, "duplicate key"):
        return &pgproto3.ErrorResponse{
            Severity: "ERROR",
            Code:     ErrCodeUniqueViolation,
            Message:  msg,
        }

    case strings.Contains(msg, "null value") || strings.Contains(msg, "NOT NULL"):
        return &pgproto3.ErrorResponse{
            Severity: "ERROR",
            Code:     ErrCodeNotNullViolation,
            Message:  msg,
        }

    case strings.Contains(msg, "serialization failure") || strings.Contains(msg, "could not serialize"):
        return &pgproto3.ErrorResponse{
            Severity: "ERROR",
            Code:     ErrCodeSerializationFailure,
            Message:  msg,
        }

    default:
        return &pgproto3.ErrorResponse{
            Severity: "ERROR",
            Code:     ErrCodeInternalError,
            Message:  msg,
        }
    }
}

// mapDukDBErrorType maps specific dukdb.Error types.
func mapDukDBErrorType(err *dukdb.Error) *pgproto3.ErrorResponse {
    switch err.Type {
    case dukdb.ErrorTypeSyntax:
        return &pgproto3.ErrorResponse{
            Severity: "ERROR",
            Code:     ErrCodeSyntaxError,
            Message:  err.Msg,
            Position: int32(err.Position),
        }

    case dukdb.ErrorTypeConstraint:
        return &pgproto3.ErrorResponse{
            Severity: "ERROR",
            Code:     ErrCodeIntegrityConstraintViolation,
            Message:  err.Msg,
        }

    case dukdb.ErrorTypeTransaction:
        return &pgproto3.ErrorResponse{
            Severity: "ERROR",
            Code:     ErrCodeInvalidTransactionState,
            Message:  err.Msg,
        }

    case dukdb.ErrorTypeNotImplemented:
        return &pgproto3.ErrorResponse{
            Severity: "ERROR",
            Code:     "0A000", // feature_not_supported
            Message:  err.Msg,
        }

    default:
        return &pgproto3.ErrorResponse{
            Severity: "ERROR",
            Code:     ErrCodeInternalError,
            Message:  err.Msg,
        }
    }
}
```

---

## Key Design Decisions

### 1. Server Runs In-Process as Goroutine

**Decision**: The PostgreSQL server runs in the same process as the application, started as a goroutine.

**Rationale**:
- Simplest integration model
- Shares memory with the application
- No IPC overhead for database operations
- Easy to start/stop with the application

**Implementation**:
```go
// StartServer starts the PostgreSQL wire protocol server.
// It runs in a goroutine and returns immediately.
func StartServer(config *ServerConfig) (*Server, error) {
    srv := &Server{
        config:   config,
        sessions: NewSessionManager(dukdb.GetBackend()),
    }

    go func() {
        if err := srv.ListenAndServe(); err != nil {
            srv.log.Error("server error", "err", err)
        }
    }()

    return srv, nil
}
```

### 2. No Connection Pooling at Server Level

**Decision**: Each wire protocol session maps to a dedicated BackendConn. No connection pooling at the server level.

**Rationale**:
- Simplifies session state management
- Transactions are session-scoped in PostgreSQL
- Prepared statements are session-scoped
- Client-side connection pools (pgx, HikariCP) handle pooling

**Trade-off**: More memory per connection, but cleaner semantics.

### 3. Both PostgreSQL Wire AND Native Driver Simultaneously

**Decision**: Support both access methods simultaneously to the same database.

**Rationale**:
- Allows migration without breaking existing code
- Enables mixed workloads (ORMs via wire, analytics via native)
- Testing can use either path

**Implementation**:
```go
// Shared Engine backend - both access methods use the same Engine
var sharedEngine = engine.NewEngine()

// Native driver access
db, _ := sql.Open("dukdb", "path/to/db")

// Wire protocol access (to same database)
pgServer, _ := postgres.StartServer(&ServerConfig{
    ListenAddr:   ":5432",
    DatabasePath: "path/to/db",
})

// Both can run concurrently
```

**Considerations**:
- Transaction isolation between wire and native connections works naturally
- Schema changes are visible to both after commit
- Concurrent writes are serialized by the engine's transaction manager

### 4. PostgreSQL Compatibility Mode is Opt-In per Session

**Decision**: PostgreSQL compatibility (function aliases, type mapping) is enabled by default for wire protocol sessions but can be disabled.

**Rationale**:
- Wire protocol clients expect PostgreSQL behavior
- Native driver users may want DuckDB-native behavior
- Allows testing both modes

```go
// Wire protocol sessions get PostgreSQL compat by default
session.PostgresCompatMode = true

// Can be toggled via SET command
// SET dukdb.postgres_compat_mode = off;
```

---

## Implementation Plan

### Phase 1: Server Infrastructure

1. Create server package structure
2. Implement basic server with psql-wire
3. Handle startup sequence (SSL negotiation, startup message)
4. Implement trust authentication
5. Send server parameters

**Deliverables**:
- `internal/postgres/server/server.go`
- `internal/postgres/server/config.go`
- Basic connection test with psql

### Phase 2: Simple Query Protocol

1. Implement simple query handler
2. Integrate with BackendConn.Query
3. Build FieldDescription from results
4. Send DataRow messages
5. Handle multi-statement queries

**Deliverables**:
- `internal/postgres/server/handler.go`
- `internal/postgres/wire/result_writer.go`
- SELECT/INSERT/UPDATE/DELETE via psql

### Phase 3: Session Management

1. Implement Session structure
2. Session-to-BackendConn mapping
3. Session cleanup on disconnect
4. Session parameters storage

**Deliverables**:
- `internal/postgres/server/session.go`
- Connection lifecycle tests

### Phase 4: Extended Query Protocol

1. Implement Parse handler (prepared statements)
2. Implement Bind handler (parameter binding)
3. Implement Describe handler (statement/portal metadata)
4. Implement Execute handler
5. Implement Sync handler

**Deliverables**:
- `internal/postgres/server/prepared.go`
- Prepared statement tests with pgx driver

### Phase 5: Transaction Handling

1. Implement transaction state machine
2. Handle BEGIN/COMMIT/ROLLBACK
3. Handle failed transaction state
4. Implement savepoint support
5. ReadyForQuery transaction indicator

**Deliverables**:
- `internal/postgres/server/transaction.go`
- Transaction isolation tests

### Phase 6: Error Handling

1. Define PostgreSQL error code constants
2. Map dukdb errors to SQLSTATE codes
3. Send ErrorResponse messages
4. Handle error recovery

**Deliverables**:
- `internal/postgres/wire/error.go`
- Error handling tests

### Phase 7: Authentication

1. Implement MD5 authentication
2. Implement SCRAM-SHA-256 authentication
3. Add password callback support
4. TLS configuration

**Deliverables**:
- `internal/postgres/server/auth.go`
- `internal/postgres/server/tls.go`
- Authentication tests

### Phase 8: Integration and Testing

1. Integration tests with psql
2. Integration tests with pgx
3. Integration tests with JDBC
4. Performance benchmarks
5. Documentation

**Deliverables**:
- Integration test suite
- Performance benchmark results
- User documentation

---

## Testing Strategy

### Unit Tests

1. **Handler Tests**
   - Simple query parsing and execution
   - Extended query protocol message handling
   - Error response generation

2. **Session Tests**
   - Session creation and cleanup
   - Prepared statement lifecycle
   - Portal management

3. **Transaction Tests**
   - State transitions
   - Error handling
   - Savepoint support

### Integration Tests

1. **psql Client**
   - Connect and execute basic queries
   - Transaction commands
   - `\d` metadata commands

2. **pgx Driver**
   - Connection pool
   - Prepared statements
   - Type scanning

3. **ORM Tests**
   - GORM connection
   - SQLAlchemy connection
   - TypeORM connection

### Performance Tests

1. **Connection Throughput**
   - Connections per second
   - Concurrent connection limit

2. **Query Latency**
   - Simple query round-trip
   - Prepared statement round-trip

3. **Data Transfer**
   - Large result set streaming
   - Bulk INSERT performance

---

## References

- [PostgreSQL Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol.html)
- [psql-wire Documentation](https://pkg.go.dev/github.com/jeroenrinzema/psql-wire)
- [pgproto3 Documentation](https://pkg.go.dev/github.com/jackc/pgx/v5/pgproto3)
- [Wire Protocol Research](./research.md)
- [Type Mapping Specification](../type-mapping/spec.md)
- [Function Aliases Specification](../function-aliases/spec.md)
