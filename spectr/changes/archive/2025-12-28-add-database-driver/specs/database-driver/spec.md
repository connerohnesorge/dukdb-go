## ADDED Requirements

### Requirement: DSN Parsing

The package SHALL parse connection strings matching dukdb-go format.

#### Scenario: In-memory database DSN
- GIVEN DSN `:memory:`
- WHEN calling ParseDSN()
- THEN Config.Path is ":memory:"

#### Scenario: Empty DSN
- GIVEN empty string DSN ""
- WHEN calling ParseDSN()
- THEN Config.Path is ":memory:" (same as :memory:)

#### Scenario: File database DSN
- GIVEN DSN "/path/to/database.db"
- WHEN calling ParseDSN()
- THEN Config.Path is "/path/to/database.db"

#### Scenario: Relative path DSN
- GIVEN DSN "./database.db"
- WHEN calling ParseDSN()
- THEN Config.Path is "./database.db" (relative to process cwd)

#### Scenario: DSN with access_mode option
- GIVEN DSN "/path/to/db.db?access_mode=read_only"
- WHEN calling ParseDSN()
- THEN Config.AccessMode is "read_only"

#### Scenario: DSN with threads option
- GIVEN DSN ":memory:?threads=4"
- WHEN calling ParseDSN()
- THEN Config.Threads is 4

#### Scenario: DSN with max_memory option (uppercase)
- GIVEN DSN ":memory:?max_memory=4GB"
- WHEN calling ParseDSN()
- THEN Config.MaxMemory is "4GB"

#### Scenario: DSN with max_memory option (lowercase)
- GIVEN DSN ":memory:?max_memory=4gb"
- WHEN calling ParseDSN()
- THEN Config.MaxMemory is "4GB" (normalized to uppercase)

#### Scenario: DSN with multiple options
- GIVEN DSN ":memory:?access_mode=read_only&threads=4&max_memory=2GB"
- WHEN calling ParseDSN()
- THEN Config.AccessMode is "read_only", Threads is 4, MaxMemory is "2GB"

#### Scenario: Invalid access_mode value
- GIVEN DSN with access_mode=invalid_value
- WHEN calling ParseDSN()
- THEN error of type ErrorTypeSettings is returned
- AND error message is "invalid access_mode: invalid_value"

#### Scenario: Invalid access_mode case (uppercase)
- GIVEN DSN with access_mode=READ_ONLY
- WHEN calling ParseDSN()
- THEN error of type ErrorTypeSettings is returned
- AND error message is "invalid access_mode: READ_ONLY"

#### Scenario: Invalid threads value (non-numeric)
- GIVEN DSN with threads=abc
- WHEN calling ParseDSN()
- THEN error of type ErrorTypeSettings is returned
- AND error message is "invalid threads: abc"

#### Scenario: Invalid threads value (zero)
- GIVEN DSN with threads=0
- WHEN calling ParseDSN()
- THEN error of type ErrorTypeSettings is returned
- AND error message is "threads must be 1-128, got 0"

#### Scenario: Invalid threads value (too high)
- GIVEN DSN with threads=200
- WHEN calling ParseDSN()
- THEN error of type ErrorTypeSettings is returned
- AND error message is "threads must be 1-128, got 200"

#### Scenario: Invalid max_memory value
- GIVEN DSN with max_memory=invalid
- WHEN calling ParseDSN()
- THEN error of type ErrorTypeSettings is returned
- AND error message is "invalid max_memory: invalid"

#### Scenario: Unknown option
- GIVEN DSN with unknown option "?invalid_option=value"
- WHEN calling ParseDSN()
- THEN error of type ErrorTypeSettings is returned
- AND error message is "unknown option: invalid_option"

#### Scenario: Mixed valid and invalid options
- GIVEN DSN ":memory:?threads=4&invalid_option=value"
- WHEN calling ParseDSN()
- THEN error of type ErrorTypeSettings is returned
- AND error message is "unknown option: invalid_option"

### Requirement: Driver Registration

The package SHALL register with database/sql as "dukdb".

#### Scenario: Driver available after import
- GIVEN import _ "github.com/.../dukdb"
- WHEN calling sql.Open("dukdb", dsn)
- THEN connection is opened without "unknown driver" error

#### Scenario: OpenConnector returns Connector
- GIVEN driver registered as "dukdb"
- WHEN calling driver.OpenConnector(dsn)
- THEN *Connector implementing driver.Connector is returned

### Requirement: Connector Implementation

The package SHALL implement driver.Connector for connection pooling.

#### Scenario: Connect returns new connection
- GIVEN a Connector with valid config
- WHEN calling Connect(ctx)
- THEN new *Conn is returned

#### Scenario: Connect respects context cancellation
- GIVEN cancelled context
- WHEN calling Connect(ctx)
- THEN context.Canceled is returned

#### Scenario: Connector creates backend lazily
- GIVEN a Connector
- WHEN Connector is created via OpenConnector()
- THEN backend is nil (not yet created)
- AND first Connect() call creates the backend via sync.Once

#### Scenario: Connector reuses backend
- GIVEN a Connector
- WHEN Connect is called multiple times
- THEN all connections share the same backend instance

#### Scenario: Backend creation failure persists
- GIVEN a Connector with invalid config
- WHEN first Connect() fails during backend creation
- THEN subsequent Connect() calls return same initErr

#### Scenario: Driver method returns driver
- GIVEN a Connector
- WHEN calling Driver()
- THEN new *Driver{} is returned (stateless)

### Requirement: Connection Health

The package SHALL validate connection health.

#### Scenario: Ping healthy connection
- GIVEN an open, healthy connection
- WHEN calling Ping(ctx)
- THEN nil is returned

#### Scenario: Ping crashed backend
- GIVEN a connection whose backend has crashed
- WHEN calling Ping(ctx)
- THEN ErrorTypeConnection is returned

#### Scenario: Ping with cancelled context
- GIVEN cancelled context
- WHEN calling Ping(ctx)
- THEN context.Canceled is returned

#### Scenario: Ping with deadline exceeded
- GIVEN context with past deadline
- WHEN calling Ping(ctx)
- THEN context.DeadlineExceeded is returned

### Requirement: Session Management

The package SHALL manage session state for connection pooling.

#### Scenario: ResetSession clears transaction
- GIVEN connection with uncommitted transaction (inTx = true)
- WHEN ResetSession is called
- THEN ROLLBACK is executed via backend
- AND inTx is set to false

#### Scenario: ResetSession on clean connection
- GIVEN connection with no active transaction (inTx = false)
- WHEN ResetSession is called
- THEN no ROLLBACK is executed
- AND nil is returned

#### Scenario: ResetSession ROLLBACK failure
- GIVEN connection with active transaction
- WHEN ResetSession is called and ROLLBACK fails
- THEN inTx is still set to false
- AND the ROLLBACK error is returned

#### Scenario: ResetSession with cancelled context
- GIVEN cancelled context
- WHEN ResetSession is called
- THEN ROLLBACK is still attempted (cleanup is critical)
- AND inTx is set to false regardless

#### Scenario: IsValid for healthy connection
- GIVEN healthy connection with alive backend
- WHEN IsValid is called
- THEN returns true

#### Scenario: IsValid for crashed backend
- GIVEN connection with crashed backend (IsAlive() returns false)
- WHEN IsValid is called
- THEN returns false

### Requirement: Configuration Defaults

The package SHALL apply sensible defaults for omitted options.

#### Scenario: Default access_mode
- GIVEN DSN without access_mode option
- WHEN calling ParseDSN()
- THEN Config.AccessMode is "automatic"

#### Scenario: Default threads
- GIVEN DSN without threads option
- WHEN calling ParseDSN()
- THEN Config.Threads is runtime.NumCPU() clamped to 1-128

#### Scenario: Default threads clamping (low)
- GIVEN runtime.NumCPU() returns 0
- WHEN applying default threads
- THEN Config.Threads is 1

#### Scenario: Default threads clamping (high)
- GIVEN runtime.NumCPU() returns 256
- WHEN applying default threads
- THEN Config.Threads is 128

#### Scenario: Default max_memory
- GIVEN DSN without max_memory option
- WHEN calling ParseDSN()
- THEN Config.MaxMemory is "80%"

#### Scenario: max_memory percentage resolution
- GIVEN Config.MaxMemory is "80%" and system has 16GB RAM
- WHEN resolving max_memory for ProcessBackend
- THEN absolute value approximately 12.8GB is passed to backend

### Requirement: Thread Safety

The package SHALL be safe for concurrent use.

#### Scenario: Concurrent sql.DB access
- GIVEN single sql.DB instance
- WHEN accessed from 100 goroutines simultaneously
- THEN no data races occur (go test -race passes)

#### Scenario: Concurrent Connector.Connect
- GIVEN single Connector
- WHEN Connect called from 10 goroutines
- THEN all connections created successfully
- AND backend created exactly once via sync.Once

#### Scenario: Connection pool stress test
- GIVEN sql.DB with SetMaxOpenConns(10)
- WHEN 100 goroutines perform queries
- THEN all queries complete successfully
- AND maximum 10 concurrent connections used

#### Scenario: Conn serialization
- GIVEN single Conn
- WHEN accessed from multiple goroutines
- THEN operations are serialized via mutex
- AND no data races occur

### Requirement: Config Immutability

The package SHALL ensure configuration is immutable after creation.

#### Scenario: Config fields are immutable
- GIVEN Config returned from ParseDSN()
- WHEN attempting to modify Config fields
- THEN fields cannot be changed (no setter methods)

#### Scenario: Connector config is immutable
- GIVEN Connector with parsed Config
- WHEN Connector is used for multiple Connect() calls
- THEN same Config is used for all connections
