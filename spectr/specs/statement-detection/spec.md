# Statement Detection Specification

## Requirements

### Requirement: Statement Type Constants

The system SHALL provide constants for all 30 DuckDB statement types.

#### Scenario: Core DML types defined
- GIVEN the statement type constants
- WHEN checking for DML types
- THEN STATEMENT_TYPE_SELECT is defined and unique
- AND STATEMENT_TYPE_INSERT is defined and unique
- AND STATEMENT_TYPE_UPDATE is defined and unique
- AND STATEMENT_TYPE_DELETE is defined and unique
- AND all four have different values

#### Scenario: DDL types defined
- GIVEN the statement type constants
- WHEN checking for DDL types
- THEN STATEMENT_TYPE_CREATE is defined
- AND STATEMENT_TYPE_DROP is defined
- AND STATEMENT_TYPE_ALTER is defined

#### Scenario: New statement types defined
- GIVEN the complete statement type constants
- WHEN checking for new types
- THEN STATEMENT_TYPE_MERGE_INTO equals 28
- AND STATEMENT_TYPE_UPDATE_EXTENSIONS equals 29
- AND STATEMENT_TYPE_COPY_DATABASE equals 30

#### Scenario: Statement type name
- GIVEN statement type STATEMENT_TYPE_SELECT
- WHEN calling StmtTypeName(type)
- THEN returns "SELECT"

#### Scenario: All types have names
- GIVEN any valid StmtType value
- WHEN calling String()
- THEN returns non-empty string

### Requirement: Statement Return Type

The system SHALL classify statements by what they return.

#### Scenario: SELECT returns rows
- GIVEN statement type STATEMENT_TYPE_SELECT
- WHEN calling ReturnType()
- THEN returns RETURN_QUERY_RESULT

#### Scenario: INSERT returns changed count
- GIVEN statement type STATEMENT_TYPE_INSERT
- WHEN calling ReturnType()
- THEN returns RETURN_CHANGED_ROWS

#### Scenario: CREATE returns nothing
- GIVEN statement type STATEMENT_TYPE_CREATE
- WHEN calling ReturnType()
- THEN returns RETURN_NOTHING

#### Scenario: EXPLAIN returns rows
- GIVEN statement type STATEMENT_TYPE_EXPLAIN
- WHEN calling ReturnType()
- THEN returns RETURN_QUERY_RESULT

#### Scenario: PRAGMA returns rows
- GIVEN statement type STATEMENT_TYPE_PRAGMA
- WHEN calling ReturnType()
- THEN returns RETURN_QUERY_RESULT

### Requirement: Statement Classification Methods

The system SHALL provide classification methods on StmtType.

#### Scenario: IsDML for INSERT
- GIVEN statement type STATEMENT_TYPE_INSERT
- WHEN calling IsDML()
- THEN returns true

#### Scenario: IsDML for UPDATE
- GIVEN statement type STATEMENT_TYPE_UPDATE
- WHEN calling IsDML()
- THEN returns true

#### Scenario: IsDML for DELETE
- GIVEN statement type STATEMENT_TYPE_DELETE
- WHEN calling IsDML()
- THEN returns true

#### Scenario: IsDML for MERGE_INTO
- GIVEN statement type STATEMENT_TYPE_MERGE_INTO
- WHEN calling IsDML()
- THEN returns true

#### Scenario: IsDML for SELECT
- GIVEN statement type STATEMENT_TYPE_SELECT
- WHEN calling IsDML()
- THEN returns false

#### Scenario: IsDDL for CREATE
- GIVEN statement type STATEMENT_TYPE_CREATE
- WHEN calling IsDDL()
- THEN returns true

#### Scenario: IsDDL for DROP
- GIVEN statement type STATEMENT_TYPE_DROP
- WHEN calling IsDDL()
- THEN returns true

#### Scenario: IsDDL for ALTER
- GIVEN statement type STATEMENT_TYPE_ALTER
- WHEN calling IsDDL()
- THEN returns true

#### Scenario: IsDDL for SELECT
- GIVEN statement type STATEMENT_TYPE_SELECT
- WHEN calling IsDDL()
- THEN returns false

#### Scenario: IsQuery for SELECT
- GIVEN statement type STATEMENT_TYPE_SELECT
- WHEN calling IsQuery()
- THEN returns true

#### Scenario: IsQuery for INSERT
- GIVEN statement type STATEMENT_TYPE_INSERT
- WHEN calling IsQuery()
- THEN returns false

#### Scenario: ModifiesData for INSERT
- GIVEN statement type STATEMENT_TYPE_INSERT
- WHEN calling ModifiesData()
- THEN returns true

#### Scenario: ModifiesData for CREATE
- GIVEN statement type STATEMENT_TYPE_CREATE
- WHEN calling ModifiesData()
- THEN returns true

#### Scenario: ModifiesData for SELECT
- GIVEN statement type STATEMENT_TYPE_SELECT
- WHEN calling ModifiesData()
- THEN returns false

### Requirement: Statement Properties

The system SHALL provide statement properties struct.

#### Scenario: Properties struct fields
- GIVEN a StmtProperties struct
- WHEN inspecting fields
- THEN Type field is StmtType
- AND ReturnType field is StmtReturnType
- AND IsReadOnly field is bool
- AND IsStreaming field is bool
- AND ColumnCount field is int
- AND ParamCount field is int

#### Scenario: SELECT properties
- GIVEN a prepared SELECT statement
- WHEN calling Properties()
- THEN Type is STATEMENT_TYPE_SELECT
- AND ReturnType is RETURN_QUERY_RESULT
- AND IsReadOnly is true
- AND IsStreaming is true

#### Scenario: INSERT properties
- GIVEN a prepared INSERT statement
- WHEN calling Properties()
- THEN Type is STATEMENT_TYPE_INSERT
- AND ReturnType is RETURN_CHANGED_ROWS
- AND IsReadOnly is false
- AND IsStreaming is false

#### Scenario: CREATE TABLE properties
- GIVEN a prepared CREATE TABLE statement
- WHEN calling Properties()
- THEN Type is STATEMENT_TYPE_CREATE
- AND ReturnType is RETURN_NOTHING
- AND IsReadOnly is false

### Requirement: Public Stmt API

The system SHALL expose statement properties on Stmt type.

#### Scenario: StatementType from Stmt
- GIVEN a prepared statement for "SELECT 1"
- WHEN calling stmt.StatementType()
- THEN returns STATEMENT_TYPE_SELECT
- AND error is nil

#### Scenario: Properties from Stmt
- GIVEN a prepared statement for "INSERT INTO t VALUES (1)"
- WHEN calling stmt.Properties()
- THEN returns StmtProperties with Type STATEMENT_TYPE_INSERT
- AND error is nil

#### Scenario: IsReadOnly from Stmt
- GIVEN a prepared statement for "SELECT 1"
- WHEN calling stmt.IsReadOnly()
- THEN returns true
- AND error is nil

#### Scenario: IsQuery from Stmt
- GIVEN a prepared statement for "SELECT 1"
- WHEN calling stmt.IsQuery()
- THEN returns true
- AND error is nil

#### Scenario: Closed statement error
- GIVEN a closed prepared statement
- WHEN calling stmt.StatementType()
- THEN error indicates statement is closed

#### Scenario: Closed statement properties error
- GIVEN a closed prepared statement
- WHEN calling stmt.Properties()
- THEN error indicates statement is closed

### Requirement: Parser Statement Type Support

The system SHALL return correct type from parser AST.

#### Scenario: SELECT AST type
- GIVEN parsed SELECT statement
- WHEN calling statement.Type()
- THEN returns STATEMENT_TYPE_SELECT

#### Scenario: INSERT AST type
- GIVEN parsed INSERT statement
- WHEN calling statement.Type()
- THEN returns STATEMENT_TYPE_INSERT

#### Scenario: UPDATE AST type
- GIVEN parsed UPDATE statement
- WHEN calling statement.Type()
- THEN returns STATEMENT_TYPE_UPDATE

#### Scenario: DELETE AST type
- GIVEN parsed DELETE statement
- WHEN calling statement.Type()
- THEN returns STATEMENT_TYPE_DELETE

#### Scenario: CREATE TABLE AST type
- GIVEN parsed CREATE TABLE statement
- WHEN calling statement.Type()
- THEN returns STATEMENT_TYPE_CREATE

#### Scenario: DROP TABLE AST type
- GIVEN parsed DROP TABLE statement
- WHEN calling statement.Type()
- THEN returns STATEMENT_TYPE_DROP

#### Scenario: EXPLAIN AST type
- GIVEN parsed EXPLAIN statement
- WHEN calling statement.Type()
- THEN returns STATEMENT_TYPE_EXPLAIN

### Requirement: Read-Only Detection

The system SHALL correctly identify read-only statements.

#### Scenario: SELECT is read-only
- GIVEN a prepared SELECT statement
- WHEN checking IsReadOnly property
- THEN returns true

#### Scenario: EXPLAIN is read-only
- GIVEN a prepared EXPLAIN statement
- WHEN checking IsReadOnly property
- THEN returns true

#### Scenario: PRAGMA is read-only
- GIVEN a prepared PRAGMA statement
- WHEN checking IsReadOnly property
- THEN returns true

#### Scenario: INSERT is not read-only
- GIVEN a prepared INSERT statement
- WHEN checking IsReadOnly property
- THEN returns false

#### Scenario: UPDATE is not read-only
- GIVEN a prepared UPDATE statement
- WHEN checking IsReadOnly property
- THEN returns false

#### Scenario: DELETE is not read-only
- GIVEN a prepared DELETE statement
- WHEN checking IsReadOnly property
- THEN returns false

#### Scenario: CREATE is not read-only
- GIVEN a prepared CREATE statement
- WHEN checking IsReadOnly property
- THEN returns false

#### Scenario: PREPARE is read-only
- GIVEN a prepared PREPARE statement
- WHEN checking IsReadOnly property
- THEN returns true

#### Scenario: RELATION is read-only
- GIVEN statement type STATEMENT_TYPE_RELATION
- WHEN checking IsReadOnly
- THEN returns true

#### Scenario: LOGICAL_PLAN is read-only
- GIVEN statement type STATEMENT_TYPE_LOGICAL_PLAN
- WHEN checking IsReadOnly
- THEN returns true

