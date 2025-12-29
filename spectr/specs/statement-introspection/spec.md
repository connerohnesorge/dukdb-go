# Statement Introspection Specification

## Requirements

### Requirement: Statement Type Detection

The prepared statement SHALL report its type.

#### Scenario: SELECT statement type
- GIVEN prepared statement "SELECT * FROM t"
- WHEN calling StatementType()
- THEN STATEMENT_TYPE_SELECT is returned

#### Scenario: INSERT statement type
- GIVEN prepared statement "INSERT INTO t VALUES (1)"
- WHEN calling StatementType()
- THEN STATEMENT_TYPE_INSERT is returned

#### Scenario: UPDATE statement type
- GIVEN prepared statement "UPDATE t SET x = 1"
- WHEN calling StatementType()
- THEN STATEMENT_TYPE_UPDATE is returned

#### Scenario: DELETE statement type
- GIVEN prepared statement "DELETE FROM t"
- WHEN calling StatementType()
- THEN STATEMENT_TYPE_DELETE is returned

### Requirement: Parameter Metadata

The prepared statement SHALL provide parameter metadata.

#### Scenario: NumInput returns parameter count
- GIVEN prepared statement "SELECT $1, $2"
- WHEN calling NumInput()
- THEN 2 is returned

#### Scenario: ParamName for named parameter
- GIVEN prepared statement "SELECT @name"
- WHEN calling ParamName(1)
- THEN "name" is returned

#### Scenario: ParamType for typed parameter
- GIVEN prepared statement with INTEGER parameter
- WHEN calling ParamType(1)
- THEN TYPE_INTEGER is returned

#### Scenario: ParamName index out of range
- GIVEN prepared statement with 2 parameters
- WHEN calling ParamName(5)
- THEN error is returned

### Requirement: Column Metadata

The prepared statement SHALL provide result column metadata.

#### Scenario: ColumnCount returns column count
- GIVEN prepared statement "SELECT a, b, c FROM t"
- WHEN calling ColumnCount()
- THEN 3 is returned

#### Scenario: ColumnName returns column name
- GIVEN prepared statement "SELECT name FROM t"
- WHEN calling ColumnName(0)
- THEN "name" is returned

#### Scenario: ColumnType returns column type
- GIVEN prepared statement with INTEGER column
- WHEN calling ColumnType(0)
- THEN TYPE_INTEGER is returned

#### Scenario: ColumnTypeInfo for nested type
- GIVEN prepared statement with LIST(INTEGER) column
- WHEN calling ColumnTypeInfo(0)
- THEN TypeInfo with child type is returned

### Requirement: Parameter Binding

The prepared statement SHALL support parameter binding.

#### Scenario: Bind single parameter
- GIVEN prepared statement with 1 parameter
- WHEN calling Bind(1, 42)
- THEN no error is returned

#### Scenario: Bind multiple parameters
- GIVEN prepared statement with 3 parameters
- WHEN calling Bind for each
- THEN all parameters are bound

#### Scenario: Bind index out of range
- GIVEN prepared statement with 2 parameters
- WHEN calling Bind(5, value)
- THEN error is returned

### Requirement: Bound Execution

The prepared statement SHALL execute with bound parameters.

#### Scenario: ExecBound executes DML
- GIVEN bound INSERT statement
- WHEN calling ExecBound()
- THEN row is inserted

#### Scenario: QueryBound returns results
- GIVEN bound SELECT statement
- WHEN calling QueryBound()
- THEN Rows with results is returned

#### Scenario: Execute without binding
- GIVEN statement with unbound parameters
- WHEN calling ExecBound()
- THEN error is returned for missing parameters

