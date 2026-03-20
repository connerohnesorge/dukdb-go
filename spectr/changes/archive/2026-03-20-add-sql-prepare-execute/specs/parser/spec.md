## ADDED Requirements

### Requirement: PREPARE Statement Parsing

The parser SHALL parse `PREPARE name AS statement` to create named prepared statements with parameter placeholders.

#### Scenario: PREPARE a SELECT statement

- WHEN parsing `PREPARE my_query AS SELECT * FROM users WHERE id = $1`
- THEN the parser produces a PrepareStmt with Name="my_query"
- AND Inner is a SelectStmt with a $1 parameter placeholder

#### Scenario: PREPARE an INSERT statement

- WHEN parsing `PREPARE my_insert AS INSERT INTO t (a, b) VALUES ($1, $2)`
- THEN the parser produces a PrepareStmt with Name="my_insert"
- AND Inner is an InsertStmt with $1 and $2 parameter placeholders

#### Scenario: PREPARE a DELETE statement

- WHEN parsing `PREPARE my_delete AS DELETE FROM t WHERE id = $1`
- THEN the parser produces a PrepareStmt with Name="my_delete"
- AND Inner is a DeleteStmt

### Requirement: EXECUTE Statement Parsing

The parser SHALL parse `EXECUTE name` and `EXECUTE name(params)` for executing named prepared statements.

#### Scenario: EXECUTE without parameters

- WHEN parsing `EXECUTE my_query`
- THEN the parser produces an ExecuteStmt with Name="my_query" and empty Params

#### Scenario: EXECUTE with parameters

- WHEN parsing `EXECUTE my_query(42, 'hello')`
- THEN the parser produces an ExecuteStmt with Name="my_query"
- AND Params contains IntLiteral(42) and StringLiteral('hello')

#### Scenario: EXECUTE with expressions as parameters

- WHEN parsing `EXECUTE my_query(1 + 2, CURRENT_DATE)`
- THEN the parser produces an ExecuteStmt with expression parameters
- AND Params[0] is a BinaryExpr (1 + 2)

### Requirement: DEALLOCATE Statement Parsing

The parser SHALL parse `DEALLOCATE [PREPARE] name` and `DEALLOCATE ALL` for releasing prepared statements.

#### Scenario: DEALLOCATE by name

- WHEN parsing `DEALLOCATE my_query`
- THEN the parser produces a DeallocateStmt with Name="my_query"

#### Scenario: DEALLOCATE PREPARE by name

- WHEN parsing `DEALLOCATE PREPARE my_query`
- THEN the parser produces a DeallocateStmt with Name="my_query"

#### Scenario: DEALLOCATE ALL

- WHEN parsing `DEALLOCATE ALL`
- THEN the parser produces a DeallocateStmt with Name="" (empty, meaning all)
