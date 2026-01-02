## ADDED Requirements

### Requirement: CREATE VIEW Statement
The parser SHALL parse `CREATE VIEW` statements with the following syntax:
```
CREATE VIEW [IF NOT EXISTS] [schema_name.]view_name AS select_statement
```

#### Scenario: Basic CREATE VIEW
- WHEN parsing `CREATE VIEW my_view AS SELECT * FROM t`
- THEN CreateViewStmt SHALL have View="my_view", Schema="main", IfNotExists=false
- THEN CreateViewStmt.Query SHALL be a bound SelectStmt

#### Scenario: CREATE VIEW with schema and IF NOT EXISTS
- WHEN parsing `CREATE VIEW IF NOT EXISTS schema.my_view AS SELECT id, name FROM t`
- THEN CreateViewStmt SHALL have View="my_view", Schema="schema", IfNotExists=true

#### Scenario: CREATE VIEW with complex SELECT
- WHEN parsing `CREATE VIEW v AS SELECT a, SUM(b) AS total FROM t GROUP BY a`
- THEN CreateViewStmt.Query SHALL contain all SELECT clause elements

### Requirement: DROP VIEW Statement
The parser SHALL parse `DROP VIEW` statements with the following syntax:
```
DROP VIEW [IF EXISTS] [schema_name.]view_name
```

#### Scenario: Basic DROP VIEW
- WHEN parsing `DROP VIEW my_view`
- THEN DropViewStmt SHALL have View="my_view", Schema="main", IfExists=false

#### Scenario: DROP VIEW with IF EXISTS
- WHEN parsing `DROP VIEW IF EXISTS schema.my_view`
- THEN DropViewStmt SHALL have View="my_view", Schema="schema", IfExists=true

### Requirement: CREATE INDEX Statement
The parser SHALL parse `CREATE INDEX` statements with the following syntax:
```
CREATE [UNIQUE] INDEX [IF NOT EXISTS] index_name ON table_name (column_name [, ...])
```

#### Scenario: Basic CREATE INDEX
- WHEN parsing `CREATE INDEX idx ON t (col1)`
- THEN CreateIndexStmt SHALL have Index="idx", Table="t", Columns=["col1"], IsUnique=false

#### Scenario: CREATE UNIQUE INDEX
- WHEN parsing `CREATE UNIQUE INDEX idx ON t (col1, col2)`
- THEN CreateIndexStmt SHALL have IsUnique=true, Columns=["col1", "col2"]

#### Scenario: CREATE INDEX with schema
- WHEN parsing `CREATE INDEX IF NOT EXISTS schema.idx ON schema.t (col1)`
- THEN CreateIndexStmt SHALL have Schema="schema", Index="idx", Table="t"

### Requirement: DROP INDEX Statement
The parser SHALL parse `DROP INDEX` statements with the following syntax:
```
DROP INDEX [IF EXISTS] [schema_name.]index_name
```

#### Scenario: Basic DROP INDEX
- WHEN parsing `DROP INDEX my_idx`
- THEN DropIndexStmt SHALL have Index="my_idx", Schema="main", IfExists=false

### Requirement: CREATE SEQUENCE Statement
The parser SHALL parse `CREATE SEQUENCE` statements with the following syntax:
```
CREATE SEQUENCE [IF NOT EXISTS] [schema_name.]sequence_name
[START WITH start_value]
[INCREMENT BY increment_value]
[MINVALUE min_value | NO MINVALUE]
[MAXVALUE max_value | NO MAXVALUE]
[CYCLE | NO CYCLE]
```

#### Scenario: Basic CREATE SEQUENCE
- WHEN parsing `CREATE SEQUENCE seq`
- THEN CreateSequenceStmt SHALL have Sequence="seq", Schema="main"

#### Scenario: CREATE SEQUENCE with all options
- WHEN parsing `CREATE SEQUENCE seq START WITH 100 INCREMENT BY 2 MINVALUE 1 MAXVALUE 1000 CYCLE`
- THEN CreateSequenceStmt SHALL have StartWith=100, IncrementBy=2, MinValue=1, MaxValue=1000, IsCycle=true

#### Scenario: CREATE SEQUENCE with NO MINVALUE/NO MAXVALUE
- WHEN parsing `CREATE SEQUENCE seq INCREMENT BY -1 NO MINVALUE NO MAXVALUE`
- THEN CreateSequenceStmt SHALL have MinValue=nil, MaxValue=nil

### Requirement: DROP SEQUENCE Statement
The parser SHALL parse `DROP SEQUENCE` statements with the following syntax:
```
DROP SEQUENCE [IF EXISTS] [schema_name.]sequence_name
```

#### Scenario: Basic DROP SEQUENCE
- WHEN parsing `DROP SEQUENCE my_seq`
- THEN DropSequenceStmt SHALL have Sequence="my_seq", Schema="main"

### Requirement: CREATE SCHEMA Statement
The parser SHALL parse `CREATE SCHEMA` statements with the following syntax:
```
CREATE SCHEMA [IF NOT EXISTS] schema_name
```

#### Scenario: Basic CREATE SCHEMA
- WHEN parsing `CREATE SCHEMA my_schema`
- THEN CreateSchemaStmt SHALL have Schema="my_schema", IfNotExists=false

#### Scenario: CREATE SCHEMA with IF NOT EXISTS
- WHEN parsing `CREATE SCHEMA IF NOT EXISTS my_schema`
- THEN CreateSchemaStmt SHALL have IfNotExists=true

### Requirement: DROP SCHEMA Statement
The parser SHALL parse `DROP SCHEMA` statements with the following syntax:
```
DROP SCHEMA [IF EXISTS] schema_name [CASCADE | RESTRICT]
```

#### Scenario: Basic DROP SCHEMA
- WHEN parsing `DROP SCHEMA my_schema`
- THEN DropSchemaStmt SHALL have Schema="my_schema", IfExists=false, Cascade=false

#### Scenario: DROP SCHEMA with CASCADE
- WHEN parsing `DROP SCHEMA my_schema CASCADE`
- THEN DropSchemaStmt SHALL have Cascade=true

### Requirement: ALTER TABLE Statement (Extended)
The parser SHALL parse `ALTER TABLE` statements with additional operations:
```
ALTER TABLE [IF EXISTS] [schema_name.]table_name
  RENAME TO new_table_name
  | RENAME COLUMN old_name TO new_name
  | DROP COLUMN column_name
  | SET (option = value [, ...])
```

#### Scenario: ALTER TABLE RENAME TO
- WHEN parsing `ALTER TABLE t RENAME TO new_t`
- THEN AlterTableStmt SHALL have Operation=AlterTableRenameTo, NewTableName="new_t"

#### Scenario: ALTER TABLE RENAME COLUMN
- WHEN parsing `ALTER TABLE t RENAME COLUMN old_col TO new_col`
- THEN AlterTableStmt SHALL have Operation=AlterTableRenameColumn, OldColumn="old_col", NewColumn="new_col"

#### Scenario: ALTER TABLE DROP COLUMN
- WHEN parsing `ALTER TABLE t DROP COLUMN col`
- THEN AlterTableStmt SHALL have Operation=AlterTableDropColumn, Column="col"

### Requirement: DDL Statement Type Detection
The parser SHALL correctly identify DDL statement types for the `Statement.Type()` interface.

#### Scenario: Statement type for CREATE VIEW
- WHEN calling `Type()` on a parsed `CreateViewStmt`
- THEN it SHALL return `dukdb.STATEMENT_TYPE_CREATE`

#### Scenario: Statement type for DROP VIEW
- WHEN calling `Type()` on a parsed `DropViewStmt`
- THEN it SHALL return `dukdb.STATEMENT_TYPE_DROP`

#### Scenario: Statement type for other DDL
- WHEN calling `Type()` on any parsed DDL statement
- THEN it SHALL return the appropriate statement type (CREATE or DROP)

### Requirement: DDL Parameter Collection
The parser SHALL correctly collect parameter placeholders from DDL statements.

#### Scenario: Parameters in CREATE VIEW AS SELECT
- WHEN collecting parameters from `CREATE VIEW v AS SELECT * FROM t WHERE id = ?`
- THEN the collector SHALL return a single parameter at position 1

#### Scenario: Parameters in CREATE SEQUENCE
- WHEN collecting parameters from `CREATE SEQUENCE s START WITH ?`
- THEN the collector SHALL return a single parameter at position 1

### Requirement: DDL Parameter Counting
The parser SHALL correctly count parameters in DDL statements.

#### Scenario: Parameter count in DDL
- WHEN counting parameters in `CREATE VIEW v AS SELECT * FROM t WHERE id = $1 AND name = $2`
- THEN the counter SHALL return 2
