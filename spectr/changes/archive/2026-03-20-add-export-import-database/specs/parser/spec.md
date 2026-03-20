## ADDED Requirements

### Requirement: EXPORT DATABASE Statement Parsing

The parser SHALL parse `EXPORT DATABASE 'path'` with optional parenthesized options including FORMAT specification.

#### Scenario: EXPORT DATABASE with default format

- WHEN parsing `EXPORT DATABASE '/tmp/mydb'`
- THEN the parser produces an ExportDatabaseStmt with Path="/tmp/mydb" and empty Options

#### Scenario: EXPORT DATABASE with FORMAT option

- WHEN parsing `EXPORT DATABASE '/tmp/mydb' (FORMAT PARQUET)`
- THEN the parser produces an ExportDatabaseStmt with Path="/tmp/mydb" and Options={"FORMAT": "PARQUET"}

#### Scenario: EXPORT DATABASE with multiple options

- WHEN parsing `EXPORT DATABASE '/tmp/mydb' (FORMAT CSV, DELIMITER '|', HEADER true)`
- THEN the parser produces an ExportDatabaseStmt with Options containing FORMAT, DELIMITER, and HEADER

### Requirement: IMPORT DATABASE Statement Parsing

The parser SHALL parse `IMPORT DATABASE 'path'` to load a previously exported database.

#### Scenario: IMPORT DATABASE basic

- WHEN parsing `IMPORT DATABASE '/tmp/mydb'`
- THEN the parser produces an ImportDatabaseStmt with Path="/tmp/mydb"
