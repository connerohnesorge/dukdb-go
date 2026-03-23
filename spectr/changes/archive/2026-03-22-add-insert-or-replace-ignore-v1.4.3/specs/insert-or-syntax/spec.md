# INSERT OR REPLACE/IGNORE Syntax

## ADDED Requirements

### Requirement: INSERT OR IGNORE SHALL skip conflicting rows

INSERT OR IGNORE INTO table VALUES (...) SHALL skip rows that would cause a primary key or unique constraint violation, equivalent to ON CONFLICT DO NOTHING.

#### Scenario: Conflict skipped

Given a table t(id INTEGER PRIMARY KEY, name VARCHAR) with row (1, 'old')
When the user executes `INSERT OR IGNORE INTO t VALUES (1, 'new')`
Then the existing row MUST remain unchanged as (1, 'old')

#### Scenario: No conflict inserts normally

Given a table t(id INTEGER PRIMARY KEY, name VARCHAR)
When the user executes `INSERT OR IGNORE INTO t VALUES (1, 'hello')`
Then the row (1, 'hello') MUST be inserted

### Requirement: INSERT OR REPLACE SHALL replace conflicting rows

INSERT OR REPLACE INTO table VALUES (...) SHALL replace existing rows when a primary key or unique constraint violation occurs, equivalent to ON CONFLICT DO UPDATE SET all columns.

#### Scenario: Conflict replaces row

Given a table t(id INTEGER PRIMARY KEY, name VARCHAR) with row (1, 'old')
When the user executes `INSERT OR REPLACE INTO t VALUES (1, 'new')`
Then the row MUST be updated to (1, 'new')

#### Scenario: No conflict inserts normally

Given a table t(id INTEGER PRIMARY KEY, name VARCHAR)
When the user executes `INSERT OR REPLACE INTO t VALUES (1, 'hello')`
Then the row (1, 'hello') MUST be inserted
