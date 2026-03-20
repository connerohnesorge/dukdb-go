## ADDED Requirements

### Requirement: SQL-Level Prepared Statement Execution

The engine SHALL support PREPARE/EXECUTE/DEALLOCATE for named SQL-level prepared statements with plan caching and parameter substitution.

#### Scenario: PREPARE and EXECUTE a SELECT

- WHEN executing `PREPARE q AS SELECT $1 + $2`
- AND then executing `EXECUTE q(10, 20)`
- THEN the result contains a single row with value 30

#### Scenario: EXECUTE with different parameters reuses plan

- GIVEN `PREPARE q AS SELECT * FROM users WHERE id = $1`
- WHEN executing `EXECUTE q(1)` then `EXECUTE q(2)` then `EXECUTE q(3)`
- THEN each execution returns the correct filtered rows
- AND the plan is parsed, bound, and planned only once (during PREPARE)

#### Scenario: PREPARE INSERT and EXECUTE multiple times

- GIVEN `PREPARE ins AS INSERT INTO t (id, name) VALUES ($1, $2)`
- WHEN executing `EXECUTE ins(1, 'alice')` then `EXECUTE ins(2, 'bob')`
- THEN both rows are inserted into the table

#### Scenario: DEALLOCATE removes prepared statement

- GIVEN `PREPARE q AS SELECT 1`
- WHEN executing `DEALLOCATE q`
- AND then executing `EXECUTE q`
- THEN an error is returned: prepared statement "q" does not exist

#### Scenario: DEALLOCATE ALL removes all prepared statements

- GIVEN `PREPARE q1 AS SELECT 1` and `PREPARE q2 AS SELECT 2`
- WHEN executing `DEALLOCATE ALL`
- AND then executing `EXECUTE q1`
- THEN an error is returned: prepared statement "q1" does not exist

#### Scenario: Error on duplicate PREPARE name

- GIVEN `PREPARE q AS SELECT 1`
- WHEN executing `PREPARE q AS SELECT 2`
- THEN an error is returned: prepared statement "q" already exists

#### Scenario: Error on EXECUTE unknown name

- WHEN executing `EXECUTE nonexistent`
- THEN an error is returned: prepared statement "nonexistent" does not exist

#### Scenario: Error on EXECUTE wrong parameter count

- GIVEN `PREPARE q AS SELECT $1 + $2`
- WHEN executing `EXECUTE q(42)`
- THEN an error is returned: expected 2 parameters, got 1

#### Scenario: Error on DEALLOCATE unknown name

- WHEN executing `DEALLOCATE nonexistent`
- THEN an error is returned: prepared statement "nonexistent" does not exist

#### Scenario: Prepared statements are connection-scoped

- GIVEN connection A with `PREPARE q AS SELECT 1`
- WHEN connection B executes `EXECUTE q`
- THEN an error is returned (prepared statement not visible across connections)

#### Scenario: Connection close cleans up prepared statements

- GIVEN a connection with multiple prepared statements
- WHEN the connection is closed
- THEN all prepared statement resources are released
