## ADDED Requirements

### Requirement: ON CONFLICT Clause Parsing

The parser SHALL parse the `ON CONFLICT` clause in INSERT statements, supporting both `DO NOTHING` and `DO UPDATE SET` actions with optional conflict target columns and WHERE filters.

#### Scenario: INSERT ... ON CONFLICT DO NOTHING without conflict columns

- WHEN parsing `INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT DO NOTHING`
- THEN the parser produces an InsertStmt with OnConflict.Action = OnConflictDoNothing
- AND OnConflict.ConflictColumns is empty (infer from PK)

#### Scenario: INSERT ... ON CONFLICT (columns) DO NOTHING

- WHEN parsing `INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING`
- THEN the parser produces an InsertStmt with OnConflict.ConflictColumns = ["id"]
- AND OnConflict.Action = OnConflictDoNothing

#### Scenario: INSERT ... ON CONFLICT DO UPDATE SET

- WHEN parsing `INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`
- THEN the parser produces an InsertStmt with OnConflict.Action = OnConflictDoUpdate
- AND OnConflict.UpdateSet contains one SetClause mapping "name" to a column ref of "EXCLUDED"."name"

#### Scenario: INSERT ... ON CONFLICT DO UPDATE SET with WHERE

- WHEN parsing `INSERT INTO t (id, val) VALUES (1, 10) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE EXCLUDED.val > t.val`
- THEN the parser produces an InsertStmt with OnConflict.UpdateWhere containing the comparison expression
- AND the WHERE filter references both EXCLUDED and the target table

#### Scenario: INSERT ... ON CONFLICT with multiple conflict columns

- WHEN parsing `INSERT INTO t (a, b, c) VALUES (1, 2, 3) ON CONFLICT (a, b) DO NOTHING`
- THEN OnConflict.ConflictColumns = ["a", "b"]

#### Scenario: INSERT ... SELECT ... ON CONFLICT

- WHEN parsing `INSERT INTO t SELECT * FROM source ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`
- THEN the parser produces an InsertStmt with both a Select and an OnConflict clause

#### Scenario: ON CONFLICT with RETURNING

- WHEN parsing `INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING *`
- THEN the parser produces an InsertStmt with both OnConflict and Returning populated
- AND ON CONFLICT is parsed before RETURNING

#### Scenario: ON CONFLICT conflict target WHERE (partial index)

- WHEN parsing `INSERT INTO t VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING`
- THEN OnConflict.ConflictWhere contains the predicate `id > 0`
- AND OnConflict.Action = OnConflictDoNothing
