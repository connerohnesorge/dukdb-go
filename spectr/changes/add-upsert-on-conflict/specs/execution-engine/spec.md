## ADDED Requirements

### Requirement: UPSERT Execution (INSERT ... ON CONFLICT)

The engine SHALL support INSERT ... ON CONFLICT DO NOTHING and INSERT ... ON CONFLICT DO UPDATE SET with conflict detection against PRIMARY KEY and UNIQUE indexes, EXCLUDED pseudo-table evaluation, and batch-optimized conflict resolution.

#### Scenario: DO NOTHING skips conflicting rows

- GIVEN table "t" with PRIMARY KEY (id) and existing row (1, 'old')
- WHEN executing `INSERT INTO t (id, name) VALUES (1, 'new'), (2, 'added') ON CONFLICT (id) DO NOTHING`
- THEN row (1, 'old') remains unchanged
- AND row (2, 'added') is inserted
- AND RowsAffected returns 1

#### Scenario: DO UPDATE updates conflicting rows with EXCLUDED

- GIVEN table "t" with PRIMARY KEY (id) and existing row (1, 'old')
- WHEN executing `INSERT INTO t (id, name) VALUES (1, 'new') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`
- THEN row (1, 'old') is updated to (1, 'new')
- AND RowsAffected returns 1

#### Scenario: DO UPDATE with WHERE filter on update action

- GIVEN table "t" with PRIMARY KEY (id) and existing rows (1, 10), (2, 20)
- WHEN executing `INSERT INTO t (id, val) VALUES (1, 5), (2, 30) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE EXCLUDED.val > t.val`
- THEN row (1, 10) remains unchanged (EXCLUDED.val=5 is NOT > t.val=10)
- AND row (2, 20) is updated to (2, 30) (EXCLUDED.val=30 > t.val=20)
- AND RowsAffected returns 1

#### Scenario: DO NOTHING with no conflicts inserts all rows

- GIVEN table "t" with PRIMARY KEY (id) and no existing rows
- WHEN executing `INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b') ON CONFLICT (id) DO NOTHING`
- THEN both rows are inserted
- AND RowsAffected returns 2

#### Scenario: Conflict detection on UNIQUE index (non-PK)

- GIVEN table "t" with columns (id INTEGER, email VARCHAR) and UNIQUE INDEX on (email)
- AND existing row (1, 'alice@test.com')
- WHEN executing `INSERT INTO t VALUES (2, 'alice@test.com') ON CONFLICT (email) DO NOTHING`
- THEN the insert is skipped
- AND RowsAffected returns 0

#### Scenario: Conflict detection on composite key

- GIVEN table "t" with PRIMARY KEY (a, b) and existing row (1, 2, 'old')
- WHEN executing `INSERT INTO t (a, b, c) VALUES (1, 2, 'new') ON CONFLICT (a, b) DO UPDATE SET c = EXCLUDED.c`
- THEN row (1, 2, 'old') is updated to (1, 2, 'new')
- AND RowsAffected returns 1

#### Scenario: UPSERT with RETURNING clause

- GIVEN table "t" with PRIMARY KEY (id) and existing row (1, 'old')
- WHEN executing `INSERT INTO t (id, name) VALUES (1, 'new'), (2, 'fresh') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING id, name`
- THEN the result set contains rows (1, 'new') and (2, 'fresh')
- AND the updated row appears in RETURNING output

#### Scenario: INSERT ... SELECT ... ON CONFLICT

- GIVEN table "target" with PRIMARY KEY (id) and existing row (1, 'old')
- AND table "source" with rows (1, 'updated'), (3, 'new')
- WHEN executing `INSERT INTO target SELECT * FROM source ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`
- THEN target contains (1, 'updated') and (3, 'new')
- AND the original row (1, 'old') is replaced

#### Scenario: DO NOTHING without explicit conflict columns infers PK

- GIVEN table "t" with PRIMARY KEY (id) and existing row (1, 'old')
- WHEN executing `INSERT INTO t (id, name) VALUES (1, 'new') ON CONFLICT DO NOTHING`
- THEN the insert is skipped (conflict detected on inferred PK column "id")
- AND RowsAffected returns 0

#### Scenario: Error when no unique constraint matches conflict columns

- GIVEN table "t" with PRIMARY KEY (id) and no unique index on (name)
- WHEN executing `INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (name) DO NOTHING`
- THEN an error is returned indicating no unique constraint covers column "name"

#### Scenario: WAL logging for upsert operations

- GIVEN table "t" with PRIMARY KEY (id)
- WHEN executing an upsert that inserts some rows and updates others
- THEN inserted rows are logged as INSERT WAL entries
- AND updated rows are logged as UPDATE WAL entries
- AND skipped rows (DO NOTHING) produce no WAL entries

#### Scenario: Bulk upsert performance

- GIVEN table "t" with PRIMARY KEY (id) and 1000 existing rows
- WHEN executing an INSERT with 10000 rows and ON CONFLICT DO UPDATE
- THEN conflict detection uses batch-optimized key lookup
- AND the operation completes without per-row table scans for non-conflicting rows

### Requirement: EXCLUDED Pseudo-Table

The engine SHALL provide an EXCLUDED pseudo-table scope within ON CONFLICT DO UPDATE expressions that references the column values from the row that caused the conflict.

#### Scenario: EXCLUDED references insert values in SET clause

- GIVEN an INSERT that conflicts on row (1, 'old', 100)
- AND the INSERT attempted values (1, 'new', 200)
- WHEN the DO UPDATE SET clause references `EXCLUDED.name`
- THEN `EXCLUDED.name` evaluates to 'new' (the attempted insert value)

#### Scenario: EXCLUDED in WHERE clause of DO UPDATE

- WHEN the DO UPDATE WHERE clause references `EXCLUDED.val > t.val`
- THEN `EXCLUDED.val` evaluates to the attempted insert value for column "val"
- AND `t.val` evaluates to the existing row's value for column "val"

#### Scenario: EXCLUDED with expression combining existing and new values

- GIVEN an INSERT that conflicts with existing row (1, 100)
- AND attempted insert values (1, 50)
- WHEN executing `ON CONFLICT (id) DO UPDATE SET val = t.val + EXCLUDED.val`
- THEN the updated row has val = 150 (existing 100 + attempted 50)

#### Scenario: EXCLUDED is not accessible outside ON CONFLICT

- WHEN a SELECT statement references `EXCLUDED.col`
- THEN a binding error is returned indicating EXCLUDED is only valid in ON CONFLICT DO UPDATE

#### Scenario: NULL values in UNIQUE conflict columns do not trigger conflicts

- GIVEN table "t" with UNIQUE INDEX on (email) and existing row (1, NULL)
- WHEN executing `INSERT INTO t (id, email) VALUES (2, NULL) ON CONFLICT (email) DO NOTHING`
- THEN the insert succeeds (NULL != NULL per SQL standard)
- AND RowsAffected returns 1

#### Scenario: DO UPDATE preserves non-updated column values

- GIVEN table "t" with PRIMARY KEY (id) and columns (id, name, score) and existing row (1, 'alice', 100)
- WHEN executing `INSERT INTO t (id, name, score) VALUES (1, 'bob', 200) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`
- THEN row becomes (1, 'bob', 100) — score is NOT updated and retains its existing value
- AND RowsAffected returns 1

#### Scenario: DO NOTHING with RETURNING returns empty for skipped rows

- GIVEN table "t" with PRIMARY KEY (id) and existing row (1, 'old')
- WHEN executing `INSERT INTO t (id, name) VALUES (1, 'new') ON CONFLICT (id) DO NOTHING RETURNING id, name`
- THEN the result set is empty (skipped rows produce no RETURNING output)

#### Scenario: Error on partial composite key conflict target

- GIVEN table "t" with PRIMARY KEY (a, b)
- WHEN executing `INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT (a) DO NOTHING`
- THEN an error is returned indicating conflict target must include all columns of the constraint

#### Scenario: Error when DO UPDATE SET modifies conflict target column

- GIVEN table "t" with PRIMARY KEY (id)
- WHEN executing `INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id`
- THEN a binding error is returned indicating conflict target columns cannot be modified in DO UPDATE SET
