## ADDED Requirements

### Requirement: UNIQUE Constraint Enforcement

The engine SHALL enforce UNIQUE constraints on INSERT and UPDATE, rejecting rows that violate uniqueness.

#### Scenario: UNIQUE violation on INSERT

- GIVEN table "t" with UNIQUE (email) and existing row (1, 'alice@test.com')
- WHEN executing `INSERT INTO t VALUES (2, 'alice@test.com')`
- THEN a constraint violation error is returned

#### Scenario: UNIQUE allows NULL duplicates

- GIVEN table "t" with UNIQUE (email) and existing row (1, NULL)
- WHEN executing `INSERT INTO t VALUES (2, NULL)`
- THEN the insert succeeds (NULL != NULL per SQL standard)

#### Scenario: Composite UNIQUE violation

- GIVEN table "t" with UNIQUE (a, b) and existing row (1, 2, 'old')
- WHEN executing `INSERT INTO t VALUES (1, 2, 'new')`
- THEN a constraint violation error is returned

#### Scenario: UNIQUE enforced on UPDATE

- GIVEN table "t" with UNIQUE (email) and rows (1, 'alice'), (2, 'bob')
- WHEN executing `UPDATE t SET email = 'alice' WHERE id = 2`
- THEN a constraint violation error is returned

### Requirement: CHECK Constraint Enforcement

The engine SHALL enforce CHECK constraints on INSERT and UPDATE, rejecting rows where the CHECK expression evaluates to FALSE.

#### Scenario: CHECK violation on INSERT

- GIVEN table "t" with CHECK (age >= 0)
- WHEN executing `INSERT INTO t (name, age) VALUES ('alice', -1)`
- THEN a constraint violation error is returned

#### Scenario: CHECK passes with NULL

- GIVEN table "t" with CHECK (age >= 0)
- WHEN executing `INSERT INTO t (name, age) VALUES ('alice', NULL)`
- THEN the insert succeeds (NULL does not violate CHECK per SQL standard)

#### Scenario: CHECK with multiple columns

- GIVEN table "t" with CHECK (end_date > start_date)
- WHEN executing `INSERT INTO t VALUES ('2024-01-05', '2024-01-01')`
- THEN a constraint violation error is returned (end < start)

#### Scenario: CHECK enforced on UPDATE

- GIVEN table "t" with CHECK (age >= 0) and existing row ('alice', 25)
- WHEN executing `UPDATE t SET age = -5 WHERE name = 'alice'`
- THEN a constraint violation error is returned

### Requirement: FOREIGN KEY Enforcement

The engine SHALL enforce FOREIGN KEY constraints on INSERT/UPDATE of the child table and reject DELETE/UPDATE of referenced parent rows (NO ACTION/RESTRICT only, matching DuckDB v1.4.3).

#### Scenario: FK violation on INSERT into child

- GIVEN parent table "users" with PK (id) and rows (1), (2)
- AND child table "orders" with FK (user_id) REFERENCES users(id)
- WHEN executing `INSERT INTO orders (id, user_id) VALUES (1, 999)`
- THEN a FK violation error is returned (user 999 does not exist)

#### Scenario: FK allows NULL reference

- GIVEN parent table "users" with PK (id)
- AND child table "orders" with FK (user_id) REFERENCES users(id)
- WHEN executing `INSERT INTO orders (id, user_id) VALUES (1, NULL)`
- THEN the insert succeeds (NULL FK is allowed)

#### Scenario: FK ON DELETE RESTRICT prevents deletion

- GIVEN parent "users" with row (1) and child "orders" with FK REFERENCES users(id) and rows referencing user 1
- WHEN executing `DELETE FROM users WHERE id = 1`
- THEN a FK violation error is returned (cannot delete referenced row)

#### Scenario: FK ON DELETE NO ACTION (default) prevents deletion

- GIVEN parent "users" with row (1) and child "orders" with FK (default NO ACTION) and rows referencing user 1
- WHEN executing `DELETE FROM users WHERE id = 1`
- THEN a FK violation error is returned (same as RESTRICT for immediate constraints)

#### Scenario: FK rejects CASCADE action at parse time

- WHEN executing `CREATE TABLE t (ref_id INTEGER REFERENCES other(id) ON DELETE CASCADE)`
- THEN a parse error is returned: "FOREIGN KEY constraints cannot use CASCADE, SET NULL or SET DEFAULT"

#### Scenario: FK validation during CREATE TABLE

- WHEN executing `CREATE TABLE t (ref_id INTEGER REFERENCES nonexistent(id))`
- THEN an error is returned indicating referenced table does not exist

#### Scenario: Self-referencing FK

- GIVEN table "employees" with PK (id) and FK (manager_id) REFERENCES employees(id)
- WHEN executing `INSERT INTO employees (id, name, manager_id) VALUES (1, 'CEO', NULL)`
- AND then `INSERT INTO employees (id, name, manager_id) VALUES (2, 'VP', 1)`
- THEN both inserts succeed (manager_id=NULL is allowed, manager_id=1 exists)
