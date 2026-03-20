## ADDED Requirements

### Requirement: Foreign Key Constraint Parsing

The parser SHALL parse column-level `REFERENCES parent(col)` and table-level `FOREIGN KEY (cols) REFERENCES parent(cols)` clauses with optional `ON DELETE` and `ON UPDATE` action specifications. Only NO ACTION and RESTRICT actions SHALL be accepted; CASCADE, SET NULL, and SET DEFAULT SHALL be rejected at parse time with error "FOREIGN KEY constraints cannot use CASCADE, SET NULL or SET DEFAULT".

#### Scenario: Column-level REFERENCES with default action

- WHEN parsing `CREATE TABLE orders (id INTEGER, user_id INTEGER REFERENCES users(id))`
- THEN the parsed AST includes a ForeignKeyRef on the user_id column with RefTable="users", RefColumns=["id"], OnDelete=NoAction, OnUpdate=NoAction

#### Scenario: Table-level FOREIGN KEY with RESTRICT

- WHEN parsing `CREATE TABLE orders (id INTEGER, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT)`
- THEN the parsed AST includes a TableConstraint with Type="FOREIGN_KEY", Columns=["user_id"], RefTable="users", RefColumns=["id"], OnDelete=Restrict

#### Scenario: FOREIGN KEY rejects CASCADE action

- WHEN parsing `CREATE TABLE t (ref_id INTEGER REFERENCES other(id) ON DELETE CASCADE)`
- THEN a parse error is returned containing "FOREIGN KEY constraints cannot use CASCADE, SET NULL or SET DEFAULT"

#### Scenario: FOREIGN KEY rejects SET NULL action

- WHEN parsing `CREATE TABLE t (ref_id INTEGER REFERENCES other(id) ON UPDATE SET NULL)`
- THEN a parse error is returned containing "FOREIGN KEY constraints cannot use CASCADE, SET NULL or SET DEFAULT"

#### Scenario: Composite FOREIGN KEY

- WHEN parsing `CREATE TABLE line_items (order_id INTEGER, item_no INTEGER, FOREIGN KEY (order_id, item_no) REFERENCES order_items(order_id, item_no))`
- THEN the parsed AST includes a TableConstraint with Type="FOREIGN_KEY", Columns=["order_id","item_no"], RefTable="order_items", RefColumns=["order_id","item_no"]

#### Scenario: Named FOREIGN KEY constraint

- WHEN parsing `CREATE TABLE orders (user_id INTEGER, CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id))`
- THEN the parsed AST includes a TableConstraint with Name="fk_user" and Type="FOREIGN_KEY"

### Requirement: Foreign Key Catalog Storage

The catalog SHALL store `ForeignKeyConstraintDef` entries in `TableDef.Constraints` containing child column names, referenced table name, referenced column names, and referential actions. The `ForeignKeyConstraintDef` SHALL support `Clone()` for deep copying.

#### Scenario: ForeignKeyConstraintDef stored in TableDef

- GIVEN a CREATE TABLE with `FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT`
- WHEN the table is created in the catalog
- THEN `TableDef.Constraints` contains a `*ForeignKeyConstraintDef` with Columns=["user_id"], RefTable="users", RefColumns=["id"], OnDelete=Restrict

#### Scenario: Clone preserves ForeignKeyConstraintDef

- GIVEN a TableDef with a ForeignKeyConstraintDef
- WHEN Clone() is called on the TableDef
- THEN the cloned TableDef contains a separate ForeignKeyConstraintDef with identical values

### Requirement: CREATE TABLE Foreign Key Validation

The engine SHALL validate foreign key constraints during CREATE TABLE by verifying that the referenced parent table exists and that the referenced columns form a PRIMARY KEY or UNIQUE constraint on the parent table.

#### Scenario: Referenced table does not exist

- WHEN executing `CREATE TABLE orders (id INTEGER, user_id INTEGER REFERENCES nonexistent(id))`
- THEN an error is returned indicating the referenced table "nonexistent" does not exist

#### Scenario: Referenced column does not exist

- GIVEN table "users" with columns (id INTEGER, name VARCHAR)
- WHEN executing `CREATE TABLE orders (id INTEGER, user_id INTEGER REFERENCES users(missing_col))`
- THEN an error is returned indicating the referenced column does not exist

#### Scenario: Referenced columns are not a key

- GIVEN table "users" with columns (id INTEGER PRIMARY KEY, name VARCHAR)
- WHEN executing `CREATE TABLE orders (id INTEGER, user_name VARCHAR REFERENCES users(name))`
- THEN an error is returned indicating the referenced columns do not form a primary key or unique constraint

#### Scenario: Valid FK referencing primary key

- GIVEN table "users" with columns (id INTEGER PRIMARY KEY, name VARCHAR)
- WHEN executing `CREATE TABLE orders (id INTEGER, user_id INTEGER REFERENCES users(id))`
- THEN the table is created successfully with the FK constraint stored

### Requirement: INSERT Foreign Key Enforcement

The engine SHALL enforce foreign key constraints on INSERT into a child table by verifying that each non-NULL FK column value exists in the referenced parent table's key columns.

#### Scenario: FK violation on INSERT into child

- GIVEN parent table "users" with PK (id) containing rows (1), (2)
- AND child table "orders" with FK (user_id) REFERENCES users(id)
- WHEN executing `INSERT INTO orders (id, user_id) VALUES (1, 999)`
- THEN a constraint error is returned with Msg containing "foreign key violation" and "not present in table"

#### Scenario: FK allows NULL reference on INSERT

- GIVEN parent table "users" with PK (id)
- AND child table "orders" with FK (user_id) REFERENCES users(id)
- WHEN executing `INSERT INTO orders (id, user_id) VALUES (1, NULL)`
- THEN the insert succeeds because NULL FK values are always allowed

#### Scenario: Valid FK reference on INSERT

- GIVEN parent table "users" with PK (id) containing row (1)
- AND child table "orders" with FK (user_id) REFERENCES users(id)
- WHEN executing `INSERT INTO orders (id, user_id) VALUES (1, 1)`
- THEN the insert succeeds because user_id=1 exists in users

#### Scenario: Composite FK violation on INSERT

- GIVEN parent table "order_items" with PK (order_id, item_no) containing row (1, 1)
- AND child table "line_items" with FK (order_id, item_no) REFERENCES order_items(order_id, item_no)
- WHEN executing `INSERT INTO line_items (id, order_id, item_no) VALUES (1, 1, 999)`
- THEN a constraint error is returned because (1, 999) does not exist in order_items

### Requirement: DELETE Foreign Key Enforcement

The engine SHALL enforce foreign key constraints on DELETE from a parent table by verifying that no child table rows reference the key values being deleted (NO ACTION/RESTRICT behavior).

#### Scenario: DELETE blocked by child reference

- GIVEN parent "users" with row (id=1) and child "orders" with FK (user_id) REFERENCES users(id) containing row (user_id=1)
- WHEN executing `DELETE FROM users WHERE id = 1`
- THEN a constraint error is returned with Msg containing "foreign key violation" and "still referenced from table"

#### Scenario: DELETE allowed when no child references

- GIVEN parent "users" with rows (id=1), (id=2) and child "orders" with FK referencing users(id) containing only (user_id=1)
- WHEN executing `DELETE FROM users WHERE id = 2`
- THEN the delete succeeds because no child rows reference id=2

#### Scenario: DELETE allowed when child FK is NULL

- GIVEN parent "users" with row (id=1) and child "orders" with FK (user_id) REFERENCES users(id) containing row (user_id=NULL)
- WHEN executing `DELETE FROM users WHERE id = 1`
- THEN the delete succeeds because NULL FK values do not constitute a reference

### Requirement: UPDATE Foreign Key Enforcement

The engine SHALL enforce foreign key constraints on UPDATE of both child and parent tables. Updating a child table's FK columns SHALL require the new values to exist in the parent. Updating a parent table's key columns SHALL be rejected if any child rows reference the old values.

#### Scenario: UPDATE child FK to non-existent parent

- GIVEN parent "users" with rows (id=1), (id=2) and child "orders" with FK (user_id) REFERENCES users(id) containing (user_id=1)
- WHEN executing `UPDATE orders SET user_id = 999 WHERE user_id = 1`
- THEN a constraint error is returned because user_id=999 does not exist in users

#### Scenario: UPDATE child FK to valid parent

- GIVEN parent "users" with rows (id=1), (id=2) and child "orders" with FK (user_id) REFERENCES users(id) containing (user_id=1)
- WHEN executing `UPDATE orders SET user_id = 2 WHERE user_id = 1`
- THEN the update succeeds because user_id=2 exists in users

#### Scenario: UPDATE parent key blocked by child reference

- GIVEN parent "users" with row (id=1) and child "orders" with FK (user_id) REFERENCES users(id) containing (user_id=1)
- WHEN executing `UPDATE users SET id = 99 WHERE id = 1`
- THEN a constraint error is returned because child rows still reference id=1

#### Scenario: UPDATE parent key allowed when no child references

- GIVEN parent "users" with rows (id=1), (id=2) and child "orders" with FK referencing users(id) containing only (user_id=1)
- WHEN executing `UPDATE users SET id = 99 WHERE id = 2`
- THEN the update succeeds because no child rows reference id=2

### Requirement: Self-Referencing Foreign Key

The engine SHALL support self-referencing foreign keys where a table's FK column references its own primary key. NULL self-references SHALL always be allowed.

#### Scenario: Self-referencing FK insert with NULL

- GIVEN table "employees" with PK (id) and FK (manager_id) REFERENCES employees(id)
- WHEN executing `INSERT INTO employees (id, name, manager_id) VALUES (1, 'CEO', NULL)`
- THEN the insert succeeds because NULL FK is allowed

#### Scenario: Self-referencing FK insert with valid reference

- GIVEN table "employees" with PK (id) and FK (manager_id) REFERENCES employees(id) containing row (id=1, manager_id=NULL)
- WHEN executing `INSERT INTO employees (id, name, manager_id) VALUES (2, 'VP', 1)`
- THEN the insert succeeds because manager_id=1 exists in employees

#### Scenario: Self-referencing FK insert with invalid reference

- GIVEN table "employees" with PK (id) and FK (manager_id) REFERENCES employees(id) containing row (id=1)
- WHEN executing `INSERT INTO employees (id, name, manager_id) VALUES (2, 'VP', 999)`
- THEN a constraint error is returned because manager_id=999 does not exist in employees
