## ADDED Requirements

### Requirement: Constraint Storage in Catalog

The catalog SHALL store UNIQUE, CHECK, and FOREIGN KEY constraint definitions as part of TableDef, supporting constraint enumeration and validation.

#### Scenario: TableDef stores UNIQUE constraint

- GIVEN a CREATE TABLE with UNIQUE (email)
- WHEN the table is created in the catalog
- THEN TableDef.Constraints contains a UniqueConstraintDef with Columns=["email"]

#### Scenario: TableDef stores CHECK constraint

- GIVEN a CREATE TABLE with CHECK (age >= 0)
- WHEN the table is created in the catalog
- THEN TableDef.Constraints contains a CheckConstraintDef with Expression="age >= 0"

#### Scenario: TableDef stores FOREIGN KEY constraint

- GIVEN a CREATE TABLE with FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
- WHEN the table is created in the catalog
- THEN TableDef.Constraints contains a ForeignKeyConstraintDef with RefTable="users", OnDelete=CASCADE
