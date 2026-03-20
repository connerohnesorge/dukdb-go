# Information Schema Specification

## Requirements

### Requirement: information_schema.tables View

The system SHALL provide a view listing all tables and views.

#### Scenario: Query all tables and views

- WHEN user executes `SELECT * FROM information_schema.tables`
- THEN result contains columns: `table_catalog VARCHAR`, `table_schema VARCHAR`, `table_name VARCHAR`, `table_type VARCHAR`, `is_insertable_into VARCHAR`, `commit_action VARCHAR`
- AND `table_catalog` is 'main'
- AND `table_schema` includes 'main', 'pg_catalog', 'information_schema'
- AND `table_type` is 'BASE TABLE' or 'VIEW'
- AND `is_insertable_into` is 'YES' for base tables, 'NO' for views (unless updatable view)

#### Scenario: Filter by schema

- WHEN user executes `SELECT * FROM information_schema.tables WHERE table_schema = 'main'`
- THEN returns only tables from 'main' schema

#### Scenario: Filter by table type

- WHEN user executes `SELECT * FROM information_schema.tables WHERE table_type = 'BASE TABLE'`
- THEN returns only base tables (not views)

### Requirement: information_schema.columns View

The system SHALL provide a view listing all columns with their properties.

#### Scenario: Query all columns

- WHEN user executes `SELECT * FROM information_schema.columns`
- THEN result contains columns: `table_catalog VARCHAR`, `table_schema VARCHAR`, `table_name VARCHAR`, `column_name VARCHAR`, `ordinal_position INTEGER`, `column_default VARCHAR`, `is_nullable VARCHAR`, `data_type VARCHAR`, `character_maximum_length INTEGER`, `numeric_precision INTEGER`, `numeric_scale INTEGER`, `character_set_name VARCHAR`, `collation_name VARCHAR`, `column_type VARCHAR`, `comment VARCHAR`
- AND `ordinal_position` is 1-based column position
- AND `is_nullable` is 'YES' or 'NO'
- AND `data_type` matches SQL type names

#### Scenario: Query columns for specific table

- WHEN user executes `SELECT * FROM information_schema.columns WHERE table_name = 'users'`
- THEN returns all columns from 'users' table
- AND columns ordered by ordinal_position

#### Scenario: Join with tables view

- WHEN user executes `SELECT * FROM information_schema.columns c JOIN information_schema.tables t ON c.table_name = t.table_name`
- THEN JOIN produces correct results
- AND matching rows have same table_name

### Requirement: information_schema.schemata View

The system SHALL provide a view listing all schemas (databases in DuckDB terminology).

#### Scenario: Query all schemas

- WHEN user executes `SELECT * FROM information_schema.schemata`
- THEN result contains columns: `catalog_name VARCHAR`, `schema_name VARCHAR`, `schema_owner VARCHAR`, `default_character_set_name VARCHAR`, `sql_path VARCHAR`, `comment VARCHAR`
- AND includes 'main', 'pg_catalog', 'information_schema' schemas
- AND `catalog_name` is 'main' for all rows

#### Scenario: Filter by schema name

- WHEN user executes `SELECT * FROM information_schema.schemata WHERE schema_name = 'main'`
- THEN returns 1 row for 'main' schema

### Requirement: information_schema.views View

The system SHALL provide a view listing all views.

#### Scenario: Query all views

- WHEN user executes `SELECT * FROM information_schema.views`
- THEN result contains columns: `table_catalog VARCHAR`, `table_schema VARCHAR`, `table_name VARCHAR`, `view_definition VARCHAR`, `check_option VARCHAR`, `is_updatable VARCHAR`, `is_insertable_into VARCHAR`, `is_trigger_updatable VARCHAR`, `is_trigger_deletable VARCHAR`, `is_trigger_insertable_into VARCHAR`, `comment VARCHAR`
- AND `view_definition` contains original SELECT query
- AND `is_updatable` is 'NO' for read-only views

#### Scenario: Query view definitions

- WHEN user executes `SELECT view_definition FROM information_schema.views WHERE table_name = 'my_view'`
- THEN returns original SELECT statement that defined the view

### Requirement: information_schema.table_constraints View

The system SHALL provide a view listing all constraints.

#### Scenario: Query all constraints

- WHEN user executes `SELECT * FROM information_schema.table_constraints`
- THEN result contains columns: `constraint_catalog VARCHAR`, `constraint_schema VARCHAR`, `constraint_name VARCHAR`, `table_catalog VARCHAR`, `table_schema VARCHAR`, `table_name VARCHAR`, `constraint_type VARCHAR`, `is_deferrable VARCHAR`, `initially_deferred VARCHAR`, `enforced VARCHAR`
- AND `constraint_type` includes: 'PRIMARY KEY', 'UNIQUE', 'CHECK', 'FOREIGN KEY'
- AND `enforced` is 'YES' for active constraints

#### Scenario: Find primary keys

- WHEN user executes `SELECT * FROM information_schema.table_constraints WHERE constraint_type = 'PRIMARY KEY'`
- THEN returns all primary key constraints

### Requirement: information_schema.key_column_usage View

The system SHALL provide a view mapping constraints to columns.

#### Scenario: Query key column usage

- WHEN user executes `SELECT * FROM information_schema.key_column_usage`
- THEN result contains columns: `constraint_catalog VARCHAR`, `constraint_schema VARCHAR`, `constraint_name VARCHAR`, `table_catalog VARCHAR`, `table_schema VARCHAR`, `table_name VARCHAR`, `column_name VARCHAR`, `ordinal_position INTEGER`, `position_in_unique_constraint INTEGER`, `referenced_table_schema VARCHAR`, `referenced_table_name VARCHAR`, `referenced_column_name VARCHAR`
- AND shows which columns participate in which constraints
- AND for foreign keys, shows referenced table/column

#### Scenario: Find columns in primary key

- WHEN user executes `SELECT column_name FROM information_schema.key_column_usage WHERE table_name = 'users' AND constraint_type = 'PRIMARY KEY'`
- THEN returns primary key column names

### Requirement: information_schema.referential_constraints View

The system SHALL provide a view listing foreign key relationships.

#### Scenario: Query foreign keys

- WHEN user executes `SELECT * FROM information_schema.referential_constraints`
- THEN result contains columns: `constraint_catalog VARCHAR`, `constraint_schema VARCHAR`, `constraint_name VARCHAR`, `unique_constraint_catalog VARCHAR`, `unique_constraint_schema VARCHAR`, `unique_constraint_name VARCHAR`, `match_option VARCHAR`, `update_rule VARCHAR`, `delete_rule VARCHAR`
- AND `match_option` is 'NONE', 'FULL', or 'PARTIAL'
- AND `update_rule`, `delete_rule` are actions: 'NO ACTION', 'CASCADE', 'SET NULL', 'SET DEFAULT'

### Requirement: information_schema.constraint_column_usage View

The system SHALL provide a view showing columns referenced by constraints.

#### Scenario: Query constraint column usage

- WHEN user executes `SELECT * FROM information_schema.constraint_column_usage`
- THEN result contains columns: `table_catalog VARCHAR`, `table_schema VARCHAR`, `table_name VARCHAR`, `column_name VARCHAR`, `constraint_catalog VARCHAR`, `constraint_schema VARCHAR`, `constraint_name VARCHAR`
- AND shows which columns are referenced by constraints
- AND especially useful for finding what constraints reference a column

### Requirement: information_schema.check_constraints View

The system SHALL provide a view listing check constraints.

#### Scenario: Query check constraints

- WHEN user executes `SELECT * FROM information_schema.check_constraints`
- THEN result contains columns: `constraint_catalog VARCHAR`, `constraint_schema VARCHAR`, `constraint_name VARCHAR`, `check_clause VARCHAR`
- AND `check_clause` contains the CHECK expression

### Requirement: information_schema.triggers View

The system SHALL provide a view listing triggers (initially empty, for extensibility).

#### Scenario: Query triggers

- WHEN user executes `SELECT * FROM information_schema.triggers`
- THEN result contains columns: `trigger_catalog VARCHAR`, `trigger_schema VARCHAR`, `trigger_name VARCHAR`, `event_manipulation VARCHAR`, `event_object_catalog VARCHAR`, `event_object_schema VARCHAR`, `event_object_table VARCHAR`, `action_statement VARCHAR`, `action_orientation VARCHAR`, `action_timing VARCHAR`, `created TIMESTAMP`
- AND initially returns empty result set (no triggers implemented yet)
- AND structure ready for future trigger implementation

### Requirement: information_schema.routines View

The system SHALL provide a view listing functions and procedures.

#### Scenario: Query functions

- WHEN user executes `SELECT * FROM information_schema.routines`
- THEN result contains columns: `routine_catalog VARCHAR`, `routine_schema VARCHAR`, `routine_name VARCHAR`, `routine_type VARCHAR`, `data_type VARCHAR`, `routine_body VARCHAR`, `routine_definition VARCHAR`, `external_language VARCHAR`, `parameter_style VARCHAR`, `is_deterministic VARCHAR`, `created TIMESTAMP`, `last_altered TIMESTAMP`, `comment VARCHAR`
- AND `routine_type` is 'FUNCTION' or 'PROCEDURE'
- AND includes built-in and user-defined functions

#### Scenario: Find aggregate functions

- WHEN user executes `SELECT routine_name FROM information_schema.routines WHERE routine_name LIKE 'sum' OR routine_name LIKE 'avg'`
- THEN returns aggregate function names

### Requirement: information_schema.parameters View

The system SHALL provide a view listing function parameters.

#### Scenario: Query function parameters

- WHEN user executes `SELECT * FROM information_schema.parameters`
- THEN result contains columns: `specific_catalog VARCHAR`, `specific_schema VARCHAR`, `specific_name VARCHAR`, `ordinal_position INTEGER`, `parameter_mode VARCHAR`, `parameter_name VARCHAR`, `data_type VARCHAR`, `character_maximum_length INTEGER`, `numeric_precision INTEGER`, `numeric_scale INTEGER`
- AND `parameter_mode` is 'IN', 'OUT', 'INOUT'
- AND includes parameters for user-defined functions

#### Scenario: Join parameters with routines

- WHEN user executes `SELECT * FROM information_schema.parameters p JOIN information_schema.routines r ON p.specific_name = r.routine_name`
- THEN produces correct results matching parameters to functions

### Requirement: information_schema Schema Creation

The system SHALL create and register information_schema views at connection initialization.

#### Scenario: information_schema exists

- WHEN user connects to database
- THEN 'information_schema' schema is automatically created
- AND all 12 views are available for querying

#### Scenario: Schema visibility

- WHEN user executes `SHOW SCHEMAS` or queries information_schema.schemata
- THEN 'information_schema' appears in the list

