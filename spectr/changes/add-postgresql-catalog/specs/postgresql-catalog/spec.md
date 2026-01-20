# PostgreSQL Catalog Specification

## ADDED Requirements

### Requirement: OID System for Catalog Objects

The system SHALL provide a stable OID (object identifier) system for all catalog objects.

#### Scenario: Deterministic OID generation

- WHEN catalog object is queried
- THEN object has a stable OID
- AND OID is unique within object type (namespace OID ≠ table OID)
- AND OID is deterministic (same object gets same OID on reconnection)
- AND OID is 32-bit integer >= 1

#### Scenario: OID ranges

- WHEN system generates OIDs
- THEN namespace OIDs are in range 2000-2999
- AND system table OIDs are in range 1000-1999
- AND user table OIDs are in range 16000+
- AND user-defined type OIDs are in range 10000+

#### Scenario: OID collisions avoided

- WHEN OID is generated for new object
- THEN OID does not conflict with existing objects
- AND OID allocation is consistent across connections

### Requirement: pg_catalog.pg_namespace View

The system SHALL provide namespace (schema) catalog.

#### Scenario: Query schemas

- WHEN user executes `SELECT * FROM pg_catalog.pg_namespace`
- THEN result contains columns: `oid OID`, `nspname NAME`, `nspowner OID`, `nspacl ACLITEM[]`, `nspcomment TEXT`
- AND includes system schemas: 'pg_catalog', 'information_schema'
- AND includes user schemas from catalog
- AND OID is stable for each schema

#### Scenario: Find schema by name

- WHEN user executes `SELECT * FROM pg_catalog.pg_namespace WHERE nspname = 'main'`
- THEN returns 1 row with schema 'main'

### Requirement: pg_catalog.pg_class View

The system SHALL provide class (table, view, index, etc.) catalog.

#### Scenario: Query tables and views

- WHEN user executes `SELECT * FROM pg_catalog.pg_class`
- THEN result contains columns: `oid OID`, `relname NAME`, `relnamespace OID`, `reltype OID`, `relowner OID`, `relam OID`, `relfilenode OID`, `reltablespace OID`, `relpages BIGINT`, `reltuples FLOAT8`, `relallvisible INTEGER`, `reltoastrelid OID`, `relhasindex BOOLEAN`, `relisshared BOOLEAN`, `relpersistence CHAR`, `relkind CHAR`, `relnatts SMALLINT`, `relchecks SMALLINT`, `relhasoids BOOLEAN`, `relhastriggers BOOLEAN`, `relhasrules BOOLEAN`, `relhassubclass BOOLEAN`, `relacl ACLITEM[]`, `reloptions TEXT[]`
- AND `relkind` is 'r' (table), 'v' (view), 'i' (index), 'm' (materialized view), 'S' (sequence)
- AND includes all tables, views, indexes

#### Scenario: Find tables in schema

- WHEN user executes `SELECT * FROM pg_catalog.pg_class WHERE relnamespace = pg_catalog.pg_namespace.oid AND nspname = 'main' AND relkind = 'r'`
- THEN returns all user tables in 'main' schema

#### Scenario: System tables visible

- WHEN user queries pg_catalog.pg_class
- THEN system tables appear with system OIDs
- AND pg_catalog.pg_class itself appears with OID < 16000

### Requirement: pg_catalog.pg_attribute View

The system SHALL provide attribute (column) catalog.

#### Scenario: Query columns

- WHEN user executes `SELECT * FROM pg_catalog.pg_attribute`
- THEN result contains columns: `attrelid OID`, `attname NAME`, `atttypid OID`, `attlen SMALLINT`, `attnum SMALLINT`, `attcacheoff INTEGER`, `atttypmod INTEGER`, `attbyval BOOLEAN`, `attstorage CHAR`, `attalign CHAR`, `attnotnull BOOLEAN`, `atthasdef BOOLEAN`, `atthasmissing BOOLEAN`, `attisdropped BOOLEAN`, `attislocal BOOLEAN`, `attinhcount INTEGER`, `attcollation OID`, `attacl ACLITEM[]`, `attoptions TEXT[]`, `attfdw TEXT`
- AND `attnum` is 1-based column position
- AND includes all columns from all tables

#### Scenario: Find columns for table

- WHEN user executes `SELECT * FROM pg_catalog.pg_attribute WHERE attrelid = table_oid`
- THEN returns all columns for that table
- AND ordered by attnum

#### Scenario: Join attribute with class

- WHEN user executes `SELECT c.relname, a.attname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid WHERE c.relkind = 'r'`
- THEN produces correct table and column pairs

### Requirement: pg_catalog.pg_type View

The system SHALL provide type catalog.

#### Scenario: Query data types

- WHEN user executes `SELECT * FROM pg_catalog.pg_type`
- THEN result contains columns: `oid OID`, `typname NAME`, `typnamespace OID`, `typowner OID`, `typlen SMALLINT`, `typbyval BOOLEAN`, `typtype CHAR`, `typcategory CHAR`, `typispreferred BOOLEAN`, `typisdefined BOOLEAN`, `typdelim CHAR`, `typrelid OID`, `typsubscript OID`, `typelem OID`, `typarray OID`, `typinput OID`, `typoutput OID`, `typreceive OID`, `typsend OID`, `typmodin OID`, `typmodout OID`, `typanalyze OID`, `typalign CHAR`, `typstorage CHAR`, `typnotnull BOOLEAN`, `typbasetype OID`, `typtypmod INTEGER`, `typacl ACLITEM[]`, `typcomment TEXT`
- AND includes all built-in types: int2, int4, int8, float4, float8, text, varchar, bool, date, time, timestamp
- AND type OIDs match PostgreSQL standard OIDs where possible

#### Scenario: Find type information

- WHEN user executes `SELECT * FROM pg_catalog.pg_type WHERE typname = 'int4'`
- THEN returns 1 row with type information

### Requirement: pg_catalog.pg_proc View

The system SHALL provide function catalog.

#### Scenario: Query functions

- WHEN user executes `SELECT * FROM pg_catalog.pg_proc`
- THEN result contains columns: `oid OID`, `proname NAME`, `pronamespace OID`, `proowner OID`, `prolang OID`, `procost FLOAT4`, `prorows FLOAT4`, `provariadic OID`, `prosupport OID`, `prokind CHAR`, `prosecdef BOOLEAN`, `proleakproof BOOLEAN`, `proisstrict BOOLEAN`, `proiscachable BOOLEAN`, `provolatile CHAR`, `proparallel CHAR`, `pronargs SMALLINT`, `pronargdefaults SMALLINT`, `prorettype OID`, `proargtypes OID[]`, `proallargtypes OID[]`, `proargmodes CHAR[]`, `proargnames TEXT[]`, `proargdefaults TEXT`, `protrftypes OID[]`, `prosrc TEXT`, `probin TEXT`, `proconfig TEXT[]`, `proacl ACLITEM[]`
- AND includes built-in functions (sum, avg, count, etc.)
- AND includes user-defined functions
- AND `prokind` is 'f' (function) or 'a' (aggregate)

#### Scenario: Find aggregate functions

- WHEN user executes `SELECT * FROM pg_catalog.pg_proc WHERE prokind = 'a'`
- THEN returns aggregate functions (sum, avg, count, etc.)

### Requirement: pg_catalog.pg_index View

The system SHALL provide index catalog.

#### Scenario: Query indexes

- WHEN user executes `SELECT * FROM pg_catalog.pg_index`
- THEN result contains columns: `indexrelid OID`, `indrelid OID`, `indnatts SMALLINT`, `indnkeyatts SMALLINT`, `indisunique BOOLEAN`, `indisprimary BOOLEAN`, `indisexclusion BOOLEAN`, `indimmediate BOOLEAN`, `indisclustered BOOLEAN`, `indisvalid BOOLEAN`, `indisready BOOLEAN`, `indislive BOOLEAN`, `indisreplident BOOLEAN`, `indkey INT2VECTOR`, `indcollation OIDVECTOR`, `indclass OIDVECTOR`, `indoption INT2VECTOR`, `indexprs TEXT`, `indpred TEXT`
- AND includes all indexes from all tables
- AND `indkey` shows which columns are indexed

#### Scenario: Find indexes for table

- WHEN user executes `SELECT * FROM pg_catalog.pg_index WHERE indrelid = table_oid`
- THEN returns indexes on that table

### Requirement: pg_catalog.pg_constraint View

The system SHALL provide constraint catalog.

#### Scenario: Query constraints

- WHEN user executes `SELECT * FROM pg_catalog.pg_constraint`
- THEN result contains columns: `oid OID`, `conname NAME`, `connamespace OID`, `contype CHAR`, `condeferrable BOOLEAN`, `condeferred BOOLEAN`, `convalidated BOOLEAN`, `conrelid OID`, `contypid OID`, `conindid OID`, `conpfeqop OID[]`, `conppeqop OID[]`, `conffeqop OID[]`, `conexclop OID[]`, `conparentid OID`, `confmatchtype CHAR`, `confupdtype CHAR`, `confdeltype CHAR`, `conperiod BOOLEAN`, `conislocal BOOLEAN`, `coninhcount INTEGER`, `connoinherit BOOLEAN`, `conkey INT2VECTOR`, `confkey INT2VECTOR`, `conpfeqop_v OID[]`, `conppeqop_v OID[]`, `conffeqop_v OID[]`, `conexclop_v OID[]`, `conbin BYTEA`, `consrc TEXT`
- AND `contype` is 'c' (check), 'f' (foreign key), 'p' (primary key), 'u' (unique)
- AND includes all constraints

#### Scenario: Find primary keys

- WHEN user executes `SELECT * FROM pg_catalog.pg_constraint WHERE contype = 'p'`
- THEN returns primary key constraints

### Requirement: pg_catalog.pg_database View

The system SHALL provide database catalog.

#### Scenario: Query databases

- WHEN user executes `SELECT * FROM pg_catalog.pg_database`
- THEN result contains columns: `oid OID`, `datname NAME`, `datdba OID`, `encoding INTEGER`, `datcollate NAME`, `datctype NAME`, `datistemplate BOOLEAN`, `datallowconn BOOLEAN`, `datconnlimit INTEGER`, `dattablespace OID`, `datfrozenxid XID`, `datminmxid XID`, `datchecksum BOOLEAN`, `datacl ACLITEM[]`
- AND returns database 'main'
- AND `datname` is 'main'

### Requirement: pg_catalog.pg_user / pg_catalog.pg_roles Views

The system SHALL provide user and role catalogs.

#### Scenario: Query users

- WHEN user executes `SELECT * FROM pg_catalog.pg_user`
- THEN result contains columns: `usename NAME`, `usesysid OID`, `usecanlogin BOOLEAN`, `usesuper BOOLEAN`, `usecreatedb BOOLEAN`, `usecreaterole BOOLEAN`, `usecanreplication BOOLEAN`, `usbypassrls BOOLEAN`, `passwd TEXT`, `valuntil TIMESTAMP`
- AND returns current user

#### Scenario: Query roles

- WHEN user executes `SELECT * FROM pg_catalog.pg_roles`
- THEN returns similar data as pg_user but with role naming

### Requirement: pg_catalog.pg_tables View (User Tables)

The system SHALL provide simplified user tables view.

#### Scenario: Query user tables

- WHEN user executes `SELECT * FROM pg_catalog.pg_tables`
- THEN result contains columns: `schemaname NAME`, `tablename NAME`, `tableowner NAME`, `tablespace NAME`, `hasindexes BOOLEAN`, `hasrules BOOLEAN`, `hastriggers BOOLEAN`, `rowsecurity BOOLEAN`
- AND excludes system schemas (pg_catalog, information_schema)
- AND includes all user tables

### Requirement: pg_catalog.pg_views View (User Views)

The system SHALL provide simplified user views view.

#### Scenario: Query user views

- WHEN user executes `SELECT * FROM pg_catalog.pg_views`
- THEN result contains columns: `schemaname NAME`, `viewname NAME`, `viewowner NAME`, `definition TEXT`
- AND excludes system views
- AND includes all user views

### Requirement: pg_catalog.pg_settings View

The system SHALL provide settings catalog.

#### Scenario: Query settings

- WHEN user executes `SELECT * FROM pg_catalog.pg_settings`
- THEN result contains columns: `name TEXT`, `setting TEXT`, `unit TEXT`, `category TEXT`, `short_desc TEXT`, `extra_desc TEXT`, `context TEXT`, `vartype TEXT`, `source TEXT`, `min_val TEXT`, `max_val TEXT`, `enumvals TEXT[]`, `boot_val TEXT`, `reset_val TEXT`, `pending_restart BOOLEAN`
- AND returns configuration settings from internal/metadata/settings.go

### Requirement: pg_catalog.pg_stat_activity View

The system SHALL provide active connection information.

#### Scenario: Query active connections

- WHEN user executes `SELECT * FROM pg_catalog.pg_stat_activity`
- THEN result contains columns: `datid OID`, `datname NAME`, `pid BIGINT`, `usesysid OID`, `usename NAME`, `application_name TEXT`, `client_addr INET`, `client_hostname TEXT`, `client_port INTEGER`, `backend_start TIMESTAMP`, `xact_start TIMESTAMP`, `state_change TIMESTAMP`, `wait_event_type TEXT`, `wait_event TEXT`, `state TEXT`, `query TEXT`, `backend_type TEXT`
- AND includes current connection
- AND integrates with monitoring provider if available

### Requirement: pg_catalog.pg_stat_statements View (Optional)

The system SHALL provide query statistics view (implementation dependent).

#### Scenario: Query statistics available

- WHEN user executes `SELECT * FROM pg_catalog.pg_stat_statements`
- THEN result contains columns: `userid OID`, `dbid OID`, `queryid BIGINT`, `query TEXT`, `calls BIGINT`, `total_time FLOAT8`, `mean_time FLOAT8`, `max_time FLOAT8`, `min_time FLOAT8`, `rows BIGINT`, `shared_blks_hit BIGINT`, `shared_blks_read BIGINT`, `shared_blks_dirtied BIGINT`, `shared_blks_written BIGINT`, `local_blks_hit BIGINT`, `local_blks_read BIGINT`, `local_blks_dirtied BIGINT`, `local_blks_written BIGINT`, `temp_blks_read BIGINT`, `temp_blks_written BIGINT`, `blk_read_time FLOAT8`, `blk_write_time FLOAT8`, `wal_records BIGINT`, `wal_fpi BIGINT`, `wal_bytes NUMERIC`
- AND initially empty if query statistics not available
- AND can be populated by stats provider

### Requirement: pg_catalog Schema Initialization

The system SHALL create and register pg_catalog views at connection initialization.

#### Scenario: pg_catalog exists

- WHEN user connects to database
- THEN 'pg_catalog' schema is automatically created
- AND all views are available for querying

#### Scenario: Schema visibility

- WHEN user executes `SHOW SCHEMAS` or queries information_schema.schemata
- THEN 'pg_catalog' appears in the list
- AND queries show system catalog objects

### Requirement: Tool Compatibility

The system SHALL support tool introspection for pg_catalog.

#### Scenario: DBeaver can introspect database

- WHEN DBeaver connects to dukdb-go
- THEN DBeaver can query pg_catalog views
- AND DBeaver displays schema tree correctly
- AND Shows tables, views, columns in DBeaver interface

#### Scenario: psql-like tools work

- WHEN psql-like tool queries database
- THEN `\dt` (list tables) works by querying pg_catalog
- AND `\dv` (list views) works by querying pg_catalog
- AND `\df` (list functions) works by querying pg_catalog
