# System Tables Specification

## Overview

This specification defines the implementation of SQL standard metadata tables (information_schema) and PostgreSQL compatibility tables (pg_catalog) in dukdb-go. These tables provide standardized ways to query database metadata and ensure compatibility with existing database tools and applications.

## information_schema

information_schema is a set of views defined by the SQL standard that provide metadata about the database in a standardized format across different database systems.

### Core Views

#### information_schema.tables

Contains one row for each table or view in the database.

**Columns:**
```sql
CREATE VIEW information_schema.tables AS
SELECT
    database_name AS table_catalog,
    schema_name AS table_schema,
    table_name,
    CASE table_type
        WHEN 'BASE TABLE' THEN 'TABLE'
        WHEN 'VIEW' THEN 'VIEW'
        WHEN 'LOCAL TEMPORARY' THEN 'LOCAL TEMPORARY'
        ELSE table_type
    END AS table_type,
    NULL::varchar AS self_referencing_column_name,
    NULL::varchar AS reference_generation,
    NULL::varchar AS user_defined_type_catalog,
    NULL::varchar AS user_defined_type_schema,
    NULL::varchar AS user_defined_type_name,
    CASE temporary
        WHEN TRUE THEN 'YES'
        ELSE 'NO'
    END AS is_insertable_into,
    'NO' AS is_typed,
    CASE temporary
        WHEN TRUE THEN 'YES'
        ELSE 'NO'
    END AS commit_action
FROM duckdb_tables();
```

**Example Usage:**
```sql
-- List all user tables
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_type = 'TABLE'
  AND table_schema NOT IN ('information_schema', 'pg_catalog');

-- Count tables by schema
SELECT table_schema, COUNT(*) as table_count
FROM information_schema.tables
GROUP BY table_schema;
```

**Implementation Notes:**
- Excludes system tables by default
- table_type values: 'TABLE', 'VIEW', 'LOCAL TEMPORARY'
- self_referencing_column_name always NULL (not supported)

#### information_schema.columns

Contains one row for each column in each table.

**Columns:**
```sql
CREATE VIEW information_schema.columns AS
SELECT
    t.database_name AS table_catalog,
    t.schema_name AS table_schema,
    t.table_name,
    c.column_name,
    c.ordinal_position,
    c.column_default AS column_default,
    CASE c.is_nullable
        WHEN TRUE THEN 'YES'
        ELSE 'NO'
    END AS is_nullable,
    c.data_type AS data_type,
    c.character_maximum_length,
    c.character_octet_length,
    c.numeric_precision,
    c.numeric_precision_radix,
    c.numeric_scale,
    c.datetime_precision,
    c.interval_type,
    c.interval_precision,
    NULL::varchar AS character_set_catalog,
    NULL::varchar AS character_set_schema,
    NULL::varchar AS character_set_name,
    NULL::varchar AS collation_catalog,
    NULL::varchar AS collation_schema,
    NULL::varchar AS collation_name,
    NULL::varchar AS domain_catalog,
    NULL::varchar AS domain_schema,
    NULL::varchar AS domain_name,
    NULL::varchar AS udt_catalog,
    NULL::varchar AS udt_schema,
    NULL::varchar AS udt_name,
    NULL::varchar AS scope_catalog,
    NULL::varchar AS scope_schema,
    NULL::varchar AS scope_name,
    NULL::bigint AS maximum_cardinality,
    NULL::varchar AS dtd_identifier,
    NULL::varchar AS is_self_referencing,
    NULL::varchar AS is_identity,
    NULL::varchar AS identity_generation,
    NULL::varchar AS identity_start,
    NULL::varchar AS identity_increment,
    NULL::varchar AS identity_maximum,
    NULL::varchar AS identity_minimum,
    NULL::varchar AS identity_cycle,
    NULL::varchar AS is_generated,
    NULL::varchar AS generation_expression,
    CASE
        WHEN c.is_updatable THEN 'YES'
        ELSE 'NO'
    END AS is_updatable
FROM information_schema.tables t
JOIN duckdb_columns() c ON t.table_name = c.table_name
WHERE t.table_type IN ('TABLE', 'VIEW');
```

**Example Usage:**
```sql
-- Get column information for a table
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_name = 'users'
ORDER BY ordinal_position;

-- Find all VARCHAR columns
SELECT table_schema, table_name, column_name, character_maximum_length
FROM information_schema.columns
WHERE data_type LIKE '%VARCHAR%';
```

**Implementation Notes:**
- ordinal_position is 1-based
- character_maximum_length for character types
- numeric_precision/scale for numeric types
- datetime_precision for temporal types

#### information_schema.schemata

Contains one row for each schema in the database.

**Columns:**
```sql
CREATE VIEW information_schema.schemata AS
SELECT
    database_name AS catalog_name,
    schema_name AS schema_name,
    NULL::varchar AS schema_owner,
    NULL::varchar AS default_character_set_catalog,
    NULL::varchar AS default_character_set_schema,
    NULL::varchar AS default_character_set_name,
    NULL::varchar AS sql_path
FROM duckdb_schemata();
```

**Example Usage:**
```sql
-- List all schemas
SELECT catalog_name, schema_name
FROM information_schema.schemata
ORDER BY schema_name;

-- Count objects by schema
SELECT s.schema_name,
       COUNT(DISTINCT t.table_name) as table_count,
       COUNT(DISTINCT v.view_name) as view_count
FROM information_schema.schemata s
LEFT JOIN information_schema.tables t ON s.schema_name = t.table_schema
LEFT JOIN information_schema.views v ON s.schema_name = v.table_schema
GROUP BY s.schema_name;
```

#### information_schema.views

Contains one row for each view in the database.

**Columns:**
```sql
CREATE VIEW information_schema.views AS
SELECT
    database_name AS table_catalog,
    schema_name AS table_schema,
    view_name AS table_name,
    sql AS view_definition,
    CASE temporary
        WHEN TRUE THEN 'YES'
        ELSE 'NO'
    END AS is_updatable,
    'NO' AS is_insertable_into,
    'NO' AS is_trigger_updatable,
    'NO' AS is_trigger_deletable,
    'NO' AS is_trigger_insertable_into
FROM duckdb_views();
```

**Example Usage:**
```sql
-- List all views with their definitions
SELECT table_schema, table_name, view_definition
FROM information_schema.views
ORDER BY table_schema, table_name;

-- Find views referencing a specific table
SELECT table_schema, table_name
FROM information_schema.views
WHERE view_definition LIKE '%users%';
```

#### information_schema.table_constraints

Contains one row for each table constraint.

**Columns:**
```sql
CREATE VIEW information_schema.table_constraints AS
SELECT
    database_name AS constraint_catalog,
    schema_name AS constraint_schema,
    constraint_name,
    CASE constraint_type
        WHEN 'PRIMARY KEY' THEN 'PRIMARY KEY'
        WHEN 'FOREIGN KEY' THEN 'FOREIGN KEY'
        WHEN 'UNIQUE' THEN 'UNIQUE'
        WHEN 'CHECK' THEN 'CHECK'
    END AS constraint_type,
    'DEFERRABLE' AS is_deferrable,
    'INITIALLY DEFERRED' AS initially_deferred,
    'NO' AS enforced,
    'YES' AS validated
FROM duckdb_constraints();
```

**Example Usage:**
```sql
-- List all constraints by table
SELECT constraint_schema, table_name, constraint_name, constraint_type
FROM information_schema.table_constraints
ORDER BY constraint_schema, table_name;

-- Find tables without primary keys
SELECT table_schema, table_name
FROM information_schema.tables t
WHERE NOT EXISTS (
    SELECT 1
    FROM information_schema.table_constraints tc
    WHERE tc.constraint_schema = t.table_schema
      AND tc.table_name = t.table_name
      AND tc.constraint_type = 'PRIMARY KEY'
);
```

#### information_schema.key_column_usage

Contains one row for each column that is part of a key (primary, foreign, or unique).

**Columns:**
```sql
CREATE VIEW information_schema.key_column_usage AS
SELECT
    database_name AS constraint_catalog,
    schema_name AS constraint_schema,
    constraint_name,
    table_catalog,
    table_schema,
    table_name,
    column_name,
    ordinal_position,
    position_in_unique_constraint,
    NULL::varchar AS constraint_schema_ref,
    NULL::varchar AS constraint_name_ref
FROM duckdb_key_columns();
```

#### information_schema.referential_constraints

Contains one row for each foreign key constraint.

**Columns:**
```sql
CREATE VIEW information_schema.referential_constraints AS
SELECT
    database_name AS constraint_catalog,
    schema_name AS constraint_schema,
    constraint_name,
    'MATCH SIMPLE' AS match_option,
    CASE update_rule
        WHEN 'CASCADE' THEN 'CASCADE'
        WHEN 'SET NULL' THEN 'SET NULL'
        WHEN 'SET DEFAULT' THEN 'SET DEFAULT'
        ELSE 'NO ACTION'
    END AS update_rule,
    CASE delete_rule
        WHEN 'CASCADE' THEN 'CASCADE'
        WHEN 'SET NULL' THEN 'SET NULL'
        WHEN 'SET DEFAULT' THEN 'SET DEFAULT'
        ELSE 'NO ACTION'
    END AS delete_rule,
    NULL::varchar AS unique_constraint_catalog,
    NULL::varchar AS unique_constraint_schema,
    NULL::varchar AS unique_constraint_name,
    'NONE' AS constraint_trigger_count
FROM duckdb_referential_constraints();
```

#### information_schema.check_constraints

Contains one row for each check constraint.

**Columns:**
```sql
CREATE VIEW information_schema.check_constraints AS
SELECT
    database_name AS constraint_catalog,
    schema_name AS constraint_schema,
    constraint_name,
    check_clause AS check_clause
FROM duckdb_check_constraints();
```

**Example Usage:**
```sql
-- List all check constraints
SELECT constraint_schema, constraint_name, check_clause
FROM information_schema.check_constraints;

-- Example result:
-- constraint_schema | constraint_name | check_clause
-- ------------------|-----------------|-------------
-- main              | chk_price       | price > 0
-- main              | chk_email       | email LIKE '%@%.%'
```

#### information_schema.routines

Contains one row for each function or procedure.

**Columns:**
```sql
CREATE VIEW information_schema.routines AS
SELECT
    schema_name AS routine_catalog,
    schema_name AS routine_schema,
    function_name AS routine_name,
    'FUNCTION' AS routine_type,
    NULL::varchar AS module_catalog,
    NULL::varchar AS module_schema,
    NULL::varchar AS module_name,
    NULL::varchar AS udt_catalog,
    NULL::varchar AS udt_schema,
    NULL::varchar AS udt_name,
    NULL::varchar AS specific_name,
    routine_language AS routine_language,
    NULL::varchar AS routine_definition,
    NULL::varchar AS external_name,
    'ORIGINAL' AS external_language,
    'GENERAL' AS parameter_style,
    CASE
        WHEN is_deterministic THEN 'YES'
        ELSE 'NO'
    END AS is_deterministic,
    'NO' AS sql_data_access,
    NULL::varchar AS sql_path,
    security_type AS security_type,
    created_at AS created,
    NULL::varchar AS last_altered,
    NULL::varchar AS sql_mode,
    'YES' AS routine_body,
    NULL::varchar AS character_set_client,
    NULL::varchar AS collation_connection,
    NULL::varchar AS database_collation
FROM duckdb_functions();
```

#### information_schema.parameters

Contains one row for each parameter of each function.

**Columns:**
```sql
CREATE VIEW information_schema.parameters AS
SELECT
    schema_name AS specific_catalog,
    schema_name AS specific_schema,
    function_name AS specific_name,
    0 AS ordinal_position,
    'IN' AS parameter_mode,
    parameter_name,
    NULL::varchar AS user_defined_type_catalog,
    NULL::varchar AS user_defined_type_schema,
    NULL::varchar AS user_defined_type_name,
    data_type AS data_type,
    character_maximum_length,
    character_octet_length,
    numeric_precision,
    numeric_precision_radix,
    numeric_scale,
    datetime_precision,
    interval_type,
    interval_precision,
    NULL::varchar AS character_set_catalog,
    NULL::varchar AS character_set_schema,
    NULL::varchar AS character_set_name,
    NULL::varchar AS collation_catalog,
    NULL::varchar AS collation_schema,
    NULL::varchar AS collation_name,
    NULL::varchar AS domain_catalog,
    NULL::varchar AS domain_schema,
    NULL::varchar AS domain_name
FROM duckdb_function_parameters();
```

### Additional information_schema Views

#### information_schema.user_defined_types

Contains information about user-defined types (if any).

#### information_schema.domains

Contains information about domain types (if any).

#### information_schema.element_types

Contains information about array element types.

#### information_schema.triggers

Contains information about triggers (when implemented).

#### information_schema.sequences

Contains information about sequences.

**Columns:**
```sql
CREATE VIEW information_schema.sequences AS
SELECT
    database_name AS sequence_catalog,
    schema_name AS sequence_schema,
    sequence_name,
    data_type AS data_type,
    numeric_precision,
    numeric_precision_radix,
    numeric_scale,
    start_value,
    minimum_value,
    maximum_value,
    increment,
    cycle_option
FROM duckdb_sequences();
```

## pg_catalog

pg_catalog provides PostgreSQL-compatible system tables for compatibility with PostgreSQL tools and applications.

### Core Tables

#### pg_catalog.pg_class

Catalog of tables, indexes, sequences, and views.

**Columns:**
```sql
CREATE TABLE pg_catalog.pg_class (
    oid OID,
    relname NAME,
    relnamespace OID,
    reltype OID,
    reloftype OID,
    relowner OID,
    relam OID,
    relfilenode OID,
    reltablespace OID,
    relpages INTEGER,
    reltuples REAL,
    relallvisible INTEGER,
    reltoastrelid OID,
    relhasindex BOOLEAN,
    relisshared BOOLEAN,
    relpersistence "char",
    relkind "char",
    relnatts SMALLINT,
    relchecks SMALLINT,
    relhasoids BOOLEAN,
    relhaspkey BOOLEAN,
    relhasrules BOOLEAN,
    relhastriggers BOOLEAN,
    relhassubclass BOOLEAN,
    relrowsecurity BOOLEAN,
    relforcerowsecurity BOOLEAN,
    relispopulated BOOLEAN,
    relreplident "char",
    relispartition BOOLEAN,
    relfrozenxid XID,
    relminmxid XID,
    relacl ACLITEM[],
    reloptions TEXT[],
    relpartbound TEXT
);
```

**Key Columns:**
- `oid`: Object identifier
- `relname`: Object name
- `relnamespace`: Schema OID
- `relkind`: Object type ('r'=table, 'i'=index, 'S'=sequence, 'v'=view)
- `reltuples`: Estimated row count
- `relhaspkey`: Has primary key

**Example Usage:**
```sql
-- List all tables
SELECT c.relname AS table_name
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('information_schema', 'pg_catalog');

-- Find tables without primary keys
SELECT relname
FROM pg_catalog.pg_class
WHERE relkind = 'r'
  AND relhaspkey = false;
```

#### pg_catalog.pg_namespace

Catalog of schemas (namespaces).

**Columns:**
```sql
CREATE TABLE pg_catalog.pg_namespace (
    oid OID,
    nspname NAME,
    nspowner OID,
    nspacl ACLITEM[]
);
```

**Example Usage:**
```sql
-- List all schemas
SELECT nspname AS schema_name
FROM pg_catalog.pg_namespace
ORDER BY nspname;

-- Count objects by schema
SELECT n.nspname, COUNT(*) as object_count
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
GROUP BY n.nspname;
```

#### pg_catalog.pg_attribute

Catalog of table columns (attributes).

**Columns:**
```sql
CREATE TABLE pg_catalog.pg_attribute (
    attrelid OID,
    attname NAME,
    atttypid OID,
    attstattarget INTEGER,
    attlen SMALLINT,
    attnum SMALLINT,
    attndims INTEGER,
    attcacheoff INTEGER,
    atttypmod INTEGER,
    attbyval BOOLEAN,
    attstorage "char",
    attalign "char",
    attnotnull BOOLEAN,
    atthasdef BOOLEAN,
    attidentity "char",
    attgenerated "char",
    attisdropped BOOLEAN,
    attislocal BOOLEAN,
    attinhcount INTEGER,
    attcollation OID,
    attacl ACLITEM[],
    attoptions TEXT[],
    attfdwoptions TEXT[]
);
```

**Key Columns:**
- `attrelid`: Table OID
- `attname`: Column name
- `atttypid`: Data type OID
- `attnum`: Column number (1-based)
- `attnotnull`: Has NOT NULL constraint
- `atthasdef`: Has default value

**Example Usage:**
```sql
-- Get columns for a table
SELECT a.attname AS column_name,
       pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
       a.attnotnull AS not_null,
       a.atthasdef AS has_default
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = 'users'::regclass
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;
```

#### pg_catalog.pg_index

Catalog of indexes.

**Columns:**
```sql
CREATE TABLE pg_catalog.pg_index (
    indexrelid OID,
    indrelid OID,
    indnatts SMALLINT,
    nindisunique BOOLEAN,
    indisprimary BOOLEAN,
    indisexclusion BOOLEAN,
    indimmediate BOOLEAN,
    indisclustered BOOLEAN,
    indisvalid BOOLEAN,
    indcheckxmin BOOLEAN,
    indisready BOOLEAN,
    indislive BOOLEAN,
    indisreplident BOOLEAN,
    indkey INT2VECTOR,
    indcollation OIDVECTOR,
    indclass OIDVECTOR,
    indoption INT2VECTOR,
    indexprs TEXT,
    indpred TEXT
);
```

**Key Columns:**
- `indexrelid`: Index OID
- `indrelid`: Table OID
- `indisunique`: Is unique index
- `indisprimary`: Is primary key index
- `indkey`: Array of column numbers

#### pg_catalog.pg_constraint

Catalog of table constraints.

**Columns:**
```sql
CREATE TABLE pg_catalog.pg_constraint (
    oid OID,
    conname NAME,
    connamespace OID,
    contype "char",
    condeferrable BOOLEAN,
    condeferred BOOLEAN,
    convalidated BOOLEAN,
    conrelid OID,
    contypid OID,
    conindid OID,
    conparentid OID,
    confrelid OID,
    confupdtype "char",
    confdeltype "char",
    confmatchtype "char",
    conislocal BOOLEAN,
    coninhcount INTEGER,
    connoinherit BOOLEAN,
    conkey INT2VECTOR,
    confkey INT2VECTOR,
    conpfeqop OIDVECTOR,
    conppeqop OIDVECTOR,
    conffeqop OIDVECTOR,
    conexcl OIDVECTOR,
    conbin TEXT
);
```

**Constraint Types (contype):**
- 'c': CHECK constraint
- 'f': FOREIGN KEY constraint
- 'p': PRIMARY KEY constraint
- 'u': UNIQUE constraint
- 't': TRIGGER constraint
- 'x': EXCLUSION constraint

#### pg_catalog.pg_proc

Catalog of functions and procedures.

**Columns:**
```sql
CREATE TABLE pg_catalog.pg_proc (
    oid OID,
    proname NAME,
    pronamespace OID,
    proowner OID,
    prolang OID,
    procost REAL,
    prorows REAL,
    provariadic OID,
    prosupport TEXT,
    prokind "char",
    prosecdef BOOLEAN,
    proleakproof BOOLEAN,
    proisstrict BOOLEAN,
    proretset BOOLEAN,
    provolatile "char",
    proparallel "char",
    pronargs SMALLINT,
    pronargdefaults SMALLINT,
    prorettype OID,
    proargtypes OIDVECTOR,
    proallargtypes OID[],
    proargmodes "char"[],
    proargnames TEXT[],
    proargdefaults TEXT,
    protrftypes OID[],
    prosrc TEXT,
    probin TEXT,
    proconfig TEXT[],
    proacl ACLITEM[]
);
```

#### pg_catalog.pg_type

Catalog of data types.

**Columns:**
```sql
CREATE TABLE pg_catalog.pg_type (
    oid OID,
    typname NAME,
    typnamespace OID,
    typowner OID,
    typlen SMALLINT,
    typbyval BOOLEAN,
    typtype "char",
    typcategory "char",
    typispreferred BOOLEAN,
    typisdefined BOOLEAN,
    typdelim "char",
    typrelid OID,
    typelem OID,
    typarray OID,
    typinput TEXT,
    typoutput TEXT,
    typreceive TEXT,
    typsend TEXT,
    typmodin TEXT,
    typmodout TEXT,
    typanalyze TEXT,
    typalign "char",
    typstorage "char",
    typnotnull BOOLEAN,
    typbasetype OID,
    typtypmod INTEGER,
    typndims INTEGER,
    typcollation OID,
    typdefaultbin TEXT,
    typdefault TEXT,
    typacl ACLITEM[]
);
```

**Example Usage:**
```sql
-- List all types
SELECT oid, typname
FROM pg_catalog.pg_type
ORDER BY typname;

-- Get type information
SELECT t.typname,
       pg_catalog.format_type(t.oid, NULL) AS formatted_type
FROM pg_catalog.pg_type t
WHERE t.typname = 'varchar';
```

#### pg_catalog.pg_database

Catalog of databases.

**Columns:**
```sql
CREATE TABLE pg_catalog.pg_database (
    oid OID,
    datname NAME,
    datdba OID,
    encoding INTEGER,
    datcollate TEXT,
    datctype TEXT,
    datistemplate BOOLEAN,
    datallowconn BOOLEAN,
    datconnlimit INTEGER,
    datlastsysoid OID,
    datfrozenxid XID,
    datminmxid XID,
    dattablespace OID,
    datacl ACLITEM[]
);
```

#### pg_catalog.pg_user

Catalog of database users.

**Columns:**
```sql
CREATE TABLE pg_catalog.pg_user (
    usename NAME,
    usesysid OID,
    usecreatedb BOOLEAN,
    usesuper BOOLEAN,
    userepl BOOLEAN,
    usebypassrls BOOLEAN,
    passwd TEXT,
    valuntil TIMESTAMP,
    useconfig TEXT[]
);
```

#### pg_catalog.pg_stat_activity

Shows active server sessions.

**Columns:**
```sql
CREATE TABLE pg_catalog.pg_stat_activity (
    datid OID,
    datname NAME,
    pid INTEGER,
    usesysid OID,
    usename NAME,
    application_name TEXT,
    client_addr INET,
    client_hostname TEXT,
    client_port INTEGER,
    backend_start TIMESTAMP,
    xact_start TIMESTAMP,
    query_start TIMESTAMP,
    state_change TIMESTAMP,
    wait_event_type TEXT,
    wait_event TEXT,
    state TEXT,
    backend_xid XID,
    backend_xmin XID,
    query TEXT,
    backend_type TEXT
);
```

**Example Usage:**
```sql
-- Show active queries
SELECT pid, usename, application_name, state, query
FROM pg_catalog.pg_stat_activity
WHERE state = 'active';

-- Show idle connections
SELECT pid, usename, application_name, state_change
FROM pg_catalog.pg_stat_activity
WHERE state = 'idle';
```

#### pg_catalog.pg_settings

Shows server settings/parameters.

**Columns:**
```sql
CREATE TABLE pg_catalog.pg_settings (
    name TEXT,
    setting TEXT,
    unit TEXT,
    category TEXT,
    short_desc TEXT,
    extra_desc TEXT,
    context TEXT,
    vartype TEXT,
    source TEXT,
    min_val TEXT,
    max_val TEXT,
    enumvals TEXT[],
    boot_val TEXT,
    reset_val TEXT,
    sourcefile TEXT,
    sourceline INTEGER,
    pending_restart BOOLEAN
);
```

**Example Usage:**
```sql
-- Show all settings
SELECT name, setting, unit, category
FROM pg_catalog.pg_settings
ORDER BY name;

-- Find memory-related settings
SELECT name, setting, unit
FROM pg_catalog.pg_settings
WHERE name LIKE '%memory%';
```

### PostgreSQL-Specific Views

These views provide additional PostgreSQL compatibility.

#### pg_catalog.pg_indexes

Convenience view showing index information.

```sql
CREATE VIEW pg_catalog.pg_indexes AS
SELECT
    n.nspname AS schemaname,
    c.relname AS tablename,
    i.relname AS indexname,
    NULL::text AS tablespace,
    pg_get_indexdef(i.oid) AS indexdef
FROM pg_catalog.pg_index x
JOIN pg_catalog.pg_class c ON c.oid = x.indrelid
JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace;
```

#### pg_catalog.pg_tables

Convenience view showing table information.

```sql
CREATE VIEW pg_catalog.pg_tables AS
SELECT
    n.nspname AS schemaname,
    c.relname AS tablename,
    pg_get_userbyid(c.relowner) AS tableowner,
    NULL::text AS tablespace,
    FALSE AS hasindexes,
    FALSE AS hasrules,
    FALSE AS hastriggers,
    FALSE AS rowsecurity
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r';
```

## Implementation Details

### System Table Generation

System tables are generated from system function results:

```go
func generateInformationSchemaTable(tableName string) *DataChunk {
    switch tableName {
    case "tables":
        return generateInformationSchemaTables()
    case "columns":
        return generateInformationSchemaColumns()
    case "schemata":
        return generateInformationSchemaSchemata()
    // ... other views
    }
}

func generateInformationSchemaTables() *DataChunk {
    // Get data from duckdb_tables()
    tables := duckdbTables()

    // Transform to information_schema format
    chunk := NewDataChunk()
    chunk.AddVector("table_catalog", types.VARCHAR)
    chunk.AddVector("table_schema", types.VARCHAR)
    chunk.AddVector("table_name", types.VARCHAR)
    chunk.AddVector("table_type", types.VARCHAR)
    // ... add all columns

    for _, table := range tables {
        chunk.Append([]interface{}{
            table.DatabaseName,
            table.SchemaName,
            table.TableName,
            transformTableType(table.TableType),
            // ... map all fields
        })
    }

    return chunk
}
```

### Caching Strategy

1. **Metadata Cache**: Cache system table results
2. **Invalidation**: Invalidate on DDL changes
3. **Lazy Loading**: Generate on first access
4. **TTL**: Time-based expiration for cached views

```go
type SystemTableCache struct {
    mu       sync.RWMutex
    tables   map[string]*CachedTable
    ttl      time.Duration
}

type CachedTable struct {
    data      *DataChunk
    createdAt time.Time
    updatedAt time.Time
}

func (c *SystemTableCache) Get(tableName string) (*DataChunk, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()

    if table, ok := c.tables[tableName]; ok {
        if time.Since(table.createdAt) < c.ttl {
            return table.data, true
        }
    }

    return nil, false
}
```

### Index Usage

System tables support indexes on common query patterns:

```sql
-- Create indexes for frequent queries
CREATE INDEX idx_tables_schema ON __system_tables(schema_name);
CREATE INDEX idx_tables_name ON __system_tables(table_name);
CREATE INDEX idx_columns_table ON __system_columns(table_name);
CREATE INDEX idx_functions_name ON __system_functions(function_name);
```

### Performance Optimizations

1. **Predicate Pushdown**: Push WHERE clauses to system functions
2. **Column Projection**: Only generate required columns
3. **Parallel Generation**: Generate multiple views in parallel
4. **Incremental Updates**: Update only changed metadata
5. **Materialized Views**: Pre-compute complex joins

## Usage Examples

### Schema Discovery

```sql
-- Discover database schema
SELECT table_schema, table_name, table_type
FROM information_schema.tables
ORDER BY table_schema, table_name;

-- Get column details
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_name = 'orders'
ORDER BY ordinal_position;

-- Find all foreign keys
SELECT tc.table_name, tc.constraint_name,
       kcu.column_name,
       ccu.table_name AS foreign_table_name,
       ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY';
```

### PostgreSQL Compatibility

```sql
-- PostgreSQL-style table listing
SELECT n.nspname AS schema_name,
       c.relname AS table_name,
       CASE c.relkind
           WHEN 'r' THEN 'table'
           WHEN 'v' THEN 'view'
           WHEN 'i' THEN 'index'
           WHEN 'S' THEN 'sequence'
       END AS table_type
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'v')
  AND n.nspname NOT IN ('information_schema', 'pg_catalog')
ORDER BY n.nspname, c.relname;

-- Get column info PostgreSQL style
SELECT a.attname AS column_name,
       pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
       a.attnotnull AS not_null,
       a.atthasdef AS has_default
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = 'users'::regclass
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;
```

### System Monitoring

```sql
-- Monitor active queries PostgreSQL style
SELECT pid, usename, application_name, state, query_start, query
FROM pg_catalog.pg_stat_activity
WHERE state = 'active';

-- Check database settings
SELECT name, setting, unit, short_desc
FROM pg_catalog.pg_settings
WHERE name LIKE '%memory%'
   OR name LIKE '%thread%';
```

## Testing Requirements

### Unit Tests

1. **View Definition Tests**: Verify all views are created correctly
2. **Column Mapping Tests**: Verify correct column mappings
3. **Data Type Tests**: Verify correct type conversions
4. **Filter Tests**: Test WHERE clause support
5. **Join Tests**: Test joining system tables

### Integration Tests

1. **DuckDB Compatibility**: Compare with DuckDB v1.4.3 output
2. **PostgreSQL Tool Compatibility**: Test with common PostgreSQL tools
3. **Performance Tests**: Benchmark large catalog queries
4. **Concurrent Access**: Test thread safety
5. **DDL Trigger Tests**: Verify cache invalidation

### Compatibility Tests

```sql
-- Test PostgreSQL compatibility views
-- These should return same results as PostgreSQL

-- List schemas
SELECT schema_name
FROM information_schema.schemata
ORDER BY schema_name;

-- List tables with columns
SELECT t.table_schema, t.table_name, c.column_name, c.data_type
FROM information_schema.tables t
JOIN information_schema.columns c ON t.table_name = c.table_name
ORDER BY t.table_schema, t.table_name, c.ordinal_position;

-- Check PostgreSQL system tables
SELECT COUNT(*) FROM pg_catalog.pg_class;
SELECT COUNT(*) FROM pg_catalog.pg_namespace;
SELECT COUNT(*) FROM pg_catalog.pg_attribute;
```

## Performance Considerations

### Query Optimization

1. **Use Specific Filters**: Filter early in WHERE clause
2. **Avoid SELECT ***: Select only needed columns
3. **Use LIMIT**: Limit large result sets
4. **Cache Results**: Cache frequently accessed metadata

### Memory Usage

1. **Streaming**: Stream large result sets
2. **Chunking**: Process in chunks
3. **Lazy Loading**: Load metadata on demand
4. **Cleanup**: Clean up unused cached data

### Indexing Strategy

```sql
-- Common access patterns
-- Schema + Table lookups
-- Table + Column lookups
-- Function name lookups
-- Constraint lookups

-- Create appropriate indexes
CREATE INDEX idx_info_tables_lookup ON __system_tables(schema_name, table_name);
CREATE INDEX idx_info_columns_lookup ON __system_columns(table_name, column_name);
```

## Security Considerations

### Access Control

1. **Schema Visibility**: Users see only accessible schemas
2. **Table Permissions**: Filter based on table privileges
3. **Column Masking**: Hide sensitive columns
4. **Row Level Security**: Apply RLS policies

### Privileges

```sql
-- Grant access to information_schema
GRANT SELECT ON information_schema.tables TO public;
GRANT SELECT ON information_schema.columns TO public;

-- Grant access to pg_catalog
GRANT SELECT ON pg_catalog.pg_class TO public;
GRANT SELECT ON pg_catalog.pg_namespace TO public;

-- Restrict sensitive views
REVOKE SELECT ON pg_catalog.pg_user FROM public;
REVOKE SELECT ON pg_catalog.pg_stat_activity FROM public;
```

### Audit Logging

All metadata queries are logged:
```
[timestamp] user='user1' query='SELECT * FROM information_schema.tables' rows=25
[timestamp] user='user2' query='SELECT * FROM pg_catalog.pg_class' rows=100
```

## Future Enhancements

1. **Additional Views**: Complete SQL standard views
2. **Performance Views**: More performance monitoring
3. **Extension Views**: Views for extensions
4. **Custom Views**: User-defined metadata views
5. **Real-time Updates**: Live metadata updates
6. **Distributed Metadata**: Multi-node metadata views