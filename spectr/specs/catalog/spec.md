# Catalog Specification

## Requirements

### Requirement: View Definition Storage
The catalog SHALL provide storage for view definitions with the following structure:
- View name (unique within schema)
- Schema name (namespace)
- View query (serialized SELECT statement)

#### Scenario: Create a view in main schema
- WHEN calling `catalog.CreateView(viewDef)` with a ViewDef for "my_view"
- THEN the view SHALL be stored in the "main" schema
- THEN subsequent `GetView("my_view")` SHALL return the view definition

#### Scenario: Create a view in custom schema
- WHEN calling `catalog.CreateViewInSchema("schema", viewDef)` for "my_view"
- THEN the view SHALL be stored in "schema" schema
- THEN subsequent `GetViewInSchema("schema", "my_view")` SHALL return the view definition

#### Scenario: View name conflict in same schema
- WHEN creating a view with a name that already exists in the schema
- THEN the catalog SHALL return an error
- THEN the existing view SHALL remain unchanged

#### Scenario: View with IF NOT EXISTS
- WHEN creating a view that already exists with IF NOT EXISTS flag
- THEN the catalog SHALL NOT return an error
- THEN the existing view SHALL remain unchanged

### Requirement: View Retrieval
The catalog SHALL provide methods to retrieve view definitions.

#### Scenario: Get view in default schema
- WHEN calling `GetView("view_name")`
- THEN the catalog SHALL search in the "main" schema
- THEN it SHALL return (ViewDef, true) if found
- THEN it SHALL return (nil, false) if not found

#### Scenario: Get view in specific schema
- WHEN calling `GetViewInSchema("schema_name", "view_name")`
- THEN the catalog SHALL search only in the specified schema
- THEN it SHALL return (ViewDef, true) if found
- THEN it SHALL return (nil, false) if not found

#### Scenario: Get non-existent view
- WHEN calling `GetView("nonexistent")`
- THEN the catalog SHALL return (nil, false)

### Requirement: View Deletion
The catalog SHALL provide methods to delete view definitions.

#### Scenario: Drop existing view
- WHEN calling `DropView("view_name")`
- THEN the view SHALL be removed from the "main" schema
- THEN subsequent `GetView("view_name")` SHALL return (nil, false)

#### Scenario: Drop view with IF EXISTS
- WHEN calling `DropViewInSchema("schema", "view_name", true)` for non-existent view
- THEN the catalog SHALL NOT return an error

#### Scenario: Drop non-existent view without IF EXISTS
- WHEN calling `DropView("nonexistent", false)`
- THEN the catalog SHALL return an error

#### Scenario: Drop view with CASCADE
- WHEN calling `DropSchema("schema", true)` where schema contains views
- THEN all views in the schema SHALL also be dropped

### Requirement: Index Definition Storage
The catalog SHALL provide storage for index definitions with the following structure:
- Index name (unique within schema)
- Schema name
- Table name (referenced table)
- Column names (indexed columns)
- Unique flag

#### Scenario: Create an index
- WHEN calling `catalog.CreateIndex(indexDef)` for "my_idx" on table "t"
- THEN the index SHALL be stored in the "main" schema
- THEN subsequent `GetIndex("my_idx")` SHALL return the index definition

#### Scenario: Index name conflict
- WHEN creating an index with a name that already exists
- THEN the catalog SHALL return an error

#### Scenario: Index references table
- WHEN creating an index, the catalog SHALL NOT validate table existence at creation time
- THEN the index stores the table name for reference during query planning

### Requirement: Index Retrieval
The catalog SHALL provide methods to retrieve index definitions.

#### Scenario: Get index by name
- WHEN calling `GetIndex("index_name")`
- THEN the catalog SHALL return (IndexDef, true) if found
- THEN the catalog SHALL return (nil, false) if not found

#### Scenario: Get indexes for table
- WHEN calling `GetIndexesForTable("schema", "table_name")`
- THEN the catalog SHALL return all indexes defined on the specified table

### Requirement: Index Deletion
The catalog SHALL provide methods to delete index definitions.

#### Scenario: Drop existing index
- WHEN calling `DropIndex("index_name")`
- THEN the index SHALL be removed from the catalog
- THEN subsequent `GetIndex("index_name")` SHALL return (nil, false)

#### Scenario: Drop non-existent index
- WHEN calling `DropIndex("nonexistent", false)`
- THEN the catalog SHALL return an error

### Requirement: Sequence Definition Storage
The catalog SHALL provide storage for sequence definitions with the following structure:
- Sequence name (unique within schema)
- Schema name
- Current value
- Start with value
- Increment by value
- Minimum value
- Maximum value
- Cycle flag

#### Scenario: Create a sequence
- WHEN calling `catalog.CreateSequence(seqDef)` with default options
- THEN the sequence SHALL have CurrentVal=1, StartWith=1, IncrementBy=1
- THEN the sequence SHALL have MinValue=math.MinInt64, MaxValue=math.MaxInt64
- THEN the sequence SHALL have IsCycle=false

#### Scenario: Create sequence with options
- WHEN creating a sequence with START WITH 100, INCREMENT BY 2, CYCLE
- THEN the sequence SHALL store all provided options
- THEN the CurrentVal SHALL be set to StartWith

#### Scenario: Sequence name conflict
- WHEN creating a sequence with a name that already exists
- THEN the catalog SHALL return an error

### Requirement: Sequence Retrieval
The catalog SHALL provide methods to retrieve sequence definitions.

#### Scenario: Get sequence by name
- WHEN calling `GetSequence("sequence_name")`
- THEN the catalog SHALL return (SequenceDef, true) if found
- THEN the catalog SHALL return (nil, false) if not found

#### Scenario: Get sequence in specific schema
- WHEN calling `GetSequenceInSchema("schema", "sequence_name")`
- THEN the catalog SHALL return the sequence from the specified schema

### Requirement: Sequence Deletion
The catalog SHALL provide methods to delete sequence definitions.

#### Scenario: Drop existing sequence
- WHEN calling `DropSequence("sequence_name")`
- THEN the sequence SHALL be removed from the catalog
- THEN subsequent `GetSequence("sequence_name")` SHALL return (nil, false)

### Requirement: Sequence Value Generation
The catalog SHALL provide methods to generate sequence values.

#### Scenario: NEXTVAL increments and returns
- WHEN calling `NextVal("sequence_name")` on a sequence with CurrentVal=1
- THEN the method SHALL return 2
- THEN the sequence CurrentVal SHALL be updated to 2

#### Scenario: NEXTVAL respects increment
- WHEN calling `NextVal` on a sequence with IncrementBy=5
- THEN the return value SHALL increase by 5

#### Scenario: NEXTVAL at max value without cycle
- WHEN calling `NextVal` when CurrentVal equals MaxValue and IsCycle=false
- THEN the method SHALL return an error

#### Scenario: NEXTVAL at max value with cycle
- WHEN calling `NextVal` when CurrentVal equals MaxValue and IsCycle=true
- THEN the method SHALL return the StartWith value
- THEN CurrentVal SHALL be reset to StartWith

#### Scenario: CURRVAL returns current value
- WHEN calling `CurrVal("sequence_name")` on a sequence with CurrentVal=5
- THEN the method SHALL return 5
- THEN the sequence SHALL not be modified

### Requirement: Schema Namespace Management
The catalog SHALL support schema namespaces for organizing database objects.

#### Scenario: Create schema
- WHEN calling `catalog.CreateSchema("my_schema")`
- THEN a new schema SHALL be created with empty tables, views, indexes, sequences maps
- THEN subsequent `GetSchema("my_schema")` SHALL return the schema

#### Scenario: Create schema with IF NOT EXISTS
- WHEN calling `CreateSchema("my_schema", true)` on an existing schema
- THEN the method SHALL NOT return an error
- THEN the existing schema SHALL remain unchanged

#### Scenario: Drop empty schema
- WHEN calling `DropSchema("my_schema")` on an empty schema
- THEN the schema SHALL be removed from the catalog

#### Scenario: Drop schema with RESTRICT (default)
- WHEN calling `DropSchema("my_schema", false)` on a schema with objects
- THEN the method SHALL return an error
- THEN the schema SHALL not be removed

#### Scenario: Drop schema with CASCADE
- WHEN calling `DropSchema("my_schema", true)` on a schema with objects
- THEN all objects (tables, views, indexes, sequences) in the schema SHALL be dropped
- THEN the schema SHALL be removed

### Requirement: Cross-Schema Object Resolution
The catalog SHALL support resolving objects across schemas.

#### Scenario: Get table in specific schema
- WHEN calling `GetTableInSchema("schema", "table")`
- THEN the catalog SHALL search only in the specified schema

#### Scenario: Get view in specific schema
- WHEN calling `GetViewInSchema("schema", "view")`
- THEN the catalog SHALL search only in the specified schema

#### Scenario: Schema-qualified table reference
- WHEN resolving `schema.table` reference
- THEN the binder SHALL use `GetTableInSchema("schema", "table")`

### Requirement: Catalog Transaction Safety
All catalog operations SHALL be thread-safe using appropriate locking.

#### Scenario: Concurrent view creation
- WHEN multiple goroutines create views concurrently
- THEN exactly one view SHALL be created
- THEN the others SHALL receive "already exists" errors

#### Sequence: Concurrent sequence access
- WHEN multiple goroutines call NextVal on the same sequence
- THEN each call SHALL return a unique, incrementing value
- THEN no values SHALL be skipped or duplicated

### Requirement: Catalog Persistence
Catalog changes for DDL operations SHALL be persistable via WAL.

#### Scenario: WAL entry for CREATE VIEW
- WHEN creating a view, the WAL SHALL write a CreateViewEntry with Schema, Name, Query

#### Scenario: WAL entry for CREATE INDEX
- WHEN creating an index, the WAL SHALL write a CreateIndexEntry with Schema, Table, Name, Columns, IsUnique

#### Scenario: WAL entry for CREATE SEQUENCE
- WHEN creating a sequence, the WAL SHALL write a CreateSequenceEntry with all sequence options

#### Scenario: WAL recovery for DDL
- WHEN recovering from WAL, the catalog SHALL replay all DDL entries
- THEN all created objects SHALL be restored to their post-DDL state

### Requirement: Catalog Serialization
Catalog entries for views, indexes, and sequences SHALL be serializable.

#### Scenario: Serialize view definition
- WHEN serializing a ViewDef to bytes
- THEN the bytes SHALL contain the view name, schema, and query
- THEN deserializing SHALL produce an equivalent ViewDef

#### Scenario: Serialize index definition
- WHEN serializing an IndexDef to bytes
- THEN the bytes SHALL contain the index name, schema, table, columns, and flags
- THEN deserializing SHALL produce an equivalent IndexDef

#### Scenario: Serialize sequence definition
- WHEN serializing a SequenceDef to bytes
- THEN the bytes SHALL contain all sequence metadata and current value
- THEN deserializing SHALL produce an equivalent SequenceDef

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
