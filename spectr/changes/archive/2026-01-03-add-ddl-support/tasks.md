# Tasks: Full DDL Support

## 1. Parser Implementation

- [ ] 1.1 Add DDL statement types to `internal/parser/ast.go`
  - [ ] 1.1.1 Add `CreateViewStmt` struct with Schema, View, IfNotExists, Query fields
  - [ ] 1.1.2 Add `DropViewStmt` struct with Schema, View, IfExists fields
  - [ ] 1.1.3 Add `CreateIndexStmt` struct with Schema, Table, Index, IfNotExists, Columns, IsUnique fields
  - [ ] 1.1.4 Add `DropIndexStmt` struct with Schema, Index, IfExists fields
  - [ ] 1.1.5 Add `CreateSequenceStmt` struct with all sequence options
  - [ ] 1.1.6 Add `DropSequenceStmt` struct
  - [ ] 1.1.7 Add `CreateSchemaStmt` and `DropSchemaStmt` structs
  - [ ] 1.1.8 Add `AlterTableStmt` struct with operation types

- [ ] 1.2 Add visitor methods to `internal/parser/visitor.go`
  - [ ] 1.2.1 Add `VisitCreateViewStmt(stmt *CreateViewStmt)`
  - [ ] 1.2.2 Add `VisitDropViewStmt(stmt *DropViewStmt)`
  - [ ] 1.2.3 Add visitor methods for all new statement types

- [ ] 1.3 Create `internal/parser/parser_ddl.go` with parsing functions
  - [ ] 1.3.1 Create `parseCreateView()` function
  - [ ] 1.3.2 Create `parseDropView()` function
  - [ ] 1.3.3 Create `parseCreateIndex()` function
  - [ ] 1.3.4 Create `parseDropIndex()` function
  - [ ] 1.3.5 Create `parseCreateSequence()` function
  - [ ] 1.3.6 Create `parseDropSequence()` function
  - [ ] 1.3.7 Create `parseCreateSchema()` function
  - [ ] 1.3.8 Create `parseDropSchema()` function
  - [ ] 1.3.9 Create `parseAlterTable()` function

- [ ] 1.4 Update `internal/parser/parser.go` dispatch logic
  - [ ] 1.4.1 Update `parseCreate()` to dispatch to new DDL handlers
  - [ ] 1.4.2 Update `parseDrop()` to dispatch to new DDL handlers

- [ ] 1.5 Add parser tests for DDL statements
  - [ ] 1.5.1 Test CREATE VIEW parsing
  - [ ] 1.5.2 Test DROP VIEW parsing
  - [ ] 1.5.3 Test CREATE INDEX parsing
  - [ ] 1.5.4 Test DROP INDEX parsing
  - [ ] 1.5.5 Test CREATE SEQUENCE parsing
  - [ ] 1.5.6 Test DROP SEQUENCE parsing
  - [ ] 1.5.7 Test CREATE/DROP SCHEMA parsing
  - [ ] 1.5.8 Test ALTER TABLE parsing

## 2. Catalog Implementation

- [ ] 2.1 Create `internal/catalog/view.go`
  - [ ] 2.1.1 Define `ViewDef` struct with Name, Schema, Query fields
  - [ ] 2.1.2 Add `NewViewDef()` constructor
  - [ ] 2.1.3 Add `ViewDef` methods

- [ ] 2.2 Create `internal/catalog/index.go`
  - [ ] 2.2.1 Define `IndexDef` struct with Name, Schema, Table, Columns, IsUnique fields
  - [ ] 2.2.2 Add `NewIndexDef()` constructor

- [ ] 2.3 Create `internal/catalog/sequence.go`
  - [ ] 2.3.1 Define `SequenceDef` struct with all sequence metadata
  - [ ] 2.3.2 Add `NewSequenceDef()` constructor
  - [ ] 2.3.3 Add sequence value management methods

- [ ] 2.4 Update `internal/catalog/schema.go`
  - [ ] 2.4.1 Add `views`, `indexes`, `sequences` maps to Schema struct
  - [ ] 2.4.2 Add `GetView()`, `CreateView()`, `DropView()` methods
  - [ ] 2.4.3 Add `GetIndex()`, `CreateIndex()`, `DropIndex()` methods
  - [ ] 2.4.4 Add `GetSequence()`, `CreateSequence()`, `DropSequence()` methods
  - [ ] 2.4.5 Update `NewSchema()` to initialize new maps

- [ ] 2.5 Update `internal/catalog/catalog.go`
  - [ ] 2.5.1 Add `GetView()`, `GetViewInSchema()` methods
  - [ ] 2.5.2 Add `CreateView()`, `DropView()` methods
  - [ ] 2.5.3 Add `GetIndex()`, `GetIndexInSchema()` methods
  - [ ] 2.5.4 Add `CreateIndex()`, `DropIndex()` methods
  - [ ] 2.5.5 Add `GetSequence()`, `GetSequenceInSchema()` methods
  - [ ] 2.5.6 Add `CreateSequence()`, `DropSequence()` methods

- [ ] 2.6 Add catalog tests
  - [ ] 2.6.1 Test view CRUD operations
  - [ ] 2.6.2 Test index CRUD operations
  - [ ] 2.6.3 Test sequence CRUD operations
  - [ ] 2.6.4 Test schema namespace isolation

## 3. Binder Implementation

- [ ] 3.1 Update `internal/binder/binder.go` Bind() method
  - [ ] 3.1.1 Add case for `*parser.CreateViewStmt`
  - [ ] 3.1.2 Add case for `*parser.DropViewStmt`
  - [ ] 3.1.3 Add case for `*parser.CreateIndexStmt`
  - [ ] 3.1.4 Add case for `*parser.DropIndexStmt`
  - [ ] 3.1.5 Add case for `*parser.CreateSequenceStmt`
  - [ ] 3.1.6 Add case for `*parser.DropSequenceStmt`
  - [ ] 3.1.7 Add case for `*parser.CreateSchemaStmt`
  - [ ] 3.1.8 Add case for `*parser.DropSchemaStmt`
  - [ ] 3.1.9 Add case for `*parser.AlterTableStmt`

- [ ] 3.2 Create `internal/binder/bind_ddl.go`
  - [ ] 3.2.1 Create `bindCreateView()` function
  - [ ] 3.2.2 Create `bindDropView()` function
  - [ ] 3.2.3 Create `bindCreateIndex()` function
  - [ ] 3.2.4 Create `bindDropIndex()` function
  - [ ] 3.2.5 Create `bindCreateSequence()` function
  - [ ] 3.2.6 Create `bindDropSequence()` function
  - [ ] 3.2.7 Create `bindCreateSchema()` function
  - [ ] 3.2.8 Create `bindDropSchema()` function
  - [ ] 3.2.9 Create `bindAlterTable()` function

- [ ] 3.3 Update `internal/binder/bind_stmt.go`
  - [ ] 3.3.1 Modify `bindTableRef()` to check for views before tables
  - [ ] 3.3.2 Create `bindViewRef()` function for view resolution
  - [ ] 3.3.3 Implement recursive view expansion in `bindViewRef()`

- [ ] 3.4 Add bound statement types
  - [ ] 3.4.1 Add `BoundCreateViewStmt` struct
  - [ ] 3.4.2 Add `BoundDropViewStmt` struct
  - [ ] 3.4.3 Add `BoundCreateIndexStmt` struct
  - [ ] 3.4.4 Add `BoundDropIndexStmt` struct
  - [ ] 3.4.5 Add `BoundCreateSequenceStmt` struct
  - [ ] 3.4.6 Add `BoundDropSequenceStmt` struct
  - [ ] 3.4.7 Add `BoundCreateSchemaStmt` struct
  - [ ] 3.4.8 Add `BoundDropSchemaStmt` struct
  - [ ] 3.4.9 Add `BoundAlterTableStmt` struct

- [ ] 3.5 Add binder tests
  - [ ] 3.5.1 Test view binding and resolution
  - [ ] 3.5.2 Test circular view detection
  - [ ] 3.5.3 Test DDL validation
  - [ ] 3.5.4 Test schema-qualified references

## 4. WAL Integration

- [ ] 4.1 Create `internal/wal/entry_handler.go`
  - [ ] 4.1.1 Define `EntryHandler` interface
  - [ ] 4.1.2 Implement `handleCreateTable()` (existing entry, wire up)
  - [ ] 4.1.3 Implement `handleDropTable()` (existing entry, wire up)
  - [ ] 4.1.4 Implement `handleCreateView()`
  - [ ] 4.1.5 Implement `handleDropView()`
  - [ ] 4.1.6 Implement `handleCreateIndex()`
  - [ ] 4.1.7 Implement `handleDropIndex()`
  - [ ] 4.1.8 Implement `handleCreateSequence()`
  - [ ] 4.1.9 Implement `handleDropSequence()`
  - [ ] 4.1.10 Implement `handleCreateSchema()`
  - [ ] 4.1.11 Implement `handleDropSchema()`

- [ ] 4.2 Update `internal/wal/handler.go` (or reader.go) to use handlers
  - [ ] 4.2.1 Wire up entry handlers to entry dispatch
  - [ ] 4.2.2 Update WAL recovery to replay DDL entries

- [ ] 4.3 Add WAL tests
  - [ ] 4.3.1 Test DDL entry serialization/deserialization
  - [ ] 4.3.2 Test DDL WAL recovery
  - [ ] 4.3.3 Test crash recovery with DDL statements

## 5. Planner and Executor

- [ ] 5.1 Update `internal/planner/logical.go`
  - [ ] 5.1.1 Add `LogicalCreateView` node
  - [ ] 5.1.2 Add `LogicalDropView` node
  - [ ] 5.1.3 Add `LogicalCreateIndex` node
  - [ ] 5.1.4 Add `LogicalDropIndex` node
  - [ ] 5.1.5 Add `LogicalCreateSequence` node
  - [ ] 5.1.6 Add `LogicalDropSequence` node
  - [ ] 5.1.7 Add `LogicalCreateSchema` node
  - [ ] 5.1.8 Add `LogicalDropSchema` node
  - [ ] 5.1.9 Add `LogicalAlterTable` node

- [ ] 5.2 Update `internal/planner/physical.go`
  - [ ] 5.2.1 Add `PhysicalCreateView` node
  - [ ] 5.2.2 Add `PhysicalDropView` node
  - [ ] 5.2.3 Add `PhysicalCreateIndex` node
  - [ ] 5.2.4 Add `PhysicalDropIndex` node
  - [ ] 5.2.5 Add `PhysicalCreateSequence` node
  - [ ] 5.2.6 Add `PhysicalDropSequence` node
  - [ ] 5.2.7 Add `PhysicalCreateSchema` node
  - [ ] 5.2.8 Add `PhysicalDropSchema` node
  - [ ] 5.2.9 Add `PhysicalAlterTable` node

- [ ] 5.3 Create `internal/executor/ddl.go`
  - [ ] 5.3.1 Implement `ExecCreateView()` function
  - [ ] 5.3.2 Implement `ExecDropView()` function
  - [ ] 5.3.3 Implement `ExecCreateIndex()` function
  - [ ] 5.3.4 Implement `ExecDropIndex()` function
  - [ ] 5.3.5 Implement `ExecCreateSequence()` function
  - [ ] 5.3.6 Implement `ExecDropSequence()` function
  - [ ] 5.3.7 Implement `ExecCreateSchema()` function
  - [ ] 5.3.8 Implement `ExecDropSchema()` function
  - [ ] 5.3.9 Implement `ExecAlterTable()` function

- [ ] 5.4 Add planner/executor tests
  - [ ] 5.4.1 Test DDL plan generation
  - [ ] 5.4.2 Test DDL execution
  - [ ] 5.4.3 Test view creation and querying
  - [ ] 5.4.4 Test sequence operations

## 6. Storage (Indexes)

- [ ] 6.1 Create `internal/storage/index.go`
  - [ ] 6.1.1 Define `HashIndex` struct
  - [ ] 6.1.2 Define `RowID` type
  - [ ] 6.1.3 Implement hash key generation
  - [ ] 6.1.4 Implement index insertion
  - [ ] 6.1.5 Implement index lookup
  - [ ] 6.1.6 Implement index deletion

- [ ] 6.2 Create `internal/storage/index_test.go`
  - [ ] 6.2.1 Test hash index operations
  - [ ] 6.2.2 Test composite key handling
  - [ ] 6.2.3 Test unique constraint checking

## 7. Sequence Functions

- [ ] 7.1 Add sequence functions to expression evaluation
  - [ ] 7.1.1 Implement `NEXTVAL(sequence_name)` function
  - [ ] 7.1.2 Implement `CURRVAL(sequence_name)` function
  - [ ] 7.1.3 Add functions to function registry

- [ ] 7.2 Update binder to handle sequence functions
  - [ ] 7.2.1 Add sequence function resolution
  - [ ] 7.2.2 Add sequence catalog lookup

- [ ] 7.3 Add sequence tests
  - [ ] 7.3.1 Test NEXTVAL basic operation
  - [ ] 7.3.2 Test CURRVAL basic operation
  - [ ] 7.3.3 Test cycle behavior
  - [ ] 7.3.4 Test min/max bounds

## 8. ALTER TABLE Extensions

- [ ] 8.1 Implement ALTER TABLE operations
  - [ ] 8.1.1 Implement RENAME TO
  - [ ] 8.1.2 Implement RENAME COLUMN
  - [ ] 8.1.3 Implement DROP COLUMN
  - [ ] 8.1.4 Implement SET (table options)

- [ ] 8.2 Update catalog for ALTER operations
  - [ ] 8.2.1 Add table rename method
  - [ ] 8.2.2 Add column rename method
  - [ ] 8.2.3 Add column drop method

- [ ] 8.3 Add ALTER TABLE tests
  - [ ] 8.3.1 Test table rename
  - [ ] 8.3.2 Test column rename
  - [ ] 8.3.3 Test column drop
  - [ ] 8.3.4 Test dependent object handling

## 9. Integration Tests

- [ ] 9.1 Create DDL integration tests
  - [ ] 9.1.1 Test full view lifecycle (create, query, drop)
  - [ ] 9.1.2 Test full index lifecycle (create, use, drop)
  - [ ] 9.1.3 Test full sequence lifecycle (create, use, drop)
  - [ ] 9.1.4 Test schema creation and use
  - [ ] 9.1.5 Test ALTER TABLE operations

- [ ] 9.2 Create compatibility tests
  - [ ] 9.2.1 Test syntax compatibility with DuckDB
  - [ ] 9.2.2 Test behavior compatibility with DuckDB

- [ ] 9.3 Test WAL recovery with DDL
  - [ ] 9.3.1 Test recovery after CREATE VIEW
  - [ ] 9.3.2 Test recovery after CREATE INDEX
  - [ ] 9.3.3 Test recovery after CREATE SEQUENCE
  - [ ] 9.3.4 Test recovery after schema changes

## 10. Documentation

- [ ] 10.1 Add DDL documentation
  - [ ] 10.1.1 Document CREATE VIEW syntax and usage
  - [ ] 10.1.2 Document CREATE INDEX syntax and usage
  - [ ] 10.1.3 Document CREATE SEQUENCE syntax and usage
  - [ ] 10.1.4 Document schema usage
  - [ ] 10.1.5 Document ALTER TABLE operations

- [ ] 10.2 Update existing documentation
  - [ ] 10.2.1 Update SQL syntax reference
  - [ ] 10.2.2 Update catalog documentation
