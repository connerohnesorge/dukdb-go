## Context

The dukdb-go project implements a DuckDB-compatible SQL engine entirely in pure Go. This enables use in any Go environment without CGO, external binaries, or runtime dependencies.

## Goals / Non-Goals

**Goals:**
- Pure Go implementation with zero external dependencies
- DuckDB SQL dialect compatibility (core subset)
- Columnar/vectorized execution for analytics workloads
- In-memory and file-based storage support
- Thread-safe concurrent query execution

**Non-Goals:**
- 100% DuckDB feature parity (focus on core SQL first)
- Matching DuckDB's exact performance (correctness first)

## Decisions

### Decision 1: SQL Parser Selection

**What:** Adapt `github.com/auxten/postgresql-parser` to DuckDB syntax.

**Why:**
- DuckDB's SQL dialect is PostgreSQL-based
- postgresql-parser is a mature pure Go implementation
- Can extend for DuckDB-specific syntax

**Alternatives rejected:**
- vitess/sqlparser: MySQL dialect, would need significant modification
- Custom parser: More control but excessive development effort
- pingcap/tidb/parser: MySQL dialect, not PostgreSQL-compatible

**Full DuckDB SQL Syntax Support (from parser source analysis):**

**SELECT clause extensions:**
- `DISTINCT ON (col1, col2)` - PostgreSQL-style distinct
- `QUALIFY` clause - filter window function results
- `SAMPLE n% (SYSTEM|BERNOULLI|RESERVOIR) [SEED value]` - table sampling
- `LIMIT n%` - LIMIT with PERCENT modifier
- `EXCLUDE`, `REPLACE`, `RENAME` in column selection
- `COLUMNS(regex)` / `COLUMNS(*)` - dynamic column selection with pattern matching
- `* EXCLUDE (col1, col2)` - select all except specific columns
- `* REPLACE (expr AS col)` - select all with column replacement

**GROUP BY extensions:**
- `GROUP BY ALL` - auto-detect grouping columns (AggregateHandling)
- `GROUPING SETS ((a), (b), (a, b))` - explicit grouping sets
- `ROLLUP (a, b, c)` - hierarchical grouping
- `CUBE (a, b)` - all combinations grouping

**Set operations:**
- `UNION [ALL|DISTINCT]`, `INTERSECT [ALL|DISTINCT]`, `EXCEPT [ALL|DISTINCT]`
- `UNION BY NAME` - structural union matching by column name

**JOIN types:**
- `INNER`, `LEFT`, `RIGHT`, `FULL OUTER`, `CROSS` (standard)
- `NATURAL JOIN` - auto-match columns by name
- `SEMI JOIN`, `ANTI JOIN` - existence testing
- `ASOF JOIN` - temporal/time-series join with inequality
- `POSITIONAL JOIN` - join by row position

**Window functions:**
- Full `OVER (PARTITION BY ... ORDER BY ... frame_spec)` support
- Frame types: `ROWS`, `RANGE`, `GROUPS`
- Frame bounds: `UNBOUNDED PRECEDING/FOLLOWING`, `n PRECEDING/FOLLOWING`, `CURRENT ROW`
- Frame exclusion: `EXCLUDE CURRENT ROW`, `EXCLUDE GROUP`, `EXCLUDE TIES`, `EXCLUDE NO OTHERS`
- `FILTER (WHERE ...)` clause for window functions
- `IGNORE NULLS` / `RESPECT NULLS` for FIRST_VALUE, LAST_VALUE, NTH_VALUE, LEAD, LAG
- Ranking functions: `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `NTILE(n)`, `PERCENT_RANK()`, `CUME_DIST()`
- Value functions: `FIRST_VALUE()`, `LAST_VALUE()`, `NTH_VALUE(expr, n)`, `LEAD()`, `LAG()`
- Special: `WINDOW_FILL()` (fill NULLs with previous non-NULL)

**Common Table Expressions:**
- `WITH cte AS (...)` - basic CTEs
- `WITH RECURSIVE cte AS (anchor UNION ALL recursive)` - recursive CTEs

**PIVOT/UNPIVOT:**
- `PIVOT table ON pivot_cols USING agg FOR pivot_value IN (values)`
- `UNPIVOT table ON value_cols INTO NAME name_col VALUE value_col`

**DML extensions:**
- `INSERT INTO ... VALUES ... ON CONFLICT ...` - UPSERT
- `INSERT INTO ... RETURNING *` - return inserted rows
- `UPDATE ... SET ... RETURNING *` - return updated rows
- `DELETE FROM ... RETURNING *` - return deleted rows
- `MERGE INTO target USING source ON condition WHEN MATCHED/NOT MATCHED THEN ...`

**DDL statements:**
- `CREATE TABLE`, `CREATE TABLE AS SELECT (CTAS)`
- `CREATE VIEW [OR REPLACE]`
- `CREATE TYPE type_name AS (field1 type1, ...)`
- `CREATE FUNCTION` / `CREATE MACRO`
- `CREATE SEQUENCE`, `CREATE INDEX`
- `ALTER TABLE ADD/DROP/RENAME/ALTER COLUMN`
- `DROP TABLE/VIEW/TYPE/FUNCTION [CASCADE|RESTRICT]`
- Column constraints: `PRIMARY KEY`, `FOREIGN KEY ... REFERENCES`, `UNIQUE`, `CHECK`, `NOT NULL`, `DEFAULT`
- `GENERATED ALWAYS/BY DEFAULT AS IDENTITY`

**Database operations:**
- `ATTACH 'database.db' AS alias`
- `DETACH DATABASE alias`
- `EXPORT DATABASE [name] TO 'path' [OPTIONS ...]`
- `CHECKPOINT`
- `VACUUM`

**COPY statement (full syntax):**
- `COPY table TO 'file' (FORMAT type, HEADER bool, DELIMITER char, QUOTE char, ...)`
- `COPY table FROM 'file' (FORMAT type, ...)`
- Formats: CSV, PARQUET, JSON

**Prepared statements:**
- `PREPARE stmt_name AS query`
- `EXECUTE stmt_name [USING params]`
- `DEALLOCATE stmt_name`

**Utility statements:**
- `EXPLAIN [ANALYZE] query`
- `PRAGMA pragma_name = value`
- `SET variable = value` / `SET variable TO value` / `RESET variable`
- `LOAD 'extension_name'`
- `INSTALL 'extension_name'`
- `CALL procedure_name(args)`

**Expressions:**
- Arithmetic: `+`, `-`, `*`, `/`, `%`, `//` (integer div), `**` (power)
- Comparison: `=`, `<>`, `!=`, `<`, `>`, `<=`, `>=`, `BETWEEN ... AND ...`
- Logical: `AND`, `OR`, `NOT`
- Pattern matching: `LIKE`, `ILIKE`, `SIMILAR TO`, `GLOB`, `~~`, `!~~`
- NULL handling: `IS NULL`, `IS NOT NULL`, `COALESCE`, `NULLIF`, `IFNULL`
- `CASE WHEN ... THEN ... ELSE ... END`
- `IN (...)`, `NOT IN (...)`, `IN (SELECT ...)`
- `EXISTS (SELECT ...)`, `ANY (SELECT ...)`, `ALL (SELECT ...)`
- `COLLATE collation_name`
- Cast: `CAST(expr AS type)`, `expr::type`, `type 'literal'`

**Subqueries:**
- Scalar subqueries in SELECT/WHERE
- `IN (SELECT ...)` / `NOT IN (SELECT ...)`
- `EXISTS (SELECT ...)`
- `ANY (SELECT ...)` / `ALL (SELECT ...)`
- `VALUES ((1, 2), (3, 4))` as table constructor

**Literal syntax:**
- List literals: `[1, 2, 3]` or `list_value(1, 2, 3)`
- Struct literals: `{a: 1, b: 2}` or `struct_pack(a := 1, b := 2)`
- Map literals: `map([keys], [values])`
- Lambda expressions: `x -> x + 1` for higher-order functions

**Type system:**
- Integers: `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `HUGEINT`, `UHUGEINT`
- Unsigned: `UTINYINT`, `USMALLINT`, `UINTEGER`, `UBIGINT`
- Floating: `FLOAT`, `DOUBLE`, `DECIMAL(width, scale)`
- Temporal: `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ`, `INTERVAL`
- Text: `VARCHAR`, `TEXT`, `BLOB`
- Complex: `STRUCT`, `LIST`, `MAP`, `UNION`, `ENUM`, `UUID`, `JSON`
- Boolean: `BOOLEAN`

**Time travel (AT clause):**
- `SELECT * FROM table AT (TIMESTAMP => ts_value)`
- `SELECT * FROM table AT (TRANSACTION => txn_id)`

**Table functions:**
- `read_parquet('file.parquet')`
- `read_csv('file.csv', columns=...)`
- `read_json('file.json')`
- `generate_series(start, stop, step)`
- `unnest(array)`, `range(n)`
- `OFFSET AS offset_col` - row numbering in FROM clause

### Decision 2: Columnar Storage Model (DuckDB-style)

**What:** Store data in column-major format using DuckDB's Vector/DataChunk model.

```go
// ValidityMask tracks NULL values efficiently using bit manipulation
// Matches DuckDB's validity_mask.hpp pattern
type ValidityMask struct {
    mask   []uint64  // Bit array where 1 = valid, 0 = NULL
    count  int       // Number of bits
}

func (v *ValidityMask) IsValid(idx int) bool {
    entry := idx / 64
    bit := uint64(1) << (idx % 64)
    return v.mask[entry]&bit != 0
}

func (v *ValidityMask) SetInvalid(idx int) {
    entry := idx / 64
    bit := uint64(1) << (idx % 64)
    v.mask[entry] &^= bit
}

// SelectionVector enables operating on subsets of rows without copying
// Matches DuckDB's selection_vector.hpp pattern
type SelectionVector struct {
    indices []uint32  // Indices into the data array
    count   int       // Number of selected rows
}

func NewFlatSelectionVector(count int) *SelectionVector {
    indices := make([]uint32, count)
    for i := 0; i < count; i++ {
        indices[i] = uint32(i)
    }
    return &SelectionVector{indices: indices, count: count}
}

// Vector represents a single column of data (matches DuckDB's Vector)
type Vector struct {
    typ      Type
    data     any              // []int64, []float64, []string, etc.
    validity *ValidityMask    // NULL tracking
    count    int              // Number of rows
}

// DataChunk is a collection of vectors (matches DuckDB's DataChunk)
type DataChunk struct {
    vectors   []*Vector
    count     int              // Number of rows in this chunk
    capacity  int              // Maximum rows per chunk (STANDARD_VECTOR_SIZE = 2048)
    selection *SelectionVector // Optional selection vector for filtered operations
}

const STANDARD_VECTOR_SIZE = 2048

func NewDataChunk(types []Type, capacity int) *DataChunk {
    if capacity == 0 {
        capacity = STANDARD_VECTOR_SIZE
    }
    vectors := make([]*Vector, len(types))
    for i, t := range types {
        vectors[i] = NewVector(t, capacity)
    }
    return &DataChunk{
        vectors:  vectors,
        count:    0,
        capacity: capacity,
    }
}

// Table is a collection of chunks organized by row groups
type Table struct {
    schema    *TableDef
    rowGroups []*RowGroup
}

type RowGroup struct {
    chunks   []*DataChunk
    rowCount int
}
```

**Why:** This model matches DuckDB's architecture:
- ValidityMask for efficient NULL tracking (bit manipulation vs bool array)
- SelectionVector enables filter pushdown without copying data
- DataChunk as the unit of vectorized processing
- STANDARD_VECTOR_SIZE = 2048 matches DuckDB

### Decision 3: Execution Model (DuckDB-style Pipeline Execution)

**What:** Push-based pipeline execution with Global/Local state separation.

```go
// OperatorState separates global (shared) from local (per-thread) state
// Matches DuckDB's operator state pattern

// GlobalOperatorState holds state shared across all threads
type GlobalOperatorState interface {
    // Global state for the operator (e.g., hash table for joins)
}

// LocalOperatorState holds thread-local state
type LocalOperatorState interface {
    // Per-thread state (e.g., partial aggregates)
}

// Source operators produce data (e.g., table scans)
type Source interface {
    // GetData fills chunk with data, returns true if more data available
    GetData(ctx *ExecutionContext, chunk *DataChunk, state LocalOperatorState) (bool, error)

    // GetGlobalState returns the global state for this source
    GetGlobalState() GlobalOperatorState

    // GetLocalState returns a new local state for a thread
    GetLocalState(gstate GlobalOperatorState) LocalOperatorState
}

// Sink operators consume data (e.g., result output, hash table build)
type Sink interface {
    // Sink consumes a chunk of data
    Sink(ctx *ExecutionContext, chunk *DataChunk, state LocalOperatorState) error

    // Combine merges local state into global state
    Combine(ctx *ExecutionContext, gstate GlobalOperatorState, lstate LocalOperatorState) error

    // Finalize completes the sink operation
    Finalize(ctx *ExecutionContext, gstate GlobalOperatorState) error

    GetGlobalState() GlobalOperatorState
    GetLocalState(gstate GlobalOperatorState) LocalOperatorState
}

// Operator transforms data in the pipeline (filter, project, etc.)
type Operator interface {
    // Execute processes input chunk and produces output chunk
    Execute(ctx *ExecutionContext, input *DataChunk, output *DataChunk, state LocalOperatorState) error

    GetGlobalState() GlobalOperatorState
    GetLocalState(gstate GlobalOperatorState) LocalOperatorState
}

// Pipeline represents a chain of operators from source to sink
type Pipeline struct {
    source    Source
    operators []Operator
    sink      Sink
}

// ExecutionContext provides query-level context
type ExecutionContext struct {
    ctx       context.Context
    txn       *Transaction
    allocator *ChunkAllocator
}

// ChunkAllocator manages DataChunk and Vector memory for reuse
type ChunkAllocator struct {
    chunkPool  sync.Pool
    vectorPool sync.Pool
}

func (a *ChunkAllocator) Allocate(types []Type) *DataChunk {
    if v := a.chunkPool.Get(); v != nil {
        chunk := v.(*DataChunk)
        chunk.Reset()
        return chunk
    }
    return NewDataChunk(types, STANDARD_VECTOR_SIZE)
}

func (a *ChunkAllocator) Release(chunk *DataChunk) {
    a.chunkPool.Put(chunk)
}

// AllocateVector gets a reusable Vector for intermediate expression evaluation
func (a *ChunkAllocator) AllocateVector(typ Type) *Vector {
    if v := a.vectorPool.Get(); v != nil {
        vec := v.(*Vector)
        vec.Reset(typ)
        return vec
    }
    return NewVector(typ, STANDARD_VECTOR_SIZE)
}

// ReleaseVector returns a Vector to the pool for reuse
func (a *ChunkAllocator) ReleaseVector(vec *Vector) {
    a.vectorPool.Put(vec)
}
```

**Pipeline Execution Flow:**
1. Source produces DataChunks
2. Operators transform chunks in sequence
3. Sink consumes final chunks
4. Pipelines can be parallelized with thread-local state

**Operators to implement:**
- `TableScan` (Source) - Scans table chunks
- `Filter` (Operator) - Applies WHERE predicates with SelectionVector
- `Project` (Operator) - SELECT column projection
- `HashJoinBuild` (Sink) - Builds hash table for join
- `HashJoinProbe` (Source/Operator) - Probes hash table
- `HashAggregate` (Sink/Source) - GROUP BY aggregation
- `Sort` (Sink/Source) - ORDER BY sorting
- `Limit` (Operator) - LIMIT/OFFSET
- `Result` (Sink) - Final result output

### Decision 4: Catalog Structure

**What:** In-memory catalog with schema/table/column hierarchy.

```go
type Catalog struct {
    mu       sync.RWMutex
    schemas  map[string]*Schema
    default_ string  // Default schema name
}

type Schema struct {
    Name   string
    Tables map[string]*TableDef
}

type TableDef struct {
    Name    string
    Columns []ColumnDef
    Indexes []IndexDef
}

type ColumnDef struct {
    Name     string
    Type     Type
    Nullable bool
    Default  Expr  // Default value expression
}
```

### Decision 5: Query Planning

**What:** Two-phase planning: logical plan → physical plan.

**Logical Plan Nodes:**
```go
type LogicalPlan interface {
    Children() []LogicalPlan
    Schema() *Schema
}

// Examples:
type LogicalScan struct { Table string }
type LogicalFilter struct { Child LogicalPlan; Predicate Expr }
type LogicalProject struct { Child LogicalPlan; Exprs []Expr }
type LogicalJoin struct { Left, Right LogicalPlan; Condition Expr; JoinType JoinType }
type LogicalAggregate struct { Child LogicalPlan; GroupBy []Expr; Aggregates []AggExpr }
```

**Physical Plan Nodes:**
```go
type PhysicalPlan interface {
    ToOperator() Operator
    Children() []PhysicalPlan
}

// Physical plans include implementation details:
// - HashJoin vs NestedLoopJoin
// - IndexScan vs SeqScan
// - HashAggregate vs SortAggregate
```

### Decision 6: Expression Evaluation (DuckDB-style Vectorized)

**What:** Vectorized expression evaluation with SelectionVector support.

```go
// ExpressionState holds evaluation state (matches DuckDB pattern)
type ExpressionState struct {
    // Intermediate results for expression evaluation
    intermediate *Vector
}

type Expr interface {
    // Evaluate evaluates the expression on a chunk
    // Uses selection vector to only process selected rows
    Evaluate(chunk *DataChunk, sel *SelectionVector, count int, result *Vector) error

    // ReturnType returns the result type
    ReturnType() Type

    // GetExpressionState creates state for this expression
    GetExpressionState() *ExpressionState
}

// Example expressions with vectorized evaluation:

type ColumnRef struct {
    index int  // Column index in chunk
}

func (c *ColumnRef) Evaluate(chunk *DataChunk, sel *SelectionVector, count int, result *Vector) error {
    // Copy from source column using selection vector
    srcVec := chunk.vectors[c.index]
    if sel == nil {
        // Flat copy
        copyVector(srcVec, result, count)
    } else {
        // Selective copy using indices
        for i := 0; i < count; i++ {
            copyValue(srcVec, int(sel.indices[i]), result, i)
        }
    }
    return nil
}

type Literal struct {
    value any
    typ   Type
}

func (l *Literal) Evaluate(chunk *DataChunk, sel *SelectionVector, count int, result *Vector) error {
    // Fill result with constant value
    fillConstant(result, l.value, count)
    return nil
}

type BinaryOp struct {
    left, right Expr
    op          OpType
    leftVec     *Vector  // Intermediate storage
    rightVec    *Vector
}

func (b *BinaryOp) Evaluate(chunk *DataChunk, sel *SelectionVector, count int, result *Vector) error {
    // Evaluate operands
    if err := b.left.Evaluate(chunk, sel, count, b.leftVec); err != nil {
        return err
    }
    if err := b.right.Evaluate(chunk, sel, count, b.rightVec); err != nil {
        return err
    }
    // Apply operator vectorized
    return evalBinaryOp(b.op, b.leftVec, b.rightVec, result, count)
}

// evalBinaryOp applies operation vectorized with type specialization
func evalBinaryOp(op OpType, left, right, result *Vector, count int) error {
    // Type-specialized evaluation for performance
    switch left.typ {
    case TYPE_INTEGER:
        return evalBinaryOpInt32(op, left, right, result, count)
    case TYPE_BIGINT:
        return evalBinaryOpInt64(op, left, right, result, count)
    case TYPE_DOUBLE:
        return evalBinaryOpFloat64(op, left, right, result, count)
    // ... other types
    }
    return fmt.Errorf("unsupported type for binary op: %v", left.typ)
}

type FunctionCall struct {
    name string
    args []Expr
    fn   ScalarFunction  // Resolved function
}
```

**Vectorized Function Interface:**
```go
// ScalarFunction is a vectorized function implementation
type ScalarFunction func(args []*Vector, result *Vector, count int) error

// Example: vectorized addition for int64
func addInt64(args []*Vector, result *Vector, count int) error {
    left := args[0].data.([]int64)
    right := args[1].data.([]int64)
    out := result.data.([]int64)
    for i := 0; i < count; i++ {
        out[i] = left[i] + right[i]
    }
    return nil
}
```

### Decision 7: Filter Operator with SelectionVector

**What:** Filter operator uses SelectionVector to avoid data copying.

```go
// FilterOperator applies predicates without copying data
type FilterOperator struct {
    predicate Expr
}

type FilterLocalState struct {
    // Temporary selection vector for filter results
    sel *SelectionVector
}

func (f *FilterOperator) Execute(ctx *ExecutionContext, input *DataChunk, output *DataChunk, state LocalOperatorState) error {
    lstate := state.(*FilterLocalState)

    // Evaluate predicate to boolean vector
    result := ctx.allocator.AllocateVector(TYPE_BOOLEAN)
    defer ctx.allocator.ReleaseVector(result)

    if err := f.predicate.Evaluate(input, input.selection, input.count, result); err != nil {
        return err
    }

    // Build selection vector from true values
    bools := result.data.([]bool)
    count := 0
    for i := 0; i < input.count; i++ {
        idx := i
        if input.selection != nil {
            idx = int(input.selection.indices[i])
        }
        if result.validity.IsValid(i) && bools[i] {
            lstate.sel.indices[count] = uint32(idx)
            count++
        }
    }
    lstate.sel.count = count

    // Reference input vectors with new selection (zero-copy)
    output.vectors = input.vectors
    output.selection = lstate.sel
    output.count = count

    return nil
}
```

**Why:** SelectionVector-based filtering:
- Avoids copying data for intermediate results
- Enables predicate pushdown optimization
- Matches DuckDB's filter implementation

### Decision 8: HashAggregate with Global/Local State

**What:** HashAggregate operator uses separate global and local state for parallelization.

```go
// HashAggregateGlobalState holds the final aggregated hash table
type HashAggregateGlobalState struct {
    mu        sync.Mutex
    ht        *AggregateHashTable  // Final merged hash table
    groupBy   []int                // Group by column indices
    aggExprs  []AggregateExpr      // Aggregate expressions
    finalized bool
}

// HashAggregateLocalState holds thread-local partial aggregates
type HashAggregateLocalState struct {
    ht *AggregateHashTable  // Thread-local partial aggregates
}

// AggregateHashTable maps group keys to aggregate states
type AggregateHashTable struct {
    entries map[uint64]*AggregateEntry  // hash -> entry
    keys    *DataChunk                   // Stored group by keys
}

type AggregateEntry struct {
    keyOffset int           // Offset into keys chunk
    states    []AggState    // Aggregate states for this group
}

// AggState is the state for a single aggregate function
type AggState interface {
    Initialize()
    Update(input *Vector, count int, sel *SelectionVector)
    Combine(other AggState)  // For parallel merge
    Finalize(result *Vector, offset int)
}

// HashAggregateSink implements Sink for building aggregates
type HashAggregateSink struct {
    groupByCols []int
    aggExprs    []AggregateExpr
}

func (h *HashAggregateSink) Sink(ctx *ExecutionContext, chunk *DataChunk, state LocalOperatorState) error {
    lstate := state.(*HashAggregateLocalState)

    // For each row in chunk
    for i := 0; i < chunk.count; i++ {
        idx := i
        if chunk.selection != nil {
            idx = int(chunk.selection.indices[i])
        }

        // Compute hash of group by keys
        hash := computeGroupHash(chunk, h.groupByCols, idx)

        // Find or create entry
        entry := lstate.ht.FindOrCreate(hash, chunk, h.groupByCols, idx)

        // Update aggregates for this group
        for j, agg := range entry.states {
            agg.Update(chunk.vectors[h.aggExprs[j].InputCol], 1, nil)
        }
    }
    return nil
}

func (h *HashAggregateSink) Combine(ctx *ExecutionContext, gstate GlobalOperatorState, lstate LocalOperatorState) error {
    gs := gstate.(*HashAggregateGlobalState)
    ls := lstate.(*HashAggregateLocalState)

    gs.mu.Lock()
    defer gs.mu.Unlock()

    // Merge local hash table into global
    for hash, entry := range ls.ht.entries {
        if gentry, exists := gs.ht.entries[hash]; exists {
            // Combine aggregate states
            for i := range entry.states {
                gentry.states[i].Combine(entry.states[i])
            }
        } else {
            // Copy entry to global
            gs.ht.entries[hash] = entry
        }
    }
    return nil
}

// HashAggregateSource implements Source for reading aggregated results
type HashAggregateSource struct {
    gstate *HashAggregateGlobalState
}

type HashAggregateSourceLocalState struct {
    position int  // Current position in hash table iteration
}

func (h *HashAggregateSource) GetData(ctx *ExecutionContext, chunk *DataChunk, state LocalOperatorState) (bool, error) {
    lstate := state.(*HashAggregateSourceLocalState)
    gs := h.gstate

    // Build output chunk from hash table entries
    outputIdx := 0
    entries := gs.ht.GetEntries()

    for lstate.position < len(entries) && outputIdx < STANDARD_VECTOR_SIZE {
        entry := entries[lstate.position]

        // Copy group by keys to output
        for i, colIdx := range gs.groupBy {
            copyValue(gs.ht.keys.vectors[i], entry.keyOffset, chunk.vectors[colIdx], outputIdx)
        }

        // Finalize aggregates and copy to output
        aggOutputStart := len(gs.groupBy)
        for i, aggState := range entry.states {
            aggState.Finalize(chunk.vectors[aggOutputStart+i], outputIdx)
        }

        outputIdx++
        lstate.position++
    }

    chunk.count = outputIdx
    hasMore := lstate.position < len(entries)
    return hasMore, nil
}
```

**Why:**
- Global/Local separation enables parallel aggregation
- Thread-local hash tables avoid lock contention during build
- Combine phase merges partial results
- Matches DuckDB's parallel aggregate execution

### Decision 9: Transaction Model (DuckDB-compatible)

**What:** Single-writer MVCC with snapshot isolation, matching DuckDB exactly.

```go
type Transaction struct {
    ID          uint64
    StartTS     uint64      // Snapshot timestamp for reads
    CommitTS    uint64      // Commit timestamp (0 if uncommitted)
    State       TxnState    // Active, Committed, Aborted
    WriteSet    map[TableID]*WriteBuffer  // Buffered writes
    ReadSet     []ReadEntry // For conflict detection
}

type TxnState int
const (
    TxnActive TxnState = iota
    TxnCommitted
    TxnAborted
)

type TransactionManager struct {
    mu          sync.RWMutex
    nextTxnID   uint64
    nextTS      uint64
    activeTxns  map[uint64]*Transaction
    writeLock   sync.Mutex  // Only one writer at a time
}

// Begin starts a new transaction with snapshot isolation
func (tm *TransactionManager) Begin(readOnly bool) (*Transaction, error) {
    tm.mu.Lock()
    defer tm.mu.Unlock()

    txn := &Transaction{
        ID:       tm.nextTxnID,
        StartTS:  tm.nextTS,
        State:    TxnActive,
        WriteSet: make(map[TableID]*WriteBuffer),
    }
    tm.nextTxnID++

    if !readOnly {
        // Acquire write lock for write transactions
        tm.writeLock.Lock()
    }

    tm.activeTxns[txn.ID] = txn
    return txn, nil
}
```

**Isolation guarantees (matching DuckDB):**
- Snapshot isolation: reads see consistent snapshot at transaction start
- Single-writer: only one write transaction active at a time
- Serializable writes: write conflicts are prevented by write lock
- Read transactions never block

### Decision 10: Supported SQL Subset (Phase 1 - Core, Phase 2 - Extended)

**Phase 1 - Core SQL (MVP):**

**DDL:**
- `CREATE TABLE`, `DROP TABLE [CASCADE|RESTRICT]`
- `CREATE INDEX`, `DROP INDEX`
- `ALTER TABLE ADD/DROP COLUMN`
- Column constraints: `PRIMARY KEY`, `NOT NULL`, `DEFAULT`, `UNIQUE`

**DML:**
- `SELECT` with `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`
- `INSERT INTO ... VALUES`, `INSERT INTO ... SELECT`
- `UPDATE ... SET ... WHERE`
- `DELETE FROM ... WHERE`

**SELECT extensions:**
- `DISTINCT`, `DISTINCT ON`
- `GROUP BY`, `HAVING`
- `ORDER BY ... ASC|DESC NULLS FIRST|LAST`

**Expressions:**
- Arithmetic: `+`, `-`, `*`, `/`, `%`
- Comparison: `=`, `<>`, `<`, `>`, `<=`, `>=`, `BETWEEN`
- Logical: `AND`, `OR`, `NOT`
- NULL handling: `IS NULL`, `IS NOT NULL`, `COALESCE`, `NULLIF`
- Pattern: `LIKE`, `ILIKE`
- `CASE WHEN ... THEN ... ELSE ... END`
- `IN (...)`, `EXISTS (...)`
- Type casts: `CAST(x AS type)`, `x::type`

**Aggregates:**
- `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- `COUNT(DISTINCT ...)`

**Joins:**
- `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL OUTER JOIN`, `CROSS JOIN`

**Set operations:**
- `UNION [ALL]`, `INTERSECT`, `EXCEPT`

**CTEs:**
- `WITH cte AS (...) SELECT ...`

---

**Phase 2 - Extended SQL:**

**Advanced SELECT:**
- `QUALIFY` clause
- `SAMPLE n% (SYSTEM|BERNOULLI|RESERVOIR)`
- `LIMIT n%`
- Window functions with `OVER (PARTITION BY ... ORDER BY ... frame)`

**Advanced GROUP BY:**
- `GROUP BY ALL`
- `GROUPING SETS`, `ROLLUP`, `CUBE`

**Advanced Joins:**
- `NATURAL JOIN`
- `SEMI JOIN`, `ANTI JOIN`
- `ASOF JOIN`

**DML extensions:**
- `INSERT ... ON CONFLICT ...` (UPSERT)
- `RETURNING` clause
- `MERGE INTO`

**DDL extensions:**
- `CREATE TABLE AS SELECT`
- `CREATE VIEW`
- `CREATE TYPE`
- `GENERATED ... AS IDENTITY`
- `FOREIGN KEY ... REFERENCES`
- `CHECK` constraints

**Utility:**
- `EXPLAIN [ANALYZE]`
- `COPY TO/FROM`
- `ATTACH/DETACH`
- `PIVOT/UNPIVOT`
- `WITH RECURSIVE`

**Literal syntax:**
- List: `[1, 2, 3]`
- Struct: `{a: 1, b: 2}`
- Map: `map([k], [v])`
- Lambda: `x -> x + 1`

### Decision 11: Backend Interface Implementation

**What:** The Engine implements the Backend interface from project-foundation.

```go
type Engine struct {
    catalog  *Catalog
    storage  *Storage
    txnMgr   *TransactionManager
}

func (e *Engine) Open(path string, config *Config) (BackendConn, error)
func (e *Engine) Close() error

type EngineConn struct {
    engine *Engine
    txn    *Transaction
}

func (c *EngineConn) Execute(ctx context.Context, query string, args []driver.NamedValue) (int64, error)
func (c *EngineConn) Query(ctx context.Context, query string, args []driver.NamedValue) ([]map[string]any, []string, error)
func (c *EngineConn) Prepare(ctx context.Context, query string) (BackendStmt, error)
func (c *EngineConn) Close() error
func (c *EngineConn) Ping(ctx context.Context) error
```

### Decision 12: DuckDB File Format Compatibility

**What:** Full read/write compatibility with DuckDB's native file format.

**File Layout (byte-level specification from DuckDB src/storage/):**
```
Offset 0-7:     Block header (8 bytes, DEFAULT_BLOCK_HEADER_SIZE)
Offset 8-11:    Magic bytes "DUCK" (4 bytes, at MAGIC_BYTE_OFFSET=8)
Offset 12-19:   Version number (uint64)
Offset 20-51:   Flags[4] (4 × uint64 = 32 bytes)
Offset 52-83:   Library version string (32 bytes, MAX_VERSION_SIZE)
Offset 84-115:  Source ID string (32 bytes, MAX_VERSION_SIZE)
Offset 116-123: Encryption metadata (8 bytes, ENCRYPTION_METADATA_LEN)
Offset 124-139: Database identifier (16 bytes, DB_IDENTIFIER_LEN)
Offset 140-147: Encrypted canary (8 bytes, CANARY_BYTE_SIZE) - "DUCKKEY" encrypted
Offset 148+:    Padding to FILE_HEADER_SIZE (4096 bytes)

DatabaseHeader #1 at offset 4096 (FILE_HEADER_SIZE)
DatabaseHeader #2 at offset 8192 (FILE_HEADER_SIZE × 2)
Data blocks start at offset 12288 (FILE_HEADER_SIZE × 3)
```

**Storage Constants (from storage_info.hpp):**
```go
const (
    // Block sizes
    DEFAULT_BLOCK_ALLOC_SIZE       = 262144  // 256 KB per block
    FILE_HEADER_SIZE               = 4096    // 4 KB file header
    DEFAULT_BLOCK_HEADER_SIZE      = 8       // 8 bytes per block header
    DEFAULT_ENCRYPTION_BLOCK_HEADER = 40     // 40 bytes for encrypted blocks

    // Magic bytes
    MAGIC_BYTE_OFFSET = 8  // Magic starts after block header
    MAGIC_BYTES       = "DUCK"  // 4 bytes, NOT 8

    // Header field sizes
    FLAG_COUNT            = 4   // 4 uint64 flags
    MAX_VERSION_SIZE      = 32  // Library version string
    ENCRYPTION_METADATA_LEN = 8
    DB_IDENTIFIER_LEN     = 16
    CANARY_BYTE_SIZE      = 8   // Encrypted "DUCKKEY"
    AES_NONCE_LEN         = 16
    AES_IV_LEN            = 16
    AES_TAG_LEN           = 16

    // Metadata
    METADATA_BLOCK_COUNT  = 64  // 64 metadata blocks per storage block
    DEFAULT_ROW_GROUP_SIZE = 122880  // Rows per row group, NOT bytes
)
```

**Main Header Structure:**
```go
type MainHeader struct {
    // Offset 8: Magic bytes (4 bytes)
    Magic [4]byte  // "DUCK"

    // Offset 12: Storage format version
    VersionNumber uint64

    // Offset 20: Feature flags (32 bytes total)
    // flags[0] bit 0: ENCRYPTED_DATABASE_FLAG
    Flags [4]uint64

    // Offset 52: DuckDB library version (32 bytes)
    LibraryVersion [32]byte

    // Offset 84: Source ID for build identification (32 bytes)
    SourceID [32]byte

    // Offset 116: Encryption metadata (8 bytes)
    EncryptionMetadata [8]byte

    // Offset 124: Unique database identifier (16 bytes)
    DatabaseID [16]byte

    // Offset 140: Encrypted canary for key validation (8 bytes)
    // Stores encrypted "DUCKKEY" - wrong key detection
    EncryptedCanary [8]byte
}
```

**Database Header Structure (dual-header checkpoint):**
```go
// Two DatabaseHeaders exist at offsets 4096 and 8192
// Active header determined by highest iteration count
type DatabaseHeader struct {
    // Checkpoint iteration count (used for WAL recovery)
    Iteration uint64

    // Pointer to metadata block
    MetaBlock MetaBlockPointer

    // Pointer to free block list
    FreeList MetaBlockPointer

    // Total number of blocks in database
    BlockCount uint64

    // Block allocation size (typically 262144)
    BlockAllocSize uint64

    // Vector size for this database
    VectorSize uint64

    // Serialization compatibility version
    SerializationCompatibility uint64
}
```

**Block Pointer Structures:**
```go
// BlockPointer: 12 bytes (8 + 4 with padding)
type BlockPointer struct {
    BlockID uint64  // Block identifier
    Offset  uint32  // Offset within block
    // 4 bytes padding
}

// MetaBlockPointer: 12 bytes
type MetaBlockPointer struct {
    BlockPointer uint64  // 56-bit block_id + 8-bit index
    Offset       uint32  // Offset within metadata block
    // 4 bytes padding
}
```

**Checksum Algorithm (from checksum.cpp - EXACT MATCH REQUIRED):**
```go
// DuckDB checksum algorithm - MUST match exactly for file compatibility
// Source: duckdb/src/common/checksum.cpp

const (
    // Multiplier used in single-value checksum
    checksumMultiplier = 0xbf58476d1ce4e5b9

    // Robin-hood hashing constants for remainder bytes
    rhSeed       = 0xe17a1465
    rhMultiplier = 0xc6a4a7935bd1e995
    rhR          = 47
)

// checksumUint64 computes checksum for a single uint64 value
// (checksum.cpp lines 7-9)
func checksumUint64(x uint64) uint64 {
    return x * checksumMultiplier
}

// ChecksumRemainder handles remaining bytes using robin-hood hashing
// (checksum.cpp lines 14-65) - CRITICAL: exact algorithm required
func ChecksumRemainder(data []byte) uint64 {
    size := len(data)
    h := uint64(rhSeed) ^ (uint64(size) * rhMultiplier)

    // Process 8-byte blocks (should be <= 7 bytes for remainder)
    for len(data) >= 8 {
        k := binary.LittleEndian.Uint64(data)
        k *= rhMultiplier
        k ^= k >> rhR
        k *= rhMultiplier
        h ^= k
        h *= rhMultiplier
        data = data[8:]
    }

    // Process remaining bytes
    switch len(data) {
    case 7:
        h ^= uint64(data[6]) << 48
        fallthrough
    case 6:
        h ^= uint64(data[5]) << 40
        fallthrough
    case 5:
        h ^= uint64(data[4]) << 32
        fallthrough
    case 4:
        h ^= uint64(data[3]) << 24
        fallthrough
    case 3:
        h ^= uint64(data[2]) << 16
        fallthrough
    case 2:
        h ^= uint64(data[1]) << 8
        fallthrough
    case 1:
        h ^= uint64(data[0])
        h *= rhMultiplier
    }

    // Final mixing
    h ^= h >> rhR
    h *= rhMultiplier
    h ^= h >> rhR
    return h
}

// Checksum computes the checksum for a buffer
// (checksum.cpp lines 67-80) - CRITICAL: initial seed is 5381, NOT 0
func Checksum(buffer []byte) uint64 {
    result := uint64(5381)  // IMPORTANT: Initial seed is 5381

    // Process uint64 chunks with recursive checksum call
    for len(buffer) >= 8 {
        val := binary.LittleEndian.Uint64(buffer)
        result ^= checksumUint64(val)
        buffer = buffer[8:]
    }

    // Process remainder bytes
    if len(buffer) > 0 {
        result ^= ChecksumRemainder(buffer)
    }

    return result
}
```

**Checksum Storage Location:**
- MainHeader checksum is stored at the END of the 4KB file header block
- DatabaseHeader checksums are stored at the end of each 4KB header block
- Checksum covers bytes from block start to checksum field (exclusive)
- For main header with skip_block_header=true: checksum includes block header prefix

**ChecksumAndWrite operation (single_file_block_manager.cpp line 1095):**
```go
// When writing headers, checksum is computed and written atomically
func ChecksumAndWrite(buffer []byte, offset int64, skipBlockHeader bool) {
    delta := 0
    if skipBlockHeader {
        delta = DEFAULT_BLOCK_HEADER_SIZE  // 8 bytes
    }
    // Checksum includes block header when skipBlockHeader is true
    checksum := Checksum(buffer[0 : len(buffer)-8+delta])
    binary.LittleEndian.PutUint64(buffer[len(buffer)-8:], checksum)
    // Write to file at offset
}
```

**Encryption Support:**
```go
// When ENCRYPTED_DATABASE_FLAG is set in Flags[0]:
type EncryptionInfo struct {
    // AES-256-GCM encryption
    Nonce [16]byte  // AES nonce
    IV    [16]byte  // Initialization vector
    Tag   [16]byte  // Authentication tag

    // Canary for early wrong-key detection
    // Encrypted version of "DUCKKEY"
    Canary [8]byte
}
```

**Compatibility target:**
- Files created by dukdb-go can be opened by DuckDB CLI
- Files created by DuckDB can be opened by dukdb-go
- Support encrypted databases (AES-256-GCM)
- Support dual-header atomic checkpoints

**Implementation approach:**
- Implement MainHeader and DatabaseHeader readers/writers
- Support dual-header checkpoint: read both, use highest iteration
- Implement XOR+MurmurHash checksum algorithm exactly
- Support encryption detection and decryption (optional write)
- Test with DuckDB CLI for validation

### Decision 13: Built-in Format Support

**What:** Build common data formats directly into the engine (no extensions).

**Supported formats:**
- **Parquet:** Apache Parquet files (via pure Go library)
- **JSON:** JSON and NDJSON files
- **CSV:** CSV/TSV files with type inference

```go
// Format registration
type FormatReader interface {
    Open(path string, options map[string]any) (TableScan, error)
    InferSchema(path string) (*Schema, error)
}

type FormatWriter interface {
    Create(path string, schema *Schema, options map[string]any) (TableWriter, error)
}

// Built-in formats
var formats = map[string]Format{
    "parquet": &ParquetFormat{},
    "json":    &JSONFormat{},
    "csv":     &CSVFormat{},
}
```

**SQL syntax:**
```sql
-- Read from Parquet
SELECT * FROM read_parquet('data.parquet');
SELECT * FROM 'data.parquet';

-- Write to formats
COPY table TO 'out.parquet' (FORMAT PARQUET);
COPY table TO 'out.csv' (FORMAT CSV, HEADER true);
```

**Parquet implementation:**
- Use `github.com/parquet-go/parquet-go` (pure Go)
- Support predicate pushdown for efficient scanning
- Handle nested types (structs, lists, maps)
- Compression codecs: SNAPPY, GZIP, ZSTD, LZ4, BROTLI (all major codecs)

### Decision 14: Implementation Decisions (User Choices)

**Parser approach:** Fork & Modify `github.com/auxten/postgresql-parser`
- Create forked copy in `internal/parser`
- Modify directly to add DuckDB-specific syntax
- No external parser dependency at runtime

**File format strategy:** Full read/write from start
- Implement file format compatibility before in-memory engine
- Ensures DuckDB file compatibility from day one

**Testing approach:** TDD - Tests First
- Write spec-based tests before implementation
- Ensures completeness and correctness

**Phase 2 SQL:** Incremental from start
- Plan Phase 2 features from the beginning
- Add as parser allows, not deferred

**Error handling:** Match error types only
- Same ErrorType (e.g., ErrorTypeParser)
- Message wording can differ from DuckDB

**Memory management:** Pure in-memory only
- All `:memory:` data stays in RAM
- No spill-to-disk for simplicity

**Concurrency model:** Match DuckDB behavior
- Single-writer MVCC as designed
- Multi-process file locking like DuckDB

**BLOB encoding:** Accept both `\x` and `0x` prefixes on input
- Output uses `\x` to match DuckDB

**Module structure:**
```
github.com/connerohnesorge/dukdb-go/
├── internal/
│   ├── parser/      # Forked postgresql-parser
│   ├── catalog/     # Schema/table management
│   ├── executor/    # Query execution pipeline
│   ├── storage/     # File format I/O
│   ├── types/       # Internal type definitions
│   └── planner/     # Query planning
├── *.go             # Public API (driver, types, errors)
└── go.mod
```

**CLI:** No CLI initially
- Focus on library quality
- Use tests and Go code for validation

**Parameter markers:** PostgreSQL-style `$1, $2, ...`
- Matches DuckDB primary syntax

### Decision 15: External Dependencies

**Required pure Go libraries:**
```go
// go.mod dependencies
require (
    github.com/parquet-go/parquet-go v0.x.x    // Parquet format
    github.com/google/uuid v1.x.x               // UUID parsing
    github.com/mitchellh/mapstructure v1.x.x    // Composite[T] scanning
    github.com/coder/quartz v0.1.x              // Deterministic time testing
)
```

**Parser:** Vendored fork of `github.com/auxten/postgresql-parser`
- Copied to `internal/parser`
- Modified for DuckDB syntax

**Compression libraries (for Parquet):**
- SNAPPY: `github.com/golang/snappy`
- GZIP: stdlib `compress/gzip`
- ZSTD: `github.com/klauspost/compress/zstd`
- LZ4: `github.com/pierrec/lz4/v4`
- BROTLI: `github.com/google/brotli/go/cbrotli` or pure Go alternative

### Decision 16: Type Inference for CSV/JSON

**What:** Match DuckDB's type inference behavior.

**CSV type inference (matching DuckDB):**
1. Sample first 1000 rows by default (configurable via `sample_size`)
2. Try parsing in order: INTEGER → BIGINT → DOUBLE → TIMESTAMP → DATE → BOOLEAN → VARCHAR
3. If any value fails type, try next type
4. Final fallback is VARCHAR
5. NULL detection: empty string or "NULL" (configurable)

**JSON type inference:**
- JSON types map directly: number → DOUBLE, string → VARCHAR, boolean → BOOLEAN, null → NULL
- Arrays → LIST, objects → STRUCT
- No sampling needed - JSON is self-describing

```go
// Type inference order (matching DuckDB's auto_detect)
var typeInferenceOrder = []Type{
    TYPE_INTEGER,
    TYPE_BIGINT,
    TYPE_DOUBLE,
    TYPE_TIMESTAMP,
    TYPE_DATE,
    TYPE_BOOLEAN,
    TYPE_VARCHAR,  // Final fallback
}
```

### Decision 17: Deterministic Simulation Testing with Quartz

**What:** All time-dependent code uses `github.com/coder/quartz` Clock interface from day one. Zero flaky tests policy.

**Why:** Database drivers have many time-dependent operations (timeouts, timestamps, intervals, WAL, checkpoints). Traditional time mocking leads to flaky tests. Quartz provides deterministic control.

**Clock Interface Injection (using Option pattern):**
```go
// Option configures Engine
type Option func(*Engine)

// WithClock injects a clock for testing
func WithClock(clk quartz.Clock) Option {
    return func(e *Engine) {
        e.clock = clk
    }
}

// Engine accepts clock for all time operations
type Engine struct {
    catalog  *Catalog
    storage  *Storage
    txnMgr   *TransactionManager
    clock    quartz.Clock  // Injected clock for all time operations
}

// NewEngine creates engine with real clock (production)
// Optional WithClock() for testing
func NewEngine(opts ...Option) *Engine {
    e := &Engine{
        clock: quartz.NewReal(),  // Default: real clock
        // ...
    }
    for _, o := range opts {
        o(e)
    }
    return e
}

// Test usage:
// mClock := quartz.NewMock(t)
// engine := NewEngine(WithClock(mClock))
```

**Time-Dependent Operations (all use injected clock):**

1. **Query Timeouts:**
```go
func (c *EngineConn) Query(ctx context.Context, query string, args []driver.NamedValue) ([]map[string]any, []string, error) {
    // Use clock for timeout checking
    deadline, hasDeadline := ctx.Deadline()
    if hasDeadline {
        remaining := c.engine.clock.Until(deadline)
        if remaining <= 0 {
            return nil, nil, context.DeadlineExceeded
        }
    }
    // ...
}
```

2. **Transaction Timestamps:**
```go
func (tm *TransactionManager) Begin(readOnly bool) (*Transaction, error) {
    txn := &Transaction{
        ID:        tm.nextTxnID,
        StartTime: tm.clock.Now(),  // Use clock, not time.Now()
        // ...
    }
    // ...
}
```

3. **TIMESTAMP Values:**
```go
// current_timestamp() function uses injected clock
func builtinCurrentTimestamp(ctx *ExecutionContext) time.Time {
    return ctx.clock.Now()
}
```

4. **WAL/Checkpoint Timing:**
```go
type WALManager struct {
    clock           quartz.Clock
    lastCheckpoint  time.Time
    checkpointTimer quartz.Timer
}

func (w *WALManager) Start(ctx context.Context) {
    // Use TickerFunc for deterministic testing
    w.clock.TickerFunc(ctx, checkpointInterval, func() error {
        return w.maybeCheckpoint()
    })
}
```

5. **Connection Pool Idle Timeout:**
```go
type ConnPool struct {
    clock       quartz.Clock
    idleTimeout time.Duration
}

func (p *ConnPool) cleanupIdle(ctx context.Context) {
    p.clock.TickerFunc(ctx, cleanupInterval, func() error {
        now := p.clock.Now()
        for conn := range p.idle {
            if now.Sub(conn.lastUsed) > p.idleTimeout {
                conn.Close()
            }
        }
        return nil
    })
}
```

**Testing Patterns:**

1. **Basic Time Advancement:**
```go
func TestQueryTimeout(t *testing.T) {
    mClock := quartz.NewMock(t)
    engine := NewEngineWithClock(mClock)

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    // Start a slow query
    trap := mClock.Trap().Now()
    go engine.Query(ctx, "SELECT * FROM large_table", nil)

    // Advance past timeout
    c := trap.Wait(ctx)
    mClock.Advance(6 * time.Second)
    c.Release()

    // Assert timeout error
}
```

2. **Transaction Timestamp Testing:**
```go
func TestTransactionTimestamp(t *testing.T) {
    mClock := quartz.NewMock(t)
    engine := NewEngineWithClock(mClock)

    // Set clock to specific time
    specificTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
    mClock.Set(specificTime)

    txn, _ := engine.txnMgr.Begin(false)

    // Transaction start time is deterministic
    assert.Equal(t, specificTime, txn.StartTime)
}
```

3. **Checkpoint Timing:**
```go
func TestWALCheckpoint(t *testing.T) {
    mClock := quartz.NewMock(t)
    wal := NewWALManagerWithClock(mClock)

    ctx := context.Background()
    trap := mClock.Trap().TickerFunc()

    go wal.Start(ctx)

    // Wait for ticker to be created
    call := trap.Wait(ctx)
    call.Release()
    trap.Close()

    // Advance to trigger checkpoint
    w := mClock.Advance(checkpointInterval)
    w.MustWait(ctx)

    // Assert checkpoint occurred
    assert.True(t, wal.checkpointCalled)
}
```

4. **CURRENT_TIMESTAMP Determinism:**
```go
func TestCurrentTimestamp(t *testing.T) {
    mClock := quartz.NewMock(t)
    engine := NewEngineWithClock(mClock)

    fixedTime := time.Date(2024, 6, 15, 14, 30, 45, 123456000, time.UTC)
    mClock.Set(fixedTime)

    result, _ := engine.Query(ctx, "SELECT current_timestamp", nil)

    // Result is deterministic
    assert.Equal(t, fixedTime, result[0]["current_timestamp"])
}
```

**Zero Flaky Tests Policy:**

1. **No `time.Sleep()` in tests** - Use `mClock.Advance()` instead
2. **No `runtime.Gosched()`** - Use traps for synchronization
3. **No polling/Eventually** - Use `Advance().MustWait()` for determinism
4. **No real timeouts in unit tests** - All timeouts through mock clock
5. **Tag clock calls** for precise trap matching in complex scenarios

**Tagging Convention (for trap matching):**
```go
// Tag format: "Component", "Method", ["phase"]
// This makes tests robust against code changes

func (tm *TransactionManager) Begin(readOnly bool) (*Transaction, error) {
    txn := &Transaction{
        ID:        tm.nextTxnID,
        StartTime: tm.clock.Now("TransactionManager", "Begin"),
        // ...
    }
    // ...
}

func (c *EngineConn) Query(ctx context.Context, query string, args []driver.NamedValue) {
    start := c.engine.clock.Now("EngineConn", "Query", "start")
    // ... execute query ...
    end := c.engine.clock.Now("EngineConn", "Query", "end")
    // duration = end.Sub(start)
}

func (w *WALManager) maybeCheckpoint() error {
    now := w.clock.Now("WALManager", "maybeCheckpoint")
    elapsed := now.Sub(w.lastCheckpoint)
    if elapsed < checkpointInterval {
        next := w.clock.Until(w.lastCheckpoint.Add(checkpointInterval), "WALManager", "maybeCheckpoint", "until")
        // ...
    }
    // ...
}
```

**Trap Matching with Tags:**
```go
func TestWALCheckpointTiming(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    mClock := quartz.NewMock(t)

    // Only trap the "until" call, not the "Now" call
    trap := mClock.Trap().Until("WALManager", "maybeCheckpoint", "until")
    defer trap.Close()

    wal := NewWALManager(WithClock(mClock))
    go wal.Start(ctx)

    // Wait for the until call to be trapped
    call := trap.MustWait(ctx)

    // Advance clock WHILE the call is trapped
    mClock.Advance(3 * time.Millisecond)

    // Now release - the Until() will use the advanced time
    call.MustRelease(ctx)

    // Verify behavior with edge case timing
}
```

**Advance Restriction (important Quartz behavior):**
```go
// Quartz only allows advancing to the NEXT timer/ticker event
// This ensures deterministic behavior

// WRONG - will fail if timer is at 1 second:
mClock.AfterFunc(time.Second, func() { ... })
mClock.Advance(2*time.Second)  // ERROR: advances past event

// CORRECT - advance exactly to event:
mClock.AfterFunc(time.Second, func() { ... })
mClock.Advance(time.Second).MustWait(ctx)

// For multiple events, loop:
for i := 0; i < 10; i++ {
    mClock.Advance(time.Second).MustWait(ctx)
}

// Or use AdvanceNext() when duration unknown:
d, w := mClock.AdvanceNext()
w.MustWait(ctx)
// d contains actual duration advanced
```

**MustWait Pattern (standard idiom):**
```go
// Always wait for triggered events to complete
mClock.Advance(5*time.Second).MustWait(ctx)

// MustWait = Wait + fail test on error
// Equivalent to:
w := mClock.Advance(5*time.Second)
err := w.Wait(ctx)
if err != nil {
    t.Fatal("timer/ticker never completed")
}
```

**Clock Propagation:**
```go
// ExecutionContext carries clock for all operations
type ExecutionContext struct {
    ctx       context.Context
    txn       *Transaction
    allocator *ChunkAllocator
    clock     quartz.Clock  // Propagated to all operators
}

// Operators receive clock via context
func (s *SeqScanOperator) GetData(ctx *ExecutionContext, chunk *DataChunk, state LocalOperatorState) (bool, error) {
    // Any time-dependent operation uses ctx.clock
    if deadline, ok := ctx.ctx.Deadline(); ok {
        if ctx.clock.Until(deadline) <= 0 {
            return false, context.DeadlineExceeded
        }
    }
    // ...
}
```

**Integration Test Time Control:**
```go
func TestComplexWorkflow(t *testing.T) {
    mClock := quartz.NewMock(t)
    engine := NewEngineWithClock(mClock)

    // Create table, insert data
    engine.Execute(ctx, "CREATE TABLE t (id INT, ts TIMESTAMP)", nil)

    // Insert with deterministic timestamp
    mClock.Set(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
    engine.Execute(ctx, "INSERT INTO t VALUES (1, current_timestamp)", nil)

    // Advance time
    mClock.Advance(24 * time.Hour)
    engine.Execute(ctx, "INSERT INTO t VALUES (2, current_timestamp)", nil)

    // Query with deterministic results
    result, _ := engine.Query(ctx, "SELECT * FROM t ORDER BY ts", nil)

    // Assertions are fully deterministic
    assert.Equal(t, "2024-01-01 00:00:00", result[0]["ts"])
    assert.Equal(t, "2024-01-02 00:00:00", result[1]["ts"])
}
```

## Risks / Trade-offs

- **Risk:** Incomplete SQL coverage
  - Mitigation: Start with core subset, expand based on usage

- **Risk:** Performance vs C++ DuckDB
  - Mitigation: Focus on correctness first; optimize hot paths

- **Risk:** DuckDB file format complexity
  - Mitigation: Start with reading, implement writing incrementally

- **Trade-off:** Development time
  - Mitigation: Leverage existing Go libraries (parser, parquet, etc.)

- **Trade-off:** Maintenance burden
  - Mitigation: Clean architecture, comprehensive tests
