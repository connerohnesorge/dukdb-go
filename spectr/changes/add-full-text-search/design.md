# Full-Text Search - Design Details

## Implementation Details

### Inverted Index Data Structure

The inverted index maps terms to posting lists. Each posting list contains document IDs and per-document term frequencies, enabling efficient BM25 scoring.

```go
// PostingEntry represents a single document's occurrence data for a term.
type PostingEntry struct {
    DocID     int64  // Row ID in the source table
    TermFreq  int    // Number of times the term appears in this document
    FieldMask uint64 // Bitmask of which indexed columns contain the term
}

// PostingList is the list of documents containing a specific term.
type PostingList struct {
    Term       string
    DocFreq    int             // Number of documents containing this term
    Entries    []PostingEntry  // Sorted by DocID for merge operations
}

// InvertedIndex is the core FTS data structure.
type InvertedIndex struct {
    TableName   string
    IDColumn    string              // Column used as document identifier
    TextColumns []string            // Columns included in the index
    Stemmer     string              // Stemmer name (e.g., "porter")
    StopWords   string              // Stop word list name (e.g., "english")

    // Term -> PostingList mapping
    Index       map[string]*PostingList

    // Document metadata for BM25 scoring
    DocCount    int64               // Total number of indexed documents
    AvgDocLen   float64             // Average document length (in terms)
    DocLengths  map[int64]int       // DocID -> document length (term count)

    mu          sync.RWMutex        // Concurrency control
}
```

**Storage Layout**:

The inverted index is stored in memory during active use and persisted to the catalog for durability. The index is rebuilt on database open if the source table has been modified since last index build.

```
FTS Index Storage:
  catalog entry: fts_index_{table_name}
    ├── metadata: table, id_col, text_cols, stemmer, stopwords
    ├── index: serialized map[string]*PostingList
    └── doc_metadata: doc_count, avg_doc_len, doc_lengths
```

### Tokenizer Pipeline

The tokenizer transforms raw text into normalized terms for indexing and querying. The pipeline is:

```
Raw Text
    │
    ▼
1. Unicode Lowercasing (strings.ToLower)
    │
    ▼
2. Punctuation Splitting (split on non-alphanumeric)
    │
    ▼
3. Stop Word Filtering (remove common words)
    │
    ▼
4. Stemming (Porter stemmer)
    │
    ▼
Normalized Terms
```

```go
// Tokenizer defines the text processing pipeline.
type Tokenizer struct {
    StopWords map[string]struct{}
    Stemmer   func(string) string
}

// Tokenize processes raw text into normalized terms.
func (t *Tokenizer) Tokenize(text string) []string {
    // Step 1: Lowercase
    text = strings.ToLower(text)

    // Step 2: Split on non-alphanumeric characters
    tokens := splitOnNonAlphanumeric(text)

    // Step 3 & 4: Filter stop words and stem
    result := make([]string, 0, len(tokens))
    for _, token := range tokens {
        if token == "" {
            continue
        }
        if _, isStop := t.StopWords[token]; isStop {
            continue
        }
        stemmed := t.Stemmer(token)
        if stemmed != "" {
            result = append(result, stemmed)
        }
    }
    return result
}

func splitOnNonAlphanumeric(s string) []string {
    return strings.FieldsFunc(s, func(r rune) bool {
        return !unicode.IsLetter(r) && !unicode.IsDigit(r)
    })
}
```

### Porter Stemmer

A pure Go implementation of the Porter stemming algorithm. The stemmer reduces inflected words to their root form (e.g., "running" -> "run", "cats" -> "cat").

The Porter stemmer has five steps, each applying a set of rules:

```go
// PorterStem applies the Porter stemming algorithm to a word.
func PorterStem(word string) string {
    if len(word) <= 2 {
        return word
    }
    word = step1a(word) // Plural forms: -sses, -ies, -s
    word = step1b(word) // Past tense: -eed, -ed, -ing
    word = step1c(word) // Y -> I when preceded by consonant
    word = step2(word)  // Derivational: -ational -> -ate, -tional -> -tion, etc.
    word = step3(word)  // Derivational: -icate -> -ic, -alize -> -al, etc.
    word = step4(word)  // Derivational: -al, -ance, -ence, -er, -ic, etc.
    word = step5(word)  // Tidying: remove trailing -e, reduce -ll
    return word
}
```

**Step 1a examples**:
- "caresses" -> "caress"
- "ponies" -> "poni"
- "cats" -> "cat"

**Step 1b examples**:
- "feed" -> "feed" (measure = 0, no change)
- "agreed" -> "agree"
- "plastered" -> "plaster"
- "hopping" -> "hop"

### Stop Words

English stop words are the most commonly occurring words that carry little semantic meaning:

```go
var EnglishStopWords = map[string]struct{}{
    "a": {}, "an": {}, "and": {}, "are": {}, "as": {}, "at": {},
    "be": {}, "but": {}, "by": {}, "for": {}, "if": {}, "in": {},
    "into": {}, "is": {}, "it": {}, "no": {}, "not": {}, "of": {},
    "on": {}, "or": {}, "such": {}, "that": {}, "the": {}, "their": {},
    "then": {}, "there": {}, "these": {}, "they": {}, "this": {},
    "to": {}, "was": {}, "will": {}, "with": {},
    // ... full list includes ~174 words matching DuckDB's English stop word list
}
```

### BM25 Scoring Formula

BM25 (Best Matching 25) is the standard relevance scoring function for information retrieval. The formula for a query Q containing terms q1, q2, ..., qn against document D:

```
score(D, Q) = SUM_i [ IDF(qi) * (f(qi, D) * (k1 + 1)) / (f(qi, D) + k1 * (1 - b + b * |D| / avgdl)) ]
```

Where:
- `f(qi, D)` = frequency of term qi in document D
- `|D|` = length of document D (in terms)
- `avgdl` = average document length across all documents
- `k1` = term frequency saturation parameter (default 1.2)
- `b` = document length normalization parameter (default 0.75)
- `IDF(qi)` = inverse document frequency of term qi

IDF calculation:

```
IDF(qi) = ln((N - n(qi) + 0.5) / (n(qi) + 0.5) + 1)
```

Where:
- `N` = total number of documents
- `n(qi)` = number of documents containing term qi

```go
// BM25Params holds the tunable parameters for BM25 scoring.
type BM25Params struct {
    K1 float64 // Term frequency saturation (default 1.2)
    B  float64 // Length normalization (default 0.75)
}

// DefaultBM25Params returns the standard BM25 parameters.
func DefaultBM25Params() BM25Params {
    return BM25Params{K1: 1.2, B: 0.75}
}

// ScoreBM25 computes the BM25 score for a document against a query.
func (idx *InvertedIndex) ScoreBM25(docID int64, queryTerms []string, params BM25Params) float64 {
    score := 0.0
    docLen := float64(idx.DocLengths[docID])

    for _, term := range queryTerms {
        posting, exists := idx.Index[term]
        if !exists {
            continue
        }

        // Find the entry for this document
        entry := findEntry(posting, docID)
        if entry == nil {
            continue
        }

        // IDF component
        n := float64(posting.DocFreq)
        N := float64(idx.DocCount)
        idf := math.Log((N-n+0.5)/(n+0.5) + 1.0)

        // TF component with saturation and length normalization
        tf := float64(entry.TermFreq)
        tfNorm := (tf * (params.K1 + 1.0)) /
            (tf + params.K1*(1.0-params.B+params.B*docLen/idx.AvgDocLen))

        score += idf * tfNorm
    }

    return score
}
```

### PRAGMA Handlers

FTS indexes are managed via PRAGMA statements, matching DuckDB's FTS extension API.

#### PRAGMA Registration Integration Points

FTS PRAGMAs must be registered in two places:

**1. Binder registration** (`internal/binder/bind_pragma.go`):

Add a new `PragmaTypeFTS` category and register both PRAGMA names in `pragmaCategories`:

```go
// In internal/binder/statements.go, add to PragmaType iota:
PragmaTypeFTS  // FTS index management

// In internal/binder/bind_pragma.go, add to pragmaCategories map:
"create_fts_index": PragmaTypeFTS,
"drop_fts_index":   PragmaTypeFTS,
```

**2. Executor case statements** (`internal/executor/physical_maintenance.go`):

Add cases to the `executePragma()` switch statement:

```go
case "create_fts_index":
    return e.pragmaCreateFTSIndex(ctx, plan)
case "drop_fts_index":
    return e.pragmaDropFTSIndex(ctx, plan)
```

These handler methods should be defined in a new file `internal/executor/pragma_fts.go`.

#### PRAGMA Keyword Arguments

The current PRAGMA parser (`internal/parser/parser_pragma.go`) only supports
positional arguments via `parseExprList()`. FTS PRAGMAs need keyword arguments
like `stemmer='porter'`.

**Solution**: Extend `PragmaStmt` and the parser to support keyword arguments:

```go
// In internal/parser/ast.go, extend PragmaStmt:
type PragmaStmt struct {
    Name   string            // Pragma name
    Args   []Expr            // Positional arguments
    KWArgs map[string]Expr   // Keyword arguments (e.g., stemmer='porter')
    Value  Expr              // For SET PRAGMA name = value
}
```

In `internal/parser/parser_pragma.go`, after parsing positional args inside the
parenthesized argument list, check for `identifier = expr` patterns:

```go
// Inside parsePragma, within the tokenLParen case, replace parseExprList() with:
args, kwargs, err := p.parsePragmaArgList()

// New method:
func (p *parser) parsePragmaArgList() ([]Expr, map[string]Expr, error) {
    var args []Expr
    kwargs := make(map[string]Expr)

    for p.current().typ != tokenRParen {
        if len(args) > 0 || len(kwargs) > 0 {
            if _, err := p.expect(tokenComma); err != nil {
                return nil, nil, err
            }
        }

        // Check for keyword argument: identifier = expr
        if p.current().typ == tokenIdent && p.peek().typ == tokenOperator && p.peek().value == "=" {
            key := p.advance().value   // consume identifier
            p.advance()                 // consume '='
            val, err := p.parseExpr()
            if err != nil {
                return nil, nil, err
            }
            kwargs[key] = val
            continue
        }

        // Positional argument
        expr, err := p.parseExpr()
        if err != nil {
            return nil, nil, err
        }
        args = append(args, expr)
    }
    return args, kwargs, nil
}
```

Similarly, `BoundPragmaStmt` in `internal/binder/statements.go` must carry the
bound keyword arguments:

```go
type BoundPragmaStmt struct {
    Name       string
    PragmaType PragmaType
    Args       []BoundExpr
    KWArgs     map[string]BoundExpr  // Bound keyword arguments
    Value      BoundExpr
}
```

And `bindPragma` in `internal/binder/bind_pragma.go` must bind them:

```go
if len(stmt.KWArgs) > 0 {
    bound.KWArgs = make(map[string]BoundExpr, len(stmt.KWArgs))
    for key, arg := range stmt.KWArgs {
        boundArg, err := b.bindExpr(arg, dukdb.TYPE_ANY)
        if err != nil {
            return nil, err
        }
        bound.KWArgs[key] = boundArg
    }
}
```

The `PhysicalPragma` planner node (`internal/planner/physical.go`) must also
propagate `KWArgs` so that executor handlers can access them.

**Create FTS Index**:

```sql
PRAGMA create_fts_index('table_name', 'id_col', 'text_col1', 'text_col2',
    stemmer='porter', stopwords='english', strip_accents=1, lower=1, overwrite=0);
```

```go
// HandleCreateFTSIndex processes the create_fts_index PRAGMA.
// Positional args: table_name, id_column, text_column(s)
// Keyword args: stemmer, stopwords, strip_accents, lower, overwrite
func HandleCreateFTSIndex(ctx *ExecutionContext, args []string, kwargs map[string]string) error {
    if len(args) < 3 {
        return fmt.Errorf("create_fts_index requires at least 3 arguments: table, id_column, text_column(s)")
    }

    tableName := args[0]
    idColumn := args[1]
    textColumns := args[2:]

    stemmer := kwargs["stemmer"]   // default: "porter"
    stopwords := kwargs["stopwords"] // default: "english"

    // 1. Validate table and columns exist
    table, err := ctx.Catalog.GetTable(tableName)
    if err != nil {
        return fmt.Errorf("table %q not found: %w", tableName, err)
    }

    // 2. Validate columns exist
    if err := validateColumns(table, idColumn, textColumns); err != nil {
        return err
    }

    // 3. Create inverted index
    index := NewInvertedIndex(tableName, idColumn, textColumns, stemmer, stopwords)

    // 4. Build index by scanning all rows
    if err := index.BuildFromTable(ctx, table); err != nil {
        return fmt.Errorf("failed to build FTS index: %w", err)
    }

    // 5. Register index in catalog
    ctx.Catalog.RegisterFTSIndex(tableName, index)

    return nil
}
```

**Drop FTS Index**:

```sql
PRAGMA drop_fts_index('table_name');
```

```go
func HandleDropFTSIndex(ctx *ExecutionContext, args []string) error {
    if len(args) < 1 {
        return fmt.Errorf("drop_fts_index requires table name argument")
    }
    tableName := args[0]
    return ctx.Catalog.DropFTSIndex(tableName)
}
```

### match_bm25 Function Implementation

The `match_bm25` function is exposed as a scalar function that returns the BM25 score for a given document ID against a search query.

**SQL Syntax**:

```sql
SELECT *, fts_main_table_name.match_bm25(id_col, 'search query') AS score
FROM table_name
WHERE score IS NOT NULL
ORDER BY score DESC;
```

#### Schema-Qualified Function Resolution

The current `FunctionCall` AST node has no `Schema` field, and the parser
(`internal/parser/parser.go` around line 4024) treats `name.ident` as a
`ColumnRef` -- it never checks whether the token after the dot is followed by
`(`, which would indicate a function call rather than a column reference.

**Parser fix** (`internal/parser/parser.go`, in the `tokenDot` branch around line 4024):

After consuming the dot and reading the next identifier, check if it is followed
by `(`. If so, parse it as a function call with a schema qualifier:

```go
// Column reference with table prefix -- OR schema-qualified function call
if p.current().typ == tokenDot {
    p.advance()
    if p.current().typ == tokenStar {
        p.advance()
        return &StarExpr{Table: name}, nil
    }
    if p.current().typ != tokenIdent {
        return nil, p.errorf("expected column name after dot")
    }
    col := p.advance().value

    // NEW: Check if this is a schema-qualified function call (e.g., fts_main_docs.match_bm25(...))
    if p.current().typ == tokenLParen {
        fnExpr, err := p.parseFunctionCall(col)
        if err != nil {
            return nil, err
        }
        // Attach the schema qualifier to the FunctionCall
        if fn, ok := fnExpr.(*FunctionCall); ok {
            fn.Schema = name
        }
        return fnExpr, nil
    }

    return &ColumnRef{Table: name, Column: col}, nil
}
```

**AST extension** (`internal/parser/ast.go`): Add a `Schema` field to `FunctionCall`:

```go
type FunctionCall struct {
    Schema   string          // Schema/namespace qualifier (e.g., "fts_main_docs")
    Name     string
    Args     []Expr
    Distinct bool
    Star     bool
    OrderBy  []OrderByExpr
}
```

**Binder resolution** (`internal/binder/bind_expr.go`): In `bindFunctionCall`,
when `f.Schema` is non-empty and has the `fts_main_` prefix, resolve it as an
FTS match function rather than looking up a regular function:

```go
func (b *Binder) bindFunctionCall(f *parser.FunctionCall) (BoundExpr, error) {
    // Handle schema-qualified FTS functions: fts_main_<table>.match_bm25(...)
    if f.Schema != "" && strings.HasPrefix(strings.ToLower(f.Schema), "fts_main_") {
        tableName := strings.TrimPrefix(strings.ToLower(f.Schema), "fts_main_")
        return b.bindFTSMatchFunction(tableName, f)
    }

    // ... existing function binding logic ...
}
```

This approach avoids inventing a general schema-qualified function dispatch
mechanism. The `fts_main_` prefix is treated as a special namespace convention
recognized only by the FTS subsystem, matching DuckDB's behavior.

```go
// MatchBM25Function implements the match_bm25 scalar function.
type MatchBM25Function struct {
    TableName string
    Index     *InvertedIndex
}

// Execute computes BM25 score for a given document ID and query string.
func (f *MatchBM25Function) Execute(idValue interface{}, queryStr string) (interface{}, error) {
    // 1. Tokenize the query using the same pipeline as indexing
    tokenizer := f.Index.GetTokenizer()
    queryTerms := tokenizer.Tokenize(queryStr)

    if len(queryTerms) == 0 {
        return nil, nil // No searchable terms -> NULL score
    }

    // 2. Resolve document ID
    docID, err := resolveDocID(idValue)
    if err != nil {
        return nil, err
    }

    // 3. Check if document contains any query terms
    if !f.Index.HasDocument(docID, queryTerms) {
        return nil, nil // Document doesn't match -> NULL score
    }

    // 4. Compute BM25 score
    score := f.Index.ScoreBM25(docID, queryTerms, DefaultBM25Params())

    return score, nil
}
```

### Index Maintenance

When rows are inserted, updated, or deleted from an indexed table, the FTS index must be updated accordingly.

#### DML Integration Hook Points

The executor already has an index maintenance pattern for B-tree/hash indexes
via `updateIndexesForInsert()` in `internal/executor/operator.go` (line ~1899).
FTS maintenance follows the same pattern -- calling into the catalog to find FTS
indexes for the affected table, then invoking `AddDocument`/`RemoveDocument`.

A single helper method on the Executor handles the dispatch:

```go
// internal/executor/fts_maintenance.go

// updateFTSIndexesForInsert updates FTS indexes after rows are inserted.
func (e *Executor) updateFTSIndexesForInsert(tableName string, tableDef *catalog.TableDef, rows [][]any, startRowID uint64) error {
    ftsIdx, ok := e.catalog.GetFTSIndex(tableName)
    if !ok {
        return nil // No FTS index on this table
    }
    idx := ftsIdx.Index.(*fts.InvertedIndex)
    for i, row := range rows {
        docID := int64(startRowID) + int64(i)
        fieldValues := rowToFieldMap(row, tableDef, ftsIdx.TextColumns)
        idx.AddDocument(docID, fieldValues)
    }
    return nil
}

// updateFTSIndexesForDelete updates FTS indexes after rows are deleted.
func (e *Executor) updateFTSIndexesForDelete(tableName string, deletedRowIDs []storage.RowID) error {
    ftsIdx, ok := e.catalog.GetFTSIndex(tableName)
    if !ok {
        return nil
    }
    idx := ftsIdx.Index.(*fts.InvertedIndex)
    for _, rowID := range deletedRowIDs {
        idx.RemoveDocument(int64(rowID))
    }
    return nil
}

// updateFTSIndexesForUpdate updates FTS indexes after rows are updated.
// UPDATE = remove old entry + add new entry.
func (e *Executor) updateFTSIndexesForUpdate(tableName string, tableDef *catalog.TableDef, updates []rowUpdateInfo) error {
    ftsIdx, ok := e.catalog.GetFTSIndex(tableName)
    if !ok {
        return nil
    }
    idx := ftsIdx.Index.(*fts.InvertedIndex)
    for _, u := range updates {
        idx.RemoveDocument(int64(u.RowID))
        fieldValues := rowToFieldMap(u.NewValues, tableDef, ftsIdx.TextColumns)
        idx.AddDocument(int64(u.RowID), fieldValues)
    }
    return nil
}
```

**INSERT hook** (`internal/executor/operator.go`): Call `updateFTSIndexesForInsert`
immediately after `updateIndexesForInsert` (line ~1901):

```go
// Update all indexes on this table with the newly inserted rows
if err := e.updateIndexesForInsert(plan.Table, plan.TableDef, allInsertedValues, startRowID); err != nil {
    return nil, fmt.Errorf("failed to update indexes: %w", err)
}
// Update FTS indexes
if err := e.updateFTSIndexesForInsert(plan.Table, plan.TableDef, allInsertedValues, startRowID); err != nil {
    return nil, fmt.Errorf("failed to update FTS indexes: %w", err)
}
```

**DELETE hook** (`internal/executor/physical_delete.go`): After collecting
`deletedRows` and marking tombstones, call `updateFTSIndexesForDelete` with the
deleted RowIDs. Place the call in the same location where `recordUndo` is called
for delete operations.

**UPDATE hook** (`internal/executor/physical_update.go`): After applying row
updates via `storage.UpdateRows()`, call `updateFTSIndexesForUpdate`. Note that
the existing `updates` slice already contains both the RowID and new values,
which is exactly what the FTS maintenance function needs.

```go
// AddDocument indexes a new document.
func (idx *InvertedIndex) AddDocument(docID int64, fieldValues map[string]string) {
    idx.mu.Lock()
    defer idx.mu.Unlock()

    totalTerms := 0
    for colIdx, col := range idx.TextColumns {
        text, ok := fieldValues[col]
        if !ok {
            continue
        }
        terms := idx.tokenizer.Tokenize(text)
        totalTerms += len(terms)

        // Count term frequencies
        termFreqs := make(map[string]int)
        for _, term := range terms {
            termFreqs[term]++
        }

        // Update posting lists
        for term, freq := range termFreqs {
            posting, exists := idx.Index[term]
            if !exists {
                posting = &PostingList{Term: term}
                idx.Index[term] = posting
            }
            posting.Entries = append(posting.Entries, PostingEntry{
                DocID:     docID,
                TermFreq:  freq,
                FieldMask: 1 << uint(colIdx),
            })
            posting.DocFreq++
        }
    }

    // Update document metadata
    idx.DocLengths[docID] = totalTerms
    idx.DocCount++
    idx.recalcAvgDocLen()
}

// RemoveDocument removes a document from the index.
func (idx *InvertedIndex) RemoveDocument(docID int64) {
    idx.mu.Lock()
    defer idx.mu.Unlock()

    for _, posting := range idx.Index {
        for i, entry := range posting.Entries {
            if entry.DocID == docID {
                posting.Entries = append(posting.Entries[:i], posting.Entries[i+1:]...)
                posting.DocFreq--
                break
            }
        }
        // Remove empty posting lists
        if posting.DocFreq == 0 {
            delete(idx.Index, posting.Term)
        }
    }

    delete(idx.DocLengths, docID)
    idx.DocCount--
    idx.recalcAvgDocLen()
}
```

## Context

**Architecture Alignment**:
- Builds on existing PRAGMA handling in `internal/executor/`
- Reuses catalog infrastructure for storing FTS index metadata
- Follows the same pattern as other index types in `internal/storage/`
- Tokenizer/stemmer are self-contained in `internal/fts/` with no external dependencies

**Key Integration Points**:
1. `internal/binder/bind_pragma.go` - Register `create_fts_index` and `drop_fts_index` in `pragmaCategories` map with `PragmaTypeFTS`
2. `internal/executor/physical_maintenance.go` - Add case statements in `executePragma()` switch for `create_fts_index` and `drop_fts_index`
3. `internal/catalog/catalog.go` - Add `ftsIndexes` field to `Schema` struct and `RegisterFTSIndex`/`DropFTSIndex`/`GetFTSIndex` methods
4. `internal/parser/parser.go` - Extend dot-notation parsing to support schema-qualified function calls
5. `internal/parser/ast.go` - Add `Schema` field to `FunctionCall`, add `KWArgs` field to `PragmaStmt`
6. `internal/binder/bind_expr.go` - Resolve `fts_main_<table>.match_bm25()` via schema prefix
7. `internal/executor/operator.go` - Add FTS index maintenance call after `updateIndexesForInsert` (line ~1899)
8. `internal/executor/physical_delete.go` - Add FTS index maintenance call after delete tombstone marking
9. `internal/executor/physical_update.go` - Add FTS index maintenance call after row update

### Catalog FTS Index Storage

The `Schema` struct in `internal/catalog/catalog.go` currently holds `tables`,
`views`, `indexes`, and `sequences` maps. FTS indexes must be added as a
separate map because they are a distinct object type from B-tree/hash indexes.

**Schema struct extension** (`internal/catalog/catalog.go`):

```go
type Schema struct {
    mu        sync.RWMutex
    name      string
    tables    map[string]*TableDef
    views     map[string]*ViewDef
    indexes   map[string]*IndexDef
    sequences map[string]*SequenceDef
    ftsIndexes map[string]*FTSIndexDef  // table_name -> FTS index definition
}
```

Update `NewSchema` to initialize the map:

```go
func NewSchema(name string) *Schema {
    return &Schema{
        name:       name,
        tables:     make(map[string]*TableDef),
        views:      make(map[string]*ViewDef),
        indexes:    make(map[string]*IndexDef),
        sequences:  make(map[string]*SequenceDef),
        ftsIndexes: make(map[string]*FTSIndexDef),
    }
}
```

Add an `FTSIndexDef` type (can live in `internal/catalog/fts_index.go`):

```go
// FTSIndexDef holds the catalog metadata for an FTS index.
// The actual InvertedIndex lives in internal/fts/ and is referenced here.
type FTSIndexDef struct {
    TableName   string
    IDColumn    string
    TextColumns []string
    Stemmer     string
    StopWords   string
    Index       interface{} // *fts.InvertedIndex, typed as interface to avoid import cycle
}
```

Add methods on `Schema`:

```go
func (s *Schema) RegisterFTSIndex(tableName string, def *FTSIndexDef) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.ftsIndexes[strings.ToLower(tableName)] = def
}

func (s *Schema) GetFTSIndex(tableName string) (*FTSIndexDef, bool) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    def, ok := s.ftsIndexes[strings.ToLower(tableName)]
    return def, ok
}

func (s *Schema) DropFTSIndex(tableName string) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    key := strings.ToLower(tableName)
    if _, ok := s.ftsIndexes[key]; !ok {
        return fmt.Errorf("no FTS index found for table %q", tableName)
    }
    delete(s.ftsIndexes, key)
    return nil
}
```

Add forwarding methods on `Catalog` that delegate to the appropriate schema
(defaulting to "main"), following the same pattern as `GetTable`/`GetIndex`.

## Goals / Non-Goals

**Goals**:
- DuckDB-compatible FTS syntax via PRAGMA create_fts_index / drop_fts_index
- BM25 scoring via match_bm25 function with correct relevance ranking
- Pure Go implementation with zero external dependencies
- Support for Porter stemming and English stop words
- Thread-safe index operations
- Index maintenance on INSERT/UPDATE/DELETE

**Non-Goals**:
- Persistent inverted index on disk (index is rebuilt on database open; persistence is future work)
- Phrase search or proximity queries (future work)
- Fuzzy matching or typo tolerance (future work)
- Custom analyzers or tokenizer plugins (future work)
- Multiple stemmer languages beyond Porter English (future work)
- Partitioned or distributed FTS indexes (single-machine only)
- Real-time incremental index updates during bulk COPY operations (index rebuilt after COPY)

## Decisions

**Decision 1: In-Memory Inverted Index**
- **Choice**: Store inverted index entirely in memory
- **Rationale**: Simpler implementation, fast query performance, matches dukdb-go's current storage model where most data is in memory
- **Alternative**: Disk-based inverted index with memory-mapped segments - more complex, needed only for very large corpora

**Decision 2: Porter Stemmer from Scratch**
- **Choice**: Implement Porter stemmer in pure Go within the project
- **Rationale**: No external dependency, well-documented algorithm, pure Go requirement
- **Alternative**: Use a third-party Go stemmer library - adds dependency, may not be compatible with TinyGo/WASM

**Decision 3: Schema-Qualified Function Syntax**
- **Choice**: Use `fts_main_table_name.match_bm25(id, query)` syntax
- **Rationale**: Matches DuckDB FTS extension API exactly, enables drop-in compatibility
- **Alternative**: Simple `match_bm25(table, id, query)` - simpler but breaks DuckDB compatibility

**Decision 4: Synchronous Index Maintenance**
- **Choice**: Update FTS index synchronously on INSERT/UPDATE/DELETE
- **Rationale**: Ensures index consistency, simpler than async with eventual consistency
- **Alternative**: Async background index updates - better write throughput but stale search results

## Risks / Trade-offs

**Risk 1: Memory Usage for Large Text Corpora**
- **Mitigation**: Document memory requirements; recommend index only needed columns
- **Trade-off**: Large text columns with millions of rows could consume significant memory

**Risk 2: Index Rebuild Cost on Database Open**
- **Mitigation**: Index rebuild is O(N * avg_doc_length); acceptable for moderate datasets
- **Trade-off**: Cold start time increases with corpus size; disk persistence is future work

**Risk 3: Write Performance Impact**
- **Mitigation**: Index updates are lightweight (hash map operations); synchronous is acceptable for OLAP workloads
- **Trade-off**: High-throughput INSERT workloads may see latency increase

**Risk 4: Porter Stemmer Edge Cases**
- **Mitigation**: Comprehensive test suite against known Porter stemmer test vectors
- **Trade-off**: Some edge cases may differ from DuckDB's Snowball stemmer implementation

## Migration Plan

This is a new capability with no breaking changes.

1. **Phase 1**: Implement tokenizer pipeline and Porter stemmer with tests
2. **Phase 2**: Implement inverted index data structure and BM25 scoring
3. **Phase 3**: Add PRAGMA handlers for create/drop FTS index
4. **Phase 4**: Implement match_bm25 function and integrate with query execution
5. **Phase 5**: Add index maintenance hooks for INSERT/UPDATE/DELETE
6. **Phase 6**: Integration tests and performance benchmarks

All phases are additive. No existing functionality is modified.

## Open Questions

1. **Multi-column weighting**: Should different text columns have configurable BM25 weights?
   - Current plan: All columns weighted equally; per-column weights as future enhancement

2. **Index persistence format**: When disk persistence is added, what serialization format?
   - Current plan: Defer to future work; consider Protocol Buffers or custom binary format

3. **Accent stripping**: DuckDB supports `strip_accents` option. Should we implement Unicode normalization?
   - Current plan: Implement basic accent stripping via Unicode NFKD decomposition

4. **Concurrent index builds**: Should index creation be parallelized for large tables?
   - Current plan: Single-threaded build initially; parallel build as optimization
