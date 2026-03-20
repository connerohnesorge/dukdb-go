## 1. Tokenizer Pipeline

- [ ] 1.1 Create `internal/fts/` package with tokenizer interface and pipeline
- [ ] 1.2 Implement Unicode-aware lowercasing and punctuation splitting
- [ ] 1.3 Implement English stop word list (~174 words matching DuckDB)
- [ ] 1.4 Implement Porter stemmer algorithm (all 5 steps)
- [ ] 1.5 Write tokenizer unit tests covering edge cases (empty strings, Unicode, mixed case)
- [ ] 1.6 Write Porter stemmer tests against known test vectors

## 2. Inverted Index

- [ ] 2.1 Define PostingEntry, PostingList, and InvertedIndex data structures
- [ ] 2.2 Implement AddDocument to build posting lists from tokenized text
- [ ] 2.3 Implement RemoveDocument to remove a document from all posting lists
- [ ] 2.4 Implement BuildFromTable to scan a table and build the full index
- [ ] 2.5 Add thread-safe access with sync.RWMutex for concurrent reads
- [ ] 2.6 Write inverted index unit tests (add, remove, rebuild, concurrent access)

## 3. BM25 Scoring

- [ ] 3.1 Implement BM25 scoring function with configurable k1 and b parameters
- [ ] 3.2 Implement IDF calculation using document frequency statistics
- [ ] 3.3 Implement document length normalization
- [ ] 3.4 Write BM25 scoring tests with known expected scores
- [ ] 3.5 Test BM25 ranking correctness (more relevant documents score higher)

## 4. PRAGMA Handlers

- [ ] 4.1 Extend PragmaStmt AST with KWArgs field (`internal/parser/ast.go`)
- [ ] 4.2 Implement `parsePragmaArgList()` in `internal/parser/parser_pragma.go` to parse `key=value` keyword arguments
- [ ] 4.3 Add `PragmaTypeFTS` to PragmaType enum (`internal/binder/statements.go`)
- [ ] 4.4 Register `create_fts_index` and `drop_fts_index` in `pragmaCategories` map (`internal/binder/bind_pragma.go`)
- [ ] 4.5 Add `KWArgs` to `BoundPragmaStmt` and bind them in `bindPragma` (`internal/binder/bind_pragma.go`)
- [ ] 4.6 Propagate `KWArgs` through `PhysicalPragma` planner node (`internal/planner/physical.go`)
- [ ] 4.7 Add `case "create_fts_index"` and `case "drop_fts_index"` to `executePragma()` switch (`internal/executor/physical_maintenance.go`)
- [ ] 4.8 Implement `pragmaCreateFTSIndex` and `pragmaDropFTSIndex` handler methods (`internal/executor/pragma_fts.go`)
- [ ] 4.9 Write PRAGMA handler tests (valid args, keyword args, invalid args, missing table, etc.)

## 5. FTS Catalog Integration

- [ ] 5.1 Create `FTSIndexDef` type in `internal/catalog/fts_index.go`
- [ ] 5.2 Add `ftsIndexes map[string]*FTSIndexDef` field to `Schema` struct (`internal/catalog/catalog.go`)
- [ ] 5.3 Initialize `ftsIndexes` in `NewSchema()` (`internal/catalog/catalog.go`)
- [ ] 5.4 Add `RegisterFTSIndex`, `GetFTSIndex`, `DropFTSIndex` methods on `Schema`
- [ ] 5.5 Add forwarding methods on `Catalog` that delegate to the appropriate schema
- [ ] 5.6 Validate table and column existence during index creation
- [ ] 5.7 Prevent duplicate FTS index creation (unless overwrite=1)
- [ ] 5.8 Write catalog integration tests

## 6. match_bm25 Function

- [ ] 6.1 Add `Schema` field to `FunctionCall` AST node (`internal/parser/ast.go`)
- [ ] 6.2 Extend parser dot-notation handling to detect schema-qualified function calls (`internal/parser/parser.go`, ~line 4024): after consuming `name.ident`, check for `(` and parse as function call with schema
- [ ] 6.3 Add `bindFTSMatchFunction` in binder to resolve `fts_main_<table>.match_bm25()` via schema prefix check (`internal/binder/bind_expr.go`)
- [ ] 6.4 Implement match_bm25 as a scalar function returning BM25 score or NULL (`internal/fts/match_bm25.go`)
- [ ] 6.5 Register match_bm25 evaluation in executor expression evaluator
- [ ] 6.6 Handle NULL score for non-matching documents (WHERE score IS NOT NULL filtering)
- [ ] 6.7 Write match_bm25 unit tests with sample data

## 7. Index Maintenance

- [ ] 7.1 Create `internal/executor/fts_maintenance.go` with `updateFTSIndexesForInsert`, `updateFTSIndexesForDelete`, `updateFTSIndexesForUpdate` helper methods
- [ ] 7.2 Hook `updateFTSIndexesForInsert` into INSERT path in `internal/executor/operator.go` after `updateIndexesForInsert` (~line 1901)
- [ ] 7.3 Hook `updateFTSIndexesForDelete` into DELETE path in `internal/executor/physical_delete.go` after tombstone marking, near `recordUndo` call
- [ ] 7.4 Hook `updateFTSIndexesForUpdate` into UPDATE path in `internal/executor/physical_update.go` after `storage.UpdateRows()`, near `recordUndo` call
- [ ] 7.5 Write index maintenance tests (insert/update/delete reflect in search results)

## 8. Integration Testing

- [ ] 8.1 End-to-end test: create table, create FTS index, search with match_bm25
- [ ] 8.2 Test multi-column FTS index (search across multiple text columns)
- [ ] 8.3 Test BM25 result ordering (ORDER BY score DESC)
- [ ] 8.4 Test drop FTS index and verify search no longer works
- [ ] 8.5 Test index with empty table and table with NULL text values
- [ ] 8.6 Test with large dataset (10k+ documents) for correctness and performance
- [ ] 8.7 Test concurrent search queries with race detector enabled
- [ ] 8.8 Test DuckDB syntax compatibility (PRAGMA and match_bm25 syntax)
