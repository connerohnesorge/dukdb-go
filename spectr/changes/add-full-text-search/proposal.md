# Change: Add Full-Text Search

## Why

dukdb-go currently has no full-text search capability. DuckDB v1.4.3 provides an FTS extension that allows users to create inverted indexes on text columns, then search them using BM25 scoring. Full-text search is a fundamental feature for applications that need to query natural language content efficiently. Without it, users must resort to slow LIKE/ILIKE pattern matching or external search systems, breaking the single-database experience that DuckDB provides.

Implementing FTS as a pure Go component aligns with dukdb-go's zero-CGO constraint and enables full-text search in WASM and TinyGo environments where external search libraries are unavailable.

## What Changes

- Add inverted index data structure in `internal/storage/` for mapping terms to document IDs with term frequency data
- Implement tokenizer pipeline: Unicode-aware lowercasing, punctuation splitting, stop word filtering, Porter stemming
- Add PRAGMA handlers for `create_fts_index` and `drop_fts_index`
- Implement `match_bm25` table macro function for BM25 relevance scoring
- Add FTS catalog entries to track which tables have full-text indexes
- Integrate FTS index maintenance with INSERT/UPDATE/DELETE operations on indexed tables

## Impact

- **Affected specs**:
  - `full-text-search` (new capability)

- **Affected code**:
  - `internal/storage/` - New inverted index data structure
  - `internal/catalog/` - FTS index catalog entries
  - `internal/executor/` - PRAGMA handlers for FTS index creation/deletion
  - `internal/executor/` - `match_bm25` function execution
  - `internal/parser/` - Parse FTS-specific PRAGMA arguments
  - `internal/engine/` - Register FTS PRAGMA handlers and table macros

- **New components**:
  - `internal/fts/` - Tokenizer, stemmer, stop words, inverted index
  - `internal/fts/stemmer/` - Porter stemmer implementation
  - `internal/fts/tokenizer/` - Text tokenization pipeline

- **Dependencies**:
  - None on external packages (pure Go implementation)
  - Builds on existing PRAGMA infrastructure, catalog, and storage layer
