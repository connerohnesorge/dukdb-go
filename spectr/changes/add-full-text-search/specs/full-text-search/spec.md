# Full-Text Search - Delta Spec

## ADDED Requirements

### Requirement: FTS Index Creation via PRAGMA

The system MUST support creating full-text search indexes on table columns using PRAGMA syntax compatible with DuckDB's FTS extension.

#### Scenario: Create FTS index on single text column

- WHEN user executes `PRAGMA create_fts_index('documents', 'id', 'content')`
- THEN an inverted index is built from all rows in the `documents` table
- AND the `content` column is tokenized, filtered, and stemmed
- AND the index is registered in the catalog for the `documents` table

#### Scenario: Create FTS index on multiple text columns

- WHEN user executes `PRAGMA create_fts_index('articles', 'id', 'title', 'body')`
- THEN an inverted index is built covering both `title` and `body` columns
- AND terms from all specified columns are included in the index
- AND per-document term frequencies account for all indexed columns

#### Scenario: Create FTS index with stemmer option

- WHEN user executes `PRAGMA create_fts_index('docs', 'id', 'text', stemmer='porter')`
- THEN the Porter stemming algorithm is applied during tokenization
- AND indexed terms are stored in stemmed form

#### Scenario: Create FTS index with stop words option

- WHEN user executes `PRAGMA create_fts_index('docs', 'id', 'text', stopwords='english')`
- THEN English stop words are filtered out during tokenization
- AND common words like "the", "is", "and" are not indexed

#### Scenario: Error on nonexistent table

- WHEN user executes `PRAGMA create_fts_index('nonexistent', 'id', 'text')`
- THEN an error is returned indicating the table does not exist

#### Scenario: Error on nonexistent column

- WHEN user executes `PRAGMA create_fts_index('docs', 'id', 'nonexistent_col')`
- THEN an error is returned indicating the column does not exist

#### Scenario: Error on duplicate FTS index

- WHEN an FTS index already exists on the table
- AND user executes `PRAGMA create_fts_index('docs', 'id', 'text')` without `overwrite=1`
- THEN an error is returned indicating an FTS index already exists

#### Scenario: Overwrite existing FTS index

- WHEN an FTS index already exists on the table
- AND user executes `PRAGMA create_fts_index('docs', 'id', 'text', overwrite=1)`
- THEN the existing index is dropped and a new one is built

---

### Requirement: FTS Index Deletion via PRAGMA

The system MUST support dropping full-text search indexes using PRAGMA syntax.

#### Scenario: Drop existing FTS index

- WHEN user executes `PRAGMA drop_fts_index('documents')`
- THEN the FTS index for the `documents` table is removed from the catalog
- AND associated inverted index data is freed from memory

#### Scenario: Error on dropping nonexistent FTS index

- WHEN no FTS index exists on the table
- AND user executes `PRAGMA drop_fts_index('documents')`
- THEN an error is returned indicating no FTS index exists for the table

---

### Requirement: BM25 Search via match_bm25 Function

The system MUST provide a `match_bm25` function that returns BM25 relevance scores for documents matching a search query.

#### Scenario: Basic BM25 search query

- WHEN user executes `SELECT *, fts_main_documents.match_bm25(id, 'search terms') AS score FROM documents WHERE score IS NOT NULL ORDER BY score DESC`
- THEN each matching document receives a BM25 score based on term relevance
- AND documents not matching any query term receive NULL score
- AND results are ordered by descending relevance

#### Scenario: Multi-term query scoring

- WHEN user searches with multiple terms like `'database indexing'`
- THEN the BM25 score is the sum of individual term BM25 scores
- AND a document matching both terms scores higher than one matching only one

#### Scenario: NULL score for non-matching documents

- WHEN a document contains none of the query terms
- THEN match_bm25 returns NULL for that document
- AND the document is excluded by `WHERE score IS NOT NULL`

#### Scenario: Empty query returns NULL for all documents

- WHEN user searches with an empty string or only stop words
- THEN match_bm25 returns NULL for all documents
- AND no results are returned after WHERE filtering

#### Scenario: Error when no FTS index exists

- WHEN user calls `fts_main_table.match_bm25(id, 'query')` on a table without an FTS index
- THEN an error is returned indicating no FTS index exists for the table

---

### Requirement: Tokenization Pipeline

The system MUST implement a text tokenization pipeline that normalizes text for indexing and searching.

#### Scenario: Unicode lowercasing

- WHEN text "Hello WORLD" is tokenized
- THEN tokens are lowercased to "hello" and "world"

#### Scenario: Punctuation splitting

- WHEN text "hello, world! foo-bar" is tokenized
- THEN text is split into tokens: "hello", "world", "foo", "bar"

#### Scenario: Stop word filtering with English stop words

- WHEN text "the quick brown fox" is tokenized with English stop words
- THEN "the" is removed as a stop word
- AND "quick", "brown", "fox" are retained

#### Scenario: Porter stemming

- WHEN text "running cats agreed" is tokenized with Porter stemmer
- THEN "running" is stemmed to "run"
- AND "cats" is stemmed to "cat"
- AND "agreed" is stemmed to "agre"

#### Scenario: Empty and whitespace-only text

- WHEN empty string or whitespace-only text is tokenized
- THEN an empty token list is returned

#### Scenario: Same pipeline for indexing and querying

- WHEN a document is indexed and later searched
- THEN the same tokenization pipeline is applied to both document text and search query
- AND stemmed query terms match stemmed index terms

---

### Requirement: BM25 Scoring Correctness

The system MUST implement the BM25 scoring algorithm with correct mathematical formulation.

#### Scenario: IDF calculation

- GIVEN a corpus of 100 documents where 10 contain term "database"
- WHEN IDF is calculated for "database"
- THEN IDF = ln((100 - 10 + 0.5) / (10 + 0.5) + 1) which is approximately 2.25

#### Scenario: Term frequency saturation

- GIVEN two documents where document A has term frequency 1 and document B has term frequency 10
- WHEN BM25 scores are compared (with k1=1.2, same document length)
- THEN document B scores higher than document A
- AND the score difference is sub-linear (not 10x higher)

#### Scenario: Document length normalization

- GIVEN two documents with same term frequency but different lengths
- WHEN BM25 scores are compared (with b=0.75)
- THEN the shorter document scores higher
- AND length normalization follows the BM25 formula

#### Scenario: Default BM25 parameters

- WHEN no custom parameters are specified
- THEN k1 defaults to 1.2
- AND b defaults to 0.75

---

### Requirement: FTS Index Maintenance

The system MUST keep the FTS index consistent with the underlying table data as rows are inserted, updated, or deleted.

#### Scenario: New rows are indexed on INSERT

- GIVEN an FTS index exists on a table
- WHEN a new row is inserted into the table
- THEN the new row's text columns are tokenized and added to the inverted index
- AND the new row is immediately searchable via match_bm25

#### Scenario: Deleted rows are removed from index on DELETE

- GIVEN an FTS index exists on a table
- WHEN a row is deleted from the table
- THEN the row's terms are removed from the inverted index
- AND the deleted row no longer appears in match_bm25 results

#### Scenario: Updated rows are re-indexed on UPDATE

- GIVEN an FTS index exists on a table
- WHEN a row's indexed text column is updated
- THEN the old terms are removed from the inverted index
- AND the new text is tokenized and added to the inverted index
- AND search results reflect the updated content

#### Scenario: Document statistics are updated

- GIVEN an FTS index with document count and average document length
- WHEN rows are inserted or deleted
- THEN document count is updated
- AND average document length is recalculated
- AND BM25 scores reflect current corpus statistics

---

### Requirement: Thread-Safe FTS Operations

The system MUST support concurrent read access to FTS indexes and safe write access during index maintenance.

#### Scenario: Concurrent search queries

- GIVEN an FTS index on a table
- WHEN multiple goroutines execute match_bm25 queries concurrently
- THEN all queries return correct results
- AND no race conditions occur (verified with Go race detector)

#### Scenario: Search during index maintenance

- GIVEN an FTS index on a table
- WHEN a search query executes concurrently with an INSERT/DELETE
- THEN the search query completes without error
- AND returns results consistent with either before or after the modification

---

### Requirement: DuckDB FTS Syntax Compatibility

The system MUST accept the same FTS syntax as DuckDB's FTS extension for drop-in compatibility.

#### Scenario: PRAGMA create_fts_index syntax

- WHEN user executes `PRAGMA create_fts_index('table', 'id_col', 'col1', 'col2', stemmer='porter', stopwords='english')`
- THEN the system parses and executes the PRAGMA identically to DuckDB
- AND positional arguments identify the table, ID column, and text columns
- AND keyword arguments configure stemmer and stop words

#### Scenario: Schema-qualified match_bm25 syntax

- WHEN user executes `fts_main_tablename.match_bm25(id_col, 'query')`
- THEN the system resolves `fts_main_tablename` as the FTS index namespace for `tablename`
- AND executes the BM25 scoring function against that index

#### Scenario: PRAGMA drop_fts_index syntax

- WHEN user executes `PRAGMA drop_fts_index('table')`
- THEN the system drops the FTS index for the specified table
