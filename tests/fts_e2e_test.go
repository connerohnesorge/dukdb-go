package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFTSCreateAndSearch(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create table and insert data
	_, err = db.Exec(`CREATE TABLE docs(id INTEGER PRIMARY KEY, content VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO docs VALUES (1, 'The quick brown fox jumps over the lazy dog')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO docs VALUES (2, 'A quick brown cat sits on the mat')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO docs VALUES (3, 'The dog runs in the park')`)
	require.NoError(t, err)

	// Create FTS index
	_, err = db.Exec(`PRAGMA create_fts_index('docs', 'content')`)
	require.NoError(t, err)

	// Search for "quick brown"
	rows, err := db.Query(`SELECT * FROM fts_search('docs', 'quick brown')`)
	require.NoError(t, err)
	defer rows.Close()

	type result struct {
		rowid int64
		score float64
	}
	var results []result
	for rows.Next() {
		var r result
		err := rows.Scan(&r.rowid, &r.score)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	// Should find docs matching "quick brown" (docs 0 and 1, 0-indexed row IDs)
	require.NotEmpty(t, results, "should find matching documents")
	require.Len(t, results, 2, "should find exactly 2 matching documents")

	// Both results should have positive scores
	for _, r := range results {
		assert.Greater(t, r.score, 0.0, "score should be positive")
	}
}

func TestFTSDropIndex(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE docs(id INTEGER, content VARCHAR)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO docs VALUES (1, 'hello world')`)
	require.NoError(t, err)

	// Create FTS index
	_, err = db.Exec(`PRAGMA create_fts_index('docs', 'content')`)
	require.NoError(t, err)

	// Drop FTS index
	_, err = db.Exec(`PRAGMA drop_fts_index('docs')`)
	require.NoError(t, err)

	// Search should fail after dropping index
	_, err = db.Query(`SELECT * FROM fts_search('docs', 'hello')`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no FTS index")
}

func TestFTSEmptyQuery(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE docs(id INTEGER, content VARCHAR)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO docs VALUES (1, 'hello world')`)
	require.NoError(t, err)

	_, err = db.Exec(`PRAGMA create_fts_index('docs', 'content')`)
	require.NoError(t, err)

	// Search for something not in the corpus
	rows, err := db.Query(`SELECT * FROM fts_search('docs', 'elephant')`)
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 0, count, "should return no results for non-matching query")
}

func TestFTSRanking(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE docs(id INTEGER, content VARCHAR)`)
	require.NoError(t, err)

	// Doc with "database" once
	_, err = db.Exec(`INSERT INTO docs VALUES (1, 'database systems handle queries efficiently')`)
	require.NoError(t, err)
	// Doc with "database" three times - should score higher
	_, err = db.Exec(`INSERT INTO docs VALUES (2, 'database design database optimization database performance')`)
	require.NoError(t, err)
	// Doc without "database"
	_, err = db.Exec(`INSERT INTO docs VALUES (3, 'web application framework')`)
	require.NoError(t, err)

	_, err = db.Exec(`PRAGMA create_fts_index('docs', 'content')`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT * FROM fts_search('docs', 'database')`)
	require.NoError(t, err)
	defer rows.Close()

	type result struct {
		rowid int64
		score float64
	}
	var results []result
	for rows.Next() {
		var r result
		err := rows.Scan(&r.rowid, &r.score)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	// Should find 2 documents
	require.Len(t, results, 2)

	// Results should be sorted by score descending (highest first)
	assert.GreaterOrEqual(t, results[0].score, results[1].score,
		"first result should have higher or equal score")
}

func TestFTSMatchBM25Alias(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE docs(id INTEGER, content VARCHAR)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO docs VALUES (1, 'hello world test')`)
	require.NoError(t, err)

	_, err = db.Exec(`PRAGMA create_fts_index('docs', 'content')`)
	require.NoError(t, err)

	// match_bm25 should work as an alias for fts_search
	rows, err := db.Query(`SELECT * FROM match_bm25('docs', 'hello')`)
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 1, count, "match_bm25 should find 1 result")
}

func TestFTSCreateIndexMissingTable(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Should fail when table doesn't exist
	_, err = db.Exec(`PRAGMA create_fts_index('nonexistent', 'content')`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestFTSCreateIndexMissingColumn(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE docs(id INTEGER, content VARCHAR)`)
	require.NoError(t, err)

	// Should fail when column doesn't exist
	_, err = db.Exec(`PRAGMA create_fts_index('docs', 'nonexistent')`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestFTSDropNonexistentIndex(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Should fail when no index exists
	_, err = db.Exec(`PRAGMA drop_fts_index('nonexistent')`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no FTS index")
}

func TestFTSWithNullValues(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE docs(id INTEGER, content VARCHAR)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO docs VALUES (1, 'hello world')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO docs VALUES (2, NULL)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO docs VALUES (3, 'goodbye world')`)
	require.NoError(t, err)

	// Should not fail when indexing NULL values
	_, err = db.Exec(`PRAGMA create_fts_index('docs', 'content')`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT * FROM fts_search('docs', 'world')`)
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	// Should find 2 documents (skipping the NULL one)
	assert.Equal(t, 2, count)
}

func TestFTSEmptyTable(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE docs(id INTEGER, content VARCHAR)`)
	require.NoError(t, err)

	// Should not fail on empty table
	_, err = db.Exec(`PRAGMA create_fts_index('docs', 'content')`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT * FROM fts_search('docs', 'anything')`)
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 0, count, "should return no results for empty table")
}

func TestFTSReplaceIndex(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE docs(id INTEGER, content VARCHAR)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO docs VALUES (1, 'hello world')`)
	require.NoError(t, err)

	// Create index twice - second should replace first
	_, err = db.Exec(`PRAGMA create_fts_index('docs', 'content')`)
	require.NoError(t, err)
	_, err = db.Exec(`PRAGMA create_fts_index('docs', 'content')`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT * FROM fts_search('docs', 'hello')`)
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 1, count, "should still find results after re-creating index")
}
