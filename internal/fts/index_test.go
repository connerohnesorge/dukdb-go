package fts

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvertedIndex_AddAndSearch(t *testing.T) {
	idx := NewInvertedIndex("docs", "content")

	idx.AddDocument(1, "The quick brown fox jumps over the lazy dog")
	idx.AddDocument(2, "A quick brown cat sits on the mat")
	idx.AddDocument(3, "The dog runs in the park")

	results := idx.Search("quick brown")
	require.NotEmpty(t, results)

	// Both docs 1 and 2 should match
	docIDs := make(map[int64]bool)
	for _, r := range results {
		docIDs[r.DocID] = true
	}
	assert.True(t, docIDs[1], "doc 1 should match 'quick brown'")
	assert.True(t, docIDs[2], "doc 2 should match 'quick brown'")
	assert.False(t, docIDs[3], "doc 3 should not match 'quick brown'")
}

func TestInvertedIndex_EmptyQuery(t *testing.T) {
	idx := NewInvertedIndex("docs", "content")
	idx.AddDocument(1, "hello world")

	results := idx.Search("")
	assert.Empty(t, results)
}

func TestInvertedIndex_NoMatches(t *testing.T) {
	idx := NewInvertedIndex("docs", "content")
	idx.AddDocument(1, "hello world")

	results := idx.Search("elephant")
	assert.Empty(t, results)
}

func TestInvertedIndex_BM25Ranking(t *testing.T) {
	idx := NewInvertedIndex("docs", "content")

	// Doc 1 has "quick" and "brown" - should score highest for "quick brown"
	idx.AddDocument(1, "The quick brown fox jumps over the lazy dog")
	// Doc 2 also has "quick" and "brown"
	idx.AddDocument(2, "A quick brown cat sits on the mat")
	// Doc 3 only has "dog" - should not match "quick brown"
	idx.AddDocument(3, "The dog runs in the park")

	results := idx.Search("quick brown")
	require.Len(t, results, 2)

	// Both docs should have positive scores
	for _, r := range results {
		assert.Greater(t, r.Score, 0.0, "score should be positive")
	}
}

func TestInvertedIndex_TermFrequencyMatters(t *testing.T) {
	idx := NewInvertedIndex("docs", "content")

	// Doc 1 mentions "database" once
	idx.AddDocument(1, "database systems handle queries efficiently")
	// Doc 2 mentions "database" three times (higher TF)
	idx.AddDocument(2, "database design database optimization database performance")

	results := idx.Search("database")
	require.Len(t, results, 2)

	// Doc 2 should score higher because of higher term frequency
	assert.Equal(t, int64(2), results[0].DocID, "doc with higher TF should rank first")
}

func TestInvertedIndex_RemoveDocument(t *testing.T) {
	idx := NewInvertedIndex("docs", "content")

	idx.AddDocument(1, "hello world")
	idx.AddDocument(2, "hello there")

	// Both should match
	results := idx.Search("hello")
	require.Len(t, results, 2)

	// Remove doc 1
	idx.RemoveDocument(1)

	// Only doc 2 should match now
	results = idx.Search("hello")
	require.Len(t, results, 1)
	assert.Equal(t, int64(2), results[0].DocID)
}

func TestInvertedIndex_RemoveNonexistent(t *testing.T) {
	idx := NewInvertedIndex("docs", "content")
	idx.AddDocument(1, "hello world")

	// Should not panic
	idx.RemoveDocument(999)

	// Original doc still searchable
	results := idx.Search("hello")
	require.Len(t, results, 1)
}

func TestInvertedIndex_DocCount(t *testing.T) {
	idx := NewInvertedIndex("docs", "content")
	assert.Equal(t, 0, idx.DocCount())

	idx.AddDocument(1, "hello")
	assert.Equal(t, 1, idx.DocCount())

	idx.AddDocument(2, "world")
	assert.Equal(t, 2, idx.DocCount())

	idx.RemoveDocument(1)
	assert.Equal(t, 1, idx.DocCount())
}

func TestInvertedIndex_ConcurrentReads(t *testing.T) {
	idx := NewInvertedIndex("docs", "content")

	// Add some documents
	for i := int64(0); i < 100; i++ {
		idx.AddDocument(i, "hello world test document number")
	}

	// Concurrent reads should not panic or race
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results := idx.Search("hello world")
			assert.NotEmpty(t, results)
		}()
	}
	wg.Wait()
}

func TestRegistry_BasicOperations(t *testing.T) {
	reg := NewRegistry()

	// Create index
	idx := reg.CreateIndex("docs", "content")
	require.NotNil(t, idx)

	// Get index
	got, ok := reg.GetIndex("docs")
	require.True(t, ok)
	assert.Equal(t, idx, got)

	// Has index
	assert.True(t, reg.HasIndex("docs"))
	assert.False(t, reg.HasIndex("other"))

	// Drop index
	existed := reg.DropIndex("docs")
	assert.True(t, existed)
	assert.False(t, reg.HasIndex("docs"))

	// Drop non-existent
	existed = reg.DropIndex("docs")
	assert.False(t, existed)
}
