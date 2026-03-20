package fts

import (
	"math"
	"sort"
	"sync"
)

// Posting represents a single term occurrence in a document.
type Posting struct {
	DocID    int64 // Row ID in the source table
	TermFreq int   // Number of times this term appears in this document
}

// InvertedIndex is an in-memory inverted index for full-text search.
// It supports concurrent reads via sync.RWMutex and provides BM25-scored search.
type InvertedIndex struct {
	mu         sync.RWMutex
	postings   map[string][]Posting // term -> sorted postings
	docLengths map[int64]int        // docID -> total token count
	totalDocs  int
	avgDocLen  float64
	tableName  string
	columnName string
}

// NewInvertedIndex creates a new empty inverted index for the given table and column.
func NewInvertedIndex(tableName, columnName string) *InvertedIndex {
	return &InvertedIndex{
		postings:   make(map[string][]Posting),
		docLengths: make(map[int64]int),
		tableName:  tableName,
		columnName: columnName,
	}
}

// TableName returns the table name this index is associated with.
func (idx *InvertedIndex) TableName() string {
	return idx.tableName
}

// ColumnName returns the column name this index is associated with.
func (idx *InvertedIndex) ColumnName() string {
	return idx.columnName
}

// AddDocument indexes a document (row) into the inverted index.
// The docID should correspond to the row ID in the source table.
func (idx *InvertedIndex) AddDocument(docID int64, text string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	tokens := Tokenize(text)
	idx.docLengths[docID] = len(tokens)
	idx.totalDocs++

	// Count term frequencies
	termFreqs := make(map[string]int)
	for _, token := range tokens {
		termFreqs[token]++
	}

	// Add postings
	for term, freq := range termFreqs {
		idx.postings[term] = append(idx.postings[term], Posting{
			DocID:    docID,
			TermFreq: freq,
		})
	}

	// Update average document length
	idx.recalcAvgDocLen()
}

// RemoveDocument removes a document from the index.
func (idx *InvertedIndex) RemoveDocument(docID int64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if _, exists := idx.docLengths[docID]; !exists {
		return
	}

	delete(idx.docLengths, docID)
	idx.totalDocs--

	for term, postings := range idx.postings {
		filtered := postings[:0]
		for _, p := range postings {
			if p.DocID != docID {
				filtered = append(filtered, p)
			}
		}
		if len(filtered) == 0 {
			delete(idx.postings, term)
		} else {
			idx.postings[term] = filtered
		}
	}

	idx.recalcAvgDocLen()
}

// recalcAvgDocLen recalculates the average document length. Caller must hold idx.mu.
func (idx *InvertedIndex) recalcAvgDocLen() {
	totalLen := 0
	for _, l := range idx.docLengths {
		totalLen += l
	}
	if idx.totalDocs > 0 {
		idx.avgDocLen = float64(totalLen) / float64(idx.totalDocs)
	} else {
		idx.avgDocLen = 0
	}
}

// SearchResult represents a search result with BM25 score.
type SearchResult struct {
	DocID int64
	Score float64
}

// Search performs a BM25 search for the given query string.
// Results are returned sorted by score in descending order.
func (idx *InvertedIndex) Search(query string) []SearchResult {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	queryTokens := Tokenize(query)
	if len(queryTokens) == 0 {
		return nil
	}

	// BM25 parameters (standard defaults)
	k1 := 1.2
	b := 0.75

	scores := make(map[int64]float64)

	for _, term := range queryTokens {
		postings, ok := idx.postings[term]
		if !ok {
			continue
		}

		// IDF = log((N - n + 0.5) / (n + 0.5) + 1)
		n := float64(len(postings))
		N := float64(idx.totalDocs)
		idf := math.Log((N-n+0.5)/(n+0.5) + 1)

		for _, posting := range postings {
			tf := float64(posting.TermFreq)
			docLen := float64(idx.docLengths[posting.DocID])

			// BM25 = IDF * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl/avgdl))
			denom := tf + k1*(1-b+b*docLen/idx.avgDocLen)
			if denom == 0 {
				continue
			}
			score := idf * (tf * (k1 + 1)) / denom
			scores[posting.DocID] += score
		}
	}

	// Convert to sorted results
	results := make([]SearchResult, 0, len(scores))
	for docID, score := range scores {
		results = append(results, SearchResult{DocID: docID, Score: score})
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score // Descending by score
		}
		return results[i].DocID < results[j].DocID // Stable tiebreaker
	})

	return results
}

// DocCount returns the total number of indexed documents.
func (idx *InvertedIndex) DocCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.totalDocs
}
