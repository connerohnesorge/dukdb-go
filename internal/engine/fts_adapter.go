package engine

import (
	"github.com/dukdb/dukdb-go/internal/executor"
	"github.com/dukdb/dukdb-go/internal/fts"
)

// ftsRegistryAdapter adapts fts.Registry to executor.FTSRegistryInterface.
type ftsRegistryAdapter struct {
	registry *fts.Registry
}

func (a *ftsRegistryAdapter) CreateIndex(tableName, columnName string) executor.FTSIndex {
	idx := a.registry.CreateIndex(tableName, columnName)
	return &ftsIndexAdapter{idx: idx}
}

func (a *ftsRegistryAdapter) GetIndex(tableName string) (executor.FTSIndex, bool) {
	idx, ok := a.registry.GetIndex(tableName)
	if !ok {
		return nil, false
	}
	return &ftsIndexAdapter{idx: idx}, true
}

func (a *ftsRegistryAdapter) DropIndex(tableName string) bool {
	return a.registry.DropIndex(tableName)
}

func (a *ftsRegistryAdapter) HasIndex(tableName string) bool {
	return a.registry.HasIndex(tableName)
}

// ftsIndexAdapter adapts fts.InvertedIndex to executor.FTSIndex.
type ftsIndexAdapter struct {
	idx *fts.InvertedIndex
}

func (a *ftsIndexAdapter) Search(query string) []executor.FTSSearchResult {
	results := a.idx.Search(query)
	out := make([]executor.FTSSearchResult, len(results))
	for i, r := range results {
		out[i] = executor.FTSSearchResult{
			DocID: r.DocID,
			Score: r.Score,
		}
	}
	return out
}

func (a *ftsIndexAdapter) AddDocument(docID int64, text string) {
	a.idx.AddDocument(docID, text)
}

func (a *ftsIndexAdapter) RemoveDocument(docID int64) {
	a.idx.RemoveDocument(docID)
}

func (a *ftsIndexAdapter) TableName() string {
	return a.idx.TableName()
}

func (a *ftsIndexAdapter) ColumnName() string {
	return a.idx.ColumnName()
}

func (a *ftsIndexAdapter) DocCount() int {
	return a.idx.DocCount()
}
