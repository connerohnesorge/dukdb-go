package metadata

import (
	"sort"

	"github.com/dukdb/dukdb-go/internal/parser"
)

// GetKeywords returns SQL keyword metadata.
func GetKeywords() []KeywordMetadata {
	keywords := parser.SQLKeywords()
	sort.Strings(keywords)

	result := make([]KeywordMetadata, 0, len(keywords))
	for _, keyword := range keywords {
		result = append(result, KeywordMetadata{
			Keyword:  keyword,
			Category: "SQL",
			Reserved: true,
		})
	}

	return result
}
