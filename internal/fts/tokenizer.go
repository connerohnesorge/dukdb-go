// Package fts provides full-text search functionality including tokenization,
// inverted indexing, and BM25 scoring.
package fts

import (
	"strings"
	"unicode"
)

// Tokenize splits text into lowercase tokens, removing punctuation and stop words.
// It is Unicode-aware: it uses unicode.IsLetter and unicode.IsDigit for character
// classification and strings.ToLower for case folding.
func Tokenize(text string) []string {
	var tokens []string
	var current strings.Builder

	for _, r := range strings.ToLower(text) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			current.WriteRune(r)
		} else if current.Len() > 0 {
			word := current.String()
			if !isStopWord(word) {
				tokens = append(tokens, word)
			}
			current.Reset()
		}
	}
	if current.Len() > 0 {
		word := current.String()
		if !isStopWord(word) {
			tokens = append(tokens, word)
		}
	}
	return tokens
}

// stopWords is the English stop word list. This covers the most common English
// function words that carry little semantic meaning for search.
var stopWords = map[string]bool{
	"a": true, "an": true, "and": true, "are": true, "as": true, "at": true,
	"be": true, "but": true, "by": true, "for": true, "if": true, "in": true,
	"into": true, "is": true, "it": true, "no": true, "not": true, "of": true,
	"on": true, "or": true, "such": true, "that": true, "the": true, "their": true,
	"then": true, "there": true, "these": true, "they": true, "this": true,
	"to": true, "was": true, "will": true, "with": true,
	"about": true, "after": true, "all": true, "also": true, "am": true,
	"been": true, "before": true, "between": true, "both": true, "can": true,
	"could": true, "did": true, "do": true, "does": true, "each": true,
	"from": true, "had": true, "has": true, "have": true, "he": true,
	"her": true, "here": true, "him": true, "his": true, "how": true,
	"i": true, "its": true, "just": true, "may": true, "me": true,
	"more": true, "most": true, "my": true, "nor": true, "only": true,
	"other": true, "our": true, "out": true, "over": true, "own": true,
	"same": true, "she": true, "should": true, "so": true, "some": true,
	"still": true, "than": true, "them": true, "those": true, "through": true,
	"too": true, "under": true, "up": true, "very": true, "we": true,
	"were": true, "what": true, "when": true, "where": true, "which": true,
	"while": true, "who": true, "whom": true, "why": true, "would": true,
	"you": true, "your": true,
}

func isStopWord(word string) bool {
	return stopWords[word]
}
