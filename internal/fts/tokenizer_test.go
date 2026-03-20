package fts

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTokenize_Basic(t *testing.T) {
	tokens := Tokenize("The quick brown fox jumps over the lazy dog")
	// "the" is a stop word, should be removed
	assert.NotContains(t, tokens, "the")
	assert.Contains(t, tokens, "quick")
	assert.Contains(t, tokens, "brown")
	assert.Contains(t, tokens, "fox")
	assert.Contains(t, tokens, "jumps")
	assert.Contains(t, tokens, "lazy")
	assert.Contains(t, tokens, "dog")
	// "over" is a stop word, should be removed
	assert.NotContains(t, tokens, "over")
}

func TestTokenize_Empty(t *testing.T) {
	tokens := Tokenize("")
	assert.Empty(t, tokens)
}

func TestTokenize_OnlyStopWords(t *testing.T) {
	tokens := Tokenize("the and is a")
	assert.Empty(t, tokens)
}

func TestTokenize_Punctuation(t *testing.T) {
	tokens := Tokenize("hello, world! this is a test.")
	assert.Contains(t, tokens, "hello")
	assert.Contains(t, tokens, "world")
	assert.Contains(t, tokens, "test")
	// stop words removed
	assert.NotContains(t, tokens, "this")
	assert.NotContains(t, tokens, "is")
	assert.NotContains(t, tokens, "a")
}

func TestTokenize_Unicode(t *testing.T) {
	tokens := Tokenize("Der schnelle braune Fuchs")
	assert.Contains(t, tokens, "der")
	assert.Contains(t, tokens, "schnelle")
	assert.Contains(t, tokens, "braune")
	assert.Contains(t, tokens, "fuchs")
}

func TestTokenize_MixedCase(t *testing.T) {
	tokens := Tokenize("GoLang Programming LANGUAGE")
	assert.Contains(t, tokens, "golang")
	assert.Contains(t, tokens, "programming")
	assert.Contains(t, tokens, "language")
}

func TestTokenize_Numbers(t *testing.T) {
	tokens := Tokenize("version 2 of golang is great")
	assert.Contains(t, tokens, "version")
	assert.Contains(t, tokens, "2")
	assert.Contains(t, tokens, "golang")
	assert.Contains(t, tokens, "great")
}

func TestIsStopWord(t *testing.T) {
	require.True(t, isStopWord("the"))
	require.True(t, isStopWord("and"))
	require.True(t, isStopWord("is"))
	require.False(t, isStopWord("fox"))
	require.False(t, isStopWord("quick"))
}
