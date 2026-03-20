package collation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBinaryCollator(t *testing.T) {
	c := &BinaryCollator{}
	assert.Equal(t, "BINARY", c.Name())
	assert.Equal(t, -1, c.Compare("apple", "banana"))
	assert.Equal(t, 1, c.Compare("banana", "apple"))
	assert.Equal(t, 0, c.Compare("apple", "apple"))
	// Binary: uppercase letters come before lowercase in ASCII.
	assert.Equal(t, -1, c.Compare("Apple", "apple"))
}

func TestNocaseCollator(t *testing.T) {
	c := &NocaseCollator{}
	assert.Equal(t, "NOCASE", c.Name())
	assert.Equal(t, 0, c.Compare("Apple", "apple"))
	assert.Equal(t, 0, c.Compare("BANANA", "banana"))
	assert.Equal(t, -1, c.Compare("apple", "banana"))
	assert.Equal(t, 1, c.Compare("cherry", "banana"))
}

func TestCCollator(t *testing.T) {
	c := &CCollator{}
	assert.Equal(t, "C", c.Name())
	assert.Equal(t, -1, c.Compare("a", "b"))
	assert.Equal(t, 0, c.Compare("x", "x"))
}

func TestPosixCollator(t *testing.T) {
	c := &PosixCollator{}
	assert.Equal(t, "POSIX", c.Name())
	assert.Equal(t, -1, c.Compare("a", "b"))
}

func TestNFCCollator(t *testing.T) {
	c := &NFCCollator{}
	assert.Equal(t, "NFC", c.Name())
	// NFC normalization: combining character forms should be equal to precomposed.
	// e + combining acute = e-acute precomposed
	assert.Equal(t, 0, c.Compare("e\u0301", "\u00e9"))
}

func TestNoaccentCollator(t *testing.T) {
	c := &NoaccentCollator{}
	assert.Equal(t, "NOACCENT", c.Name())
	// Accented characters should compare equal to their base forms.
	assert.Equal(t, 0, c.Compare("\u00e9", "e"))  // e-acute vs e
	assert.Equal(t, 0, c.Compare("caf\u00e9", "cafe"))
	assert.Equal(t, -1, c.Compare("cafe", "caff"))
}

func TestLocaleCollator(t *testing.T) {
	// Test German locale collator.
	registry := NewRegistry()
	c, ok := registry.Get("de_DE")
	require.True(t, ok, "de_DE locale should be resolvable")
	assert.Equal(t, "DE_DE", c.Name())
	// In German collation, ae and a-umlaut may sort differently than binary.
	// At minimum, the collator should work without errors.
	result := c.Compare("apfel", "birne")
	assert.Equal(t, -1, result)
}

func TestRegistryBuiltins(t *testing.T) {
	r := NewRegistry()
	collations := r.ListCollations()
	assert.Contains(t, collations, "BINARY")
	assert.Contains(t, collations, "NOCASE")
	assert.Contains(t, collations, "C")
	assert.Contains(t, collations, "POSIX")
	assert.Contains(t, collations, "NFC")
	assert.Contains(t, collations, "NOACCENT")
}

func TestRegistryGet(t *testing.T) {
	r := NewRegistry()

	c, ok := r.Get("binary")
	require.True(t, ok)
	assert.Equal(t, "BINARY", c.Name())

	c, ok = r.Get("NOCASE")
	require.True(t, ok)
	assert.Equal(t, "NOCASE", c.Name())

	_, ok = r.Get("nonexistent_xyz_123")
	assert.False(t, ok)
}

func TestRegistryChainedCollation(t *testing.T) {
	r := NewRegistry()

	// Test chained collation: de_DE.NOCASE
	c, ok := r.Get("de_DE.NOCASE")
	require.True(t, ok)
	assert.Equal(t, "DE_DE.NOCASE", c.Name())

	// Test that it is case-insensitive.
	assert.Equal(t, 0, c.Compare("Apfel", "apfel"))

	// Test chained collation: BINARY.NOCASE.NOACCENT
	c, ok = r.Get("BINARY.NOCASE.NOACCENT")
	require.True(t, ok)
	assert.Equal(t, "BINARY.NOCASE.NOACCENT", c.Name())

	// Should be both case- and accent-insensitive.
	assert.Equal(t, 0, c.Compare("Caf\u00e9", "cafe"))
}

func TestRegistryCustomCollator(t *testing.T) {
	r := NewRegistry()
	r.Register(&BinaryCollator{}) // re-register is fine

	c, ok := r.Get("BINARY")
	require.True(t, ok)
	assert.Equal(t, "BINARY", c.Name())
}

func TestNocaseWrapper(t *testing.T) {
	inner := &BinaryCollator{}
	w := NewNocaseWrapper(inner)
	assert.Equal(t, "BINARY.NOCASE", w.Name())
	assert.Equal(t, 0, w.Compare("Hello", "hello"))
	assert.Equal(t, -1, w.Compare("abc", "def"))
}

func TestNoaccentWrapper(t *testing.T) {
	inner := &BinaryCollator{}
	w := NewNoaccentWrapper(inner)
	assert.Equal(t, "BINARY.NOACCENT", w.Name())
	assert.Equal(t, 0, w.Compare("caf\u00e9", "cafe"))
}

func TestDefaultRegistry(t *testing.T) {
	// Ensure the global default registry is functional.
	c, ok := DefaultRegistry.Get("BINARY")
	require.True(t, ok)
	assert.Equal(t, "BINARY", c.Name())
}

func TestMustGet(t *testing.T) {
	r := NewRegistry()
	c := r.MustGet("BINARY")
	assert.Equal(t, "BINARY", c.Name())

	assert.Panics(t, func() {
		r.MustGet("nonexistent_xyz_123")
	})
}

func TestRemoveAccents(t *testing.T) {
	assert.Equal(t, "cafe", removeAccents("caf\u00e9"))
	assert.Equal(t, "uber", removeAccents("\u00fcber"))
	assert.Equal(t, "hello", removeAccents("hello"))
}
