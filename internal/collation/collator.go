// Package collation provides collation support for locale-aware string
// comparison, sorting, and case conversion.
package collation

import (
	"strings"
	"unicode"

	"golang.org/x/text/collate"
	"golang.org/x/text/language"
	"golang.org/x/text/unicode/norm"
)

// Collator defines the interface for string comparison with collation.
type Collator interface {
	// Compare returns -1 if a < b, 0 if a == b, 1 if a > b
	// according to the collation rules.
	Compare(a, b string) int
	// Name returns the canonical name of this collation.
	Name() string
}

// BinaryCollator compares strings byte-by-byte (default Go string comparison).
type BinaryCollator struct{}

// Name returns "BINARY".
func (*BinaryCollator) Name() string { return "BINARY" }

// Compare compares two strings using byte-level ordering.
func (*BinaryCollator) Compare(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}

	return 0
}

// NocaseCollator compares strings case-insensitively using Unicode case folding.
type NocaseCollator struct{}

// Name returns "NOCASE".
func (*NocaseCollator) Name() string { return "NOCASE" }

// Compare compares two strings case-insensitively.
func (*NocaseCollator) Compare(a, b string) int {
	al := strings.ToLower(a)
	bl := strings.ToLower(b)
	if al < bl {
		return -1
	}
	if al > bl {
		return 1
	}

	return 0
}

// CCollator is the C/POSIX locale collator (same as binary comparison).
type CCollator struct{}

// Name returns "C".
func (*CCollator) Name() string { return "C" }

// Compare compares two strings using C locale (byte-level) ordering.
func (*CCollator) Compare(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}

	return 0
}

// PosixCollator is the POSIX locale collator (same as C).
type PosixCollator struct{}

// Name returns "POSIX".
func (*PosixCollator) Name() string { return "POSIX" }

// Compare compares two strings using POSIX locale (byte-level) ordering.
func (*PosixCollator) Compare(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}

	return 0
}

// NFCCollator normalizes strings to NFC form before comparison.
type NFCCollator struct{}

// Name returns "NFC".
func (*NFCCollator) Name() string { return "NFC" }

// Compare compares two strings after NFC normalization.
func (*NFCCollator) Compare(a, b string) int {
	an := norm.NFC.String(a)
	bn := norm.NFC.String(b)
	if an < bn {
		return -1
	}
	if an > bn {
		return 1
	}

	return 0
}

// NoaccentCollator compares strings after removing combining marks (accents).
// It uses NFKD decomposition and strips combining characters.
type NoaccentCollator struct{}

// Name returns "NOACCENT".
func (*NoaccentCollator) Name() string { return "NOACCENT" }

// Compare compares two strings after removing accents.
func (*NoaccentCollator) Compare(a, b string) int {
	an := removeAccents(a)
	bn := removeAccents(b)
	if an < bn {
		return -1
	}
	if an > bn {
		return 1
	}

	return 0
}

// removeAccents removes combining marks from a string by decomposing to NFKD
// and stripping characters in the Unicode "Mark" category.
func removeAccents(s string) string {
	decomposed := norm.NFKD.String(s)
	var b strings.Builder
	for _, r := range decomposed {
		if !unicode.Is(unicode.Mn, r) {
			b.WriteRune(r)
		}
	}

	return b.String()
}

// LocaleCollator wraps golang.org/x/text/collate.Collator for locale-aware
// string comparison.
type LocaleCollator struct {
	name string
	coll *collate.Collator
}

// NewLocaleCollator creates a new locale-aware collator for the given locale tag.
func NewLocaleCollator(name string, tag language.Tag) *LocaleCollator {
	return &LocaleCollator{
		name: strings.ToUpper(name),
		coll: collate.New(tag),
	}
}

// Name returns the locale name of this collator.
func (c *LocaleCollator) Name() string { return c.name }

// Compare compares two strings using locale-aware Unicode collation rules.
func (c *LocaleCollator) Compare(a, b string) int {
	return c.coll.CompareString(a, b)
}

// NocaseWrapper wraps another collator and applies case-insensitive comparison.
type NocaseWrapper struct {
	inner Collator
	name  string
}

// NewNocaseWrapper creates a case-insensitive wrapper around the given collator.
func NewNocaseWrapper(inner Collator) *NocaseWrapper {
	return &NocaseWrapper{
		inner: inner,
		name:  inner.Name() + ".NOCASE",
	}
}

// Name returns the chained collation name.
func (c *NocaseWrapper) Name() string { return c.name }

// Compare compares two strings case-insensitively through the wrapped collator.
func (c *NocaseWrapper) Compare(a, b string) int {
	return c.inner.Compare(strings.ToLower(a), strings.ToLower(b))
}

// NoaccentWrapper wraps another collator and removes accents before comparison.
type NoaccentWrapper struct {
	inner Collator
	name  string
}

// NewNoaccentWrapper creates an accent-insensitive wrapper around the given collator.
func NewNoaccentWrapper(inner Collator) *NoaccentWrapper {
	return &NoaccentWrapper{
		inner: inner,
		name:  inner.Name() + ".NOACCENT",
	}
}

// Name returns the chained collation name.
func (c *NoaccentWrapper) Name() string { return c.name }

// Compare compares two strings after removing accents through the wrapped collator.
func (c *NoaccentWrapper) Compare(a, b string) int {
	return c.inner.Compare(removeAccents(a), removeAccents(b))
}
