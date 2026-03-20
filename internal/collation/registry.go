package collation

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"golang.org/x/text/language"
)

// Registry holds all available collations.
type Registry struct {
	mu        sync.RWMutex
	collators map[string]Collator
}

// NewRegistry creates a new collation registry with built-in collators registered.
func NewRegistry() *Registry {
	r := &Registry{
		collators: make(map[string]Collator),
	}
	r.registerBuiltins()

	return r
}

// registerBuiltins registers the default set of collators.
func (r *Registry) registerBuiltins() {
	r.Register(&BinaryCollator{})
	r.Register(&NocaseCollator{})
	r.Register(&CCollator{})
	r.Register(&PosixCollator{})
	r.Register(&NFCCollator{})
	r.Register(&NoaccentCollator{})
}

// Register adds a collator to the registry.
func (r *Registry) Register(c Collator) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.collators[strings.ToUpper(c.Name())] = c
}

// Get retrieves a collator by name. It supports chained modifiers like
// "de_DE.NOCASE.NOACCENT" by resolving the base collator first, then
// wrapping it with the appropriate modifier collators.
func (r *Registry) Get(name string) (Collator, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	upper := strings.ToUpper(strings.TrimSpace(name))

	// Try direct lookup first.
	if c, ok := r.collators[upper]; ok {
		return c, true
	}

	// Try resolving chained collation (e.g., "DE_DE.NOCASE.NOACCENT").
	return r.resolveChained(upper)
}

// resolveChained resolves a chained collation specification like "DE_DE.NOCASE.NOACCENT".
// Caller must hold at least a read lock.
func (r *Registry) resolveChained(name string) (Collator, bool) {
	parts := strings.Split(name, ".")
	if len(parts) < 2 {
		// Not a chained collation; try as a locale tag.
		return r.tryLocale(name)
	}

	// Resolve the base collator.
	base, ok := r.collators[parts[0]]
	if !ok {
		// Try as a locale tag.
		base, ok = r.tryLocale(parts[0])
		if !ok {
			return nil, false
		}
	}

	// Apply modifiers in order.
	result := base
	for _, mod := range parts[1:] {
		switch mod {
		case "NOCASE":
			result = NewNocaseWrapper(result)
		case "NOACCENT":
			result = NewNoaccentWrapper(result)
		default:
			return nil, false
		}
	}

	return result, true
}

// tryLocale attempts to parse the name as a BCP 47 or underscore-separated
// locale tag and create a LocaleCollator for it.
func (*Registry) tryLocale(name string) (Collator, bool) {
	// Convert underscore-separated locale (e.g., "DE_DE") to BCP 47 (e.g., "de-DE").
	bcp47 := strings.ReplaceAll(name, "_", "-")
	tag, err := language.Parse(strings.ToLower(bcp47))
	if err != nil {
		return nil, false
	}

	return NewLocaleCollator(name, tag), true
}

// ListCollations returns a sorted list of all registered collation names.
func (r *Registry) ListCollations() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.collators))
	for name := range r.collators {
		names = append(names, name)
	}
	sort.Strings(names)

	return names
}

// MustGet retrieves a collator by name and panics if not found.
// This is intended for use in initialization code where a missing collation
// indicates a programming error.
func (r *Registry) MustGet(name string) Collator {
	c, ok := r.Get(name)
	if !ok {
		panic(fmt.Sprintf("collation not found: %s", name))
	}

	return c
}

// DefaultRegistry is a global registry with built-in collations.
var DefaultRegistry = NewRegistry()
