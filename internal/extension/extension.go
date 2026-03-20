// Package extension provides the extension loading framework for dukdb-go.
// Extensions are compiled-in Go packages; there is no dynamic loading.
// INSTALL is a no-op, LOAD activates an extension.
package extension

import (
	"fmt"
	"sort"
	"sync"
)

// Extension is the interface all dukdb-go extensions implement.
type Extension interface {
	Name() string
	Description() string
	Version() string
	Load() error
	Unload() error
}

// Registry holds all registered extensions.
type Registry struct {
	mu         sync.RWMutex
	extensions map[string]*ExtensionEntry
}

// ExtensionEntry tracks an extension's state.
type ExtensionEntry struct {
	Extension Extension
	Installed bool // Always true for compiled-in extensions
	Loaded    bool // True after LOAD
}

// NewRegistry creates a new extension registry.
func NewRegistry() *Registry {
	return &Registry{
		extensions: make(map[string]*ExtensionEntry),
	}
}

// Register adds an extension to the registry (called at init time).
func (r *Registry) Register(ext Extension) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.extensions[ext.Name()] = &ExtensionEntry{
		Extension: ext,
		Installed: true,
		Loaded:    false,
	}
}

// Install is a no-op for compiled-in extensions.
// It returns nil even for unknown extension names, matching DuckDB behavior
// where INSTALL downloads but our extensions are already compiled in.
func (r *Registry) Install(name string) error {
	// No-op - all extensions are already compiled in
	return nil
}

// Load activates an extension.
func (r *Registry) Load(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.extensions[name]
	if !ok {
		return fmt.Errorf("extension %q not found", name)
	}
	if entry.Loaded {
		return nil // Already loaded
	}
	if err := entry.Extension.Load(); err != nil {
		return err
	}
	entry.Loaded = true
	return nil
}

// ListExtensions returns info about all registered extensions sorted by name.
func (r *Registry) ListExtensions() []ExtensionInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]ExtensionInfo, 0, len(r.extensions))
	for _, entry := range r.extensions {
		result = append(result, ExtensionInfo{
			Name:        entry.Extension.Name(),
			Description: entry.Extension.Description(),
			Version:     entry.Extension.Version(),
			Installed:   entry.Installed,
			Loaded:      entry.Loaded,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

// Get returns the extension entry for the given name, or nil if not found.
func (r *Registry) Get(name string) *ExtensionEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.extensions[name]
}

// ExtensionInfo holds metadata about a registered extension.
type ExtensionInfo struct {
	Name        string
	Description string
	Version     string
	Installed   bool
	Loaded      bool
}
