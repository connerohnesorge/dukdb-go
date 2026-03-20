package extension

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testExtension is a simple extension for testing.
type testExtension struct {
	name    string
	loaded  bool
	loadErr error
}

func (t *testExtension) Name() string        { return t.name }
func (t *testExtension) Description() string  { return "test extension" }
func (t *testExtension) Version() string      { return "0.1.0" }
func (t *testExtension) Load() error {
	if t.loadErr != nil {
		return t.loadErr
	}
	t.loaded = true
	return nil
}
func (t *testExtension) Unload() error {
	t.loaded = false
	return nil
}

func TestRegistryRegister(t *testing.T) {
	r := NewRegistry()
	ext := &testExtension{name: "test"}
	r.Register(ext)

	entry := r.Get("test")
	require.NotNil(t, entry)
	assert.True(t, entry.Installed)
	assert.False(t, entry.Loaded)
}

func TestRegistryLoad(t *testing.T) {
	r := NewRegistry()
	ext := &testExtension{name: "test"}
	r.Register(ext)

	err := r.Load("test")
	require.NoError(t, err)

	entry := r.Get("test")
	require.NotNil(t, entry)
	assert.True(t, entry.Loaded)
	assert.True(t, ext.loaded)
}

func TestRegistryLoadAlreadyLoaded(t *testing.T) {
	r := NewRegistry()
	ext := &testExtension{name: "test"}
	r.Register(ext)

	err := r.Load("test")
	require.NoError(t, err)

	// Loading again should be a no-op
	err = r.Load("test")
	require.NoError(t, err)
}

func TestRegistryLoadUnknown(t *testing.T) {
	r := NewRegistry()
	err := r.Load("nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestRegistryInstallNoOp(t *testing.T) {
	r := NewRegistry()
	// Install is a no-op even for unknown extensions
	err := r.Install("anything")
	require.NoError(t, err)
}

func TestRegistryListExtensions(t *testing.T) {
	r := NewRegistry()
	r.Register(&testExtension{name: "beta"})
	r.Register(&testExtension{name: "alpha"})

	list := r.ListExtensions()
	require.Len(t, list, 2)
	// Should be sorted by name
	assert.Equal(t, "alpha", list[0].Name)
	assert.Equal(t, "beta", list[1].Name)
	assert.True(t, list[0].Installed)
	assert.False(t, list[0].Loaded)
}

func TestDefaultRegistry(t *testing.T) {
	r := DefaultRegistry()
	list := r.ListExtensions()
	require.Len(t, list, 4)

	names := make([]string, len(list))
	for i, ext := range list {
		names[i] = ext.Name
	}
	assert.Contains(t, names, "csv")
	assert.Contains(t, names, "json")
	assert.Contains(t, names, "parquet")
	assert.Contains(t, names, "icu")
}
