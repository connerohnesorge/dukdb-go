package extension

// builtinCSV wraps the CSV read/write functionality.
type builtinCSV struct{}

func (b *builtinCSV) Name() string        { return "csv" }
func (b *builtinCSV) Description() string  { return "CSV reader and writer" }
func (b *builtinCSV) Version() string      { return "1.0.0" }
func (b *builtinCSV) Load() error          { return nil } // Already wired in
func (b *builtinCSV) Unload() error        { return nil }

// builtinJSON wraps the JSON read/write functionality.
type builtinJSON struct{}

func (b *builtinJSON) Name() string        { return "json" }
func (b *builtinJSON) Description() string  { return "JSON reader and writer" }
func (b *builtinJSON) Version() string      { return "1.0.0" }
func (b *builtinJSON) Load() error          { return nil }
func (b *builtinJSON) Unload() error        { return nil }

// builtinParquet wraps the Parquet read/write functionality.
type builtinParquet struct{}

func (b *builtinParquet) Name() string        { return "parquet" }
func (b *builtinParquet) Description() string  { return "Parquet reader and writer" }
func (b *builtinParquet) Version() string      { return "1.0.0" }
func (b *builtinParquet) Load() error          { return nil }
func (b *builtinParquet) Unload() error        { return nil }

// builtinICU wraps the ICU collation and timezone functionality.
type builtinICU struct{}

func (b *builtinICU) Name() string        { return "icu" }
func (b *builtinICU) Description() string  { return "ICU collation and timezone support" }
func (b *builtinICU) Version() string      { return "1.0.0" }
func (b *builtinICU) Load() error          { return nil }
func (b *builtinICU) Unload() error        { return nil }

// DefaultRegistry creates a registry with all built-in extensions.
func DefaultRegistry() *Registry {
	r := NewRegistry()
	r.Register(&builtinCSV{})
	r.Register(&builtinJSON{})
	r.Register(&builtinParquet{})
	r.Register(&builtinICU{})
	return r
}
