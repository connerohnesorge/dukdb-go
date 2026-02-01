# Extension Loading Specification

**Specification ID:** `extension-loading-v1.0`
**Version:** 1.0
**Status:** Draft
**Last Updated:** 2024-01-20

## Overview

This specification defines the dynamic loading mechanism for extensions in dukdb-go, ensuring secure, efficient, and reliable loading of extension binaries while maintaining compatibility with DuckDB v1.4.3 extension behaviors.

## Requirements

### Functional Requirements

1. **Dynamic Loading**: Extensions must be loadable at runtime without restarting the database
2. **Version Compatibility**: Loader must verify DuckDB version compatibility
3. **Dependency Resolution**: Automatic resolution of extension dependencies
4. **Conflict Detection**: Detect and handle conflicting extensions
5. **Atomic Operations**: Extension loading must be atomic (all-or-nothing)
6. **Rollback Support**: Ability to rollback failed loading attempts
7. **Lazy Loading**: Support for on-demand loading of extensions
8. **Preloading**: Support for preloading frequently used extensions

### Non-Functional Requirements

1. **Performance**: Loading overhead < 100ms for typical extensions
2. **Memory**: Memory overhead < 10MB per loaded extension
3. **Security**: All extensions verified before loading
4. **Reliability**: 99.9% successful loading rate
5. **Scalability**: Support for 100+ extensions loaded simultaneously
6. **Isolation**: Extensions cannot interfere with each other

## Extension Binary Format

### File Structure

```
Extension Binary (.dukdb)
┌─────────────────────────────────────────────────────────────┐
│                    Header (64 bytes)                        │
├─────────────────────────────────────────────────────────────┤
│                  Manifest Section                           │
├─────────────────────────────────────────────────────────────┤
│                  Code Section                               │
├─────────────────────────────────────────────────────────────┤
│                  Data Section                               │
├─────────────────────────────────────────────────────────────┤
│                  Symbol Table                               │
├─────────────────────────────────────────────────────────────┤
│                  Signature Block                            │
└─────────────────────────────────────────────────────────────┘
```

### Header Format

```go
type ExtensionHeader struct {
    Magic       [8]byte  // "DUKDBEXT"
    Version     uint32   // Format version
    Flags       uint32   // Feature flags
    ManifestOffset uint64
    ManifestSize   uint64
    CodeOffset     uint64
    CodeSize       uint64
    DataOffset     uint64
    DataSize       uint64
    SymbolOffset   uint64
    SymbolSize     uint64
    SignatureOffset uint64
    SignatureSize   uint64
}
```

### Manifest Schema

```yaml
# Extension Manifest Format
name: "extension_name"
version: "1.0.0"
duckdb_version: ">=1.4.0,<2.0.0"

metadata:
  title: "Extension Title"
  description: "Extension description"
  author: "Author Name"
  license: "MIT"
  homepage: "https://example.com"
  repository: "https://github.com/user/repo"

capabilities:
  functions:
    - name: "custom_function"
      type: "scalar"
      arguments:
        - type: "INTEGER"
        - type: "VARCHAR"
      return_type: "DOUBLE"
      volatility: "immutable"

  types:
    - name: "custom_type"
      size: 16
      alignment: 8
      storage_type: "BLOB"

  operators:
    - name: "custom_op"
      symbol: "%%"
      arguments: ["custom_type", "custom_type"]
      return_type: "BOOLEAN"

  file_formats:
    - name: "custom_format"
      extensions: [".custom"]
      mime_types: ["application/x-custom"]

dependencies:
  - name: "required_extension"
    version: ">=1.2.0"
    optional: false

permissions:
  - type: "file"
    paths: ["/tmp/custom_ext"]
    access: ["read", "write"]
  - type: "network"
    hosts: ["api.example.com"]
    ports: [443]

signature:
  algorithm: "RSA-SHA256"
  fingerprint: "SHA256:abcdef..."
  timestamp: 2024-01-20T00:00:00Z
```

## Loading Process

### Load Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Validate  │────▶│   Resolve   │────▶│   Load      │
│  Extension  │     │Dependencies │     │  Binary     │
└─────────────┘     └─────────────┘     └─────────────┘
       │                   │                   │
       ▼                   ▼                   ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Security   │     │   Verify    │────▶│  Register   │
│    Check    │     │  Signature  │     │  Symbols    │
└─────────────┘     └─────────────┘     └─────────────┘
       │                                     │
       └─────────────────────────────────────┘
                            │
                            ▼
                    ┌─────────────┐
                    │   Activate  │
                    │  Extension  │
                    └─────────────┘
```

### Loading Algorithm

```go
func (l *Loader) LoadExtension(name string, config LoadConfig) (*LoadedExtension, error) {
    // 1. Validate extension
    if err := l.validateExtension(name); err != nil {
        return nil, fmt.Errorf("validation failed: %w", err)
    }

    // 2. Resolve dependencies
    deps, err := l.resolveDependencies(name)
    if err != nil {
        return nil, fmt.Errorf("dependency resolution failed: %w", err)
    }

    // 3. Load dependencies first
    for _, dep := range deps {
        if _, err := l.LoadExtension(dep.Name, config); err != nil {
            return nil, fmt.Errorf("failed to load dependency %s: %w", dep.Name, err)
        }
    }

    // 4. Load binary
    binary, err := l.loadBinary(name)
    if err != nil {
        return nil, fmt.Errorf("failed to load binary: %w", err)
    }

    // 5. Verify signature
    if err := l.verifySignature(binary); err != nil {
        return nil, fmt.Errorf("signature verification failed: %w", err)
    }

    // 6. Create sandbox
    sandbox, err := l.createSandbox(binary.Manifest)
    if err != nil {
        return nil, fmt.Errorf("sandbox creation failed: %w", err)
    }

    // 7. Initialize extension
    ext, err := l.initializeExtension(binary, sandbox)
    if err != nil {
        sandbox.Cleanup()
        return nil, fmt.Errorf("initialization failed: %w", err)
    }

    // 8. Register symbols
    if err := l.registerSymbols(ext); err != nil {
        ext.Cleanup()
        return nil, fmt.Errorf("symbol registration failed: %w", err)
    }

    // 9. Activate extension
    if err := ext.Activate(); err != nil {
        l.unregisterSymbols(ext)
        ext.Cleanup()
        return nil, fmt.Errorf("activation failed: %w", err)
    }

    return ext, nil
}
```

### Dependency Resolution

```go
type DependencyResolver struct {
    registry ExtensionRegistry
    graph    *DependencyGraph
}

func (r *DependencyResolver) Resolve(name string) ([]Dependency, error) {
    manifest, err := r.registry.GetManifest(name)
    if err != nil {
        return nil, err
    }

    // Build dependency graph
    graph := NewDependencyGraph()
    if err := r.buildGraph(manifest, graph); err != nil {
        return nil, err
    }

    // Check for cycles
    if err := graph.CheckCycles(); err != nil {
        return nil, err
    }

    // Get topological order
    order, err := graph.TopologicalSort()
    if err != nil {
        return nil, err
    }

    return order, nil
}

func (r *DependencyResolver) buildGraph(manifest *Manifest, graph *DependencyGraph) error {
    for _, dep := range manifest.Dependencies {
        // Check if already resolved
        if graph.HasNode(dep.Name) {
            continue
        }

        // Get dependency manifest
        depManifest, err := r.registry.GetManifest(dep.Name)
        if err != nil {
            if dep.Optional {
                continue // Skip optional dependencies
            }
            return fmt.Errorf("required dependency %s not found", dep.Name)
        }

        // Check version compatibility
        if !r.checkVersion(dep.Version, depManifest.Version) {
            return fmt.Errorf("incompatible version for %s: need %s, have %s",
                dep.Name, dep.Version, depManifest.Version)
        }

        // Add to graph
        graph.AddNode(dep.Name, depManifest)
        graph.AddEdge(manifest.Name, dep.Name)

        // Recursively resolve dependencies
        if err := r.buildGraph(depManifest, graph); err != nil {
            return err
        }
    }

    return nil
}
```

## Security Verification

### Signature Verification

```go
type SignatureVerifier struct {
    trustedKeys []PublicKey
    CRL         *CertificateRevocationList
}

func (v *SignatureVerifier) Verify(binary *ExtensionBinary) error {
    // 1. Check if signature present
    if binary.Signature == nil {
        return fmt.Errorf("extension not signed")
    }

    // 2. Verify certificate chain
    chain, err := v.verifyCertificateChain(binary.Signature.Certificate)
    if err != nil {
        return fmt.Errorf("invalid certificate chain: %w", err)
    }

    // 3. Check revocation
    for _, cert := range chain {
        if v.CRL.IsRevoked(cert.SerialNumber) {
            return fmt.Errorf("certificate %s revoked", cert.SerialNumber)
        }
    }

    // 4. Verify signature
    data := binary.GetSignedData()
    if err := v.verifySignature(data, binary.Signature); err != nil {
        return fmt.Errorf("signature verification failed: %w", err)
    }

    // 5. Check permissions
    if err := v.checkPermissions(binary.Manifest, chain[0]); err != nil {
        return fmt.Errorf("permission check failed: %w", err)
    }

    return nil
}
```

### Permission Validation

```go
func (v *SignatureVerifier) checkPermissions(manifest *Manifest, cert *Certificate) error {
    // Extract permissions from certificate
    certPerms := v.extractPermissions(cert)

    // Check each requested permission
    for _, perm := range manifest.Permissions {
        if !v.hasPermission(certPerms, perm) {
            return fmt.Errorf("missing permission: %s", perm.Type)
        }
    }

    return nil
}
```

## Symbol Registration

### Registration Process

```go
type SymbolRegistry struct {
    functions  FunctionRegistry
    types      TypeRegistry
    operators  OperatorRegistry
    formats    FileFormatRegistry
}

func (r *SymbolRegistry) RegisterExtension(ext *LoadedExtension) error {
    manifest := ext.GetManifest()

    // Register functions
    for _, fn := range manifest.Functions {
        switch fn.Type {
        case "scalar":
            if err := r.registerScalarFunction(ext, fn); err != nil {
                return err
            }
        case "aggregate":
            if err := r.registerAggregateFunction(ext, fn); err != nil {
                return err
            }
        case "table":
            if err := r.registerTableFunction(ext, fn); err != nil {
                return err
            }
        }
    }

    // Register types
    for _, typ := range manifest.Types {
        if err := r.registerType(ext, typ); err != nil {
            return err
        }
    }

    // Register operators
    for _, op := range manifest.Operators {
        if err := r.registerOperator(ext, op); err != nil {
            return err
        }
    }

    // Register file formats
    for _, format := range manifest.FileFormats {
        if err := r.registerFileFormat(ext, format); err != nil {
            return err
        }
    }

    return nil
}

func (r *SymbolRegistry) registerScalarFunction(ext *LoadedExtension, def FunctionDef) error {
    // Get function implementation from extension
    impl, err := ext.GetSymbol(def.Name)
    if err != nil {
        return fmt.Errorf("function %s not found in extension", def.Name)
    }

    // Create function definition
    fn := &ScalarFunction{
        Name:       def.Name,
        Arguments:  def.Arguments,
        ReturnType: def.ReturnType,
        Volatility: def.Volatility,
        Function:   impl.(ScalarFunctionImpl),
        Extension:  ext,
    }

    // Register with function registry
    return r.functions.Register(fn)
}
```

## Lazy Loading

### Implementation

```go
type LazyLoader struct {
    registry    ExtensionRegistry
    loader      *Loader
    loaded      map[string]*LoadedExtension
    loading     map[string]chan struct{}
    mutex       sync.Mutex
}

func (l *LazyLoader) GetExtension(name string) (*LoadedExtension, error) {
    // Fast path: already loaded
    l.mutex.Lock()
    if ext, ok := l.loaded[name]; ok {
        l.mutex.Unlock()
        return ext, nil
    }

    // Check if already loading
    if ch, ok := l.loading[name]; ok {
        l.mutex.Unlock()
        <-ch // Wait for loading to complete
        return l.GetExtension(name) // Try again
    }

    // Start loading
    ch := make(chan struct{})
    l.loading[name] = ch
    l.mutex.Unlock()

    // Load extension
    ext, err := l.loader.LoadExtension(name, DefaultLoadConfig)

    // Update state
    l.mutex.Lock()
    delete(l.loading, name)
    if err == nil {
        l.loaded[name] = ext
    }
    close(ch)
    l.mutex.Unlock()

    return ext, err
}
```

## Error Handling

### Error Types

```go
type LoadError struct {
    Extension string
    Phase     string
    Cause     error
    Context   map[string]interface{}
}

func (e *LoadError) Error() string {
    return fmt.Sprintf("failed to load extension %s in phase %s: %v",
        e.Extension, e.Phase, e.Cause)
}

func (e *LoadError) Unwrap() error {
    return e.Cause
}

// Specific error types
type ValidationError struct {
    LoadError
    Issues []ValidationIssue
}

type DependencyError struct {
    LoadError
    Missing []string
}

type SignatureError struct {
    LoadError
    Reason string
}
```

### Recovery Mechanisms

```go
type LoadRecovery interface {
    // Retry with backoff
    RetryWithBackoff(fn func() error, attempts int) error

    // Fallback to alternative
    TryFallback(primary, fallback string) (*LoadedExtension, error)

    // Partial load recovery
    RecoverPartialLoad(name string, loaded []string) error

    // Cleanup on failure
    CleanupFailedLoad(name string) error
}
```

## Performance Optimization

### Caching Strategy

```go
type LoadCache struct {
    manifests *LRUCache[string, *Manifest]
    binaries  *LRUCache[string, *ExtensionBinary]
    symbols   *LRUCache[string, interface{}]
}

func (c *LoadCache) GetManifest(name string) (*Manifest, bool) {
    return c.manifests.Get(name)
}

func (c *LoadCache) PutManifest(name string, manifest *Manifest) {
    c.manifests.Put(name, manifest)
}
```

### Preloading

```go
type PreloadManager struct {
    config   *PreloadConfig
    loader   *Loader
    priority []string
}

func (m *PreloadManager) Start() {
    for _, name := range m.priority {
        go func(n string) {
            if _, err := m.loader.LoadExtension(n, DefaultLoadConfig); err != nil {
                log.Printf("Failed to preload extension %s: %v", n, err)
            }
        }(name)
    }
}
```

## Testing

### Load Testing

```go
type LoadTestSuite struct {
    loader *Loader
    registry ExtensionRegistry
}

func (s *LoadTestSuite) TestLoadValidExtension() {
    ext, err := s.loader.LoadExtension("test_ext", DefaultLoadConfig)
    require.NoError(t, err)
    require.NotNil(t, ext)
    require.Equal(t, "test_ext", ext.Name())
}

func (s *LoadTestSuite) TestLoadWithDependencies() {
    // Load extension with dependencies
    ext, err := s.loader.LoadExtension("ext_with_deps", DefaultLoadConfig)
    require.NoError(t, err)

    // Verify dependencies loaded
    deps := ext.GetDependencies()
    require.Len(t, deps, 2)
}

func (s *LoadTestSuite) TestLoadInvalidSignature() {
    _, err := s.loader.LoadExtension("invalid_sig", DefaultLoadConfig)
    require.Error(t, err)
    require.IsType(t, &SignatureError{}, err)
}
```

### Performance Testing

```go
func BenchmarkLoadExtension(b *testing.B) {
    loader := NewLoader(NewConfig())

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        ext, err := loader.LoadExtension("benchmark_ext", DefaultLoadConfig)
        if err != nil {
            b.Fatal(err)
        }
        ext.Unload()
    }
}
```

## Configuration

### Load Configuration

```go
type LoadConfig struct {
    // Security
    VerifySignature bool
    TrustedKeys     []PublicKey

    // Performance
    EnableCache     bool
    PreloadSymbols  bool

    // Behavior
    LazyLoad        bool
    AutoDependencies bool

    // Resource limits
    MaxMemory       int64
    MaxLoadTime     time.Duration
}
```

### Default Configuration

```yaml
# Default load configuration
load:
  verify_signature: true
  enable_cache: true
  preload_symbols: false
  lazy_load: true
  auto_dependencies: true
  max_memory: 100MB
  max_load_time: 30s

  # Cache settings
  cache_size: 1000
  cache_ttl: 1h

  # Preload settings
  preload_extensions: ["parquet", "json"]
```

This specification provides a complete foundation for implementing secure, efficient, and reliable extension loading in dukdb-go while maintaining compatibility with DuckDB v1.4.3 and adhering to the zero-CGO constraint. The design prioritizes security, performance, and developer experience through comprehensive validation, caching, and error handling mechanisms.## Version History

| Version | Date | Changes | Author |
|---------|------|---------|--------|
| 1.0 | 2024-01-20 | Initial specification | dukdb-go Team |