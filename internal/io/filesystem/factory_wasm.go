//go:build js && wasm

package filesystem

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// FileSystemFactoryFunc is a function that creates a FileSystem from a URL.
type FileSystemFactoryFunc func(ctx context.Context, url string) (FileSystem, error)

// FileSystemFactory manages filesystem creation based on URL schemes.
type FileSystemFactory interface {
	// GetFileSystem returns a FileSystem for the given URL.
	GetFileSystem(ctx context.Context, url string) (FileSystem, error)
	// RegisterScheme registers a factory function for a scheme.
	RegisterScheme(scheme string, factory FileSystemFactoryFunc)
	// UnregisterScheme removes a registered scheme.
	UnregisterScheme(scheme string)
	// SupportedSchemes returns a list of supported schemes.
	SupportedSchemes() []string
}

// factory is the default implementation of FileSystemFactory.
type factory struct {
	mu      sync.RWMutex
	schemes map[string]FileSystemFactoryFunc
}

// defaultFactory is the global default factory instance.
var (
	defaultFactory     *factory
	defaultFactoryOnce sync.Once
)

// DefaultFactory returns the global default FileSystemFactory.
// In WASM, only HTTP/HTTPS schemes are fully supported.
func DefaultFactory() FileSystemFactory {
	defaultFactoryOnce.Do(func() {
		defaultFactory = newFactory()
	})

	return defaultFactory
}

// NewFileSystemFactory creates a new FileSystemFactory with default schemes registered.
func NewFileSystemFactory() FileSystemFactory {
	return newFactory()
}

// newFactory creates a new factory with WASM-compatible schemes.
func newFactory() *factory {
	f := &factory{
		schemes: make(map[string]FileSystemFactoryFunc),
	}

	// Register local filesystem stub (returns error for all operations)
	f.schemes["file"] = f.newLocalFileSystemStub
	f.schemes[""] = f.newLocalFileSystemStub

	// Register HTTP schemes (fully functional in WASM)
	registerHTTPSchemesInternal(f)

	// Register cloud provider stubs (return errors with helpful messages)
	registerCloudSchemesStub(f)

	return f
}

// registerCloudSchemesStub registers stub factories for cloud providers in WASM.
// These return helpful error messages directing users to use HTTP with pre-signed URLs.
func registerCloudSchemesStub(f *factory) {
	s3Stub := func(_ context.Context, _ string) (FileSystem, error) {
		return newWASMCloudFileSystem("s3"), nil
	}
	f.schemes["s3"] = s3Stub
	f.schemes["s3a"] = s3Stub
	f.schemes["s3n"] = s3Stub

	gcsStub := func(_ context.Context, _ string) (FileSystem, error) {
		return newWASMCloudFileSystem("gs"), nil
	}
	f.schemes["gs"] = gcsStub
	f.schemes["gcs"] = gcsStub

	azureStub := func(_ context.Context, _ string) (FileSystem, error) {
		return newWASMCloudFileSystem("azure"), nil
	}
	f.schemes["azure"] = azureStub
	f.schemes["az"] = azureStub
}

// GetFileSystem returns a FileSystem for the given URL.
func (f *factory) GetFileSystem(ctx context.Context, url string) (FileSystem, error) {
	scheme := extractScheme(url)

	f.mu.RLock()
	factoryFunc, ok := f.schemes[scheme]
	f.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unsupported scheme: %s", scheme)
	}

	return factoryFunc(ctx, url)
}

// RegisterScheme registers a factory function for a scheme.
func (f *factory) RegisterScheme(scheme string, factoryFunc FileSystemFactoryFunc) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.schemes[scheme] = factoryFunc
}

// UnregisterScheme removes a registered scheme.
func (f *factory) UnregisterScheme(scheme string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.schemes, scheme)
}

// SupportedSchemes returns a list of supported schemes.
func (f *factory) SupportedSchemes() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	schemes := make([]string, 0, len(f.schemes))
	for scheme := range f.schemes {
		if scheme != "" { // Skip empty scheme (default)
			schemes = append(schemes, scheme)
		}
	}

	return schemes
}

// newLocalFileSystemStub creates a stub local filesystem for WASM.
func (*factory) newLocalFileSystemStub(_ context.Context, _ string) (FileSystem, error) {
	return &wasmLocalFileSystem{}, nil
}

// extractScheme extracts the scheme from a URL.
// Returns an empty string if no scheme is found.
func extractScheme(url string) string {
	if idx := strings.Index(url, "://"); idx >= 0 {
		return strings.ToLower(url[:idx])
	}

	return ""
}

// GetFileSystem is a convenience function that uses the default factory.
func GetFileSystem(ctx context.Context, url string) (FileSystem, error) {
	return DefaultFactory().GetFileSystem(ctx, url)
}

// RegisterScheme is a convenience function that registers a scheme with the default factory.
func RegisterScheme(scheme string, factoryFunc FileSystemFactoryFunc) {
	DefaultFactory().RegisterScheme(scheme, factoryFunc)
}

// IsCloudURL returns true if the URL refers to a cloud storage location.
func IsCloudURL(url string) bool {
	scheme := extractScheme(url)

	switch scheme {
	case "s3", "s3a", "s3n", "gs", "gcs", "azure", "az", "http", "https":
		return true
	default:
		return false
	}
}

// IsLocalURL returns true if the URL refers to a local file.
func IsLocalURL(url string) bool {
	scheme := extractScheme(url)

	return scheme == "" || scheme == "file"
}
