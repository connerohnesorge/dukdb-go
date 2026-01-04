//go:build js && wasm

package filesystem

import (
	"context"
	"testing"
)

// TestWASMInterfacesCompile verifies that core interfaces compile for WASM.
func TestWASMInterfacesCompile(t *testing.T) {
	// Verify FileSystem interface exists and can be used
	var _ FileSystem = (*wasmLocalFileSystem)(nil)
	var _ FileSystem = (*wasmCloudFileSystem)(nil)
	var _ FileSystem = (*HTTPFileSystem)(nil)

	// Verify FileInfo interface exists
	var _ FileInfo = (*HTTPFileInfo)(nil)
}

// TestWASMCompatibleSchemes tests the WASMCompatibleSchemes function.
func TestWASMCompatibleSchemes(t *testing.T) {
	schemes := WASMCompatibleSchemes()
	if len(schemes) != 2 {
		t.Errorf("expected 2 WASM compatible schemes, got %d", len(schemes))
	}

	hasHTTP := false
	hasHTTPS := false
	for _, s := range schemes {
		if s == "http" {
			hasHTTP = true
		}
		if s == "https" {
			hasHTTPS = true
		}
	}

	if !hasHTTP || !hasHTTPS {
		t.Errorf("expected http and https in WASM compatible schemes, got %v", schemes)
	}
}

// TestIsWASMCompatible tests the IsWASMCompatible function.
func TestIsWASMCompatible(t *testing.T) {
	tests := []struct {
		scheme     string
		compatible bool
	}{
		{"http", true},
		{"https", true},
		{"s3", false},
		{"gs", false},
		{"azure", false},
		{"file", false},
		{"", false},
	}

	for _, tt := range tests {
		result := IsWASMCompatible(tt.scheme)
		if result != tt.compatible {
			t.Errorf("IsWASMCompatible(%q) = %v, expected %v", tt.scheme, result, tt.compatible)
		}
	}
}

// TestWASMLocalFileSystemReturnsErrors tests that local filesystem stub returns errors.
func TestWASMLocalFileSystemReturnsErrors(t *testing.T) {
	fs := NewLocalFileSystemWASM()

	// All operations should return ErrWASMNotSupported
	_, err := fs.Open("/test/path")
	if err != ErrWASMNotSupported {
		t.Errorf("Open() error = %v, expected ErrWASMNotSupported", err)
	}

	_, err = fs.Create("/test/path")
	if err != ErrWASMNotSupported {
		t.Errorf("Create() error = %v, expected ErrWASMNotSupported", err)
	}

	err = fs.MkdirAll("/test/path")
	if err != ErrWASMNotSupported {
		t.Errorf("MkdirAll() error = %v, expected ErrWASMNotSupported", err)
	}

	_, err = fs.Stat("/test/path")
	if err != ErrWASMNotSupported {
		t.Errorf("Stat() error = %v, expected ErrWASMNotSupported", err)
	}

	err = fs.Remove("/test/path")
	if err != ErrWASMNotSupported {
		t.Errorf("Remove() error = %v, expected ErrWASMNotSupported", err)
	}

	_, err = fs.ReadDir("/test/path")
	if err != ErrWASMNotSupported {
		t.Errorf("ReadDir() error = %v, expected ErrWASMNotSupported", err)
	}

	_, err = fs.Exists("/test/path")
	if err != ErrWASMNotSupported {
		t.Errorf("Exists() error = %v, expected ErrWASMNotSupported", err)
	}

	// URI should still work
	uri := fs.URI()
	if uri != "file://" {
		t.Errorf("URI() = %q, expected \"file://\"", uri)
	}

	// Capabilities should indicate no support
	caps := fs.Capabilities()
	if caps.SupportsSeek || caps.SupportsWrite || caps.SupportsDelete {
		t.Errorf("Capabilities should indicate no support in WASM")
	}
}

// TestWASMCloudFileSystemReturnsErrors tests that cloud filesystem stubs return errors.
func TestWASMCloudFileSystemReturnsErrors(t *testing.T) {
	fs := newWASMCloudFileSystem("s3")

	_, err := fs.Open("s3://bucket/key")
	if err != ErrWASMNotSupported {
		t.Errorf("Open() error = %v, expected ErrWASMNotSupported", err)
	}

	uri := fs.URI()
	if uri != "s3://" {
		t.Errorf("URI() = %q, expected \"s3://\"", uri)
	}
}

// TestNewWASMFileSystem tests the NewWASMFileSystem function.
func TestNewWASMFileSystem(t *testing.T) {
	ctx := context.Background()

	// HTTP should return a fully functional filesystem
	httpFS, err := NewWASMFileSystem(ctx, "http")
	if err != nil {
		t.Fatalf("NewWASMFileSystem(http) error = %v", err)
	}
	if _, ok := httpFS.(*HTTPFileSystem); !ok {
		t.Errorf("NewWASMFileSystem(http) should return *HTTPFileSystem")
	}

	// HTTPS should also work
	httpsFS, err := NewWASMFileSystem(ctx, "https")
	if err != nil {
		t.Fatalf("NewWASMFileSystem(https) error = %v", err)
	}
	if _, ok := httpsFS.(*HTTPFileSystem); !ok {
		t.Errorf("NewWASMFileSystem(https) should return *HTTPFileSystem")
	}

	// File should return stub
	fileFS, err := NewWASMFileSystem(ctx, "file")
	if err != nil {
		t.Fatalf("NewWASMFileSystem(file) error = %v", err)
	}
	if _, ok := fileFS.(*wasmLocalFileSystem); !ok {
		t.Errorf("NewWASMFileSystem(file) should return *wasmLocalFileSystem")
	}

	// S3 should return stub
	s3FS, err := NewWASMFileSystem(ctx, "s3")
	if err != nil {
		t.Fatalf("NewWASMFileSystem(s3) error = %v", err)
	}
	if _, ok := s3FS.(*wasmCloudFileSystem); !ok {
		t.Errorf("NewWASMFileSystem(s3) should return *wasmCloudFileSystem")
	}
}

// TestWASMFactoryDefaultSchemes tests that the WASM factory has expected schemes.
func TestWASMFactoryDefaultSchemes(t *testing.T) {
	factory := NewFileSystemFactory()

	schemes := factory.SupportedSchemes()
	expectedSchemes := map[string]bool{
		"file":  true,
		"http":  true,
		"https": true,
		"s3":    true,
		"s3a":   true,
		"s3n":   true,
		"gs":    true,
		"gcs":   true,
		"azure": true,
		"az":    true,
	}

	for _, s := range schemes {
		if !expectedSchemes[s] {
			t.Errorf("unexpected scheme in WASM factory: %s", s)
		}
	}
}

// TestWASMHTTPFileSystemWorks tests that HTTP filesystem works in WASM.
func TestWASMHTTPFileSystemWorks(t *testing.T) {
	ctx := context.Background()

	fs, err := NewHTTPFileSystem(ctx, DefaultHTTPConfig())
	if err != nil {
		t.Fatalf("NewHTTPFileSystem() error = %v", err)
	}

	// Verify capabilities
	caps := fs.Capabilities()
	if !caps.SupportsSeek {
		t.Error("HTTP filesystem should support seek via range requests")
	}
	if !caps.SupportsRange {
		t.Error("HTTP filesystem should support range requests")
	}
	if !caps.ContextTimeout {
		t.Error("HTTP filesystem should support context timeout")
	}

	// Write operations should not be supported
	if caps.SupportsWrite {
		t.Error("HTTP filesystem should not support write")
	}
	if caps.SupportsDelete {
		t.Error("HTTP filesystem should not support delete")
	}
}
