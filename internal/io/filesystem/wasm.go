//go:build js && wasm

// Package filesystem provides WASM-specific stubs and documentation for cloud storage.
//
// WASM Compatibility Notes:
//
// The filesystem package is designed to be WASM-compatible with the following caveats:
//
// Fully Supported in WASM:
//   - FileSystem interface and types (filesystem.go)
//   - HTTP filesystem (http_filesystem.go) - uses fetch API in browser
//   - Async patterns (wasm_async.go) - Promise-based APIs for JavaScript
//   - Retry logic (retry.go)
//   - URL parsing utilities
//
// Not Supported in WASM:
//   - Local filesystem (local.go) - browsers cannot access local files
//   - S3 filesystem (s3_*.go) - minio-go SDK uses os/user which is unavailable
//   - GCS filesystem (gcs_*.go) - GCS SDK uses os/user which is unavailable
//   - Azure filesystem (azure_*.go) - Azure SDK uses memory-mapped files
//
// Workarounds:
//   - For local files: Use File input elements or drag-and-drop in the browser
//   - For cloud storage: Use pre-signed URLs via HTTP filesystem
//   - For S3: Generate pre-signed URLs server-side and use HTTP filesystem
//   - For GCS: Generate signed URLs server-side and use HTTP filesystem
//   - For Azure: Generate SAS URLs server-side and use HTTP filesystem
//
// Async Patterns:
//
// For proper integration with JavaScript's async model, use the async helpers:
//
//	// Register async functions for JavaScript access
//	filesystem.RegisterWASMAsyncFunctions()
//
//	// From JavaScript:
//	// const fs = await dukdb.createHTTPFileSystem();
//	// const file = await fs.open("https://example.com/data.csv");
//	// const data = await file.read(1024);
//	// await file.close();
//
// The async helpers provide:
//   - WrapAsPromise: Wrap any Go function as a JavaScript Promise
//   - FetchWithContext: Use JavaScript fetch API with Go context
//   - AsyncHTTPFile: Async file operations returning Promises
//   - AsyncHTTPFileSystem: Async filesystem operations returning Promises
//   - Yield/Sleep: Cooperatively yield to the JS event loop
//
// Example WASM Usage (synchronous):
//
//	// Using HTTP filesystem with pre-signed S3 URL
//	fs, _ := filesystem.NewHTTPFileSystem(ctx, filesystem.DefaultHTTPConfig())
//	file, _ := fs.Open("https://bucket.s3.amazonaws.com/key?X-Amz-Signature=...")
//
//	// Reading the file
//	data, _ := io.ReadAll(file)
//
// Example WASM Usage (asynchronous):
//
//	// In your main.go:
//	func main() {
//	    filesystem.RegisterWASMAsyncFunctions()
//	    select {} // Keep runtime alive
//	}
//
//	// In JavaScript:
//	// const file = await dukdb.openHTTPFile("https://example.com/data.csv");
//	// const data = await file.read(8192);
package filesystem

import (
	"context"
	"errors"
)

// ErrWASMNotSupported is returned when an operation is not supported in WASM.
var ErrWASMNotSupported = errors.New("operation not supported in WASM environment")

// wasmLocalFileSystem is a stub for local filesystem in WASM.
// It always returns ErrWASMNotSupported for all operations.
type wasmLocalFileSystem struct{}

// NewLocalFileSystemWASM creates a stub local filesystem for WASM.
// All operations will return ErrWASMNotSupported.
func NewLocalFileSystemWASM() FileSystem {
	return &wasmLocalFileSystem{}
}

func (*wasmLocalFileSystem) Open(_ string) (File, error) {
	return nil, ErrWASMNotSupported
}

func (*wasmLocalFileSystem) Create(_ string) (File, error) {
	return nil, ErrWASMNotSupported
}

func (*wasmLocalFileSystem) MkdirAll(_ string) error {
	return ErrWASMNotSupported
}

func (*wasmLocalFileSystem) Stat(_ string) (FileInfo, error) {
	return nil, ErrWASMNotSupported
}

func (*wasmLocalFileSystem) Remove(_ string) error {
	return ErrWASMNotSupported
}

func (*wasmLocalFileSystem) RemoveDir(_ string) error {
	return ErrWASMNotSupported
}

func (*wasmLocalFileSystem) ReadDir(_ string) ([]DirEntry, error) {
	return nil, ErrWASMNotSupported
}

func (*wasmLocalFileSystem) Exists(_ string) (bool, error) {
	return false, ErrWASMNotSupported
}

func (*wasmLocalFileSystem) URI() string {
	return "file://"
}

func (*wasmLocalFileSystem) Capabilities() FileSystemCapabilities {
	return FileSystemCapabilities{
		SupportsSeek:    false,
		SupportsAppend:  false,
		SupportsRange:   false,
		SupportsDirList: false,
		SupportsWrite:   false,
		SupportsDelete:  false,
		ContextTimeout:  false,
	}
}

// Verify wasmLocalFileSystem implements FileSystem interface.
var _ FileSystem = (*wasmLocalFileSystem)(nil)

// wasmCloudFileSystem is a stub for cloud filesystems (S3, GCS, Azure) in WASM.
type wasmCloudFileSystem struct {
	scheme string
}

// newWASMCloudFileSystem creates a stub cloud filesystem for WASM.
func newWASMCloudFileSystem(scheme string) FileSystem {
	return &wasmCloudFileSystem{scheme: scheme}
}

func (fs *wasmCloudFileSystem) Open(_ string) (File, error) {
	return nil, ErrWASMNotSupported
}

func (fs *wasmCloudFileSystem) Create(_ string) (File, error) {
	return nil, ErrWASMNotSupported
}

func (fs *wasmCloudFileSystem) MkdirAll(_ string) error {
	return ErrWASMNotSupported
}

func (fs *wasmCloudFileSystem) Stat(_ string) (FileInfo, error) {
	return nil, ErrWASMNotSupported
}

func (fs *wasmCloudFileSystem) Remove(_ string) error {
	return ErrWASMNotSupported
}

func (fs *wasmCloudFileSystem) RemoveDir(_ string) error {
	return ErrWASMNotSupported
}

func (fs *wasmCloudFileSystem) ReadDir(_ string) ([]DirEntry, error) {
	return nil, ErrWASMNotSupported
}

func (fs *wasmCloudFileSystem) Exists(_ string) (bool, error) {
	return false, ErrWASMNotSupported
}

func (fs *wasmCloudFileSystem) URI() string {
	return fs.scheme + "://"
}

func (fs *wasmCloudFileSystem) Capabilities() FileSystemCapabilities {
	return FileSystemCapabilities{
		SupportsSeek:    false,
		SupportsAppend:  false,
		SupportsRange:   false,
		SupportsDirList: false,
		SupportsWrite:   false,
		SupportsDelete:  false,
		ContextTimeout:  false,
	}
}

// Verify wasmCloudFileSystem implements FileSystem interface.
var _ FileSystem = (*wasmCloudFileSystem)(nil)

// WASMCompatibleSchemes returns the list of schemes that are fully functional in WASM.
func WASMCompatibleSchemes() []string {
	return []string{"http", "https"}
}

// IsWASMCompatible returns true if the given URL scheme is fully supported in WASM.
func IsWASMCompatible(scheme string) bool {
	switch scheme {
	case "http", "https":
		return true
	default:
		return false
	}
}

// NewWASMFileSystem creates an appropriate filesystem for a given scheme in WASM.
// For http/https, returns a fully functional HTTPFileSystem.
// For other schemes, returns a stub that returns ErrWASMNotSupported.
func NewWASMFileSystem(ctx context.Context, scheme string) (FileSystem, error) {
	switch scheme {
	case "http", "https":
		return NewHTTPFileSystem(ctx, DefaultHTTPConfig())
	case "file", "":
		return &wasmLocalFileSystem{}, nil
	case "s3", "s3a", "s3n":
		return newWASMCloudFileSystem("s3"), nil
	case "gs", "gcs":
		return newWASMCloudFileSystem("gs"), nil
	case "azure", "az":
		return newWASMCloudFileSystem("azure"), nil
	default:
		return nil, ErrWASMNotSupported
	}
}
