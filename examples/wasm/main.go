//go:build js && wasm

// Package main provides a WASM entry point for dukdb-go browser testing.
// This module exposes filesystem operations to JavaScript for testing
// the HTTP filesystem and async patterns in a browser environment.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"syscall/js"

	"github.com/dukdb/dukdb-go/internal/io/filesystem"
)

func main() {
	// Register all async functions from the filesystem package
	filesystem.RegisterWASMAsyncFunctions()

	// Register additional test functions
	registerTestFunctions()

	// Keep the Go runtime alive
	fmt.Println("dukdb-go WASM module initialized")
	select {}
}

// registerTestFunctions registers additional test-specific functions for browser testing.
func registerTestFunctions() {
	global := js.Global()

	// Ensure dukdb namespace exists
	dukdb := global.Get("dukdb")
	if dukdb.IsUndefined() || dukdb.IsNull() {
		dukdb = js.Global().Get("Object").New()
		global.Set("dukdb", dukdb)
	}

	// Register version info
	dukdb.Set("version", "0.1.0-wasm")

	// Register test helper for checking WASM compatibility
	dukdb.Set("isWASMCompatible", js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		if len(args) < 1 {
			return false
		}
		scheme := args[0].String()
		return filesystem.IsWASMCompatible(scheme)
	}))

	// Register list of compatible schemes
	dukdb.Set("getCompatibleSchemes", js.FuncOf(func(_ js.Value, _ []js.Value) interface{} {
		schemes := filesystem.WASMCompatibleSchemes()
		arr := js.Global().Get("Array").New(len(schemes))
		for i, scheme := range schemes {
			arr.SetIndex(i, scheme)
		}
		return arr
	}))

	// Register test function for HTTP read
	dukdb.Set("testHTTPRead", js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		if len(args) < 1 {
			return wrapPromiseError(errors.New("testHTTPRead requires a URL argument"))
		}

		url := args[0].String()

		return filesystem.WrapAsPromise(func() (interface{}, error) {
			ctx := context.Background()
			fs, err := filesystem.NewHTTPFileSystem(ctx, filesystem.DefaultHTTPConfig())
			if err != nil {
				return nil, fmt.Errorf("failed to create HTTP filesystem: %w", err)
			}

			file, err := fs.Open(url)
			if err != nil {
				return nil, fmt.Errorf("failed to open URL: %w", err)
			}
			defer func() { _ = file.Close() }()

			// Read up to 64KB
			buf := make([]byte, 65536)
			n, err := file.Read(buf)
			if err != nil && err != io.EOF {
				return nil, fmt.Errorf("failed to read: %w", err)
			}

			return map[string]interface{}{
				"success":     true,
				"bytesRead":   n,
				"dataPreview": string(buf[:min(n, 200)]) + "...",
			}, nil
		})
	}))

	// Register test function for HTTP stat
	dukdb.Set("testHTTPStat", js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		if len(args) < 1 {
			return wrapPromiseError(errors.New("testHTTPStat requires a URL argument"))
		}

		url := args[0].String()

		return filesystem.WrapAsPromise(func() (interface{}, error) {
			ctx := context.Background()
			fs, err := filesystem.NewHTTPFileSystem(ctx, filesystem.DefaultHTTPConfig())
			if err != nil {
				return nil, fmt.Errorf("failed to create HTTP filesystem: %w", err)
			}

			info, err := fs.Stat(url)
			if err != nil {
				return nil, fmt.Errorf("failed to stat URL: %w", err)
			}

			return map[string]interface{}{
				"success": true,
				"name":    info.Name(),
				"size":    info.Size(),
				"isDir":   info.IsDir(),
				"modTime": info.ModTime().UnixMilli(),
			}, nil
		})
	}))

	// Register test function for HTTP range read
	dukdb.Set("testHTTPRangeRead", js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		if len(args) < 3 {
			return wrapPromiseError(
				errors.New("testHTTPRangeRead requires URL, start, and length arguments"),
			)
		}

		url := args[0].String()
		start := int64(args[1].Float())
		length := int(args[2].Float())

		return filesystem.WrapAsPromise(func() (interface{}, error) {
			ctx := context.Background()
			fs, err := filesystem.NewHTTPFileSystem(ctx, filesystem.DefaultHTTPConfig())
			if err != nil {
				return nil, fmt.Errorf("failed to create HTTP filesystem: %w", err)
			}

			file, err := fs.Open(url)
			if err != nil {
				return nil, fmt.Errorf("failed to open URL: %w", err)
			}
			defer func() { _ = file.Close() }()

			// Seek to position
			if seeker, ok := file.(io.Seeker); ok {
				_, err = seeker.Seek(start, io.SeekStart)
				if err != nil {
					return nil, fmt.Errorf("failed to seek: %w", err)
				}
			}

			// Read requested bytes
			buf := make([]byte, length)
			n, err := file.Read(buf)
			if err != nil && err != io.EOF {
				return nil, fmt.Errorf("failed to read: %w", err)
			}

			return map[string]interface{}{
				"success":     true,
				"bytesRead":   n,
				"startPos":    start,
				"dataPreview": string(buf[:min(n, 100)]),
			}, nil
		})
	}))

	// Register test function for fetch API
	dukdb.Set("testFetch", js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		if len(args) < 1 {
			return wrapPromiseError(errors.New("testFetch requires a URL argument"))
		}

		url := args[0].String()

		return filesystem.WrapAsPromise(func() (interface{}, error) {
			ctx := context.Background()
			opts := filesystem.DefaultFetchOptions()
			opts.Method = "GET"

			resp, err := filesystem.FetchWithContext(ctx, url, opts)
			if err != nil {
				return nil, fmt.Errorf("fetch failed: %w", err)
			}

			return map[string]interface{}{
				"success":    resp.OK,
				"status":     resp.Status,
				"statusText": resp.StatusText,
				"bodyLength": len(resp.Body),
				"url":        resp.URL,
			}, nil
		})
	}))

	// Register test function for filesystem factory
	dukdb.Set("testFileSystemFactory", js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		if len(args) < 1 {
			return wrapPromiseError(errors.New("testFileSystemFactory requires a URL argument"))
		}

		url := args[0].String()

		return filesystem.WrapAsPromise(func() (interface{}, error) {
			ctx := context.Background()
			fs, err := filesystem.GetFileSystem(ctx, url)
			if err != nil {
				return nil, fmt.Errorf("failed to get filesystem: %w", err)
			}

			caps := fs.Capabilities()

			return map[string]interface{}{
				"success":         true,
				"uri":             fs.URI(),
				"supportsSeek":    caps.SupportsSeek,
				"supportsRange":   caps.SupportsRange,
				"supportsWrite":   caps.SupportsWrite,
				"supportsDirList": caps.SupportsDirList,
			}, nil
		})
	}))

	// Register run all tests function
	dukdb.Set("runAllTests", js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		testURL := "https://raw.githubusercontent.com/duckdb/duckdb/main/README.md"
		if len(args) > 0 && args[0].Type() == js.TypeString {
			testURL = args[0].String()
		}

		return filesystem.WrapAsPromise(func() (interface{}, error) {
			results := make(map[string]interface{})

			// Test 1: WASM compatibility check
			results["wasmCompatibility"] = map[string]interface{}{
				"http":  filesystem.IsWASMCompatible("http"),
				"https": filesystem.IsWASMCompatible("https"),
				"s3":    filesystem.IsWASMCompatible("s3"),
				"gs":    filesystem.IsWASMCompatible("gs"),
				"azure": filesystem.IsWASMCompatible("azure"),
			}

			// Test 2: Get compatible schemes
			results["compatibleSchemes"] = filesystem.WASMCompatibleSchemes()

			// Test 3: HTTP filesystem stat
			ctx := context.Background()
			fs, err := filesystem.NewHTTPFileSystem(ctx, filesystem.DefaultHTTPConfig())
			if err != nil {
				results["httpStat"] = map[string]interface{}{
					"success": false,
					"error":   err.Error(),
				}
			} else {
				info, err := fs.Stat(testURL)
				if err != nil {
					results["httpStat"] = map[string]interface{}{
						"success": false,
						"error":   err.Error(),
					}
				} else {
					results["httpStat"] = map[string]interface{}{
						"success": true,
						"name":    info.Name(),
						"size":    info.Size(),
					}
				}
			}

			// Test 4: Fetch API
			opts := filesystem.DefaultFetchOptions()
			resp, err := filesystem.FetchWithContext(ctx, testURL, opts)
			if err != nil {
				results["fetchAPI"] = map[string]interface{}{
					"success": false,
					"error":   err.Error(),
				}
			} else {
				results["fetchAPI"] = map[string]interface{}{
					"success":    resp.OK,
					"status":     resp.Status,
					"bodyLength": len(resp.Body),
				}
			}

			return map[string]interface{}{
				"success": true,
				"testURL": testURL,
				"results": results,
			}, nil
		})
	}))

	fmt.Println("Test functions registered")
}

// wrapPromiseError creates a rejected Promise with the given error.
func wrapPromiseError(err error) js.Value {
	return filesystem.WrapAsPromise(func() (interface{}, error) {
		return nil, err
	})
}

// min returns the minimum of two integers.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
