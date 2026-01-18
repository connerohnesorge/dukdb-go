//go:build js && wasm

// Package filesystem provides WASM async support for filesystem operations.
// This file contains helpers for wrapping Go operations as JavaScript Promises
// and working with the browser's async model.
//
// WASM Async Model Overview:
//
// Go's WASM runtime uses a single-threaded execution model by default.
// Long-running operations need to yield to the JavaScript event loop
// to avoid blocking the browser. This file provides utilities for:
//
//   - Wrapping blocking Go functions as JavaScript Promises
//   - Using the JavaScript fetch API for HTTP operations
//   - Providing callback-based and promise-based APIs
//   - Handling context cancellation in async operations
//
// Usage Example:
//
//	// Create an async operation that returns a promise
//	promise := WrapAsPromise(func() (interface{}, error) {
//	    return doSomeWork(), nil
//	})
//
//	// The JavaScript side can await this promise:
//	// const result = await promise;
package filesystem

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"syscall/js"
	"time"
)

// AsyncResult represents the result of an async operation.
type AsyncResult struct {
	// Data is the result data (can be bytes, string, or other types).
	Data interface{}
	// Error is any error that occurred.
	Error error
}

// AsyncCallback is a callback function for async operations.
type AsyncCallback func(result AsyncResult)

// JSPromiseHandler wraps Go async operations as JavaScript Promises.
type JSPromiseHandler struct {
	// mu protects concurrent access.
	mu sync.Mutex
	// pending tracks pending operations for cleanup.
	pending map[int64]context.CancelFunc
	// nextID is the next operation ID.
	nextID int64
}

// NewJSPromiseHandler creates a new JSPromiseHandler.
func NewJSPromiseHandler() *JSPromiseHandler {
	return &JSPromiseHandler{
		pending: make(map[int64]context.CancelFunc),
	}
}

// globalPromiseHandler is the default promise handler.
var (
	globalPromiseHandler     *JSPromiseHandler
	globalPromiseHandlerOnce sync.Once
)

// GetPromiseHandler returns the global promise handler.
func GetPromiseHandler() *JSPromiseHandler {
	globalPromiseHandlerOnce.Do(func() {
		globalPromiseHandler = NewJSPromiseHandler()
	})
	return globalPromiseHandler
}

// WrapAsPromise wraps a blocking Go function as a JavaScript Promise.
// The function runs in a goroutine and resolves/rejects the promise when complete.
//
// Parameters:
//   - fn: A function that returns a result and error
//
// Returns:
//   - A JavaScript Promise that resolves to the result or rejects with the error
//
// Example JavaScript usage:
//
//	const promise = goModule.WrapAsPromise(...);
//	const result = await promise;
func WrapAsPromise(fn func() (interface{}, error)) js.Value {
	// Get the Promise constructor from JavaScript
	promiseConstructor := js.Global().Get("Promise")

	// Create a new Promise
	handler := js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		resolve := args[0]
		reject := args[1]

		// Run the Go function in a goroutine
		go func() {
			result, err := fn()

			if err != nil {
				// Reject with error
				errorObj := js.Global().Get("Error").New(err.Error())
				reject.Invoke(errorObj)
				return
			}

			// Resolve with result
			jsResult := goToJS(result)
			resolve.Invoke(jsResult)
		}()

		return nil
	})

	return promiseConstructor.New(handler)
}

// WrapAsPromiseWithContext wraps a blocking Go function with context as a JavaScript Promise.
// The context can be used for cancellation.
func WrapAsPromiseWithContext(
	ctx context.Context,
	fn func(ctx context.Context) (interface{}, error),
) js.Value {
	promiseConstructor := js.Global().Get("Promise")

	handler := js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		resolve := args[0]
		reject := args[1]

		go func() {
			// Check context before starting
			select {
			case <-ctx.Done():
				errorObj := js.Global().Get("Error").New(ctx.Err().Error())
				reject.Invoke(errorObj)
				return
			default:
			}

			result, err := fn(ctx)

			if err != nil {
				errorObj := js.Global().Get("Error").New(err.Error())
				reject.Invoke(errorObj)
				return
			}

			jsResult := goToJS(result)
			resolve.Invoke(jsResult)
		}()

		return nil
	})

	return promiseConstructor.New(handler)
}

// goToJS converts a Go value to a JavaScript value.
func goToJS(v interface{}) js.Value {
	if v == nil {
		return js.Null()
	}

	switch val := v.(type) {
	case []byte:
		// Convert to Uint8Array
		uint8Array := js.Global().Get("Uint8Array").New(len(val))
		js.CopyBytesToJS(uint8Array, val)
		return uint8Array
	case string:
		return js.ValueOf(val)
	case int:
		return js.ValueOf(val)
	case int64:
		return js.ValueOf(float64(val)) // JS numbers are float64
	case float64:
		return js.ValueOf(val)
	case bool:
		return js.ValueOf(val)
	case map[string]interface{}:
		obj := js.Global().Get("Object").New()
		for k, v := range val {
			obj.Set(k, goToJS(v))
		}
		return obj
	case []interface{}:
		arr := js.Global().Get("Array").New(len(val))
		for i, item := range val {
			arr.SetIndex(i, goToJS(item))
		}
		return arr
	default:
		return js.ValueOf(fmt.Sprintf("%v", v))
	}
}

// jsToGo converts a JavaScript value to a Go value.
func jsToGo(v js.Value) interface{} {
	switch v.Type() {
	case js.TypeNull, js.TypeUndefined:
		return nil
	case js.TypeBoolean:
		return v.Bool()
	case js.TypeNumber:
		return v.Float()
	case js.TypeString:
		return v.String()
	case js.TypeObject:
		// Check if it's an array
		if v.InstanceOf(js.Global().Get("Array")) {
			length := v.Length()
			arr := make([]interface{}, length)
			for i := 0; i < length; i++ {
				arr[i] = jsToGo(v.Index(i))
			}
			return arr
		}
		// Check if it's a Uint8Array
		if v.InstanceOf(js.Global().Get("Uint8Array")) {
			length := v.Length()
			bytes := make([]byte, length)
			js.CopyBytesToGo(bytes, v)
			return bytes
		}
		// Check if it's an ArrayBuffer
		if v.InstanceOf(js.Global().Get("ArrayBuffer")) {
			uint8Array := js.Global().Get("Uint8Array").New(v)
			length := uint8Array.Length()
			bytes := make([]byte, length)
			js.CopyBytesToGo(bytes, uint8Array)
			return bytes
		}
		// Regular object - convert to map
		obj := make(map[string]interface{})
		keys := js.Global().Get("Object").Call("keys", v)
		for i := 0; i < keys.Length(); i++ {
			key := keys.Index(i).String()
			obj[key] = jsToGo(v.Get(key))
		}
		return obj
	default:
		return nil
	}
}

// FetchOptions contains options for the fetch API.
type FetchOptions struct {
	// Method is the HTTP method (GET, POST, etc.)
	Method string
	// Headers are the HTTP headers.
	Headers map[string]string
	// Body is the request body (for POST, PUT, etc.)
	Body []byte
	// Mode is the CORS mode ("cors", "no-cors", "same-origin")
	Mode string
	// Credentials controls credentials ("omit", "same-origin", "include")
	Credentials string
	// Cache controls caching ("default", "no-store", "reload", etc.)
	Cache string
}

// DefaultFetchOptions returns default fetch options.
func DefaultFetchOptions() FetchOptions {
	return FetchOptions{
		Method:      "GET",
		Headers:     make(map[string]string),
		Mode:        "cors",
		Credentials: "same-origin",
		Cache:       "default",
	}
}

// FetchResponse represents a response from the fetch API.
type FetchResponse struct {
	// Status is the HTTP status code.
	Status int
	// StatusText is the HTTP status text.
	StatusText string
	// Headers are the response headers.
	Headers map[string]string
	// Body is the response body.
	Body []byte
	// OK indicates if the response was successful (status 200-299).
	OK bool
	// URL is the final URL after any redirects.
	URL string
}

// FetchWithContext performs an HTTP request using the JavaScript fetch API.
// This is the recommended way to make HTTP requests in WASM as it properly
// integrates with the browser's async model and handles CORS.
//
// Parameters:
//   - ctx: Context for cancellation
//   - url: The URL to fetch
//   - opts: Fetch options (method, headers, body, etc.)
//
// Returns:
//   - FetchResponse with status, headers, and body
//   - error if the fetch failed
func FetchWithContext(ctx context.Context, url string, opts FetchOptions) (*FetchResponse, error) {
	// Create abort controller for cancellation
	abortController := js.Global().Get("AbortController").New()

	// Set up context cancellation
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			abortController.Call("abort")
		case <-done:
		}
	}()
	defer close(done)

	// Build fetch options
	fetchOpts := js.Global().Get("Object").New()
	fetchOpts.Set("method", opts.Method)
	fetchOpts.Set("signal", abortController.Get("signal"))

	if opts.Mode != "" {
		fetchOpts.Set("mode", opts.Mode)
	}
	if opts.Credentials != "" {
		fetchOpts.Set("credentials", opts.Credentials)
	}
	if opts.Cache != "" {
		fetchOpts.Set("cache", opts.Cache)
	}

	// Set headers
	if len(opts.Headers) > 0 {
		headers := js.Global().Get("Headers").New()
		for key, value := range opts.Headers {
			headers.Call("append", key, value)
		}
		fetchOpts.Set("headers", headers)
	}

	// Set body for non-GET requests
	if len(opts.Body) > 0 && opts.Method != "GET" && opts.Method != "HEAD" {
		uint8Array := js.Global().Get("Uint8Array").New(len(opts.Body))
		js.CopyBytesToJS(uint8Array, opts.Body)
		fetchOpts.Set("body", uint8Array)
	}

	// Perform fetch
	resultChan := make(chan *FetchResponse, 1)
	errChan := make(chan error, 1)

	// Use Promise.prototype.then to handle the async result
	fetchPromise := js.Global().Call("fetch", url, fetchOpts)

	// Create the then callback
	thenCallback := js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		response := args[0]

		// Extract response metadata
		resp := &FetchResponse{
			Status:     response.Get("status").Int(),
			StatusText: response.Get("statusText").String(),
			OK:         response.Get("ok").Bool(),
			URL:        response.Get("url").String(),
			Headers:    make(map[string]string),
		}

		// Extract headers
		headers := response.Get("headers")
		if !headers.IsUndefined() && !headers.IsNull() {
			// Use entries() to iterate headers
			entries := headers.Call("entries")
			for {
				next := entries.Call("next")
				if next.Get("done").Bool() {
					break
				}
				pair := next.Get("value")
				key := pair.Index(0).String()
				value := pair.Index(1).String()
				resp.Headers[key] = value
			}
		}

		// Get body as ArrayBuffer and convert to bytes
		arrayBufferPromise := response.Call("arrayBuffer")
		bodyCallback := js.FuncOf(func(_ js.Value, bodyArgs []js.Value) interface{} {
			arrayBuffer := bodyArgs[0]
			uint8Array := js.Global().Get("Uint8Array").New(arrayBuffer)
			length := uint8Array.Length()
			resp.Body = make([]byte, length)
			js.CopyBytesToGo(resp.Body, uint8Array)
			resultChan <- resp
			return nil
		})
		defer bodyCallback.Release()

		bodyErrorCallback := js.FuncOf(func(_ js.Value, errArgs []js.Value) interface{} {
			errChan <- fmt.Errorf("failed to read response body: %s", errArgs[0].Get("message").String())
			return nil
		})
		defer bodyErrorCallback.Release()

		arrayBufferPromise.Call("then", bodyCallback).Call("catch", bodyErrorCallback)

		return nil
	})
	defer thenCallback.Release()

	catchCallback := js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		errMsg := "fetch failed"
		if len(args) > 0 {
			if msg := args[0].Get("message"); !msg.IsUndefined() {
				errMsg = msg.String()
			}
		}
		errChan <- errors.New(errMsg)
		return nil
	})
	defer catchCallback.Release()

	fetchPromise.Call("then", thenCallback).Call("catch", catchCallback)

	// Wait for result or context cancellation
	select {
	case resp := <-resultChan:
		return resp, nil
	case err := <-errChan:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// AsyncHTTPFile provides async file operations for HTTP resources in WASM.
// It wraps HTTPFile with async-friendly methods that return Promises.
type AsyncHTTPFile struct {
	// file is the underlying HTTP file.
	file *HTTPFile
	// mu protects concurrent access.
	mu sync.Mutex
}

// NewAsyncHTTPFile creates a new AsyncHTTPFile from an HTTPFile.
func NewAsyncHTTPFile(file *HTTPFile) *AsyncHTTPFile {
	return &AsyncHTTPFile{file: file}
}

// AsyncRead reads data asynchronously and returns a Promise.
// The Promise resolves to a Uint8Array containing the data.
func (f *AsyncHTTPFile) AsyncRead(length int) js.Value {
	return WrapAsPromise(func() (interface{}, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		buf := make([]byte, length)
		n, err := f.file.Read(buf)
		if err != nil && err != io.EOF {
			return nil, err
		}
		return buf[:n], nil
	})
}

// AsyncReadAt reads data at a specific offset asynchronously.
// Returns a Promise that resolves to a Uint8Array.
func (f *AsyncHTTPFile) AsyncReadAt(length int, offset int64) js.Value {
	return WrapAsPromise(func() (interface{}, error) {
		buf := make([]byte, length)
		n, err := f.file.ReadAt(buf, offset)
		if err != nil && err != io.EOF {
			return nil, err
		}
		return buf[:n], nil
	})
}

// AsyncStat returns file info asynchronously.
// Returns a Promise that resolves to an object with file metadata.
func (f *AsyncHTTPFile) AsyncStat() js.Value {
	return WrapAsPromise(func() (interface{}, error) {
		info, err := f.file.Stat()
		if err != nil {
			return nil, err
		}

		return map[string]interface{}{
			"name":    info.Name(),
			"size":    info.Size(),
			"isDir":   info.IsDir(),
			"modTime": info.ModTime().UnixMilli(),
		}, nil
	})
}

// AsyncClose closes the file asynchronously.
// Returns a Promise that resolves when the file is closed.
func (f *AsyncHTTPFile) AsyncClose() js.Value {
	return WrapAsPromise(func() (interface{}, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		err := f.file.Close()
		return nil, err
	})
}

// AsyncSeek seeks to a position asynchronously.
// Returns a Promise that resolves to the new position.
func (f *AsyncHTTPFile) AsyncSeek(offset int64, whence int) js.Value {
	return WrapAsPromise(func() (interface{}, error) {
		f.mu.Lock()
		defer f.mu.Unlock()

		pos, err := f.file.Seek(offset, whence)
		return pos, err
	})
}

// AsyncHTTPFileSystem provides async filesystem operations for WASM.
type AsyncHTTPFileSystem struct {
	// fs is the underlying HTTP filesystem.
	fs *HTTPFileSystem
}

// NewAsyncHTTPFileSystem creates a new AsyncHTTPFileSystem.
func NewAsyncHTTPFileSystem(fs *HTTPFileSystem) *AsyncHTTPFileSystem {
	return &AsyncHTTPFileSystem{fs: fs}
}

// AsyncOpen opens a file asynchronously.
// Returns a Promise that resolves to an AsyncHTTPFile.
func (afs *AsyncHTTPFileSystem) AsyncOpen(path string) js.Value {
	return WrapAsPromise(func() (interface{}, error) {
		file, err := afs.fs.Open(path)
		if err != nil {
			return nil, err
		}

		httpFile, ok := file.(*HTTPFile)
		if !ok {
			return nil, errors.New("expected HTTPFile")
		}

		// Return an object with async methods
		asyncFile := NewAsyncHTTPFile(httpFile)
		return createAsyncFileJS(asyncFile), nil
	})
}

// AsyncStat returns file info asynchronously.
// Returns a Promise that resolves to file metadata.
func (afs *AsyncHTTPFileSystem) AsyncStat(path string) js.Value {
	return WrapAsPromise(func() (interface{}, error) {
		info, err := afs.fs.Stat(path)
		if err != nil {
			return nil, err
		}

		return map[string]interface{}{
			"name":    info.Name(),
			"size":    info.Size(),
			"isDir":   info.IsDir(),
			"modTime": info.ModTime().UnixMilli(),
		}, nil
	})
}

// AsyncExists checks if a path exists asynchronously.
// Returns a Promise that resolves to a boolean.
func (afs *AsyncHTTPFileSystem) AsyncExists(path string) js.Value {
	return WrapAsPromise(func() (interface{}, error) {
		exists, err := afs.fs.Exists(path)
		return exists, err
	})
}

// createAsyncFileJS creates a JavaScript object representing an AsyncHTTPFile.
func createAsyncFileJS(f *AsyncHTTPFile) interface{} {
	// Return a map that will be converted to a JS object
	// The JS object will have methods that return Promises
	return map[string]interface{}{
		"read": js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
			length := 8192 // Default buffer size
			if len(args) > 0 && args[0].Type() == js.TypeNumber {
				length = args[0].Int()
			}
			return f.AsyncRead(length)
		}),
		"readAt": js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
			if len(args) < 2 {
				return WrapAsPromise(func() (interface{}, error) {
					return nil, errors.New("readAt requires length and offset arguments")
				})
			}
			length := args[0].Int()
			offset := int64(args[1].Float())
			return f.AsyncReadAt(length, offset)
		}),
		"stat": js.FuncOf(func(_ js.Value, _ []js.Value) interface{} {
			return f.AsyncStat()
		}),
		"close": js.FuncOf(func(_ js.Value, _ []js.Value) interface{} {
			return f.AsyncClose()
		}),
		"seek": js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
			if len(args) < 2 {
				return WrapAsPromise(func() (interface{}, error) {
					return nil, errors.New("seek requires offset and whence arguments")
				})
			}
			offset := int64(args[0].Float())
			whence := args[1].Int()
			return f.AsyncSeek(offset, whence)
		}),
	}
}

// WASMHTTPClient provides a WASM-compatible HTTP client using the fetch API.
// This is useful when the standard net/http client doesn't work well in WASM.
type WASMHTTPClient struct {
	// defaultHeaders are headers applied to all requests.
	defaultHeaders map[string]string
	// timeout is the default timeout for requests.
	timeout time.Duration
}

// NewWASMHTTPClient creates a new WASM HTTP client.
func NewWASMHTTPClient() *WASMHTTPClient {
	return &WASMHTTPClient{
		defaultHeaders: map[string]string{
			"User-Agent": DefaultHTTPUserAgent,
		},
		timeout: DefaultHTTPTimeout,
	}
}

// SetHeader sets a default header for all requests.
func (c *WASMHTTPClient) SetHeader(key, value string) {
	c.defaultHeaders[key] = value
}

// SetTimeout sets the default timeout for requests.
func (c *WASMHTTPClient) SetTimeout(timeout time.Duration) {
	c.timeout = timeout
}

// Get performs an async GET request.
// Returns a Promise that resolves to FetchResponse.
func (c *WASMHTTPClient) Get(url string) js.Value {
	return WrapAsPromise(func() (interface{}, error) {
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()

		opts := DefaultFetchOptions()
		opts.Method = "GET"
		opts.Headers = c.defaultHeaders

		resp, err := FetchWithContext(ctx, url, opts)
		if err != nil {
			return nil, err
		}

		return map[string]interface{}{
			"status":     resp.Status,
			"statusText": resp.StatusText,
			"ok":         resp.OK,
			"url":        resp.URL,
			"body":       resp.Body,
			"headers":    resp.Headers,
		}, nil
	})
}

// Head performs an async HEAD request.
// Returns a Promise that resolves to FetchResponse (without body).
func (c *WASMHTTPClient) Head(url string) js.Value {
	return WrapAsPromise(func() (interface{}, error) {
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()

		opts := DefaultFetchOptions()
		opts.Method = "HEAD"
		opts.Headers = c.defaultHeaders

		resp, err := FetchWithContext(ctx, url, opts)
		if err != nil {
			return nil, err
		}

		return map[string]interface{}{
			"status":     resp.Status,
			"statusText": resp.StatusText,
			"ok":         resp.OK,
			"url":        resp.URL,
			"headers":    resp.Headers,
		}, nil
	})
}

// GetRange performs an async GET request with a Range header.
// Returns a Promise that resolves to FetchResponse with partial content.
func (c *WASMHTTPClient) GetRange(url string, start, end int64) js.Value {
	return WrapAsPromise(func() (interface{}, error) {
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()

		opts := DefaultFetchOptions()
		opts.Method = "GET"
		opts.Headers = make(map[string]string)
		for k, v := range c.defaultHeaders {
			opts.Headers[k] = v
		}
		opts.Headers["Range"] = fmt.Sprintf("bytes=%d-%d", start, end)

		resp, err := FetchWithContext(ctx, url, opts)
		if err != nil {
			return nil, err
		}

		return map[string]interface{}{
			"status":     resp.Status,
			"statusText": resp.StatusText,
			"ok":         resp.OK,
			"url":        resp.URL,
			"body":       resp.Body,
			"headers":    resp.Headers,
		}, nil
	})
}

// RegisterWASMAsyncFunctions registers async filesystem functions for JavaScript access.
// Call this from your main.go to expose async functions to JavaScript.
//
// Example:
//
//	func main() {
//	    filesystem.RegisterWASMAsyncFunctions()
//	    // Keep the Go runtime alive
//	    select {}
//	}
//
// JavaScript usage:
//
//	const file = await dukdb.openHTTPFile("https://example.com/data.csv");
//	const data = await file.read(1024);
func RegisterWASMAsyncFunctions() {
	global := js.Global()

	// Ensure dukdb namespace exists
	dukdb := global.Get("dukdb")
	if dukdb.IsUndefined() || dukdb.IsNull() {
		dukdb = js.Global().Get("Object").New()
		global.Set("dukdb", dukdb)
	}

	// Register fetch helper
	dukdb.Set("fetch", js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		if len(args) < 1 {
			return WrapAsPromise(func() (interface{}, error) {
				return nil, errors.New("fetch requires a URL argument")
			})
		}

		url := args[0].String()
		opts := DefaultFetchOptions()

		// Parse options if provided
		if len(args) > 1 && !args[1].IsUndefined() && !args[1].IsNull() {
			jsOpts := args[1]
			if method := jsOpts.Get("method"); !method.IsUndefined() {
				opts.Method = method.String()
			}
			if headers := jsOpts.Get("headers"); !headers.IsUndefined() {
				opts.Headers = make(map[string]string)
				keys := js.Global().Get("Object").Call("keys", headers)
				for i := 0; i < keys.Length(); i++ {
					key := keys.Index(i).String()
					opts.Headers[key] = headers.Get(key).String()
				}
			}
		}

		return WrapAsPromise(func() (interface{}, error) {
			ctx, cancel := context.WithTimeout(context.Background(), DefaultHTTPTimeout)
			defer cancel()

			resp, err := FetchWithContext(ctx, url, opts)
			if err != nil {
				return nil, err
			}

			return map[string]interface{}{
				"status":     resp.Status,
				"statusText": resp.StatusText,
				"ok":         resp.OK,
				"url":        resp.URL,
				"body":       resp.Body,
				"headers":    resp.Headers,
			}, nil
		})
	}))

	// Register HTTP filesystem creator
	dukdb.Set("createHTTPFileSystem", js.FuncOf(func(_ js.Value, _ []js.Value) interface{} {
		return WrapAsPromise(func() (interface{}, error) {
			fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
			if err != nil {
				return nil, err
			}

			asyncFS := NewAsyncHTTPFileSystem(fs)

			return map[string]interface{}{
				"open": js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
					if len(args) < 1 {
						return WrapAsPromise(func() (interface{}, error) {
							return nil, errors.New("open requires a path argument")
						})
					}
					return asyncFS.AsyncOpen(args[0].String())
				}),
				"stat": js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
					if len(args) < 1 {
						return WrapAsPromise(func() (interface{}, error) {
							return nil, errors.New("stat requires a path argument")
						})
					}
					return asyncFS.AsyncStat(args[0].String())
				}),
				"exists": js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
					if len(args) < 1 {
						return WrapAsPromise(func() (interface{}, error) {
							return nil, errors.New("exists requires a path argument")
						})
					}
					return asyncFS.AsyncExists(args[0].String())
				}),
			}, nil
		})
	}))

	// Register convenience function for opening HTTP files
	dukdb.Set("openHTTPFile", js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		if len(args) < 1 {
			return WrapAsPromise(func() (interface{}, error) {
				return nil, errors.New("openHTTPFile requires a URL argument")
			})
		}

		url := args[0].String()

		return WrapAsPromise(func() (interface{}, error) {
			fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
			if err != nil {
				return nil, err
			}

			file, err := fs.Open(url)
			if err != nil {
				return nil, err
			}

			httpFile, ok := file.(*HTTPFile)
			if !ok {
				return nil, errors.New("expected HTTPFile")
			}

			asyncFile := NewAsyncHTTPFile(httpFile)
			return createAsyncFileJS(asyncFile), nil
		})
	}))
}

// Yield yields control to the JavaScript event loop.
// This is useful for long-running operations to prevent blocking the browser.
func Yield() {
	// Create a resolved Promise and wait for its microtask
	done := make(chan struct{})
	callback := js.FuncOf(func(_ js.Value, _ []js.Value) interface{} {
		close(done)
		return nil
	})
	defer callback.Release()

	js.Global().Get("Promise").Call("resolve").Call("then", callback)
	<-done
}

// Sleep pauses execution for the specified duration, yielding to the event loop.
func Sleep(d time.Duration) {
	done := make(chan struct{})
	callback := js.FuncOf(func(_ js.Value, _ []js.Value) interface{} {
		close(done)
		return nil
	})
	defer callback.Release()

	js.Global().Call("setTimeout", callback, int(d.Milliseconds()))
	<-done
}
