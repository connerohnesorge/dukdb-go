//go:build js && wasm

package filesystem

import (
	"context"
	"syscall/js"
	"testing"
	"time"
)

// TestGoToJSConversion tests the goToJS conversion function.
func TestGoToJSConversion(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		checkFn  func(js.Value) bool
		typeName string
	}{
		{
			name:     "nil",
			input:    nil,
			checkFn:  func(v js.Value) bool { return v.IsNull() },
			typeName: "null",
		},
		{
			name:     "string",
			input:    "hello",
			checkFn:  func(v js.Value) bool { return v.String() == "hello" },
			typeName: "string",
		},
		{
			name:     "int",
			input:    42,
			checkFn:  func(v js.Value) bool { return v.Int() == 42 },
			typeName: "number",
		},
		{
			name:     "int64",
			input:    int64(9999999999),
			checkFn:  func(v js.Value) bool { return int64(v.Float()) == 9999999999 },
			typeName: "number",
		},
		{
			name:     "float64",
			input:    3.14159,
			checkFn:  func(v js.Value) bool { return v.Float() == 3.14159 },
			typeName: "number",
		},
		{
			name:     "bool_true",
			input:    true,
			checkFn:  func(v js.Value) bool { return v.Bool() == true },
			typeName: "boolean",
		},
		{
			name:     "bool_false",
			input:    false,
			checkFn:  func(v js.Value) bool { return v.Bool() == false },
			typeName: "boolean",
		},
		{
			name:  "bytes",
			input: []byte{1, 2, 3, 4, 5},
			checkFn: func(v js.Value) bool {
				if !v.InstanceOf(js.Global().Get("Uint8Array")) {
					return false
				}
				if v.Length() != 5 {
					return false
				}
				return v.Index(0).Int() == 1 && v.Index(4).Int() == 5
			},
			typeName: "Uint8Array",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := goToJS(tt.input)
			if !tt.checkFn(result) {
				t.Errorf("goToJS(%v) conversion failed for type %s", tt.input, tt.typeName)
			}
		})
	}
}

// TestGoToJSMap tests conversion of Go maps to JS objects.
func TestGoToJSMap(t *testing.T) {
	input := map[string]interface{}{
		"name":  "test",
		"count": 42,
		"valid": true,
	}

	result := goToJS(input)

	if result.Type() != js.TypeObject {
		t.Errorf("expected object, got %v", result.Type())
	}

	if result.Get("name").String() != "test" {
		t.Errorf("expected name='test', got %v", result.Get("name").String())
	}

	if result.Get("count").Int() != 42 {
		t.Errorf("expected count=42, got %v", result.Get("count").Int())
	}

	if !result.Get("valid").Bool() {
		t.Errorf("expected valid=true, got %v", result.Get("valid").Bool())
	}
}

// TestGoToJSArray tests conversion of Go slices to JS arrays.
func TestGoToJSArray(t *testing.T) {
	input := []interface{}{"a", "b", "c"}

	result := goToJS(input)

	if !result.InstanceOf(js.Global().Get("Array")) {
		t.Error("expected Array")
	}

	if result.Length() != 3 {
		t.Errorf("expected length 3, got %d", result.Length())
	}

	if result.Index(0).String() != "a" {
		t.Errorf("expected first element 'a', got %s", result.Index(0).String())
	}
}

// TestJsToGoConversion tests the jsToGo conversion function.
func TestJsToGoConversion(t *testing.T) {
	// Test string
	strVal := js.ValueOf("hello")
	if jsToGo(strVal) != "hello" {
		t.Error("string conversion failed")
	}

	// Test number
	numVal := js.ValueOf(42.5)
	if jsToGo(numVal) != 42.5 {
		t.Error("number conversion failed")
	}

	// Test boolean
	boolVal := js.ValueOf(true)
	if jsToGo(boolVal) != true {
		t.Error("boolean conversion failed")
	}

	// Test null
	nullVal := js.Null()
	if jsToGo(nullVal) != nil {
		t.Error("null conversion failed")
	}

	// Test undefined
	undefinedVal := js.Undefined()
	if jsToGo(undefinedVal) != nil {
		t.Error("undefined conversion failed")
	}
}

// TestJsToGoArray tests conversion of JS arrays to Go slices.
func TestJsToGoArray(t *testing.T) {
	arr := js.Global().Get("Array").New(3)
	arr.SetIndex(0, "x")
	arr.SetIndex(1, "y")
	arr.SetIndex(2, "z")

	result := jsToGo(arr)
	slice, ok := result.([]interface{})
	if !ok {
		t.Fatal("expected []interface{}")
	}

	if len(slice) != 3 {
		t.Errorf("expected length 3, got %d", len(slice))
	}

	if slice[0] != "x" || slice[1] != "y" || slice[2] != "z" {
		t.Errorf("unexpected array values: %v", slice)
	}
}

// TestJsToGoUint8Array tests conversion of JS Uint8Array to Go []byte.
func TestJsToGoUint8Array(t *testing.T) {
	data := []byte{10, 20, 30, 40, 50}
	uint8Array := js.Global().Get("Uint8Array").New(len(data))
	js.CopyBytesToJS(uint8Array, data)

	result := jsToGo(uint8Array)
	bytes, ok := result.([]byte)
	if !ok {
		t.Fatal("expected []byte")
	}

	if len(bytes) != 5 {
		t.Errorf("expected length 5, got %d", len(bytes))
	}

	for i, b := range data {
		if bytes[i] != b {
			t.Errorf("byte mismatch at index %d: expected %d, got %d", i, b, bytes[i])
		}
	}
}

// TestDefaultFetchOptions tests that default options are reasonable.
func TestDefaultFetchOptions(t *testing.T) {
	opts := DefaultFetchOptions()

	if opts.Method != "GET" {
		t.Errorf("expected Method='GET', got %s", opts.Method)
	}

	if opts.Mode != "cors" {
		t.Errorf("expected Mode='cors', got %s", opts.Mode)
	}

	if opts.Credentials != "same-origin" {
		t.Errorf("expected Credentials='same-origin', got %s", opts.Credentials)
	}

	if opts.Cache != "default" {
		t.Errorf("expected Cache='default', got %s", opts.Cache)
	}

	if opts.Headers == nil {
		t.Error("Headers should not be nil")
	}
}

// TestPromiseHandlerCreation tests JSPromiseHandler creation.
func TestPromiseHandlerCreation(t *testing.T) {
	handler := NewJSPromiseHandler()

	if handler == nil {
		t.Fatal("handler should not be nil")
	}

	if handler.pending == nil {
		t.Error("pending map should not be nil")
	}
}

// TestGetPromiseHandler tests the global promise handler singleton.
func TestGetPromiseHandler(t *testing.T) {
	h1 := GetPromiseHandler()
	h2 := GetPromiseHandler()

	if h1 != h2 {
		t.Error("GetPromiseHandler should return the same instance")
	}
}

// TestWASMHTTPClientCreation tests WASMHTTPClient creation.
func TestWASMHTTPClientCreation(t *testing.T) {
	client := NewWASMHTTPClient()

	if client == nil {
		t.Fatal("client should not be nil")
	}

	if client.timeout != DefaultHTTPTimeout {
		t.Errorf("expected timeout %v, got %v", DefaultHTTPTimeout, client.timeout)
	}

	if client.defaultHeaders["User-Agent"] != DefaultHTTPUserAgent {
		t.Errorf("expected User-Agent %s, got %s", DefaultHTTPUserAgent, client.defaultHeaders["User-Agent"])
	}
}

// TestWASMHTTPClientSetHeader tests setting headers.
func TestWASMHTTPClientSetHeader(t *testing.T) {
	client := NewWASMHTTPClient()

	client.SetHeader("X-Custom-Header", "custom-value")

	if client.defaultHeaders["X-Custom-Header"] != "custom-value" {
		t.Error("header not set correctly")
	}
}

// TestWASMHTTPClientSetTimeout tests setting timeout.
func TestWASMHTTPClientSetTimeout(t *testing.T) {
	client := NewWASMHTTPClient()

	newTimeout := 60 * time.Second
	client.SetTimeout(newTimeout)

	if client.timeout != newTimeout {
		t.Errorf("expected timeout %v, got %v", newTimeout, client.timeout)
	}
}

// TestAsyncHTTPFileCreation tests AsyncHTTPFile creation.
func TestAsyncHTTPFileCreation(t *testing.T) {
	// Create a mock HTTP file
	httpFile := newHTTPFile("https://example.com/test.txt", nil, DefaultHTTPConfig())

	asyncFile := NewAsyncHTTPFile(httpFile)

	if asyncFile == nil {
		t.Fatal("asyncFile should not be nil")
	}

	if asyncFile.file != httpFile {
		t.Error("file reference not set correctly")
	}
}

// TestAsyncHTTPFileSystemCreation tests AsyncHTTPFileSystem creation.
func TestAsyncHTTPFileSystemCreation(t *testing.T) {
	ctx := context.Background()
	fs, err := NewHTTPFileSystem(ctx, DefaultHTTPConfig())
	if err != nil {
		t.Fatalf("failed to create HTTP filesystem: %v", err)
	}

	asyncFS := NewAsyncHTTPFileSystem(fs)

	if asyncFS == nil {
		t.Fatal("asyncFS should not be nil")
	}

	if asyncFS.fs != fs {
		t.Error("filesystem reference not set correctly")
	}
}

// TestWrapAsPromiseSuccess tests WrapAsPromise with a successful function.
func TestWrapAsPromiseSuccess(t *testing.T) {
	done := make(chan struct{})
	var result interface{}
	var gotError bool

	promise := WrapAsPromise(func() (interface{}, error) {
		return "success", nil
	})

	// Check that we got a Promise
	if !promise.InstanceOf(js.Global().Get("Promise")) {
		t.Fatal("WrapAsPromise should return a Promise")
	}

	// Set up then handler
	thenCallback := js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		result = jsToGo(args[0])
		close(done)
		return nil
	})
	defer thenCallback.Release()

	catchCallback := js.FuncOf(func(_ js.Value, _ []js.Value) interface{} {
		gotError = true
		close(done)
		return nil
	})
	defer catchCallback.Release()

	promise.Call("then", thenCallback).Call("catch", catchCallback)

	// Wait for the promise to resolve
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("promise did not resolve in time")
	}

	if gotError {
		t.Error("promise should not have rejected")
	}

	if result != "success" {
		t.Errorf("expected 'success', got %v", result)
	}
}

// TestWrapAsPromiseError tests WrapAsPromise with a failing function.
func TestWrapAsPromiseError(t *testing.T) {
	done := make(chan struct{})
	var gotError bool
	var errorMsg string

	promise := WrapAsPromise(func() (interface{}, error) {
		return nil, context.DeadlineExceeded
	})

	thenCallback := js.FuncOf(func(_ js.Value, _ []js.Value) interface{} {
		close(done)
		return nil
	})
	defer thenCallback.Release()

	catchCallback := js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		gotError = true
		if len(args) > 0 {
			errorMsg = args[0].Get("message").String()
		}
		close(done)
		return nil
	})
	defer catchCallback.Release()

	promise.Call("then", thenCallback).Call("catch", catchCallback)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("promise did not resolve in time")
	}

	if !gotError {
		t.Error("promise should have rejected")
	}

	if errorMsg != context.DeadlineExceeded.Error() {
		t.Errorf("expected error message '%s', got '%s'", context.DeadlineExceeded.Error(), errorMsg)
	}
}

// TestWrapAsPromiseWithContextCancellation tests context cancellation.
func TestWrapAsPromiseWithContextCancellation(t *testing.T) {
	done := make(chan struct{})
	var gotError bool

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately
	cancel()

	promise := WrapAsPromiseWithContext(ctx, func(ctx context.Context) (interface{}, error) {
		// This should check context before proceeding
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return "success", nil
		}
	})

	thenCallback := js.FuncOf(func(_ js.Value, _ []js.Value) interface{} {
		close(done)
		return nil
	})
	defer thenCallback.Release()

	catchCallback := js.FuncOf(func(_ js.Value, _ []js.Value) interface{} {
		gotError = true
		close(done)
		return nil
	})
	defer catchCallback.Release()

	promise.Call("then", thenCallback).Call("catch", catchCallback)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("promise did not resolve in time")
	}

	if !gotError {
		t.Error("promise should have rejected due to context cancellation")
	}
}

// TestCreateAsyncFileJS tests the async file JS object creation.
func TestCreateAsyncFileJS(t *testing.T) {
	httpFile := newHTTPFile("https://example.com/test.txt", nil, DefaultHTTPConfig())
	asyncFile := NewAsyncHTTPFile(httpFile)

	result := createAsyncFileJS(asyncFile)

	fileMap, ok := result.(map[string]interface{})
	if !ok {
		t.Fatal("expected map[string]interface{}")
	}

	// Check that all methods are present
	methods := []string{"read", "readAt", "stat", "close", "seek"}
	for _, method := range methods {
		if _, exists := fileMap[method]; !exists {
			t.Errorf("missing method: %s", method)
		}
	}
}

// TestYield tests the Yield function (non-blocking).
func TestYield(t *testing.T) {
	// This is a simple test that just ensures Yield doesn't hang
	done := make(chan struct{})

	go func() {
		Yield()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Error("Yield should complete quickly")
	}
}

// TestSleep tests the Sleep function.
func TestSleep(t *testing.T) {
	start := time.Now()

	done := make(chan struct{})
	go func() {
		Sleep(100 * time.Millisecond)
		close(done)
	}()

	select {
	case <-done:
		elapsed := time.Since(start)
		if elapsed < 90*time.Millisecond {
			t.Errorf("Sleep returned too quickly: %v", elapsed)
		}
	case <-time.After(1 * time.Second):
		t.Error("Sleep took too long")
	}
}

// TestFetchResponseStructure tests FetchResponse structure.
func TestFetchResponseStructure(t *testing.T) {
	resp := &FetchResponse{
		Status:     200,
		StatusText: "OK",
		OK:         true,
		URL:        "https://example.com",
		Headers:    map[string]string{"Content-Type": "text/plain"},
		Body:       []byte("Hello, World!"),
	}

	if resp.Status != 200 {
		t.Error("Status not set")
	}
	if resp.StatusText != "OK" {
		t.Error("StatusText not set")
	}
	if !resp.OK {
		t.Error("OK not set")
	}
	if resp.URL != "https://example.com" {
		t.Error("URL not set")
	}
	if resp.Headers["Content-Type"] != "text/plain" {
		t.Error("Headers not set")
	}
	if string(resp.Body) != "Hello, World!" {
		t.Error("Body not set")
	}
}

// TestAsyncResultStructure tests AsyncResult structure.
func TestAsyncResultStructure(t *testing.T) {
	result := AsyncResult{
		Data:  []byte{1, 2, 3},
		Error: nil,
	}

	if result.Data == nil {
		t.Error("Data should not be nil")
	}
	if result.Error != nil {
		t.Error("Error should be nil")
	}
}
