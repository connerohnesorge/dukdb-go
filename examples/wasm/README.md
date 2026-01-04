# dukdb-go WASM Browser Testing

This directory contains test files for running dukdb-go in a browser environment using WebAssembly (WASM).

## Overview

The dukdb-go driver is designed to work in WASM environments with the following compatibility:

| Filesystem | WASM Support | Notes |
|------------|--------------|-------|
| HTTP/HTTPS | Full | Uses JavaScript fetch API |
| S3 | Stub | Use pre-signed URLs via HTTP |
| GCS | Stub | Use signed URLs via HTTP |
| Azure | Stub | Use SAS URLs via HTTP |
| Local | Stub | Not available in browsers |

## Files

- `main.go` - Go WASM entry point that registers filesystem functions for JavaScript
- `index.html` - Test page with UI for testing filesystem operations
- `test.js` - JavaScript test functions and UI logic
- `build.sh` - Build script for compiling Go to WASM
- `wasm_exec.js` - Go WASM runtime (copied from Go installation during build)
- `dukdb.wasm` - Compiled WASM binary (generated during build)

## Building

### Prerequisites

- Go 1.21 or later
- A web browser with WASM support (all modern browsers)

### Build Steps

```bash
# From this directory
./build.sh

# Or manually
GOOS=js GOARCH=wasm go build -o dukdb.wasm ./main.go
cp "$(go env GOROOT)/misc/wasm/wasm_exec.js" .
```

## Running

You must serve the files via HTTP due to browser security restrictions. Opening `index.html` directly will not work.

### Using Python (recommended)

```bash
cd examples/wasm
./build.sh
python3 -m http.server 8080
# Open http://localhost:8080 in your browser
```

### Using Node.js

```bash
cd examples/wasm
./build.sh
npx serve .
# Open the URL shown in your terminal
```

### Using Go

Create a simple server or use any Go HTTP server package.

## Test Page Features

The test page (`index.html`) provides:

1. **WASM Status Indicator** - Shows if the WASM module loaded successfully
2. **Compatibility Grid** - Visual display of which filesystem schemes are supported
3. **Test URL Input** - Configure the URL to test against
4. **Individual Tests**:
   - HTTP Read - Test reading a file over HTTP
   - HTTP Stat - Test getting file metadata (HEAD request)
   - Fetch API - Test the JavaScript fetch integration
   - FileSystem Factory - Test the filesystem factory pattern
   - Range Read - Test HTTP range requests for partial content
5. **Run All Tests** - Execute all tests at once
6. **Console Output** - Log of all operations with timestamps
7. **Results Display** - JSON output of each test result

## JavaScript API

Once the WASM module is loaded, the following functions are available on `window.dukdb`:

```javascript
// Check WASM compatibility
dukdb.isWASMCompatible('http')  // true
dukdb.isWASMCompatible('s3')    // false

// Get compatible schemes
dukdb.getCompatibleSchemes()    // ['http', 'https']

// Test functions (return Promises)
await dukdb.testHTTPRead(url)
await dukdb.testHTTPStat(url)
await dukdb.testFetch(url)
await dukdb.testHTTPRangeRead(url, start, length)
await dukdb.testFileSystemFactory(url)
await dukdb.runAllTests(url)

// Async FileSystem API
const fs = await dukdb.createHTTPFileSystem()
const file = await fs.open('https://example.com/data.csv')
const data = await file.read(8192)
const stat = await file.stat()
await file.close()

// Direct fetch helper
const response = await dukdb.fetch(url, { method: 'GET', headers: {} })
```

## Working with Cloud Storage in WASM

Since native cloud SDKs (AWS SDK, GCS SDK, Azure SDK) are not available in WASM, you must use pre-signed URLs:

### S3 Pre-signed URLs

Generate pre-signed URLs server-side:

```go
// Server-side (Go)
presignClient := s3.NewPresignClient(s3Client)
presignedURL, _ := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
    Bucket: aws.String("my-bucket"),
    Key:    aws.String("my-file.csv"),
}, s3.WithPresignExpires(15*time.Minute))

// Send presignedURL.URL to the browser
```

Then use in browser:

```javascript
// Browser-side (JavaScript)
const fs = await dukdb.createHTTPFileSystem();
const file = await fs.open(presignedURL);
const data = await file.read(65536);
```

### GCS Signed URLs

```go
// Server-side (Go)
signedURL, _ := storage.SignedURL("my-bucket", "my-file.csv", &storage.SignedURLOptions{
    Method:  "GET",
    Expires: time.Now().Add(15 * time.Minute),
})
```

### Azure SAS URLs

```go
// Server-side (Go)
sasURL := blobClient.URL() + "?" + sasQueryParams.Encode()
```

## Troubleshooting

### WASM Fails to Load

- Ensure `wasm_exec.js` is present (run `build.sh`)
- Check browser console for errors
- Verify the server is serving with correct MIME types

### CORS Errors

- The test URL must allow cross-origin requests
- Use URLs from services that provide CORS headers
- For development, use a CORS proxy or local files

### Tests Fail

- Check the Console Output section for detailed error messages
- Verify the test URL is accessible
- Some URLs may require HTTPS

## Architecture

```
Browser                          Go WASM
+-------------------+           +-------------------+
| index.html        |           | main.go           |
| - Load WASM       |  <----    | - RegisterWASM    |
| - Test UI         |           |   AsyncFunctions  |
+-------------------+           +-------------------+
         |                              |
         v                              v
+-------------------+           +-------------------+
| test.js           |  <---->   | filesystem pkg    |
| - Test functions  |  Promise  | - HTTPFileSystem  |
| - Console logging |  based    | - AsyncHTTPFile   |
+-------------------+           +-------------------+
         |                              |
         v                              v
+-------------------+           +-------------------+
| fetch API         |  <---->   | FetchWithContext  |
| (Browser native)  |           | (Go wrapper)      |
+-------------------+           +-------------------+
```

## License

Same license as the main dukdb-go project.
