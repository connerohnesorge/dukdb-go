# WebAssembly (WASM) Guide

dukdb-go compiles to WebAssembly for use in browsers and other WASM environments. This guide covers building, deploying, and using dukdb-go with cloud storage in WASM.

## Overview

### What Works in WASM

| Feature | Status | Notes |
|---------|--------|-------|
| Core SQL Engine | Fully Supported | All SQL operations work |
| HTTP/HTTPS Filesystem | Fully Supported | Uses browser fetch API |
| JSON Type & Functions | Fully Supported | |
| Geometry Functions | Fully Supported | |
| Extended Types | Fully Supported | BIGNUM, VARIANT, LAMBDA |
| Async/Promise API | Fully Supported | Native JavaScript integration |

### What Does NOT Work in WASM

| Feature | Status | Workaround |
|---------|--------|------------|
| Local Filesystem | Not Supported | Use File API / drag-and-drop |
| S3 Filesystem | Not Supported | Use pre-signed URLs via HTTP |
| GCS Filesystem | Not Supported | Use signed URLs via HTTP |
| Azure Filesystem | Not Supported | Use SAS URLs via HTTP |

### Why Cloud SDKs Do Not Work

The native cloud storage SDKs (AWS SDK, GCS SDK, Azure SDK) are not compatible with WASM because they depend on:

- `os/user` package (not available in WASM)
- Memory-mapped files (not available in browsers)
- Native networking (browsers require fetch API)
- Local credential files (browsers cannot access filesystem)

The recommended approach is to use pre-signed URLs with the HTTP filesystem, which works perfectly in WASM.

## Building for WASM

### Basic Build

```bash
GOOS=js GOARCH=wasm go build -o dukdb.wasm ./cmd/wasm/
```

### Example main.go for WASM

Create `cmd/wasm/main.go`:

```go
//go:build js && wasm

package main

import (
    "syscall/js"

    _ "github.com/dukdb/dukdb-go"
    _ "github.com/dukdb/dukdb-go/internal/engine"
    "github.com/dukdb/dukdb-go/internal/io/filesystem"
)

func main() {
    // Register async filesystem functions for JavaScript
    filesystem.RegisterWASMAsyncFunctions()

    // Register additional dukdb functions as needed
    registerDukDBFunctions()

    // Keep the Go runtime alive
    select {}
}

func registerDukDBFunctions() {
    global := js.Global()

    // Ensure dukdb namespace exists
    dukdb := global.Get("dukdb")
    if dukdb.IsUndefined() || dukdb.IsNull() {
        dukdb = js.Global().Get("Object").New()
        global.Set("dukdb", dukdb)
    }

    // Register database creation function
    dukdb.Set("open", js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
        return filesystem.WrapAsPromise(func() (interface{}, error) {
            // Create in-memory database
            // Implementation depends on your database setup
            return map[string]interface{}{
                "ready": true,
            }, nil
        })
    }))
}
```

### Build with Optimizations

For production, use build flags to reduce binary size:

```bash
GOOS=js GOARCH=wasm go build \
    -ldflags="-s -w" \
    -o dukdb.wasm \
    ./cmd/wasm/
```

### Using TinyGo (Smaller Binary)

TinyGo produces much smaller WASM binaries:

```bash
tinygo build -o dukdb.wasm -target wasm ./cmd/wasm/
```

Note: TinyGo has some limitations with reflection-heavy code.

## Browser Integration

### HTML Setup

```html
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>DukDB WASM Demo</title>
</head>
<body>
    <h1>DukDB in the Browser</h1>
    <div id="output"></div>

    <!-- Go WASM support script (from Go installation) -->
    <script src="wasm_exec.js"></script>

    <script>
        // Initialize Go WASM runtime
        const go = new Go();

        WebAssembly.instantiateStreaming(fetch("dukdb.wasm"), go.importObject)
            .then((result) => {
                go.run(result.instance);
                console.log("DukDB WASM loaded successfully");

                // Now dukdb namespace is available
                initDatabase();
            })
            .catch((err) => {
                console.error("Failed to load WASM:", err);
            });

        async function initDatabase() {
            // Wait for dukdb to be available
            if (typeof dukdb === 'undefined') {
                setTimeout(initDatabase, 100);
                return;
            }

            console.log("DukDB ready!");

            // Create HTTP filesystem for cloud data
            const fs = await dukdb.createHTTPFileSystem();
            console.log("HTTP filesystem created");
        }
    </script>
</body>
</html>
```

### Getting wasm_exec.js

Copy from your Go installation:

```bash
cp "$(go env GOROOT)/misc/wasm/wasm_exec.js" ./static/
```

### JavaScript Module Pattern

For modern JavaScript applications:

```javascript
// dukdb-loader.js
export async function loadDukDB() {
    const go = new Go();

    const response = await fetch('dukdb.wasm');
    const bytes = await response.arrayBuffer();
    const result = await WebAssembly.instantiate(bytes, go.importObject);

    // Start the Go runtime (non-blocking)
    go.run(result.instance);

    // Wait for dukdb to be available
    while (typeof globalThis.dukdb === 'undefined') {
        await new Promise(resolve => setTimeout(resolve, 10));
    }

    return globalThis.dukdb;
}

// Usage
import { loadDukDB } from './dukdb-loader.js';

const dukdb = await loadDukDB();
const fs = await dukdb.createHTTPFileSystem();
```

## Using Cloud Storage in WASM

Since native cloud SDKs do not work in WASM, use pre-signed URLs with the HTTP filesystem.

### HTTP Filesystem (Fully Supported)

The HTTP filesystem works in WASM using the browser's fetch API:

```javascript
// Create HTTP filesystem
const fs = await dukdb.createHTTPFileSystem();

// Open a file from any HTTP URL
const file = await fs.open("https://example.com/data.csv");

// Read file contents
const data = await file.read(8192);

// Get file info
const info = await file.stat();
console.log("File size:", info.size);

// Close when done
await file.close();
```

### S3 via Pre-Signed URLs

Generate pre-signed URLs on your backend, then use them in the browser:

```javascript
// Backend (Node.js/Go/Python) generates pre-signed URL
// Frontend receives: https://bucket.s3.region.amazonaws.com/key?X-Amz-...

async function readFromS3(presignedUrl) {
    const file = await dukdb.openHTTPFile(presignedUrl);

    try {
        // Read CSV data
        const data = await file.read(1024 * 1024); // Read 1MB

        // Process the data
        const text = new TextDecoder().decode(data);
        console.log("CSV content:", text.substring(0, 200));

        return data;
    } finally {
        await file.close();
    }
}

// Example pre-signed URL from your backend
const presignedUrl = "https://my-bucket.s3.us-east-1.amazonaws.com/data.csv" +
    "?X-Amz-Algorithm=AWS4-HMAC-SHA256" +
    "&X-Amz-Credential=..." +
    "&X-Amz-Date=..." +
    "&X-Amz-Expires=3600" +
    "&X-Amz-SignedHeaders=host" +
    "&X-Amz-Signature=...";

const data = await readFromS3(presignedUrl);
```

### GCS via Signed URLs

Generate signed URLs for Google Cloud Storage:

```javascript
// Backend generates signed URL
// Frontend receives: https://storage.googleapis.com/bucket/object?X-Goog-...

async function readFromGCS(signedUrl) {
    const file = await dukdb.openHTTPFile(signedUrl);

    try {
        const data = await file.read(1024 * 1024);
        return data;
    } finally {
        await file.close();
    }
}

// Example signed URL from your backend
const signedUrl = "https://storage.googleapis.com/my-bucket/data.parquet" +
    "?X-Goog-Algorithm=GOOG4-RSA-SHA256" +
    "&X-Goog-Credential=..." +
    "&X-Goog-Date=..." +
    "&X-Goog-Expires=3600" +
    "&X-Goog-SignedHeaders=host" +
    "&X-Goog-Signature=...";

const data = await readFromGCS(signedUrl);
```

### Azure via SAS URLs

Generate SAS (Shared Access Signature) URLs for Azure:

```javascript
// Backend generates SAS URL
// Frontend receives: https://account.blob.core.windows.net/container/blob?sv=...

async function readFromAzure(sasUrl) {
    const file = await dukdb.openHTTPFile(sasUrl);

    try {
        const data = await file.read(1024 * 1024);
        return data;
    } finally {
        await file.close();
    }
}

// Example SAS URL from your backend
const sasUrl = "https://myaccount.blob.core.windows.net/mycontainer/data.json" +
    "?sv=2021-06-08" +
    "&st=2024-01-01T00:00:00Z" +
    "&se=2024-01-02T00:00:00Z" +
    "&sr=b" +
    "&sp=r" +
    "&sig=...";

const data = await readFromAzure(sasUrl);
```

## Async API Reference

The WASM async API uses JavaScript Promises for all operations.

### dukdb.createHTTPFileSystem()

Creates an async HTTP filesystem.

```javascript
const fs = await dukdb.createHTTPFileSystem();
```

Returns an object with methods:
- `open(url)` - Open a file, returns Promise<File>
- `stat(url)` - Get file info, returns Promise<FileInfo>
- `exists(url)` - Check if file exists, returns Promise<boolean>

### dukdb.openHTTPFile(url)

Convenience function to open an HTTP file directly.

```javascript
const file = await dukdb.openHTTPFile("https://example.com/data.csv");
```

Returns a File object with methods:
- `read(length)` - Read bytes, returns Promise<Uint8Array>
- `readAt(length, offset)` - Read at offset, returns Promise<Uint8Array>
- `seek(offset, whence)` - Seek to position, returns Promise<number>
- `stat()` - Get file info, returns Promise<FileInfo>
- `close()` - Close the file, returns Promise<void>

### dukdb.fetch(url, options)

Low-level fetch wrapper with Go context integration.

```javascript
const response = await dukdb.fetch("https://example.com/api/data", {
    method: "GET",
    headers: {
        "Authorization": "Bearer token123"
    }
});

console.log("Status:", response.status);
console.log("Body:", new TextDecoder().decode(response.body));
```

### FileInfo Object

File metadata returned by `stat()`:

```javascript
const info = await file.stat();
console.log({
    name: info.name,      // File name
    size: info.size,      // Size in bytes
    isDir: info.isDir,    // Always false for HTTP
    modTime: info.modTime // Modification time (Unix ms)
});
```

### Seek Constants

For `file.seek(offset, whence)`:

```javascript
const SEEK_START = 0;   // Relative to start
const SEEK_CURRENT = 1; // Relative to current position
const SEEK_END = 2;     // Relative to end

// Examples
await file.seek(0, SEEK_START);    // Go to beginning
await file.seek(100, SEEK_START);  // Go to byte 100
await file.seek(-10, SEEK_END);    // Go to 10 bytes before end
```

## Complete Browser Example

A complete example reading a CSV file from S3 using a pre-signed URL:

```html
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>DukDB Cloud Storage Demo</title>
    <style>
        body { font-family: system-ui, sans-serif; max-width: 800px; margin: 40px auto; padding: 0 20px; }
        #output { background: #f5f5f5; padding: 20px; border-radius: 8px; white-space: pre-wrap; font-family: monospace; }
        button { padding: 10px 20px; font-size: 16px; cursor: pointer; margin: 10px 5px 10px 0; }
        input { padding: 10px; width: 100%; font-size: 14px; margin: 10px 0; }
    </style>
</head>
<body>
    <h1>DukDB WASM + Cloud Storage</h1>

    <div>
        <label for="urlInput">Pre-signed URL or HTTP URL:</label>
        <input type="text" id="urlInput" placeholder="https://example.com/data.csv or pre-signed S3 URL">
    </div>

    <button onclick="loadFile()">Load File</button>
    <button onclick="loadPublicExample()">Load Public Example</button>

    <h3>Output:</h3>
    <div id="output">Waiting for DukDB to load...</div>

    <script src="wasm_exec.js"></script>
    <script>
        let dukdbReady = false;

        // Initialize Go WASM runtime
        const go = new Go();

        WebAssembly.instantiateStreaming(fetch("dukdb.wasm"), go.importObject)
            .then((result) => {
                go.run(result.instance);
                waitForDukDB();
            })
            .catch((err) => {
                document.getElementById('output').textContent = "Error loading WASM: " + err;
            });

        function waitForDukDB() {
            if (typeof dukdb !== 'undefined') {
                dukdbReady = true;
                document.getElementById('output').textContent = "DukDB ready! Enter a URL and click 'Load File'.";
            } else {
                setTimeout(waitForDukDB, 100);
            }
        }

        async function loadFile() {
            if (!dukdbReady) {
                alert("DukDB is still loading...");
                return;
            }

            const url = document.getElementById('urlInput').value.trim();
            if (!url) {
                alert("Please enter a URL");
                return;
            }

            const output = document.getElementById('output');
            output.textContent = "Loading file...\n";

            try {
                // Open the file
                const file = await dukdb.openHTTPFile(url);
                output.textContent += "File opened successfully\n";

                // Get file info
                const info = await file.stat();
                output.textContent += `File size: ${info.size} bytes\n`;
                output.textContent += `File name: ${info.name}\n\n`;

                // Read first 4KB of content
                const data = await file.read(4096);
                output.textContent += `Read ${data.length} bytes\n\n`;

                // Decode as text
                const text = new TextDecoder().decode(data);
                output.textContent += "Content preview:\n";
                output.textContent += "─".repeat(40) + "\n";
                output.textContent += text.substring(0, 2000);
                if (text.length > 2000) {
                    output.textContent += "\n... (truncated)";
                }

                // Close the file
                await file.close();
                output.textContent += "\n─".repeat(40) + "\n";
                output.textContent += "File closed.";

            } catch (err) {
                output.textContent += "Error: " + err.message;
            }
        }

        function loadPublicExample() {
            // Use a public CSV dataset
            document.getElementById('urlInput').value =
                "https://raw.githubusercontent.com/datasets/airport-codes/master/data/airport-codes.csv";
            loadFile();
        }
    </script>
</body>
</html>
```

## Limitations

### No Local File Access

Browsers cannot access the local filesystem directly. Workarounds:

1. **File Input**: Let users select files via `<input type="file">`
2. **Drag and Drop**: Accept dropped files
3. **IndexedDB**: Store data in browser storage

Example with File Input:

```javascript
document.getElementById('fileInput').addEventListener('change', async (event) => {
    const file = event.target.files[0];
    const text = await file.text();
    // Process the CSV/JSON/Parquet data
});
```

### No Direct Cloud SDK Access

As explained above, use pre-signed URLs instead.

### CORS Requirements

When fetching from cross-origin URLs, the server must send appropriate CORS headers:

```
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, HEAD, OPTIONS
Access-Control-Allow-Headers: Range, Content-Type
Access-Control-Expose-Headers: Content-Length, Content-Range
```

Most cloud storage providers support CORS configuration:
- **S3**: Configure CORS in bucket settings
- **GCS**: Configure CORS with `gsutil cors set`
- **Azure**: Configure CORS in storage account settings

### Memory Constraints

WASM has memory limits (typically 4GB maximum). For large files:
- Use range requests to read portions
- Stream data instead of loading entirely
- Process data in chunks

```javascript
// Process large file in chunks
async function processLargeFile(url) {
    const file = await dukdb.openHTTPFile(url);
    const info = await file.stat();

    const chunkSize = 1024 * 1024; // 1MB chunks
    let offset = 0;

    while (offset < info.size) {
        const chunk = await file.readAt(chunkSize, offset);
        // Process chunk...
        offset += chunk.length;
    }

    await file.close();
}
```

## Generating Pre-Signed URLs

### AWS S3 (Go)

```go
import (
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/service/s3"
)

func generatePresignedURL(client *s3.PresignClient, bucket, key string) (string, error) {
    request, err := client.PresignGetObject(context.Background(),
        &s3.GetObjectInput{
            Bucket: aws.String(bucket),
            Key:    aws.String(key),
        },
        s3.WithPresignExpires(time.Hour), // URL valid for 1 hour
    )
    if err != nil {
        return "", err
    }
    return request.URL, nil
}
```

### GCS (Go)

```go
import "cloud.google.com/go/storage"

func generateSignedURL(bucket, object string) (string, error) {
    client, _ := storage.NewClient(context.Background())

    url, err := client.Bucket(bucket).SignedURL(object, &storage.SignedURLOptions{
        Method:  "GET",
        Expires: time.Now().Add(time.Hour),
    })
    return url, err
}
```

### Azure (Go)

```go
import "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"

func generateSASURL(containerURL, blobName string) (string, error) {
    sasURL, err := sas.BlobSignatureValues{
        Protocol:      sas.ProtocolHTTPS,
        StartTime:     time.Now().UTC(),
        ExpiryTime:    time.Now().UTC().Add(time.Hour),
        Permissions:   "r",
        ContainerName: containerName,
        BlobName:      blobName,
    }.SignWithSharedKey(credential)

    return sasURL.String(), err
}
```

## Performance Tips

1. **Use Range Requests**: For Parquet files, only fetch the columns you need
2. **Enable Compression**: Ensure gzip/brotli compression is enabled on your server
3. **Cache Results**: Use browser caching for repeated requests
4. **Parallel Reads**: Read multiple ranges concurrently for large files
5. **Pre-warm Connections**: Open connections before you need data

## Troubleshooting

### "operation not supported in WASM environment"

You are trying to use a feature that requires native code (S3, GCS, Azure SDKs, or local filesystem). Use HTTP with pre-signed URLs instead.

### CORS Errors

Check browser console for CORS errors. Ensure the server sends proper CORS headers.

### Memory Issues

If you see out-of-memory errors:
- Process data in smaller chunks
- Close files promptly after use
- Avoid loading entire large files into memory

### Fetch Failed

Common causes:
- Network connectivity issues
- URL is incorrect or expired (for pre-signed URLs)
- Server returned an error
- CORS blocking the request

## See Also

- [Cloud Storage Integration](cloud-storage.md) - General cloud storage documentation
- [Secrets Management](secrets.md) - Managing credentials (server-side only)
- [Extended Types](types.md) - Information on supported data types
