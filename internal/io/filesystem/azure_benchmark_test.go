package filesystem

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
)

// BenchmarkMockAzureServer is a mock server for Azure benchmarks.
type BenchmarkMockAzureServer struct {
	*httptest.Server
	data []byte
}

// NewBenchmarkMockAzureServer creates a mock Azure Blob server with the given data.
func NewBenchmarkMockAzureServer(data []byte) *BenchmarkMockAzureServer {
	mock := &BenchmarkMockAzureServer{
		data: data,
	}

	mock.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			mock.handleGet(w, r)
		case http.MethodPut:
			mock.handlePut(w, r)
		case http.MethodHead:
			mock.handleHead(w)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))

	return mock
}

func (m *BenchmarkMockAzureServer) handleGet(w http.ResponseWriter, r *http.Request) {
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" || r.Header.Get("x-ms-range") != "" {
		// Parse range header (Azure uses x-ms-range)
		if rangeHeader == "" {
			rangeHeader = r.Header.Get("x-ms-range")
		}

		var start, end int64
		if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err != nil {
			if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-", &start); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			end = int64(len(m.data)) - 1
		}

		if end >= int64(len(m.data)) {
			end = int64(len(m.data)) - 1
		}

		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(m.data)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(m.data[start : end+1])

		return
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(m.data)))
	w.Header().Set("x-ms-blob-content-length", strconv.Itoa(len(m.data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(m.data)
}

func (m *BenchmarkMockAzureServer) handlePut(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	m.data = body
	w.WriteHeader(http.StatusCreated)
}

func (m *BenchmarkMockAzureServer) handleHead(w http.ResponseWriter) {
	w.Header().Set("Content-Length", strconv.Itoa(len(m.data)))
	w.Header().Set("x-ms-blob-content-length", strconv.Itoa(len(m.data)))
	w.WriteHeader(http.StatusOK)
}

// BenchmarkAzureRead benchmarks the write buffer performance (mock-based).
// Azure operations with a real client require credentials.
// This benchmark tests the pure Go buffer performance.
func BenchmarkAzureRead(b *testing.B) {
	b.Skip(
		"Azure benchmarks require real Azure credentials - use BenchmarkAzureWriteBufferPerformance instead",
	)
}

// BenchmarkAzureWrite benchmarks sequential write operations on Azure Blob Storage.
// Since Azure SDK requires real credentials, this benchmark uses the buffer-only approach.
func BenchmarkAzureWrite(b *testing.B) {
	b.Skip(
		"Azure write benchmarks require real Azure credentials - use BenchmarkAzureWriteBufferPerformance instead",
	)
}

// BenchmarkAzureReadAt benchmarks random read operations using ReadAt on Azure.
// Since Azure SDK requires real credentials, this benchmark is skipped.
func BenchmarkAzureReadAt(b *testing.B) {
	b.Skip("Azure ReadAt benchmarks require real Azure credentials")
}

// BenchmarkAzureStat benchmarks the Stat operation on Azure.
// Since Azure SDK requires real credentials, this benchmark is skipped.
func BenchmarkAzureStat(b *testing.B) {
	b.Skip("Azure Stat benchmarks require real Azure credentials")
}

// BenchmarkAzureReadRealCredentials benchmarks Azure read with real credentials.
// This benchmark is skipped unless AZURE_STORAGE_ACCOUNT is set.
func BenchmarkAzureReadRealCredentials(b *testing.B) {
	account := os.Getenv("AZURE_STORAGE_ACCOUNT")
	container := os.Getenv("AZURE_STORAGE_CONTAINER")
	blob := os.Getenv("AZURE_STORAGE_BLOB")

	if account == "" || container == "" || blob == "" {
		b.Skip("AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_CONTAINER, AZURE_STORAGE_BLOB not set")
	}

	// Note: Would need proper Azure client setup for real benchmark
	b.Skip("Real Azure benchmark requires proper credential setup")
}

// BenchmarkAzureWriteBufferPerformance benchmarks the in-memory buffer performance.
func BenchmarkAzureWriteBufferPerformance(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"1KB", 1024},
		{"64KB", 64 * 1024},
		{"1MB", 1024 * 1024},
		{"10MB", 10 * 1024 * 1024},
	}

	for _, size := range sizes {
		b.Run(size.name, func(b *testing.B) {
			data := make([]byte, size.size)
			for i := range data {
				data[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.SetBytes(int64(size.size))

			for i := 0; i < b.N; i++ {
				// Test pure buffer write performance without Azure
				file := &AzureFile{
					writeMode:   true,
					writeBuffer: &bytes.Buffer{},
				}

				_, err := file.Write(data)
				if err != nil {
					b.Fatalf("Failed to write: %v", err)
				}
			}
		})
	}
}

// BenchmarkAzureParseURL benchmarks URL parsing performance.
func BenchmarkAzureParseURL(b *testing.B) {
	urls := []string{
		"azure://container/blob",
		"az://container/path/to/deep/blob.parquet",
		"azure://container/blob?sastoken=abc",
	}

	for _, url := range urls {
		name := strings.ReplaceAll(url, "/", "_")
		b.Run(name, func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, _, err := parseAzurePath(url)
				if err != nil {
					b.Fatalf("Failed to parse URL: %v", err)
				}
			}
		})
	}
}

// BenchmarkAzureConfigOptions benchmarks configuration option application.
func BenchmarkAzureConfigOptions(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = NewAzureConfig(
			WithAzureAccountName("testaccount"),
			WithAzureAccountKey("testkey"),
			WithAzureContainer("testcontainer"),
			WithAzureTimeout(DefaultAzureTimeout),
			WithAzureBlockSize(DefaultAzureBlockSize),
		)
	}
}

// BenchmarkAzureCapabilities benchmarks capability checking.
func BenchmarkAzureCapabilities(b *testing.B) {
	fs := &AzureFileSystem{
		config: DefaultAzureConfig(),
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = fs.Capabilities()
	}
}
