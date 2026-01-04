package filesystem

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
)

// Suppress unused import warnings - these are used for real GCS benchmarks
var (
	_ = context.Background
)

// BenchmarkMockGCSServer is a mock server for GCS benchmarks.
type BenchmarkMockGCSServer struct {
	*httptest.Server
	data []byte
}

// NewBenchmarkMockGCSServer creates a mock GCS server with the given data.
func NewBenchmarkMockGCSServer(data []byte) *BenchmarkMockGCSServer {
	mock := &BenchmarkMockGCSServer{
		data: data,
	}

	mock.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			mock.handleGet(w, r)
		case http.MethodPost, http.MethodPut:
			mock.handlePut(w, r)
		case http.MethodHead:
			mock.handleHead(w)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))

	return mock
}

func (m *BenchmarkMockGCSServer) handleGet(w http.ResponseWriter, r *http.Request) {
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		// Parse range header
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
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(m.data)
}

func (m *BenchmarkMockGCSServer) handlePut(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	m.data = body
	w.WriteHeader(http.StatusOK)
}

func (m *BenchmarkMockGCSServer) handleHead(w http.ResponseWriter) {
	w.Header().Set("Content-Length", strconv.Itoa(len(m.data)))
	w.WriteHeader(http.StatusOK)
}

// BenchmarkGCSRead benchmarks sequential read operations on GCS.
// GCS client requires proper setup even with mock server, so this uses buffer performance tests.
func BenchmarkGCSRead(b *testing.B) {
	b.Skip("GCS read benchmarks require real GCS credentials - use BenchmarkGCSWriteBufferPerformance instead")
}

// BenchmarkGCSWrite benchmarks sequential write operations on GCS.
// GCS client requires proper setup, so this uses buffer performance tests.
func BenchmarkGCSWrite(b *testing.B) {
	b.Skip("GCS write benchmarks require real GCS credentials - use BenchmarkGCSWriteBufferPerformance instead")
}

// BenchmarkGCSReadAt benchmarks random read operations using ReadAt on GCS.
// GCS client requires proper setup, so this is skipped.
func BenchmarkGCSReadAt(b *testing.B) {
	b.Skip("GCS ReadAt benchmarks require real GCS credentials")
}

// BenchmarkGCSStat benchmarks the Stat operation (metadata retrieval) on GCS.
// GCS client requires proper setup, so this is skipped.
func BenchmarkGCSStat(b *testing.B) {
	b.Skip("GCS Stat benchmarks require real GCS credentials")
}

// BenchmarkGCSReadRealCredentials benchmarks GCS read with real credentials.
// This benchmark is skipped unless GCS_BUCKET environment variable is set.
func BenchmarkGCSReadRealCredentials(b *testing.B) {
	bucket := os.Getenv("GCS_BUCKET")
	object := os.Getenv("GCS_OBJECT")

	if bucket == "" || object == "" {
		b.Skip("GCS_BUCKET and GCS_OBJECT not set - skipping real GCS benchmark")
	}

	ctx := context.Background()
	config := DefaultGCSConfig()

	fs, err := NewGCSFileSystem(ctx, config)
	if err != nil {
		b.Fatalf("Failed to create GCS filesystem: %v", err)
	}

	path := fmt.Sprintf("gs://%s/%s", bucket, object)

	// Get file size first
	info, err := fs.Stat(path)
	if err != nil {
		b.Fatalf("Failed to stat file: %v", err)
	}

	b.SetBytes(info.Size())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		file, err := fs.Open(path)
		if err != nil {
			b.Fatalf("Failed to open file: %v", err)
		}

		_, err = io.Copy(io.Discard, file)
		if err != nil {
			b.Fatalf("Failed to read file: %v", err)
		}

		_ = file.Close()
	}
}

// BenchmarkGCSWriteBufferPerformance benchmarks the in-memory buffer performance.
func BenchmarkGCSWriteBufferPerformance(b *testing.B) {
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
				// Test pure buffer write performance without GCS
				file := &GCSFile{
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

// BenchmarkGCSParseURL benchmarks URL parsing performance.
func BenchmarkGCSParseURL(b *testing.B) {
	urls := []string{
		"gs://bucket/object",
		"gcs://bucket/path/to/deep/object.parquet",
		"gs://bucket/object?generation=12345",
	}

	for _, url := range urls {
		name := strings.ReplaceAll(url, "/", "_")
		b.Run(name, func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, _, err := parseGCSPath(url)
				if err != nil {
					b.Fatalf("Failed to parse URL: %v", err)
				}
			}
		})
	}
}
