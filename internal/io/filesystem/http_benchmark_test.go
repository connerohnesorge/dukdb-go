package filesystem

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// BenchmarkMockHTTPServer is a mock server for HTTP benchmarks.
type BenchmarkMockHTTPServer struct {
	*httptest.Server
	data []byte
}

// NewBenchmarkMockHTTPServer creates a mock HTTP server with the given data.
func NewBenchmarkMockHTTPServer(data []byte) *BenchmarkMockHTTPServer {
	mock := &BenchmarkMockHTTPServer{
		data: data,
	}

	mock.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			mock.handleGet(w, r)
		case http.MethodHead:
			mock.handleHead(w)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))

	return mock
}

func (m *BenchmarkMockHTTPServer) handleGet(w http.ResponseWriter, r *http.Request) {
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
		w.Header().Set("Accept-Ranges", "bytes")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(m.data[start : end+1])

		return
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(m.data)))
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(m.data)
}

func (m *BenchmarkMockHTTPServer) handleHead(w http.ResponseWriter) {
	w.Header().Set("Content-Length", strconv.Itoa(len(m.data)))
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
}

// BenchmarkHTTPRead benchmarks sequential read operations on HTTP.
func BenchmarkHTTPRead(b *testing.B) {
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

			mock := NewBenchmarkMockHTTPServer(data)
			defer mock.Close()

			fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
			if err != nil {
				b.Fatalf("Failed to create HTTP filesystem: %v", err)
			}

			b.ResetTimer()
			b.SetBytes(int64(size.size))

			for i := 0; i < b.N; i++ {
				file, err := fs.Open(mock.URL + "/test.bin")
				if err != nil {
					b.Fatalf("Failed to open file: %v", err)
				}

				buf := make([]byte, size.size)
				_, err = io.ReadFull(file, buf)

				if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
					b.Fatalf("Failed to read file: %v", err)
				}

				_ = file.Close()
			}
		})
	}
}

// BenchmarkHTTPReadAt benchmarks random read operations using ReadAt on HTTP.
func BenchmarkHTTPReadAt(b *testing.B) {
	dataSize := 10 * 1024 * 1024 // 10MB
	data := make([]byte, dataSize)

	for i := range data {
		data[i] = byte(i % 256)
	}

	mock := NewBenchmarkMockHTTPServer(data)
	defer mock.Close()

	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	if err != nil {
		b.Fatalf("Failed to create HTTP filesystem: %v", err)
	}

	readSizes := []struct {
		name string
		size int
	}{
		{"1KB", 1024},
		{"64KB", 64 * 1024},
		{"256KB", 256 * 1024},
	}

	for _, readSize := range readSizes {
		b.Run(readSize.name, func(b *testing.B) {
			file, err := fs.Open(mock.URL + "/test.bin")
			if err != nil {
				b.Fatalf("Failed to open file: %v", err)
			}
			defer func() { _ = file.Close() }()

			b.ResetTimer()
			b.SetBytes(int64(readSize.size))

			buf := make([]byte, readSize.size)

			for i := 0; i < b.N; i++ {
				// Random offset
				offset := int64((i * 1234) % (dataSize - readSize.size))
				_, err := file.ReadAt(buf, offset)

				if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
					b.Fatalf("Failed to read at offset %d: %v", offset, err)
				}
			}
		})
	}
}

// BenchmarkHTTPStat benchmarks the Stat operation (HEAD request) on HTTP.
func BenchmarkHTTPStat(b *testing.B) {
	data := make([]byte, 1024) // 1KB for stat tests
	for i := range data {
		data[i] = byte(i % 256)
	}

	mock := NewBenchmarkMockHTTPServer(data)
	defer mock.Close()

	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	if err != nil {
		b.Fatalf("Failed to create HTTP filesystem: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := fs.Stat(mock.URL + "/test.bin")
		if err != nil {
			b.Fatalf("Failed to stat file: %v", err)
		}
	}
}

// BenchmarkHTTPExists benchmarks the Exists check on HTTP.
func BenchmarkHTTPExists(b *testing.B) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	mock := NewBenchmarkMockHTTPServer(data)
	defer mock.Close()

	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	if err != nil {
		b.Fatalf("Failed to create HTTP filesystem: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := fs.Exists(mock.URL + "/test.bin")
		if err != nil {
			b.Fatalf("Failed to check existence: %v", err)
		}
	}
}

// BenchmarkHTTPReadRealURL benchmarks HTTP read with a real URL.
// This benchmark is skipped unless HTTP_BENCHMARK_URL is set.
func BenchmarkHTTPReadRealURL(b *testing.B) {
	url := os.Getenv("HTTP_BENCHMARK_URL")
	if url == "" {
		b.Skip("HTTP_BENCHMARK_URL not set - skipping real HTTP benchmark")
	}

	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	if err != nil {
		b.Fatalf("Failed to create HTTP filesystem: %v", err)
	}

	// Get file size first
	info, err := fs.Stat(url)
	if err != nil {
		b.Fatalf("Failed to stat file: %v", err)
	}

	b.SetBytes(info.Size())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		file, err := fs.Open(url)
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

// BenchmarkHTTPParseURL benchmarks URL parsing performance.
func BenchmarkHTTPParseURL(b *testing.B) {
	urls := []string{
		"http://example.com/file.csv",
		"https://example.com/path/to/deep/file.parquet",
		"https://example.com/file?token=abc&version=2",
	}

	for _, url := range urls {
		name := strings.ReplaceAll(url, "/", "_")
		b.Run(name, func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := parseHTTPURL(url)
				if err != nil {
					b.Fatalf("Failed to parse URL: %v", err)
				}
			}
		})
	}
}

// BenchmarkHTTPConfigOptions benchmarks configuration option application.
func BenchmarkHTTPConfigOptions(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = NewHTTPConfig(
			WithHTTPTimeout(30*time.Second),
			WithHTTPFollowRedirects(true),
			WithHTTPMaxRedirects(10),
			WithHTTPUserAgent("benchmark-agent"),
			WithHTTPHeader("X-Custom", "value"),
		)
	}
}

// BenchmarkHTTPCapabilities benchmarks capability checking.
func BenchmarkHTTPCapabilities(b *testing.B) {
	fs := &HTTPFileSystem{
		config: DefaultHTTPConfig(),
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = fs.Capabilities()
	}
}

// BenchmarkHTTPHeaderApplication benchmarks header application to requests.
func BenchmarkHTTPHeaderApplication(b *testing.B) {
	config := NewHTTPConfig(
		WithHTTPUserAgent("benchmark-agent"),
		WithHTTPHeader("X-Custom-1", "value1"),
		WithHTTPHeader("X-Custom-2", "value2"),
		WithHTTPHeader("X-Custom-3", "value3"),
		WithHTTPBasicAuth("user", "pass"),
	)

	file := &HTTPFile{
		url:    "http://example.com/test",
		config: config,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest(http.MethodGet, file.url, nil)
		file.applyHeaders(req)
	}
}

// BenchmarkHTTPSeek benchmarks seeking in HTTP files.
func BenchmarkHTTPSeek(b *testing.B) {
	data := make([]byte, 10*1024*1024) // 10MB
	for i := range data {
		data[i] = byte(i % 256)
	}

	mock := NewBenchmarkMockHTTPServer(data)
	defer mock.Close()

	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	if err != nil {
		b.Fatalf("Failed to create HTTP filesystem: %v", err)
	}

	file, err := fs.Open(mock.URL + "/test.bin")
	if err != nil {
		b.Fatalf("Failed to open file: %v", err)
	}
	defer func() { _ = file.Close() }()

	// Do an initial read to populate size
	buf := make([]byte, 1)
	_, _ = file.Read(buf)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		offset := int64((i * 1000) % (10 * 1024 * 1024))
		_, err := file.Seek(offset, io.SeekStart)
		if err != nil {
			b.Fatalf("Failed to seek: %v", err)
		}
	}
}

// BenchmarkHTTPRedirectHandling benchmarks redirect handling.
func BenchmarkHTTPRedirectHandling(b *testing.B) {
	finalData := []byte("final content")

	redirectCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/redirect") {
			redirectCount++
			http.Redirect(w, r, "/final", http.StatusFound)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(finalData)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(finalData)
	}))
	defer server.Close()

	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	if err != nil {
		b.Fatalf("Failed to create HTTP filesystem: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		file, err := fs.Open(server.URL + "/redirect")
		if err != nil {
			b.Fatalf("Failed to open file: %v", err)
		}

		_, err = io.ReadAll(file)
		if err != nil {
			b.Fatalf("Failed to read: %v", err)
		}

		_ = file.Close()
	}
}

// BenchmarkHTTPConnectionReuse benchmarks connection reuse through Keep-Alive.
func BenchmarkHTTPConnectionReuse(b *testing.B) {
	data := make([]byte, 1024) // 1KB
	for i := range data {
		data[i] = byte(i % 256)
	}

	mock := NewBenchmarkMockHTTPServer(data)
	defer mock.Close()

	fs, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	if err != nil {
		b.Fatalf("Failed to create HTTP filesystem: %v", err)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(data)))

	for i := 0; i < b.N; i++ {
		file, err := fs.Open(mock.URL + "/test.bin")
		if err != nil {
			b.Fatalf("Failed to open file: %v", err)
		}

		_, err = io.ReadAll(file)
		if err != nil {
			b.Fatalf("Failed to read: %v", err)
		}

		_ = file.Close()
	}
}
