package filesystem

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// BenchmarkMockS3Server is a mock server for S3 benchmarks.
type BenchmarkMockS3Server struct {
	*httptest.Server
	data []byte
}

// NewBenchmarkMockS3Server creates a mock S3 server with the given data.
func NewBenchmarkMockS3Server(data []byte) *BenchmarkMockS3Server {
	mock := &BenchmarkMockS3Server{
		data: data,
	}

	mock.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			mock.handleGet(w, r)
		case http.MethodPut:
			mock.handlePut(w, r)
		case http.MethodHead:
			mock.handleHead(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))

	return mock
}

func (m *BenchmarkMockS3Server) handleGet(w http.ResponseWriter, r *http.Request) {
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		// Parse range header
		var start, end int64
		if _, err := parseRangeHeader(rangeHeader, int64(len(m.data)), &start, &end); err == nil {
			w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
			w.Header().Set("Last-Modified", "Sat, 04 Jan 2026 12:00:00 GMT")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(m.data[start : end+1])

			return
		}
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(m.data)))
	w.Header().Set("Last-Modified", "Sat, 04 Jan 2026 12:00:00 GMT")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(m.data)
}

func (m *BenchmarkMockS3Server) handlePut(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	m.data = body
	w.WriteHeader(http.StatusOK)
}

func (m *BenchmarkMockS3Server) handleHead(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Length", strconv.Itoa(len(m.data)))
	w.Header().Set("Last-Modified", "Sat, 04 Jan 2026 12:00:00 GMT")
	w.WriteHeader(http.StatusOK)
}

// parseRangeHeader parses an HTTP Range header.
func parseRangeHeader(header string, totalSize int64, start, end *int64) (bool, error) {
	// Simple parser for "bytes=start-end" format
	if !strings.HasPrefix(header, "bytes=") {
		return false, nil
	}

	parts := strings.Split(strings.TrimPrefix(header, "bytes="), "-")
	if len(parts) != 2 {
		return false, nil
	}

	var s, e int64
	if parts[0] != "" {
		if _, err := parseRange(parts[0], &s); err != nil {
			return false, err
		}
	}

	if parts[1] != "" {
		if _, err := parseRange(parts[1], &e); err != nil {
			return false, err
		}
	} else {
		e = totalSize - 1
	}

	*start = s
	*end = e

	return true, nil
}

func parseRange(s string, v *int64) (bool, error) {
	var val int64

	for _, c := range s {
		if c >= '0' && c <= '9' {
			val = val*10 + int64(c-'0')
		} else {
			return false, nil
		}
	}

	*v = val

	return true, nil
}

// BenchmarkS3Read benchmarks sequential read operations.
func BenchmarkS3Read(b *testing.B) {
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

			mock := NewBenchmarkMockS3Server(data)
			defer mock.Close()

			config := S3Config{
				Endpoint:        strings.TrimPrefix(mock.URL, "http://"),
				UseSSL:          false,
				URLStyle:        S3URLStylePath,
				Region:          "us-east-1",
				AccessKeyID:     "test-key",
				SecretAccessKey: "test-secret",
			}

			fs, err := NewS3FileSystem(context.Background(), config)
			if err != nil {
				b.Fatalf("Failed to create filesystem: %v", err)
			}

			b.ResetTimer()
			b.SetBytes(int64(size.size))

			for i := 0; i < b.N; i++ {
				file, err := fs.Open("s3://test-bucket/test-key")
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

// BenchmarkS3Write benchmarks sequential write operations.
func BenchmarkS3Write(b *testing.B) {
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

			mock := NewBenchmarkMockS3Server(nil)
			defer mock.Close()

			config := S3Config{
				Endpoint:        strings.TrimPrefix(mock.URL, "http://"),
				UseSSL:          false,
				URLStyle:        S3URLStylePath,
				Region:          "us-east-1",
				AccessKeyID:     "test-key",
				SecretAccessKey: "test-secret",
			}

			fs, err := NewS3FileSystem(context.Background(), config)
			if err != nil {
				b.Fatalf("Failed to create filesystem: %v", err)
			}

			b.ResetTimer()
			b.SetBytes(int64(size.size))

			for i := 0; i < b.N; i++ {
				file, err := fs.Create("s3://test-bucket/test-key")
				if err != nil {
					b.Fatalf("Failed to create file: %v", err)
				}

				_, err = file.Write(data)
				if err != nil {
					b.Fatalf("Failed to write file: %v", err)
				}

				err = file.Close()
				if err != nil {
					b.Fatalf("Failed to close file: %v", err)
				}
			}
		})
	}
}

// BenchmarkS3ReadAt benchmarks random read operations using ReadAt.
func BenchmarkS3ReadAt(b *testing.B) {
	dataSize := 10 * 1024 * 1024 // 10MB
	data := make([]byte, dataSize)

	for i := range data {
		data[i] = byte(i % 256)
	}

	mock := NewBenchmarkMockS3Server(data)
	defer mock.Close()

	config := S3Config{
		Endpoint:        strings.TrimPrefix(mock.URL, "http://"),
		UseSSL:          false,
		URLStyle:        S3URLStylePath,
		Region:          "us-east-1",
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	}

	fs, err := NewS3FileSystem(context.Background(), config)
	if err != nil {
		b.Fatalf("Failed to create filesystem: %v", err)
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
			file, err := fs.Open("s3://test-bucket/test-key")
			if err != nil {
				b.Fatalf("Failed to open file: %v", err)
			}
			defer func() { _ = file.Close() }()

			s3File, ok := file.(*S3File)
			if !ok {
				b.Fatal("File is not an S3File")
			}

			b.ResetTimer()
			b.SetBytes(int64(readSize.size))

			buf := make([]byte, readSize.size)

			for i := 0; i < b.N; i++ {
				// Random offset
				offset := int64((i * 1234) % (dataSize - readSize.size))
				_, err := s3File.ReadAt(buf, offset)

				if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
					b.Fatalf("Failed to read at offset %d: %v", offset, err)
				}
			}
		})
	}
}

// BenchmarkRetryCalculateDelay benchmarks the delay calculation for retries.
func BenchmarkRetryCalculateDelay(b *testing.B) {
	cfg := DefaultRetryConfig()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = calculateDelay(cfg, i%10)
	}
}

// BenchmarkMultipartWriter benchmarks the multipart writer performance.
func BenchmarkMultipartWriter(b *testing.B) {
	// Skip this benchmark in short mode as it requires more setup
	if testing.Short() {
		b.Skip("Skipping multipart benchmark in short mode")
	}

	sizes := []struct {
		name string
		size int
	}{
		{"10MB", 10 * 1024 * 1024},
		{"50MB", 50 * 1024 * 1024},
	}

	for _, size := range sizes {
		b.Run(size.name, func(b *testing.B) {
			data := make([]byte, size.size)
			for i := range data {
				data[i] = byte(i % 256)
			}

			// Create mock server that accepts multipart uploads
			uploadedData := make([]byte, 0, size.size)
			mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "uploads"):
					// InitiateMultipartUpload
					w.Header().Set("Content-Type", "application/xml")
					_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
						<InitiateMultipartUploadResult>
							<Bucket>test-bucket</Bucket>
							<Key>test-key</Key>
							<UploadId>test-upload-id</UploadId>
						</InitiateMultipartUploadResult>`))
				case r.Method == http.MethodPut && strings.Contains(r.URL.RawQuery, "partNumber"):
					// UploadPart
					body, _ := io.ReadAll(r.Body)
					uploadedData = append(uploadedData, body...)
					w.Header().Set("ETag", "\"test-etag\"")
					w.WriteHeader(http.StatusOK)
				case r.Method == http.MethodPost && strings.Contains(r.URL.RawQuery, "uploadId"):
					// CompleteMultipartUpload
					w.Header().Set("Content-Type", "application/xml")
					_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
						<CompleteMultipartUploadResult>
							<Bucket>test-bucket</Bucket>
							<Key>test-key</Key>
							<ETag>"test-etag"</ETag>
						</CompleteMultipartUploadResult>`))
				default:
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer mock.Close()

			config := S3Config{
				Endpoint:             strings.TrimPrefix(mock.URL, "http://"),
				UseSSL:               false,
				URLStyle:             S3URLStylePath,
				Region:               "us-east-1",
				AccessKeyID:          "test-key",
				SecretAccessKey:      "test-secret",
				MultipartPartSize:    5 * 1024 * 1024, // 5MB parts
				MultipartConcurrency: 4,
			}

			fs, err := NewS3FileSystem(context.Background(), config)
			if err != nil {
				b.Fatalf("Failed to create filesystem: %v", err)
			}

			b.ResetTimer()
			b.SetBytes(int64(size.size))

			for i := 0; i < b.N; i++ {
				uploadedData = uploadedData[:0]

				writer, err := fs.CreateMultipartWriter(context.Background(), "s3://test-bucket/test-key")
				if err != nil {
					b.Fatalf("Failed to create multipart writer: %v", err)
				}

				_, err = writer.Write(data)
				if err != nil {
					b.Fatalf("Failed to write data: %v", err)
				}

				err = writer.Close()
				if err != nil {
					b.Fatalf("Failed to close writer: %v", err)
				}
			}
		})
	}
}

// BenchmarkS3Stat benchmarks the Stat operation (metadata retrieval).
func BenchmarkS3Stat(b *testing.B) {
	data := make([]byte, 1024) // 1KB for stat tests
	for i := range data {
		data[i] = byte(i % 256)
	}

	mock := NewBenchmarkMockS3Server(data)
	defer mock.Close()

	config := S3Config{
		Endpoint:        strings.TrimPrefix(mock.URL, "http://"),
		UseSSL:          false,
		URLStyle:        S3URLStylePath,
		Region:          "us-east-1",
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	}

	fs, err := NewS3FileSystem(context.Background(), config)
	if err != nil {
		b.Fatalf("Failed to create filesystem: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := fs.Stat("s3://test-bucket/test-key")
		if err != nil {
			b.Fatalf("Failed to stat file: %v", err)
		}
	}
}

// BenchmarkConcurrentReader benchmarks the concurrent reader performance.
func BenchmarkConcurrentReader(b *testing.B) {
	sizes := []struct {
		name    string
		size    int
		workers int
	}{
		{"10MB_1worker", 10 * 1024 * 1024, 1},
		{"10MB_4workers", 10 * 1024 * 1024, 4},
		{"10MB_8workers", 10 * 1024 * 1024, 8},
	}

	for _, size := range sizes {
		b.Run(size.name, func(b *testing.B) {
			data := make([]byte, size.size)
			for i := range data {
				data[i] = byte(i % 256)
			}

			mock := NewBenchmarkMockS3Server(data)
			defer mock.Close()

			config := S3Config{
				Endpoint:                strings.TrimPrefix(mock.URL, "http://"),
				UseSSL:                  false,
				URLStyle:                S3URLStylePath,
				Region:                  "us-east-1",
				AccessKeyID:             "test-key",
				SecretAccessKey:         "test-secret",
				ConcurrentReadWorkers:   size.workers,
				ConcurrentReadChunkSize: 1 * 1024 * 1024, // 1MB chunks
			}

			fs, err := NewS3FileSystem(context.Background(), config)
			if err != nil {
				b.Fatalf("Failed to create filesystem: %v", err)
			}

			b.ResetTimer()
			b.SetBytes(int64(size.size))

			for i := 0; i < b.N; i++ {
				reader, err := fs.CreateConcurrentReader(context.Background(), "s3://test-bucket/test-key")
				if err != nil {
					b.Fatalf("Failed to create concurrent reader: %v", err)
				}

				result, err := reader.ReadAll(context.Background())
				if err != nil {
					b.Fatalf("Failed to read all: %v", err)
				}

				if !bytes.Equal(result, data) {
					b.Fatal("Data mismatch")
				}
			}
		})
	}
}
