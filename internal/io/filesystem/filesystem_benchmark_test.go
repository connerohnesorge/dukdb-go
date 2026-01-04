package filesystem

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// BenchmarkLocalFileSystemRead benchmarks local filesystem read operations.
// This serves as the baseline for cloud filesystem comparisons.
func BenchmarkLocalFileSystemRead(b *testing.B) {
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
			// Create temp file with test data
			tmpDir := b.TempDir()
			tmpFile := filepath.Join(tmpDir, "test.bin")

			data := make([]byte, size.size)
			for i := range data {
				data[i] = byte(i % 256)
			}

			if err := os.WriteFile(tmpFile, data, 0644); err != nil {
				b.Fatalf("Failed to create test file: %v", err)
			}

			fs := NewLocalFileSystem("")

			b.ResetTimer()
			b.SetBytes(int64(size.size))

			for i := 0; i < b.N; i++ {
				file, err := fs.Open(tmpFile)
				if err != nil {
					b.Fatalf("Failed to open file: %v", err)
				}

				buf := make([]byte, size.size)
				_, err = io.ReadFull(file, buf)
				if err != nil && err != io.EOF {
					b.Fatalf("Failed to read file: %v", err)
				}

				_ = file.Close()
			}
		})
	}
}

// BenchmarkLocalFileSystemWrite benchmarks local filesystem write operations.
func BenchmarkLocalFileSystemWrite(b *testing.B) {
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
			tmpDir := b.TempDir()

			data := make([]byte, size.size)
			for i := range data {
				data[i] = byte(i % 256)
			}

			fs := NewLocalFileSystem("")

			b.ResetTimer()
			b.SetBytes(int64(size.size))

			for i := 0; i < b.N; i++ {
				tmpFile := filepath.Join(tmpDir, fmt.Sprintf("test_%d.bin", i))

				file, err := fs.Create(tmpFile)
				if err != nil {
					b.Fatalf("Failed to create file: %v", err)
				}

				_, err = file.Write(data)
				if err != nil {
					b.Fatalf("Failed to write file: %v", err)
				}

				_ = file.Close()
			}
		})
	}
}

// BenchmarkLocalFileSystemReadAt benchmarks local filesystem random read operations.
func BenchmarkLocalFileSystemReadAt(b *testing.B) {
	dataSize := 10 * 1024 * 1024 // 10MB

	tmpDir := b.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.bin")

	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	fs := NewLocalFileSystem("")

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
			file, err := fs.Open(tmpFile)
			if err != nil {
				b.Fatalf("Failed to open file: %v", err)
			}
			defer func() { _ = file.Close() }()

			b.ResetTimer()
			b.SetBytes(int64(readSize.size))

			buf := make([]byte, readSize.size)

			for i := 0; i < b.N; i++ {
				offset := int64((i * 1234) % (dataSize - readSize.size))
				_, err := file.ReadAt(buf, offset)
				if err != nil && err != io.EOF {
					b.Fatalf("Failed to read at offset %d: %v", offset, err)
				}
			}
		})
	}
}

// BenchmarkLocalFileSystemStat benchmarks local filesystem stat operations.
func BenchmarkLocalFileSystemStat(b *testing.B) {
	tmpDir := b.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.bin")

	data := make([]byte, 1024)
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	fs := NewLocalFileSystem("")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := fs.Stat(tmpFile)
		if err != nil {
			b.Fatalf("Failed to stat file: %v", err)
		}
	}
}

// BenchmarkLocalVsS3Read compares local vs mock S3 read performance.
// This benchmark shows the overhead of the S3 abstraction layer.
func BenchmarkLocalVsS3Read(b *testing.B) {
	dataSize := 64 * 1024 // 64KB

	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Setup mock S3
	mockS3 := NewBenchmarkMockS3Server(data)
	defer mockS3.Close()

	s3Config := S3Config{
		Endpoint:        strings.TrimPrefix(mockS3.URL, "http://"),
		UseSSL:          false,
		URLStyle:        S3URLStylePath,
		Region:          "us-east-1",
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	}

	s3FS, err := NewS3FileSystem(context.Background(), s3Config)
	if err != nil {
		b.Fatalf("Failed to create S3 filesystem: %v", err)
	}

	// Setup local file
	tmpDir := b.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.bin")
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	localFS := NewLocalFileSystem("")

	b.Run("Local", func(b *testing.B) {
		b.SetBytes(int64(dataSize))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			file, err := localFS.Open(tmpFile)
			if err != nil {
				b.Fatalf("Failed to open file: %v", err)
			}

			_, err = io.ReadAll(file)
			if err != nil {
				b.Fatalf("Failed to read: %v", err)
			}

			_ = file.Close()
		}
	})

	b.Run("MockS3", func(b *testing.B) {
		b.SetBytes(int64(dataSize))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			file, err := s3FS.Open("s3://test-bucket/test-key")
			if err != nil {
				b.Fatalf("Failed to open file: %v", err)
			}

			_, err = io.ReadAll(file)
			if err != nil {
				b.Fatalf("Failed to read: %v", err)
			}

			_ = file.Close()
		}
	})
}

// BenchmarkLocalVsHTTPRead compares local vs mock HTTP read performance.
func BenchmarkLocalVsHTTPRead(b *testing.B) {
	dataSize := 64 * 1024 // 64KB

	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Setup mock HTTP
	mockHTTP := NewBenchmarkMockHTTPServer(data)
	defer mockHTTP.Close()

	httpFS, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	if err != nil {
		b.Fatalf("Failed to create HTTP filesystem: %v", err)
	}

	// Setup local file
	tmpDir := b.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.bin")
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	localFS := NewLocalFileSystem("")

	b.Run("Local", func(b *testing.B) {
		b.SetBytes(int64(dataSize))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			file, err := localFS.Open(tmpFile)
			if err != nil {
				b.Fatalf("Failed to open file: %v", err)
			}

			_, err = io.ReadAll(file)
			if err != nil {
				b.Fatalf("Failed to read: %v", err)
			}

			_ = file.Close()
		}
	})

	b.Run("MockHTTP", func(b *testing.B) {
		b.SetBytes(int64(dataSize))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			file, err := httpFS.Open(mockHTTP.URL + "/test.bin")
			if err != nil {
				b.Fatalf("Failed to open file: %v", err)
			}

			_, err = io.ReadAll(file)
			if err != nil {
				b.Fatalf("Failed to read: %v", err)
			}

			_ = file.Close()
		}
	})
}

// BenchmarkAllFilesystemsRead compares all filesystem implementations.
func BenchmarkAllFilesystemsRead(b *testing.B) {
	dataSize := 64 * 1024 // 64KB

	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Setup mock servers
	mockS3 := NewBenchmarkMockS3Server(data)
	defer mockS3.Close()

	mockHTTP := NewBenchmarkMockHTTPServer(data)
	defer mockHTTP.Close()

	// Setup local file
	tmpDir := b.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.bin")
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	// Create filesystems
	localFS := NewLocalFileSystem("")

	s3Config := S3Config{
		Endpoint:        strings.TrimPrefix(mockS3.URL, "http://"),
		UseSSL:          false,
		URLStyle:        S3URLStylePath,
		Region:          "us-east-1",
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	}
	s3FS, err := NewS3FileSystem(context.Background(), s3Config)
	if err != nil {
		b.Fatalf("Failed to create S3 filesystem: %v", err)
	}

	httpFS, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	if err != nil {
		b.Fatalf("Failed to create HTTP filesystem: %v", err)
	}

	type fsTest struct {
		name string
		fs   FileSystem
		path string
	}

	tests := []fsTest{
		{"Local", localFS, tmpFile},
		{"S3_Mock", s3FS, "s3://test-bucket/test-key"},
		{"HTTP_Mock", httpFS, mockHTTP.URL + "/test.bin"},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			b.SetBytes(int64(dataSize))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				file, err := test.fs.Open(test.path)
				if err != nil {
					b.Fatalf("Failed to open file: %v", err)
				}

				_, err = io.ReadAll(file)
				if err != nil {
					b.Fatalf("Failed to read: %v", err)
				}

				_ = file.Close()
			}
		})
	}
}

// BenchmarkAllFilesystemsStat compares stat operations across filesystems.
func BenchmarkAllFilesystemsStat(b *testing.B) {
	data := make([]byte, 1024) // 1KB for stat tests

	// Setup mock servers
	mockS3 := NewBenchmarkMockS3Server(data)
	defer mockS3.Close()

	mockHTTP := NewBenchmarkMockHTTPServer(data)
	defer mockHTTP.Close()

	// Setup local file
	tmpDir := b.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.bin")
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	// Create filesystems
	localFS := NewLocalFileSystem("")

	s3Config := S3Config{
		Endpoint:        strings.TrimPrefix(mockS3.URL, "http://"),
		UseSSL:          false,
		URLStyle:        S3URLStylePath,
		Region:          "us-east-1",
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	}
	s3FS, err := NewS3FileSystem(context.Background(), s3Config)
	if err != nil {
		b.Fatalf("Failed to create S3 filesystem: %v", err)
	}

	httpFS, err := NewHTTPFileSystem(context.Background(), DefaultHTTPConfig())
	if err != nil {
		b.Fatalf("Failed to create HTTP filesystem: %v", err)
	}

	type fsTest struct {
		name string
		fs   FileSystem
		path string
	}

	tests := []fsTest{
		{"Local", localFS, tmpFile},
		{"S3_Mock", s3FS, "s3://test-bucket/test-key"},
		{"HTTP_Mock", httpFS, mockHTTP.URL + "/test.bin"},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := test.fs.Stat(test.path)
				if err != nil {
					b.Fatalf("Failed to stat: %v", err)
				}
			}
		})
	}
}

// BenchmarkFileSystemFactory benchmarks the filesystem factory creation.
func BenchmarkFileSystemFactory(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = NewFileSystemFactory()
	}
}

// BenchmarkFileSystemFactoryOpen benchmarks opening files through the factory.
func BenchmarkFileSystemFactoryOpen(b *testing.B) {
	dataSize := 1024 // 1KB

	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Setup mock HTTP server
	mockHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	defer mockHTTP.Close()

	// Setup local file
	tmpDir := b.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.bin")
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	factory := NewFileSystemFactory()

	tests := []struct {
		name string
		url  string
	}{
		{"Local", "file://" + tmpFile},
		{"HTTP", mockHTTP.URL + "/test.bin"},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				fs, err := factory.GetFileSystem(context.Background(), test.url)
				if err != nil {
					b.Fatalf("Failed to get filesystem: %v", err)
				}

				file, err := fs.Open(test.url)
				if err != nil {
					b.Fatalf("Failed to open: %v", err)
				}

				_, err = io.ReadAll(file)
				if err != nil {
					b.Fatalf("Failed to read: %v", err)
				}

				_ = file.Close()
			}
		})
	}
}

// BenchmarkIsCloudURL benchmarks the cloud URL detection.
func BenchmarkIsCloudURL(b *testing.B) {
	urls := []string{
		"s3://bucket/key",
		"gs://bucket/object",
		"azure://container/blob",
		"http://example.com/file",
		"https://example.com/file",
		"/local/path/file.csv",
		"file:///local/path/file.csv",
	}

	for _, url := range urls {
		name := strings.ReplaceAll(url, "/", "_")
		b.Run(name, func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_ = IsCloudURL(url)
			}
		})
	}
}

// BenchmarkFileSizes benchmarks different file sizes to find optimal chunk sizes.
func BenchmarkFileSizes(b *testing.B) {
	sizes := []struct {
		name      string
		size      int
		chunkSize int
	}{
		{"1KB_256B_chunk", 1024, 256},
		{"1KB_1KB_chunk", 1024, 1024},
		{"64KB_4KB_chunk", 64 * 1024, 4 * 1024},
		{"64KB_64KB_chunk", 64 * 1024, 64 * 1024},
		{"1MB_64KB_chunk", 1024 * 1024, 64 * 1024},
		{"1MB_256KB_chunk", 1024 * 1024, 256 * 1024},
		{"1MB_1MB_chunk", 1024 * 1024, 1024 * 1024},
	}

	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			data := make([]byte, s.size)
			for i := range data {
				data[i] = byte(i % 256)
			}

			tmpDir := b.TempDir()
			tmpFile := filepath.Join(tmpDir, "test.bin")
			if err := os.WriteFile(tmpFile, data, 0644); err != nil {
				b.Fatalf("Failed to create test file: %v", err)
			}

			fs := NewLocalFileSystem("")

			b.ResetTimer()
			b.SetBytes(int64(s.size))

			for i := 0; i < b.N; i++ {
				file, err := fs.Open(tmpFile)
				if err != nil {
					b.Fatalf("Failed to open: %v", err)
				}

				buf := make([]byte, s.chunkSize)
				for {
					n, err := file.Read(buf)
					if err == io.EOF || n == 0 {
						break
					}
					if err != nil {
						b.Fatalf("Failed to read: %v", err)
					}
				}

				_ = file.Close()
			}
		})
	}
}
