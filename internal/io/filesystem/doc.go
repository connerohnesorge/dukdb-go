// Package filesystem provides a pluggable filesystem interface for cloud and local storage.
//
// This package enables dukdb-go to read and write data files (CSV, JSON, Parquet)
// from various storage backends including:
//   - Local filesystem (file://)
//   - Amazon S3 (s3://)
//   - Google Cloud Storage (gs://, gcs://)
//   - Azure Blob Storage (azure://, az://)
//   - HTTP/HTTPS URLs (http://, https://)
//
// # Usage
//
// The primary entry point is the FileSystemFactory, which creates FileSystem
// instances based on URL schemes:
//
//	ctx := context.Background()
//	factory := filesystem.DefaultFactory()
//
//	// Get filesystem for a local file
//	fs, err := factory.GetFileSystem(ctx, "/path/to/file.csv")
//
//	// Get filesystem for an S3 object
//	fs, err := factory.GetFileSystem(ctx, "s3://bucket/key.parquet")
//
// Files can be opened for reading or writing:
//
//	file, err := fs.Open("s3://bucket/data.csv")
//	defer file.Close()
//
//	buf := make([]byte, 1024)
//	n, err := file.Read(buf)
//
// # Integration Points
//
// The FileSystem interface is designed for use in the following scenarios:
//
//   - COPY FROM/TO statements with cloud URLs
//   - Table functions (read_csv, read_parquet, etc.) with cloud paths
//   - Data export operations to cloud storage
//
// The core database persistence layer (internal/persistence) continues to use
// direct file operations as database files must reside on local storage.
//
// # Extending with New Schemes
//
// Custom filesystem implementations can be registered with the factory:
//
//	factory.RegisterScheme("myscheme", func(ctx context.Context, url string) (FileSystem, error) {
//	    return NewMyFileSystem(url), nil
//	})
//
// # Thread Safety
//
// The FileSystemFactory is thread-safe and can be used from multiple goroutines.
// Individual FileSystem implementations should document their thread-safety
// guarantees.
package filesystem
