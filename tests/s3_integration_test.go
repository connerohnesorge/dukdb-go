//go:build integration

package tests

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	minioEndpoint  = "localhost:9000"
	minioAccessKey = "minioadmin"
	minioSecretKey = "minioadmin"
	minioRegion    = "us-east-1"
	minioUseSSL    = false
)

// minioTestBucket creates a MinIO client, a uniquely named test bucket,
// and returns the client, bucket name, and a cleanup function.
func minioTestBucket(t *testing.T) (*minio.Client, string) {
	t.Helper()

	client, err := minio.New(minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(minioAccessKey, minioSecretKey, ""),
		Secure: minioUseSSL,
		Region: minioRegion,
	})
	if err != nil {
		t.Skipf("MinIO not available: %v", err)
	}

	// Verify connectivity by listing buckets.
	ctx := context.Background()
	_, err = client.ListBuckets(ctx)
	if err != nil {
		t.Skipf("MinIO not available: %v", err)
	}

	bucket := fmt.Sprintf("s3inttest-%d", rand.Int63())
	err = client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: minioRegion})
	require.NoError(t, err, "failed to create test bucket")

	t.Cleanup(func() {
		// Remove all objects then the bucket.
		objCh := client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true})
		for obj := range objCh {
			if obj.Err != nil {
				continue
			}
			_ = client.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{})
		}
		_ = client.RemoveBucket(ctx, bucket)
	})

	return client, bucket
}

// uploadOpts holds parameters for uploading bytes to MinIO.
type uploadOpts struct {
	client      *minio.Client
	bucket      string
	key         string
	contentType string
	data        []byte
}

// uploadBytes uploads raw bytes to a MinIO bucket under the given key.
func uploadBytes(t *testing.T, opts *uploadOpts) {
	t.Helper()
	ctx := context.Background()
	_, err := opts.client.PutObject(ctx, opts.bucket, opts.key, bytes.NewReader(opts.data), int64(len(opts.data)), minio.PutObjectOptions{
		ContentType: opts.contentType,
	})
	require.NoError(t, err, "failed to upload %s to bucket %s", opts.key, opts.bucket)
}

// createSecretSQL returns the SQL statement to create an S3 secret for MinIO.
func createSecretSQL(name string) string {
	return fmt.Sprintf(`CREATE SECRET %s (
		TYPE S3,
		KEY_ID '%s',
		SECRET '%s',
		ENDPOINT '%s',
		URL_STYLE 'path',
		USE_SSL 'false',
		REGION '%s'
	)`, name, minioAccessKey, minioSecretKey, minioEndpoint, minioRegion)
}

// openDB opens a fresh in-memory dukdb connection for testing.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	return db
}

// Task 4.1
func TestS3ReadParquetWithSecret(t *testing.T) {
	client, bucket := minioTestBucket(t)
	db := openDB(t)

	// Create a table and export it as parquet locally, then upload.
	_, err := db.Exec("CREATE TABLE parquet_src(id INTEGER, name VARCHAR, value DOUBLE)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO parquet_src VALUES (1, 'Alice', 10.5), (2, 'Bob', 20.3), (3, 'Charlie', 30.1)")
	require.NoError(t, err)

	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "data.parquet")
	_, err = db.Exec(fmt.Sprintf("COPY parquet_src TO '%s' (FORMAT PARQUET)", parquetPath))
	require.NoError(t, err)

	parquetData, err := os.ReadFile(parquetPath)
	require.NoError(t, err)
	uploadBytes(t, &uploadOpts{client: client, bucket: bucket, key: "data.parquet", contentType: "application/octet-stream", data: parquetData})

	// Create secret and read from S3.
	_, err = db.Exec(createSecretSQL("my_parquet_secret"))
	require.NoError(t, err)

	s3URL := fmt.Sprintf("s3://%s/data.parquet", bucket)
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM read_parquet('%s')", s3URL))
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	type row struct {
		id    int64
		name  string
		value float64
	}
	var results []row
	for rows.Next() {
		var r row
		err := rows.Scan(&r.id, &r.name, &r.value)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 3)
	assert.Equal(t, int64(1), results[0].id)
	assert.Equal(t, "Alice", results[0].name)
	assert.InDelta(t, 10.5, results[0].value, 0.01)
	assert.Equal(t, int64(2), results[1].id)
	assert.Equal(t, "Bob", results[1].name)
	assert.InDelta(t, 20.3, results[1].value, 0.01)
	assert.Equal(t, int64(3), results[2].id)
	assert.Equal(t, "Charlie", results[2].name)
	assert.InDelta(t, 30.1, results[2].value, 0.01)
}

// Task 4.2
func TestS3ReadCSVWithSecret(t *testing.T) {
	client, bucket := minioTestBucket(t)
	db := openDB(t)

	csvData := []byte("id,name,value\n1,Alice,100.5\n2,Bob,200.3\n3,Charlie,300.1\n")
	uploadBytes(t, &uploadOpts{client: client, bucket: bucket, key: "data.csv", contentType: "text/csv", data: csvData})

	_, err := db.Exec(createSecretSQL("my_csv_secret"))
	require.NoError(t, err)

	s3URL := fmt.Sprintf("s3://%s/data.csv", bucket)
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM read_csv('%s')", s3URL))
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	type row struct {
		id    int64
		name  string
		value float64
	}
	var results []row
	for rows.Next() {
		var r row
		err := rows.Scan(&r.id, &r.name, &r.value)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 3)
	assert.Equal(t, int64(1), results[0].id)
	assert.Equal(t, "Alice", results[0].name)
	assert.InDelta(t, 100.5, results[0].value, 0.01)
	assert.Equal(t, int64(2), results[1].id)
	assert.Equal(t, "Bob", results[1].name)
	assert.InDelta(t, 200.3, results[1].value, 0.01)
	assert.Equal(t, int64(3), results[2].id)
	assert.Equal(t, "Charlie", results[2].name)
	assert.InDelta(t, 300.1, results[2].value, 0.01)
}

// Task 4.3
func TestS3ReadJSONWithSecret(t *testing.T) {
	client, bucket := minioTestBucket(t)
	db := openDB(t)

	jsonData := []byte(`[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]`)
	uploadBytes(t, &uploadOpts{client: client, bucket: bucket, key: "data.json", contentType: "application/json", data: jsonData})

	_, err := db.Exec(createSecretSQL("my_json_secret"))
	require.NoError(t, err)

	s3URL := fmt.Sprintf("s3://%s/data.json", bucket)
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM read_json('%s')", s3URL))
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	type row struct {
		id   int64
		name string
	}
	var results []row
	for rows.Next() {
		var r row
		err := rows.Scan(&r.id, &r.name)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 2)
	assert.Equal(t, int64(1), results[0].id)
	assert.Equal(t, "Alice", results[0].name)
	assert.Equal(t, int64(2), results[1].id)
	assert.Equal(t, "Bob", results[1].name)
}

// Task 4.4
func TestS3CopyFromWithSecret(t *testing.T) {
	client, bucket := minioTestBucket(t)
	db := openDB(t)

	csvData := []byte("id,name,value\n1,Alice,100.5\n2,Bob,200.3\n3,Charlie,300.1\n")
	uploadBytes(t, &uploadOpts{client: client, bucket: bucket, key: "data.csv", contentType: "text/csv", data: csvData})

	_, err := db.Exec(createSecretSQL("my_copy_from_secret"))
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE test_table(id INTEGER, name VARCHAR, value DOUBLE)")
	require.NoError(t, err)

	s3URL := fmt.Sprintf("s3://%s/data.csv", bucket)
	_, err = db.Exec(fmt.Sprintf("COPY test_table FROM '%s' (FORMAT CSV, HEADER true)", s3URL))
	require.NoError(t, err)

	rows, err := db.Query("SELECT id, name, value FROM test_table ORDER BY id")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	type row struct {
		id    int64
		name  string
		value float64
	}
	var results []row
	for rows.Next() {
		var r row
		err := rows.Scan(&r.id, &r.name, &r.value)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 3)
	assert.Equal(t, int64(1), results[0].id)
	assert.Equal(t, "Alice", results[0].name)
	assert.InDelta(t, 100.5, results[0].value, 0.01)
	assert.Equal(t, int64(2), results[1].id)
	assert.Equal(t, "Bob", results[1].name)
	assert.InDelta(t, 200.3, results[1].value, 0.01)
	assert.Equal(t, int64(3), results[2].id)
	assert.Equal(t, "Charlie", results[2].name)
	assert.InDelta(t, 300.1, results[2].value, 0.01)
}

// Task 4.5
func TestS3CopyToWithSecret(t *testing.T) {
	client, bucket := minioTestBucket(t)
	db := openDB(t)

	_, err := db.Exec(createSecretSQL("my_copy_to_secret"))
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE export_table(id INTEGER, name VARCHAR, value DOUBLE)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO export_table VALUES (1, 'Alice', 100.5), (2, 'Bob', 200.3)")
	require.NoError(t, err)

	s3URL := fmt.Sprintf("s3://%s/output.csv", bucket)
	_, err = db.Exec(fmt.Sprintf("COPY export_table TO '%s' (FORMAT CSV, HEADER true)", s3URL))
	require.NoError(t, err)

	// Download the file from MinIO and verify content.
	ctx := context.Background()
	obj, err := client.GetObject(ctx, bucket, "output.csv", minio.GetObjectOptions{})
	require.NoError(t, err)
	defer func() { _ = obj.Close() }()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(obj)
	require.NoError(t, err)

	content := buf.String()
	assert.True(t, strings.Contains(content, "Alice"), "output CSV should contain Alice")
	assert.True(t, strings.Contains(content, "Bob"), "output CSV should contain Bob")
	// Verify it has a header line.
	lines := strings.Split(strings.TrimSpace(content), "\n")
	assert.GreaterOrEqual(t, len(lines), 3, "expected header + 2 data rows")
}

// Task 4.6
func TestS3ScopedSecretMatching(t *testing.T) {
	client, bucket := minioTestBucket(t)
	db := openDB(t)

	csvData := []byte("id,name\n1,Scoped\n2,Data\n")
	uploadBytes(t, &uploadOpts{client: client, bucket: bucket, key: "specific/data.csv", contentType: "text/csv", data: csvData})

	// Create a global secret with WRONG credentials (should fail if used).
	_, err := db.Exec(fmt.Sprintf(`CREATE SECRET global_bad_secret (
		TYPE S3,
		KEY_ID 'WRONG_KEY',
		SECRET 'WRONG_SECRET',
		ENDPOINT '%s',
		URL_STYLE 'path',
		USE_SSL 'false',
		REGION '%s'
	)`, minioEndpoint, minioRegion))
	require.NoError(t, err)

	// Create a scoped secret with correct credentials for the specific path.
	scopedPath := fmt.Sprintf("s3://%s/specific/", bucket)
	_, err = db.Exec(fmt.Sprintf(`CREATE SECRET scoped_good_secret (
		TYPE S3,
		KEY_ID '%s',
		SECRET '%s',
		ENDPOINT '%s',
		URL_STYLE 'path',
		USE_SSL 'false',
		REGION '%s',
		SCOPE '%s'
	)`, minioAccessKey, minioSecretKey, minioEndpoint, minioRegion, scopedPath))
	require.NoError(t, err)

	// Query using the scoped path should succeed because the scoped secret
	// with correct credentials is a better match than the global secret.
	s3URL := fmt.Sprintf("s3://%s/specific/data.csv", bucket)
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM read_csv('%s')", s3URL))
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	type row struct {
		id   int64
		name string
	}
	var results []row
	for rows.Next() {
		var r row
		err := rows.Scan(&r.id, &r.name)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 2)
	assert.Equal(t, int64(1), results[0].id)
	assert.Equal(t, "Scoped", results[0].name)
	assert.Equal(t, int64(2), results[1].id)
	assert.Equal(t, "Data", results[1].name)
}
