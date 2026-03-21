//go:build integration

package tests

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Task 5.1: TestS3GlobExpansion verifies that glob patterns expand across
// multiple CSV files on S3.
func TestS3GlobExpansion(t *testing.T) {
	client, bucket := minioTestBucket(t)
	db := openDB(t)

	// Upload 3 CSV files under data/ prefix.
	csv1 := []byte("id,name\n1,Alice\n2,Bob\n")
	csv2 := []byte("id,name\n3,Charlie\n4,Dave\n")
	csv3 := []byte("id,name\n5,Eve\n")

	uploadBytes(t, client, bucket, "data/part1.csv", "text/csv", csv1)
	uploadBytes(t, client, bucket, "data/part2.csv", "text/csv", csv2)
	uploadBytes(t, client, bucket, "data/part3.csv", "text/csv", csv3)

	_, err := db.Exec(createSecretSQL("glob_secret"))
	require.NoError(t, err)

	s3URL := fmt.Sprintf("s3://%s/data/*.csv", bucket)
	var count int64
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM read_csv('%s')", s3URL)).Scan(&count)
	require.NoError(t, err)

	// 2 + 2 + 1 = 5 total rows across 3 files.
	assert.Equal(t, int64(5), count, "expected 5 total rows from 3 CSV files")
}

// Task 5.2: TestS3RecursiveGlob verifies that ** recursive glob patterns
// expand across nested directories on S3.
func TestS3RecursiveGlob(t *testing.T) {
	client, bucket := minioTestBucket(t)
	db := openDB(t)

	// Upload files under nested directory structure.
	jan2024 := []byte("id,name\n1,Jan2024\n")
	feb2024 := []byte("id,name\n2,Feb2024\n")
	jan2025 := []byte("id,name\n3,Jan2025\n")

	uploadBytes(t, client, bucket, "data/2024/jan.csv", "text/csv", jan2024)
	uploadBytes(t, client, bucket, "data/2024/feb.csv", "text/csv", feb2024)
	uploadBytes(t, client, bucket, "data/2025/jan.csv", "text/csv", jan2025)

	_, err := db.Exec(createSecretSQL("recursive_glob_secret"))
	require.NoError(t, err)

	s3URL := fmt.Sprintf("s3://%s/data/**/*.csv", bucket)
	var count int64
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM read_csv('%s')", s3URL)).Scan(&count)
	require.NoError(t, err)

	// 1 + 1 + 1 = 3 total rows across 3 files.
	assert.Equal(t, int64(3), count, "expected 3 total rows from recursive glob")
}

// Task 5.3: TestS3GlobNoMatches verifies that querying a glob pattern with
// no matching files returns an appropriate error.
func TestS3GlobNoMatches(t *testing.T) {
	_, bucket := minioTestBucket(t)
	db := openDB(t)

	_, err := db.Exec(createSecretSQL("no_match_secret"))
	require.NoError(t, err)

	s3URL := fmt.Sprintf("s3://%s/nonexistent/*.csv", bucket)
	_, err = db.Query(fmt.Sprintf("SELECT * FROM read_csv('%s')", s3URL))
	require.Error(t, err, "expected error when no files match glob pattern")

	errMsg := strings.ToLower(err.Error())
	assert.True(t,
		strings.Contains(errMsg, "no files") ||
			strings.Contains(errMsg, "no match") ||
			strings.Contains(errMsg, "not found") ||
			strings.Contains(errMsg, "pattern") ||
			strings.Contains(errMsg, "empty"),
		"error should mention no files matched, got: %s", err.Error())
}

// Task 6.1: TestS3ParquetRoundTrip verifies writing Parquet to S3 via COPY TO
// and reading it back via read_parquet.
func TestS3ParquetRoundTrip(t *testing.T) {
	_, bucket := minioTestBucket(t)
	db := openDB(t)

	// Create and populate source table.
	_, err := db.Exec("CREATE TABLE parquet_rt(id INTEGER, name VARCHAR, value DOUBLE)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO parquet_rt VALUES (1, 'Alpha', 1.1), (2, 'Beta', 2.2), (3, 'Gamma', 3.3)")
	require.NoError(t, err)

	_, err = db.Exec(createSecretSQL("parquet_rt_secret"))
	require.NoError(t, err)

	// Write to S3.
	s3URL := fmt.Sprintf("s3://%s/roundtrip.parquet", bucket)
	_, err = db.Exec(fmt.Sprintf("COPY parquet_rt TO '%s' (FORMAT PARQUET)", s3URL))
	require.NoError(t, err)

	// Read back from S3.
	rows, err := db.Query(fmt.Sprintf("SELECT id, name, value FROM read_parquet('%s') ORDER BY id", s3URL))
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
	assert.Equal(t, "Alpha", results[0].name)
	assert.InDelta(t, 1.1, results[0].value, 0.01)
	assert.Equal(t, int64(2), results[1].id)
	assert.Equal(t, "Beta", results[1].name)
	assert.InDelta(t, 2.2, results[1].value, 0.01)
	assert.Equal(t, int64(3), results[2].id)
	assert.Equal(t, "Gamma", results[2].name)
	assert.InDelta(t, 3.3, results[2].value, 0.01)
}

// Task 6.2: TestS3CSVRoundTrip verifies writing CSV to S3 via COPY TO
// and reading it back via read_csv.
func TestS3CSVRoundTrip(t *testing.T) {
	_, bucket := minioTestBucket(t)
	db := openDB(t)

	// Create and populate source table.
	_, err := db.Exec("CREATE TABLE csv_rt(id INTEGER, name VARCHAR, score DOUBLE)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO csv_rt VALUES (10, 'X', 99.9), (20, 'Y', 88.8)")
	require.NoError(t, err)

	_, err = db.Exec(createSecretSQL("csv_rt_secret"))
	require.NoError(t, err)

	// Write to S3.
	s3URL := fmt.Sprintf("s3://%s/roundtrip.csv", bucket)
	_, err = db.Exec(fmt.Sprintf("COPY csv_rt TO '%s' (FORMAT CSV, HEADER true)", s3URL))
	require.NoError(t, err)

	// Read back from S3.
	rows, err := db.Query(fmt.Sprintf("SELECT id, name, score FROM read_csv('%s') ORDER BY id", s3URL))
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	type row struct {
		id    int64
		name  string
		score float64
	}
	var results []row
	for rows.Next() {
		var r row
		err := rows.Scan(&r.id, &r.name, &r.score)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 2)
	assert.Equal(t, int64(10), results[0].id)
	assert.Equal(t, "X", results[0].name)
	assert.InDelta(t, 99.9, results[0].score, 0.01)
	assert.Equal(t, int64(20), results[1].id)
	assert.Equal(t, "Y", results[1].name)
	assert.InDelta(t, 88.8, results[1].score, 0.01)
}

// Task 6.3: TestS3NoSecretError verifies that querying an S3 URL without
// a configured secret returns an actionable error message.
func TestS3NoSecretError(t *testing.T) {
	db := openDB(t)

	// Query S3 without any secret configured.
	_, err := db.Query("SELECT * FROM read_csv('s3://nonexistent-bucket/data.csv')")
	require.Error(t, err, "expected error when no S3 secret is configured")

	errMsg := strings.ToLower(err.Error())
	assert.True(t,
		strings.Contains(errMsg, "secret") ||
			strings.Contains(errMsg, "credential") ||
			strings.Contains(errMsg, "authentication") ||
			strings.Contains(errMsg, "no s3") ||
			strings.Contains(errMsg, "not found") ||
			strings.Contains(errMsg, "no matching"),
		"error should mention credentials/secrets, got: %s", err.Error())
}

// Task 6.4: TestS3ErrorUsesMsgField verifies that S3 errors returned through
// database/sql carry a useful message, either as a *dukdb.Error with a
// non-empty Msg field or as a descriptive error string.
func TestS3ErrorUsesMsgField(t *testing.T) {
	db := openDB(t)

	// Query S3 without a secret -- should produce a meaningful error.
	_, err := db.Query("SELECT * FROM read_csv('s3://nonexistent-bucket/data.csv')")
	require.Error(t, err, "expected error when no S3 secret is configured")

	// Check if we can unwrap to a *dukdb.Error.
	var dukErr *dukdb.Error
	if errors.As(err, &dukErr) {
		assert.NotEmpty(t, dukErr.Msg,
			"dukdb.Error.Msg should not be empty for S3 errors")
		t.Logf("dukdb.Error.Msg = %q", dukErr.Msg)
	} else {
		// If the error is not a *dukdb.Error (e.g. wrapped by database/sql),
		// at minimum the error string should be non-empty and descriptive.
		errStr := err.Error()
		assert.NotEmpty(t, errStr, "error string should not be empty")
		assert.True(t, len(errStr) > 10,
			"error message should be descriptive, got: %s", errStr)
		t.Logf("error (not *dukdb.Error): %s", errStr)
	}
}

// TestS3ParquetRoundTripLocal is a helper test to verify Parquet COPY TO / read_parquet
// works locally before testing S3. This is not an S3 test per se but validates
// the local path works. Kept here for debugging convenience.
func TestS3ParquetRoundTripLocal(t *testing.T) {
	db := openDB(t)

	_, err := db.Exec("CREATE TABLE local_prt(id INTEGER, name VARCHAR)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO local_prt VALUES (1, 'Hello'), (2, 'World')")
	require.NoError(t, err)

	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "local.parquet")
	_, err = db.Exec(fmt.Sprintf("COPY local_prt TO '%s' (FORMAT PARQUET)", parquetPath))
	require.NoError(t, err)

	// Verify the file was created.
	_, err = os.Stat(parquetPath)
	require.NoError(t, err, "parquet file should exist")

	var count int64
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", parquetPath)).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)
}
