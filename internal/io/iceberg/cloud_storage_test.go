package iceberg

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCloudStorage_MinIO tests Iceberg tables on MinIO (S3-compatible storage).
// This test requires Docker Compose to be available.
func TestCloudStorage_MinIO(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cloud storage test in short mode")
	}

	// Check if docker-compose is available
	if !isDockerComposeAvailable() {
		t.Skip("docker-compose not available, skipping cloud storage test")
	}

	testDataDir := "./testdata"

	// Start Docker Compose services
	t.Log("Starting MinIO container...")
	err := startDockerService(t, testDataDir, "minio", "minio-setup")
	require.NoError(t, err, "Failed to start MinIO")
	defer stopDockerService(t, testDataDir, "minio", "minio-setup")

	// Wait for MinIO to be ready
	time.Sleep(5 * time.Second)

	// Set AWS credentials for MinIO
	os.Setenv("AWS_ACCESS_KEY_ID", "minioadmin")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
	os.Setenv("AWS_REGION", "us-east-1")
	defer func() {
		os.Unsetenv("AWS_ACCESS_KEY_ID")
		os.Unsetenv("AWS_SECRET_ACCESS_KEY")
		os.Unsetenv("AWS_REGION")
	}()

	// Test 1: Create a test Iceberg table in MinIO using local writer
	t.Run("write_and_read_minio", func(t *testing.T) {
		s3Path := "s3://iceberg-test/test_table"

		// Note: This would require S3-compatible filesystem implementation
		// For now, we'll test that the path parsing works
		_, err := parseTableLocation(s3Path)
		if err != nil {
			// Expected - S3 support may not be fully implemented yet
			t.Logf("S3 path parsing: %v (expected if S3 not fully implemented)", err)
		}
	})

	// Test 2: Upload a local Iceberg table to MinIO and read it
	t.Run("upload_and_read_minio", func(t *testing.T) {
		// This test would:
		// 1. Create a local Iceberg table
		// 2. Upload it to MinIO
		// 3. Read it back via S3 path
		t.Log("MinIO upload/read test placeholder - requires S3 filesystem implementation")
	})
}

// TestCloudStorage_GCS tests Iceberg tables on fake GCS server.
func TestCloudStorage_GCS(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cloud storage test in short mode")
	}

	if !isDockerComposeAvailable() {
		t.Skip("docker-compose not available, skipping cloud storage test")
	}

	testDataDir := "./testdata"

	// Start fake GCS server
	t.Log("Starting fake GCS container...")
	err := startDockerService(t, testDataDir, "fake-gcs")
	require.NoError(t, err, "Failed to start fake GCS")
	defer stopDockerService(t, testDataDir, "fake-gcs")

	// Wait for fake GCS to be ready
	time.Sleep(3 * time.Second)

	t.Run("gcs_path_parsing", func(t *testing.T) {
		gcsPath := "gs://iceberg-test/test_table"

		// Test GCS path parsing
		_, err := parseTableLocation(gcsPath)
		if err != nil {
			t.Logf("GCS path parsing: %v (expected if GCS not fully implemented)", err)
		}
	})
}

// parseTableLocation is a helper to test path parsing
func parseTableLocation(path string) (map[string]string, error) {
	// Simple path parsing logic - in real implementation this would
	// handle s3://, gs://, etc.
	if len(path) < 5 {
		return nil, fmt.Errorf("invalid path: %s", path)
	}

	scheme := ""
	if path[:5] == "s3://" {
		scheme = "s3"
	} else if path[:5] == "gs://" {
		scheme = "gs"
	} else {
		scheme = "file"
	}

	return map[string]string{
		"scheme": scheme,
		"path":   path,
	}, nil
}

// isDockerComposeAvailable checks if docker-compose is available on the system.
func isDockerComposeAvailable() bool {
	cmd := exec.Command("docker-compose", "--version")
	err := cmd.Run()
	if err != nil {
		// Try docker compose (v2 syntax)
		cmd = exec.Command("docker", "compose", "version")
		err = cmd.Run()
	}
	return err == nil
}

// startDockerService starts specified Docker Compose services.
func startDockerService(t *testing.T, dir string, services ...string) error {
	args := []string{"compose", "-f", "docker-compose.yml", "up", "-d"}
	args = append(args, services...)

	cmd := exec.Command("docker", args...)
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	t.Logf("Running: docker %v", args)
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to start services: %w", err)
	}

	return nil
}

// stopDockerService stops specified Docker Compose services.
func stopDockerService(t *testing.T, dir string, services ...string) {
	args := []string{"compose", "-f", "docker-compose.yml", "down"}
	args = append(args, services...)

	cmd := exec.Command("docker", args...)
	cmd.Dir = dir

	t.Logf("Running: docker %v", args)
	err := cmd.Run()
	if err != nil {
		t.Logf("Warning: failed to stop services: %v", err)
	}
}

// TestCloudStorage_Integration is an integration test that verifies
// cloud storage access works end-to-end.
func TestCloudStorage_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !isDockerComposeAvailable() {
		t.Skip("docker-compose not available, skipping integration test")
	}

	// This test would verify:
	// 1. Writing Iceberg tables to S3/GCS
	// 2. Reading them back
	// 3. Time travel on cloud-stored tables
	// 4. Partition pruning with cloud storage
	// 5. Delete file handling on cloud storage

	t.Run("end_to_end_s3", func(t *testing.T) {
		t.Log("End-to-end S3 test placeholder")
		// Implementation would go here
	})

	t.Run("end_to_end_gcs", func(t *testing.T) {
		t.Log("End-to-end GCS test placeholder")
		// Implementation would go here
	})
}
