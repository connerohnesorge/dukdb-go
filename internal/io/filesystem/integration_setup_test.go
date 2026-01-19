//go:build integration

package filesystem

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/azure/azurite"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Global integration test containers and contexts
var (
	// Azure Azurite container state
	azuriteCtx          context.Context
	azuriteTestContainer *azurite.Container
	azuriteEndpoint     string

	// LocalStack S3 container state
	localstackContainer *localstack.LocalStackContainer
)

// setupAzuriteContainer starts an Azurite container and returns the container, endpoint, and error.
func setupAzuriteContainer(ctx context.Context) (*azurite.Container, string, error) {
	// Create Azurite container
	container, err := azurite.Run(ctx,
		"mcr.microsoft.com/azure-storage/azurite:latest",
		testcontainers.WithWaitStrategy(wait.ForLog("Azurite Blob service is successfully listening")),
	)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create Azurite container: %w", err)
	}

	// Get the blob service URL
	baseURL, err := container.BlobServiceURL(ctx)
	if err != nil {
		container.Terminate(ctx) //nolint:errcheck
		return nil, "", fmt.Errorf("failed to get Azurite blob service URL: %w", err)
	}

	// Append the account name to match Azure SDK expectations
	endpoint := baseURL + "/" + azuriteAccountName
	if !strings.HasSuffix(endpoint, "/") {
		endpoint += "/"
	}

	return container, endpoint, nil
}

// setupLocalStackContainer creates and starts a LocalStack container for testing.
func setupLocalStackContainer(ctx context.Context) (*localstack.LocalStackContainer, string, error) {
	// Run LocalStack container with S3 service enabled
	container, err := localstack.Run(
		ctx,
		"localstack/localstack:latest",
		testcontainers.WithEnv(map[string]string{"SERVICES": "s3"}),
		testcontainers.WithWaitStrategy(wait.ForLog("Ready.")),
	)
	if err != nil {
		return nil, "", fmt.Errorf("failed to start LocalStack container: %w", err)
	}

	// Get the mapped port for the LocalStack service (default 4566)
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		container.Terminate(ctx) //nolint:errcheck
		return nil, "", fmt.Errorf("failed to create Docker provider: %w", err)
	}

	host, err := provider.DaemonHost(ctx)
	if err != nil {
		container.Terminate(ctx) //nolint:errcheck
		return nil, "", fmt.Errorf("failed to get Docker daemon host: %w", err)
	}

	mappedPort, err := container.MappedPort(ctx, "4566/tcp")
	if err != nil {
		container.Terminate(ctx) //nolint:errcheck
		return nil, "", fmt.Errorf("failed to get mapped port: %w", err)
	}

	// Construct the endpoint URL
	endpoint := fmt.Sprintf("http://%s:%s", host, mappedPort.Port())

	return container, endpoint, nil
}

// TestMain sets up all integration test containers and manages their lifecycle.
func TestMain(m *testing.M) {
	mainCtx := context.Background()
	
	// Setup Azurite container
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	azuriteCtx = context.Background()
	
	var azuriteErr error
	azuriteTestContainer, azuriteEndpoint, azuriteErr = setupAzuriteContainer(setupCtx)
	setupCancel()
	
	if azuriteErr != nil {
		fmt.Fprintf(os.Stderr, "Failed to start Azurite container: %v\n", azuriteErr)
		os.Exit(1)
	}

	// Setup LocalStack container
	setupCtx, setupCancel = context.WithTimeout(mainCtx, 2*time.Minute)
	var localstackErr error
	var localstackEndpoint string
	localstackContainer, localstackEndpoint, localstackErr = setupLocalStackContainer(setupCtx)
	setupCancel()
	
	if localstackErr != nil {
		fmt.Fprintf(os.Stderr, "Failed to start LocalStack container: %v\n", localstackErr)
		// Clean up Azurite before exiting
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		if azuriteTestContainer != nil {
			azuriteTestContainer.Terminate(cleanupCtx) //nolint:errcheck
		}
		cleanupCancel()
		os.Exit(1)
	}

	// Set the LocalStack endpoint for tests
	os.Setenv("LOCALSTACK_ENDPOINT", localstackEndpoint)
	
	// Run tests
	code := m.Run()

	// Cleanup both containers with a fresh context
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if azuriteTestContainer != nil {
		if err := azuriteTestContainer.Terminate(cleanupCtx); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to terminate Azurite container: %v\n", err)
		}
	}
	if localstackContainer != nil {
		if err := localstackContainer.Terminate(cleanupCtx); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to terminate LocalStack container: %v\n", err)
		}
	}
	cleanupCancel()

	os.Exit(code)
}

// isIntegrationTest returns true if tests are being run with the integration build tag.
func isIntegrationTest() bool {
	// This function is called during TestMain, which only runs when tests are executed.
	// If we're here and have integration tests, we should proceed.
	return true
}
