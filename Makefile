.PHONY: test lint build clean

# Run tests
test:
	CGO_ENABLED=0 go test -v ./...

# Run linters
lint:
	golangci-lint run ./...

# Build the package (verify compilation)
build:
	CGO_ENABLED=0 go build ./...

# Verify cgo is rejected
build-cgo:
	@echo "Verifying that CGO_ENABLED=1 build fails..."
	@if CGO_ENABLED=1 go build ./... 2>/dev/null; then \
		echo "ERROR: Build succeeded with CGO_ENABLED=1 - cgo constraint not working"; \
		exit 1; \
	else \
		echo "SUCCESS: CGO_ENABLED=1 build correctly rejected"; \
	fi

# Clean build artifacts
clean:
	go clean ./...

# Run all checks
check: lint build build-cgo test
