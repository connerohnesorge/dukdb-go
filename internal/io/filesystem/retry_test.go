package filesystem

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultRetryConfig(t *testing.T) {
	cfg := DefaultRetryConfig()

	assert.Equal(t, 3, cfg.MaxRetries)
	assert.Equal(t, 100*time.Millisecond, cfg.InitialDelay)
	assert.Equal(t, 30*time.Second, cfg.MaxDelay)
	assert.Equal(t, 2.0, cfg.BackoffFactor)
	assert.Equal(t, 0.2, cfg.Jitter)
}

func TestNoRetryConfig(t *testing.T) {
	cfg := NoRetryConfig()

	assert.Equal(t, 0, cfg.MaxRetries)
}

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "context canceled",
			err:      context.Canceled,
			expected: false,
		},
		{
			name:     "context deadline exceeded",
			err:      context.DeadlineExceeded,
			expected: false,
		},
		{
			name:     "connection reset",
			err:      errors.New("connection reset by peer"),
			expected: true,
		},
		{
			name:     "connection refused",
			err:      errors.New("connection refused"),
			expected: true,
		},
		{
			name:     "timeout",
			err:      errors.New("connection timed out"),
			expected: true,
		},
		{
			name:     "service unavailable",
			err:      errors.New("service unavailable"),
			expected: true,
		},
		{
			name:     "internal server error",
			err:      errors.New("internal server error"),
			expected: true,
		},
		{
			name:     "bad gateway",
			err:      errors.New("bad gateway"),
			expected: true,
		},
		{
			name:     "gateway timeout",
			err:      errors.New("gateway timeout"),
			expected: true,
		},
		{
			name:     "throttling",
			err:      errors.New("request throttling"),
			expected: true,
		},
		{
			name:     "rate exceeded",
			err:      errors.New("rate exceeded"),
			expected: true,
		},
		{
			name:     "too many requests",
			err:      errors.New("too many requests"),
			expected: true,
		},
		{
			name:     "generic error",
			err:      errors.New("some random error"),
			expected: false,
		},
		{
			name:     "not found error",
			err:      errors.New("object not found"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsRetryableError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsRetryableHTTPStatus(t *testing.T) {
	tests := []struct {
		status   int
		expected bool
	}{
		{200, false},
		{201, false},
		{204, false},
		{400, false},
		{401, false},
		{403, false},
		{404, false},
		{408, true}, // Request Timeout
		{429, true}, // Too Many Requests
		{500, true}, // Internal Server Error
		{502, true}, // Bad Gateway
		{503, true}, // Service Unavailable
		{504, true}, // Gateway Timeout
	}

	for _, tt := range tests {
		t.Run(string(rune(tt.status)), func(t *testing.T) {
			result := IsRetryableHTTPStatus(tt.status)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestWithRetry_SuccessOnFirstAttempt(t *testing.T) {
	cfg := RetryConfig{
		MaxRetries:    3,
		InitialDelay:  10 * time.Millisecond,
		MaxDelay:      100 * time.Millisecond,
		BackoffFactor: 2.0,
	}

	attempts := 0
	fn := func() (int, error) {
		attempts++
		return 42, nil
	}

	result := WithRetry(context.Background(), cfg, fn)

	assert.NoError(t, result.LastError)
	assert.Equal(t, 42, result.Value)
	assert.Equal(t, 1, result.Attempts)
	assert.Equal(t, 1, attempts)
}

func TestWithRetry_SuccessAfterRetries(t *testing.T) {
	cfg := RetryConfig{
		MaxRetries:    3,
		InitialDelay:  1 * time.Millisecond,
		MaxDelay:      10 * time.Millisecond,
		BackoffFactor: 2.0,
	}

	attempts := 0
	fn := func() (int, error) {
		attempts++
		if attempts < 3 {
			return 0, errors.New("connection reset")
		}
		return 42, nil
	}

	result := WithRetry(context.Background(), cfg, fn)

	assert.NoError(t, result.LastError)
	assert.Equal(t, 42, result.Value)
	assert.Equal(t, 3, result.Attempts)
	assert.Equal(t, 3, attempts)
}

func TestWithRetry_FailureAfterAllRetries(t *testing.T) {
	cfg := RetryConfig{
		MaxRetries:    2,
		InitialDelay:  1 * time.Millisecond,
		MaxDelay:      10 * time.Millisecond,
		BackoffFactor: 2.0,
	}

	attempts := 0
	expectedErr := errors.New("connection reset")
	fn := func() (int, error) {
		attempts++
		return 0, expectedErr
	}

	result := WithRetry(context.Background(), cfg, fn)

	assert.Error(t, result.LastError)
	assert.Equal(t, 0, result.Value)
	assert.Equal(t, 3, result.Attempts) // Initial + 2 retries
	assert.Equal(t, 3, attempts)
}

func TestWithRetry_NoRetryOnNonRetryableError(t *testing.T) {
	cfg := RetryConfig{
		MaxRetries:    3,
		InitialDelay:  1 * time.Millisecond,
		MaxDelay:      10 * time.Millisecond,
		BackoffFactor: 2.0,
	}

	attempts := 0
	fn := func() (int, error) {
		attempts++
		return 0, errors.New("object not found") // Non-retryable
	}

	result := WithRetry(context.Background(), cfg, fn)

	assert.Error(t, result.LastError)
	assert.Equal(t, 1, result.Attempts)
	assert.Equal(t, 1, attempts)
}

func TestWithRetry_ContextCanceled(t *testing.T) {
	cfg := RetryConfig{
		MaxRetries:    3,
		InitialDelay:  100 * time.Millisecond,
		MaxDelay:      1 * time.Second,
		BackoffFactor: 2.0,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	attempts := 0
	fn := func() (int, error) {
		attempts++
		if attempts == 1 {
			cancel() // Cancel after first attempt
		}
		return 0, errors.New("connection reset")
	}

	result := WithRetry(ctx, cfg, fn)

	assert.Error(t, result.LastError)
	assert.True(t, errors.Is(result.LastError, context.Canceled))
}

func TestWithRetryFunc(t *testing.T) {
	cfg := RetryConfig{
		MaxRetries:    2,
		InitialDelay:  1 * time.Millisecond,
		MaxDelay:      10 * time.Millisecond,
		BackoffFactor: 2.0,
	}

	attempts := 0
	fn := func() (string, error) {
		attempts++
		if attempts < 2 {
			return "", errors.New("connection reset")
		}
		return "success", nil
	}

	value, err := WithRetryFunc(context.Background(), cfg, fn)

	assert.NoError(t, err)
	assert.Equal(t, "success", value)
	assert.Equal(t, 2, attempts)
}

func TestCalculateDelay(t *testing.T) {
	cfg := RetryConfig{
		InitialDelay:  100 * time.Millisecond,
		MaxDelay:      1 * time.Second,
		BackoffFactor: 2.0,
		Jitter:        0.0, // No jitter for predictable testing
	}

	tests := []struct {
		attempt       int
		expectedDelay time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 200 * time.Millisecond},
		{2, 400 * time.Millisecond},
		{3, 800 * time.Millisecond},
		{4, 1 * time.Second}, // Capped at MaxDelay
		{5, 1 * time.Second}, // Still capped
	}

	for _, tt := range tests {
		t.Run(string(rune(tt.attempt)), func(t *testing.T) {
			delay := calculateDelay(cfg, tt.attempt)
			assert.Equal(t, tt.expectedDelay, delay)
		})
	}
}

func TestCalculateDelay_WithJitter(t *testing.T) {
	cfg := RetryConfig{
		InitialDelay:  100 * time.Millisecond,
		MaxDelay:      1 * time.Second,
		BackoffFactor: 2.0,
		Jitter:        0.2, // 20% jitter
	}

	// Run multiple times to verify jitter adds variance
	delays := make([]time.Duration, 10)
	for i := 0; i < 10; i++ {
		delays[i] = calculateDelay(cfg, 0)
	}

	// At least some delays should be different (with high probability)
	// due to jitter
	baseDelay := 100 * time.Millisecond
	minExpected := time.Duration(float64(baseDelay) * 0.8)
	maxExpected := time.Duration(float64(baseDelay) * 1.2)

	for _, d := range delays {
		assert.GreaterOrEqual(t, d, minExpected)
		assert.LessOrEqual(t, d, maxExpected)
	}
}

func TestRetryableFunc(t *testing.T) {
	cfg := RetryConfig{
		MaxRetries:    2,
		InitialDelay:  1 * time.Millisecond,
		MaxDelay:      10 * time.Millisecond,
		BackoffFactor: 2.0,
	}

	attempts := 0
	fn := func() (int, error) {
		attempts++
		if attempts < 2 {
			return 0, errors.New("connection reset")
		}
		return 100, nil
	}

	retryable := NewRetryableFunc(cfg, fn)

	// Test Execute
	value, err := retryable.Execute(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 100, value)

	// Reset for ExecuteWithResult test
	attempts = 0

	result := retryable.ExecuteWithResult(context.Background())
	require.NoError(t, result.LastError)
	assert.Equal(t, 100, result.Value)
	assert.Equal(t, 2, result.Attempts)
}

// mockNetError is a mock network error for testing.
type mockNetError struct {
	message   string
	timeout   bool
	temporary bool
}

func (e *mockNetError) Error() string   { return e.message }
func (e *mockNetError) Timeout() bool   { return e.timeout }
func (e *mockNetError) Temporary() bool { return e.temporary }

// Ensure mockNetError implements net.Error
var _ net.Error = (*mockNetError)(nil)

func TestIsRetryableError_NetError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "timeout error",
			err:      &mockNetError{message: "timeout", timeout: true, temporary: false},
			expected: true,
		},
		{
			name:     "temporary error only (not retryable since Go 1.18)",
			err:      &mockNetError{message: "temporary", timeout: false, temporary: true},
			expected: false, // Temporary() is deprecated and no longer used
		},
		{
			name:     "permanent network error",
			err:      &mockNetError{message: "permanent", timeout: false, temporary: false},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsRetryableError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestWithRetry_ZeroMaxRetries(t *testing.T) {
	cfg := NoRetryConfig()

	attempts := 0
	fn := func() (int, error) {
		attempts++
		if attempts == 1 {
			return 0, errors.New("connection reset")
		}
		return 42, nil
	}

	result := WithRetry(context.Background(), cfg, fn)

	// With 0 retries, should still make 1 attempt
	assert.Error(t, result.LastError)
	assert.Equal(t, 1, result.Attempts)
	assert.Equal(t, 1, attempts)
}
