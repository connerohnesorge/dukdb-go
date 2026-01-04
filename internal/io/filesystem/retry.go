// Package filesystem provides retry utilities for transient errors in cloud storage operations.
package filesystem

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"time"
)

// RetryConfig contains configuration for retry behavior.
type RetryConfig struct {
	// MaxRetries is the maximum number of retry attempts (0 = no retries).
	MaxRetries int
	// InitialDelay is the delay before the first retry.
	InitialDelay time.Duration
	// MaxDelay is the maximum delay between retries.
	MaxDelay time.Duration
	// BackoffFactor is the multiplier for exponential backoff.
	BackoffFactor float64
	// Jitter adds randomness to retry delays (0.0 to 1.0).
	Jitter float64
}

// DefaultRetryConfig returns a default retry configuration suitable for S3 operations.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:    3,
		InitialDelay:  100 * time.Millisecond,
		MaxDelay:      30 * time.Second,
		BackoffFactor: 2.0,
		Jitter:        0.2,
	}
}

// NoRetryConfig returns a configuration that disables retries.
func NoRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries: 0,
	}
}

// IsRetryableError determines if an error is retryable.
// Retryable errors include transient network errors and certain HTTP status codes.
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for context cancellation (not retryable)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Check for network errors
	var netErr net.Error
	if errors.As(err, &netErr) {
		// Only check Timeout() - Temporary() is deprecated since Go 1.18
		return netErr.Timeout()
	}

	// Check error message for common retryable patterns
	errMsg := strings.ToLower(err.Error())
	retryablePatterns := []string{
		"connection reset",
		"connection refused",
		"connection timed out",
		"timeout",
		"temporary failure",
		"service unavailable",
		"internal server error",
		"bad gateway",
		"gateway timeout",
		"request timeout",
		"throttling",
		"rate exceeded",
		"slow down",
		"too many requests",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(errMsg, pattern) {
			return true
		}
	}

	return false
}

// IsRetryableHTTPStatus returns true if the HTTP status code is retryable.
func IsRetryableHTTPStatus(statusCode int) bool {
	switch statusCode {
	case http.StatusTooManyRequests,        // 429
		http.StatusInternalServerError,      // 500
		http.StatusBadGateway,               // 502
		http.StatusServiceUnavailable,       // 503
		http.StatusGatewayTimeout,           // 504
		http.StatusRequestTimeout:           // 408
		return true
	default:
		return false
	}
}

// RetryResult contains the result of a retry operation.
type RetryResult[T any] struct {
	// Value is the successful result.
	Value T
	// Attempts is the number of attempts made.
	Attempts int
	// LastError is the last error encountered (nil on success).
	LastError error
}

// WithRetry executes a function with retry logic according to the provided configuration.
// It returns the result and number of attempts.
func WithRetry[T any](ctx context.Context, cfg RetryConfig, fn func() (T, error)) RetryResult[T] {
	var result RetryResult[T]
	var lastErr error
	var zero T

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		result.Attempts = attempt + 1

		// Check context before each attempt
		if ctx.Err() != nil {
			result.LastError = ctx.Err()
			return result
		}

		value, err := fn()
		if err == nil {
			result.Value = value
			return result
		}

		lastErr = err

		// Check if error is retryable
		if !IsRetryableError(err) {
			result.Value = zero
			result.LastError = err
			return result
		}

		// Don't sleep after the last attempt
		if attempt < cfg.MaxRetries {
			delay := calculateDelay(cfg, attempt)
			select {
			case <-ctx.Done():
				result.Value = zero
				result.LastError = ctx.Err()
				return result
			case <-time.After(delay):
				// Continue to next attempt
			}
		}
	}

	result.Value = zero
	result.LastError = lastErr

	return result
}

// WithRetryFunc is a simpler version that just returns value and error.
func WithRetryFunc[T any](ctx context.Context, cfg RetryConfig, fn func() (T, error)) (T, error) {
	result := WithRetry(ctx, cfg, fn)
	return result.Value, result.LastError
}

// calculateDelay computes the delay for a given attempt with exponential backoff and jitter.
func calculateDelay(cfg RetryConfig, attempt int) time.Duration {
	// Calculate base delay with exponential backoff
	delay := cfg.InitialDelay
	for i := 0; i < attempt; i++ {
		delay = time.Duration(float64(delay) * cfg.BackoffFactor)
		if delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
			break
		}
	}

	// Add jitter
	if cfg.Jitter > 0 {
		jitterAmount := float64(delay) * cfg.Jitter
		jitter := (rand.Float64()*2 - 1) * jitterAmount
		delay = time.Duration(float64(delay) + jitter)
	}

	// Ensure delay is not negative
	if delay < 0 {
		delay = cfg.InitialDelay
	}

	return delay
}

// RetryableFunc wraps a function to make it retryable.
type RetryableFunc[T any] struct {
	fn  func() (T, error)
	cfg RetryConfig
}

// NewRetryableFunc creates a new RetryableFunc with the given configuration.
func NewRetryableFunc[T any](cfg RetryConfig, fn func() (T, error)) *RetryableFunc[T] {
	return &RetryableFunc[T]{
		fn:  fn,
		cfg: cfg,
	}
}

// Execute runs the function with retry logic.
func (r *RetryableFunc[T]) Execute(ctx context.Context) (T, error) {
	return WithRetryFunc(ctx, r.cfg, r.fn)
}

// ExecuteWithResult runs the function and returns detailed results.
func (r *RetryableFunc[T]) ExecuteWithResult(ctx context.Context) RetryResult[T] {
	return WithRetry(ctx, r.cfg, r.fn)
}
