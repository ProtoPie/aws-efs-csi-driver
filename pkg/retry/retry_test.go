package retry

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestDefaultStrategy(t *testing.T) {
	strategy := DefaultStrategy()

	if strategy.MaxAttempts != 5 {
		t.Errorf("Expected MaxAttempts to be 5, got %d", strategy.MaxAttempts)
	}

	if strategy.InitialDelay != 100*time.Millisecond {
		t.Errorf("Expected InitialDelay to be 100ms, got %v", strategy.InitialDelay)
	}

	if strategy.MaxDelay != 30*time.Second {
		t.Errorf("Expected MaxDelay to be 30s, got %v", strategy.MaxDelay)
	}

	if strategy.Multiplier != 2.0 {
		t.Errorf("Expected Multiplier to be 2.0, got %f", strategy.Multiplier)
	}

	if strategy.JitterFactor != 0.3 {
		t.Errorf("Expected JitterFactor to be 0.3, got %f", strategy.JitterFactor)
	}
}

func TestEFSProvisioningStrategy(t *testing.T) {
	strategy := EFSProvisioningStrategy()

	if strategy.MaxAttempts != 10 {
		t.Errorf("Expected MaxAttempts to be 10, got %d", strategy.MaxAttempts)
	}

	if strategy.InitialDelay != 500*time.Millisecond {
		t.Errorf("Expected InitialDelay to be 500ms, got %v", strategy.InitialDelay)
	}
}

func TestLockAcquisitionStrategy(t *testing.T) {
	strategy := LockAcquisitionStrategy()

	if strategy.MaxAttempts != 20 {
		t.Errorf("Expected MaxAttempts to be 20, got %d", strategy.MaxAttempts)
	}

	if strategy.MaxDelay != 5*time.Second {
		t.Errorf("Expected MaxDelay to be 5s, got %v", strategy.MaxDelay)
	}
}

func TestDoSuccess(t *testing.T) {
	strategy := &Strategy{
		MaxAttempts:     3,
		InitialDelay:    10 * time.Millisecond,
		MaxDelay:        100 * time.Millisecond,
		Multiplier:      2.0,
		JitterFactor:    0.1,
		RetryableErrors: func(err error) bool { return true }, // Always retry for test
	}

	ctx := context.Background()
	attempts := 0

	err := strategy.Do(ctx, func() error {
		attempts++
		if attempts < 2 {
			return errors.New("temporary error")
		}
		return nil
	})

	if err != nil {
		t.Errorf("Expected success, got error: %v", err)
	}

	if attempts != 2 {
		t.Errorf("Expected 2 attempts, got %d", attempts)
	}
}

func TestDoMaxAttemptsExceeded(t *testing.T) {
	strategy := &Strategy{
		MaxAttempts:     3,
		InitialDelay:    10 * time.Millisecond,
		MaxDelay:        50 * time.Millisecond,
		Multiplier:      2.0,
		JitterFactor:    0.1,
		RetryableErrors: func(err error) bool { return true },
	}

	ctx := context.Background()
	attempts := 0

	err := strategy.Do(ctx, func() error {
		attempts++
		return errors.New("persistent error")
	})

	if err == nil {
		t.Error("Expected error, got nil")
	}

	if attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", attempts)
	}

	if !strings.Contains(err.Error(), "failed after 3 attempts") {
		t.Errorf("Expected error message to contain 'failed after 3 attempts', got: %v", err)
	}
}

func TestDoNonRetryableError(t *testing.T) {
	strategy := &Strategy{
		MaxAttempts:     5,
		InitialDelay:    10 * time.Millisecond,
		MaxDelay:        100 * time.Millisecond,
		Multiplier:      2.0,
		JitterFactor:    0.1,
		RetryableErrors: func(err error) bool { return false },
	}

	ctx := context.Background()
	attempts := 0
	expectedErr := errors.New("non-retryable error")

	err := strategy.Do(ctx, func() error {
		attempts++
		return expectedErr
	})

	if err != expectedErr {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}

	if attempts != 1 {
		t.Errorf("Expected 1 attempt (no retry), got %d", attempts)
	}
}

func TestDoContextCancellation(t *testing.T) {
	strategy := &Strategy{
		MaxAttempts:     5,
		InitialDelay:    100 * time.Millisecond,
		MaxDelay:        1 * time.Second,
		Multiplier:      2.0,
		JitterFactor:    0.1,
		RetryableErrors: func(err error) bool { return true },
	}

	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0

	// Cancel context after first attempt
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := strategy.Do(ctx, func() error {
		attempts++
		return errors.New("error")
	})

	if err == nil {
		t.Error("Expected context cancellation error, got nil")
	}

	if !strings.Contains(err.Error(), "context cancelled") {
		t.Errorf("Expected context cancellation error, got: %v", err)
	}

	// Should have made 1 or 2 attempts before cancellation
	if attempts > 2 {
		t.Errorf("Expected at most 2 attempts before cancellation, got %d", attempts)
	}
}

func TestDoWithResult(t *testing.T) {
	strategy := &Strategy{
		MaxAttempts:     3,
		InitialDelay:    10 * time.Millisecond,
		MaxDelay:        100 * time.Millisecond,
		Multiplier:      2.0,
		JitterFactor:    0.1,
		RetryableErrors: func(err error) bool { return true }, // Always retry for test
	}

	ctx := context.Background()
	attempts := 0

	result, err := DoWithResult(ctx, strategy, func() (string, error) {
		attempts++
		if attempts < 3 {
			return "", errors.New("temporary error")
		}
		return "success", nil
	})

	if err != nil {
		t.Errorf("Expected success, got error: %v", err)
	}

	if result != "success" {
		t.Errorf("Expected result 'success', got '%s'", result)
	}

	if attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", attempts)
	}
}

func TestCalculateDelay(t *testing.T) {
	tests := []struct {
		name         string
		strategy     *Strategy
		attempt      int
		minExpected  time.Duration
		maxExpected  time.Duration
	}{
		{
			name: "first retry",
			strategy: &Strategy{
				InitialDelay: 100 * time.Millisecond,
				MaxDelay:     10 * time.Second,
				Multiplier:   2.0,
				JitterFactor: 0.2,
			},
			attempt:     0,
			minExpected: 80 * time.Millisecond,  // 100ms - 20%
			maxExpected: 120 * time.Millisecond, // 100ms + 20%
		},
		{
			name: "exponential growth",
			strategy: &Strategy{
				InitialDelay: 100 * time.Millisecond,
				MaxDelay:     10 * time.Second,
				Multiplier:   2.0,
				JitterFactor: 0.0, // No jitter for predictable test
			},
			attempt:     3,
			minExpected: 800 * time.Millisecond, // 100ms * 2^3
			maxExpected: 800 * time.Millisecond,
		},
		{
			name: "capped at max delay",
			strategy: &Strategy{
				InitialDelay: 100 * time.Millisecond,
				MaxDelay:     500 * time.Millisecond,
				Multiplier:   2.0,
				JitterFactor: 0.0,
			},
			attempt:     10, // Would be 102.4 seconds without cap
			minExpected: 500 * time.Millisecond,
			maxExpected: 500 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Run multiple times to account for randomness
			for i := 0; i < 10; i++ {
				delay := tt.strategy.calculateDelay(tt.attempt)
				if delay < tt.minExpected || delay > tt.maxExpected {
					t.Errorf("Delay %v not in expected range [%v, %v]",
						delay, tt.minExpected, tt.maxExpected)
				}
			}
		})
	}
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
			name:     "conflict error",
			err:      k8serrors.NewConflict(schema.GroupResource{}, "test", errors.New("conflict")),
			expected: true,
		},
		{
			name:     "timeout error",
			err:      k8serrors.NewTimeoutError("timeout", 30),
			expected: true,
		},
		{
			name:     "service unavailable",
			err:      k8serrors.NewServiceUnavailable("service unavailable"),
			expected: true,
		},
		{
			name:     "too many requests",
			err:      k8serrors.NewTooManyRequests("too many requests", 60),
			expected: true,
		},
		{
			name:     "not found error",
			err:      k8serrors.NewNotFound(schema.GroupResource{}, "test"),
			expected: false,
		},
		{
			name:     "generic error",
			err:      errors.New("generic error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsRetryableError(tt.err)
			if result != tt.expected {
				t.Errorf("IsRetryableError(%v) = %v, expected %v", tt.err, result, tt.expected)
			}
		})
	}
}

func TestToK8sBackoff(t *testing.T) {
	strategy := &Strategy{
		MaxAttempts:  5,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     10 * time.Second,
		Multiplier:   2.0,
		JitterFactor: 0.3,
	}

	backoff := strategy.ToK8sBackoff()

	if backoff.Duration != strategy.InitialDelay {
		t.Errorf("Expected Duration %v, got %v", strategy.InitialDelay, backoff.Duration)
	}

	if backoff.Factor != strategy.Multiplier {
		t.Errorf("Expected Factor %f, got %f", strategy.Multiplier, backoff.Factor)
	}

	if backoff.Jitter != strategy.JitterFactor {
		t.Errorf("Expected Jitter %f, got %f", strategy.JitterFactor, backoff.Jitter)
	}

	if backoff.Steps != strategy.MaxAttempts {
		t.Errorf("Expected Steps %d, got %d", strategy.MaxAttempts, backoff.Steps)
	}

	if backoff.Cap != strategy.MaxDelay {
		t.Errorf("Expected Cap %v, got %v", strategy.MaxDelay, backoff.Cap)
	}
}

func TestExponentialBackoff(t *testing.T) {
	ctx := context.Background()
	attempts := 0

	// Use a custom strategy that always retries for testing
	strategy := &Strategy{
		MaxAttempts:     3,
		InitialDelay:    10 * time.Millisecond,
		MaxDelay:        10 * time.Millisecond * 32,
		Multiplier:      2.0,
		JitterFactor:    0.3,
		RetryableErrors: func(err error) bool { return true },
	}

	err := strategy.Do(ctx, func() error {
		attempts++
		if attempts < 2 {
			return errors.New("temporary error")
		}
		return nil
	})

	if err != nil {
		t.Errorf("Expected success, got error: %v", err)
	}

	if attempts != 2 {
		t.Errorf("Expected 2 attempts, got %d", attempts)
	}
}

func TestExponentialBackoffWithResult(t *testing.T) {
	ctx := context.Background()
	attempts := 0

	// Use a custom strategy that always retries for testing
	strategy := &Strategy{
		MaxAttempts:     3,
		InitialDelay:    10 * time.Millisecond,
		MaxDelay:        10 * time.Millisecond * 32,
		Multiplier:      2.0,
		JitterFactor:    0.3,
		RetryableErrors: func(err error) bool { return true },
	}

	result, err := DoWithResult(ctx, strategy, func() (int, error) {
		attempts++
		if attempts < 2 {
			return 0, errors.New("temporary error")
		}
		return 42, nil
	})

	if err != nil {
		t.Errorf("Expected success, got error: %v", err)
	}

	if result != 42 {
		t.Errorf("Expected result 42, got %d", result)
	}

	if attempts != 2 {
		t.Errorf("Expected 2 attempts, got %d", attempts)
	}
}

func TestConcurrentRetries(t *testing.T) {
	strategy := &Strategy{
		MaxAttempts:     3,
		InitialDelay:    10 * time.Millisecond,
		MaxDelay:        100 * time.Millisecond,
		Multiplier:      2.0,
		JitterFactor:    0.3,
		RetryableErrors: func(err error) bool { return true }, // Always retry for test
	}

	ctx := context.Background()
	const numGoroutines = 10

	var totalAttempts int32
	errChan := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			localAttempts := 0
			err := strategy.DoWithName(ctx, fmt.Sprintf("goroutine-%d", id), func() error {
				localAttempts++
				atomic.AddInt32(&totalAttempts, 1)
				if localAttempts < 2 {
					return errors.New("temporary error")
				}
				return nil
			})
			errChan <- err
		}(i)
	}

	// Collect results
	for i := 0; i < numGoroutines; i++ {
		err := <-errChan
		if err != nil {
			t.Errorf("Goroutine failed: %v", err)
		}
	}

	// Each goroutine should have made 2 attempts
	expectedAttempts := int32(numGoroutines * 2)
	if totalAttempts != expectedAttempts {
		t.Errorf("Expected total attempts %d, got %d", expectedAttempts, totalAttempts)
	}
}

func TestRetryWithTimeout(t *testing.T) {
	strategy := &Strategy{
		MaxAttempts:     10,
		InitialDelay:    100 * time.Millisecond,
		MaxDelay:        1 * time.Second,
		Multiplier:      2.0,
		JitterFactor:    0.1,
		RetryableErrors: func(err error) bool { return true },
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	startTime := time.Now()
	attempts := 0

	err := strategy.Do(ctx, func() error {
		attempts++
		return errors.New("always fails")
	})

	elapsed := time.Since(startTime)

	if err == nil {
		t.Error("Expected timeout error, got nil")
	}

	// Should timeout around 200ms
	if elapsed > 300*time.Millisecond {
		t.Errorf("Operation took too long: %v", elapsed)
	}

	// Should have made 2-3 attempts before timeout
	if attempts > 3 {
		t.Errorf("Made too many attempts before timeout: %d", attempts)
	}
}

func BenchmarkRetryStrategy(b *testing.B) {
	strategy := DefaultStrategy()
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = strategy.Do(ctx, func() error {
			return nil // Immediate success
		})
	}
}

func BenchmarkRetryWithBackoff(b *testing.B) {
	strategy := &Strategy{
		MaxAttempts:     3,
		InitialDelay:    1 * time.Millisecond,
		MaxDelay:        10 * time.Millisecond,
		Multiplier:      2.0,
		JitterFactor:    0.3,
		RetryableErrors: IsRetryableError,
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		attempts := 0
		_ = strategy.Do(ctx, func() error {
			attempts++
			if attempts < 2 {
				return errors.New("temporary error")
			}
			return nil
		})
	}
}