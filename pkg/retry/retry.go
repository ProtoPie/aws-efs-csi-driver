package retry

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	k8sretry "k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
)

// Strategy defines the retry strategy configuration
type Strategy struct {
	// MaxAttempts is the maximum number of retry attempts
	MaxAttempts int

	// InitialDelay is the initial delay before the first retry
	InitialDelay time.Duration

	// MaxDelay is the maximum delay between retries
	MaxDelay time.Duration

	// Multiplier is the exponential backoff multiplier
	Multiplier float64

	// JitterFactor is the jitter factor (0.0 to 1.0)
	// 0.0 means no jitter, 1.0 means up to 100% jitter
	JitterFactor float64

	// RetryableErrors determines if an error should trigger a retry
	RetryableErrors func(error) bool
}

// DefaultStrategy returns a default retry strategy
func DefaultStrategy() *Strategy {
	return &Strategy{
		MaxAttempts:     5,
		InitialDelay:    100 * time.Millisecond,
		MaxDelay:        30 * time.Second,
		Multiplier:      2.0,
		JitterFactor:    0.3,
		RetryableErrors: IsRetryableError,
	}
}

// EFSProvisioningStrategy returns a retry strategy optimized for EFS provisioning operations
func EFSProvisioningStrategy() *Strategy {
	return &Strategy{
		MaxAttempts:     10,
		InitialDelay:    500 * time.Millisecond,
		MaxDelay:        60 * time.Second,
		Multiplier:      1.5,
		JitterFactor:    0.2,
		RetryableErrors: IsEFSRetryableError,
	}
}

// LockAcquisitionStrategy returns a retry strategy optimized for distributed lock acquisition
func LockAcquisitionStrategy() *Strategy {
	return &Strategy{
		MaxAttempts:     20,
		InitialDelay:    50 * time.Millisecond,
		MaxDelay:        5 * time.Second,
		Multiplier:      1.3,
		JitterFactor:    0.5,
		RetryableErrors: IsLockRetryableError,
	}
}

// AWSAPIStrategy returns a retry strategy optimized for AWS API calls
func AWSAPIStrategy() *Strategy {
	return &Strategy{
		MaxAttempts:     8,
		InitialDelay:    200 * time.Millisecond,
		MaxDelay:        20 * time.Second,
		Multiplier:      2.0,
		JitterFactor:    0.25,
		RetryableErrors: IsAWSRetryableError,
	}
}

// Operation represents a retryable operation
type Operation func() error

// OperationWithResult represents a retryable operation that returns a result
type OperationWithResult[T any] func() (T, error)

// Do executes the operation with the configured retry strategy
func (s *Strategy) Do(ctx context.Context, operation Operation) error {
	return s.DoWithName(ctx, "operation", operation)
}

// DoWithName executes the operation with the configured retry strategy and a descriptive name
func (s *Strategy) DoWithName(ctx context.Context, name string, operation Operation) error {
	var lastErr error

	for attempt := 0; attempt < s.MaxAttempts; attempt++ {
		// Check context before attempting
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context cancelled before attempt %d: %w", attempt+1, err)
		}

		// Execute the operation
		err := operation()
		if err == nil {
			if attempt > 0 {
				klog.V(4).Infof("Operation %s succeeded after %d attempts", name, attempt+1)
			}
			return nil
		}

		lastErr = err

		// Check if error is retryable
		if !s.RetryableErrors(err) {
			klog.V(3).Infof("Operation %s failed with non-retryable error: %v", name, err)
			return err
		}

		// Don't sleep after the last attempt
		if attempt < s.MaxAttempts-1 {
			delay := s.calculateDelay(attempt)
			klog.V(4).Infof("Operation %s failed (attempt %d/%d), retrying in %v: %v",
				name, attempt+1, s.MaxAttempts, delay, err)

			// Sleep with context cancellation support
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during retry delay: %w", ctx.Err())
			}
		}
	}

	return fmt.Errorf("operation %s failed after %d attempts: %w", name, s.MaxAttempts, lastErr)
}

// DoWithResult executes an operation that returns a result with retry logic
func DoWithResult[T any](ctx context.Context, s *Strategy, operation OperationWithResult[T]) (T, error) {
	return DoWithResultAndName(ctx, s, "operation", operation)
}

// DoWithResultAndName executes an operation that returns a result with retry logic and a descriptive name
func DoWithResultAndName[T any](ctx context.Context, s *Strategy, name string, operation OperationWithResult[T]) (T, error) {
	var zero T
	var lastErr error

	for attempt := 0; attempt < s.MaxAttempts; attempt++ {
		// Check context before attempting
		if err := ctx.Err(); err != nil {
			return zero, fmt.Errorf("context cancelled before attempt %d: %w", attempt+1, err)
		}

		// Execute the operation
		result, err := operation()
		if err == nil {
			if attempt > 0 {
				klog.V(4).Infof("Operation %s succeeded after %d attempts", name, attempt+1)
			}
			return result, nil
		}

		lastErr = err

		// Check if error is retryable
		if !s.RetryableErrors(err) {
			klog.V(3).Infof("Operation %s failed with non-retryable error: %v", name, err)
			return zero, err
		}

		// Don't sleep after the last attempt
		if attempt < s.MaxAttempts-1 {
			delay := s.calculateDelay(attempt)
			klog.V(4).Infof("Operation %s failed (attempt %d/%d), retrying in %v: %v",
				name, attempt+1, s.MaxAttempts, delay, err)

			// Sleep with context cancellation support
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return zero, fmt.Errorf("context cancelled during retry delay: %w", ctx.Err())
			}
		}
	}

	return zero, fmt.Errorf("operation %s failed after %d attempts: %w", name, s.MaxAttempts, lastErr)
}

// calculateDelay calculates the delay for the given attempt number
func (s *Strategy) calculateDelay(attempt int) time.Duration {
	// Calculate exponential backoff
	delay := float64(s.InitialDelay) * math.Pow(s.Multiplier, float64(attempt))

	// Cap at maximum delay
	if delay > float64(s.MaxDelay) {
		delay = float64(s.MaxDelay)
	}

	// Add jitter to prevent thundering herd
	if s.JitterFactor > 0 {
		// Generate random jitter between -jitterFactor and +jitterFactor
		jitter := (rand.Float64()*2 - 1) * s.JitterFactor * delay
		delay = delay + jitter

		// Ensure delay doesn't go negative
		if delay < float64(s.InitialDelay)/2 {
			delay = float64(s.InitialDelay) / 2
		}
	}

	return time.Duration(delay)
}

// ToK8sBackoff converts the strategy to a k8s.io/apimachinery/pkg/util/wait.Backoff
func (s *Strategy) ToK8sBackoff() wait.Backoff {
	return wait.Backoff{
		Duration: s.InitialDelay,
		Factor:   s.Multiplier,
		Jitter:   s.JitterFactor,
		Steps:    s.MaxAttempts,
		Cap:      s.MaxDelay,
	}
}

// WithK8sRetry wraps an operation to use k8s.io/client-go/util/retry with our strategy
func (s *Strategy) WithK8sRetry(operation func() error) error {
	backoff := s.ToK8sBackoff()
	return k8sretry.OnError(backoff, s.RetryableErrors, operation)
}

// IsRetryableError determines if an error should trigger a retry (default implementation)
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Kubernetes API errors
	if errors.IsConflict(err) || errors.IsServerTimeout(err) || errors.IsServiceUnavailable(err) || errors.IsTooManyRequests(err) {
		return true
	}

	// Check for transient network errors
	if errors.IsTimeout(err) || errors.IsInternalError(err) {
		return true
	}

	return false
}

// IsEFSRetryableError determines if an EFS-related error should trigger a retry
func IsEFSRetryableError(err error) bool {
	if IsRetryableError(err) {
		return true
	}

	// Add EFS-specific retryable errors here
	// For example, check for specific AWS error codes

	return false
}

// IsLockRetryableError determines if a lock-related error should trigger a retry
func IsLockRetryableError(err error) bool {
	// Always retry lock conflicts
	if errors.IsConflict(err) {
		return true
	}

	// Retry timeout errors for locks
	if errors.IsTimeout(err) {
		return true
	}

	return IsRetryableError(err)
}

// IsAWSRetryableError determines if an AWS API error should trigger a retry
func IsAWSRetryableError(err error) bool {
	if IsRetryableError(err) {
		return true
	}

	// Add AWS-specific retryable error checking here
	// This would check for throttling, service unavailable, etc.

	return false
}

// ExponentialBackoff is a convenience function for simple exponential backoff with jitter
func ExponentialBackoff(ctx context.Context, maxAttempts int, initialDelay time.Duration, operation Operation) error {
	strategy := &Strategy{
		MaxAttempts:     maxAttempts,
		InitialDelay:    initialDelay,
		MaxDelay:        initialDelay * 32, // Default to 32x initial delay as max
		Multiplier:      2.0,
		JitterFactor:    0.3,
		RetryableErrors: IsRetryableError,
	}
	return strategy.Do(ctx, operation)
}

// ExponentialBackoffWithResult is a convenience function for operations that return results
func ExponentialBackoffWithResult[T any](ctx context.Context, maxAttempts int, initialDelay time.Duration, operation OperationWithResult[T]) (T, error) {
	strategy := &Strategy{
		MaxAttempts:     maxAttempts,
		InitialDelay:    initialDelay,
		MaxDelay:        initialDelay * 32,
		Multiplier:      2.0,
		JitterFactor:    0.3,
		RetryableErrors: IsRetryableError,
	}
	return DoWithResult(ctx, strategy, operation)
}