/*
Copyright 2024 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package efsns

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
)

const (
	// DefaultMaxRetries is the default maximum number of retry attempts
	DefaultMaxRetries = 5

	// DefaultBaseDelay is the base delay for exponential backoff
	DefaultBaseDelay = 100 * time.Millisecond

	// DefaultMaxDelay is the maximum delay between retries
	DefaultMaxDelay = 30 * time.Second

	// DefaultJitterFactor is the jitter factor to add randomness to retry delays
	DefaultJitterFactor = 0.1

	// CircuitBreakerFailureThreshold is the number of consecutive failures before opening the circuit
	CircuitBreakerFailureThreshold = 10

	// CircuitBreakerRecoveryTimeout is the time to wait before trying to close the circuit
	CircuitBreakerRecoveryTimeout = 30 * time.Second
)

// RetryConfig defines configuration for retry operations
type RetryConfig struct {
	MaxRetries    int
	BaseDelay     time.Duration
	MaxDelay      time.Duration
	JitterFactor  float64
	ShouldRetry   func(error) bool
	OnRetry       func(int, error, time.Duration)
}

// DefaultRetryConfig returns a default retry configuration
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:   DefaultMaxRetries,
		BaseDelay:    DefaultBaseDelay,
		MaxDelay:     DefaultMaxDelay,
		JitterFactor: DefaultJitterFactor,
		ShouldRetry:  DefaultShouldRetryFunc,
		OnRetry:      DefaultOnRetryFunc,
	}
}

// CircuitBreakerState represents the state of a circuit breaker
type CircuitBreakerState int

const (
	CircuitBreakerClosed CircuitBreakerState = iota
	CircuitBreakerOpen
	CircuitBreakerHalfOpen
)

// CircuitBreaker implements the circuit breaker pattern to prevent cascade failures
type CircuitBreaker struct {
	maxFailures      int
	recoveryTimeout  time.Duration
	failureCount     int
	lastFailureTime  time.Time
	state            CircuitBreakerState
	successThreshold int
	successCount     int
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(maxFailures int, recoveryTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		maxFailures:      maxFailures,
		recoveryTimeout:  recoveryTimeout,
		state:            CircuitBreakerClosed,
		successThreshold: 3, // Number of successful calls required to close the circuit
	}
}

// Execute executes a function with circuit breaker protection
func (cb *CircuitBreaker) Execute(fn func() error) error {
	if cb.state == CircuitBreakerOpen {
		if time.Since(cb.lastFailureTime) < cb.recoveryTimeout {
			return NewEFSNSError(ErrTimeout, "CircuitBreaker", "", 
				"circuit breaker is open", nil)
		}
		cb.state = CircuitBreakerHalfOpen
		cb.successCount = 0
	}

	err := fn()
	
	if err != nil {
		cb.recordFailure()
		return err
	}
	
	cb.recordSuccess()
	return nil
}

// recordFailure records a failure and updates circuit breaker state
func (cb *CircuitBreaker) recordFailure() {
	cb.failureCount++
	cb.lastFailureTime = time.Now()
	
	if cb.state == CircuitBreakerHalfOpen {
		cb.state = CircuitBreakerOpen
		cb.successCount = 0
	} else if cb.failureCount >= cb.maxFailures {
		cb.state = CircuitBreakerOpen
	}
}

// recordSuccess records a success and updates circuit breaker state
func (cb *CircuitBreaker) recordSuccess() {
	cb.failureCount = 0
	
	if cb.state == CircuitBreakerHalfOpen {
		cb.successCount++
		if cb.successCount >= cb.successThreshold {
			cb.state = CircuitBreakerClosed
		}
	}
}

// RetryWithExponentialBackoff executes a function with exponential backoff retry logic
func RetryWithExponentialBackoff(ctx context.Context, config *RetryConfig, fn func() error) error {
	if config == nil {
		config = DefaultRetryConfig()
	}

	var lastErr error
	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err

		// Don't retry if we've exceeded max attempts
		if attempt == config.MaxRetries {
			break
		}

		// Check if error is retryable
		if !config.ShouldRetry(err) {
			return err
		}

		// Calculate delay with exponential backoff and jitter
		delay := calculateDelay(attempt, config.BaseDelay, config.MaxDelay, config.JitterFactor)

		// Call retry callback if configured
		if config.OnRetry != nil {
			config.OnRetry(attempt+1, err, delay)
		}

		// Wait with context cancellation support
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			// Continue to next attempt
		}
	}

	return lastErr
}

// calculateDelay calculates the delay for the next retry attempt
func calculateDelay(attempt int, baseDelay, maxDelay time.Duration, jitterFactor float64) time.Duration {
	// Exponential backoff: delay = baseDelay * 2^attempt
	delay := time.Duration(float64(baseDelay) * math.Pow(2, float64(attempt)))
	
	if delay > maxDelay {
		delay = maxDelay
	}
	
	// Add jitter to prevent thundering herd
	if jitterFactor > 0 {
		jitter := time.Duration(float64(delay) * jitterFactor * (rand.Float64()*2 - 1))
		delay += jitter
	}
	
	if delay < 0 {
		delay = baseDelay
	}
	
	return delay
}

// DefaultShouldRetryFunc determines if an error should be retried
func DefaultShouldRetryFunc(err error) bool {
	if err == nil {
		return false
	}

	// Check EFS-NS specific error types
	if IsRetryable(err) {
		return true
	}

	// Check AWS SDK errors
	if isAWSRetryableError(err) {
		return true
	}

	// Check Kubernetes API errors
	if isKubernetesRetryableError(err) {
		return true
	}

	return false
}

// isAWSRetryableError checks if an AWS error is retryable
func isAWSRetryableError(err error) bool {
	// Check for HTTP status codes that are retryable
	var responseError *awshttp.ResponseError
	if errors.As(err, &responseError) {
		statusCode := responseError.ResponseError.HTTPStatusCode()
		switch statusCode {
		case 429, 500, 502, 503, 504:
			return true
		}
	}

	// Check for smithy errors
	var apiError smithy.APIError
	if errors.As(err, &apiError) {
		switch apiError.ErrorCode() {
		case "Throttling", "ThrottledException", "TooManyRequestsException":
			return true
		case "InternalServerError", "ServiceUnavailable":
			return true
		case "RequestTimeout", "RequestTimeoutException":
			return true
		}
	}

	// Check for connection errors
	if strings.Contains(err.Error(), "connection") ||
		strings.Contains(err.Error(), "timeout") ||
		strings.Contains(err.Error(), "network") {
		return true
	}

	return false
}

// isKubernetesRetryableError checks if a Kubernetes API error is retryable
func isKubernetesRetryableError(err error) bool {
	if k8serrors.IsTimeout(err) ||
		k8serrors.IsServerTimeout(err) ||
		k8serrors.IsServiceUnavailable(err) ||
		k8serrors.IsTooManyRequests(err) ||
		k8serrors.IsInternalError(err) {
		return true
	}

	// Temporary network errors
	if k8serrors.IsUnexpectedServerError(err) {
		return true
	}

	return false
}

// DefaultOnRetryFunc is the default callback for retry attempts
func DefaultOnRetryFunc(attempt int, err error, delay time.Duration) {
	klog.V(3).Infof("Retry attempt %d after error: %v (waiting %v)", attempt, err, delay)
}

// WithCircuitBreaker wraps a function with circuit breaker protection
func WithCircuitBreaker(cb *CircuitBreaker, fn func() error) func() error {
	return func() error {
		return cb.Execute(fn)
	}
}

// RetryableEFSOperation wraps EFS operations with retry logic
func RetryableEFSOperation(ctx context.Context, operation string, namespace string, fn func() error) error {
	config := DefaultRetryConfig()
	config.OnRetry = func(attempt int, err error, delay time.Duration) {
		klog.V(3).Infof("Retrying EFS operation %s for namespace %s: attempt %d, error: %v, delay: %v",
			operation, namespace, attempt, err, delay)
	}

	return RetryWithExponentialBackoff(ctx, config, fn)
}

// RetryableKubernetesOperation wraps Kubernetes operations with retry logic
func RetryableKubernetesOperation(ctx context.Context, operation string, namespace string, fn func() error) error {
	config := DefaultRetryConfig()
	config.MaxRetries = 3 // Lower retry count for Kubernetes API
	config.OnRetry = func(attempt int, err error, delay time.Duration) {
		klog.V(3).Infof("Retrying Kubernetes operation %s for namespace %s: attempt %d, error: %v, delay: %v",
			operation, namespace, attempt, err, delay)
	}

	return RetryWithExponentialBackoff(ctx, config, fn)
}

// RetryableAWSOperation wraps AWS SDK operations with retry and circuit breaker
func RetryableAWSOperation(ctx context.Context, cb *CircuitBreaker, operation string, namespace string, fn func() error) error {
	config := DefaultRetryConfig()
	config.OnRetry = func(attempt int, err error, delay time.Duration) {
		klog.V(3).Infof("Retrying AWS operation %s for namespace %s: attempt %d, error: %v, delay: %v",
			operation, namespace, attempt, err, delay)
	}

	wrappedFn := fn
	if cb != nil {
		wrappedFn = WithCircuitBreaker(cb, fn)
	}

	return RetryWithExponentialBackoff(ctx, config, wrappedFn)
}

// OperationWithTimeout executes an operation with a timeout
func OperationWithTimeout(ctx context.Context, timeout time.Duration, operation string, fn func(context.Context) error) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- fn(timeoutCtx)
	}()

	select {
	case err := <-done:
		return err
	case <-timeoutCtx.Done():
		return NewEFSNSError(ErrTimeout, operation, "", 
			fmt.Sprintf("operation timed out after %v", timeout), timeoutCtx.Err())
	}
}