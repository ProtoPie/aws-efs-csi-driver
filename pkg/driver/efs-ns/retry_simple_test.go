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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryWithExponentialBackoff_Success(t *testing.T) {
	ctx := context.Background()
	config := DefaultRetryConfig()
	
	callCount := 0
	fn := func() error {
		callCount++
		if callCount < 3 {
			return NewEFSNSError(ErrAWSAPIFailed, "test", "test", "throttling error", nil)
		}
		return nil
	}

	err := RetryWithExponentialBackoff(ctx, config, fn)
	require.NoError(t, err)
	assert.Equal(t, 3, callCount)
}

func TestRetryWithExponentialBackoff_MaxRetriesExceeded(t *testing.T) {
	ctx := context.Background()
	config := DefaultRetryConfig()
	config.MaxRetries = 2
	
	callCount := 0
	fn := func() error {
		callCount++
		return NewEFSNSError(ErrAWSAPIFailed, "test", "test", "persistent error", nil)
	}

	err := RetryWithExponentialBackoff(ctx, config, fn)
	require.Error(t, err)
	assert.Equal(t, 3, callCount) // Original call + 2 retries
	assert.Contains(t, err.Error(), "persistent error")
}

func TestRetryWithExponentialBackoff_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	config := DefaultRetryConfig()
	config.BaseDelay = 100 * time.Millisecond
	
	callCount := 0
	fn := func() error {
		callCount++
		if callCount == 2 {
			cancel() // Cancel context on second call
		}
		return NewEFSNSError(ErrAWSAPIFailed, "test", "test", "error", nil)
	}

	err := RetryWithExponentialBackoff(ctx, config, fn)
	require.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestCircuitBreaker_OpenCircuit(t *testing.T) {
	cb := NewCircuitBreaker(2, 1*time.Second)
	
	// Trigger failures to open circuit
	err1 := cb.Execute(func() error { return errors.New("error1") })
	assert.Error(t, err1)
	
	err2 := cb.Execute(func() error { return errors.New("error2") })
	assert.Error(t, err2)
	
	// Circuit should now be open
	err3 := cb.Execute(func() error { return nil })
	require.Error(t, err3)
	assert.Contains(t, err3.Error(), "circuit breaker is open")
	assert.Equal(t, ErrTimeout, GetEFSNSErrorType(err3))
}

func TestCircuitBreaker_HalfOpenRecovery(t *testing.T) {
	cb := NewCircuitBreaker(2, 10*time.Millisecond)
	
	// Open the circuit
	cb.Execute(func() error { return errors.New("error1") })
	cb.Execute(func() error { return errors.New("error2") })
	
	// Wait for recovery timeout
	time.Sleep(15 * time.Millisecond)
	
	// Should be in half-open state - allow some calls through
	successCount := 0
	for i := 0; i < 5; i++ {
		err := cb.Execute(func() error { return nil })
		if err == nil {
			successCount++
		}
	}
	
	// Should have had at least some successful calls
	assert.Greater(t, successCount, 0)
}

func TestDefaultShouldRetryFunc(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		shouldRetry bool
	}{
		{
			name:        "nil error",
			err:         nil,
			shouldRetry: false,
		},
		{
			name:        "retryable EFS-NS error",
			err:         NewEFSNSError(ErrAWSAPIFailed, "test", "test", "throttling", nil),
			shouldRetry: true,
		},
		{
			name:        "non-retryable EFS-NS error",
			err:         NewEFSNSError(ErrInvalidParameter, "test", "test", "invalid param", nil),
			shouldRetry: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DefaultShouldRetryFunc(tt.err)
			assert.Equal(t, tt.shouldRetry, result)
		})
	}
}

func TestCalculateDelay(t *testing.T) {
	baseDelay := 100 * time.Millisecond
	maxDelay := 5 * time.Second
	jitterFactor := 0.1

	tests := []struct {
		attempt      int
		expectedMin  time.Duration
		expectedMax  time.Duration
	}{
		{0, 90 * time.Millisecond, 110 * time.Millisecond},
		{1, 180 * time.Millisecond, 220 * time.Millisecond},
		{2, 360 * time.Millisecond, 440 * time.Millisecond},
		{10, maxDelay - (maxDelay/10), maxDelay + (maxDelay/10)}, // Should be capped at maxDelay
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			delay := calculateDelay(tt.attempt, baseDelay, maxDelay, jitterFactor)
			assert.GreaterOrEqual(t, delay, tt.expectedMin)
			assert.LessOrEqual(t, delay, tt.expectedMax)
		})
	}
}

func TestRetryableOperations(t *testing.T) {
	ctx := context.Background()
	
	t.Run("RetryableEFSOperation", func(t *testing.T) {
		callCount := 0
		err := RetryableEFSOperation(ctx, "test-operation", "test-ns", func() error {
			callCount++
			if callCount < 2 {
				return NewEFSNSError(ErrAWSAPIFailed, "test", "test", "throttling error", nil)
			}
			return nil
		})
		
		require.NoError(t, err)
		assert.Equal(t, 2, callCount)
	})
	
	t.Run("RetryableKubernetesOperation", func(t *testing.T) {
		callCount := 0
		err := RetryableKubernetesOperation(ctx, "test-operation", "test-ns", func() error {
			callCount++
			if callCount < 2 {
				return NewEFSNSError(ErrAWSAPIFailed, "test", "test", "throttling error", nil)
			}
			return nil
		})
		
		require.NoError(t, err)
		assert.Equal(t, 2, callCount)
	})
	
	t.Run("RetryableAWSOperation", func(t *testing.T) {
		cb := NewCircuitBreaker(CircuitBreakerFailureThreshold, CircuitBreakerRecoveryTimeout)
		
		callCount := 0
		err := RetryableAWSOperation(ctx, cb, "test-operation", "test-ns", func() error {
			callCount++
			if callCount < 2 {
				return NewEFSNSError(ErrAWSAPIFailed, "test", "test", "throttling error", nil)
			}
			return nil
		})
		
		require.NoError(t, err)
		assert.Equal(t, 2, callCount)
	})
}

func TestOperationWithTimeout(t *testing.T) {
	t.Run("successful operation", func(t *testing.T) {
		ctx := context.Background()
		timeout := 1 * time.Second
		
		err := OperationWithTimeout(ctx, timeout, "test-op", func(ctx context.Context) error {
			return nil
		})
		
		require.NoError(t, err)
	})
	
	t.Run("timeout exceeded", func(t *testing.T) {
		ctx := context.Background()
		timeout := 10 * time.Millisecond
		
		err := OperationWithTimeout(ctx, timeout, "test-op", func(ctx context.Context) error {
			time.Sleep(100 * time.Millisecond)
			return nil
		})
		
		require.Error(t, err)
		assert.Equal(t, ErrTimeout, GetEFSNSErrorType(err))
		assert.Contains(t, err.Error(), "operation timed out")
	})
}