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

package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestNewNamespaceLockManager(t *testing.T) {
	tests := []struct {
		name     string
		options  *NamespaceLockManagerOptions
		identity string
		validate func(t *testing.T, manager *NamespaceLockManager)
	}{
		{
			name:     "with default options",
			identity: "test-instance-1",
			options:  nil,
			validate: func(t *testing.T, manager *NamespaceLockManager) {
				if manager.lockTimeout != DefaultLockTimeout {
					t.Errorf("expected lockTimeout %v, got %v", DefaultLockTimeout, manager.lockTimeout)
				}
				if manager.renewPeriod != DefaultLockRenewalPeriod {
					t.Errorf("expected renewPeriod %v, got %v", DefaultLockRenewalPeriod, manager.renewPeriod)
				}
				if manager.namespace != "kube-system" {
					t.Errorf("expected namespace kube-system, got %s", manager.namespace)
				}
			},
		},
		{
			name:     "with custom options",
			identity: "test-instance-2",
			options: &NamespaceLockManagerOptions{
				LockTimeout:   60 * time.Second,
				RenewalPeriod: 20 * time.Second,
				MaxRetries:    5,
				BackoffBase:   200 * time.Millisecond,
				BackoffMax:    20 * time.Second,
				JitterFactor:  0.3,
				LockNamespace: "efs-locks",
			},
			validate: func(t *testing.T, manager *NamespaceLockManager) {
				if manager.lockTimeout != 60*time.Second {
					t.Errorf("expected lockTimeout 60s, got %v", manager.lockTimeout)
				}
				if manager.renewPeriod != 20*time.Second {
					t.Errorf("expected renewPeriod 20s, got %v", manager.renewPeriod)
				}
				if manager.maxRetries != 5 {
					t.Errorf("expected maxRetries 5, got %d", manager.maxRetries)
				}
				if manager.namespace != "efs-locks" {
					t.Errorf("expected namespace efs-locks, got %s", manager.namespace)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			manager := NewNamespaceLockManager(client, test.identity, test.options)
			defer manager.Stop(context.Background())

			if manager.identity != test.identity {
				t.Errorf("expected identity %s, got %s", test.identity, manager.identity)
			}

			test.validate(t, manager)
		})
	}
}

func TestAcquireNamespaceLock(t *testing.T) {
	tests := []struct {
		name          string
		namespace     string
		timeout       time.Duration
		existingLock  *LockInfo
		expectError   bool
		errorContains string
	}{
		{
			name:        "acquire new lock successfully",
			namespace:   "test-namespace",
			timeout:     5 * time.Second,
			expectError: false,
		},
		{
			name:      "acquire lock when expired lock exists",
			namespace: "test-namespace",
			timeout:   5 * time.Second,
			existingLock: &LockInfo{
				Holder:    "other-instance",
				Timestamp: time.Now().Add(-2 * time.Minute),
				Type:      LockTypeNamespace,
				Namespace: "test-namespace",
				ExpiresAt: time.Now().Add(-1 * time.Minute), // Expired
			},
			expectError: false,
		},
		{
			name:      "fail to acquire when lock is held",
			namespace: "test-namespace",
			timeout:   1 * time.Second,
			existingLock: &LockInfo{
				Holder:    "other-instance",
				Timestamp: time.Now(),
				Type:      LockTypeNamespace,
				Namespace: "test-namespace",
				ExpiresAt: time.Now().Add(1 * time.Minute), // Not expired
			},
			expectError:   true,
			errorContains: "lock namespace:test-namespace is held by other-instance",
		},
		{
			name:      "reacquire own lock successfully",
			namespace: "test-namespace",
			timeout:   5 * time.Second,
			existingLock: &LockInfo{
				Holder:    "test-instance",
				Timestamp: time.Now(),
				Type:      LockTypeNamespace,
				Namespace: "test-namespace",
				ExpiresAt: time.Now().Add(1 * time.Minute),
			},
			expectError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			manager := NewNamespaceLockManager(client, "test-instance", &NamespaceLockManagerOptions{
				LockTimeout:   30 * time.Second,
				RenewalPeriod: 10 * time.Second,
				MaxRetries:    2,
				BackoffBase:   10 * time.Millisecond,
				LockNamespace: "test-locks",
			})
			defer manager.Stop(context.Background())

			// Create existing lock if specified
			if test.existingLock != nil {
				data, _ := json.Marshal(test.existingLock)
				cm := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      manager.getLockConfigMapName(fmt.Sprintf("%s:%s", LockTypeNamespace, test.namespace)),
						Namespace: "test-locks",
						Annotations: map[string]string{
							LockHolderAnnotation:    test.existingLock.Holder,
							LockTimestampAnnotation: test.existingLock.Timestamp.Format(time.RFC3339),
							LockTypeAnnotation:      test.existingLock.Type,
						},
					},
					Data: map[string]string{
						"lock": string(data),
					},
				}
				_, err := client.CoreV1().ConfigMaps("test-locks").Create(context.Background(), cm, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("failed to create existing lock: %v", err)
				}
			}

			ctx, cancel := context.WithTimeout(context.Background(), test.timeout)
			defer cancel()

			err := manager.AcquireNamespaceLock(ctx, test.namespace, test.timeout)

			if test.expectError {
				if err == nil {
					t.Error("expected error but got none")
				} else if test.errorContains != "" && !containsSubstring(err.Error(), test.errorContains) {
					t.Errorf("error should contain %q, got: %v", test.errorContains, err)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				// Verify lock was acquired
				lockKey := fmt.Sprintf("%s:%s", LockTypeNamespace, test.namespace)
				if _, ok := manager.activeLocks.Load(lockKey); !ok {
					t.Error("lock not found in active locks")
				}

				// Cleanup
				err = manager.ReleaseNamespaceLock(context.Background(), test.namespace)
				if err != nil {
					t.Errorf("failed to release lock: %v", err)
				}
			}
		})
	}
}

func TestAcquireEFSLock(t *testing.T) {
	client := fake.NewSimpleClientset()
	manager := NewNamespaceLockManager(client, "test-instance", &NamespaceLockManagerOptions{
		LockTimeout:   30 * time.Second,
		RenewalPeriod: 10 * time.Second,
		LockNamespace: "test-locks",
	})
	defer manager.Stop(context.Background())

	efsId := "fs-12345678"
	ctx := context.Background()

	// Test acquiring EFS lock
	err := manager.AcquireEFSLock(ctx, efsId, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to acquire EFS lock: %v", err)
	}

	// Verify lock was acquired
	lockKey := fmt.Sprintf("%s:%s", LockTypeEFS, efsId)
	if _, ok := manager.activeLocks.Load(lockKey); !ok {
		t.Error("EFS lock not found in active locks")
	}

	// Test releasing EFS lock
	err = manager.ReleaseEFSLock(ctx, efsId)
	if err != nil {
		t.Errorf("failed to release EFS lock: %v", err)
	}

	// Verify lock was released
	if _, ok := manager.activeLocks.Load(lockKey); ok {
		t.Error("EFS lock still in active locks after release")
	}
}

func TestConcurrentLockAcquisition(t *testing.T) {
	client := fake.NewSimpleClientset()
	namespace := "test-namespace"
	numWorkers := 10
	successCount := atomic.Int32{}
	errorCount := atomic.Int32{}

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func(id int) {
			defer wg.Done()

			manager := NewNamespaceLockManager(client, fmt.Sprintf("instance-%d", id), &NamespaceLockManagerOptions{
				LockTimeout:   2 * time.Second,
				RenewalPeriod: 500 * time.Millisecond,
				MaxRetries:    3,
				BackoffBase:   50 * time.Millisecond,
				LockNamespace: "test-locks",
			})
			defer manager.Stop(context.Background())

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err := manager.AcquireNamespaceLock(ctx, namespace, 5*time.Second)
			if err != nil {
				errorCount.Add(1)
				t.Logf("Worker %d failed to acquire lock: %v", id, err)
			} else {
				successCount.Add(1)
				t.Logf("Worker %d acquired lock", id)

				// Hold lock briefly
				time.Sleep(100 * time.Millisecond)

				// Release lock
				err = manager.ReleaseNamespaceLock(context.Background(), namespace)
				if err != nil {
					t.Logf("Worker %d failed to release lock: %v", id, err)
				}
			}
		}(i)
	}

	wg.Wait()

	// At least one worker should have acquired the lock
	if successCount.Load() == 0 {
		t.Error("no worker acquired the lock")
	}

	t.Logf("Success: %d, Errors: %d", successCount.Load(), errorCount.Load())
}

func TestLockRenewal(t *testing.T) {
	client := fake.NewSimpleClientset()
	manager := NewNamespaceLockManager(client, "test-instance", &NamespaceLockManagerOptions{
		LockTimeout:   2 * time.Second,
		RenewalPeriod: 500 * time.Millisecond,
		LockNamespace: "test-locks",
	})
	defer manager.Stop(context.Background())

	namespace := "test-namespace"
	ctx := context.Background()

	// Acquire lock
	err := manager.AcquireNamespaceLock(ctx, namespace, 5*time.Second)
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}

	// Wait for multiple renewal periods
	time.Sleep(2 * time.Second)

	// Check that lock is still held
	lockKey := fmt.Sprintf("%s:%s", LockTypeNamespace, namespace)
	if _, ok := manager.activeLocks.Load(lockKey); !ok {
		t.Error("lock was not renewed and is no longer active")
	}

	// Verify ConfigMap still exists and is owned by us
	cmName := manager.getLockConfigMapName(lockKey)
	cm, err := client.CoreV1().ConfigMaps("test-locks").Get(ctx, cmName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get lock ConfigMap: %v", err)
	}

	var lockInfo LockInfo
	if lockData, ok := cm.Data["lock"]; ok {
		if err := json.Unmarshal([]byte(lockData), &lockInfo); err != nil {
			t.Fatalf("failed to unmarshal lock data: %v", err)
		}
	}

	if lockInfo.Holder != "test-instance" {
		t.Errorf("expected lock holder to be test-instance, got %s", lockInfo.Holder)
	}

	// Check that expiration time was updated
	if time.Now().After(lockInfo.ExpiresAt) {
		t.Error("lock expiration time was not updated by renewal")
	}

	// Release lock
	err = manager.ReleaseNamespaceLock(ctx, namespace)
	if err != nil {
		t.Errorf("failed to release lock: %v", err)
	}
}

func TestCalculateBackoff(t *testing.T) {
	manager := &NamespaceLockManager{
		backoffBase:  100 * time.Millisecond,
		backoffMax:   5 * time.Second,
		jitterFactor: 0.2,
	}

	tests := []struct {
		attempt     int
		minExpected time.Duration
		maxExpected time.Duration
	}{
		{
			attempt:     0,
			minExpected: 80 * time.Millisecond,  // base - 20%
			maxExpected: 120 * time.Millisecond, // base + 20%
		},
		{
			attempt:     1,
			minExpected: 160 * time.Millisecond, // (base * 2) - 20%
			maxExpected: 240 * time.Millisecond, // (base * 2) + 20%
		},
		{
			attempt:     2,
			minExpected: 320 * time.Millisecond, // (base * 4) - 20%
			maxExpected: 480 * time.Millisecond, // (base * 4) + 20%
		},
		{
			attempt:     10,
			minExpected: 4 * time.Second, // capped at max - 20%
			maxExpected: 6 * time.Second, // capped at max + 20%
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("attempt_%d", test.attempt), func(t *testing.T) {
			// Test multiple times to account for randomness
			for i := 0; i < 10; i++ {
				backoff := manager.CalculateBackoff(test.attempt)
				if backoff < test.minExpected || backoff > test.maxExpected {
					t.Errorf("backoff %v not in expected range [%v, %v]", backoff, test.minExpected, test.maxExpected)
				}
			}
		})
	}
}

func TestSanitizeConfigMapName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			input:    "efs-lock-namespace:test",
			expected: "efs-lock-namespace-test",
		},
		{
			input:    "EFS-LOCK-NAMESPACE:TEST",
			expected: "efs-lock-namespace-test",
		},
		{
			input:    "-efs-lock-",
			expected: "efs-lock",
		},
		{
			input:    "efs@lock#namespace$test%",
			expected: "efs-lock-namespace-test",
		},
		{
			input:    "very-long-name-that-exceeds-the-kubernetes-limit-of-sixty-three-characters",
			expected: "very-long-name-that-exceeds-the-kubernetes-limit-of-sixty-three",
		},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			result := sanitizeConfigMapName(test.input)
			if result != test.expected {
				t.Errorf("expected %q, got %q", test.expected, result)
			}
		})
	}
}

func TestDeadlockDetection(t *testing.T) {
	client := fake.NewSimpleClientset()
	manager := NewNamespaceLockManager(client, "test-instance", &NamespaceLockManagerOptions{
		LockTimeout:   30 * time.Second,
		RenewalPeriod: 10 * time.Second,
		LockNamespace: "test-locks",
	})
	defer manager.Stop(context.Background())

	// Simulate multiple locks being held
	for i := 0; i < 6; i++ {
		lockKey := fmt.Sprintf("test-lock-%d", i)
		manager.activeLocks.Store(lockKey, &activeLock{
			key:       lockKey,
			lockType:  LockTypeNamespace,
			holder:    "test-instance",
			expiresAt: time.Now().Add(-6*time.Minute + time.Duration(i)*time.Minute),
			renewCh:   make(chan struct{}),
			doneCh:    make(chan struct{}),
		})
	}

	// Run deadlock detection
	manager.detectDeadlocks()

	// This test primarily verifies that deadlock detection doesn't panic
	// In a real scenario, we would check logs for the warning message
}

func TestCleanupExpiredLocks(t *testing.T) {
	client := fake.NewSimpleClientset()
	manager := NewNamespaceLockManager(client, "test-instance", &NamespaceLockManagerOptions{
		LockNamespace: "test-locks",
	})
	defer manager.Stop(context.Background())

	// Create expired lock owned by another instance
	expiredLockInfo := &LockInfo{
		Holder:    "other-instance",
		Timestamp: time.Now().Add(-2 * time.Hour),
		Type:      LockTypeNamespace,
		Namespace: "old-namespace",
		ExpiresAt: time.Now().Add(-1 * time.Hour),
	}
	data, _ := json.Marshal(expiredLockInfo)
	expiredCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "efs-lock-namespace-old-namespace",
			Namespace: "test-locks",
			Annotations: map[string]string{
				LockHolderAnnotation:    expiredLockInfo.Holder,
				LockTimestampAnnotation: expiredLockInfo.Timestamp.Format(time.RFC3339),
				LockTypeAnnotation:      LockTypeNamespace,
			},
		},
		Data: map[string]string{
			"lock": string(data),
		},
	}
	_, err := client.CoreV1().ConfigMaps("test-locks").Create(context.Background(), expiredCM, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create expired lock: %v", err)
	}

	// Create valid lock owned by current instance
	validLockInfo := &LockInfo{
		Holder:    "test-instance",
		Timestamp: time.Now(),
		Type:      LockTypeNamespace,
		Namespace: "current-namespace",
		ExpiresAt: time.Now().Add(1 * time.Hour),
	}
	data, _ = json.Marshal(validLockInfo)
	validCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "efs-lock-namespace-current-namespace",
			Namespace: "test-locks",
			Annotations: map[string]string{
				LockHolderAnnotation:    validLockInfo.Holder,
				LockTimestampAnnotation: validLockInfo.Timestamp.Format(time.RFC3339),
				LockTypeAnnotation:      LockTypeNamespace,
			},
		},
		Data: map[string]string{
			"lock": string(data),
		},
	}
	_, err = client.CoreV1().ConfigMaps("test-locks").Create(context.Background(), validCM, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create valid lock: %v", err)
	}

	// Run cleanup
	manager.cleanupExpiredLocks()

	// Verify expired lock was deleted
	_, err = client.CoreV1().ConfigMaps("test-locks").Get(context.Background(), "efs-lock-namespace-old-namespace", metav1.GetOptions{})
	if !errors.IsNotFound(err) {
		t.Error("expired lock was not cleaned up")
	}

	// Verify valid lock still exists
	_, err = client.CoreV1().ConfigMaps("test-locks").Get(context.Background(), "efs-lock-namespace-current-namespace", metav1.GetOptions{})
	if err != nil {
		t.Error("valid lock was incorrectly cleaned up")
	}
}

func TestGetMetrics(t *testing.T) {
	client := fake.NewSimpleClientset()
	manager := NewNamespaceLockManager(client, "test-instance", &NamespaceLockManagerOptions{
		LockNamespace: "test-locks",
	})
	defer manager.Stop(context.Background())

	// Perform some operations
	ctx := context.Background()

	// Acquire and release a lock
	err := manager.AcquireNamespaceLock(ctx, "test-namespace", 5*time.Second)
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}
	err = manager.ReleaseNamespaceLock(ctx, "test-namespace")
	if err != nil {
		t.Fatalf("failed to release lock: %v", err)
	}

	// Get metrics
	metrics := manager.GetMetrics()

	if metrics["acquisitions"] != 1 {
		t.Errorf("expected 1 acquisition, got %d", metrics["acquisitions"])
	}
	if metrics["releases"] != 1 {
		t.Errorf("expected 1 release, got %d", metrics["releases"])
	}
	if metrics["active_locks"] != 0 {
		t.Errorf("expected 0 active locks, got %d", metrics["active_locks"])
	}
}

func TestStop(t *testing.T) {
	client := fake.NewSimpleClientset()
	manager := NewNamespaceLockManager(client, "test-instance", &NamespaceLockManagerOptions{
		LockNamespace: "test-locks",
	})

	// Acquire some locks
	ctx := context.Background()
	err := manager.AcquireNamespaceLock(ctx, "namespace1", 5*time.Second)
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}
	err = manager.AcquireEFSLock(ctx, "fs-12345", 5*time.Second)
	if err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}

	// Stop the manager
	err = manager.Stop(ctx)
	if err != nil {
		t.Errorf("failed to stop manager: %v", err)
	}

	// Verify all locks were released
	cmList, err := client.CoreV1().ConfigMaps("test-locks").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to list ConfigMaps: %v", err)
	}
	if len(cmList.Items) != 0 {
		t.Errorf("expected 0 ConfigMaps after stop, got %d", len(cmList.Items))
	}

	// Verify no active locks remain
	activeCount := 0
	manager.activeLocks.Range(func(key, value interface{}) bool {
		activeCount++
		return true
	})
	if activeCount != 0 {
		t.Errorf("expected 0 active locks after stop, got %d", activeCount)
	}
}

func TestTimeoutHandling(t *testing.T) {
	client := fake.NewSimpleClientset()

	// Create a lock that's already held
	existingLock := &LockInfo{
		Holder:    "other-instance",
		Timestamp: time.Now(),
		Type:      LockTypeNamespace,
		Namespace: "test-namespace",
		ExpiresAt: time.Now().Add(10 * time.Second),
	}
	data, _ := json.Marshal(existingLock)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "efs-lock-namespace-test-namespace",
			Namespace: "test-locks",
		},
		Data: map[string]string{
			"lock": string(data),
		},
	}
	_, err := client.CoreV1().ConfigMaps("test-locks").Create(context.Background(), cm, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create existing lock: %v", err)
	}

	manager := NewNamespaceLockManager(client, "test-instance", &NamespaceLockManagerOptions{
		LockTimeout:   1 * time.Second,
		MaxRetries:    2,
		BackoffBase:   100 * time.Millisecond,
		LockNamespace: "test-locks",
	})
	defer manager.Stop(context.Background())

	// Try to acquire lock with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = manager.AcquireNamespaceLock(ctx, "test-namespace", 500*time.Millisecond)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("expected timeout error, got nil")
	}

	// Verify it timed out reasonably quickly
	if elapsed > 1*time.Second {
		t.Errorf("lock acquisition took too long: %v", elapsed)
	}
}

func TestConflictResolution(t *testing.T) {
	client := fake.NewSimpleClientset()

	// Simulate conflict by intercepting Update calls
	conflictCount := 0
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (handled bool, ret runtime.Object, err error) {
		if conflictCount < 2 {
			conflictCount++
			return true, nil, errors.NewConflict(corev1.Resource("configmaps"), "test", fmt.Errorf("simulated conflict"))
		}
		return false, nil, nil
	})

	manager := NewNamespaceLockManager(client, "test-instance", &NamespaceLockManagerOptions{
		MaxRetries:    3,
		BackoffBase:   10 * time.Millisecond,
		LockNamespace: "test-locks",
	})
	defer manager.Stop(context.Background())

	// Create an expired lock to trigger update path
	expiredLock := &LockInfo{
		Holder:    "other-instance",
		Timestamp: time.Now().Add(-2 * time.Minute),
		Type:      LockTypeNamespace,
		Namespace: "test-namespace",
		ExpiresAt: time.Now().Add(-1 * time.Minute),
	}
	data, _ := json.Marshal(expiredLock)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "efs-lock-namespace-test-namespace",
			Namespace: "test-locks",
		},
		Data: map[string]string{
			"lock": string(data),
		},
	}
	_, err := client.CoreV1().ConfigMaps("test-locks").Create(context.Background(), cm, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create expired lock: %v", err)
	}

	// Try to acquire lock - should succeed after retries
	err = manager.AcquireNamespaceLock(context.Background(), "test-namespace", 5*time.Second)
	if err != nil {
		t.Errorf("failed to acquire lock after conflicts: %v", err)
	}

	// Verify conflicts were encountered and resolved
	if conflictCount != 2 {
		t.Errorf("expected 2 conflicts, got %d", conflictCount)
	}

	// Cleanup
	err = manager.ReleaseNamespaceLock(context.Background(), "test-namespace")
	if err != nil {
		t.Errorf("failed to release lock: %v", err)
	}
}

// Helper function to check if a string contains a substring
func containsSubstring(s, substr string) bool {
	return len(substr) > 0 && len(s) >= len(substr) && s[:len(s)] != "" &&
		(s == substr || len(s) > len(substr) && (findSubstringInString(s, substr) >= 0))
}

func findSubstringInString(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}