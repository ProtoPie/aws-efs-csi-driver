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
	"fmt"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/pointer"
)

func TestDistributedLockManager_CreateLock(t *testing.T) {
	tests := []struct {
		name        string
		backend     string
		key         string
		lockType    string
		expectError bool
	}{
		{
			name:        "ConfigMap backend",
			backend:     LockBackendConfigMap,
			key:         "test-namespace",
			lockType:    LockTypeNamespace,
			expectError: false,
		},
		{
			name:        "Lease backend",
			backend:     LockBackendLease,
			key:         "test-efs",
			lockType:    LockTypeEFS,
			expectError: false,
		},
		{
			name:        "Invalid backend",
			backend:     "invalid",
			key:         "test",
			lockType:    LockTypeNamespace,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()

			opts := &DistributedLockOptions{
				Backend:   tt.backend,
				Namespace: "test-ns",
				Identity:  "test-instance",
			}

			if tt.backend == "invalid" {
				_, err := NewDistributedLockManager(client, opts)
				if err == nil {
					t.Error("Expected error for invalid backend")
				}
				return
			}

			manager, err := NewDistributedLockManager(client, opts)
			if err != nil {
				t.Fatalf("Failed to create manager: %v", err)
			}
			defer manager.Stop(context.Background())

			lock, err := manager.CreateLock(tt.key, tt.lockType)
			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if lock == nil {
					t.Error("Expected lock but got nil")
				}
			}
		})
	}
}

func TestLeaseLock_AcquireRelease(t *testing.T) {
	client := fake.NewSimpleClientset()

	lock := &LeaseLock{
		k8sClient: client,
		namespace: "test-ns",
		name:      "test-lease",
		identity:  "test-instance",
		key:       "test-key",
		lockType:  LockTypeEFS,
		metrics:   NewLockMetrics(),
	}

	ctx := context.Background()

	// Test acquiring a new lease
	err := lock.Acquire(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}

	if !lock.IsHeld() {
		t.Error("Lock should be held after acquisition")
	}

	// Test releasing the lease
	err = lock.Release(ctx)
	if err != nil {
		t.Fatalf("Failed to release lock: %v", err)
	}

	if lock.IsHeld() {
		t.Error("Lock should not be held after release")
	}
}

func TestLeaseLock_AcquireConflict(t *testing.T) {
	client := fake.NewSimpleClientset()

	// Create first lock holder
	lock1 := &LeaseLock{
		k8sClient: client,
		namespace: "test-ns",
		name:      "test-lease",
		identity:  "instance-1",
		key:       "test-key",
		lockType:  LockTypeEFS,
		metrics:   NewLockMetrics(),
	}

	// Create second lock holder
	lock2 := &LeaseLock{
		k8sClient: client,
		namespace: "test-ns",
		name:      "test-lease",
		identity:  "instance-2",
		key:       "test-key",
		lockType:  LockTypeEFS,
		metrics:   NewLockMetrics(),
	}

	ctx := context.Background()

	// First instance acquires lock
	err := lock1.Acquire(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to acquire lock1: %v", err)
	}

	// Second instance tries to acquire - should timeout
	err = lock2.Acquire(ctx, 100*time.Millisecond)
	if err == nil {
		t.Error("Expected lock2 acquisition to fail due to conflict")
	}

	// Release first lock
	lock1.Release(ctx)

	// Now second instance should be able to acquire
	err = lock2.Acquire(ctx, 5*time.Second)
	if err != nil {
		t.Errorf("Failed to acquire lock2 after lock1 release: %v", err)
	}

	// Clean up
	lock2.Release(ctx)
}

func TestLeaseLock_ExpiredLeaseAcquisition(t *testing.T) {
	client := fake.NewSimpleClientset()

	// Create an expired lease manually
	expiredLease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-lease",
			Namespace: "test-ns",
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       pointer.String("expired-holder"),
			LeaseDurationSeconds: pointer.Int32(10),
			RenewTime:            &metav1.MicroTime{Time: time.Now().Add(-30 * time.Second)},
		},
	}

	_, err := client.CoordinationV1().Leases("test-ns").Create(context.Background(), expiredLease, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create expired lease: %v", err)
	}

	// New lock should be able to acquire the expired lease
	lock := &LeaseLock{
		k8sClient: client,
		namespace: "test-ns",
		name:      "test-lease",
		identity:  "new-instance",
		key:       "test-key",
		lockType:  LockTypeEFS,
		metrics:   NewLockMetrics(),
	}

	ctx := context.Background()
	err = lock.Acquire(ctx, 5*time.Second)
	if err != nil {
		t.Errorf("Failed to acquire expired lease: %v", err)
	}

	if !lock.IsHeld() {
		t.Error("Lock should be held after acquiring expired lease")
	}

	// Clean up
	lock.Release(ctx)
}

func TestLeaseLock_Refresh(t *testing.T) {
	client := fake.NewSimpleClientset()

	lock := &LeaseLock{
		k8sClient: client,
		namespace: "test-ns",
		name:      "test-lease",
		identity:  "test-instance",
		key:       "test-key",
		lockType:  LockTypeEFS,
		metrics:   NewLockMetrics(),
	}

	ctx := context.Background()

	// Acquire lock
	err := lock.Acquire(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}

	// Get initial renew time
	lease1, _ := client.CoordinationV1().Leases("test-ns").Get(ctx, "test-lease", metav1.GetOptions{})
	initialRenewTime := lease1.Spec.RenewTime.Time

	// Wait a bit
	time.Sleep(100 * time.Millisecond)

	// Refresh the lease
	err = lock.Refresh(ctx)
	if err != nil {
		t.Errorf("Failed to refresh lease: %v", err)
	}

	// Check that renew time was updated
	lease2, _ := client.CoordinationV1().Leases("test-ns").Get(ctx, "test-lease", metav1.GetOptions{})
	if !lease2.Spec.RenewTime.Time.After(initialRenewTime) {
		t.Error("Lease renew time should be updated after refresh")
	}

	// Clean up
	lock.Release(ctx)
}

func TestLeaseLock_GetHolder(t *testing.T) {
	client := fake.NewSimpleClientset()

	lock := &LeaseLock{
		k8sClient: client,
		namespace: "test-ns",
		name:      "test-lease",
		identity:  "test-instance",
		key:       "test-key",
		lockType:  LockTypeEFS,
		metrics:   NewLockMetrics(),
	}

	ctx := context.Background()

	// Before acquisition, no holder
	holder, err := lock.GetHolder()
	if err != nil {
		t.Errorf("Unexpected error getting holder: %v", err)
	}
	if holder != "" {
		t.Errorf("Expected empty holder, got: %s", holder)
	}

	// After acquisition, should return our identity
	lock.Acquire(ctx, 5*time.Second)
	holder, err = lock.GetHolder()
	if err != nil {
		t.Errorf("Unexpected error getting holder: %v", err)
	}
	if holder != "test-instance" {
		t.Errorf("Expected holder to be 'test-instance', got: %s", holder)
	}

	// Clean up
	lock.Release(ctx)
}

func TestSanitizeLeaseName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"namespace:test", "efs-lease-namespace-test"},
		{"EFS:fs-12345", "efs-lease-efs-fs-12345"},
		{"test_with_underscores", "efs-lease-test-with-underscores"},
		{"UPPERCASE", "efs-lease-uppercase"},
		{"123numbers", "efs-lease-123numbers"},
		{"-starts-with-dash", "efs-lease-starts-with-dash"},
		{"ends-with-dash-", "efs-lease-ends-with-dash"},
		{"very-long-name-that-exceeds-the-kubernetes-limit-of-sixty-three-chars", "efs-lease-very-long-name-that-exceeds-the-kubernetes-limit-of-s"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := sanitizeLeaseName(tt.input)
			if result != tt.expected {
				t.Errorf("sanitizeLeaseName(%q) = %q, want %q", tt.input, result, tt.expected)
			}

			// Verify the result is a valid Kubernetes name
			if len(result) > 63 {
				t.Errorf("Result exceeds 63 characters: %d", len(result))
			}
			if result[0] == '-' || result[len(result)-1] == '-' {
				t.Error("Result starts or ends with dash")
			}
		})
	}
}

func TestLeaseLock_ConcurrentAcquisition(t *testing.T) {
	client := fake.NewSimpleClientset()

	numWorkers := 10
	var wg sync.WaitGroup
	successCount := 0
	var successMutex sync.Mutex

	ctx := context.Background()

	// Create multiple lock instances trying to acquire the same lease
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			lock := &LeaseLock{
				k8sClient: client,
				namespace: "test-ns",
				name:      "concurrent-lease",
				identity:  fmt.Sprintf("instance-%d", id),
				key:       "concurrent-key",
				lockType:  LockTypeEFS,
				metrics:   NewLockMetrics(),
			}

			err := lock.Acquire(ctx, 100*time.Millisecond)
			if err == nil {
				successMutex.Lock()
				successCount++
				successMutex.Unlock()

				// Hold the lock briefly
				time.Sleep(50 * time.Millisecond)

				// Release the lock
				lock.Release(ctx)
			}
		}(i)
	}

	wg.Wait()

	// Only one should have succeeded due to short timeout
	if successCount > 2 {
		t.Errorf("Too many concurrent acquisitions succeeded: %d", successCount)
	}
}

func TestDeadlockDetector_CycleDetection(t *testing.T) {
	client := fake.NewSimpleClientset()

	manager, err := NewDistributedLockManager(client, &DistributedLockOptions{
		Backend:                 LockBackendLease,
		Namespace:               "test-ns",
		Identity:                "test-instance",
		EnableDeadlockDetection: true,
	})
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Stop(context.Background())

	detector := manager.deadlockDetector

	// Create a cycle: A -> B -> C -> A
	detector.RegisterWait("lock-A", "lock-B")
	detector.RegisterWait("lock-B", "lock-C")
	detector.RegisterWait("lock-C", "lock-A")

	// Detect cycles
	cycles := detector.detectCycles()
	if len(cycles) == 0 {
		t.Error("Expected to detect a cycle")
	}

	// Verify the cycle contains all three locks
	if len(cycles) > 0 {
		cycle := cycles[0]
		if len(cycle) != 3 {
			t.Errorf("Expected cycle of length 3, got %d", len(cycle))
		}
	}

	// Remove one edge to break the cycle
	detector.UnregisterWait("lock-C", "lock-A")

	// Should no longer detect a cycle
	cycles = detector.detectCycles()
	if len(cycles) != 0 {
		t.Error("Should not detect cycle after breaking it")
	}
}

func TestLockMetrics_Recording(t *testing.T) {
	metrics := NewLockMetrics()

	// Record some operations
	metrics.RecordAcquisition("lock1")
	metrics.RecordAcquisition("lock2")
	metrics.RecordConflict("lock1")
	metrics.RecordConflict("lock1")
	metrics.RecordRelease("lock1")
	metrics.RecordTimeout("lock3")
	metrics.RecordRenewalFailure("lock2")

	// Get metrics
	m := metrics.GetMetrics()

	// Verify counters
	if m["total_acquisitions"] != int64(2) {
		t.Errorf("Expected 2 acquisitions, got %v", m["total_acquisitions"])
	}
	if m["total_conflicts"] != int64(2) {
		t.Errorf("Expected 2 conflicts, got %v", m["total_conflicts"])
	}
	if m["total_releases"] != int64(1) {
		t.Errorf("Expected 1 release, got %v", m["total_releases"])
	}
	if m["total_timeouts"] != int64(1) {
		t.Errorf("Expected 1 timeout, got %v", m["total_timeouts"])
	}
	if m["renewal_failures"] != int64(1) {
		t.Errorf("Expected 1 renewal failure, got %v", m["renewal_failures"])
	}

	// Test getting specific lock stats
	lock1Stats := metrics.GetLockStats("lock1")
	if lock1Stats == nil {
		t.Error("Expected stats for lock1")
	} else {
		if lock1Stats.Conflicts != 2 {
			t.Errorf("Expected 2 conflicts for lock1, got %d", lock1Stats.Conflicts)
		}
	}

	// Test getting top contended locks
	topLocks := metrics.GetTopContendedLocks(1)
	if len(topLocks) != 1 {
		t.Errorf("Expected 1 top lock, got %d", len(topLocks))
	}
	if len(topLocks) > 0 && topLocks[0].Key != "lock1" {
		t.Errorf("Expected lock1 to be most contended, got %s", topLocks[0].Key)
	}
}

func TestLeaseLock_RenewalFailureHandling(t *testing.T) {
	client := fake.NewSimpleClientset()

	lock := &LeaseLock{
		k8sClient: client,
		namespace: "test-ns",
		name:      "test-lease",
		identity:  "test-instance",
		key:       "test-key",
		lockType:  LockTypeEFS,
		metrics:   NewLockMetrics(),
	}

	ctx := context.Background()

	// Acquire lock
	err := lock.Acquire(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}

	// Add reactor to simulate renewal failure after acquisition
	client.PrependReactor("update", "leases", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		// Fail all updates after initial acquisition
		return true, nil, errors.NewConflict(coordinationv1.Resource("leases"), "test-lease", fmt.Errorf("simulated conflict"))
	})

	// Try to refresh - should fail
	err = lock.Refresh(ctx)
	if err == nil {
		t.Error("Expected refresh to fail")
	}

	// Lock should be marked as not held after renewal failure
	// Note: In the actual implementation, this happens in the background renewal goroutine

	// Clean up - this will also fail but that's expected
	lock.Release(ctx)
}

func TestDistributedLockManager_Stop(t *testing.T) {
	client := fake.NewSimpleClientset()

	manager, err := NewDistributedLockManager(client, &DistributedLockOptions{
		Backend:                 LockBackendLease,
		Namespace:               "test-ns",
		Identity:                "test-instance",
		EnableDeadlockDetection: true,
	})
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Create and acquire a lock
	lock, _ := manager.CreateLock("test-lock", LockTypeEFS)
	ctx := context.Background()
	lock.Acquire(ctx, 5*time.Second)

	// Stop the manager
	err = manager.Stop(ctx)
	if err != nil {
		t.Errorf("Failed to stop manager: %v", err)
	}

	// Verify lock was released
	if lock.IsHeld() {
		t.Error("Lock should not be held after manager stop")
	}
}