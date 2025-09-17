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
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
)

func TestNamespaceLockManager_HighConcurrencyStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	client := fake.NewSimpleClientset()
	manager := NewNamespaceLockManager(client, "stress-test-instance", &NamespaceLockManagerOptions{
		LockTimeout:   5 * time.Second,
		RenewalPeriod: 1 * time.Second,
		MaxRetries:    5,
		BackoffBase:   50 * time.Millisecond,
		LockNamespace: "stress-test-locks",
	})
	defer manager.Stop(context.Background())

	const (
		numWorkers    = 100
		numNamespaces = 10
		operationsPerWorker = 50
	)

	var (
		totalAcquisitions int64
		totalReleases     int64
		totalErrors       int64
	)

	var wg sync.WaitGroup

	// Create workers that randomly acquire and release locks
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < operationsPerWorker; j++ {
				namespace := fmt.Sprintf("stress-ns-%d", j%numNamespaces)

				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

				err := manager.AcquireNamespaceLock(ctx, namespace, 2*time.Second)
				if err != nil {
					atomic.AddInt64(&totalErrors, 1)
					cancel()
					continue
				}

				atomic.AddInt64(&totalAcquisitions, 1)

				// Hold lock for a random short duration
				holdTime := time.Duration(j%50) * time.Millisecond
				time.Sleep(holdTime)

				err = manager.ReleaseNamespaceLock(context.Background(), namespace)
				if err != nil {
					atomic.AddInt64(&totalErrors, 1)
				} else {
					atomic.AddInt64(&totalReleases, 1)
				}

				cancel()
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Stress test results:")
	t.Logf("  Total acquisitions: %d", totalAcquisitions)
	t.Logf("  Total releases: %d", totalReleases)
	t.Logf("  Total errors: %d", totalErrors)

	// Verify basic sanity
	if totalAcquisitions == 0 {
		t.Error("Expected at least some successful acquisitions")
	}

	if totalReleases > totalAcquisitions {
		t.Error("Cannot have more releases than acquisitions")
	}

	// Check metrics
	metrics := manager.GetMetrics()
	if acquisitions, ok := metrics["acquisitions"]; !ok || acquisitions < totalAcquisitions {
		t.Errorf("Manager metrics don't match: expected at least %d acquisitions, got %v", totalAcquisitions, acquisitions)
	}
}

func TestDistributedLockManager_MixedBackendStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	client := fake.NewSimpleClientset()

	// Test both ConfigMap and Lease backends under stress
	backends := []string{LockBackendConfigMap, LockBackendLease}

	for _, backend := range backends {
		t.Run(fmt.Sprintf("Backend_%s", backend), func(t *testing.T) {
			manager, err := NewDistributedLockManager(client, &DistributedLockOptions{
				Backend:                 backend,
				Namespace:               "stress-test-ns",
				Identity:                fmt.Sprintf("stress-instance-%s", backend),
				EnableDeadlockDetection: true,
			})
			if err != nil {
				t.Fatalf("Failed to create manager: %v", err)
			}
			defer manager.Stop(context.Background())

			const (
				numWorkers = 50
				numLocks   = 20
				operationsPerWorker = 30
			)

			var (
				successfulAcquisitions int64
				failedAcquisitions     int64
				successfulReleases     int64
			)

			var wg sync.WaitGroup

			// Create workers that compete for locks
			for i := 0; i < numWorkers; i++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()

					for j := 0; j < operationsPerWorker; j++ {
						lockKey := fmt.Sprintf("stress-lock-%d", j%numLocks)
						lockType := LockTypeEFS
						if j%2 == 0 {
							lockType = LockTypeNamespace
						}

						lock, err := manager.CreateLock(lockKey, lockType)
						if err != nil {
							atomic.AddInt64(&failedAcquisitions, 1)
							continue
						}

						ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)

						err = lock.Acquire(ctx, 500*time.Millisecond)
						if err != nil {
							atomic.AddInt64(&failedAcquisitions, 1)
							cancel()
							continue
						}

						atomic.AddInt64(&successfulAcquisitions, 1)

						// Hold lock briefly
						time.Sleep(time.Duration(j%20) * time.Millisecond)

						err = lock.Release(context.Background())
						if err == nil {
							atomic.AddInt64(&successfulReleases, 1)
						}

						cancel()
					}
				}(i)
			}

			wg.Wait()

			t.Logf("Stress test results for %s backend:", backend)
			t.Logf("  Successful acquisitions: %d", successfulAcquisitions)
			t.Logf("  Failed acquisitions: %d", failedAcquisitions)
			t.Logf("  Successful releases: %d", successfulReleases)

			// Verify some operations succeeded
			if successfulAcquisitions == 0 {
				t.Error("Expected at least some successful acquisitions")
			}
		})
	}
}

func TestDeadlockDetector_StressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	client := fake.NewSimpleClientset()

	manager, err := NewDistributedLockManager(client, &DistributedLockOptions{
		Backend:                 LockBackendLease,
		Namespace:               "deadlock-stress-ns",
		Identity:                "deadlock-stress-instance",
		EnableDeadlockDetection: true,
	})
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Stop(context.Background())

	detector := manager.deadlockDetector

	const (
		numWorkers = 20
		numLocks   = 50
		operationsPerWorker = 100
	)

	var wg sync.WaitGroup

	// Create workers that register/unregister wait relationships rapidly
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < operationsPerWorker; j++ {
				lockA := fmt.Sprintf("lock-%d", j%numLocks)
				lockB := fmt.Sprintf("lock-%d", (j+1)%numLocks)

				// Register wait
				detector.RegisterWait(lockA, lockB)

				// Sometimes create cycles
				if j%10 == 0 {
					detector.RegisterWait(lockB, lockA)
				}

				// Run detection
				if j%5 == 0 {
					detector.detectCycles()
				}

				// Unregister some waits
				if j%7 == 0 {
					detector.UnregisterWait(lockA, lockB)
				}

				// Brief pause to allow other workers
				if j%20 == 0 {
					time.Sleep(time.Microsecond)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify detector is still functional
	stats := detector.GetStatistics()
	t.Logf("Deadlock detector stats after stress test: %+v", stats)

	// Should have processed many checks
	if totalChecks, ok := stats["total_checks"].(int64); !ok || totalChecks == 0 {
		t.Error("Expected some deadlock checks to have been performed")
	}
}

func TestLockManager_RenewalStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	client := fake.NewSimpleClientset()
	manager := NewNamespaceLockManager(client, "renewal-stress-instance", &NamespaceLockManagerOptions{
		LockTimeout:   30 * time.Second,
		RenewalPeriod: 100 * time.Millisecond, // Fast renewal for stress test
		MaxRetries:    3,
		BackoffBase:   10 * time.Millisecond,
		LockNamespace: "renewal-stress-locks",
	})
	defer manager.Stop(context.Background())

	const (
		numNamespaces = 10
		testDuration  = 5 * time.Second
	)

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()

	// Acquire locks and let them be renewed automatically
	for i := 0; i < numNamespaces; i++ {
		wg.Add(1)
		go func(nsID int) {
			defer wg.Done()

			namespace := fmt.Sprintf("renewal-stress-ns-%d", nsID)

			err := manager.AcquireNamespaceLock(context.Background(), namespace, 5*time.Second)
			if err != nil {
				t.Errorf("Failed to acquire lock for namespace %s: %v", namespace, err)
				return
			}

			// Wait for test duration to allow multiple renewals
			<-ctx.Done()

			// Release lock
			err = manager.ReleaseNamespaceLock(context.Background(), namespace)
			if err != nil {
				t.Errorf("Failed to release lock for namespace %s: %v", namespace, err)
			}
		}(i)
	}

	wg.Wait()

	// Check that locks were properly maintained during the test
	metrics := manager.GetMetrics()
	t.Logf("Renewal stress test metrics: %+v", metrics)

	if acquisitions, ok := metrics["acquisitions"]; !ok || acquisitions < int64(numNamespaces) {
		t.Errorf("Expected at least %d acquisitions, got %v", numNamespaces, acquisitions)
	}
}

func TestConcurrentLockManagers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	client := fake.NewSimpleClientset()

	const numManagers = 5
	const operationsPerManager = 20

	var wg sync.WaitGroup
	var totalOperations int64

	// Create multiple lock managers operating concurrently
	for i := 0; i < numManagers; i++ {
		wg.Add(1)
		go func(managerID int) {
			defer wg.Done()

			manager := NewNamespaceLockManager(client, fmt.Sprintf("manager-%d", managerID), &NamespaceLockManagerOptions{
				LockTimeout:   2 * time.Second,
				RenewalPeriod: 500 * time.Millisecond,
				MaxRetries:    3,
				BackoffBase:   50 * time.Millisecond,
				LockNamespace: "multi-manager-locks",
			})
			defer manager.Stop(context.Background())

			for j := 0; j < operationsPerManager; j++ {
				namespace := fmt.Sprintf("shared-ns-%d", j%5) // Shared namespaces for contention

				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

				err := manager.AcquireNamespaceLock(ctx, namespace, 1*time.Second)
				if err == nil {
					atomic.AddInt64(&totalOperations, 1)

					// Hold briefly
					time.Sleep(10 * time.Millisecond)

					manager.ReleaseNamespaceLock(context.Background(), namespace)
				}

				cancel()
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Multi-manager test completed %d successful operations", totalOperations)

	if totalOperations == 0 {
		t.Error("Expected at least some successful operations")
	}
}

func BenchmarkNamespaceLockManager_AcquireRelease(b *testing.B) {
	client := fake.NewSimpleClientset()
	manager := NewNamespaceLockManager(client, "bench-instance", &NamespaceLockManagerOptions{
		LockTimeout:   5 * time.Second,
		RenewalPeriod: 1 * time.Second,
		LockNamespace: "bench-locks",
	})
	defer manager.Stop(context.Background())

	namespace := "bench-namespace"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := manager.AcquireNamespaceLock(context.Background(), namespace, 5*time.Second)
		if err != nil {
			b.Fatalf("Failed to acquire lock: %v", err)
		}

		err = manager.ReleaseNamespaceLock(context.Background(), namespace)
		if err != nil {
			b.Fatalf("Failed to release lock: %v", err)
		}
	}
}

func BenchmarkDistributedLockManager_LeaseLock(b *testing.B) {
	client := fake.NewSimpleClientset()

	manager, err := NewDistributedLockManager(client, &DistributedLockOptions{
		Backend:   LockBackendLease,
		Namespace: "bench-ns",
		Identity:  "bench-instance",
	})
	if err != nil {
		b.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Stop(context.Background())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lockKey := fmt.Sprintf("bench-lock-%d", i)
		lock, err := manager.CreateLock(lockKey, LockTypeEFS)
		if err != nil {
			b.Fatalf("Failed to create lock: %v", err)
		}

		err = lock.Acquire(context.Background(), 5*time.Second)
		if err != nil {
			b.Fatalf("Failed to acquire lock: %v", err)
		}

		err = lock.Release(context.Background())
		if err != nil {
			b.Fatalf("Failed to release lock: %v", err)
		}
	}
}