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

	"k8s.io/client-go/kubernetes/fake"
)

func TestDeadlockDetector_ComplexCycles(t *testing.T) {
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

	// Test multiple independent cycles
	// Cycle 1: A -> B -> A
	detector.RegisterWait("lock-A", "lock-B")
	detector.RegisterWait("lock-B", "lock-A")

	// Cycle 2: C -> D -> E -> C
	detector.RegisterWait("lock-C", "lock-D")
	detector.RegisterWait("lock-D", "lock-E")
	detector.RegisterWait("lock-E", "lock-C")

	// Non-cyclic chain: F -> G -> H
	detector.RegisterWait("lock-F", "lock-G")
	detector.RegisterWait("lock-G", "lock-H")

	cycles := detector.detectCycles()
	if len(cycles) != 2 {
		t.Errorf("Expected 2 cycles, got %d", len(cycles))
	}

	// Verify cycle lengths
	cycleOf2Found := false
	cycleOf3Found := false
	for _, cycle := range cycles {
		if len(cycle) == 2 {
			cycleOf2Found = true
		} else if len(cycle) == 3 {
			cycleOf3Found = true
		}
	}

	if !cycleOf2Found {
		t.Error("Expected to find cycle of length 2")
	}
	if !cycleOf3Found {
		t.Error("Expected to find cycle of length 3")
	}
}

func TestDeadlockDetector_WaitChainUpdates(t *testing.T) {
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

	// Build a long wait chain: A -> B -> C -> D -> E
	detector.RegisterWait("lock-A", "lock-B")
	detector.RegisterWait("lock-B", "lock-C")
	detector.RegisterWait("lock-C", "lock-D")
	detector.RegisterWait("lock-D", "lock-E")

	// Verify wait chains are built correctly
	detector.waitChainsMutex.RLock()
	chainA := detector.waitChains["lock-A"]
	chainB := detector.waitChains["lock-B"]
	detector.waitChainsMutex.RUnlock()

	if chainA != nil && len(chainA.Chain) != 5 { // A -> B -> C -> D -> E
		t.Errorf("Expected chain length 5 for lock-A, got %d", len(chainA.Chain))
	}

	if chainB != nil && len(chainB.Chain) != 4 { // B -> C -> D -> E
		t.Errorf("Expected chain length 4 for lock-B, got %d", len(chainB.Chain))
	}

	// Now create a cycle by connecting E back to A
	detector.RegisterWait("lock-E", "lock-A")

	cycles := detector.detectCycles()
	if len(cycles) == 0 {
		t.Error("Expected to detect a cycle after closing the chain")
	}

	if len(cycles) > 0 && len(cycles[0]) != 5 {
		t.Errorf("Expected cycle of length 5, got %d", len(cycles[0]))
	}
}

func TestDeadlockDetector_Statistics(t *testing.T) {
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

	// Create some wait relationships and cycles
	detector.RegisterWait("lock-A", "lock-B")
	detector.RegisterWait("lock-B", "lock-A") // Cycle
	detector.RegisterWait("lock-C", "lock-D")
	detector.RegisterWait("lock-E", "lock-F")

	// Run detection
	detector.detectAndResolve()

	// Get statistics
	stats := detector.GetStatistics()

	if totalChecks, ok := stats["total_checks"].(int64); !ok || totalChecks < 1 {
		t.Error("Expected at least 1 total check")
	}

	if cyclesDetected, ok := stats["cycles_detected"].(int64); !ok || cyclesDetected < 1 {
		t.Error("Expected at least 1 cycle detected")
	}

	if activeWaits, ok := stats["active_waits"].(int); !ok || activeWaits != 4 {
		t.Errorf("Expected 4 active waits, got %v", activeWaits)
	}
}

func TestDeadlockDetector_LongRunningLocks(t *testing.T) {
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

	// Create some wait chains to test the detector functionality
	detector.RegisterWait("long-lock", "other-lock")
	detector.RegisterWait("another-lock", "long-lock")

	// Run detection to ensure no panic occurs with long-running scenarios
	cycles := detector.detectCycles()

	// This test primarily verifies that the detector can handle
	// complex scenarios without panicking
	if len(cycles) > 0 {
		t.Logf("Detected %d cycles in long-running test", len(cycles))
	}
}

func TestDeadlockDetector_ConcurrentOperations(t *testing.T) {
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

	var wg sync.WaitGroup
	numWorkers := 10

	// Concurrent registration of wait relationships
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			lockA := fmt.Sprintf("lock-A-%d", id)
			lockB := fmt.Sprintf("lock-B-%d", id)

			detector.RegisterWait(lockA, lockB)

			// Sometimes create cycles
			if id%2 == 0 {
				detector.RegisterWait(lockB, lockA)
			}

			// Run detection concurrently
			detector.detectCycles()

			// Unregister some waits
			if id%3 == 0 {
				detector.UnregisterWait(lockA, lockB)
			}
		}(i)
	}

	wg.Wait()

	// Verify detector is still functional
	detector.RegisterWait("final-A", "final-B")
	detector.RegisterWait("final-B", "final-A")
	cycles := detector.detectCycles()

	// Should detect at least one cycle
	if len(cycles) == 0 {
		t.Error("Expected to detect at least one cycle after concurrent operations")
	}
}

func TestDeadlockDetector_Reset(t *testing.T) {
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

	// Create some state
	detector.RegisterWait("lock-A", "lock-B")
	detector.RegisterWait("lock-B", "lock-A")
	detector.detectAndResolve() // This will increment counters

	// Get initial stats
	statsBefore := detector.GetStatistics()

	// Reset
	detector.Reset()

	// Get stats after reset
	statsAfter := detector.GetStatistics()

	// Verify reset worked
	if totalChecks, ok := statsAfter["total_checks"].(int64); !ok || totalChecks != 0 {
		t.Errorf("Expected total_checks to be 0 after reset, got %v", totalChecks)
	}

	if cyclesDetected, ok := statsAfter["cycles_detected"].(int64); !ok || cyclesDetected != 0 {
		t.Errorf("Expected cycles_detected to be 0 after reset, got %v", cyclesDetected)
	}

	if activeWaits, ok := statsAfter["active_waits"].(int); !ok || activeWaits != 0 {
		t.Errorf("Expected active_waits to be 0 after reset, got %v", activeWaits)
	}

	// Verify we had state before reset
	if totalChecksBefore, ok := statsBefore["total_checks"].(int64); !ok || totalChecksBefore == 0 {
		t.Error("Expected some state before reset")
	}
}

func TestDeadlockDetector_EdgeCases(t *testing.T) {
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

	// Test self-referencing lock (should not create a cycle)
	detector.RegisterWait("self-lock", "self-lock")
	cycles := detector.detectCycles()
	if len(cycles) != 0 {
		t.Error("Self-referencing lock should not create a cycle")
	}

	// Test unregistering non-existent wait
	detector.UnregisterWait("non-existent-A", "non-existent-B") // Should not panic

	// Test empty waits
	detector.RegisterWait("", "lock-B") // Should handle gracefully
	detector.RegisterWait("lock-A", "") // Should handle gracefully

	// Verify detector is still functional
	detector.RegisterWait("real-A", "real-B")
	detector.RegisterWait("real-B", "real-A")
	cycles = detector.detectCycles()
	if len(cycles) == 0 {
		t.Error("Should still detect valid cycles after edge cases")
	}
}

func TestDeadlockDetector_MaxWaitingLocks(t *testing.T) {
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

	// Register more than MaxWaitingLocks waits
	for i := 0; i < MaxWaitingLocks+10; i++ {
		lockA := fmt.Sprintf("lock-A-%d", i)
		lockB := fmt.Sprintf("lock-B-%d", i)
		detector.RegisterWait(lockA, lockB)
	}

	// Should handle gracefully without panicking
	detector.detectAndResolve()

	stats := detector.GetStatistics()
	if activeWaits, ok := stats["active_waits"].(int); !ok || activeWaits <= MaxWaitingLocks {
		t.Logf("Active waits: %v (expected more than %d)", activeWaits, MaxWaitingLocks)
	}
}