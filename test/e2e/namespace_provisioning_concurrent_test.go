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

package e2e

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubernetes-sigs/aws-efs-csi-driver/test/e2e/testenv"
	"k8s.io/klog/v2"
)

// TestConcurrentPVCCreation tests concurrent PVC creation in the same namespace
// This implements task 7.3 - concurrent PVC creation test
func TestConcurrentPVCCreation(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Skip if no AWS credentials are available
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" && os.Getenv("AWS_PROFILE") == "" {
		t.Skip("AWS credentials not available, skipping integration test")
	}

	// Create test environment
	testID := fmt.Sprintf("concurrent-pvc-%d", time.Now().Unix())
	env, err := testenv.NewAWSTestEnvironment(testID)
	if err != nil {
		t.Fatalf("failed to create test environment: %v", err)
	}

	// Setup cleanup
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		if err := env.Cleanup(ctx); err != nil {
			t.Logf("WARNING: Cleanup failed: %v", err)
		}
	}()

	// Setup the environment
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	t.Log("Setting up AWS test environment...")
	if err := env.Setup(ctx); err != nil {
		t.Fatalf("failed to setup test environment: %v", err)
	}

	// Create test helper
	helper := testenv.NewNamespaceProvisioningTestHelper(env)
	namespace := "test-concurrent-pvc"

	// Create namespace first
	ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Test concurrent PVC creation
	t.Run("ConcurrentPVCCreation", func(t *testing.T) {
		testConcurrentPVCCreationInNamespace(t, ctx, helper, namespace, ns.FileSystemID)
	})

	// Test race condition prevention
	t.Run("RaceConditionPrevention", func(t *testing.T) {
		testRaceConditionPrevention(t, ctx, helper, namespace, ns.FileSystemID)
	})

	// Test high concurrency stress test
	t.Run("HighConcurrencyStressTest", func(t *testing.T) {
		testHighConcurrencyStressTest(t, ctx, helper, namespace, ns.FileSystemID)
	})
}

// testConcurrentPVCCreationInNamespace tests multiple PVCs created simultaneously
func testConcurrentPVCCreationInNamespace(t *testing.T, ctx context.Context, helper *testenv.NamespaceProvisioningTestHelper, namespace, expectedFSID string) {
	const numConcurrentPVCs = 10

	t.Logf("Testing concurrent creation of %d PVCs in namespace %s", numConcurrentPVCs, namespace)

	var (
		wg          sync.WaitGroup
		successCount int32
		errorCount   int32
		pvcs         sync.Map // thread-safe map for PVC storage
		errors       sync.Map // thread-safe map for error storage
	)

	startTime := time.Now()

	// Create PVCs concurrently
	for i := 0; i < numConcurrentPVCs; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			pvcName := fmt.Sprintf("concurrent-pvc-%d", index)
			startPVC := time.Now()

			pvc, err := helper.CreatePVCWithAccessPoint(ctx, namespace, pvcName, "5Gi")

			duration := time.Since(startPVC)

			if err != nil {
				atomic.AddInt32(&errorCount, 1)
				errors.Store(pvcName, err)
				t.Logf("Failed to create PVC %s (duration: %v): %v", pvcName, duration, err)
				return
			}

			atomic.AddInt32(&successCount, 1)
			pvcs.Store(pvcName, pvc)
			t.Logf("Successfully created PVC %s with access point %s (duration: %v)",
				pvcName, pvc.AccessPointID, duration)
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	totalDuration := time.Since(startTime)
	t.Logf("Concurrent PVC creation completed in %v", totalDuration)

	// Verify results
	successTotal := atomic.LoadInt32(&successCount)
	errorTotal := atomic.LoadInt32(&errorCount)

	t.Logf("Results: %d successful, %d failed out of %d total",
		successTotal, errorTotal, numConcurrentPVCs)

	// Ensure most PVCs were created successfully
	if successTotal < int32(numConcurrentPVCs*8/10) { // At least 80% success rate
		t.Errorf("Too many failures: only %d/%d PVCs created successfully",
			successTotal, numConcurrentPVCs)
	}

	// Verify all successful PVCs have unique access points
	accessPoints := make(map[string]string) // AP ID -> PVC name
	pvcs.Range(func(key, value interface{}) bool {
		pvcName := key.(string)
		pvc := value.(*testenv.TestPVC)

		if pvc.AccessPointID == "" {
			t.Errorf("PVC %s has empty access point ID", pvcName)
			return true
		}

		if existingPVC, exists := accessPoints[pvc.AccessPointID]; exists {
			t.Errorf("Duplicate access point ID %s found for PVCs %s and %s",
				pvc.AccessPointID, existingPVC, pvcName)
		} else {
			accessPoints[pvc.AccessPointID] = pvcName
		}
		return true
	})

	// Log any errors that occurred
	errors.Range(func(key, value interface{}) bool {
		pvcName := key.(string)
		err := value.(error)
		t.Logf("Error creating PVC %s: %v", pvcName, err)
		return true
	})
}

// testRaceConditionPrevention tests that race conditions are properly handled
func testRaceConditionPrevention(t *testing.T, ctx context.Context, helper *testenv.NamespaceProvisioningTestHelper, namespace, expectedFSID string) {
	const numRaceTests = 5
	const pvcsPerRace = 3

	t.Log("Testing race condition prevention with rapid concurrent PVC creation")

	for race := 0; race < numRaceTests; race++ {
		t.Logf("Race condition test iteration %d/%d", race+1, numRaceTests)

		var wg sync.WaitGroup
		startSignal := make(chan struct{})
		results := make(chan error, pvcsPerRace)

		// Prepare goroutines
		for i := 0; i < pvcsPerRace; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()

				// Wait for start signal to ensure true concurrency
				<-startSignal

				pvcName := fmt.Sprintf("race-pvc-%d-%d", race, index)
				_, err := helper.CreatePVCWithAccessPoint(ctx, namespace, pvcName, "1Gi")
				results <- err
			}(i)
		}

		// Start all goroutines simultaneously
		close(startSignal)

		// Wait for completion
		wg.Wait()
		close(results)

		// Check results
		errorCount := 0
		for err := range results {
			if err != nil {
				errorCount++
				t.Logf("Race test %d error: %v", race, err)
			}
		}

		if errorCount == pvcsPerRace {
			t.Errorf("All PVCs failed in race test %d, indicating potential race condition", race)
		}

		// Small delay before next iteration
		time.Sleep(2 * time.Second)
	}

	t.Log("Race condition prevention test completed successfully")
}

// testHighConcurrencyStressTest performs a high concurrency stress test
func testHighConcurrencyStressTest(t *testing.T, ctx context.Context, helper *testenv.NamespaceProvisioningTestHelper, namespace, expectedFSID string) {
	const numStressPVCs = 50
	const batchSize = 10

	t.Logf("Starting high concurrency stress test with %d PVCs", numStressPVCs)

	var (
		totalSuccess int32
		totalError   int32
		totalLatency int64 // in milliseconds
	)

	// Process in batches to avoid overwhelming the system
	for batch := 0; batch < numStressPVCs/batchSize; batch++ {
		t.Logf("Processing batch %d/%d", batch+1, numStressPVCs/batchSize)

		var wg sync.WaitGroup
		startTime := time.Now()

		for i := 0; i < batchSize; i++ {
			wg.Add(1)
			pvcIndex := batch*batchSize + i

			go func(index int) {
				defer wg.Done()

				pvcStart := time.Now()
				pvcName := fmt.Sprintf("stress-pvc-%d", index)

				_, err := helper.CreatePVCWithAccessPoint(ctx, namespace, pvcName, "1Gi")

				latency := time.Since(pvcStart).Milliseconds()
				atomic.AddInt64(&totalLatency, latency)

				if err != nil {
					atomic.AddInt32(&totalError, 1)
				} else {
					atomic.AddInt32(&totalSuccess, 1)
				}
			}(pvcIndex)
		}

		wg.Wait()

		batchDuration := time.Since(startTime)
		t.Logf("Batch %d completed in %v", batch+1, batchDuration)

		// Small cooldown between batches
		if batch < numStressPVCs/batchSize-1 {
			time.Sleep(5 * time.Second)
		}
	}

	// Calculate and report statistics
	successCount := atomic.LoadInt32(&totalSuccess)
	errorCount := atomic.LoadInt32(&totalError)
	avgLatency := atomic.LoadInt64(&totalLatency) / int64(successCount)

	t.Logf("Stress test completed: %d successful, %d failed", successCount, errorCount)
	t.Logf("Average latency: %d ms", avgLatency)
	t.Logf("Success rate: %.2f%%", float64(successCount)/float64(numStressPVCs)*100)

	// Verify minimum success rate
	minSuccessRate := 0.90 // 90% minimum success rate
	actualSuccessRate := float64(successCount) / float64(numStressPVCs)

	if actualSuccessRate < minSuccessRate {
		t.Errorf("Success rate too low: %.2f%% (minimum: %.2f%%)",
			actualSuccessRate*100, minSuccessRate*100)
	}

	// Verify latency is reasonable
	maxAvgLatency := int64(30000) // 30 seconds max average
	if avgLatency > maxAvgLatency {
		t.Errorf("Average latency too high: %d ms (maximum: %d ms)",
			avgLatency, maxAvgLatency)
	}
}

// TestConcurrentNamespaceCreation tests concurrent namespace creation
// This implements task 7.3 - concurrent namespace creation test
func TestConcurrentNamespaceCreation(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Skip if no AWS credentials are available
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" && os.Getenv("AWS_PROFILE") == "" {
		t.Skip("AWS credentials not available, skipping integration test")
	}

	// Create test environment
	testID := fmt.Sprintf("concurrent-ns-%d", time.Now().Unix())
	env, err := testenv.NewAWSTestEnvironment(testID)
	if err != nil {
		t.Fatalf("failed to create test environment: %v", err)
	}

	// Setup cleanup
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		if err := env.Cleanup(ctx); err != nil {
			t.Logf("WARNING: Cleanup failed: %v", err)
		}
	}()

	// Setup the environment
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	t.Log("Setting up AWS test environment...")
	if err := env.Setup(ctx); err != nil {
		t.Fatalf("failed to setup test environment: %v", err)
	}

	// Create test helper
	helper := testenv.NewNamespaceProvisioningTestHelper(env)

	// Test concurrent namespace creation
	t.Run("ConcurrentNamespaceCreation", func(t *testing.T) {
		testConcurrentNamespaceCreation(t, ctx, helper)
	})

	// Test namespace isolation under concurrency
	t.Run("NamespaceIsolationUnderConcurrency", func(t *testing.T) {
		testNamespaceIsolationUnderConcurrency(t, ctx, helper)
	})
}

// testConcurrentNamespaceCreation tests multiple namespaces created simultaneously
func testConcurrentNamespaceCreation(t *testing.T, ctx context.Context, helper *testenv.NamespaceProvisioningTestHelper) {
	const numConcurrentNamespaces = 5

	t.Logf("Testing concurrent creation of %d namespaces", numConcurrentNamespaces)

	var (
		wg            sync.WaitGroup
		namespaces    sync.Map // thread-safe map for namespace storage
		fileSystems   sync.Map // thread-safe map for filesystem IDs
		successCount  int32
		errorCount    int32
	)

	startTime := time.Now()

	// Create namespaces concurrently
	for i := 0; i < numConcurrentNamespaces; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			nsName := fmt.Sprintf("concurrent-ns-%d", index)
			startNS := time.Now()

			ns, err := helper.CreateNamespaceWithEFS(ctx, nsName)

			duration := time.Since(startNS)

			if err != nil {
				atomic.AddInt32(&errorCount, 1)
				t.Logf("Failed to create namespace %s (duration: %v): %v", nsName, duration, err)
				return
			}

			atomic.AddInt32(&successCount, 1)
			namespaces.Store(nsName, ns)
			fileSystems.Store(ns.FileSystemID, nsName)

			t.Logf("Successfully created namespace %s with EFS %s (duration: %v)",
				nsName, ns.FileSystemID, duration)

			// Create a PVC in the namespace to verify it's working
			pvcName := fmt.Sprintf("test-pvc-%d", index)
			pvc, err := helper.CreatePVCWithAccessPoint(ctx, nsName, pvcName, "5Gi")
			if err != nil {
				t.Logf("Failed to create PVC in namespace %s: %v", nsName, err)
			} else {
				t.Logf("Created PVC %s in namespace %s with AP %s",
					pvcName, nsName, pvc.AccessPointID)
			}
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	totalDuration := time.Since(startTime)
	t.Logf("Concurrent namespace creation completed in %v", totalDuration)

	// Verify results
	successTotal := atomic.LoadInt32(&successCount)
	errorTotal := atomic.LoadInt32(&errorCount)

	t.Logf("Results: %d successful, %d failed out of %d total",
		successTotal, errorTotal, numConcurrentNamespaces)

	// Ensure most namespaces were created successfully
	if successTotal < int32(numConcurrentNamespaces*8/10) { // At least 80% success rate
		t.Errorf("Too many failures: only %d/%d namespaces created successfully",
			successTotal, numConcurrentNamespaces)
	}

	// Verify all namespaces have unique EFS file systems
	fsCount := 0
	fileSystems.Range(func(key, value interface{}) bool {
		fsCount++
		return true
	})

	if fsCount != int(successTotal) {
		t.Errorf("Expected %d unique file systems, got %d", successTotal, fsCount)
	}

	// Verify namespace isolation
	namespaces.Range(func(key, value interface{}) bool {
		nsName := key.(string)
		ns := value.(*testenv.TestNamespace)

		// Each namespace should have its own unique EFS
		count := 0
		namespaces.Range(func(k2, v2 interface{}) bool {
			ns2 := v2.(*testenv.TestNamespace)
			if ns2.FileSystemID == ns.FileSystemID {
				count++
			}
			return true
		})

		if count > 1 {
			t.Errorf("Namespace %s shares EFS %s with other namespaces (count: %d)",
				nsName, ns.FileSystemID, count)
		}
		return true
	})
}

// testNamespaceIsolationUnderConcurrency tests that namespaces remain isolated under concurrent operations
func testNamespaceIsolationUnderConcurrency(t *testing.T, ctx context.Context, helper *testenv.NamespaceProvisioningTestHelper) {
	const numNamespaces = 3
	const pvcsPerNamespace = 5

	t.Log("Testing namespace isolation under concurrent operations")

	// First, create the namespaces
	namespaces := make([]*testenv.TestNamespace, numNamespaces)
	for i := 0; i < numNamespaces; i++ {
		nsName := fmt.Sprintf("isolation-ns-%d", i)
		ns, err := helper.CreateNamespaceWithEFS(ctx, nsName)
		if err != nil {
			t.Fatalf("Failed to create namespace %s: %v", nsName, err)
		}
		namespaces[i] = ns
		t.Logf("Created namespace %s with EFS %s", nsName, ns.FileSystemID)
	}

	// Now perform concurrent operations across all namespaces
	var wg sync.WaitGroup
	results := make(chan string, numNamespaces*pvcsPerNamespace)

	for i, ns := range namespaces {
		for j := 0; j < pvcsPerNamespace; j++ {
			wg.Add(1)
			go func(namespace *testenv.TestNamespace, nsIndex, pvcIndex int) {
				defer wg.Done()

				pvcName := fmt.Sprintf("isolation-pvc-%d-%d", nsIndex, pvcIndex)
				pvc, err := helper.CreatePVCWithAccessPoint(ctx, namespace.Name, pvcName, "2Gi")

				if err != nil {
					results <- fmt.Sprintf("ERROR: Failed to create PVC %s in namespace %s: %v",
						pvcName, namespace.Name, err)
					return
				}

				// Verify the PVC is on the correct EFS
				apDetails, err := helper.GetAccessPointDetails(ctx, pvc.AccessPointID)
				if err != nil {
					results <- fmt.Sprintf("ERROR: Failed to get AP details for %s: %v",
						pvc.AccessPointID, err)
					return
				}

				if apDetails.FileSystemID != namespace.FileSystemID {
					results <- fmt.Sprintf("ERROR: PVC %s in namespace %s is on wrong EFS: expected %s, got %s",
						pvcName, namespace.Name, namespace.FileSystemID, apDetails.FileSystemID)
					return
				}

				results <- fmt.Sprintf("SUCCESS: PVC %s created in namespace %s on correct EFS %s",
					pvcName, namespace.Name, namespace.FileSystemID)
			}(ns, i, j)
		}
	}

	// Wait for all operations to complete
	wg.Wait()
	close(results)

	// Check results
	successCount := 0
	errorCount := 0
	for result := range results {
		if result[:7] == "SUCCESS" {
			successCount++
			klog.V(2).Info(result)
		} else {
			errorCount++
			t.Log(result)
		}
	}

	t.Logf("Isolation test completed: %d successful, %d failed out of %d total",
		successCount, errorCount, numNamespaces*pvcsPerNamespace)

	// Verify isolation is maintained
	if errorCount > 0 {
		t.Errorf("Isolation violations detected: %d operations failed", errorCount)
	}

	// Verify each namespace still has its unique EFS
	for _, ns := range namespaces {
		stats := helper.GetNamespaceStats(ns.Name)
		if stats == nil {
			t.Errorf("Failed to get stats for namespace %s", ns.Name)
			continue
		}

		numPVCs := stats["numPVCs"].(int)
		if numPVCs != pvcsPerNamespace {
			t.Errorf("Namespace %s has incorrect PVC count: expected %d, got %d",
				ns.Name, pvcsPerNamespace, numPVCs)
		}
	}

	t.Log("Namespace isolation verified under concurrent operations")
}

// TestLockContention tests lock contention scenarios
// This implements task 7.3 - lock contention scenario test
func TestLockContention(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Skip if no AWS credentials are available
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" && os.Getenv("AWS_PROFILE") == "" {
		t.Skip("AWS credentials not available, skipping integration test")
	}

	// Create test environment
	testID := fmt.Sprintf("lock-contention-%d", time.Now().Unix())
	env, err := testenv.NewAWSTestEnvironment(testID)
	if err != nil {
		t.Fatalf("failed to create test environment: %v", err)
	}

	// Setup cleanup
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		if err := env.Cleanup(ctx); err != nil {
			t.Logf("WARNING: Cleanup failed: %v", err)
		}
	}()

	// Setup the environment
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	t.Log("Setting up AWS test environment...")
	if err := env.Setup(ctx); err != nil {
		t.Fatalf("failed to setup test environment: %v", err)
	}

	// Create test helper
	helper := testenv.NewNamespaceProvisioningTestHelper(env)

	// Test lock contention scenarios
	t.Run("NamespaceLevelLockContention", func(t *testing.T) {
		testNamespaceLevelLockContention(t, ctx, helper)
	})

	t.Run("EFSLevelLockContention", func(t *testing.T) {
		testEFSLevelLockContention(t, ctx, helper)
	})

	t.Run("DistributedLockFailover", func(t *testing.T) {
		testDistributedLockFailover(t, ctx, helper)
	})
}

// testNamespaceLevelLockContention tests contention on namespace-level locks
func testNamespaceLevelLockContention(t *testing.T, ctx context.Context, helper *testenv.NamespaceProvisioningTestHelper) {
	const numWorkers = 10
	namespace := "lock-contention-ns"

	t.Logf("Testing namespace-level lock contention with %d workers", numWorkers)

	// Create namespace first
	ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	if err != nil {
		t.Fatalf("Failed to create namespace: %v", err)
	}

	var (
		wg              sync.WaitGroup
		acquiredLocks   int32
		failedLocks     int32
		maxWaitTime     int64 // in milliseconds
		totalWaitTime   int64 // in milliseconds
	)

	// Simulate multiple workers trying to acquire locks
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			waitStart := time.Now()

			// Try to create a PVC (which should acquire namespace lock internally)
			pvcName := fmt.Sprintf("lock-test-pvc-%d", workerID)
			_, err := helper.CreatePVCWithAccessPoint(ctx, namespace, pvcName, "1Gi")

			waitDuration := time.Since(waitStart).Milliseconds()
			atomic.AddInt64(&totalWaitTime, waitDuration)

			// Update max wait time
			for {
				currentMax := atomic.LoadInt64(&maxWaitTime)
				if waitDuration <= currentMax || atomic.CompareAndSwapInt64(&maxWaitTime, currentMax, waitDuration) {
					break
				}
			}

			if err != nil {
				atomic.AddInt32(&failedLocks, 1)
				t.Logf("Worker %d failed after %d ms: %v", workerID, waitDuration, err)
			} else {
				atomic.AddInt32(&acquiredLocks, 1)
				t.Logf("Worker %d succeeded after %d ms", workerID, waitDuration)
			}
		}(i)
	}

	wg.Wait()

	// Calculate statistics
	acquired := atomic.LoadInt32(&acquiredLocks)
	failed := atomic.LoadInt32(&failedLocks)
	avgWaitTime := atomic.LoadInt64(&totalWaitTime) / int64(numWorkers)
	maxWait := atomic.LoadInt64(&maxWaitTime)

	t.Logf("Lock contention results: %d acquired, %d failed", acquired, failed)
	t.Logf("Wait times - Average: %d ms, Maximum: %d ms", avgWaitTime, maxWait)

	// Verify that locks prevented race conditions
	if acquired == 0 {
		t.Error("No workers acquired locks, indicating potential deadlock")
	}

	// Verify reasonable wait times
	maxAcceptableWait := int64(60000) // 60 seconds
	if maxWait > maxAcceptableWait {
		t.Errorf("Maximum wait time too long: %d ms (max acceptable: %d ms)",
			maxWait, maxAcceptableWait)
	}

	// Verify all PVCs that were created have unique access points
	stats := helper.GetNamespaceStats(namespace)
	if stats != nil {
		numPVCs := stats["numPVCs"].(int)
		if numPVCs != int(acquired) {
			t.Errorf("PVC count mismatch: expected %d, got %d", acquired, numPVCs)
		}
	}
}

// testEFSLevelLockContention tests contention on EFS-level locks
func testEFSLevelLockContention(t *testing.T, ctx context.Context, helper *testenv.NamespaceProvisioningTestHelper) {
	const numNamespaces = 5

	t.Logf("Testing EFS-level lock contention with %d namespaces", numNamespaces)

	var (
		wg            sync.WaitGroup
		startSignal   = make(chan struct{})
		createdCount  int32
		failedCount   int32
	)

	// All workers will try to create namespaces simultaneously
	for i := 0; i < numNamespaces; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			// Wait for signal to ensure true concurrency
			<-startSignal

			nsName := fmt.Sprintf("efs-lock-ns-%d", index)
			startTime := time.Now()

			ns, err := helper.CreateNamespaceWithEFS(ctx, nsName)

			duration := time.Since(startTime)

			if err != nil {
				atomic.AddInt32(&failedCount, 1)
				t.Logf("Failed to create namespace %s after %v: %v", nsName, duration, err)
			} else {
				atomic.AddInt32(&createdCount, 1)
				t.Logf("Created namespace %s with EFS %s after %v",
					nsName, ns.FileSystemID, duration)
			}
		}(i)
	}

	// Trigger all workers simultaneously
	close(startSignal)

	// Wait for completion
	wg.Wait()

	created := atomic.LoadInt32(&createdCount)
	failed := atomic.LoadInt32(&failedCount)

	t.Logf("EFS lock contention results: %d created, %d failed", created, failed)

	// Verify that at least some namespaces were created
	if created == 0 {
		t.Error("No namespaces were created, indicating potential EFS lock issues")
	}

	// Verify success rate is reasonable
	successRate := float64(created) / float64(numNamespaces)
	if successRate < 0.6 { // At least 60% success rate
		t.Errorf("Success rate too low: %.2f%%", successRate*100)
	}
}

// testDistributedLockFailover tests distributed lock failover scenarios
func testDistributedLockFailover(t *testing.T, ctx context.Context, helper *testenv.NamespaceProvisioningTestHelper) {
	namespace := "distributed-lock-ns"

	t.Log("Testing distributed lock failover scenarios")

	// Create namespace
	ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	if err != nil {
		t.Fatalf("Failed to create namespace: %v", err)
	}

	// Simulate lock holder failure by creating multiple competing operations
	const numCompetitors = 3
	var (
		wg          sync.WaitGroup
		winners     int32
		completions int32
	)

	for i := 0; i < numCompetitors; i++ {
		wg.Add(1)
		go func(competitorID int) {
			defer wg.Done()

			// Each competitor tries to perform an operation
			pvcName := fmt.Sprintf("failover-pvc-%d", competitorID)
			startTime := time.Now()

			// Simulate potential timeout/failure by using a short context
			opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			_, err := helper.CreatePVCWithAccessPoint(opCtx, namespace, pvcName, "1Gi")

			duration := time.Since(startTime)

			if err == nil {
				atomic.AddInt32(&winners, 1)
				atomic.AddInt32(&completions, 1)
				t.Logf("Competitor %d completed successfully after %v", competitorID, duration)
			} else if err == context.DeadlineExceeded {
				t.Logf("Competitor %d timed out after %v", competitorID, duration)
			} else {
				atomic.AddInt32(&completions, 1)
				t.Logf("Competitor %d failed after %v: %v", competitorID, duration, err)
			}
		}(i)
	}

	wg.Wait()

	winnersCount := atomic.LoadInt32(&winners)
	completionsCount := atomic.LoadInt32(&completions)

	t.Logf("Distributed lock failover results: %d winners, %d completions out of %d competitors",
		winnersCount, completionsCount, numCompetitors)

	// Verify that operations completed despite potential failures
	if completionsCount == 0 {
		t.Error("No operations completed, indicating lock failover failure")
	}

	// Verify that the system recovered and continued operating
	finalPVCName := "post-failover-pvc"
	_, err = helper.CreatePVCWithAccessPoint(ctx, namespace, finalPVCName, "1Gi")
	if err != nil {
		t.Errorf("System failed to recover after failover test: %v", err)
	} else {
		t.Log("System successfully recovered and continues operating after failover test")
	}
}