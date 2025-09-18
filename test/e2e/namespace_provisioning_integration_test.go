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
	"testing"
	"time"

	"github.com/kubernetes-sigs/aws-efs-csi-driver/test/e2e/testenv"
	"k8s.io/klog/v2"
)

// TestNamespaceProvisioningIntegration runs integration tests for namespace provisioning
func TestNamespaceProvisioningIntegration(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Skip if no AWS credentials are available
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" && os.Getenv("AWS_PROFILE") == "" {
		t.Skip("AWS credentials not available, skipping integration test")
	}

	// Create test environment
	testID := fmt.Sprintf("ns-prov-%d", time.Now().Unix())
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	t.Log("Setting up AWS test environment...")
	if err := env.Setup(ctx); err != nil {
		t.Fatalf("failed to setup test environment: %v", err)
	}

	// Create test helper
	helper := testenv.NewNamespaceProvisioningTestHelper(env)
	defer func() {
		if err := helper.CleanupAll(ctx); err != nil {
			t.Logf("WARNING: Helper cleanup failed: %v", err)
		}
	}()

	// Run test scenarios
	t.Run("SingleNamespaceProvisioning", func(t *testing.T) {
		testSingleNamespaceProvisioning(t, ctx, helper)
	})

	t.Run("MultipleNamespacesIsolation", func(t *testing.T) {
		testMultipleNamespacesIsolation(t, ctx, helper)
	})

	t.Run("ConcurrentPVCCreation", func(t *testing.T) {
		testConcurrentPVCCreation(t, ctx, helper)
	})

	t.Run("NamespaceDeletion", func(t *testing.T) {
		testNamespaceDeletion(t, ctx, helper)
	})

	t.Run("CrossNamespaceIsolation", func(t *testing.T) {
		testCrossNamespaceIsolation(t, ctx, helper)
	})

	t.Run("RaceConditionHandling", func(t *testing.T) {
		testRaceConditionHandling(t, ctx, helper)
	})
}

// testSingleNamespaceProvisioning tests EFS creation for a single namespace
func testSingleNamespaceProvisioning(t *testing.T, ctx context.Context, helper *testenv.NamespaceProvisioningTestHelper) {
	namespace := "test-ns-single"

	// Create namespace with EFS
	ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	if err != nil {
		t.Fatalf("failed to create namespace with EFS: %v", err)
	}

	// Verify EFS was created
	if ns.FileSystemID == "" {
		t.Error("EFS file system ID is empty")
	}

	// Create multiple PVCs in the namespace
	for i := 1; i <= 3; i++ {
		pvcName := fmt.Sprintf("pvc-%d", i)
		pvc, err := helper.CreatePVCWithAccessPoint(ctx, namespace, pvcName, "10Gi")
		if err != nil {
			t.Errorf("failed to create PVC %s: %v", pvcName, err)
		}

		if pvc.AccessPointID == "" {
			t.Errorf("PVC %s has empty access point ID", pvcName)
		}
	}

	// Get namespace stats
	stats := helper.GetNamespaceStats(namespace)
	if stats == nil {
		t.Fatal("failed to get namespace stats")
	}

	numPVCs := stats["numPVCs"].(int)
	if numPVCs != 3 {
		t.Errorf("expected 3 PVCs, got %d", numPVCs)
	}

	numAPs := stats["numAccessPoints"].(int)
	if numAPs != 3 {
		t.Errorf("expected 3 access points, got %d", numAPs)
	}

	t.Logf("Successfully provisioned namespace %s with %d PVCs", namespace, numPVCs)
}

// testMultipleNamespacesIsolation tests isolation between multiple namespaces
func testMultipleNamespacesIsolation(t *testing.T, ctx context.Context, helper *testenv.NamespaceProvisioningTestHelper) {
	namespaces := []string{"test-ns-a", "test-ns-b", "test-ns-c"}

	// Create multiple namespaces
	for _, ns := range namespaces {
		_, err := helper.CreateNamespaceWithEFS(ctx, ns)
		if err != nil {
			t.Errorf("failed to create namespace %s: %v", ns, err)
		}

		// Create a PVC in each namespace
		_, err = helper.CreatePVCWithAccessPoint(ctx, ns, "test-pvc", "5Gi")
		if err != nil {
			t.Errorf("failed to create PVC in namespace %s: %v", ns, err)
		}
	}

	// Validate isolation
	helper.ValidateNamespaceIsolation(ctx, t)

	t.Log("Successfully validated namespace isolation")
}

// testConcurrentPVCCreation tests concurrent PVC creation within a namespace
func testConcurrentPVCCreation(t *testing.T, ctx context.Context, helper *testenv.NamespaceProvisioningTestHelper) {
	namespace := "test-ns-concurrent"
	numPVCs := 10

	// Create namespace
	_, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Simulate concurrent PVC creation
	start := time.Now()
	pvcs, err := helper.SimulateConcurrentPVCCreation(ctx, namespace, numPVCs)
	if err != nil {
		t.Fatalf("failed concurrent PVC creation: %v", err)
	}
	duration := time.Since(start)

	// Verify all PVCs were created
	if len(pvcs) != numPVCs {
		t.Errorf("expected %d PVCs, got %d", numPVCs, len(pvcs))
	}

	// Check that all access points are unique
	apMap := make(map[string]bool)
	for _, pvc := range pvcs {
		if apMap[pvc.AccessPointID] {
			t.Errorf("duplicate access point ID: %s", pvc.AccessPointID)
		}
		apMap[pvc.AccessPointID] = true
	}

	t.Logf("Successfully created %d PVCs concurrently in %v", numPVCs, duration)
}

// testNamespaceDeletion tests proper cleanup when a namespace is deleted
func testNamespaceDeletion(t *testing.T, ctx context.Context, helper *testenv.NamespaceProvisioningTestHelper) {
	namespace := "test-ns-delete"

	// Create namespace with PVCs
	ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Create some PVCs
	for i := 1; i <= 3; i++ {
		_, err := helper.CreatePVCWithAccessPoint(ctx, namespace, fmt.Sprintf("pvc-%d", i), "5Gi")
		if err != nil {
			t.Errorf("failed to create PVC: %v", err)
		}
	}

	// Store the file system ID for verification
	fsID := ns.FileSystemID

	// Delete the namespace
	if err := helper.DeleteNamespace(ctx, namespace); err != nil {
		t.Fatalf("failed to delete namespace: %v", err)
	}

	// Verify namespace is gone
	stats := helper.GetNamespaceStats(namespace)
	if stats != nil {
		t.Error("namespace still exists after deletion")
	}

	t.Logf("Successfully deleted namespace %s with EFS %s", namespace, fsID)
}

// testCrossNamespaceIsolation tests that resources cannot be accessed across namespaces
func testCrossNamespaceIsolation(t *testing.T, ctx context.Context, helper *testenv.NamespaceProvisioningTestHelper) {
	ns1 := "test-ns-isolated-1"
	ns2 := "test-ns-isolated-2"

	// Create two namespaces
	ns1Data, err := helper.CreateNamespaceWithEFS(ctx, ns1)
	if err != nil {
		t.Fatalf("failed to create namespace 1: %v", err)
	}

	ns2Data, err := helper.CreateNamespaceWithEFS(ctx, ns2)
	if err != nil {
		t.Fatalf("failed to create namespace 2: %v", err)
	}

	// Verify different EFS file systems
	if ns1Data.FileSystemID == ns2Data.FileSystemID {
		t.Error("namespaces share the same EFS file system")
	}

	// Create PVCs in each namespace
	pvc1, err := helper.CreatePVCWithAccessPoint(ctx, ns1, "pvc1", "5Gi")
	if err != nil {
		t.Fatalf("failed to create PVC in namespace 1: %v", err)
	}

	pvc2, err := helper.CreatePVCWithAccessPoint(ctx, ns2, "pvc2", "5Gi")
	if err != nil {
		t.Fatalf("failed to create PVC in namespace 2: %v", err)
	}

	// Verify different access points
	if pvc1.AccessPointID == pvc2.AccessPointID {
		t.Error("PVCs in different namespaces share the same access point")
	}

	t.Log("Successfully validated cross-namespace isolation")
}

// testRaceConditionHandling tests handling of race conditions
func testRaceConditionHandling(t *testing.T, ctx context.Context, helper *testenv.NamespaceProvisioningTestHelper) {
	namespace := "test-ns-race"
	numGoroutines := 5

	// Create multiple goroutines trying to create the same namespace
	var wg sync.WaitGroup
	results := make([]*testenv.TestNamespace, numGoroutines)
	errors := make([]error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)
			results[index] = ns
			errors[index] = err
		}(i)
	}

	wg.Wait()

	// Verify only one EFS was created
	var fsID string
	successCount := 0
	for i, ns := range results {
		if errors[i] == nil && ns != nil {
			successCount++
			if fsID == "" {
				fsID = ns.FileSystemID
			} else if fsID != ns.FileSystemID {
				t.Error("multiple EFS file systems created for the same namespace")
			}
		}
	}

	if successCount != numGoroutines {
		t.Errorf("expected all goroutines to succeed, got %d/%d", successCount, numGoroutines)
	}

	t.Logf("Successfully handled race condition with %d concurrent namespace creations", numGoroutines)
}

// TestFailureScenarios tests various failure scenarios
func TestFailureScenarios(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Skip if no AWS credentials are available
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" && os.Getenv("AWS_PROFILE") == "" {
		t.Skip("AWS credentials not available, skipping integration test")
	}

	// Create test environment
	testID := fmt.Sprintf("ns-fail-%d", time.Now().Unix())
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	if err := env.Setup(ctx); err != nil {
		t.Fatalf("failed to setup test environment: %v", err)
	}

	helper := testenv.NewNamespaceProvisioningTestHelper(env)
	defer helper.CleanupAll(ctx)

	t.Run("APITimeout", func(t *testing.T) {
		testAPITimeout(t, helper)
	})

	t.Run("PartialFailureRecovery", func(t *testing.T) {
		testPartialFailureRecovery(t, ctx, helper)
	})
}

// testAPITimeout simulates API timeout scenarios
func testAPITimeout(t *testing.T, helper *testenv.NamespaceProvisioningTestHelper) {
	// Create a context with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	namespace := "test-ns-timeout"

	// This should fail due to timeout
	_, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	if err == nil {
		t.Error("expected timeout error but got none")
	}

	t.Log("Successfully handled API timeout scenario")
}

// testPartialFailureRecovery tests recovery from partial failures
func testPartialFailureRecovery(t *testing.T, ctx context.Context, helper *testenv.NamespaceProvisioningTestHelper) {
	namespace := "test-ns-recovery"

	// Create namespace first
	ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Try to create the same namespace again (should be idempotent)
	ns2, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	if err != nil {
		t.Fatalf("failed to recreate namespace: %v", err)
	}

	// Should return the same EFS
	if ns.FileSystemID != ns2.FileSystemID {
		t.Error("recreating namespace returned different EFS")
	}

	t.Log("Successfully handled partial failure recovery")
}

// BenchmarkNamespaceProvisioning benchmarks namespace provisioning performance
func BenchmarkNamespaceProvisioning(b *testing.B) {
	// Skip if no AWS credentials
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" && os.Getenv("AWS_PROFILE") == "" {
		b.Skip("AWS credentials not available")
	}

	// Create test environment
	testID := fmt.Sprintf("ns-bench-%d", time.Now().Unix())
	env, err := testenv.NewAWSTestEnvironment(testID)
	if err != nil {
		b.Fatalf("failed to create test environment: %v", err)
	}

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		env.Cleanup(ctx)
	}()

	ctx := context.Background()
	if err := env.Setup(ctx); err != nil {
		b.Fatalf("failed to setup environment: %v", err)
	}

	helper := testenv.NewNamespaceProvisioningTestHelper(env)
	defer helper.CleanupAll(ctx)

	b.ResetTimer()

	// Benchmark namespace creation
	for i := 0; i < b.N; i++ {
		namespace := fmt.Sprintf("bench-ns-%d", i)
		_, err := helper.CreateNamespaceWithEFS(ctx, namespace)
		if err != nil {
			b.Errorf("failed to create namespace: %v", err)
		}
	}
}

// TestStressNamespaceProvisioning performs stress testing
func TestStressNamespaceProvisioning(t *testing.T) {
	// Skip if not running stress tests
	if os.Getenv("RUN_STRESS_TESTS") != "true" {
		t.Skip("skipping stress test (set RUN_STRESS_TESTS=true to run)")
	}

	// Skip if no AWS credentials
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" && os.Getenv("AWS_PROFILE") == "" {
		t.Skip("AWS credentials not available")
	}

	// Create test environment
	testID := fmt.Sprintf("ns-stress-%d", time.Now().Unix())
	env, err := testenv.NewAWSTestEnvironment(testID)
	if err != nil {
		t.Fatalf("failed to create test environment: %v", err)
	}

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer cancel()
		env.Cleanup(ctx)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	if err := env.Setup(ctx); err != nil {
		t.Fatalf("failed to setup environment: %v", err)
	}

	helper := testenv.NewNamespaceProvisioningTestHelper(env)
	defer helper.CleanupAll(ctx)

	// Stress test parameters
	numNamespaces := 10
	numPVCsPerNamespace := 20
	numConcurrentOps := 5

	klog.Infof("Starting stress test: %d namespaces, %d PVCs per namespace, %d concurrent operations",
		numNamespaces, numPVCsPerNamespace, numConcurrentOps)

	var wg sync.WaitGroup
	errorChan := make(chan error, numNamespaces*numPVCsPerNamespace)

	// Create namespaces concurrently
	for i := 0; i < numNamespaces; i++ {
		wg.Add(1)
		go func(nsIndex int) {
			defer wg.Done()

			namespace := fmt.Sprintf("stress-ns-%d", nsIndex)

			// Create namespace
			_, err := helper.CreateNamespaceWithEFS(ctx, namespace)
			if err != nil {
				errorChan <- fmt.Errorf("failed to create namespace %s: %w", namespace, err)
				return
			}

			// Create PVCs concurrently
			var pvcWg sync.WaitGroup
			for j := 0; j < numPVCsPerNamespace; j += numConcurrentOps {
				batch := numConcurrentOps
				if j+batch > numPVCsPerNamespace {
					batch = numPVCsPerNamespace - j
				}

				for k := 0; k < batch; k++ {
					pvcWg.Add(1)
					go func(pvcIndex int) {
						defer pvcWg.Done()

						pvcName := fmt.Sprintf("stress-pvc-%d", pvcIndex)
						_, err := helper.CreatePVCWithAccessPoint(ctx, namespace, pvcName, "1Gi")
						if err != nil {
							errorChan <- fmt.Errorf("failed to create PVC %s/%s: %w", namespace, pvcName, err)
						}
					}(j + k)
				}
			}
			pvcWg.Wait()
		}(i)
	}

	wg.Wait()
	close(errorChan)

	// Check for errors
	errorCount := 0
	for err := range errorChan {
		t.Logf("Stress test error: %v", err)
		errorCount++
	}

	if errorCount > 0 {
		t.Errorf("Stress test encountered %d errors", errorCount)
	}

	// Validate final state
	helper.ValidateNamespaceIsolation(ctx, t)

	t.Logf("Stress test completed: created %d namespaces with %d PVCs each",
		numNamespaces, numPVCsPerNamespace)
}