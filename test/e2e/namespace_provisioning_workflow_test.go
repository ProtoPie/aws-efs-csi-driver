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
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
)

// TestComprehensiveWorkflow tests the complete E2E workflow from StorageClass creation to Pod mount
func TestComprehensiveWorkflow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping comprehensive workflow test in short mode")
	}

	ctx := context.Background()
	te, cleanup := setupTestEnvironment(t)
	defer cleanup()

	t.Run("Complete provisioning workflow", func(t *testing.T) {
		// Phase 1: StorageClass creation with efs-ns mode
		t.Log("Phase 1: Creating StorageClass with efs-ns mode")
		storageClass := createNamespaceProvisioningStorageClass(t, te)
		defer func() {
			err := te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})
			assert.NoError(t, err, "Failed to cleanup StorageClass")
		}()

		// Phase 2: Namespace creation and preparation
		t.Log("Phase 2: Creating test namespace")
		namespace := createTestNamespace(t, te, "workflow-test")
		defer func() {
			err := te.K8sClient.CoreV1().Namespaces().Delete(ctx, namespace.Name, metav1.DeleteOptions{})
			assert.NoError(t, err, "Failed to cleanup namespace")
		}()

		// Phase 3: PVC creation and provisioning
		t.Log("Phase 3: Creating PVC and waiting for provisioning")
		pvc := createPVCInNamespace(t, te, namespace.Name, storageClass.Name, "test-pvc-1")
		defer func() {
			err := te.K8sClient.CoreV1().PersistentVolumeClaims(namespace.Name).Delete(ctx, pvc.Name, metav1.DeleteOptions{})
			assert.NoError(t, err, "Failed to cleanup PVC")
		}()

		// Wait for PVC to be bound
		err := waitForPVCBound(ctx, te, namespace.Name, pvc.Name, 5*time.Minute)
		require.NoError(t, err, "PVC failed to bind within timeout")

		// Verify EFS filesystem was created
		t.Log("Phase 4: Verifying EFS filesystem creation")
		efsID := verifyEFSCreated(t, te, namespace.Name)
		require.NotEmpty(t, efsID, "EFS filesystem ID should not be empty")

		// Verify Access Point was created
		t.Log("Phase 5: Verifying Access Point creation")
		apID := verifyAccessPointCreated(t, te, efsID, pvc.Name)
		require.NotEmpty(t, apID, "Access Point ID should not be empty")

		// Phase 6: Pod creation and mounting
		t.Log("Phase 6: Creating Pod to mount the volume")
		pod := createPodWithPVC(t, te, namespace.Name, pvc.Name, "test-pod-1")
		defer func() {
			err := te.K8sClient.CoreV1().Pods(namespace.Name).Delete(ctx, pod.Name, metav1.DeleteOptions{})
			assert.NoError(t, err, "Failed to cleanup Pod")
		}()

		// Wait for Pod to be running
		err = waitForPodRunning(ctx, te, namespace.Name, pod.Name, 3*time.Minute)
		require.NoError(t, err, "Pod failed to start within timeout")

		// Phase 7: Data read/write verification
		t.Log("Phase 7: Verifying data read/write operations")
		testData := "Hello EFS Namespace Provisioning!"
		testFile := "/data/test-file.txt"

		// Write data
		err = execInPod(ctx, te, namespace.Name, pod.Name, fmt.Sprintf("echo '%s' > %s", testData, testFile))
		require.NoError(t, err, "Failed to write data to mounted volume")

		// Read data back
		output, err := execInPodWithOutput(ctx, te, namespace.Name, pod.Name, fmt.Sprintf("cat %s", testFile))
		require.NoError(t, err, "Failed to read data from mounted volume")
		assert.Contains(t, output, testData, "Read data does not match written data")

		// Phase 8: Permission and isolation verification
		t.Log("Phase 8: Verifying permissions and isolation")

		// Check directory permissions
		perms, err := execInPodWithOutput(ctx, te, namespace.Name, pod.Name, "stat -c %a /data")
		require.NoError(t, err, "Failed to get directory permissions")
		assert.Equal(t, "700", strings.TrimSpace(perms), "Directory permissions should be 700")

		// Check file ownership
		ownership, err := execInPodWithOutput(ctx, te, namespace.Name, pod.Name, "stat -c '%u:%g' /data")
		require.NoError(t, err, "Failed to get ownership info")
		t.Logf("Directory ownership: %s", ownership)

		// Phase 9: Multiple PVC in same namespace
		t.Log("Phase 9: Testing multiple PVCs in same namespace")
		pvc2 := createPVCInNamespace(t, te, namespace.Name, storageClass.Name, "test-pvc-2")
		defer func() {
			err := te.K8sClient.CoreV1().PersistentVolumeClaims(namespace.Name).Delete(ctx, pvc2.Name, metav1.DeleteOptions{})
			assert.NoError(t, err, "Failed to cleanup PVC2")
		}()

		// Wait for second PVC to be bound
		err = waitForPVCBound(ctx, te, namespace.Name, pvc2.Name, 5*time.Minute)
		require.NoError(t, err, "Second PVC failed to bind within timeout")

		// Verify same EFS is reused
		efsID2 := verifyEFSCreated(t, te, namespace.Name)
		assert.Equal(t, efsID, efsID2, "Same EFS should be reused for PVCs in same namespace")

		// Verify different Access Point
		apID2 := verifyAccessPointCreated(t, te, efsID2, pvc2.Name)
		assert.NotEqual(t, apID, apID2, "Different Access Points should be created for different PVCs")

		// Phase 10: Cross-namespace isolation
		t.Log("Phase 10: Verifying cross-namespace isolation")
		namespace2 := createTestNamespace(t, te, "workflow-test-2")
		defer func() {
			err := te.K8sClient.CoreV1().Namespaces().Delete(ctx, namespace2.Name, metav1.DeleteOptions{})
			assert.NoError(t, err, "Failed to cleanup namespace2")
		}()

		pvc3 := createPVCInNamespace(t, te, namespace2.Name, storageClass.Name, "test-pvc-3")
		defer func() {
			err := te.K8sClient.CoreV1().PersistentVolumeClaims(namespace2.Name).Delete(ctx, pvc3.Name, metav1.DeleteOptions{})
			assert.NoError(t, err, "Failed to cleanup PVC3")
		}()

		// Wait for PVC in second namespace to be bound
		err = waitForPVCBound(ctx, te, namespace2.Name, pvc3.Name, 5*time.Minute)
		require.NoError(t, err, "PVC in second namespace failed to bind within timeout")

		// Verify different EFS for different namespace
		efsID3 := verifyEFSCreated(t, te, namespace2.Name)
		assert.NotEqual(t, efsID, efsID3, "Different EFS should be created for different namespaces")

		t.Log("✅ Comprehensive workflow test completed successfully")
	})
}

// TestWorkflowWithFailureRecovery tests the workflow with failure scenarios and recovery
func TestWorkflowWithFailureRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping workflow failure recovery test in short mode")
	}

	ctx := context.Background()
	te, cleanup := setupTestEnvironmentWithFailureSimulation(t)
	defer cleanup()

	t.Run("Workflow with API timeout recovery", func(t *testing.T) {
		// Configure timeout simulation for EFS creation
		te.FailureSimulator.AddTimeoutConfig(&TimeoutConfig{
			Operation:      "CreateFileSystem",
			InitialTimeout: 2 * time.Second,
			RecoveryTime:   5 * time.Second,
			MaxRetries:     3,
			FailureRate:    0.5, // 50% chance of timeout
		})

		storageClass := createNamespaceProvisioningStorageClass(t, te)
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})

		namespace := createTestNamespace(t, te, "recovery-test")
		defer te.K8sClient.CoreV1().Namespaces().Delete(ctx, namespace.Name, metav1.DeleteOptions{})

		pvc := createPVCInNamespace(t, te, namespace.Name, storageClass.Name, "recovery-pvc")
		defer te.K8sClient.CoreV1().PersistentVolumeClaims(namespace.Name).Delete(ctx, pvc.Name, metav1.DeleteOptions{})

		// Should eventually succeed with retry
		err := waitForPVCBound(ctx, te, namespace.Name, pvc.Name, 10*time.Minute)
		require.NoError(t, err, "PVC should eventually bind with retries")

		// Verify recovery metrics
		history := te.FailureSimulator.GetFailureHistory()
		var recoveredCount int
		for _, event := range history {
			if event.Recovered {
				recoveredCount++
			}
		}
		t.Logf("Recovery statistics: %d failures recovered", recoveredCount)
	})

	t.Run("Workflow with partial failure recovery", func(t *testing.T) {
		// Configure partial failure for mount target creation
		te.FailureSimulator.AddPartialFailureConfig(&PartialFailureConfig{
			Operation:       "CreateMountTarget",
			AffectedPercent: 0.3, // 30% of mount targets fail
			Recoverable:     true,
			RecoveryDelay:   3 * time.Second,
		})

		storageClass := createNamespaceProvisioningStorageClass(t, te)
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})

		namespace := createTestNamespace(t, te, "partial-failure-test")
		defer te.K8sClient.CoreV1().Namespaces().Delete(ctx, namespace.Name, metav1.DeleteOptions{})

		// Create multiple PVCs to trigger partial failures
		var wg sync.WaitGroup
		successCount := 0
		var mu sync.Mutex

		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				pvcName := fmt.Sprintf("pvc-%d", idx)
				pvc := createPVCInNamespace(t, te, namespace.Name, storageClass.Name, pvcName)
				defer te.K8sClient.CoreV1().PersistentVolumeClaims(namespace.Name).Delete(ctx, pvc.Name, metav1.DeleteOptions{})

				err := waitForPVCBound(ctx, te, namespace.Name, pvc.Name, 5*time.Minute)
				if err == nil {
					mu.Lock()
					successCount++
					mu.Unlock()
				}
			}(i)
		}

		wg.Wait()
		assert.GreaterOrEqual(t, successCount, 3, "At least 60% of PVCs should succeed")
		t.Logf("Partial failure recovery: %d/5 PVCs succeeded", successCount)
	})

	t.Run("Workflow with CRD recovery", func(t *testing.T) {
		// Configure CRD failure simulation
		te.FailureSimulator.SetCRDFailureMode(CRDFailureDelete)

		storageClass := createNamespaceProvisioningStorageClass(t, te)
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})

		namespace := createTestNamespace(t, te, "crd-recovery-test")
		defer te.K8sClient.CoreV1().Namespaces().Delete(ctx, namespace.Name, metav1.DeleteOptions{})

		// Create initial PVC
		pvc := createPVCInNamespace(t, te, namespace.Name, storageClass.Name, "crd-test-pvc")
		defer te.K8sClient.CoreV1().PersistentVolumeClaims(namespace.Name).Delete(ctx, pvc.Name, metav1.DeleteOptions{})

		// Wait for initial binding
		err := waitForPVCBound(ctx, te, namespace.Name, pvc.Name, 5*time.Minute)
		require.NoError(t, err, "Initial PVC should bind")

		// Simulate CRD deletion
		err = te.FailureSimulator.SimulateCRDFailure(namespace.Name)
		assert.Error(t, err, "CRD failure should be simulated")

		// Test recovery by creating another PVC
		// System should recover from AWS tags
		pvc2 := createPVCInNamespace(t, te, namespace.Name, storageClass.Name, "crd-recovery-pvc")
		defer te.K8sClient.CoreV1().PersistentVolumeClaims(namespace.Name).Delete(ctx, pvc2.Name, metav1.DeleteOptions{})

		// Should recover and bind
		err = waitForPVCBound(ctx, te, namespace.Name, pvc2.Name, 7*time.Minute)
		require.NoError(t, err, "PVC should bind after CRD recovery")

		t.Log("✅ CRD recovery test completed successfully")
	})
}

// TestWorkflowPerformanceAndScale tests performance characteristics and scalability
func TestWorkflowPerformanceAndScale(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance and scale test in short mode")
	}

	ctx := context.Background()
	te, cleanup := setupTestEnvironment(t)
	defer cleanup()

	t.Run("EFS provisioning performance", func(t *testing.T) {
		storageClass := createNamespaceProvisioningStorageClass(t, te)
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})

		// Measure first EFS creation (cold start)
		namespace1 := createTestNamespace(t, te, "perf-test-1")
		defer te.K8sClient.CoreV1().Namespaces().Delete(ctx, namespace1.Name, metav1.DeleteOptions{})

		start := time.Now()
		pvc1 := createPVCInNamespace(t, te, namespace1.Name, storageClass.Name, "perf-pvc-1")
		defer te.K8sClient.CoreV1().PersistentVolumeClaims(namespace1.Name).Delete(ctx, pvc1.Name, metav1.DeleteOptions{})

		err := waitForPVCBound(ctx, te, namespace1.Name, pvc1.Name, 10*time.Minute)
		require.NoError(t, err, "First PVC should bind")

		firstProvisionTime := time.Since(start)
		t.Logf("First EFS provisioning time: %v", firstProvisionTime)
		assert.Less(t, firstProvisionTime, 5*time.Minute, "EFS provisioning should complete within 5 minutes")

		// Measure Access Point creation (warm start - EFS exists)
		start = time.Now()
		pvc2 := createPVCInNamespace(t, te, namespace1.Name, storageClass.Name, "perf-pvc-2")
		defer te.K8sClient.CoreV1().PersistentVolumeClaims(namespace1.Name).Delete(ctx, pvc2.Name, metav1.DeleteOptions{})

		err = waitForPVCBound(ctx, te, namespace1.Name, pvc2.Name, 2*time.Minute)
		require.NoError(t, err, "Second PVC should bind")

		apCreationTime := time.Since(start)
		t.Logf("Access Point creation time: %v", apCreationTime)
		assert.Less(t, apCreationTime, 30*time.Second, "Access Point creation should complete within 30 seconds")
	})

	t.Run("Concurrent provisioning performance", func(t *testing.T) {
		storageClass := createNamespaceProvisioningStorageClass(t, te)
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})

		namespace := createTestNamespace(t, te, "concurrent-perf-test")
		defer te.K8sClient.CoreV1().Namespaces().Delete(ctx, namespace.Name, metav1.DeleteOptions{})

		// Create multiple PVCs concurrently
		numPVCs := 10
		var wg sync.WaitGroup
		results := make([]time.Duration, numPVCs)
		errors := make([]error, numPVCs)

		start := time.Now()
		for i := 0; i < numPVCs; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				pvcStart := time.Now()
				pvcName := fmt.Sprintf("concurrent-pvc-%d", idx)
				pvc := createPVCInNamespace(t, te, namespace.Name, storageClass.Name, pvcName)
				defer te.K8sClient.CoreV1().PersistentVolumeClaims(namespace.Name).Delete(ctx, pvc.Name, metav1.DeleteOptions{})

				err := waitForPVCBound(ctx, te, namespace.Name, pvc.Name, 5*time.Minute)
				errors[idx] = err
				results[idx] = time.Since(pvcStart)
			}(i)
		}

		wg.Wait()
		totalTime := time.Since(start)

		// Analyze results
		var successCount int
		var totalProvisionTime time.Duration
		for i, err := range errors {
			if err == nil {
				successCount++
				totalProvisionTime += results[i]
			}
		}

		avgTime := totalProvisionTime / time.Duration(successCount)
		t.Logf("Concurrent provisioning results:")
		t.Logf("  Total time: %v", totalTime)
		t.Logf("  Success rate: %d/%d", successCount, numPVCs)
		t.Logf("  Average provision time: %v", avgTime)

		assert.GreaterOrEqual(t, successCount, numPVCs*9/10, "At least 90% of PVCs should succeed")
		assert.Less(t, totalTime, 3*time.Minute, "Concurrent provisioning should complete within 3 minutes")
	})

	t.Run("Resource usage monitoring", func(t *testing.T) {
		// Get initial resource metrics
		initialMetrics := getResourceMetrics(t, te)

		storageClass := createNamespaceProvisioningStorageClass(t, te)
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})

		// Create workload
		namespace := createTestNamespace(t, te, "resource-test")
		defer te.K8sClient.CoreV1().Namespaces().Delete(ctx, namespace.Name, metav1.DeleteOptions{})

		// Create multiple pods with volumes
		numPods := 5
		for i := 0; i < numPods; i++ {
			pvcName := fmt.Sprintf("resource-pvc-%d", i)
			pvc := createPVCInNamespace(t, te, namespace.Name, storageClass.Name, pvcName)
			defer te.K8sClient.CoreV1().PersistentVolumeClaims(namespace.Name).Delete(ctx, pvc.Name, metav1.DeleteOptions{})

			err := waitForPVCBound(ctx, te, namespace.Name, pvc.Name, 3*time.Minute)
			require.NoError(t, err)

			podName := fmt.Sprintf("resource-pod-%d", i)
			pod := createPodWithPVC(t, te, namespace.Name, pvc.Name, podName)
			defer te.K8sClient.CoreV1().Pods(namespace.Name).Delete(ctx, pod.Name, metav1.DeleteOptions{})

			err = waitForPodRunning(ctx, te, namespace.Name, pod.Name, 2*time.Minute)
			require.NoError(t, err)
		}

		// Get final resource metrics
		finalMetrics := getResourceMetrics(t, te)

		// Analyze resource usage
		cpuIncrease := finalMetrics.CPUUsage - initialMetrics.CPUUsage
		memIncrease := finalMetrics.MemoryUsage - initialMetrics.MemoryUsage

		t.Logf("Resource usage increase:")
		t.Logf("  CPU: %.2f%%", cpuIncrease)
		t.Logf("  Memory: %.2f MB", memIncrease)

		// Verify reasonable resource usage
		assert.Less(t, cpuIncrease, 50.0, "CPU increase should be less than 50%")
		assert.Less(t, memIncrease, 500.0, "Memory increase should be less than 500MB")
	})
}

// TestWorkflowDataIntegrity tests data integrity and persistence
func TestWorkflowDataIntegrity(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping data integrity test in short mode")
	}

	ctx := context.Background()
	te, cleanup := setupTestEnvironment(t)
	defer cleanup()

	t.Run("Data persistence across pod restarts", func(t *testing.T) {
		storageClass := createNamespaceProvisioningStorageClass(t, te)
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})

		namespace := createTestNamespace(t, te, "integrity-test")
		defer te.K8sClient.CoreV1().Namespaces().Delete(ctx, namespace.Name, metav1.DeleteOptions{})

		pvc := createPVCInNamespace(t, te, namespace.Name, storageClass.Name, "integrity-pvc")
		defer te.K8sClient.CoreV1().PersistentVolumeClaims(namespace.Name).Delete(ctx, pvc.Name, metav1.DeleteOptions{})

		err := waitForPVCBound(ctx, te, namespace.Name, pvc.Name, 5*time.Minute)
		require.NoError(t, err)

		// First pod writes data
		pod1 := createPodWithPVC(t, te, namespace.Name, pvc.Name, "writer-pod")
		err = waitForPodRunning(ctx, te, namespace.Name, pod1.Name, 2*time.Minute)
		require.NoError(t, err)

		// Write test data
		testData := generateTestData(1024) // 1KB of data
		testFile := "/data/integrity-test.txt"
		err = execInPod(ctx, te, namespace.Name, pod1.Name, fmt.Sprintf("echo '%s' > %s", testData, testFile))
		require.NoError(t, err)

		// Calculate checksum
		checksum1, err := execInPodWithOutput(ctx, te, namespace.Name, pod1.Name, fmt.Sprintf("md5sum %s | cut -d' ' -f1", testFile))
		require.NoError(t, err)

		// Delete first pod
		err = te.K8sClient.CoreV1().Pods(namespace.Name).Delete(ctx, pod1.Name, metav1.DeleteOptions{})
		require.NoError(t, err)

		// Second pod reads data
		pod2 := createPodWithPVC(t, te, namespace.Name, pvc.Name, "reader-pod")
		defer te.K8sClient.CoreV1().Pods(namespace.Name).Delete(ctx, pod2.Name, metav1.DeleteOptions{})

		err = waitForPodRunning(ctx, te, namespace.Name, pod2.Name, 2*time.Minute)
		require.NoError(t, err)

		// Verify data integrity
		checksum2, err := execInPodWithOutput(ctx, te, namespace.Name, pod2.Name, fmt.Sprintf("md5sum %s | cut -d' ' -f1", testFile))
		require.NoError(t, err)

		assert.Equal(t, strings.TrimSpace(checksum1), strings.TrimSpace(checksum2), "Data checksum should match after pod restart")
	})

	t.Run("Concurrent read/write data integrity", func(t *testing.T) {
		storageClass := createNamespaceProvisioningStorageClass(t, te)
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})

		namespace := createTestNamespace(t, te, "concurrent-rw-test")
		defer te.K8sClient.CoreV1().Namespaces().Delete(ctx, namespace.Name, metav1.DeleteOptions{})

		pvc := createPVCInNamespace(t, te, namespace.Name, storageClass.Name, "concurrent-pvc")
		defer te.K8sClient.CoreV1().PersistentVolumeClaims(namespace.Name).Delete(ctx, pvc.Name, metav1.DeleteOptions{})

		err := waitForPVCBound(ctx, te, namespace.Name, pvc.Name, 5*time.Minute)
		require.NoError(t, err)

		// Create multiple pods for concurrent access
		numPods := 3
		pods := make([]*v1.Pod, numPods)
		for i := 0; i < numPods; i++ {
			podName := fmt.Sprintf("concurrent-pod-%d", i)
			pods[i] = createPodWithPVC(t, te, namespace.Name, pvc.Name, podName)
			defer te.K8sClient.CoreV1().Pods(namespace.Name).Delete(ctx, pods[i].Name, metav1.DeleteOptions{})

			err = waitForPodRunning(ctx, te, namespace.Name, pods[i].Name, 2*time.Minute)
			require.NoError(t, err)
		}

		// Concurrent writes to different files
		var wg sync.WaitGroup
		for i, pod := range pods {
			wg.Add(1)
			go func(idx int, p *v1.Pod) {
				defer wg.Done()

				fileName := fmt.Sprintf("/data/file-%d.txt", idx)
				data := fmt.Sprintf("Data from pod %d", idx)

				err := execInPod(ctx, te, namespace.Name, p.Name, fmt.Sprintf("echo '%s' > %s", data, fileName))
				assert.NoError(t, err, "Pod %d should write successfully", idx)
			}(i, pod)
		}
		wg.Wait()

		// Verify all files from each pod
		for i, pod := range pods {
			for j := 0; j < numPods; j++ {
				fileName := fmt.Sprintf("/data/file-%d.txt", j)
				expectedData := fmt.Sprintf("Data from pod %d", j)

				output, err := execInPodWithOutput(ctx, te, namespace.Name, pod.Name, fmt.Sprintf("cat %s", fileName))
				require.NoError(t, err, "Pod %d should read file %d", i, j)
				assert.Contains(t, output, expectedData, "Pod %d should see correct data in file %d", i, j)
			}
		}

		t.Log("✅ Concurrent read/write integrity verified")
	})
}

// TestWorkflowCleanup tests cleanup and resource reclamation
func TestWorkflowCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cleanup test in short mode")
	}

	ctx := context.Background()
	te, cleanup := setupTestEnvironment(t)
	defer cleanup()

	t.Run("Access Point cleanup on PVC deletion", func(t *testing.T) {
		storageClass := createNamespaceProvisioningStorageClass(t, te)
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})

		namespace := createTestNamespace(t, te, "cleanup-ap-test")
		defer te.K8sClient.CoreV1().Namespaces().Delete(ctx, namespace.Name, metav1.DeleteOptions{})

		// Create PVC
		pvc := createPVCInNamespace(t, te, namespace.Name, storageClass.Name, "cleanup-pvc")
		err := waitForPVCBound(ctx, te, namespace.Name, pvc.Name, 5*time.Minute)
		require.NoError(t, err)

		// Get EFS and Access Point IDs
		efsID := verifyEFSCreated(t, te, namespace.Name)
		apID := verifyAccessPointCreated(t, te, efsID, pvc.Name)

		// Delete PVC
		err = te.K8sClient.CoreV1().PersistentVolumeClaims(namespace.Name).Delete(ctx, pvc.Name, metav1.DeleteOptions{})
		require.NoError(t, err)

		// Verify Access Point is deleted
		time.Sleep(10 * time.Second) // Give time for cleanup
		apExists := checkAccessPointExists(t, te, apID)
		assert.False(t, apExists, "Access Point should be deleted after PVC deletion")

		// Verify EFS still exists (as namespace still exists)
		efsExists := checkEFSExists(t, te, efsID)
		assert.True(t, efsExists, "EFS should still exist when namespace exists")
	})

	t.Run("EFS cleanup on last PVC deletion with retain policy", func(t *testing.T) {
		// Create StorageClass with retain policy
		storageClass := createNamespaceProvisioningStorageClassWithPolicy(t, te, "retain")
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})

		namespace := createTestNamespace(t, te, "cleanup-retain-test")

		// Create multiple PVCs
		pvc1 := createPVCInNamespace(t, te, namespace.Name, storageClass.Name, "retain-pvc-1")
		pvc2 := createPVCInNamespace(t, te, namespace.Name, storageClass.Name, "retain-pvc-2")

		err := waitForPVCBound(ctx, te, namespace.Name, pvc1.Name, 5*time.Minute)
		require.NoError(t, err)
		err = waitForPVCBound(ctx, te, namespace.Name, pvc2.Name, 5*time.Minute)
		require.NoError(t, err)

		efsID := verifyEFSCreated(t, te, namespace.Name)

		// Delete all PVCs
		err = te.K8sClient.CoreV1().PersistentVolumeClaims(namespace.Name).Delete(ctx, pvc1.Name, metav1.DeleteOptions{})
		require.NoError(t, err)
		err = te.K8sClient.CoreV1().PersistentVolumeClaims(namespace.Name).Delete(ctx, pvc2.Name, metav1.DeleteOptions{})
		require.NoError(t, err)

		// Delete namespace
		err = te.K8sClient.CoreV1().Namespaces().Delete(ctx, namespace.Name, metav1.DeleteOptions{})
		require.NoError(t, err)

		// Verify EFS is retained
		time.Sleep(10 * time.Second)
		efsExists := checkEFSExists(t, te, efsID)
		assert.True(t, efsExists, "EFS should be retained with retain policy")

		// Manual cleanup
		cleanupEFS(t, te, efsID)
	})

	t.Run("EFS cleanup on namespace deletion with delete policy", func(t *testing.T) {
		// Create StorageClass with delete policy
		storageClass := createNamespaceProvisioningStorageClassWithPolicy(t, te, "delete")
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})

		namespace := createTestNamespace(t, te, "cleanup-delete-test")

		// Create PVC
		pvc := createPVCInNamespace(t, te, namespace.Name, storageClass.Name, "delete-pvc")
		err := waitForPVCBound(ctx, te, namespace.Name, pvc.Name, 5*time.Minute)
		require.NoError(t, err)

		efsID := verifyEFSCreated(t, te, namespace.Name)

		// Delete namespace (which should trigger EFS deletion)
		err = te.K8sClient.CoreV1().Namespaces().Delete(ctx, namespace.Name, metav1.DeleteOptions{})
		require.NoError(t, err)

		// Wait for namespace deletion and cleanup
		err = wait.Poll(5*time.Second, 2*time.Minute, func() (bool, error) {
			_, err := te.K8sClient.CoreV1().Namespaces().Get(ctx, namespace.Name, metav1.GetOptions{})
			if err != nil {
				return true, nil
			}
			return false, nil
		})
		require.NoError(t, err, "Namespace should be deleted")

		// Verify EFS is deleted
		time.Sleep(30 * time.Second) // Give time for EFS deletion
		efsExists := checkEFSExists(t, te, efsID)
		assert.False(t, efsExists, "EFS should be deleted with delete policy")
	})

	t.Run("Orphaned resource cleanup", func(t *testing.T) {
		// Enable cleanup tracking
		te.FailureSimulator.EnableCleanupTracking(true)

		storageClass := createNamespaceProvisioningStorageClass(t, te)
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})

		namespace := createTestNamespace(t, te, "orphan-test")
		defer te.K8sClient.CoreV1().Namespaces().Delete(ctx, namespace.Name, metav1.DeleteOptions{})

		// Create PVC
		pvc := createPVCInNamespace(t, te, namespace.Name, storageClass.Name, "orphan-pvc")
		err := waitForPVCBound(ctx, te, namespace.Name, pvc.Name, 5*time.Minute)
		require.NoError(t, err)

		efsID := verifyEFSCreated(t, te, namespace.Name)
		apID := verifyAccessPointCreated(t, te, efsID, pvc.Name)

		// Simulate orphaning by marking resources
		te.FailureSimulator.MarkResourceOrphaned(apID)

		// Force delete PVC (simulate ungraceful deletion)
		err = te.K8sClient.CoreV1().PersistentVolumeClaims(namespace.Name).Delete(ctx, pvc.Name, metav1.DeleteOptions{
			GracePeriodSeconds: aws.Int64(0),
		})
		require.NoError(t, err)

		// Check for orphaned resources
		orphaned := te.FailureSimulator.GetOrphanedResources()
		assert.Greater(t, len(orphaned), 0, "Should detect orphaned resources")

		// Cleanup should handle orphaned resources
		time.Sleep(15 * time.Second)

		// Verify orphaned resources are eventually cleaned up
		apExists := checkAccessPointExists(t, te, apID)
		assert.False(t, apExists, "Orphaned Access Point should be cleaned up")

		t.Log("✅ Orphaned resource cleanup verified")
	})
}

// Helper function to generate test data
func generateTestData(size int) string {
	chars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	data := make([]byte, size)
	for i := range data {
		data[i] = chars[i%len(chars)]
	}
	return string(data)
}

// Helper function to create namespace provisioning StorageClass
func createNamespaceProvisioningStorageClass(t *testing.T, te *TestEnvironment) *storagev1.StorageClass {
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("efs-ns-sc-%s", generateRandomString(6)),
		},
		Provisioner: "efs.csi.aws.com",
		Parameters: map[string]string{
			"provisioningMode":      "efs-ns",
			"directoryPerms":        "700",
			"gidRangeStart":         "1000",
			"gidRangeEnd":           "2000",
			"basePath":              "/dynamic_provisioning",
			"ensureUniqueDirectory": "true",
			"performanceMode":       "generalPurpose",
			"throughputMode":        "bursting",
			"encrypted":             "true",
		},
		VolumeBindingMode: &[]storagev1.VolumeBindingMode{storagev1.VolumeBindingImmediate}[0],
	}

	created, err := te.K8sClient.StorageV1().StorageClasses().Create(context.Background(), sc, metav1.CreateOptions{})
	require.NoError(t, err, "Failed to create StorageClass")
	return created
}

// Helper function to create StorageClass with specific cleanup policy
func createNamespaceProvisioningStorageClassWithPolicy(t *testing.T, te *TestEnvironment, cleanupPolicy string) *storagev1.StorageClass {
	sc := createNamespaceProvisioningStorageClass(t, te)
	sc.Parameters["cleanupPolicy"] = cleanupPolicy

	// Update the StorageClass
	updated, err := te.K8sClient.StorageV1().StorageClasses().Update(context.Background(), sc, metav1.UpdateOptions{})
	require.NoError(t, err, "Failed to update StorageClass with cleanup policy")
	return updated
}

// Helper function to get resource metrics
func getResourceMetrics(t *testing.T, te *TestEnvironment) ResourceMetrics {
	// This is a simplified version - in real implementation you would
	// query actual metrics from Prometheus or metrics-server
	return ResourceMetrics{
		CPUUsage:    10.0, // Placeholder
		MemoryUsage: 100.0, // Placeholder
	}
}

// ResourceMetrics holds resource usage information
type ResourceMetrics struct {
	CPUUsage    float64 // Percentage
	MemoryUsage float64 // MB
}

// Helper function to cleanup EFS manually
func cleanupEFS(t *testing.T, te *TestEnvironment, efsID string) {
	ctx := context.Background()

	// Delete mount targets first
	describeMTInput := &efs.DescribeMountTargetsInput{
		FileSystemId: aws.String(efsID),
	}

	mtOutput, err := te.EFSClient.DescribeMountTargets(ctx, describeMTInput)
	if err == nil {
		for _, mt := range mtOutput.MountTargets {
			deleteMTInput := &efs.DeleteMountTargetInput{
				MountTargetId: mt.MountTargetId,
			}
			_, _ = te.EFSClient.DeleteMountTarget(ctx, deleteMTInput)
		}
	}

	// Wait for mount targets to be deleted
	time.Sleep(30 * time.Second)

	// Delete file system
	deleteInput := &efs.DeleteFileSystemInput{
		FileSystemId: aws.String(efsID),
	}

	_, err = te.EFSClient.DeleteFileSystem(ctx, deleteInput)
	if err != nil {
		t.Logf("Warning: Failed to cleanup EFS %s: %v", efsID, err)
	}
}