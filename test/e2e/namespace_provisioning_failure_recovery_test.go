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
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/driver"
	"github.com/kubernetes-sigs/aws-efs-csi-driver/test/e2e/testenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/klog/v2"
)

// TestFailureRecoveryScenarios tests various failure scenarios and recovery mechanisms
func TestFailureRecoveryScenarios(t *testing.T) {
	t.Run("API Timeout Simulations", func(t *testing.T) {
		t.Run("EFS Creation Timeout Recovery", testEFSCreationTimeoutRecovery)
		t.Run("Mount Target Creation Timeout Recovery", testMountTargetTimeoutRecovery)
		t.Run("Access Point Creation Timeout Recovery", testAccessPointTimeoutRecovery)
		t.Run("Concurrent API Timeouts", testConcurrentAPITimeouts)
		t.Run("Cascading API Failures", testCascadingAPIFailures)
	})

	t.Run("Partial Failure Recovery", func(t *testing.T) {
		t.Run("Partial Mount Target Creation", testPartialMountTargetCreation)
		t.Run("Incomplete EFS Provisioning", testIncompleteEFSProvisioning)
		t.Run("Mixed Success and Failure", testMixedSuccessAndFailure)
		t.Run("Resource Cleanup After Failure", testResourceCleanupAfterFailure)
		t.Run("Rollback on Critical Failure", testRollbackOnCriticalFailure)
	})

	t.Run("CRD Loss Recovery", func(t *testing.T) {
		t.Run("CRD Deletion During Operation", testCRDDeletionDuringOperation)
		t.Run("CRD Recreation from AWS Tags", testCRDRecreationFromTags)
		t.Run("Multiple CRD Loss and Recovery", testMultipleCRDLossAndRecovery)
		t.Run("CRD Corruption Recovery", testCRDCorruptionRecovery)
		t.Run("CRD Sync with AWS State", testCRDSyncWithAWSState)
	})
}

// testEFSCreationTimeoutRecovery tests recovery from EFS creation timeout
func testEFSCreationTimeoutRecovery(t *testing.T) {
	ctx := context.Background()
	testID := fmt.Sprintf("timeout-recovery-%d", time.Now().Unix())

	// Setup test environment with timeout simulation
	env, err := setupTimeoutSimulationEnv(t, testID)
	require.NoError(t, err, "Failed to setup test environment")
	defer env.Cleanup(ctx)

	helper := testenv.NewNamespaceProvisioningTestHelper(env)

	// Configure timeout behavior
	timeoutConfig := &TimeoutSimulationConfig{
		OperationType:     "CreateFileSystem",
		TimeoutAfter:      3 * time.Second,
		RecoverAfter:      10 * time.Second,
		MaxRetries:        5,
	}

	// Apply timeout simulation
	env.ApplyTimeoutSimulation(timeoutConfig)

	// Test namespace with retry logic
	namespace := "test-timeout-recovery"

	// Track retry attempts
	var retryCount atomic.Int32
	env.OnRetry(func(op string, attempt int) {
		retryCount.Add(1)
		klog.Infof("Retry attempt %d for operation %s", attempt, op)
	})

	// Create namespace with EFS (should timeout and retry)
	start := time.Now()
	ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	elapsed := time.Since(start)

	// Verify successful recovery
	assert.NoError(t, err, "Should recover from timeout")
	assert.NotNil(t, ns, "Namespace should be created")
	assert.NotEmpty(t, ns.FileSystemID, "EFS should be created after recovery")

	// Verify retry mechanism worked
	assert.GreaterOrEqual(t, int(retryCount.Load()), 1, "Should have retried at least once")
	assert.Less(t, elapsed, 30*time.Second, "Should complete within reasonable time")

	// Verify EFS state
	efsState, err := env.GetEFSState(ctx, ns.FileSystemID)
	assert.NoError(t, err, "Should get EFS state")
	assert.Equal(t, efstypes.LifeCycleStateAvailable, efsState.LifeCycleState, "EFS should be available")

	// Verify idempotency - second attempt should succeed immediately
	ns2, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	assert.NoError(t, err, "Second attempt should succeed")
	assert.Equal(t, ns.FileSystemID, ns2.FileSystemID, "Should reuse same EFS")
}

// testMountTargetTimeoutRecovery tests recovery from mount target creation timeout
func testMountTargetTimeoutRecovery(t *testing.T) {
	ctx := context.Background()
	testID := fmt.Sprintf("mt-timeout-%d", time.Now().Unix())

	env, err := setupTimeoutSimulationEnv(t, testID)
	require.NoError(t, err, "Failed to setup test environment")
	defer env.Cleanup(ctx)

	helper := testenv.NewNamespaceProvisioningTestHelper(env)

	// Configure mount target timeout
	timeoutConfig := &TimeoutSimulationConfig{
		OperationType:     "CreateMountTarget",
		TimeoutAfter:      2 * time.Second,
		RecoverAfter:      8 * time.Second,
		MaxRetries:        3,
		PartialSuccess:    true, // Allow some mount targets to succeed
	}

	env.ApplyTimeoutSimulation(timeoutConfig)

	namespace := "test-mt-timeout"

	// Track mount target creation attempts
	var mtAttempts sync.Map
	env.OnMountTargetCreate(func(fsID, subnetID string) {
		count, _ := mtAttempts.LoadOrStore(subnetID, &atomic.Int32{})
		count.(*atomic.Int32).Add(1)
	})

	// Create namespace with EFS
	ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	assert.NoError(t, err, "Should recover from mount target timeout")
	assert.NotNil(t, ns, "Namespace should be created")

	// Verify mount targets were created with retry
	mountTargets, err := env.GetMountTargets(ctx, ns.FileSystemID)
	assert.NoError(t, err, "Should get mount targets")
	assert.GreaterOrEqual(t, len(mountTargets), 2, "Should have multiple mount targets")

	// Verify retry attempts for failed mount targets
	mtAttempts.Range(func(key, value interface{}) bool {
		attempts := value.(*atomic.Int32).Load()
		t.Logf("Mount target for subnet %s: %d attempts", key, attempts)
		return true
	})

	// Test resilience to partial mount target failure
	pvc, err := helper.CreatePVCWithAccessPoint(ctx, namespace, "test-pvc", "5Gi")
	assert.NoError(t, err, "Should create PVC despite partial MT issues")
	assert.NotEmpty(t, pvc.AccessPointID, "Access point should be created")
}

// testAccessPointTimeoutRecovery tests recovery from access point creation timeout
func testAccessPointTimeoutRecovery(t *testing.T) {
	ctx := context.Background()
	testID := fmt.Sprintf("ap-timeout-%d", time.Now().Unix())

	env, err := setupTimeoutSimulationEnv(t, testID)
	require.NoError(t, err, "Failed to setup test environment")
	defer env.Cleanup(ctx)

	helper := testenv.NewNamespaceProvisioningTestHelper(env)
	namespace := "test-ap-timeout"

	// First create namespace with EFS successfully
	ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	require.NoError(t, err, "Should create namespace")

	// Configure access point timeout
	timeoutConfig := &TimeoutSimulationConfig{
		OperationType:     "CreateAccessPoint",
		TimeoutAfter:      1 * time.Second,
		RecoverAfter:      5 * time.Second,
		MaxRetries:        4,
		FailureRate:       0.5, // 50% failure rate
	}

	env.ApplyTimeoutSimulation(timeoutConfig)

	// Create multiple PVCs concurrently with timeout/retry
	numPVCs := 5
	var wg sync.WaitGroup
	results := make(chan error, numPVCs)

	for i := 0; i < numPVCs; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			pvcName := fmt.Sprintf("pvc-%d", index)
			pvc, err := helper.CreatePVCWithAccessPoint(ctx, namespace, pvcName, "1Gi")
			if err != nil {
				results <- fmt.Errorf("PVC %s failed: %w", pvcName, err)
			} else if pvc.AccessPointID == "" {
				results <- fmt.Errorf("PVC %s has no access point", pvcName)
			} else {
				results <- nil
			}
		}(i)
	}

	wg.Wait()
	close(results)

	// Verify recovery success
	var failures []error
	for err := range results {
		if err != nil {
			failures = append(failures, err)
		}
	}

	assert.Empty(t, failures, "All PVCs should eventually succeed")

	// Verify access points were created
	accessPoints, err := env.GetAccessPoints(ctx, ns.FileSystemID)
	assert.NoError(t, err, "Should get access points")
	assert.Equal(t, numPVCs, len(accessPoints), "All access points should be created")
}

// testConcurrentAPITimeouts tests handling of concurrent API timeout scenarios
func testConcurrentAPITimeouts(t *testing.T) {
	ctx := context.Background()
	testID := fmt.Sprintf("concurrent-timeout-%d", time.Now().Unix())

	env, err := setupTimeoutSimulationEnv(t, testID)
	require.NoError(t, err, "Failed to setup test environment")
	defer env.Cleanup(ctx)

	helper := testenv.NewNamespaceProvisioningTestHelper(env)

	// Configure multiple timeout simulations
	timeoutConfigs := []*TimeoutSimulationConfig{
		{
			OperationType: "CreateFileSystem",
			TimeoutAfter:  2 * time.Second,
			RecoverAfter:  6 * time.Second,
			MaxRetries:    3,
		},
		{
			OperationType: "CreateMountTarget",
			TimeoutAfter:  1 * time.Second,
			RecoverAfter:  4 * time.Second,
			MaxRetries:    4,
		},
		{
			OperationType: "CreateAccessPoint",
			TimeoutAfter:  1500 * time.Millisecond,
			RecoverAfter:  5 * time.Second,
			MaxRetries:    3,
		},
	}

	for _, config := range timeoutConfigs {
		env.ApplyTimeoutSimulation(config)
	}

	// Create multiple namespaces concurrently
	numNamespaces := 3
	var wg sync.WaitGroup
	errors := make(chan error, numNamespaces*3) // namespace + 2 PVCs each

	for i := 0; i < numNamespaces; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			namespace := fmt.Sprintf("ns-concurrent-%d", index)

			// Create namespace
			ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)
			if err != nil {
				errors <- fmt.Errorf("namespace %s creation failed: %w", namespace, err)
				return
			}

			// Create 2 PVCs per namespace
			for j := 0; j < 2; j++ {
				pvcName := fmt.Sprintf("pvc-%d", j)
				pvc, err := helper.CreatePVCWithAccessPoint(ctx, namespace, pvcName, "2Gi")
				if err != nil {
					errors <- fmt.Errorf("PVC %s/%s failed: %w", namespace, pvcName, err)
				} else if pvc.AccessPointID == "" {
					errors <- fmt.Errorf("PVC %s/%s has no AP", namespace, pvcName)
				}
			}

			klog.Infof("Successfully provisioned namespace %s with PVCs", namespace)
		}(i)
	}

	wg.Wait()
	close(errors)

	// Collect and verify results
	var allErrors []error
	for err := range errors {
		if err != nil {
			allErrors = append(allErrors, err)
		}
	}

	assert.Empty(t, allErrors, "All operations should eventually succeed despite concurrent timeouts")
}

// testCascadingAPIFailures tests recovery from cascading API failures
func testCascadingAPIFailures(t *testing.T) {
	ctx := context.Background()
	testID := fmt.Sprintf("cascading-fail-%d", time.Now().Unix())

	env, err := setupTimeoutSimulationEnv(t, testID)
	require.NoError(t, err, "Failed to setup test environment")
	defer env.Cleanup(ctx)

	helper := testenv.NewNamespaceProvisioningTestHelper(env)

	// Configure cascading failure scenario
	failureChain := &CascadingFailureConfig{
		InitialFailure: "CreateFileSystem",
		PropagateToOps: []string{"CreateMountTarget", "CreateAccessPoint"},
		FailureDuration: 5 * time.Second,
		RecoveryOrder:   []string{"CreateFileSystem", "CreateMountTarget", "CreateAccessPoint"},
		RecoveryDelay:   2 * time.Second,
	}

	env.ApplyCascadingFailure(failureChain)

	namespace := "test-cascading"

	// Track failure and recovery events
	var failureEvents []string
	var recoveryEvents []string
	var mu sync.Mutex

	env.OnFailure(func(op string, err error) {
		mu.Lock()
		failureEvents = append(failureEvents, fmt.Sprintf("%s: %v", op, err))
		mu.Unlock()
	})

	env.OnRecovery(func(op string) {
		mu.Lock()
		recoveryEvents = append(recoveryEvents, op)
		mu.Unlock()
	})

	// Attempt provisioning during cascading failure
	start := time.Now()
	ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	elapsed := time.Since(start)

	// Should eventually succeed after recovery
	assert.NoError(t, err, "Should recover from cascading failures")
	assert.NotNil(t, ns, "Namespace should be created")
	assert.NotEmpty(t, ns.FileSystemID, "EFS should be created")

	// Verify failure cascade occurred
	assert.GreaterOrEqual(t, len(failureEvents), 3, "Should have multiple failures")

	// Verify recovery occurred in order
	assert.GreaterOrEqual(t, len(recoveryEvents), 3, "Should have recovery events")

	// Verify total time includes failure and recovery periods
	assert.Greater(t, elapsed, failureChain.FailureDuration, "Should take longer than failure duration")

	t.Logf("Cascading failure recovery completed in %v", elapsed)
	t.Logf("Failure events: %v", failureEvents)
	t.Logf("Recovery events: %v", recoveryEvents)
}

// testPartialMountTargetCreation tests recovery from partial mount target creation
func testPartialMountTargetCreation(t *testing.T) {
	ctx := context.Background()
	testID := fmt.Sprintf("partial-mt-%d", time.Now().Unix())

	env, err := setupPartialFailureEnv(t, testID)
	require.NoError(t, err, "Failed to setup test environment")
	defer env.Cleanup(ctx)

	helper := testenv.NewNamespaceProvisioningTestHelper(env)

	// Configure partial mount target failure (fail 1 out of 3 AZs)
	partialConfig := &PartialFailureConfig{
		OperationType:    "CreateMountTarget",
		FailurePattern:   "subnet-2", // Fail for second subnet
		Retryable:        true,
		MaxRetries:       3,
		RecoveryDelay:    2 * time.Second,
	}

	env.ApplyPartialFailure(partialConfig)

	namespace := "test-partial-mt"

	// Track mount target states
	mtStates := make(map[string]string)
	var mtMu sync.Mutex

	env.OnMountTargetStateChange(func(mtID, state string) {
		mtMu.Lock()
		mtStates[mtID] = state
		mtMu.Unlock()
	})

	// Create namespace with EFS
	ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	assert.NoError(t, err, "Should succeed despite partial MT failure")
	assert.NotNil(t, ns, "Namespace should be created")

	// Verify mount targets
	mountTargets, err := env.GetMountTargets(ctx, ns.FileSystemID)
	assert.NoError(t, err, "Should get mount targets")

	// Should have mount targets in all AZs after retry
	assert.GreaterOrEqual(t, len(mountTargets), 3, "Should have mount targets in all AZs")

	// Verify all mount targets are available
	for _, mt := range mountTargets {
		assert.Equal(t, "available", mtStates[mt.ID], "Mount target %s should be available", mt.ID)
	}

	// Test PVC creation works with recovered mount targets
	pvc, err := helper.CreatePVCWithAccessPoint(ctx, namespace, "test-pvc", "10Gi")
	assert.NoError(t, err, "PVC creation should work")
	assert.NotEmpty(t, pvc.AccessPointID, "Access point should be created")
}

// testIncompleteEFSProvisioning tests recovery from incomplete EFS provisioning
func testIncompleteEFSProvisioning(t *testing.T) {
	ctx := context.Background()
	testID := fmt.Sprintf("incomplete-efs-%d", time.Now().Unix())

	env, err := setupPartialFailureEnv(t, testID)
	require.NoError(t, err, "Failed to setup test environment")
	defer env.Cleanup(ctx)

	helper := testenv.NewNamespaceProvisioningTestHelper(env)

	// Configure incomplete provisioning scenario
	incompleteConfig := &IncompleteProvisioningConfig{
		StopAfterEFSCreation:    false,
		StopAfterMountTarget:    true,
		SimulateOrphanResources: true,
		RecoveryDelay:           3 * time.Second,
	}

	env.ApplyIncompleteProvisioning(incompleteConfig)

	namespace := "test-incomplete"

	// Track provisioning stages
	stages := []string{}
	var stagesMu sync.Mutex

	env.OnProvisioningStage(func(stage string) {
		stagesMu.Lock()
		stages = append(stages, stage)
		stagesMu.Unlock()
		klog.Infof("Provisioning stage: %s", stage)
	})

	// Attempt provisioning with incomplete scenario
	ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)

	// Should recover and complete provisioning
	assert.NoError(t, err, "Should recover from incomplete provisioning")
	assert.NotNil(t, ns, "Namespace should be created")
	assert.NotEmpty(t, ns.FileSystemID, "EFS should be created")

	// Verify all stages completed
	assert.Contains(t, stages, "efs_created", "EFS creation stage")
	assert.Contains(t, stages, "mount_targets_created", "Mount targets stage")
	assert.Contains(t, stages, "provisioning_complete", "Completion stage")

	// Verify no orphan resources
	orphans, err := env.DetectOrphanResources(ctx, namespace)
	assert.NoError(t, err, "Should detect orphans")
	assert.Empty(t, orphans, "No orphan resources should remain")
}

// testMixedSuccessAndFailure tests handling of mixed success/failure scenarios
func testMixedSuccessAndFailure(t *testing.T) {
	ctx := context.Background()
	testID := fmt.Sprintf("mixed-result-%d", time.Now().Unix())

	env, err := setupPartialFailureEnv(t, testID)
	require.NoError(t, err, "Failed to setup test environment")
	defer env.Cleanup(ctx)

	helper := testenv.NewNamespaceProvisioningTestHelper(env)

	// Configure mixed success/failure pattern
	mixedConfig := &MixedResultConfig{
		SuccessRate: 0.7, // 70% success rate
		Operations: map[string]float64{
			"CreateFileSystem":  1.0,  // Always succeed
			"CreateMountTarget": 0.66, // 2 out of 3 succeed
			"CreateAccessPoint": 0.8,  // 80% success
		},
		RetryFailures: true,
		MaxRetries:    5,
	}

	env.ApplyMixedResults(mixedConfig)

	// Create multiple namespaces with PVCs
	numNamespaces := 3
	results := make(map[string]*ProvisioningResult)
	var resultsMu sync.Mutex

	var wg sync.WaitGroup
	for i := 0; i < numNamespaces; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			namespace := fmt.Sprintf("ns-mixed-%d", index)

			result := &ProvisioningResult{
				Namespace: namespace,
				Attempts:  0,
				Success:   false,
			}

			// Try namespace creation
			ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)
			if err == nil && ns != nil {
				result.Success = true
				result.FileSystemID = ns.FileSystemID

				// Try creating PVCs
				for j := 0; j < 3; j++ {
					pvcName := fmt.Sprintf("pvc-%d", j)
					pvc, err := helper.CreatePVCWithAccessPoint(ctx, namespace, pvcName, "1Gi")
					if err == nil && pvc.AccessPointID != "" {
						result.PVCsCreated++
					} else {
						result.PVCsFailed++
					}
				}
			}

			resultsMu.Lock()
			results[namespace] = result
			resultsMu.Unlock()
		}(i)
	}

	wg.Wait()

	// Verify all namespaces eventually succeeded
	for ns, result := range results {
		assert.True(t, result.Success, "Namespace %s should succeed", ns)
		assert.NotEmpty(t, result.FileSystemID, "Namespace %s should have EFS", ns)
		assert.Equal(t, 3, result.PVCsCreated, "All PVCs should be created for %s", ns)
		assert.Equal(t, 0, result.PVCsFailed, "No PVCs should fail for %s", ns)
	}
}

// testResourceCleanupAfterFailure tests resource cleanup after provisioning failure
func testResourceCleanupAfterFailure(t *testing.T) {
	ctx := context.Background()
	testID := fmt.Sprintf("cleanup-fail-%d", time.Now().Unix())

	env, err := setupPartialFailureEnv(t, testID)
	require.NoError(t, err, "Failed to setup test environment")
	defer env.Cleanup(ctx)

	helper := testenv.NewNamespaceProvisioningTestHelper(env)

	// Configure failure with cleanup tracking
	cleanupConfig := &CleanupTestConfig{
		FailAfterStage:     "mount_targets",
		TrackCleanupCalls:  true,
		VerifyNoOrphans:    true,
		CleanupGracePeriod: 5 * time.Second,
	}

	env.ApplyCleanupTest(cleanupConfig)

	namespace := "test-cleanup"

	// Track cleanup operations
	var cleanupOps []string
	var cleanupMu sync.Mutex

	env.OnCleanup(func(resourceType, resourceID string) {
		cleanupMu.Lock()
		cleanupOps = append(cleanupOps, fmt.Sprintf("%s:%s", resourceType, resourceID))
		cleanupMu.Unlock()
	})

	// Attempt provisioning that will fail
	_, err = helper.CreateNamespaceWithEFS(ctx, namespace)

	// Should fail as configured
	assert.Error(t, err, "Provisioning should fail")

	// Wait for cleanup to complete
	time.Sleep(cleanupConfig.CleanupGracePeriod + 2*time.Second)

	// Verify cleanup operations occurred
	assert.NotEmpty(t, cleanupOps, "Cleanup operations should occur")

	// Verify no orphan resources
	orphans, err := env.DetectOrphanResources(ctx, namespace)
	assert.NoError(t, err, "Should detect orphans")
	assert.Empty(t, orphans, "No orphan resources should remain after cleanup")

	// Verify specific cleanup order (reverse of creation)
	if len(cleanupOps) > 0 {
		// Mount targets should be cleaned before EFS
		mtCleanupIndex := -1
		efsCleanupIndex := -1

		for i, op := range cleanupOps {
			if strings.HasPrefix(op, "mount_target:") {
				mtCleanupIndex = i
			}
			if strings.HasPrefix(op, "filesystem:") {
				efsCleanupIndex = i
			}
		}

		if mtCleanupIndex >= 0 && efsCleanupIndex >= 0 {
			assert.Less(t, mtCleanupIndex, efsCleanupIndex, "Mount targets should be cleaned before EFS")
		}
	}

	t.Logf("Cleanup operations performed: %v", cleanupOps)
}

// testRollbackOnCriticalFailure tests rollback mechanism on critical failures
func testRollbackOnCriticalFailure(t *testing.T) {
	ctx := context.Background()
	testID := fmt.Sprintf("rollback-%d", time.Now().Unix())

	env, err := setupPartialFailureEnv(t, testID)
	require.NoError(t, err, "Failed to setup test environment")
	defer env.Cleanup(ctx)

	helper := testenv.NewNamespaceProvisioningTestHelper(env)

	// Configure critical failure with rollback
	rollbackConfig := &RollbackConfig{
		TriggerOn:      "critical_error",
		RollbackStages: []string{"access_points", "mount_targets", "filesystem"},
		SaveState:      true,
		AtomicOps:      true,
	}

	env.ApplyRollbackConfig(rollbackConfig)

	namespace := "test-rollback"

	// Track rollback operations
	var rollbackOps []RollbackOperation
	var rollbackMu sync.Mutex

	env.OnRollback(func(op RollbackOperation) {
		rollbackMu.Lock()
		rollbackOps = append(rollbackOps, op)
		rollbackMu.Unlock()
		klog.Infof("Rollback: %s for resource %s", op.Stage, op.ResourceID)
	})

	// Trigger critical failure scenario
	env.InjectCriticalError("access_point_quota_exceeded")

	// Attempt provisioning
	_, err = helper.CreateNamespaceWithEFS(ctx, namespace)

	// Should fail with rollback
	assert.Error(t, err, "Should fail due to critical error")
	assert.Contains(t, err.Error(), "critical", "Error should indicate critical failure")

	// Verify rollback occurred
	assert.NotEmpty(t, rollbackOps, "Rollback should occur")

	// Verify rollback order (reverse order)
	for i := 0; i < len(rollbackOps)-1; i++ {
		currentStage := rollbackOps[i].Stage
		nextStage := rollbackOps[i+1].Stage

		currentIndex := indexOf(rollbackConfig.RollbackStages, currentStage)
		nextIndex := indexOf(rollbackConfig.RollbackStages, nextStage)

		assert.LessOrEqual(t, currentIndex, nextIndex, "Rollback should proceed in reverse order")
	}

	// Verify system state after rollback
	state, err := env.GetSystemState(ctx, namespace)
	assert.NoError(t, err, "Should get system state")
	assert.Empty(t, state.Resources, "No resources should remain after rollback")

	// Verify saved state if configured
	if rollbackConfig.SaveState {
		savedState, err := env.GetSavedState(ctx, namespace)
		assert.NoError(t, err, "Should have saved state")
		assert.NotNil(t, savedState, "Saved state should exist")
		assert.Contains(t, savedState.FailureReason, "quota_exceeded", "Should save failure reason")
	}
}

// testCRDDeletionDuringOperation tests handling of CRD deletion during operation
func testCRDDeletionDuringOperation(t *testing.T) {
	ctx := context.Background()
	testID := fmt.Sprintf("crd-delete-%d", time.Now().Unix())

	env, err := setupCRDTestEnv(t, testID)
	require.NoError(t, err, "Failed to setup test environment")
	defer env.Cleanup(ctx)

	helper := testenv.NewNamespaceProvisioningTestHelper(env)
	namespace := "test-crd-delete"

	// Create initial namespace with EFS
	ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	require.NoError(t, err, "Should create namespace")
	require.NotEmpty(t, ns.FileSystemID, "Should have EFS")

	// Get CRD for this namespace
	crd, err := env.GetNamespaceCRD(ctx, namespace)
	require.NoError(t, err, "Should get CRD")
	require.NotNil(t, crd, "CRD should exist")

	// Start PVC creation in background
	pvcComplete := make(chan error, 1)
	go func() {
		// Add delay to ensure CRD deletion happens during operation
		time.Sleep(1 * time.Second)
		_, err := helper.CreatePVCWithAccessPoint(ctx, namespace, "test-pvc", "5Gi")
		pvcComplete <- err
	}()

	// Delete CRD while PVC creation is in progress
	time.Sleep(500 * time.Millisecond)
	err = env.DeleteNamespaceCRD(ctx, namespace)
	assert.NoError(t, err, "Should delete CRD")

	// Wait for PVC creation to complete
	pvcErr := <-pvcComplete

	// PVC creation should still succeed (using AWS tags fallback)
	assert.NoError(t, pvcErr, "PVC creation should succeed despite CRD deletion")

	// Verify CRD was recreated
	newCRD, err := env.GetNamespaceCRD(ctx, namespace)
	assert.NoError(t, err, "Should get recreated CRD")
	assert.NotNil(t, newCRD, "CRD should be recreated")
	assert.Equal(t, ns.FileSystemID, newCRD.FileSystemID, "Recreated CRD should have correct EFS ID")

	// Verify namespace mapping is intact
	mapping, err := env.GetNamespaceMapping(ctx, namespace)
	assert.NoError(t, err, "Should get namespace mapping")
	assert.Equal(t, ns.FileSystemID, mapping.FileSystemID, "Mapping should be correct")
}

// testCRDRecreationFromTags tests CRD recreation from AWS tags
func testCRDRecreationFromTags(t *testing.T) {
	ctx := context.Background()
	testID := fmt.Sprintf("crd-recreate-%d", time.Now().Unix())

	env, err := setupCRDTestEnv(t, testID)
	require.NoError(t, err, "Failed to setup test environment")
	defer env.Cleanup(ctx)

	helper := testenv.NewNamespaceProvisioningTestHelper(env)

	// Create multiple namespaces with EFS
	namespaces := []string{"ns-recreate-1", "ns-recreate-2", "ns-recreate-3"}
	fsIDs := make(map[string]string)

	for _, ns := range namespaces {
		nsObj, err := helper.CreateNamespaceWithEFS(ctx, ns)
		require.NoError(t, err, "Should create namespace %s", ns)
		fsIDs[ns] = nsObj.FileSystemID
	}

	// Delete all CRDs
	for _, ns := range namespaces {
		err := env.DeleteNamespaceCRD(ctx, ns)
		assert.NoError(t, err, "Should delete CRD for %s", ns)
	}

	// Verify CRDs are deleted
	for _, ns := range namespaces {
		crd, err := env.GetNamespaceCRD(ctx, ns)
		assert.Error(t, err, "CRD should not exist for %s", ns)
		assert.Nil(t, crd, "CRD should be nil for %s", ns)
	}

	// Trigger CRD recreation from AWS tags
	err = env.ReconcileCRDsFromAWSTags(ctx)
	assert.NoError(t, err, "Should reconcile CRDs from tags")

	// Verify CRDs are recreated with correct mappings
	for _, ns := range namespaces {
		crd, err := env.GetNamespaceCRD(ctx, ns)
		assert.NoError(t, err, "Should get recreated CRD for %s", ns)
		assert.NotNil(t, crd, "CRD should exist for %s", ns)
		assert.Equal(t, fsIDs[ns], crd.FileSystemID, "CRD should have correct EFS ID for %s", ns)
	}

	// Verify PVC creation works with recreated CRDs
	for _, ns := range namespaces {
		pvc, err := helper.CreatePVCWithAccessPoint(ctx, ns, "test-pvc", "1Gi")
		assert.NoError(t, err, "PVC creation should work for %s", ns)
		assert.NotEmpty(t, pvc.AccessPointID, "Access point should be created for %s", ns)
	}
}

// testMultipleCRDLossAndRecovery tests multiple CRD loss and recovery scenarios
func testMultipleCRDLossAndRecovery(t *testing.T) {
	ctx := context.Background()
	testID := fmt.Sprintf("multi-crd-loss-%d", time.Now().Unix())

	env, err := setupCRDTestEnv(t, testID)
	require.NoError(t, err, "Failed to setup test environment")
	defer env.Cleanup(ctx)

	helper := testenv.NewNamespaceProvisioningTestHelper(env)

	// Create namespaces
	numNamespaces := 5
	namespaces := make([]string, numNamespaces)
	fsIDs := make(map[string]string)

	for i := 0; i < numNamespaces; i++ {
		ns := fmt.Sprintf("ns-multi-%d", i)
		namespaces[i] = ns

		nsObj, err := helper.CreateNamespaceWithEFS(ctx, ns)
		require.NoError(t, err, "Should create namespace %s", ns)
		fsIDs[ns] = nsObj.FileSystemID
	}

	// Simulate random CRD losses
	lostCRDs := []string{namespaces[0], namespaces[2], namespaces[4]}
	for _, ns := range lostCRDs {
		err := env.DeleteNamespaceCRD(ctx, ns)
		assert.NoError(t, err, "Should delete CRD for %s", ns)
	}

	// Concurrent operations on all namespaces
	var wg sync.WaitGroup
	results := make(chan error, numNamespaces)

	for _, ns := range namespaces {
		wg.Add(1)
		go func(namespace string) {
			defer wg.Done()

			// Try to create PVC
			pvc, err := helper.CreatePVCWithAccessPoint(ctx, namespace, "test-pvc", "2Gi")
			if err != nil {
				results <- fmt.Errorf("namespace %s: %w", namespace, err)
			} else if pvc.AccessPointID == "" {
				results <- fmt.Errorf("namespace %s: no access point", namespace)
			} else {
				results <- nil
			}
		}(ns)
	}

	wg.Wait()
	close(results)

	// All operations should succeed
	for err := range results {
		assert.NoError(t, err, "All PVC operations should succeed")
	}

	// Verify all CRDs are present (recreated as needed)
	for _, ns := range namespaces {
		crd, err := env.GetNamespaceCRD(ctx, ns)
		assert.NoError(t, err, "Should get CRD for %s", ns)
		assert.NotNil(t, crd, "CRD should exist for %s", ns)
		assert.Equal(t, fsIDs[ns], crd.FileSystemID, "CRD should have correct mapping for %s", ns)
	}
}

// testCRDCorruptionRecovery tests recovery from CRD data corruption
func testCRDCorruptionRecovery(t *testing.T) {
	ctx := context.Background()
	testID := fmt.Sprintf("crd-corrupt-%d", time.Now().Unix())

	env, err := setupCRDTestEnv(t, testID)
	require.NoError(t, err, "Failed to setup test environment")
	defer env.Cleanup(ctx)

	helper := testenv.NewNamespaceProvisioningTestHelper(env)
	namespace := "test-crd-corrupt"

	// Create namespace with EFS
	ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	require.NoError(t, err, "Should create namespace")
	originalFSID := ns.FileSystemID

	// Corrupt the CRD data
	corruptedData := &CorruptedCRDData{
		FileSystemID: "fs-corrupted-12345",
		AccessPoints: []string{"ap-invalid-1", "ap-invalid-2"},
		InvalidField: "should-not-exist",
	}

	err = env.CorruptNamespaceCRD(ctx, namespace, corruptedData)
	assert.NoError(t, err, "Should corrupt CRD")

	// Verify CRD is corrupted
	crd, err := env.GetNamespaceCRD(ctx, namespace)
	assert.NoError(t, err, "Should get corrupted CRD")
	assert.NotEqual(t, originalFSID, crd.FileSystemID, "CRD should have corrupted data")

	// Attempt to create PVC (should trigger recovery)
	pvc, err := helper.CreatePVCWithAccessPoint(ctx, namespace, "test-pvc", "3Gi")

	// Should succeed after recovering from corruption
	assert.NoError(t, err, "Should recover from CRD corruption")
	assert.NotEmpty(t, pvc.AccessPointID, "Access point should be created")

	// Verify CRD is fixed
	fixedCRD, err := env.GetNamespaceCRD(ctx, namespace)
	assert.NoError(t, err, "Should get fixed CRD")
	assert.Equal(t, originalFSID, fixedCRD.FileSystemID, "CRD should have correct EFS ID")

	// Verify recovery audit log
	auditLog, err := env.GetRecoveryAuditLog(ctx, namespace)
	assert.NoError(t, err, "Should get audit log")
	assert.Contains(t, auditLog, "corruption_detected", "Audit should log corruption")
	assert.Contains(t, auditLog, "recovery_completed", "Audit should log recovery")
}

// testCRDSyncWithAWSState tests CRD synchronization with AWS state
func testCRDSyncWithAWSState(t *testing.T) {
	ctx := context.Background()
	testID := fmt.Sprintf("crd-sync-%d", time.Now().Unix())

	env, err := setupCRDTestEnv(t, testID)
	require.NoError(t, err, "Failed to setup test environment")
	defer env.Cleanup(ctx)

	helper := testenv.NewNamespaceProvisioningTestHelper(env)

	// Create namespace with EFS and PVCs
	namespace := "test-crd-sync"
	ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	require.NoError(t, err, "Should create namespace")

	// Create multiple PVCs
	pvcNames := []string{"pvc-1", "pvc-2", "pvc-3"}
	apIDs := make(map[string]string)

	for _, pvcName := range pvcNames {
		pvc, err := helper.CreatePVCWithAccessPoint(ctx, namespace, pvcName, "1Gi")
		require.NoError(t, err, "Should create PVC %s", pvcName)
		apIDs[pvcName] = pvc.AccessPointID
	}

	// Manually delete an access point from AWS (simulate out-of-band deletion)
	err = env.DeleteAccessPointDirect(ctx, apIDs["pvc-2"])
	assert.NoError(t, err, "Should delete access point directly")

	// CRD still has stale data
	crd, err := env.GetNamespaceCRD(ctx, namespace)
	assert.NoError(t, err, "Should get CRD")
	assert.Contains(t, crd.AccessPoints, apIDs["pvc-2"], "CRD still has deleted AP")

	// Trigger sync with AWS state
	err = env.SyncCRDWithAWS(ctx, namespace)
	assert.NoError(t, err, "Should sync CRD with AWS")

	// Verify CRD is updated
	syncedCRD, err := env.GetNamespaceCRD(ctx, namespace)
	assert.NoError(t, err, "Should get synced CRD")
	assert.NotContains(t, syncedCRD.AccessPoints, apIDs["pvc-2"], "CRD should not have deleted AP")
	assert.Contains(t, syncedCRD.AccessPoints, apIDs["pvc-1"], "CRD should have valid AP")
	assert.Contains(t, syncedCRD.AccessPoints, apIDs["pvc-3"], "CRD should have valid AP")

	// Verify periodic sync catches discrepancies
	// Add access point directly to AWS (simulate out-of-band creation)
	directAP, err := env.CreateAccessPointDirect(ctx, ns.FileSystemID, "direct-ap")
	assert.NoError(t, err, "Should create AP directly")

	// Enable periodic sync
	stopSync := env.StartPeriodicSync(ctx, 2*time.Second)
	defer stopSync()

	// Wait for sync to run
	time.Sleep(3 * time.Second)

	// Verify CRD includes the direct AP
	finalCRD, err := env.GetNamespaceCRD(ctx, namespace)
	assert.NoError(t, err, "Should get final CRD")
	assert.Contains(t, finalCRD.AccessPoints, directAP.ID, "CRD should include direct AP after sync")

	// Verify sync metrics
	metrics, err := env.GetSyncMetrics(ctx, namespace)
	assert.NoError(t, err, "Should get sync metrics")
	assert.Greater(t, metrics.SyncCount, 0, "Should have sync count")
	assert.Greater(t, metrics.DiscrepanciesFound, 0, "Should find discrepancies")
	assert.Greater(t, metrics.DiscrepanciesFixed, 0, "Should fix discrepancies")
}

// Helper function implementations

func setupTimeoutSimulationEnv(t *testing.T, testID string) (*TimeoutSimulationEnvironment, error) {
	env, err := testenv.NewAWSTestEnvironment(testID)
	if err != nil {
		return nil, err
	}

	// Wrap with timeout simulation capabilities
	return &TimeoutSimulationEnvironment{
		AWSTestEnvironment: env,
		timeoutConfigs:     make(map[string]*TimeoutSimulationConfig),
		operationCallbacks: make(map[string][]func()),
	}, nil
}

func setupPartialFailureEnv(t *testing.T, testID string) (*PartialFailureEnvironment, error) {
	env, err := testenv.NewAWSTestEnvironment(testID)
	if err != nil {
		return nil, err
	}

	// Wrap with partial failure capabilities
	return &PartialFailureEnvironment{
		AWSTestEnvironment: env,
		failureConfigs:     make(map[string]*PartialFailureConfig),
		stateTracking:      make(map[string]interface{}),
	}, nil
}

func setupCRDTestEnv(t *testing.T, testID string) (*CRDTestEnvironment, error) {
	env, err := testenv.NewAWSTestEnvironment(testID)
	if err != nil {
		return nil, err
	}

	// Create fake Kubernetes client
	k8sClient := fake.NewSimpleClientset()

	// Wrap with CRD test capabilities
	return &CRDTestEnvironment{
		AWSTestEnvironment: env,
		k8sClient:          k8sClient,
		crdCache:           make(map[string]*CRDData),
		syncMetrics:        make(map[string]*SyncMetrics),
	}, nil
}

func indexOf(slice []string, item string) int {
	for i, v := range slice {
		if v == item {
			return i
		}
	}
	return -1
}

// Test environment wrapper types

type TimeoutSimulationEnvironment struct {
	*testenv.AWSTestEnvironment
	timeoutConfigs     map[string]*TimeoutSimulationConfig
	operationCallbacks map[string][]func()
	mu                 sync.Mutex
}

type TimeoutSimulationConfig struct {
	OperationType  string
	TimeoutAfter   time.Duration
	RecoverAfter   time.Duration
	MaxRetries     int
	PartialSuccess bool
	FailureRate    float64
}

type CascadingFailureConfig struct {
	InitialFailure  string
	PropagateToOps  []string
	FailureDuration time.Duration
	RecoveryOrder   []string
	RecoveryDelay   time.Duration
}

type PartialFailureEnvironment struct {
	*testenv.AWSTestEnvironment
	failureConfigs map[string]*PartialFailureConfig
	stateTracking  map[string]interface{}
	mu             sync.Mutex
}

type PartialFailureConfig struct {
	OperationType  string
	FailurePattern string
	Retryable      bool
	MaxRetries     int
	RecoveryDelay  time.Duration
}

type IncompleteProvisioningConfig struct {
	StopAfterEFSCreation    bool
	StopAfterMountTarget    bool
	SimulateOrphanResources bool
	RecoveryDelay           time.Duration
}

type MixedResultConfig struct {
	SuccessRate   float64
	Operations    map[string]float64
	RetryFailures bool
	MaxRetries    int
}

type CleanupTestConfig struct {
	FailAfterStage     string
	TrackCleanupCalls  bool
	VerifyNoOrphans    bool
	CleanupGracePeriod time.Duration
}

type RollbackConfig struct {
	TriggerOn      string
	RollbackStages []string
	SaveState      bool
	AtomicOps      bool
}

type RollbackOperation struct {
	Stage      string
	ResourceID string
	Timestamp  time.Time
	Success    bool
}

type ProvisioningResult struct {
	Namespace    string
	FileSystemID string
	Attempts     int
	Success      bool
	PVCsCreated  int
	PVCsFailed   int
}

type CRDTestEnvironment struct {
	*testenv.AWSTestEnvironment
	k8sClient   kubernetes.Interface
	crdCache    map[string]*CRDData
	syncMetrics map[string]*SyncMetrics
	mu          sync.Mutex
}

type CRDData struct {
	FileSystemID string
	AccessPoints []string
	UpdatedAt    time.Time
}

type CorruptedCRDData struct {
	FileSystemID string
	AccessPoints []string
	InvalidField string
}

type SyncMetrics struct {
	SyncCount          int
	DiscrepanciesFound int
	DiscrepanciesFixed int
	LastSyncTime       time.Time
}

// Implement required methods for test environments
// Note: These are simplified implementations for testing

func (e *TimeoutSimulationEnvironment) ApplyTimeoutSimulation(config *TimeoutSimulationConfig) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.timeoutConfigs[config.OperationType] = config
}

func (e *TimeoutSimulationEnvironment) ApplyCascadingFailure(config *CascadingFailureConfig) {
	// Implementation for cascading failure simulation
}

func (e *TimeoutSimulationEnvironment) OnRetry(callback func(string, int)) {
	// Implementation for retry callback
}

func (e *TimeoutSimulationEnvironment) OnFailure(callback func(string, error)) {
	// Implementation for failure callback
}

func (e *TimeoutSimulationEnvironment) OnRecovery(callback func(string)) {
	// Implementation for recovery callback
}

func (e *TimeoutSimulationEnvironment) GetEFSState(ctx context.Context, fsID string) (*efstypes.FileSystemDescription, error) {
	// Implementation to get EFS state
	return nil, nil
}

func (e *TimeoutSimulationEnvironment) GetMountTargets(ctx context.Context, fsID string) ([]MountTargetInfo, error) {
	// Implementation to get mount targets
	return nil, nil
}

func (e *TimeoutSimulationEnvironment) GetAccessPoints(ctx context.Context, fsID string) ([]AccessPointInfo, error) {
	// Implementation to get access points
	return nil, nil
}

func (e *TimeoutSimulationEnvironment) OnMountTargetCreate(callback func(string, string)) {
	// Implementation for mount target creation callback
}

func (e *TimeoutSimulationEnvironment) OnMountTargetStateChange(callback func(string, string)) {
	// Implementation for mount target state change callback
}

// Similar implementations for PartialFailureEnvironment and CRDTestEnvironment methods...

type MountTargetInfo struct {
	ID       string
	SubnetID string
	State    string
}

type AccessPointInfo struct {
	ID   string
	Path string
}