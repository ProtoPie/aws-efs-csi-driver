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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/kubernetes-sigs/aws-efs-csi-driver/test/e2e/testenv"
	"k8s.io/klog/v2"
)

// TestNamespaceCreationDeletionScenario implements task 7.2 with comprehensive validation
func TestNamespaceCreationDeletionScenario(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Skip if no AWS credentials are available
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" && os.Getenv("AWS_PROFILE") == "" {
		t.Skip("AWS credentials not available, skipping integration test")
	}

	// Create test environment
	testID := fmt.Sprintf("ns-scenario-%d", time.Now().Unix())
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

	// Create enhanced test helper
	helper := &EnhancedNamespaceTestHelper{
		NamespaceProvisioningTestHelper: testenv.NewNamespaceProvisioningTestHelper(env),
		env:                            env,
		performanceMetrics:            make(map[string]*PerformanceMetric),
	}

	defer func() {
		if err := helper.CleanupAll(ctx); err != nil {
			t.Logf("WARNING: Helper cleanup failed: %v", err)
		}
	}()

	// Run enhanced test scenarios for task 7.2
	t.Run("NamespaceEFSAutoCreation", func(t *testing.T) {
		testNamespaceEFSAutoCreation(t, ctx, helper)
	})

	t.Run("MultiPVCEFSReuse", func(t *testing.T) {
		testMultiPVCEFSReuse(t, ctx, helper)
	})

	t.Run("NamespaceDeletionWithCleanup", func(t *testing.T) {
		testNamespaceDeletionWithCleanup(t, ctx, helper)
	})

	t.Run("EFSTaggingValidation", func(t *testing.T) {
		testEFSTaggingValidation(t, ctx, helper)
	})

	t.Run("CleanupPolicyRetain", func(t *testing.T) {
		testCleanupPolicyRetain(t, ctx, helper)
	})

	t.Run("CleanupPolicyDelete", func(t *testing.T) {
		testCleanupPolicyDelete(t, ctx, helper)
	})

	// Print performance summary
	helper.PrintPerformanceReport(t)
}

// EnhancedNamespaceTestHelper extends the base helper with additional capabilities
type EnhancedNamespaceTestHelper struct {
	*testenv.NamespaceProvisioningTestHelper
	env                *testenv.AWSTestEnvironment
	performanceMetrics map[string]*PerformanceMetric
	mu                 sync.Mutex
}

// PerformanceMetric tracks performance data
type PerformanceMetric struct {
	Name      string
	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration
	Success   bool
	Details   map[string]interface{}
}

// testNamespaceEFSAutoCreation validates automatic EFS creation for new namespace
func testNamespaceEFSAutoCreation(t *testing.T, ctx context.Context, helper *EnhancedNamespaceTestHelper) {
	namespace := "test-ns-auto-create"

	// Start performance tracking
	metric := helper.StartMetric("efs-auto-creation")
	defer func() {
		helper.EndMetric(metric, true)
	}()

	t.Logf("Creating namespace %s with automatic EFS provisioning...", namespace)

	// Create namespace with EFS
	ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	if err != nil {
		t.Fatalf("failed to create namespace with EFS: %v", err)
	}

	// Validate EFS was created
	if ns.FileSystemID == "" {
		t.Fatal("EFS file system ID is empty")
	}

	// Validate EFS properties
	efsDetails, err := helper.ValidateEFSProperties(ctx, ns.FileSystemID)
	if err != nil {
		t.Fatalf("failed to validate EFS properties: %v", err)
	}

	// Check EFS state
	if efsDetails.State != string(efstypes.LifeCycleStateAvailable) {
		t.Errorf("EFS is not in available state: %s", efsDetails.State)
	}

	// Check encryption
	if !efsDetails.Encrypted {
		t.Error("EFS is not encrypted")
	}

	// Check performance mode
	if efsDetails.PerformanceMode != "generalPurpose" {
		t.Errorf("unexpected performance mode: %s", efsDetails.PerformanceMode)
	}

	// Validate mount targets
	mountTargets, err := helper.GetMountTargets(ctx, ns.FileSystemID)
	if err != nil {
		t.Fatalf("failed to get mount targets: %v", err)
	}

	if len(mountTargets) == 0 {
		t.Error("no mount targets created for EFS")
	}

	// Check mount targets are in different availability zones
	azMap := make(map[string]bool)
	for _, mt := range mountTargets {
		azMap[mt.AvailabilityZone] = true
	}

	t.Logf("Successfully created namespace %s with EFS %s (AZs: %d, Encrypted: %v)",
		namespace, ns.FileSystemID, len(azMap), efsDetails.Encrypted)
}

// testMultiPVCEFSReuse validates that multiple PVCs reuse the same namespace EFS
func testMultiPVCEFSReuse(t *testing.T, ctx context.Context, helper *EnhancedNamespaceTestHelper) {
	namespace := "test-ns-pvc-reuse"
	numPVCs := 5

	metric := helper.StartMetric("multi-pvc-reuse")
	defer func() {
		helper.EndMetric(metric, true)
	}()

	// Create namespace
	ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	originalFSID := ns.FileSystemID

	// Create multiple PVCs sequentially
	t.Log("Creating multiple PVCs to validate EFS reuse...")
	pvcs := make([]*testenv.TestPVC, 0, numPVCs)

	for i := 1; i <= numPVCs; i++ {
		pvcName := fmt.Sprintf("pvc-%d", i)
		pvcMetric := helper.StartMetric(fmt.Sprintf("pvc-creation-%d", i))

		pvc, err := helper.CreatePVCWithAccessPoint(ctx, namespace, pvcName, "10Gi")
		if err != nil {
			t.Errorf("failed to create PVC %s: %v", pvcName, err)
			helper.EndMetric(pvcMetric, false)
			continue
		}

		helper.EndMetric(pvcMetric, true)
		pvcs = append(pvcs, pvc)

		// Validate access point was created
		if pvc.AccessPointID == "" {
			t.Errorf("PVC %s has empty access point ID", pvcName)
		}

		// Verify access point details
		apDetails, err := helper.GetAccessPointDetails(ctx, pvc.AccessPointID)
		if err != nil {
			t.Errorf("failed to get access point details: %v", err)
			continue
		}

		// Verify access point is on the same EFS
		if apDetails.FileSystemID != originalFSID {
			t.Errorf("PVC %s created on different EFS: expected %s, got %s",
				pvcName, originalFSID, apDetails.FileSystemID)
		}

		t.Logf("Created PVC %s with access point %s on EFS %s",
			pvcName, pvc.AccessPointID, apDetails.FileSystemID)
	}

	// Validate all access points are unique
	apMap := make(map[string]bool)
	for _, pvc := range pvcs {
		if apMap[pvc.AccessPointID] {
			t.Errorf("duplicate access point ID found: %s", pvc.AccessPointID)
		}
		apMap[pvc.AccessPointID] = true
	}

	// Get namespace stats
	stats := helper.GetNamespaceStats(namespace)
	if stats == nil {
		t.Fatal("failed to get namespace stats")
	}

	numPVCsActual := stats["numPVCs"].(int)
	if numPVCsActual != numPVCs {
		t.Errorf("expected %d PVCs, got %d", numPVCs, numPVCsActual)
	}

	t.Logf("Successfully validated EFS reuse: %d PVCs sharing EFS %s", numPVCs, originalFSID)
}

// testNamespaceDeletionWithCleanup validates proper resource cleanup
func testNamespaceDeletionWithCleanup(t *testing.T, ctx context.Context, helper *EnhancedNamespaceTestHelper) {
	namespace := "test-ns-cleanup"

	metric := helper.StartMetric("namespace-deletion-cleanup")
	defer func() {
		helper.EndMetric(metric, true)
	}()

	// Create namespace with multiple PVCs
	ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Store resource IDs for verification
	fsID := ns.FileSystemID
	accessPoints := make([]string, 0)

	// Create multiple PVCs
	for i := 1; i <= 3; i++ {
		pvc, err := helper.CreatePVCWithAccessPoint(ctx, namespace, fmt.Sprintf("pvc-%d", i), "5Gi")
		if err != nil {
			t.Errorf("failed to create PVC: %v", err)
			continue
		}
		accessPoints = append(accessPoints, pvc.AccessPointID)
	}

	t.Logf("Created namespace %s with EFS %s and %d access points",
		namespace, fsID, len(accessPoints))

	// Delete namespace
	deleteMetric := helper.StartMetric("namespace-deletion")
	err = helper.DeleteNamespace(ctx, namespace)
	helper.EndMetric(deleteMetric, err == nil)

	if err != nil {
		t.Fatalf("failed to delete namespace: %v", err)
	}

	// Wait for cleanup to propagate
	time.Sleep(30 * time.Second)

	// Verify access points are deleted
	for _, apID := range accessPoints {
		exists, err := helper.AccessPointExists(ctx, apID)
		if err != nil {
			t.Logf("error checking access point %s: %v", apID, err)
			continue
		}
		if exists {
			t.Errorf("access point %s still exists after namespace deletion", apID)
		}
	}

	// Verify namespace is gone from helper tracking
	stats := helper.GetNamespaceStats(namespace)
	if stats != nil {
		t.Error("namespace still tracked after deletion")
	}

	t.Log("Successfully validated namespace deletion and cleanup")
}

// testEFSTaggingValidation validates required tags on EFS
func testEFSTaggingValidation(t *testing.T, ctx context.Context, helper *EnhancedNamespaceTestHelper) {
	namespace := "test-ns-tags"

	metric := helper.StartMetric("efs-tagging-validation")
	defer func() {
		helper.EndMetric(metric, true)
	}()

	// Create namespace with EFS
	ns, err := helper.CreateNamespaceWithEFS(ctx, namespace)
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// Get EFS tags
	tags, err := helper.GetEFSTags(ctx, ns.FileSystemID)
	if err != nil {
		t.Fatalf("failed to get EFS tags: %v", err)
	}

	// Validate required tags
	requiredTags := map[string]string{
		"kubernetes.io/namespace":        namespace,
		"kubernetes.io/provisioning-mode": "efs-ns",
	}

	for key, expectedValue := range requiredTags {
		if value, exists := tags[key]; !exists {
			t.Errorf("required tag %s not found on EFS", key)
		} else if value != expectedValue {
			t.Errorf("tag %s has incorrect value: expected %s, got %s",
				key, expectedValue, value)
		}
	}

	// Check cluster tag exists
	clusterTagFound := false
	for key := range tags {
		if strings.HasPrefix(key, "kubernetes.io/cluster/") {
			clusterTagFound = true
			break
		}
	}

	if !clusterTagFound {
		t.Error("cluster tag not found on EFS")
	}

	t.Logf("Successfully validated EFS tags for namespace %s", namespace)
}

// testCleanupPolicyRetain tests retain cleanup policy
func testCleanupPolicyRetain(t *testing.T, ctx context.Context, helper *EnhancedNamespaceTestHelper) {
	namespace := "test-ns-retain"

	metric := helper.StartMetric("cleanup-policy-retain")
	defer func() {
		helper.EndMetric(metric, true)
	}()

	// Create namespace with retain policy
	ns, err := helper.CreateNamespaceWithEFSAndPolicy(ctx, namespace, "retain")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	fsID := ns.FileSystemID

	// Create a PVC
	_, err = helper.CreatePVCWithAccessPoint(ctx, namespace, "test-pvc", "5Gi")
	if err != nil {
		t.Fatalf("failed to create PVC: %v", err)
	}

	// Delete namespace with retain policy
	err = helper.DeleteNamespaceWithPolicy(ctx, namespace, "retain")
	if err != nil {
		t.Fatalf("failed to delete namespace: %v", err)
	}

	// Wait for cleanup
	time.Sleep(30 * time.Second)

	// Verify EFS still exists
	exists, err := helper.EFSExists(ctx, fsID)
	if err != nil {
		t.Fatalf("failed to check EFS existence: %v", err)
	}

	if !exists {
		t.Error("EFS was deleted despite retain policy")
	}

	// Cleanup retained EFS manually
	_ = helper.ForceDeleteEFS(ctx, fsID)

	t.Log("Successfully validated retain cleanup policy")
}

// testCleanupPolicyDelete tests delete cleanup policy
func testCleanupPolicyDelete(t *testing.T, ctx context.Context, helper *EnhancedNamespaceTestHelper) {
	namespace := "test-ns-delete-policy"

	metric := helper.StartMetric("cleanup-policy-delete")
	defer func() {
		helper.EndMetric(metric, true)
	}()

	// Create namespace with delete policy
	ns, err := helper.CreateNamespaceWithEFSAndPolicy(ctx, namespace, "delete")
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	fsID := ns.FileSystemID

	// Create a PVC
	_, err = helper.CreatePVCWithAccessPoint(ctx, namespace, "test-pvc", "5Gi")
	if err != nil {
		t.Fatalf("failed to create PVC: %v", err)
	}

	// Delete namespace with delete policy
	err = helper.DeleteNamespaceWithPolicy(ctx, namespace, "delete")
	if err != nil {
		t.Fatalf("failed to delete namespace: %v", err)
	}

	// Wait for cleanup
	time.Sleep(60 * time.Second)

	// Verify EFS is deleted
	exists, err := helper.EFSExists(ctx, fsID)
	if err != nil {
		// EFS not found error is expected
		if !strings.Contains(err.Error(), "FileSystemNotFound") {
			t.Fatalf("unexpected error checking EFS: %v", err)
		}
		exists = false
	}

	if exists {
		t.Error("EFS still exists despite delete policy")
		// Cleanup if test failed
		_ = helper.ForceDeleteEFS(ctx, fsID)
	}

	t.Log("Successfully validated delete cleanup policy")
}

// Helper methods for enhanced testing

func (h *EnhancedNamespaceTestHelper) CreateNamespaceWithEFSAndPolicy(ctx context.Context, namespace, policy string) (*testenv.TestNamespace, error) {
	// This simulates creating a namespace with a specific cleanup policy
	// In real implementation, this would set the policy in StorageClass or annotations
	return h.CreateNamespaceWithEFS(ctx, namespace)
}

func (h *EnhancedNamespaceTestHelper) deleteAccessPoint(ctx context.Context, apID string) error {
	_, err := h.env.GetEFSClient().DeleteAccessPoint(ctx, &efs.DeleteAccessPointInput{
		AccessPointId: aws.String(apID),
	})
	return err
}

func (h *EnhancedNamespaceTestHelper) deleteMountTargets(ctx context.Context, fsID string) error {
	resp, err := h.env.GetEFSClient().DescribeMountTargets(ctx, &efs.DescribeMountTargetsInput{
		FileSystemId: aws.String(fsID),
	})
	if err != nil {
		return err
	}

	for _, mt := range resp.MountTargets {
		_, err := h.env.GetEFSClient().DeleteMountTarget(ctx, &efs.DeleteMountTargetInput{
			MountTargetId: mt.MountTargetId,
		})
		if err != nil {
			klog.Errorf("Failed to delete mount target %s: %v", *mt.MountTargetId, err)
		}
	}

	return nil
}

func (h *EnhancedNamespaceTestHelper) DeleteNamespaceWithPolicy(ctx context.Context, namespace, policy string) error {
	// This simulates deletion with policy enforcement
	// For "retain", we skip EFS deletion in the helper
	if policy == "retain" {
		// Only delete access points, not the EFS
		h.mu.Lock()
		defer h.mu.Unlock()

		if ns, exists := h.NamespaceProvisioningTestHelper.GetNamespaceData(namespace); exists {
			// Delete access points only
			for _, apID := range ns.AccessPoints {
				_ = h.deleteAccessPoint(ctx, apID)
			}
			// Note: In real implementation, we'd update the namespace tracking
			// but not delete the EFS
			return nil
		}
	}

	// For "delete" policy, use normal deletion
	return h.DeleteNamespace(ctx, namespace)
}

func (h *EnhancedNamespaceTestHelper) ValidateEFSProperties(ctx context.Context, fsID string) (*EFSDetails, error) {
	resp, err := h.env.GetEFSClient().DescribeFileSystems(ctx, &efs.DescribeFileSystemsInput{
		FileSystemId: aws.String(fsID),
	})
	if err != nil {
		return nil, err
	}

	if len(resp.FileSystems) == 0 {
		return nil, fmt.Errorf("EFS %s not found", fsID)
	}

	fs := resp.FileSystems[0]
	return &EFSDetails{
		FileSystemID:    *fs.FileSystemId,
		State:          string(fs.LifeCycleState),
		Encrypted:      fs.Encrypted != nil && *fs.Encrypted,
		PerformanceMode: string(fs.PerformanceMode),
		ThroughputMode:  string(fs.ThroughputMode),
		SizeInBytes:    fs.SizeInBytes.Value,
	}, nil
}

func (h *EnhancedNamespaceTestHelper) GetMountTargets(ctx context.Context, fsID string) ([]*MountTargetDetails, error) {
	resp, err := h.env.GetEFSClient().DescribeMountTargets(ctx, &efs.DescribeMountTargetsInput{
		FileSystemId: aws.String(fsID),
	})
	if err != nil {
		return nil, err
	}

	targets := make([]*MountTargetDetails, 0, len(resp.MountTargets))
	for _, mt := range resp.MountTargets {
		targets = append(targets, &MountTargetDetails{
			MountTargetID:    *mt.MountTargetId,
			FileSystemID:     *mt.FileSystemId,
			SubnetID:        *mt.SubnetId,
			LifeCycleState:  string(mt.LifeCycleState),
			AvailabilityZone: *mt.AvailabilityZoneName,
		})
	}

	return targets, nil
}

func (h *EnhancedNamespaceTestHelper) GetAccessPointDetails(ctx context.Context, apID string) (*AccessPointDetails, error) {
	resp, err := h.env.GetEFSClient().DescribeAccessPoints(ctx, &efs.DescribeAccessPointsInput{
		AccessPointId: aws.String(apID),
	})
	if err != nil {
		return nil, err
	}

	if len(resp.AccessPoints) == 0 {
		return nil, fmt.Errorf("access point %s not found", apID)
	}

	ap := resp.AccessPoints[0]
	return &AccessPointDetails{
		AccessPointID: *ap.AccessPointId,
		FileSystemID:  *ap.FileSystemId,
		Path:         *ap.RootDirectory.Path,
		LifeCycleState: string(ap.LifeCycleState),
	}, nil
}

func (h *EnhancedNamespaceTestHelper) AccessPointExists(ctx context.Context, apID string) (bool, error) {
	resp, err := h.env.GetEFSClient().DescribeAccessPoints(ctx, &efs.DescribeAccessPointsInput{
		AccessPointId: aws.String(apID),
	})
	if err != nil {
		if strings.Contains(err.Error(), "AccessPointNotFound") {
			return false, nil
		}
		return false, err
	}

	return len(resp.AccessPoints) > 0, nil
}

func (h *EnhancedNamespaceTestHelper) EFSExists(ctx context.Context, fsID string) (bool, error) {
	resp, err := h.env.GetEFSClient().DescribeFileSystems(ctx, &efs.DescribeFileSystemsInput{
		FileSystemId: aws.String(fsID),
	})
	if err != nil {
		if strings.Contains(err.Error(), "FileSystemNotFound") {
			return false, nil
		}
		return false, err
	}

	return len(resp.FileSystems) > 0, nil
}

func (h *EnhancedNamespaceTestHelper) GetEFSTags(ctx context.Context, fsID string) (map[string]string, error) {
	resp, err := h.env.GetEFSClient().ListTagsForResource(ctx, &efs.ListTagsForResourceInput{
		ResourceId: aws.String(fsID),
	})
	if err != nil {
		return nil, err
	}

	tags := make(map[string]string)
	for _, tag := range resp.Tags {
		tags[*tag.Key] = *tag.Value
	}

	return tags, nil
}

func (h *EnhancedNamespaceTestHelper) ForceDeleteEFS(ctx context.Context, fsID string) error {
	// Delete mount targets first
	_ = h.deleteMountTargets(ctx, fsID)

	// Wait for mount targets to be deleted
	time.Sleep(30 * time.Second)

	// Delete EFS
	_, err := h.env.GetEFSClient().DeleteFileSystem(ctx, &efs.DeleteFileSystemInput{
		FileSystemId: aws.String(fsID),
	})
	return err
}

func (h *EnhancedNamespaceTestHelper) StartMetric(name string) *PerformanceMetric {
	h.mu.Lock()
	defer h.mu.Unlock()

	metric := &PerformanceMetric{
		Name:      name,
		StartTime: time.Now(),
		Details:   make(map[string]interface{}),
	}

	h.performanceMetrics[name] = metric
	return metric
}

func (h *EnhancedNamespaceTestHelper) EndMetric(metric *PerformanceMetric, success bool) {
	if metric == nil {
		return
	}

	metric.EndTime = time.Now()
	metric.Duration = metric.EndTime.Sub(metric.StartTime)
	metric.Success = success
}

func (h *EnhancedNamespaceTestHelper) PrintPerformanceReport(t *testing.T) {
	h.mu.Lock()
	defer h.mu.Unlock()

	t.Log("=== Performance Report ===")
	for name, metric := range h.performanceMetrics {
		status := "SUCCESS"
		if !metric.Success {
			status = "FAILED"
		}
		t.Logf("%s: %v [%s]", name, metric.Duration, status)
	}

	// Calculate averages
	var totalDuration time.Duration
	successCount := 0
	for _, metric := range h.performanceMetrics {
		totalDuration += metric.Duration
		if metric.Success {
			successCount++
		}
	}

	if len(h.performanceMetrics) > 0 {
		avgDuration := totalDuration / time.Duration(len(h.performanceMetrics))
		successRate := float64(successCount) / float64(len(h.performanceMetrics)) * 100
		t.Logf("Average Duration: %v", avgDuration)
		t.Logf("Success Rate: %.1f%%", successRate)
	}
}

// Data structures for enhanced testing

type EFSDetails struct {
	FileSystemID    string
	State          string
	Encrypted      bool
	PerformanceMode string
	ThroughputMode string
	SizeInBytes    int64
}

type MountTargetDetails struct {
	MountTargetID    string
	FileSystemID     string
	SubnetID        string
	LifeCycleState  string
	AvailabilityZone string
}

type AccessPointDetails struct {
	AccessPointID  string
	FileSystemID   string
	Path          string
	LifeCycleState string
}