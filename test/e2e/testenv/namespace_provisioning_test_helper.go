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

package testenv

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"k8s.io/klog/v2"
)

// NamespaceProvisioningTestHelper provides utilities for testing namespace provisioning scenarios
type NamespaceProvisioningTestHelper struct {
	env         *AWSTestEnvironment
	namespaces  map[string]*TestNamespace
	mu          sync.Mutex
}

// TestNamespace represents a test namespace with its resources
type TestNamespace struct {
	Name         string
	FileSystemID string
	AccessPoints []string
	PVCs         []TestPVC
	CreatedAt    time.Time
}

// TestPVC represents a test PVC
type TestPVC struct {
	Name          string
	Namespace     string
	AccessPointID string
	Size          string
	Status        string
}

// NewNamespaceProvisioningTestHelper creates a new test helper
func NewNamespaceProvisioningTestHelper(env *AWSTestEnvironment) *NamespaceProvisioningTestHelper {
	return &NamespaceProvisioningTestHelper{
		env:        env,
		namespaces: make(map[string]*TestNamespace),
	}
}

// CreateNamespaceWithEFS simulates creating a namespace with EFS provisioning
func (h *NamespaceProvisioningTestHelper) CreateNamespaceWithEFS(ctx context.Context, namespace string) (*TestNamespace, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Check if namespace already exists
	if ns, exists := h.namespaces[namespace]; exists {
		return ns, nil
	}

	// Create EFS for namespace
	fsID, err := h.createNamespaceEFS(ctx, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to create EFS for namespace %s: %w", namespace, err)
	}

	// Create mount targets
	if err := h.createMountTargets(ctx, fsID); err != nil {
		return nil, fmt.Errorf("failed to create mount targets: %w", err)
	}

	// Wait for EFS to be available
	if err := h.waitForEFSState(ctx, fsID, efstypes.LifeCycleStateAvailable); err != nil {
		return nil, fmt.Errorf("failed waiting for EFS to be available: %w", err)
	}

	ns := &TestNamespace{
		Name:         namespace,
		FileSystemID: fsID,
		AccessPoints: []string{},
		PVCs:         []TestPVC{},
		CreatedAt:    time.Now(),
	}

	h.namespaces[namespace] = ns
	klog.Infof("Created namespace %s with EFS %s", namespace, fsID)

	return ns, nil
}

// CreatePVCWithAccessPoint simulates creating a PVC with an access point
func (h *NamespaceProvisioningTestHelper) CreatePVCWithAccessPoint(ctx context.Context, namespace, pvcName, size string) (*TestPVC, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	ns, exists := h.namespaces[namespace]
	if !exists {
		return nil, fmt.Errorf("namespace %s not found", namespace)
	}

	// Create access point
	apID, err := h.createAccessPoint(ctx, ns.FileSystemID, namespace, pvcName)
	if err != nil {
		return nil, fmt.Errorf("failed to create access point: %w", err)
	}

	pvc := TestPVC{
		Name:          pvcName,
		Namespace:     namespace,
		AccessPointID: apID,
		Size:          size,
		Status:        "Bound",
	}

	ns.AccessPoints = append(ns.AccessPoints, apID)
	ns.PVCs = append(ns.PVCs, pvc)

	klog.Infof("Created PVC %s/%s with access point %s", namespace, pvcName, apID)

	return &pvc, nil
}

// SimulateConcurrentPVCCreation simulates concurrent PVC creation in a namespace
func (h *NamespaceProvisioningTestHelper) SimulateConcurrentPVCCreation(ctx context.Context, namespace string, numPVCs int) ([]TestPVC, error) {
	// Ensure namespace exists
	ns, err := h.CreateNamespaceWithEFS(ctx, namespace)
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	pvcs := make([]TestPVC, numPVCs)
	errors := make([]error, numPVCs)

	for i := 0; i < numPVCs; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			pvcName := fmt.Sprintf("pvc-%d", index)
			apID, err := h.createAccessPoint(ctx, ns.FileSystemID, namespace, pvcName)
			if err != nil {
				errors[index] = err
				return
			}

			pvcs[index] = TestPVC{
				Name:          pvcName,
				Namespace:     namespace,
				AccessPointID: apID,
				Size:          "10Gi",
				Status:        "Bound",
			}
		}(i)
	}

	wg.Wait()

	// Check for errors
	for i, err := range errors {
		if err != nil {
			return nil, fmt.Errorf("failed to create PVC %d: %w", i, err)
		}
	}

	// Update namespace with new PVCs
	h.mu.Lock()
	ns.PVCs = append(ns.PVCs, pvcs...)
	for _, pvc := range pvcs {
		ns.AccessPoints = append(ns.AccessPoints, pvc.AccessPointID)
	}
	h.mu.Unlock()

	klog.Infof("Successfully created %d PVCs concurrently in namespace %s", numPVCs, namespace)

	return pvcs, nil
}

// DeleteNamespace simulates deleting a namespace and its resources
func (h *NamespaceProvisioningTestHelper) DeleteNamespace(ctx context.Context, namespace string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	ns, exists := h.namespaces[namespace]
	if !exists {
		return fmt.Errorf("namespace %s not found", namespace)
	}

	// Delete all access points
	for _, apID := range ns.AccessPoints {
		if err := h.deleteAccessPoint(ctx, apID); err != nil {
			klog.Errorf("Failed to delete access point %s: %v", apID, err)
		}
	}

	// Delete mount targets
	if err := h.deleteMountTargets(ctx, ns.FileSystemID); err != nil {
		klog.Errorf("Failed to delete mount targets for %s: %v", ns.FileSystemID, err)
	}

	// Wait for mount targets to be deleted
	time.Sleep(30 * time.Second)

	// Delete EFS
	if err := h.deleteEFS(ctx, ns.FileSystemID); err != nil {
		return fmt.Errorf("failed to delete EFS %s: %w", ns.FileSystemID, err)
	}

	delete(h.namespaces, namespace)
	klog.Infof("Deleted namespace %s and its resources", namespace)

	return nil
}

// GetNamespaceStats returns statistics about a namespace
func (h *NamespaceProvisioningTestHelper) GetNamespaceStats(namespace string) map[string]interface{} {
	h.mu.Lock()
	defer h.mu.Unlock()

	ns, exists := h.namespaces[namespace]
	if !exists {
		return nil
	}

	return map[string]interface{}{
		"name":              ns.Name,
		"fileSystemId":      ns.FileSystemID,
		"numAccessPoints":   len(ns.AccessPoints),
		"numPVCs":          len(ns.PVCs),
		"createdAt":        ns.CreatedAt,
		"ageMinutes":       time.Since(ns.CreatedAt).Minutes(),
	}
}

// ValidateNamespaceIsolation validates that namespaces are properly isolated
func (h *NamespaceProvisioningTestHelper) ValidateNamespaceIsolation(ctx context.Context, t *testing.T) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Verify each namespace has its own EFS
	efsMap := make(map[string]string)
	for nsName, ns := range h.namespaces {
		if existingNs, exists := efsMap[ns.FileSystemID]; exists {
			t.Errorf("EFS %s is shared between namespaces %s and %s", ns.FileSystemID, existingNs, nsName)
		}
		efsMap[ns.FileSystemID] = nsName
	}

	// Verify access points are unique
	apMap := make(map[string]bool)
	for _, ns := range h.namespaces {
		for _, ap := range ns.AccessPoints {
			if apMap[ap] {
				t.Errorf("Access point %s is duplicated", ap)
			}
			apMap[ap] = true
		}
	}
}

// CleanupAll cleans up all test resources
func (h *NamespaceProvisioningTestHelper) CleanupAll(ctx context.Context) error {
	h.mu.Lock()
	namespaces := make([]string, 0, len(h.namespaces))
	for ns := range h.namespaces {
		namespaces = append(namespaces, ns)
	}
	h.mu.Unlock()

	var lastErr error
	for _, ns := range namespaces {
		if err := h.DeleteNamespace(ctx, ns); err != nil {
			klog.Errorf("Failed to cleanup namespace %s: %v", ns, err)
			lastErr = err
		}
	}

	return lastErr
}

// GetNamespaceData returns the namespace data if it exists
func (h *NamespaceProvisioningTestHelper) GetNamespaceData(namespace string) (*TestNamespace, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	ns, exists := h.namespaces[namespace]
	return ns, exists
}

// Helper methods

func (h *NamespaceProvisioningTestHelper) createNamespaceEFS(ctx context.Context, namespace string) (string, error) {
	resp, err := h.env.efsClient.CreateFileSystem(ctx, &efs.CreateFileSystemInput{
		CreationToken: aws.String(fmt.Sprintf("ns-%s-%s", namespace, h.env.TestID)),
		Tags: []efstypes.Tag{
			{
				Key:   aws.String("Name"),
				Value: aws.String(fmt.Sprintf("efs-ns-%s", namespace)),
			},
			{
				Key:   aws.String("kubernetes.io/namespace"),
				Value: aws.String(namespace),
			},
			{
				Key:   aws.String("kubernetes.io/cluster/" + h.env.ClusterName),
				Value: aws.String("owned"),
			},
			{
				Key:   aws.String("kubernetes.io/provisioning-mode"),
				Value: aws.String("efs-ns"),
			},
			{
				Key:   aws.String(TestTagKey),
				Value: aws.String(h.env.TestID),
			},
		},
		PerformanceMode: efstypes.PerformanceModeGeneralPurpose,
		Encrypted:       aws.Bool(true),
	})
	if err != nil {
		return "", err
	}

	return *resp.FileSystemId, nil
}

func (h *NamespaceProvisioningTestHelper) createMountTargets(ctx context.Context, fsID string) error {
	for _, subnetID := range h.env.SubnetIDs {
		_, err := h.env.efsClient.CreateMountTarget(ctx, &efs.CreateMountTargetInput{
			FileSystemId:   aws.String(fsID),
			SubnetId:       aws.String(subnetID),
			SecurityGroups: h.env.SecurityGroupIDs,
		})
		if err != nil {
			return fmt.Errorf("failed to create mount target in subnet %s: %w", subnetID, err)
		}
	}
	return nil
}

func (h *NamespaceProvisioningTestHelper) createAccessPoint(ctx context.Context, fsID, namespace, pvcName string) (string, error) {
	path := fmt.Sprintf("/%s/%s", namespace, pvcName)

	resp, err := h.env.efsClient.CreateAccessPoint(ctx, &efs.CreateAccessPointInput{
		FileSystemId: aws.String(fsID),
		PosixUser: &efstypes.PosixUser{
			Uid: aws.Int64(1000),
			Gid: aws.Int64(1000),
		},
		RootDirectory: &efstypes.RootDirectory{
			Path: aws.String(path),
			CreationInfo: &efstypes.CreationInfo{
				OwnerUid:    aws.Int64(1000),
				OwnerGid:    aws.Int64(1000),
				Permissions: aws.String("755"),
			},
		},
		Tags: []efstypes.Tag{
			{
				Key:   aws.String("Name"),
				Value: aws.String(fmt.Sprintf("ap-%s-%s", namespace, pvcName)),
			},
			{
				Key:   aws.String("kubernetes.io/namespace"),
				Value: aws.String(namespace),
			},
			{
				Key:   aws.String("kubernetes.io/pvc"),
				Value: aws.String(pvcName),
			},
			{
				Key:   aws.String(TestTagKey),
				Value: aws.String(h.env.TestID),
			},
		},
	})
	if err != nil {
		return "", err
	}

	// Wait for access point to be available
	apID := *resp.AccessPointId
	if err := h.waitForAccessPointState(ctx, apID, efstypes.LifeCycleStateAvailable); err != nil {
		return "", err
	}

	return apID, nil
}

func (h *NamespaceProvisioningTestHelper) deleteAccessPoint(ctx context.Context, apID string) error {
	_, err := h.env.efsClient.DeleteAccessPoint(ctx, &efs.DeleteAccessPointInput{
		AccessPointId: aws.String(apID),
	})
	return err
}

func (h *NamespaceProvisioningTestHelper) deleteMountTargets(ctx context.Context, fsID string) error {
	resp, err := h.env.efsClient.DescribeMountTargets(ctx, &efs.DescribeMountTargetsInput{
		FileSystemId: aws.String(fsID),
	})
	if err != nil {
		return err
	}

	for _, mt := range resp.MountTargets {
		_, err := h.env.efsClient.DeleteMountTarget(ctx, &efs.DeleteMountTargetInput{
			MountTargetId: mt.MountTargetId,
		})
		if err != nil {
			klog.Errorf("Failed to delete mount target %s: %v", *mt.MountTargetId, err)
		}
	}

	return nil
}

func (h *NamespaceProvisioningTestHelper) deleteEFS(ctx context.Context, fsID string) error {
	_, err := h.env.efsClient.DeleteFileSystem(ctx, &efs.DeleteFileSystemInput{
		FileSystemId: aws.String(fsID),
	})
	return err
}

func (h *NamespaceProvisioningTestHelper) waitForEFSState(ctx context.Context, fsID string, targetState efstypes.LifeCycleState) error {
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		resp, err := h.env.efsClient.DescribeFileSystems(ctx, &efs.DescribeFileSystemsInput{
			FileSystemId: aws.String(fsID),
		})
		if err != nil {
			return err
		}

		if len(resp.FileSystems) > 0 && resp.FileSystems[0].LifeCycleState == targetState {
			return nil
		}

		time.Sleep(10 * time.Second)
	}

	return fmt.Errorf("timeout waiting for EFS %s to reach state %s", fsID, targetState)
}

func (h *NamespaceProvisioningTestHelper) waitForAccessPointState(ctx context.Context, apID string, targetState efstypes.LifeCycleState) error {
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		resp, err := h.env.efsClient.DescribeAccessPoints(ctx, &efs.DescribeAccessPointsInput{
			AccessPointId: aws.String(apID),
		})
		if err != nil {
			return err
		}

		if len(resp.AccessPoints) > 0 && resp.AccessPoints[0].LifeCycleState == targetState {
			return nil
		}

		time.Sleep(5 * time.Second)
	}

	return fmt.Errorf("timeout waiting for access point %s to reach state %s", apID, targetState)
}