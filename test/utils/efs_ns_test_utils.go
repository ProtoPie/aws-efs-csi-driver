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

package utils

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
)

// EfsNsTestHelper provides helper methods for EFS-NS testing
type EfsNsTestHelper struct {
	client         clientset.Interface
	resourceTracker *TestResourceTracker
}

// NewEfsNsTestHelper creates a new EFS-NS test helper
func NewEfsNsTestHelper(client clientset.Interface) *EfsNsTestHelper {
	return &EfsNsTestHelper{
		client:          client,
		resourceTracker: NewTestResourceTracker(),
	}
}

// GetResourceTracker returns the resource tracker
func (h *EfsNsTestHelper) GetResourceTracker() *TestResourceTracker {
	return h.resourceTracker
}

// CreateEfsNsStorageClass creates a StorageClass for EFS-NS provisioning with tracking
func (h *EfsNsTestHelper) CreateEfsNsStorageClass(ctx context.Context, name string, parameters map[string]string) (*storagev1.StorageClass, error) {
	if parameters == nil {
		parameters = make(map[string]string)
	}
	parameters["provisioningMode"] = TestConstants.EfsNsProvisioning

	defaultBindingMode := storagev1.VolumeBindingImmediate
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"test-type": "efs-ns-test",
			},
		},
		Provisioner:       TestConstants.DefaultDriverName,
		Parameters:        parameters,
		VolumeBindingMode: &defaultBindingMode,
	}

	createdSC, err := h.client.StorageV1().StorageClasses().Create(ctx, sc, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}

	h.resourceTracker.AddStorageClass(createdSC.Name)
	return createdSC, nil
}

// CreateRandomEfsNsStorageClass creates a StorageClass with random name
func (h *EfsNsTestHelper) CreateRandomEfsNsStorageClass(ctx context.Context) (*storagev1.StorageClass, error) {
	name := RandomStringWithPrefix("efs-ns-sc", 8)
	return h.CreateEfsNsStorageClass(ctx, name, nil)
}

// CreatePVC creates a PVC with tracking
func (h *EfsNsTestHelper) CreatePVC(ctx context.Context, namespace, name, storageClassName string, size string) (*v1.PersistentVolumeClaim, error) {
	if size == "" {
		size = "1Gi"
	}

	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"test-type": "efs-ns-test",
			},
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: resource.MustParse(size),
				},
			},
			StorageClassName: &storageClassName,
		},
	}

	createdPVC, err := h.client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}

	h.resourceTracker.AddPVC(namespace, createdPVC.Name)
	return createdPVC, nil
}

// CreateRandomPVC creates a PVC with random name
func (h *EfsNsTestHelper) CreateRandomPVC(ctx context.Context, namespace, storageClassName string) (*v1.PersistentVolumeClaim, error) {
	name := RandomStringWithPrefix("test-pvc", 8)
	return h.CreatePVC(ctx, namespace, name, storageClassName, "")
}

// WaitForPVCBound waits for PVC to be bound with proper error handling
func (h *EfsNsTestHelper) WaitForPVCBound(ctx context.Context, namespace, pvcName string, timeout time.Duration) (*v1.PersistentVolumeClaim, error) {
	var boundPVC *v1.PersistentVolumeClaim
	
	err := wait.PollImmediate(TestConstants.PollingInterval, timeout, func() (bool, error) {
		pvc, err := h.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		
		if pvc.Status.Phase == v1.ClaimBound {
			boundPVC = pvc
			return true, nil
		}
		
		// Check for failure conditions
		if len(pvc.Status.Conditions) > 0 {
			for _, condition := range pvc.Status.Conditions {
				if condition.Type == v1.PersistentVolumeClaimResizing && condition.Status == v1.ConditionFalse {
					return false, fmt.Errorf("PVC failed to bind: %s", condition.Message)
				}
			}
		}
		
		return false, nil
	})
	
	if err != nil {
		// Get the latest PVC status for better error reporting
		pvc, getErr := h.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		if getErr == nil {
			return nil, fmt.Errorf("PVC %s/%s failed to bind within timeout %v, current phase: %s, conditions: %+v", 
				namespace, pvcName, timeout, pvc.Status.Phase, pvc.Status.Conditions)
		}
		return nil, fmt.Errorf("PVC %s/%s failed to bind within timeout %v: %w", namespace, pvcName, timeout, err)
	}
	
	return boundPVC, nil
}

// WaitForPVCDeleted waits for PVC to be deleted
func (h *EfsNsTestHelper) WaitForPVCDeleted(ctx context.Context, namespace, pvcName string, timeout time.Duration) error {
	return wait.PollImmediate(TestConstants.PollingInterval, timeout, func() (bool, error) {
		_, err := h.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		return false, nil
	})
}

// WaitForPVDeleted waits for PV to be deleted
func (h *EfsNsTestHelper) WaitForPVDeleted(ctx context.Context, pvName string, timeout time.Duration) error {
	return wait.PollImmediate(TestConstants.PollingInterval, timeout, func() (bool, error) {
		_, err := h.client.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		return false, nil
	})
}

// GetPVForPVC gets the PV associated with a PVC
func (h *EfsNsTestHelper) GetPVForPVC(ctx context.Context, namespace, pvcName string) (*v1.PersistentVolume, error) {
	pvc, err := h.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get PVC %s/%s: %w", namespace, pvcName, err)
	}
	
	if pvc.Spec.VolumeName == "" {
		return nil, fmt.Errorf("PVC %s/%s is not bound to any PV", namespace, pvcName)
	}
	
	pv, err := h.client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get PV %s: %w", pvc.Spec.VolumeName, err)
	}
	
	return pv, nil
}

// ExtractFileSystemIdFromPV extracts the EFS filesystem ID from a PV
func (h *EfsNsTestHelper) ExtractFileSystemIdFromPV(pv *v1.PersistentVolume) (string, error) {
	if pv.Spec.CSI == nil {
		return "", fmt.Errorf("PV %s is not a CSI volume", pv.Name)
	}
	
	if pv.Spec.CSI.Driver != TestConstants.DefaultDriverName {
		return "", fmt.Errorf("PV %s is not an EFS CSI volume, driver: %s", pv.Name, pv.Spec.CSI.Driver)
	}
	
	volumeHandle := pv.Spec.CSI.VolumeHandle
	if volumeHandle == "" {
		return "", fmt.Errorf("PV %s has empty volume handle", pv.Name)
	}
	
	// Volume handle format: "fs-12345678" or "fs-12345678:path"
	parts := strings.Split(volumeHandle, ":")
	fsId := strings.TrimSpace(parts[0])
	
	if !strings.HasPrefix(fsId, "fs-") {
		return "", fmt.Errorf("invalid filesystem ID format: %s", fsId)
	}
	
	return fsId, nil
}

// CreateTestPod creates a test pod with PVC mount
func (h *EfsNsTestHelper) CreateTestPod(ctx context.Context, namespace, podName, pvcName, command string) (*v1.Pod, error) {
	if command == "" {
		command = "sleep 3600"
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			Labels: map[string]string{
				"test-type": "efs-ns-test",
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:    "test-container",
					Image:   TestConstants.TestImageName,
					Command: []string{"/bin/sh", "-c", command},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "test-volume",
							MountPath: "/mnt/volume",
						},
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: "test-volume",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcName,
						},
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}

	return h.client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
}

// CreateRandomTestPod creates a test pod with random name
func (h *EfsNsTestHelper) CreateRandomTestPod(ctx context.Context, namespace, pvcName, command string) (*v1.Pod, error) {
	podName := RandomStringWithPrefix("test-pod", 8)
	return h.CreateTestPod(ctx, namespace, podName, pvcName, command)
}

// WaitForPodSuccess waits for a pod to complete successfully
func (h *EfsNsTestHelper) WaitForPodSuccess(ctx context.Context, namespace, podName string, timeout time.Duration) error {
	return wait.PollImmediate(TestConstants.PollingInterval, timeout, func() (bool, error) {
		pod, err := h.client.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		switch pod.Status.Phase {
		case v1.PodSucceeded:
			return true, nil
		case v1.PodFailed:
			return false, fmt.Errorf("pod %s/%s failed: %s", namespace, podName, pod.Status.Message)
		case v1.PodRunning:
			// Check if all containers have terminated successfully
			for _, status := range pod.Status.ContainerStatuses {
				if status.State.Terminated != nil && status.State.Terminated.ExitCode == 0 {
					return true, nil
				}
				if status.State.Terminated != nil && status.State.Terminated.ExitCode != 0 {
					return false, fmt.Errorf("container %s exited with code %d", status.Name, status.State.Terminated.ExitCode)
				}
			}
			return false, nil
		default:
			return false, nil
		}
	})
}

// WaitForPodRunning waits for a pod to be running
func (h *EfsNsTestHelper) WaitForPodRunning(ctx context.Context, namespace, podName string, timeout time.Duration) error {
	return wait.PollImmediate(TestConstants.PollingInterval, timeout, func() (bool, error) {
		pod, err := h.client.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		if pod.Status.Phase == v1.PodRunning {
			// Verify all containers are ready
			for _, status := range pod.Status.ContainerStatuses {
				if !status.Ready {
					return false, nil
				}
			}
			return true, nil
		}

		if pod.Status.Phase == v1.PodFailed {
			return false, fmt.Errorf("pod %s/%s failed: %s", namespace, podName, pod.Status.Message)
		}

		return false, nil
	})
}

// DeletePod deletes a pod with grace period
func (h *EfsNsTestHelper) DeletePod(ctx context.Context, namespace, podName string) error {
	gracePeriod := int64(30)
	return h.client.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriod,
	})
}

// CreateNamespace creates a namespace for testing with tracking
func (h *EfsNsTestHelper) CreateNamespace(ctx context.Context, name string) (*v1.Namespace, error) {
	ns := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"test-type": "efs-ns-test",
			},
		},
	}

	createdNS, err := h.client.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}

	h.resourceTracker.AddNamespace(createdNS.Name)
	return createdNS, nil
}

// CreateRandomNamespace creates a namespace with random name
func (h *EfsNsTestHelper) CreateRandomNamespace(ctx context.Context, prefix string) (*v1.Namespace, error) {
	name := RandomStringWithPrefix(prefix, 8)
	return h.CreateNamespace(ctx, name)
}

// CleanupResources cleans up all tracked resources
func (h *EfsNsTestHelper) CleanupResources(ctx context.Context) []error {
	var errors []error

	// Cleanup PVCs first (to trigger PV deletion)
	for namespace, pvcNames := range h.resourceTracker.PVCs {
		for _, pvcName := range pvcNames {
			if err := h.client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvcName, metav1.DeleteOptions{}); err != nil {
				if !apierrors.IsNotFound(err) {
					errors = append(errors, fmt.Errorf("failed to delete PVC %s/%s: %w", namespace, pvcName, err))
				}
			}
		}
	}

	// Cleanup ConfigMaps
	for namespace, configMapNames := range h.resourceTracker.ConfigMaps {
		for _, cmName := range configMapNames {
			if err := h.client.CoreV1().ConfigMaps(namespace).Delete(ctx, cmName, metav1.DeleteOptions{}); err != nil {
				if !apierrors.IsNotFound(err) {
					errors = append(errors, fmt.Errorf("failed to delete ConfigMap %s/%s: %w", namespace, cmName, err))
				}
			}
		}
	}

	// Cleanup StorageClasses
	for _, scName := range h.resourceTracker.StorageClasses {
		if err := h.client.StorageV1().StorageClasses().Delete(ctx, scName, metav1.DeleteOptions{}); err != nil {
			if !apierrors.IsNotFound(err) {
				errors = append(errors, fmt.Errorf("failed to delete StorageClass %s: %w", scName, err))
			}
		}
	}

	// Cleanup Namespaces (this will cascade delete resources within them)
	for _, nsName := range h.resourceTracker.Namespaces {
		if err := h.client.CoreV1().Namespaces().Delete(ctx, nsName, metav1.DeleteOptions{}); err != nil {
			if !apierrors.IsNotFound(err) {
				errors = append(errors, fmt.Errorf("failed to delete Namespace %s: %w", nsName, err))
			}
		}
	}

	return errors
}

// MultiNamespaceTestScenario provides utilities for multi-namespace testing
type MultiNamespaceTestScenario struct {
	helper     *EfsNsTestHelper
	namespaces []*v1.Namespace
	pvcs       map[string]*v1.PersistentVolumeClaim // namespace -> pvc
	pods       map[string]*v1.Pod                  // namespace -> pod
}

// NewMultiNamespaceTestScenario creates a new multi-namespace test scenario
func NewMultiNamespaceTestScenario(helper *EfsNsTestHelper) *MultiNamespaceTestScenario {
	return &MultiNamespaceTestScenario{
		helper: helper,
		pvcs:   make(map[string]*v1.PersistentVolumeClaim),
		pods:   make(map[string]*v1.Pod),
	}
}

// SetupNamespaces creates multiple namespaces for testing
func (m *MultiNamespaceTestScenario) SetupNamespaces(ctx context.Context, count int, namePrefix string) error {
	for i := 0; i < count; i++ {
		nsName := fmt.Sprintf("%s-%d", namePrefix, i)
		ns, err := m.helper.CreateNamespace(ctx, nsName)
		if err != nil {
			return fmt.Errorf("failed to create namespace %s: %w", nsName, err)
		}
		m.namespaces = append(m.namespaces, ns)
	}
	return nil
}

// CreatePVCsInAllNamespaces creates PVCs in all namespaces
func (m *MultiNamespaceTestScenario) CreatePVCsInAllNamespaces(ctx context.Context, storageClassName string) error {
	for _, ns := range m.namespaces {
		pvcName := "test-pvc-" + RandomString(6)
		pvc, err := m.helper.CreatePVC(ctx, ns.Name, pvcName, storageClassName, "")
		if err != nil {
			return fmt.Errorf("failed to create PVC in namespace %s: %w", ns.Name, err)
		}
		m.pvcs[ns.Name] = pvc
	}
	return nil
}

// WaitForAllPVCsBound waits for all PVCs to be bound
func (m *MultiNamespaceTestScenario) WaitForAllPVCsBound(ctx context.Context, timeout time.Duration) error {
	for nsName, pvc := range m.pvcs {
		_, err := m.helper.WaitForPVCBound(ctx, nsName, pvc.Name, timeout)
		if err != nil {
			return fmt.Errorf("PVC in namespace %s failed to bind: %w", nsName, err)
		}
	}
	return nil
}

// CreatePodsInAllNamespaces creates test pods in all namespaces
func (m *MultiNamespaceTestScenario) CreatePodsInAllNamespaces(ctx context.Context, command string) error {
	for _, ns := range m.namespaces {
		pvc, exists := m.pvcs[ns.Name]
		if !exists {
			return fmt.Errorf("no PVC found for namespace %s", ns.Name)
		}
		
		podName := "test-pod-" + RandomString(6)
		pod, err := m.helper.CreateTestPod(ctx, ns.Name, podName, pvc.Name, command)
		if err != nil {
			return fmt.Errorf("failed to create pod in namespace %s: %w", ns.Name, err)
		}
		m.pods[ns.Name] = pod
	}
	return nil
}

// WaitForAllPodsSuccess waits for all pods to complete successfully
func (m *MultiNamespaceTestScenario) WaitForAllPodsSuccess(ctx context.Context, timeout time.Duration) error {
	for nsName, pod := range m.pods {
		err := m.helper.WaitForPodSuccess(ctx, nsName, pod.Name, timeout)
		if err != nil {
			return fmt.Errorf("pod in namespace %s failed: %w", nsName, err)
		}
	}
	return nil
}

// GetFileSystemIdsForAllNamespaces gets filesystem IDs for all namespaces
func (m *MultiNamespaceTestScenario) GetFileSystemIdsForAllNamespaces(ctx context.Context) (map[string]string, error) {
	fsIds := make(map[string]string)
	
	for nsName, pvc := range m.pvcs {
		pv, err := m.helper.GetPVForPVC(ctx, nsName, pvc.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get PV for namespace %s: %w", nsName, err)
		}
		
		fsId, err := m.helper.ExtractFileSystemIdFromPV(pv)
		if err != nil {
			return nil, fmt.Errorf("failed to extract filesystem ID for namespace %s: %w", nsName, err)
		}
		
		fsIds[nsName] = fsId
	}
	
	return fsIds, nil
}

// GetNamespaces returns all created namespaces
func (m *MultiNamespaceTestScenario) GetNamespaces() []*v1.Namespace {
	return m.namespaces
}

// GetPVCs returns all created PVCs
func (m *MultiNamespaceTestScenario) GetPVCs() map[string]*v1.PersistentVolumeClaim {
	return m.pvcs
}

// GetPods returns all created pods
func (m *MultiNamespaceTestScenario) GetPods() map[string]*v1.Pod {
	return m.pods
}

// EfsNsTestAssertions provides assertion helpers for EFS-NS tests
type EfsNsTestAssertions struct {
	t interface {
		Errorf(format string, args ...interface{})
		FailNow()
	}
}

// NewEfsNsTestAssertions creates a new assertions helper
func NewEfsNsTestAssertions(t interface {
	Errorf(format string, args ...interface{})
	FailNow()
}) *EfsNsTestAssertions {
	return &EfsNsTestAssertions{t: t}
}

// AssertPVCBound asserts that a PVC is bound
func (a *EfsNsTestAssertions) AssertPVCBound(pvc *v1.PersistentVolumeClaim) {
	assert.Equal(a.t, v1.ClaimBound, pvc.Status.Phase, "PVC should be bound")
	assert.NotEmpty(a.t, pvc.Spec.VolumeName, "PVC should have a volume name")
}

// AssertPVHasEfsDriver asserts that a PV uses the EFS CSI driver
func (a *EfsNsTestAssertions) AssertPVHasEfsDriver(pv *v1.PersistentVolume) {
	require.NotNil(a.t, pv.Spec.CSI, "PV should have CSI spec")
	assert.Equal(a.t, TestConstants.DefaultDriverName, pv.Spec.CSI.Driver, "PV should use EFS CSI driver")
	assert.NotEmpty(a.t, pv.Spec.CSI.VolumeHandle, "PV should have volume handle")
}

// AssertFileSystemIdFormat asserts that a filesystem ID has the correct format
func (a *EfsNsTestAssertions) AssertFileSystemIdFormat(fsId string) {
	assert.True(a.t, strings.HasPrefix(fsId, "fs-"), "Filesystem ID should start with 'fs-'")
	assert.True(a.t, len(fsId) >= 11, "Filesystem ID should be at least 11 characters long") // fs- + 8 hex chars
}

// AssertNamespaceIsolation asserts that different namespaces have different filesystem IDs
func (a *EfsNsTestAssertions) AssertNamespaceIsolation(fsIds map[string]string) {
	require.True(a.t, len(fsIds) >= 2, "Need at least 2 namespaces to test isolation")
	
	seenIds := make(map[string]string)
	for namespace, fsId := range fsIds {
		if existingNamespace, exists := seenIds[fsId]; exists {
			a.t.Errorf("Filesystem ID %s is shared between namespaces %s and %s, but should be isolated", 
				fsId, existingNamespace, namespace)
		}
		seenIds[fsId] = namespace
	}
}

// AssertSameNamespaceSharing asserts that PVCs in the same namespace share the same filesystem
func (a *EfsNsTestAssertions) AssertSameNamespaceSharing(fsIds []string) {
	require.True(a.t, len(fsIds) >= 2, "Need at least 2 PVCs to test sharing")
	
	expectedFsId := fsIds[0]
	for i, fsId := range fsIds {
		assert.Equal(a.t, expectedFsId, fsId, 
			"PVC %d should share the same filesystem ID %s", i, expectedFsId)
	}
}