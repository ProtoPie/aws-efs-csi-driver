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

package efsns

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	// FinalizerCleanupTimeout is the timeout for finalizer cleanup operations
	FinalizerCleanupTimeout = 30 * time.Second

	// RetryInterval is the interval between retry attempts for finalizer operations
	RetryInterval = 2 * time.Second

	// MaxRetryAttempts is the maximum number of retry attempts for finalizer operations
	MaxRetryAttempts = 5
)

// FinalizerManager interface handles finalizer operations for efs-ns mode
type FinalizerManager interface {
	// AddFinalizer adds efs-ns finalizer to PVC
	AddFinalizer(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error

	// RemoveFinalizer removes efs-ns finalizer from PVC
	RemoveFinalizer(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error

	// AddNamespaceFinalizer adds finalizer to namespace for cleanup
	AddNamespaceFinalizer(ctx context.Context, namespace string) error

	// RemoveNamespaceFinalizer removes finalizer from namespace
	RemoveNamespaceFinalizer(ctx context.Context, namespace string) error

	// ProcessFinalization handles finalizer-based cleanup orchestration
	ProcessFinalization(ctx context.Context, object interface{}) error
}

// ConfigMapFinalizerManager implements FinalizerManager using Kubernetes API
type ConfigMapFinalizerManager struct {
	client    kubernetes.Interface
	clusterID string
	mutex     sync.RWMutex

	// Dependencies for cleanup operations
	pvcTracker    PVCTracker
	fileSystemMgr NamespaceFileSystemManager
}

// NewConfigMapFinalizerManager creates a new ConfigMap-based finalizer manager
func NewConfigMapFinalizerManager(
	client kubernetes.Interface,
	clusterID string,
	pvcTracker PVCTracker,
	fileSystemMgr NamespaceFileSystemManager,
) *ConfigMapFinalizerManager {
	if client == nil {
		panic("kubernetes client cannot be nil")
	}
	if clusterID == "" {
		panic("cluster ID cannot be empty")
	}
	if pvcTracker == nil {
		panic("PVC tracker cannot be nil")
	}
	if fileSystemMgr == nil {
		panic("filesystem manager cannot be nil")
	}

	return &ConfigMapFinalizerManager{
		client:        client,
		clusterID:     clusterID,
		pvcTracker:    pvcTracker,
		fileSystemMgr: fileSystemMgr,
	}
}

// AddFinalizer adds efs-ns finalizer to PVC with retry logic
func (f *ConfigMapFinalizerManager) AddFinalizer(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if pvc == nil {
		return NewEFSNSError(ErrInvalidParameter, "AddFinalizer", "", "PVC cannot be nil", nil)
	}

	klog.V(4).Infof("Adding finalizer %s to PVC %s/%s", EFSNSFinalizerName, pvc.Namespace, pvc.Name)

	// Check if finalizer already exists
	for _, finalizer := range pvc.Finalizers {
		if finalizer == EFSNSFinalizerName {
			klog.V(4).Infof("Finalizer %s already exists on PVC %s/%s", EFSNSFinalizerName, pvc.Namespace, pvc.Name)
			return nil
		}
	}

	// Add finalizer with retry logic
	return f.retryOperation(ctx, func() error {
		// Get latest version of PVC
		latestPVC, err := f.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(
			ctx, pvc.Name, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				klog.V(4).Infof("PVC %s/%s not found, skipping finalizer addition", pvc.Namespace, pvc.Name)
				return nil
			}
			return NewEFSNSError(ErrKubernetesAPIFailed, "AddFinalizer", pvc.Namespace,
				fmt.Sprintf("failed to get PVC %s: %v", pvc.Name, err), err)
		}

		// Check if finalizer already exists in latest version
		for _, finalizer := range latestPVC.Finalizers {
			if finalizer == EFSNSFinalizerName {
				return nil
			}
		}

		// Add finalizer
		latestPVC.Finalizers = append(latestPVC.Finalizers, EFSNSFinalizerName)

		// Update PVC
		_, err = f.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, latestPVC, metav1.UpdateOptions{})
		if err != nil {
			return NewEFSNSError(ErrKubernetesAPIFailed, "AddFinalizer", pvc.Namespace,
				fmt.Sprintf("failed to update PVC %s: %v", pvc.Name, err), err)
		}

		klog.V(2).Infof("Successfully added finalizer %s to PVC %s/%s", EFSNSFinalizerName, pvc.Namespace, pvc.Name)
		return nil
	})
}

// RemoveFinalizer removes efs-ns finalizer from PVC with retry logic
func (f *ConfigMapFinalizerManager) RemoveFinalizer(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if pvc == nil {
		return NewEFSNSError(ErrInvalidParameter, "RemoveFinalizer", "", "PVC cannot be nil", nil)
	}

	klog.V(4).Infof("Removing finalizer %s from PVC %s/%s", EFSNSFinalizerName, pvc.Namespace, pvc.Name)

	// Remove finalizer with retry logic
	return f.retryOperation(ctx, func() error {
		// Get latest version of PVC
		latestPVC, err := f.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(
			ctx, pvc.Name, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				klog.V(4).Infof("PVC %s/%s not found, finalizer removal complete", pvc.Namespace, pvc.Name)
				return nil
			}
			return NewEFSNSError(ErrKubernetesAPIFailed, "RemoveFinalizer", pvc.Namespace,
				fmt.Sprintf("failed to get PVC %s: %v", pvc.Name, err), err)
		}

		// Check if finalizer exists and remove it
		var updatedFinalizers []string
		finalizerFound := false
		for _, finalizer := range latestPVC.Finalizers {
			if finalizer != EFSNSFinalizerName {
				updatedFinalizers = append(updatedFinalizers, finalizer)
			} else {
				finalizerFound = true
			}
		}

		if !finalizerFound {
			klog.V(4).Infof("Finalizer %s not found on PVC %s/%s", EFSNSFinalizerName, pvc.Namespace, pvc.Name)
			return nil
		}

		// Update finalizers
		latestPVC.Finalizers = updatedFinalizers

		// Update PVC
		_, err = f.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, latestPVC, metav1.UpdateOptions{})
		if err != nil {
			return NewEFSNSError(ErrKubernetesAPIFailed, "RemoveFinalizer", pvc.Namespace,
				fmt.Sprintf("failed to update PVC %s: %v", pvc.Name, err), err)
		}

		klog.V(2).Infof("Successfully removed finalizer %s from PVC %s/%s", EFSNSFinalizerName, pvc.Namespace, pvc.Name)
		return nil
	})
}

// AddNamespaceFinalizer adds finalizer to namespace for cleanup
func (f *ConfigMapFinalizerManager) AddNamespaceFinalizer(ctx context.Context, namespace string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if namespace == "" {
		return NewEFSNSError(ErrInvalidParameter, "AddNamespaceFinalizer", "", "namespace cannot be empty", nil)
	}

	klog.V(4).Infof("Adding finalizer %s to namespace %s", EFSNSNamespaceFinalizerName, namespace)

	// Add finalizer with retry logic
	return f.retryOperation(ctx, func() error {
		// Get latest version of namespace
		ns, err := f.client.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				klog.V(4).Infof("Namespace %s not found, skipping finalizer addition", namespace)
				return nil
			}
			return NewEFSNSError(ErrKubernetesAPIFailed, "AddNamespaceFinalizer", namespace,
				fmt.Sprintf("failed to get namespace: %v", err), err)
		}

		// Check if finalizer already exists
		for _, finalizer := range ns.Finalizers {
			if finalizer == EFSNSNamespaceFinalizerName {
				klog.V(4).Infof("Finalizer %s already exists on namespace %s", EFSNSNamespaceFinalizerName, namespace)
				return nil
			}
		}

		// Add finalizer
		ns.Finalizers = append(ns.Finalizers, EFSNSNamespaceFinalizerName)

		// Update namespace
		_, err = f.client.CoreV1().Namespaces().Update(ctx, ns, metav1.UpdateOptions{})
		if err != nil {
			return NewEFSNSError(ErrKubernetesAPIFailed, "AddNamespaceFinalizer", namespace,
				fmt.Sprintf("failed to update namespace: %v", err), err)
		}

		klog.V(2).Infof("Successfully added finalizer %s to namespace %s", EFSNSNamespaceFinalizerName, namespace)
		return nil
	})
}

// RemoveNamespaceFinalizer removes finalizer from namespace
func (f *ConfigMapFinalizerManager) RemoveNamespaceFinalizer(ctx context.Context, namespace string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if namespace == "" {
		return NewEFSNSError(ErrInvalidParameter, "RemoveNamespaceFinalizer", "", "namespace cannot be empty", nil)
	}

	klog.V(4).Infof("Removing finalizer %s from namespace %s", EFSNSNamespaceFinalizerName, namespace)

	// Remove finalizer with retry logic
	return f.retryOperation(ctx, func() error {
		// Get latest version of namespace
		ns, err := f.client.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				klog.V(4).Infof("Namespace %s not found, finalizer removal complete", namespace)
				return nil
			}
			return NewEFSNSError(ErrKubernetesAPIFailed, "RemoveNamespaceFinalizer", namespace,
				fmt.Sprintf("failed to get namespace: %v", err), err)
		}

		// Check if finalizer exists and remove it
		var updatedFinalizers []string
		finalizerFound := false
		for _, finalizer := range ns.Finalizers {
			if finalizer != EFSNSNamespaceFinalizerName {
				updatedFinalizers = append(updatedFinalizers, finalizer)
			} else {
				finalizerFound = true
			}
		}

		if !finalizerFound {
			klog.V(4).Infof("Finalizer %s not found on namespace %s", EFSNSNamespaceFinalizerName, namespace)
			return nil
		}

		// Update finalizers
		ns.Finalizers = updatedFinalizers

		// Update namespace
		_, err = f.client.CoreV1().Namespaces().Update(ctx, ns, metav1.UpdateOptions{})
		if err != nil {
			return NewEFSNSError(ErrKubernetesAPIFailed, "RemoveNamespaceFinalizer", namespace,
				fmt.Sprintf("failed to update namespace: %v", err), err)
		}

		klog.V(2).Infof("Successfully removed finalizer %s from namespace %s", EFSNSNamespaceFinalizerName, namespace)
		return nil
	})
}

// ProcessFinalization handles finalizer-based cleanup orchestration
func (f *ConfigMapFinalizerManager) ProcessFinalization(ctx context.Context, object interface{}) error {
	if object == nil {
		return NewEFSNSError(ErrInvalidParameter, "ProcessFinalization", "", "object cannot be nil", nil)
	}

	// Create cleanup context with timeout
	cleanupCtx, cancel := context.WithTimeout(ctx, FinalizerCleanupTimeout)
	defer cancel()

	switch obj := object.(type) {
	case *corev1.PersistentVolumeClaim:
		return f.processPVCFinalization(cleanupCtx, obj)
	case *corev1.Namespace:
		return f.processNamespaceFinalization(cleanupCtx, obj)
	default:
		return NewEFSNSError(ErrInvalidParameter, "ProcessFinalization", "",
			fmt.Sprintf("unsupported object type: %T", object), nil)
	}
}

// processPVCFinalization handles PVC finalizer cleanup
func (f *ConfigMapFinalizerManager) processPVCFinalization(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	klog.V(4).Infof("Processing PVC finalization for %s/%s", pvc.Namespace, pvc.Name)

	// Check if our finalizer is present
	hasOurFinalizer := false
	for _, finalizer := range pvc.Finalizers {
		if finalizer == EFSNSFinalizerName {
			hasOurFinalizer = true
			break
		}
	}

	if !hasOurFinalizer {
		klog.V(4).Infof("PVC %s/%s does not have our finalizer, skipping cleanup", pvc.Namespace, pvc.Name)
		return nil
	}

	// Parse volume ID to get filesystem information
	volumeID := pvc.Annotations["volume.kubernetes.io/storage-provisioner"]
	if volumeID == "" && pvc.Spec.VolumeName != "" {
		// Try to get volume ID from PV
		pv, err := f.client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			klog.Warningf("Failed to get PV %s for PVC %s/%s: %v", pvc.Spec.VolumeName, pvc.Namespace, pvc.Name, err)
		} else if pv.Spec.CSI != nil {
			volumeID = pv.Spec.CSI.VolumeHandle
		}
	}

	// Remove PVC from tracker if volume ID is available
	if volumeID != "" {
		isNamespaceEmpty, err := f.pvcTracker.RemovePVC(ctx, pvc.Namespace, pvc.Name)
		if err != nil {
			klog.Errorf("Failed to remove PVC %s/%s from tracker: %v", pvc.Namespace, pvc.Name, err)
			// Continue with cleanup even if tracker removal fails
		} else {
			klog.V(2).Infof("Removed PVC %s/%s from tracker", pvc.Namespace, pvc.Name)

			// Check if this was the last PVC in the namespace
			if isNamespaceEmpty {
				// This was the last PVC, trigger filesystem cleanup
				klog.V(2).Infof("Last PVC in namespace %s, triggering filesystem cleanup", pvc.Namespace)
				err = f.fileSystemMgr.DeleteFileSystemForNamespace(ctx, pvc.Namespace, volumeID)
				if err != nil {
					klog.Errorf("Failed to delete filesystem for namespace %s: %v", pvc.Namespace, err)
					// Don't return error here - we want to remove the finalizer even if cleanup fails
					// to avoid blocking PVC deletion
				}
			}
		}
	}

	// Remove our finalizer to allow PVC deletion
	err := f.RemoveFinalizer(ctx, pvc)
	if err != nil {
		return NewEFSNSError(ErrFinalizerOperationFailed, "processPVCFinalization", pvc.Namespace,
			fmt.Sprintf("failed to remove finalizer from PVC %s: %v", pvc.Name, err), err)
	}

	klog.V(2).Infof("Successfully processed finalization for PVC %s/%s", pvc.Namespace, pvc.Name)
	return nil
}

// processNamespaceFinalization handles namespace finalizer cleanup
func (f *ConfigMapFinalizerManager) processNamespaceFinalization(ctx context.Context, namespace *corev1.Namespace) error {
	klog.V(4).Infof("Processing namespace finalization for %s", namespace.Name)

	// Check if our finalizer is present
	hasOurFinalizer := false
	for _, finalizer := range namespace.Finalizers {
		if finalizer == EFSNSNamespaceFinalizerName {
			hasOurFinalizer = true
			break
		}
	}

	if !hasOurFinalizer {
		klog.V(4).Infof("Namespace %s does not have our finalizer, skipping cleanup", namespace.Name)
		return nil
	}

	// Check if there are any remaining efs-ns PVCs in the namespace
	pvcCount, err := f.pvcTracker.GetPVCCount(ctx, namespace.Name)
	if err != nil {
		klog.Errorf("Failed to get PVC count for namespace %s: %v", namespace.Name, err)
	}

	if pvcCount > 0 {
		klog.V(4).Infof("Namespace %s still has %d PVCs, keeping finalizer", namespace.Name, pvcCount)
		return NewEFSNSError(ErrFinalizerOperationFailed, "processNamespaceFinalization", namespace.Name,
			fmt.Sprintf("namespace still has %d PVCs, cannot remove finalizer", pvcCount), nil)
	}

	// No PVCs remain, trigger filesystem cleanup
	klog.V(2).Infof("No PVCs remain in namespace %s, triggering filesystem cleanup", namespace.Name)

	// Get filesystem info to construct a dummy volume ID for cleanup
	fsInfo, err := f.fileSystemMgr.GetFileSystemInfo(ctx, namespace.Name)
	if err != nil && !IsEFSNSError(err, ErrFileSystemNotFound) {
		klog.Errorf("Failed to get filesystem info for namespace %s: %v", namespace.Name, err)
	}

	if fsInfo != nil {
		// Construct volume ID for cleanup
		volumeID := &EFSNSVolumeID{
			Namespace:    namespace.Name,
			FileSystemID: fsInfo.FileSystemID,
			ClusterID:    f.clusterID,
		}

		err = f.fileSystemMgr.DeleteFileSystemForNamespace(ctx, namespace.Name, volumeID.String())
		if err != nil {
			klog.Errorf("Failed to delete filesystem for namespace %s: %v", namespace.Name, err)
			// Don't return error here - we want to remove the finalizer even if cleanup fails
		}
	}

	// Remove our finalizer to allow namespace deletion
	err = f.RemoveNamespaceFinalizer(ctx, namespace.Name)
	if err != nil {
		return NewEFSNSError(ErrFinalizerOperationFailed, "processNamespaceFinalization", namespace.Name,
			fmt.Sprintf("failed to remove finalizer from namespace: %v", err), err)
	}

	klog.V(2).Infof("Successfully processed finalization for namespace %s", namespace.Name)
	return nil
}

// retryOperation executes an operation with retry logic
func (f *ConfigMapFinalizerManager) retryOperation(ctx context.Context, operation func() error) error {
	var lastErr error

	for attempt := 0; attempt < MaxRetryAttempts; attempt++ {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return NewEFSNSError(ErrTimeout, "retryOperation", "",
				"context canceled during retry operation", ctx.Err())
		default:
		}

		lastErr = operation()
		if lastErr == nil {
			return nil
		}

		// Check if this is a conflict error (optimistic concurrency)
		if errors.IsConflict(lastErr) || IsEFSNSError(lastErr, ErrKubernetesAPIFailed) {
			if attempt < MaxRetryAttempts-1 {
				klog.V(4).Infof("Operation failed with retryable error, attempt %d/%d: %v",
					attempt+1, MaxRetryAttempts, lastErr)

				// Wait before retrying
				select {
				case <-ctx.Done():
					return NewEFSNSError(ErrTimeout, "retryOperation", "",
						"context canceled during retry wait", ctx.Err())
				case <-time.After(RetryInterval):
				}
				continue
			}
		} else {
			// Non-retryable error
			break
		}
	}

	return NewEFSNSError(ErrFinalizerOperationFailed, "retryOperation", "",
		fmt.Sprintf("operation failed after %d attempts: %v", MaxRetryAttempts, lastErr), lastErr)
}
