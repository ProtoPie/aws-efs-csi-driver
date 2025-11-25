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
	"encoding/json"
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
	// PVCTrackerConfigMapPrefix is the prefix for ConfigMap names used for PVC tracking
	PVCTrackerConfigMapPrefix = "efs-ns-pvc-tracker"

	// PVCTrackerConfigMapKey is the key used in ConfigMap data to store PVC mapping
	PVCTrackerConfigMapKey = "pvc-mappings"

	// PVCTrackerNamespace is the namespace where ConfigMaps are stored
	// Using kube-system namespace for system-level components
	PVCTrackerNamespace = "kube-system"

	// PVCTrackerLabelKey identifies ConfigMaps managed by PVCTracker
	PVCTrackerLabelKey = "app.kubernetes.io/managed-by"

	// PVCTrackerLabelValue is the value for the managed-by label
	PVCTrackerLabelValue = "efs-csi-driver"

	// PVCTrackerComponentLabel identifies the specific component
	PVCTrackerComponentLabel = "app.kubernetes.io/component"

	// PVCTrackerComponentValue is the component name
	PVCTrackerComponentValue = "efs-ns-pvc-tracker"
)

// PVCTracker interface defines methods for tracking PVC to filesystem mappings
type PVCTracker interface {
	// AddPVC tracks a new PVC in the namespace
	AddPVC(ctx context.Context, namespace, pvcName, volumeID string) error

	// RemovePVC removes PVC tracking and returns true if namespace is now empty
	RemovePVC(ctx context.Context, namespace, pvcName string) (isNamespaceEmpty bool, err error)

	// GetPVCCount returns number of PVCs in namespace
	GetPVCCount(ctx context.Context, namespace string) (int32, error)

	// ListPVCsInNamespace returns all PVC names in namespace
	ListPVCsInNamespace(ctx context.Context, namespace string) ([]string, error)

	// SyncWithCluster synchronizes tracking data with actual cluster state
	SyncWithCluster(ctx context.Context) error
}

// ConfigMapPVCTracker implements PVCTracker using Kubernetes ConfigMaps for persistence
type ConfigMapPVCTracker struct {
	client    kubernetes.Interface
	clusterID string
	mutex     sync.RWMutex
}

// NewConfigMapPVCTracker creates a new ConfigMap-based PVC tracker
func NewConfigMapPVCTracker(client kubernetes.Interface, clusterID string) *ConfigMapPVCTracker {
	return &ConfigMapPVCTracker{
		client:    client,
		clusterID: clusterID,
	}
}

// AddPVC tracks a new PVC in the namespace
func (t *ConfigMapPVCTracker) AddPVC(ctx context.Context, namespace, pvcName, volumeID string) error {
	if namespace == "" {
		return NewEFSNSError(ErrInvalidParameter, "AddPVC", namespace, "namespace cannot be empty", nil)
	}
	if pvcName == "" {
		return NewEFSNSError(ErrInvalidParameter, "AddPVC", namespace, "PVC name cannot be empty", nil)
	}
	if volumeID == "" {
		return NewEFSNSError(ErrInvalidParameter, "AddPVC", namespace, "volume ID cannot be empty", nil)
	}

	// Parse volume ID to extract filesystem ID
	volID, err := ParseEFSNSVolumeID(volumeID)
	if err != nil {
		return NewEFSNSError(ErrTrackerOperationFailed, "AddPVC", namespace,
			fmt.Sprintf("failed to parse volume ID %s", volumeID), err)
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()

	klog.V(4).InfoS("Adding PVC to tracker",
		"namespace", namespace, "pvcName", pvcName, "volumeID", volumeID)

	configMapName := t.getConfigMapName(namespace)

	// Get or create ConfigMap
	configMap, err := t.client.CoreV1().ConfigMaps(PVCTrackerNamespace).Get(ctx, configMapName, metav1.GetOptions{})
	isNewConfigMap := false
	if err != nil {
		if errors.IsNotFound(err) {
			// Create new ConfigMap
			isNewConfigMap = true
			configMap = &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      configMapName,
					Namespace: PVCTrackerNamespace,
					Labels: map[string]string{
						PVCTrackerLabelKey:                    PVCTrackerLabelValue,
						PVCTrackerComponentLabel:              PVCTrackerComponentValue,
						"efs-ns.csi.aws.com/target-namespace": namespace,
					},
				},
				Data: map[string]string{
					PVCTrackerConfigMapKey: "{}",
				},
			}
		} else {
			return NewEFSNSError(ErrTrackerOperationFailed, "AddPVC", namespace,
				fmt.Sprintf("failed to get ConfigMap %s", configMapName), err)
		}
	}

	// Parse existing mappings
	mappings, err := t.parseMappings(configMap.Data[PVCTrackerConfigMapKey])
	if err != nil {
		return NewEFSNSError(ErrTrackerOperationFailed, "AddPVC", namespace,
			"failed to parse existing mappings", err)
	}

	// Add new mapping
	mappings[pvcName] = &PVCMappingEntry{
		Namespace:    namespace,
		PVCName:      pvcName,
		VolumeID:     volumeID,
		FileSystemID: volID.FileSystemID,
		CreatedAt:    time.Now(),
	}

	// Serialize mappings back to ConfigMap
	data, err := t.serializeMappings(mappings)
	if err != nil {
		return NewEFSNSError(ErrTrackerOperationFailed, "AddPVC", namespace,
			"failed to serialize mappings", err)
	}

	configMap.Data[PVCTrackerConfigMapKey] = data

	// Create or update ConfigMap
	if isNewConfigMap {
		_, err = t.client.CoreV1().ConfigMaps(PVCTrackerNamespace).Create(ctx, configMap, metav1.CreateOptions{})
		if err != nil {
			return NewEFSNSError(ErrTrackerOperationFailed, "AddPVC", namespace,
				fmt.Sprintf("failed to create ConfigMap %s", configMapName), err)
		}
	} else {
		_, err = t.client.CoreV1().ConfigMaps(PVCTrackerNamespace).Update(ctx, configMap, metav1.UpdateOptions{})
		if err != nil {
			return NewEFSNSError(ErrTrackerOperationFailed, "AddPVC", namespace,
				fmt.Sprintf("failed to update ConfigMap %s", configMapName), err)
		}
	}

	klog.V(4).InfoS("Successfully added PVC to tracker",
		"namespace", namespace, "pvcName", pvcName, "filesystemID", volID.FileSystemID)

	return nil
}

// RemovePVC removes PVC tracking and returns true if namespace is now empty
func (t *ConfigMapPVCTracker) RemovePVC(ctx context.Context, namespace, pvcName string) (bool, error) {
	if namespace == "" {
		return false, NewEFSNSError(ErrInvalidParameter, "RemovePVC", namespace, "namespace cannot be empty", nil)
	}
	if pvcName == "" {
		return false, NewEFSNSError(ErrInvalidParameter, "RemovePVC", namespace, "PVC name cannot be empty", nil)
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()

	klog.V(4).InfoS("Removing PVC from tracker",
		"namespace", namespace, "pvcName", pvcName)

	configMapName := t.getConfigMapName(namespace)

	// Get ConfigMap
	configMap, err := t.client.CoreV1().ConfigMaps(PVCTrackerNamespace).Get(ctx, configMapName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			// ConfigMap doesn't exist, so PVC is not tracked - namespace is empty
			klog.V(4).InfoS("ConfigMap not found, treating as empty namespace",
				"namespace", namespace, "pvcName", pvcName)
			return true, nil
		}
		return false, NewEFSNSError(ErrTrackerOperationFailed, "RemovePVC", namespace,
			fmt.Sprintf("failed to get ConfigMap %s", configMapName), err)
	}

	// Parse existing mappings
	mappings, err := t.parseMappings(configMap.Data[PVCTrackerConfigMapKey])
	if err != nil {
		return false, NewEFSNSError(ErrTrackerOperationFailed, "RemovePVC", namespace,
			"failed to parse existing mappings", err)
	}

	// Remove the PVC mapping
	delete(mappings, pvcName)

	isEmpty := len(mappings) == 0

	if isEmpty {
		// Delete the entire ConfigMap if no more PVCs
		err = t.client.CoreV1().ConfigMaps(PVCTrackerNamespace).Delete(ctx, configMapName, metav1.DeleteOptions{})
		if err != nil && !errors.IsNotFound(err) {
			return false, NewEFSNSError(ErrTrackerOperationFailed, "RemovePVC", namespace,
				fmt.Sprintf("failed to delete ConfigMap %s", configMapName), err)
		}
		klog.V(4).InfoS("Deleted ConfigMap as namespace is now empty",
			"namespace", namespace, "configMapName", configMapName)
	} else {
		// Update ConfigMap with remaining mappings
		data, err := t.serializeMappings(mappings)
		if err != nil {
			return false, NewEFSNSError(ErrTrackerOperationFailed, "RemovePVC", namespace,
				"failed to serialize mappings", err)
		}

		configMap.Data[PVCTrackerConfigMapKey] = data
		_, err = t.client.CoreV1().ConfigMaps(PVCTrackerNamespace).Update(ctx, configMap, metav1.UpdateOptions{})
		if err != nil {
			return false, NewEFSNSError(ErrTrackerOperationFailed, "RemovePVC", namespace,
				fmt.Sprintf("failed to update ConfigMap %s", configMapName), err)
		}
		klog.V(4).InfoS("Updated ConfigMap with remaining mappings",
			"namespace", namespace, "remainingPVCs", len(mappings))
	}

	klog.V(4).InfoS("Successfully removed PVC from tracker",
		"namespace", namespace, "pvcName", pvcName, "isNamespaceEmpty", isEmpty)

	return isEmpty, nil
}

// GetPVCCount returns number of PVCs in namespace
func (t *ConfigMapPVCTracker) GetPVCCount(ctx context.Context, namespace string) (int32, error) {
	if namespace == "" {
		return 0, NewEFSNSError(ErrInvalidParameter, "GetPVCCount", namespace, "namespace cannot be empty", nil)
	}

	t.mutex.RLock()
	defer t.mutex.RUnlock()

	configMapName := t.getConfigMapName(namespace)

	// Get ConfigMap
	configMap, err := t.client.CoreV1().ConfigMaps(PVCTrackerNamespace).Get(ctx, configMapName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			// ConfigMap doesn't exist, so no PVCs
			return 0, nil
		}
		return 0, NewEFSNSError(ErrTrackerOperationFailed, "GetPVCCount", namespace,
			fmt.Sprintf("failed to get ConfigMap %s", configMapName), err)
	}

	// Parse existing mappings
	mappings, err := t.parseMappings(configMap.Data[PVCTrackerConfigMapKey])
	if err != nil {
		return 0, NewEFSNSError(ErrTrackerOperationFailed, "GetPVCCount", namespace,
			"failed to parse existing mappings", err)
	}

	count := int32(len(mappings))
	klog.V(4).InfoS("Retrieved PVC count", "namespace", namespace, "count", count)

	return count, nil
}

// ListPVCsInNamespace returns all PVC names in namespace
func (t *ConfigMapPVCTracker) ListPVCsInNamespace(ctx context.Context, namespace string) ([]string, error) {
	if namespace == "" {
		return nil, NewEFSNSError(ErrInvalidParameter, "ListPVCsInNamespace", namespace, "namespace cannot be empty", nil)
	}

	t.mutex.RLock()
	defer t.mutex.RUnlock()

	configMapName := t.getConfigMapName(namespace)

	// Get ConfigMap
	configMap, err := t.client.CoreV1().ConfigMaps(PVCTrackerNamespace).Get(ctx, configMapName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			// ConfigMap doesn't exist, so no PVCs
			return []string{}, nil
		}
		return nil, NewEFSNSError(ErrTrackerOperationFailed, "ListPVCsInNamespace", namespace,
			fmt.Sprintf("failed to get ConfigMap %s", configMapName), err)
	}

	// Parse existing mappings
	mappings, err := t.parseMappings(configMap.Data[PVCTrackerConfigMapKey])
	if err != nil {
		return nil, NewEFSNSError(ErrTrackerOperationFailed, "ListPVCsInNamespace", namespace,
			"failed to parse existing mappings", err)
	}

	pvcNames := make([]string, 0, len(mappings))
	for pvcName := range mappings {
		pvcNames = append(pvcNames, pvcName)
	}

	klog.V(4).InfoS("Listed PVCs in namespace", "namespace", namespace, "pvcs", pvcNames)

	return pvcNames, nil
}

// SyncWithCluster synchronizes tracking data with actual cluster state
func (t *ConfigMapPVCTracker) SyncWithCluster(ctx context.Context) error {
	klog.V(4).InfoS("Starting cluster synchronization")

	t.mutex.Lock()
	defer t.mutex.Unlock()

	// Get all tracker ConfigMaps
	selector := fmt.Sprintf("%s=%s,%s=%s",
		PVCTrackerLabelKey, PVCTrackerLabelValue,
		PVCTrackerComponentLabel, PVCTrackerComponentValue)

	configMapList, err := t.client.CoreV1().ConfigMaps(PVCTrackerNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: selector,
	})
	if err != nil {
		return NewEFSNSError(ErrTrackerOperationFailed, "SyncWithCluster", "",
			"failed to list tracker ConfigMaps", err)
	}

	syncErrors := []error{}
	for _, configMap := range configMapList.Items {
		targetNamespace := configMap.Labels["efs-ns.csi.aws.com/target-namespace"]
		if targetNamespace == "" {
			klog.Warning("ConfigMap missing target namespace label", "configMap", configMap.Name)
			continue
		}

		if err := t.syncNamespace(ctx, targetNamespace, &configMap); err != nil {
			klog.ErrorS(err, "Failed to sync namespace", "namespace", targetNamespace)
			syncErrors = append(syncErrors, err)
		}
	}

	if len(syncErrors) > 0 {
		return NewEFSNSError(ErrTrackerOperationFailed, "SyncWithCluster", "",
			fmt.Sprintf("failed to sync %d namespaces", len(syncErrors)), syncErrors[0])
	}

	klog.V(4).InfoS("Completed cluster synchronization", "configMapsProcessed", len(configMapList.Items))
	return nil
}

// syncNamespace synchronizes a single namespace's tracking data with cluster state
func (t *ConfigMapPVCTracker) syncNamespace(ctx context.Context, namespace string, configMap *corev1.ConfigMap) error {
	// Parse existing mappings
	mappings, err := t.parseMappings(configMap.Data[PVCTrackerConfigMapKey])
	if err != nil {
		return fmt.Errorf("failed to parse mappings for namespace %s: %w", namespace, err)
	}

	// Get actual PVCs in the namespace
	pvcList, err := t.client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list PVCs in namespace %s: %w", namespace, err)
	}

	// Create a set of actual PVCs using efs-ns provisioner
	actualPVCs := make(map[string]bool)
	for _, pvc := range pvcList.Items {
		// Check if this PVC uses efs-ns provisioning mode
		if pvc.Spec.StorageClassName != nil {
			storageClass, err := t.client.StorageV1().StorageClasses().Get(ctx, *pvc.Spec.StorageClassName, metav1.GetOptions{})
			if err != nil {
				klog.V(4).InfoS("Could not get storage class", "storageClass", *pvc.Spec.StorageClassName, "pvc", pvc.Name, "error", err)
				continue
			}

			if mode, exists := storageClass.Parameters["provisioningMode"]; exists && mode == EFSNSProvisioningMode {
				actualPVCs[pvc.Name] = true
			}
		}
	}

	// Remove mappings for PVCs that no longer exist
	updated := false
	for pvcName := range mappings {
		if !actualPVCs[pvcName] {
			delete(mappings, pvcName)
			updated = true
			klog.V(4).InfoS("Removed orphaned PVC mapping", "namespace", namespace, "pvc", pvcName)
		}
	}

	// Update ConfigMap if changes were made
	if updated {
		if len(mappings) == 0 {
			// Delete ConfigMap if no mappings left
			err = t.client.CoreV1().ConfigMaps(PVCTrackerNamespace).Delete(ctx, configMap.Name, metav1.DeleteOptions{})
			if err != nil && !errors.IsNotFound(err) {
				return fmt.Errorf("failed to delete empty ConfigMap %s: %w", configMap.Name, err)
			}
			klog.V(4).InfoS("Deleted empty ConfigMap during sync", "namespace", namespace, "configMap", configMap.Name)
		} else {
			// Update ConfigMap with cleaned mappings
			data, err := t.serializeMappings(mappings)
			if err != nil {
				return fmt.Errorf("failed to serialize cleaned mappings: %w", err)
			}

			configMap.Data[PVCTrackerConfigMapKey] = data
			_, err = t.client.CoreV1().ConfigMaps(PVCTrackerNamespace).Update(ctx, configMap, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("failed to update ConfigMap %s: %w", configMap.Name, err)
			}
			klog.V(4).InfoS("Updated ConfigMap during sync", "namespace", namespace, "remainingMappings", len(mappings))
		}
	}

	return nil
}

// getConfigMapName generates the ConfigMap name for a namespace
func (t *ConfigMapPVCTracker) getConfigMapName(namespace string) string {
	return fmt.Sprintf("%s-%s-%s", PVCTrackerConfigMapPrefix, namespace, t.clusterID)
}

// parseMappings parses JSON string into PVC mappings
func (t *ConfigMapPVCTracker) parseMappings(data string) (map[string]*PVCMappingEntry, error) {
	if data == "" || data == "{}" {
		return make(map[string]*PVCMappingEntry), nil
	}

	var mappings map[string]*PVCMappingEntry
	if err := json.Unmarshal([]byte(data), &mappings); err != nil {
		return nil, fmt.Errorf("failed to unmarshal mappings: %w", err)
	}

	if mappings == nil {
		return make(map[string]*PVCMappingEntry), nil
	}

	return mappings, nil
}

// serializeMappings serializes PVC mappings to JSON string
func (t *ConfigMapPVCTracker) serializeMappings(mappings map[string]*PVCMappingEntry) (string, error) {
	if len(mappings) == 0 {
		return "{}", nil
	}

	data, err := json.Marshal(mappings)
	if err != nil {
		return "", fmt.Errorf("failed to marshal mappings: %w", err)
	}

	return string(data), nil
}
