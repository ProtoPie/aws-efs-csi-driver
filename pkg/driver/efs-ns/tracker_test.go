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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPVCTrackerBasicOperations(t *testing.T) {
	client := fake.NewSimpleClientset()
	clusterID := "test-cluster"
	namespace := "test-ns"
	pvcName := "test-pvc"
	volumeID := "efs-ns::test-ns::fs-12345678::test-cluster"

	tracker := NewConfigMapPVCTracker(client, clusterID)

	// Test AddPVC
	err := tracker.AddPVC(context.Background(), namespace, pvcName, volumeID)
	if err != nil {
		t.Fatalf("AddPVC failed: %v", err)
	}

	// Test GetPVCCount
	count, err := tracker.GetPVCCount(context.Background(), namespace)
	if err != nil {
		t.Fatalf("GetPVCCount failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected count=1, got %d", count)
	}

	// Test ListPVCsInNamespace
	pvcs, err := tracker.ListPVCsInNamespace(context.Background(), namespace)
	if err != nil {
		t.Fatalf("ListPVCsInNamespace failed: %v", err)
	}
	if len(pvcs) != 1 || pvcs[0] != pvcName {
		t.Errorf("Expected [%s], got %v", pvcName, pvcs)
	}

	// Test RemovePVC
	isEmpty, err := tracker.RemovePVC(context.Background(), namespace, pvcName)
	if err != nil {
		t.Fatalf("RemovePVC failed: %v", err)
	}
	if !isEmpty {
		t.Error("Expected namespace to be empty after removing last PVC")
	}

	// Verify count is now 0
	count, err = tracker.GetPVCCount(context.Background(), namespace)
	if err != nil {
		t.Fatalf("GetPVCCount failed: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected count=0 after removal, got %d", count)
	}
}

func TestPVCTrackerErrorHandling(t *testing.T) {
	client := fake.NewSimpleClientset()
	tracker := NewConfigMapPVCTracker(client, "test-cluster")

	// Test empty namespace
	err := tracker.AddPVC(context.Background(), "", "pvc", "volume-id")
	if err == nil {
		t.Error("Expected error for empty namespace")
	}

	// Test empty PVC name
	err = tracker.AddPVC(context.Background(), "ns", "", "volume-id")
	if err == nil {
		t.Error("Expected error for empty PVC name")
	}

	// Test empty volume ID
	err = tracker.AddPVC(context.Background(), "ns", "pvc", "")
	if err == nil {
		t.Error("Expected error for empty volume ID")
	}

	// Test invalid volume ID
	err = tracker.AddPVC(context.Background(), "ns", "pvc", "invalid-id")
	if err == nil {
		t.Error("Expected error for invalid volume ID")
	}
}

func TestPVCTrackerMultiplePVCs(t *testing.T) {
	client := fake.NewSimpleClientset()
	tracker := NewConfigMapPVCTracker(client, "test-cluster")
	namespace := "test-ns"

	// Add multiple PVCs
	pvcs := []string{"pvc1", "pvc2", "pvc3"}
	for i, pvcName := range pvcs {
		volumeID := "efs-ns::test-ns::fs-" + string(rune('1'+i)) + "::test-cluster"
		err := tracker.AddPVC(context.Background(), namespace, pvcName, volumeID)
		if err != nil {
			t.Fatalf("Failed to add PVC %s: %v", pvcName, err)
		}
	}

	// Check count
	count, err := tracker.GetPVCCount(context.Background(), namespace)
	if err != nil {
		t.Fatalf("GetPVCCount failed: %v", err)
	}
	if count != int32(len(pvcs)) {
		t.Errorf("Expected count=%d, got %d", len(pvcs), count)
	}

	// Remove one PVC (should not be empty)
	isEmpty, err := tracker.RemovePVC(context.Background(), namespace, "pvc1")
	if err != nil {
		t.Fatalf("RemovePVC failed: %v", err)
	}
	if isEmpty {
		t.Error("Expected namespace to not be empty after removing one PVC")
	}

	// Check count decreased
	count, err = tracker.GetPVCCount(context.Background(), namespace)
	if err != nil {
		t.Fatalf("GetPVCCount failed: %v", err)
	}
	if count != int32(len(pvcs)-1) {
		t.Errorf("Expected count=%d, got %d", len(pvcs)-1, count)
	}
}

func TestPVCTrackerSyncWithCluster(t *testing.T) {
	client := fake.NewSimpleClientset()
	tracker := NewConfigMapPVCTracker(client, "test-cluster")

	// Create a ConfigMap with tracked PVCs that don't exist in cluster
	namespace := "test-ns"
	configMapName := "efs-ns-pvc-tracker-test-ns-test-cluster"
	configMap := &corev1.ConfigMap{
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
			PVCTrackerConfigMapKey: `{"orphaned-pvc":{"namespace":"test-ns","pvcName":"orphaned-pvc","volumeId":"efs-ns::test-ns::fs-12345678::test-cluster","fileSystemId":"fs-12345678","createdAt":"2024-01-01T00:00:00Z"}}`,
		},
	}
	_, err := client.CoreV1().ConfigMaps(PVCTrackerNamespace).Create(context.Background(), configMap, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create test ConfigMap: %v", err)
	}

	// Run sync - should clean up orphaned PVC
	err = tracker.SyncWithCluster(context.Background())
	if err != nil {
		t.Fatalf("SyncWithCluster failed: %v", err)
	}

	// Verify ConfigMap was deleted since no PVCs exist
	count, err := tracker.GetPVCCount(context.Background(), namespace)
	if err != nil {
		t.Fatalf("GetPVCCount failed: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected count=0 after sync cleanup, got %d", count)
	}
}
