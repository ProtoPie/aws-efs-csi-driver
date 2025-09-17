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

package driver

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

// mockEventRecorder is a mock implementation of EventRecorder for testing
type mockEventRecorder struct {
	events []mockEvent
}

type mockEvent struct {
	eventType string
	reason    string
	message   string
	object    runtime.Object
}

func (m *mockEventRecorder) Event(object runtime.Object, eventtype, reason, message string) {
	m.events = append(m.events, mockEvent{
		eventType: eventtype,
		reason:    reason,
		message:   message,
		object:    object,
	})
}

func (m *mockEventRecorder) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	// For simplicity, just call Event
	m.Event(object, eventtype, reason, messageFmt)
}

func (m *mockEventRecorder) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	// For simplicity, just call Event
	m.Event(object, eventtype, reason, messageFmt)
}

func TestNewVolumeStatusTracker(t *testing.T) {
	k8sClient := fake.NewSimpleClientset()
	eventRecorder := &mockEventRecorder{}

	tracker := NewVolumeStatusTracker(eventRecorder, k8sClient)

	if tracker == nil {
		t.Fatal("Expected non-nil VolumeStatusTracker")
	}

	if tracker.eventRecorder != eventRecorder {
		t.Error("Event recorder not set correctly")
	}

	if tracker.k8sClient != k8sClient {
		t.Error("Kubernetes client not set correctly")
	}

	if tracker.statuses == nil {
		t.Error("Statuses map not initialized")
	}
}

func TestVolumeStatusTracker_StartVolumeProvisioning(t *testing.T) {
	k8sClient := fake.NewSimpleClientset()
	eventRecorder := &mockEventRecorder{}
	tracker := NewVolumeStatusTracker(eventRecorder, k8sClient)

	volumeID := "test-volume-1"
	pvcName := "test-pvc"
	namespace := "test-namespace"

	// Create a PVC for the event recorder to find
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: namespace,
		},
	}
	_, err := k8sClient.CoreV1().PersistentVolumeClaims(namespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create test PVC: %v", err)
	}

	tracker.StartVolumeProvisioning(volumeID, pvcName, namespace)

	// Verify status was created
	status, exists := tracker.GetVolumeStatus(volumeID)
	if !exists {
		t.Fatal("Volume status not found after starting provisioning")
	}

	if status.VolumeID != volumeID {
		t.Errorf("Expected volume ID %s, got %s", volumeID, status.VolumeID)
	}

	if status.PVCName != pvcName {
		t.Errorf("Expected PVC name %s, got %s", pvcName, status.PVCName)
	}

	if status.Namespace != namespace {
		t.Errorf("Expected namespace %s, got %s", namespace, status.Namespace)
	}

	if status.Phase != VolumePhaseInitializing {
		t.Errorf("Expected phase %s, got %s", VolumePhaseInitializing, status.Phase)
	}

	if status.ProgressPercentage != 0 {
		t.Errorf("Expected progress 0%%, got %d%%", status.ProgressPercentage)
	}

	// Verify event was emitted
	if len(eventRecorder.events) != 1 {
		t.Errorf("Expected 1 event, got %d", len(eventRecorder.events))
	} else {
		event := eventRecorder.events[0]
		if event.eventType != corev1.EventTypeNormal {
			t.Errorf("Expected event type %s, got %s", corev1.EventTypeNormal, event.eventType)
		}
		if event.reason != EventReasonProvisioning {
			t.Errorf("Expected reason %s, got %s", EventReasonProvisioning, event.reason)
		}
	}
}

func TestVolumeStatusTracker_UpdateVolumePhase(t *testing.T) {
	k8sClient := fake.NewSimpleClientset()
	eventRecorder := &mockEventRecorder{}
	tracker := NewVolumeStatusTracker(eventRecorder, k8sClient)

	volumeID := "test-volume-1"
	pvcName := "test-pvc"
	namespace := "test-namespace"

	// Create a PVC for the event recorder to find
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: namespace,
		},
	}
	_, err := k8sClient.CoreV1().PersistentVolumeClaims(namespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create test PVC: %v", err)
	}

	// Start provisioning first
	tracker.StartVolumeProvisioning(volumeID, pvcName, namespace)

	// Update phase
	newPhase := VolumePhaseEFSCreating
	newMessage := "Creating EFS filesystem"
	newProgress := 25

	tracker.UpdateVolumePhase(volumeID, newPhase, newMessage, newProgress)

	// Verify status was updated
	status, exists := tracker.GetVolumeStatus(volumeID)
	if !exists {
		t.Fatal("Volume status not found after updating phase")
	}

	if status.Phase != newPhase {
		t.Errorf("Expected phase %s, got %s", newPhase, status.Phase)
	}

	if status.Message != newMessage {
		t.Errorf("Expected message %s, got %s", newMessage, status.Message)
	}

	if status.ProgressPercentage != newProgress {
		t.Errorf("Expected progress %d%%, got %d%%", newProgress, status.ProgressPercentage)
	}

	// Verify events were emitted (start + update)
	if len(eventRecorder.events) != 2 {
		t.Errorf("Expected 2 events, got %d", len(eventRecorder.events))
	}
}

func TestVolumeStatusTracker_RecordVolumeOperation(t *testing.T) {
	tracker := NewVolumeStatusTracker(nil, fake.NewSimpleClientset())

	volumeID := "test-volume-1"
	pvcName := "test-pvc"
	namespace := "test-namespace"

	// Start provisioning first
	tracker.StartVolumeProvisioning(volumeID, pvcName, namespace)

	// Record an operation
	operation := "create_efs"
	status := "started"
	message := "Creating EFS filesystem"
	err := errors.New("test error")

	tracker.RecordVolumeOperation(volumeID, operation, status, message, err)

	// Verify operation was recorded
	volumeStatus, exists := tracker.GetVolumeStatus(volumeID)
	if !exists {
		t.Fatal("Volume status not found after recording operation")
	}

	if len(volumeStatus.Operations) != 1 {
		t.Errorf("Expected 1 operation, got %d", len(volumeStatus.Operations))
	} else {
		op := volumeStatus.Operations[0]
		if op.Operation != operation {
			t.Errorf("Expected operation %s, got %s", operation, op.Operation)
		}
		if op.Status != status {
			t.Errorf("Expected status %s, got %s", status, op.Status)
		}
		if op.Message != message {
			t.Errorf("Expected message %s, got %s", message, op.Message)
		}
		if op.Error != err {
			t.Errorf("Expected error %v, got %v", err, op.Error)
		}
		if op.EndTime != nil {
			t.Error("Expected EndTime to be nil for non-completed operation")
		}
	}

	// Record completion
	tracker.RecordVolumeOperation(volumeID, operation, "completed", "EFS created", nil)

	// Verify completion was recorded
	volumeStatus, _ = tracker.GetVolumeStatus(volumeID)
	if len(volumeStatus.Operations) != 2 {
		t.Errorf("Expected 2 operations, got %d", len(volumeStatus.Operations))
	} else {
		completedOp := volumeStatus.Operations[1]
		if completedOp.EndTime == nil {
			t.Error("Expected EndTime to be set for completed operation")
		}
	}
}

func TestVolumeStatusTracker_CompleteVolumeProvisioning(t *testing.T) {
	k8sClient := fake.NewSimpleClientset()
	eventRecorder := &mockEventRecorder{}
	tracker := NewVolumeStatusTracker(eventRecorder, k8sClient)

	volumeID := "test-volume-1"
	pvcName := "test-pvc"
	namespace := "test-namespace"
	efsID := "fs-12345"
	accessPointID := "fsap-67890"

	// Create a PVC for the event recorder to find
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: namespace,
		},
	}
	_, err := k8sClient.CoreV1().PersistentVolumeClaims(namespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create test PVC: %v", err)
	}

	// Start provisioning first
	tracker.StartVolumeProvisioning(volumeID, pvcName, namespace)

	// Complete provisioning
	tracker.CompleteVolumeProvisioning(volumeID, efsID, accessPointID)

	// Verify status was completed
	status, exists := tracker.GetVolumeStatus(volumeID)
	if !exists {
		t.Fatal("Volume status not found after completing provisioning")
	}

	if status.Phase != VolumePhaseCompleted {
		t.Errorf("Expected phase %s, got %s", VolumePhaseCompleted, status.Phase)
	}

	if status.EFSFileSystemID != efsID {
		t.Errorf("Expected EFS ID %s, got %s", efsID, status.EFSFileSystemID)
	}

	if status.AccessPointID != accessPointID {
		t.Errorf("Expected Access Point ID %s, got %s", accessPointID, status.AccessPointID)
	}

	if status.ProgressPercentage != 100 {
		t.Errorf("Expected progress 100%%, got %d%%", status.ProgressPercentage)
	}

	// Verify completion event was emitted
	if len(eventRecorder.events) < 2 {
		t.Errorf("Expected at least 2 events, got %d", len(eventRecorder.events))
	} else {
		lastEvent := eventRecorder.events[len(eventRecorder.events)-1]
		if lastEvent.reason != EventReasonProvisioned {
			t.Errorf("Expected reason %s, got %s", EventReasonProvisioned, lastEvent.reason)
		}
	}
}

func TestVolumeStatusTracker_FailVolumeProvisioning(t *testing.T) {
	k8sClient := fake.NewSimpleClientset()
	eventRecorder := &mockEventRecorder{}
	tracker := NewVolumeStatusTracker(eventRecorder, k8sClient)

	volumeID := "test-volume-1"
	pvcName := "test-pvc"
	namespace := "test-namespace"
	testError := errors.New("provisioning failed")
	retryCount := 3

	// Create a PVC for the event recorder to find
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: namespace,
		},
	}
	_, err := k8sClient.CoreV1().PersistentVolumeClaims(namespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create test PVC: %v", err)
	}

	// Start provisioning first
	tracker.StartVolumeProvisioning(volumeID, pvcName, namespace)

	// Fail provisioning
	tracker.FailVolumeProvisioning(volumeID, testError, retryCount)

	// Verify status was failed
	status, exists := tracker.GetVolumeStatus(volumeID)
	if !exists {
		t.Fatal("Volume status not found after failing provisioning")
	}

	if status.Phase != VolumePhaseFailed {
		t.Errorf("Expected phase %s, got %s", VolumePhaseFailed, status.Phase)
	}

	if status.Error != testError {
		t.Errorf("Expected error %v, got %v", testError, status.Error)
	}

	if status.RetryCount != retryCount {
		t.Errorf("Expected retry count %d, got %d", retryCount, status.RetryCount)
	}

	// Verify failure event was emitted
	if len(eventRecorder.events) < 2 {
		t.Errorf("Expected at least 2 events, got %d", len(eventRecorder.events))
	} else {
		lastEvent := eventRecorder.events[len(eventRecorder.events)-1]
		if lastEvent.eventType != corev1.EventTypeWarning {
			t.Errorf("Expected event type %s, got %s", corev1.EventTypeWarning, lastEvent.eventType)
		}
		if lastEvent.reason != EventReasonProvisioningFailed {
			t.Errorf("Expected reason %s, got %s", EventReasonProvisioningFailed, lastEvent.reason)
		}
	}
}

func TestVolumeStatusTracker_VolumeDeletion(t *testing.T) {
	k8sClient := fake.NewSimpleClientset()
	eventRecorder := &mockEventRecorder{}
	tracker := NewVolumeStatusTracker(eventRecorder, k8sClient)

	volumeID := "test-volume-1"
	pvcName := "test-pvc"
	namespace := "test-namespace"

	// Create a PVC for the event recorder to find
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: namespace,
		},
	}
	_, err := k8sClient.CoreV1().PersistentVolumeClaims(namespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create test PVC: %v", err)
	}

	// Start deletion
	tracker.StartVolumeDeletion(volumeID, pvcName, namespace)

	// Verify deletion started
	status, exists := tracker.GetVolumeStatus(volumeID)
	if !exists {
		t.Fatal("Volume status not found after starting deletion")
	}

	if status.Phase != VolumePhaseDeleting {
		t.Errorf("Expected phase %s, got %s", VolumePhaseDeleting, status.Phase)
	}

	// Complete deletion
	tracker.CompleteVolumeDeletion(volumeID)

	// Verify status was cleaned up
	_, exists = tracker.GetVolumeStatus(volumeID)
	if exists {
		t.Error("Volume status should be cleaned up after completion")
	}

	// Verify events were emitted
	if len(eventRecorder.events) != 2 {
		t.Errorf("Expected 2 events, got %d", len(eventRecorder.events))
	}
}

func TestVolumeStatusTracker_FailVolumeDeletion(t *testing.T) {
	k8sClient := fake.NewSimpleClientset()
	eventRecorder := &mockEventRecorder{}
	tracker := NewVolumeStatusTracker(eventRecorder, k8sClient)

	volumeID := "test-volume-1"
	pvcName := "test-pvc"
	namespace := "test-namespace"
	testError := errors.New("deletion failed")

	// Create a PVC for the event recorder to find
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: namespace,
		},
	}
	_, err := k8sClient.CoreV1().PersistentVolumeClaims(namespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create test PVC: %v", err)
	}

	// Start deletion
	tracker.StartVolumeDeletion(volumeID, pvcName, namespace)

	// Fail deletion
	tracker.FailVolumeDeletion(volumeID, testError)

	// Verify status shows failure
	status, exists := tracker.GetVolumeStatus(volumeID)
	if !exists {
		t.Fatal("Volume status not found after failing deletion")
	}

	if status.Error != testError {
		t.Errorf("Expected error %v, got %v", testError, status.Error)
	}

	// Verify failure event was emitted
	if len(eventRecorder.events) < 2 {
		t.Errorf("Expected at least 2 events, got %d", len(eventRecorder.events))
	} else {
		lastEvent := eventRecorder.events[len(eventRecorder.events)-1]
		if lastEvent.eventType != corev1.EventTypeWarning {
			t.Errorf("Expected event type %s, got %s", corev1.EventTypeWarning, lastEvent.eventType)
		}
		if lastEvent.reason != EventReasonDeletionFailed {
			t.Errorf("Expected reason %s, got %s", EventReasonDeletionFailed, lastEvent.reason)
		}
	}
}

func TestVolumeStatusTracker_ListVolumeStatuses(t *testing.T) {
	tracker := NewVolumeStatusTracker(nil, fake.NewSimpleClientset())

	// Start multiple provisioning operations
	volumeIDs := []string{"vol-1", "vol-2", "vol-3"}
	for _, volumeID := range volumeIDs {
		tracker.StartVolumeProvisioning(volumeID, "pvc-"+volumeID, "namespace-1")
	}

	// List all statuses
	statuses := tracker.ListVolumeStatuses()

	if len(statuses) != len(volumeIDs) {
		t.Errorf("Expected %d statuses, got %d", len(volumeIDs), len(statuses))
	}

	for _, volumeID := range volumeIDs {
		if _, exists := statuses[volumeID]; !exists {
			t.Errorf("Volume ID %s not found in list", volumeID)
		}
	}
}

func TestVolumeStatusTracker_CleanupCompletedStatuses(t *testing.T) {
	tracker := NewVolumeStatusTracker(nil, fake.NewSimpleClientset())

	// Create some completed and failed statuses with different ages
	oldTime := time.Now().Add(-2 * time.Hour)
	recentTime := time.Now().Add(-30 * time.Minute)

	// Old completed status (should be cleaned up)
	tracker.StartVolumeProvisioning("old-completed", "pvc-1", "ns-1")
	tracker.CompleteVolumeProvisioning("old-completed", "fs-1", "ap-1")
	if status, exists := tracker.statuses["old-completed"]; exists {
		status.LastUpdateTime = oldTime
	}

	// Recent completed status (should be kept)
	tracker.StartVolumeProvisioning("recent-completed", "pvc-2", "ns-1")
	tracker.CompleteVolumeProvisioning("recent-completed", "fs-2", "ap-2")
	if status, exists := tracker.statuses["recent-completed"]; exists {
		status.LastUpdateTime = recentTime
	}

	// Old failed status (should be cleaned up)
	tracker.StartVolumeProvisioning("old-failed", "pvc-3", "ns-1")
	tracker.FailVolumeProvisioning("old-failed", errors.New("test error"), 1)
	if status, exists := tracker.statuses["old-failed"]; exists {
		status.LastUpdateTime = oldTime
	}

	// Recent failed status (should be kept)
	tracker.StartVolumeProvisioning("recent-failed", "pvc-4", "ns-1")
	tracker.FailVolumeProvisioning("recent-failed", errors.New("test error"), 1)
	if status, exists := tracker.statuses["recent-failed"]; exists {
		status.LastUpdateTime = recentTime
	}

	// In-progress status (should be kept)
	tracker.StartVolumeProvisioning("in-progress", "pvc-5", "ns-1")
	if status, exists := tracker.statuses["in-progress"]; exists {
		status.LastUpdateTime = oldTime
	}

	// Verify we have 5 statuses before cleanup
	if len(tracker.statuses) != 5 {
		t.Errorf("Expected 5 statuses before cleanup, got %d", len(tracker.statuses))
	}

	// Clean up statuses older than 1 hour
	tracker.CleanupCompletedStatuses(1 * time.Hour)

	// Verify only recent and in-progress statuses remain
	expectedRemaining := []string{"recent-completed", "recent-failed", "in-progress"}
	if len(tracker.statuses) != len(expectedRemaining) {
		t.Errorf("Expected %d statuses after cleanup, got %d", len(expectedRemaining), len(tracker.statuses))
	}

	for _, volumeID := range expectedRemaining {
		if _, exists := tracker.statuses[volumeID]; !exists {
			t.Errorf("Status %s should not have been cleaned up", volumeID)
		}
	}

	// Verify old statuses were cleaned up
	cleanedUp := []string{"old-completed", "old-failed"}
	for _, volumeID := range cleanedUp {
		if _, exists := tracker.statuses[volumeID]; exists {
			t.Errorf("Status %s should have been cleaned up", volumeID)
		}
	}
}

func TestVolumeStatusTracker_GetNonExistentStatus(t *testing.T) {
	tracker := NewVolumeStatusTracker(nil, fake.NewSimpleClientset())

	status, exists := tracker.GetVolumeStatus("non-existent")
	if exists {
		t.Error("Should not find non-existent volume status")
	}
	if status != nil {
		t.Error("Status should be nil for non-existent volume")
	}
}

func TestVolumeStatusTracker_UpdateNonExistentVolume(t *testing.T) {
	tracker := NewVolumeStatusTracker(nil, fake.NewSimpleClientset())

	// Should not crash when updating non-existent volume
	tracker.UpdateVolumePhase("non-existent", VolumePhaseCompleted, "test", 100)
	tracker.RecordVolumeOperation("non-existent", "test", "completed", "test", nil)
	tracker.CompleteVolumeProvisioning("non-existent", "fs-1", "ap-1")
	tracker.FailVolumeProvisioning("non-existent", errors.New("test"), 1)
}

func TestVolumeStatusTracker_EmitProgressEventWithoutEventRecorder(t *testing.T) {
	// Test with nil event recorder
	tracker := NewVolumeStatusTracker(nil, fake.NewSimpleClientset())

	volumeID := "test-volume-1"
	pvcName := "test-pvc"
	namespace := "test-namespace"

	// Should not crash without event recorder
	tracker.StartVolumeProvisioning(volumeID, pvcName, namespace)
	tracker.UpdateVolumePhase(volumeID, VolumePhaseCompleted, "test", 100)

	// Verify status was still tracked
	status, exists := tracker.GetVolumeStatus(volumeID)
	if !exists {
		t.Error("Volume status should exist even without event recorder")
	}
	if status.Phase != VolumePhaseCompleted {
		t.Error("Phase should be updated even without event recorder")
	}
}