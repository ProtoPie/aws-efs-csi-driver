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

package fake

import (
	"context"
	"testing"

	efsv1alpha1 "github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/apis/efs/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	ktesting "k8s.io/client-go/testing"
)

func TestClientset_Create(t *testing.T) {
	ctx := context.Background()
	clientset := NewSimpleClientset()

	// Create a test EFSNamespace
	efsNamespace := &efsv1alpha1.EFSNamespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-efsnamespace",
		},
		Spec: efsv1alpha1.EFSNamespaceSpec{
			Namespace: "test-namespace",
			Region:    "us-west-2",
		},
	}

	created, err := clientset.EfsV1alpha1().EFSNamespaces().Create(ctx, efsNamespace, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create EFSNamespace: %v", err)
	}

	if created.Name != efsNamespace.Name {
		t.Errorf("Created EFSNamespace has wrong name: got %s, want %s", created.Name, efsNamespace.Name)
	}

	if created.Spec.Namespace != efsNamespace.Spec.Namespace {
		t.Errorf("Created EFSNamespace has wrong namespace: got %s, want %s",
			created.Spec.Namespace, efsNamespace.Spec.Namespace)
	}
}

func TestClientset_Get(t *testing.T) {
	ctx := context.Background()

	// Create a test EFSNamespace
	efsNamespace := &efsv1alpha1.EFSNamespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-get",
		},
		Spec: efsv1alpha1.EFSNamespaceSpec{
			Namespace:    "get-namespace",
			Region:       "us-east-1",
			FileSystemID: "fs-12345678",
		},
	}

	clientset := NewSimpleClientset(efsNamespace)

	// Get the EFSNamespace
	retrieved, err := clientset.EfsV1alpha1().EFSNamespaces().Get(ctx, "test-get", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get EFSNamespace: %v", err)
	}

	if retrieved.Name != efsNamespace.Name {
		t.Errorf("Retrieved EFSNamespace has wrong name: got %s, want %s", retrieved.Name, efsNamespace.Name)
	}

	if retrieved.Spec.FileSystemID != efsNamespace.Spec.FileSystemID {
		t.Errorf("Retrieved EFSNamespace has wrong filesystem ID: got %s, want %s",
			retrieved.Spec.FileSystemID, efsNamespace.Spec.FileSystemID)
	}
}

func TestClientset_Update(t *testing.T) {
	ctx := context.Background()

	// Create initial EFSNamespace
	efsNamespace := &efsv1alpha1.EFSNamespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-update",
		},
		Spec: efsv1alpha1.EFSNamespaceSpec{
			Namespace: "update-namespace",
			Region:    "us-west-2",
		},
	}

	clientset := NewSimpleClientset(efsNamespace)

	// Update the EFSNamespace
	efsNamespace.Spec.Region = "eu-west-1"
	efsNamespace.Spec.PerformanceMode = "maxIO"

	updated, err := clientset.EfsV1alpha1().EFSNamespaces().Update(ctx, efsNamespace, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update EFSNamespace: %v", err)
	}

	if updated.Spec.Region != "eu-west-1" {
		t.Errorf("Updated EFSNamespace has wrong region: got %s, want eu-west-1", updated.Spec.Region)
	}

	if updated.Spec.PerformanceMode != "maxIO" {
		t.Errorf("Updated EFSNamespace has wrong performance mode: got %s, want maxIO", updated.Spec.PerformanceMode)
	}
}

func TestClientset_UpdateStatus(t *testing.T) {
	ctx := context.Background()

	// Create initial EFSNamespace
	efsNamespace := &efsv1alpha1.EFSNamespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-status",
		},
		Spec: efsv1alpha1.EFSNamespaceSpec{
			Namespace: "status-namespace",
			Region:    "us-west-2",
		},
	}

	clientset := NewSimpleClientset(efsNamespace)

	// Update the status
	efsNamespace.Status = efsv1alpha1.EFSNamespaceStatus{
		State:            "Active",
		FileSystemID:     "fs-87654321",
		AccessPointCount: 3,
		Message:          "Provisioned successfully",
	}

	updated, err := clientset.EfsV1alpha1().EFSNamespaces().UpdateStatus(ctx, efsNamespace, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update EFSNamespace status: %v", err)
	}

	if updated.Status.State != "Active" {
		t.Errorf("Updated status has wrong state: got %s, want Active", updated.Status.State)
	}

	if updated.Status.AccessPointCount != 3 {
		t.Errorf("Updated status has wrong access point count: got %d, want 3", updated.Status.AccessPointCount)
	}
}

func TestClientset_Delete(t *testing.T) {
	ctx := context.Background()

	// Create initial EFSNamespace
	efsNamespace := &efsv1alpha1.EFSNamespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-delete",
		},
		Spec: efsv1alpha1.EFSNamespaceSpec{
			Namespace: "delete-namespace",
			Region:    "us-west-2",
		},
	}

	clientset := NewSimpleClientset(efsNamespace)

	// Delete the EFSNamespace
	err := clientset.EfsV1alpha1().EFSNamespaces().Delete(ctx, "test-delete", metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("Failed to delete EFSNamespace: %v", err)
	}

	// Verify it's deleted
	_, err = clientset.EfsV1alpha1().EFSNamespaces().Get(ctx, "test-delete", metav1.GetOptions{})
	if err == nil {
		t.Errorf("EFSNamespace should have been deleted but still exists")
	}
}

func TestClientset_List(t *testing.T) {
	ctx := context.Background()

	// Create multiple EFSNamespaces
	efsNamespaces := []runtime.Object{
		&efsv1alpha1.EFSNamespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-list-1",
				Labels: map[string]string{
					"environment": "dev",
				},
			},
			Spec: efsv1alpha1.EFSNamespaceSpec{
				Namespace: "namespace-1",
				Region:    "us-west-2",
			},
		},
		&efsv1alpha1.EFSNamespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-list-2",
				Labels: map[string]string{
					"environment": "prod",
				},
			},
			Spec: efsv1alpha1.EFSNamespaceSpec{
				Namespace: "namespace-2",
				Region:    "us-east-1",
			},
		},
		&efsv1alpha1.EFSNamespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-list-3",
				Labels: map[string]string{
					"environment": "dev",
				},
			},
			Spec: efsv1alpha1.EFSNamespaceSpec{
				Namespace: "namespace-3",
				Region:    "eu-west-1",
			},
		},
	}

	clientset := NewSimpleClientset(efsNamespaces...)

	// List all EFSNamespaces
	list, err := clientset.EfsV1alpha1().EFSNamespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Failed to list EFSNamespaces: %v", err)
	}

	if len(list.Items) != 3 {
		t.Errorf("List returned wrong number of items: got %d, want 3", len(list.Items))
	}

	// List with label selector
	list, err = clientset.EfsV1alpha1().EFSNamespaces().List(ctx, metav1.ListOptions{
		LabelSelector: "environment=dev",
	})
	if err != nil {
		t.Fatalf("Failed to list EFSNamespaces with label selector: %v", err)
	}

	if len(list.Items) != 2 {
		t.Errorf("List with label selector returned wrong number of items: got %d, want 2", len(list.Items))
	}
}

func TestClientset_Watch(t *testing.T) {
	ctx := context.Background()
	clientset := NewSimpleClientset()

	// Set up watch
	watcher, err := clientset.EfsV1alpha1().EFSNamespaces().Watch(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Failed to set up watch: %v", err)
	}
	defer watcher.Stop()

	// Create a goroutine to receive events
	eventReceived := make(chan bool)
	go func() {
		select {
		case event := <-watcher.ResultChan():
			if event.Type != watch.Added {
				t.Errorf("Wrong event type: got %v, want %v", event.Type, watch.Added)
			}
			efsNs, ok := event.Object.(*efsv1alpha1.EFSNamespace)
			if !ok {
				t.Errorf("Wrong object type in event")
			}
			if efsNs.Name != "watch-test" {
				t.Errorf("Wrong object name in event: got %s, want watch-test", efsNs.Name)
			}
			eventReceived <- true
		}
	}()

	// Create an EFSNamespace - this should trigger a watch event
	efsNamespace := &efsv1alpha1.EFSNamespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "watch-test",
		},
		Spec: efsv1alpha1.EFSNamespaceSpec{
			Namespace: "watch-namespace",
			Region:    "us-west-2",
		},
	}

	_, err = clientset.EfsV1alpha1().EFSNamespaces().Create(ctx, efsNamespace, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create EFSNamespace: %v", err)
	}

	// Wait for event or timeout
	select {
	case <-eventReceived:
		// Success
	case <-ctx.Done():
		t.Fatal("Timeout waiting for watch event")
	}
}

func TestClientset_Patch(t *testing.T) {
	ctx := context.Background()

	// Create initial EFSNamespace
	efsNamespace := &efsv1alpha1.EFSNamespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-patch",
		},
		Spec: efsv1alpha1.EFSNamespaceSpec{
			Namespace:       "patch-namespace",
			Region:          "us-west-2",
			PerformanceMode: "generalPurpose",
			ThroughputMode:  "bursting",
		},
	}

	clientset := NewSimpleClientset(efsNamespace)

	// Apply a patch - Note: fake client doesn't actually process patches properly,
	// but we can test that the method exists and is callable
	patchData := []byte(`{"spec":{"performanceMode":"maxIO"}}`)

	_, err := clientset.EfsV1alpha1().EFSNamespaces().Patch(
		ctx,
		"test-patch",
		"application/merge-patch+json",
		patchData,
		metav1.PatchOptions{},
	)
	if err != nil {
		t.Fatalf("Failed to patch EFSNamespace: %v", err)
	}
}

func TestClientset_Reactor(t *testing.T) {
	// Test that we can add custom reactors to the fake client
	clientset := NewSimpleClientset()

	createCalled := false
	clientset.PrependReactor("create", "efsnamespaces", func(action ktesting.Action) (bool, runtime.Object, error) {
		createCalled = true
		createAction := action.(ktesting.CreateAction)
		obj := createAction.GetObject()
		return false, obj, nil // Return false to continue with normal processing
	})

	ctx := context.Background()
	efsNamespace := &efsv1alpha1.EFSNamespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "reactor-test",
		},
		Spec: efsv1alpha1.EFSNamespaceSpec{
			Namespace: "reactor-namespace",
			Region:    "us-west-2",
		},
	}

	_, err := clientset.EfsV1alpha1().EFSNamespaces().Create(ctx, efsNamespace, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create EFSNamespace: %v", err)
	}

	if !createCalled {
		t.Errorf("Custom reactor was not called")
	}
}