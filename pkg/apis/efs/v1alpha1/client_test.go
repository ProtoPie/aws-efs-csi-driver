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

package v1alpha1

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

func TestEFSNamespaceLister(t *testing.T) {
	// Create an indexer
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})

	// Create test EFSNamespaces
	efsNs1 := &EFSNamespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-1",
			Labels: map[string]string{
				"environment": "dev",
			},
		},
		Spec: EFSNamespaceSpec{
			Namespace: "ns-1",
			Region:    "us-west-2",
		},
	}

	efsNs2 := &EFSNamespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-2",
			Labels: map[string]string{
				"environment": "prod",
			},
		},
		Spec: EFSNamespaceSpec{
			Namespace: "ns-2",
			Region:    "us-east-1",
		},
	}

	efsNs3 := &EFSNamespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-3",
			Labels: map[string]string{
				"environment": "dev",
				"team":        "platform",
			},
		},
		Spec: EFSNamespaceSpec{
			Namespace: "ns-3",
			Region:    "eu-west-1",
		},
	}

	// Add objects to indexer
	if err := indexer.Add(efsNs1); err != nil {
		t.Fatalf("Failed to add efsNs1 to indexer: %v", err)
	}
	if err := indexer.Add(efsNs2); err != nil {
		t.Fatalf("Failed to add efsNs2 to indexer: %v", err)
	}
	if err := indexer.Add(efsNs3); err != nil {
		t.Fatalf("Failed to add efsNs3 to indexer: %v", err)
	}

	// Create lister
	lister := NewEFSNamespaceLister(indexer)

	// Test List all
	allItems, err := lister.List(labels.Everything())
	if err != nil {
		t.Fatalf("Failed to list all items: %v", err)
	}
	if len(allItems) != 3 {
		t.Errorf("Expected 3 items, got %d", len(allItems))
	}

	// Test List with selector
	selector := labels.SelectorFromSet(labels.Set{"environment": "dev"})
	devItems, err := lister.List(selector)
	if err != nil {
		t.Fatalf("Failed to list dev items: %v", err)
	}
	if len(devItems) != 2 {
		t.Errorf("Expected 2 dev items, got %d", len(devItems))
	}

	// Test Get
	item, err := lister.Get("test-1")
	if err != nil {
		t.Fatalf("Failed to get test-1: %v", err)
	}
	if item.Name != "test-1" {
		t.Errorf("Got wrong item: %s", item.Name)
	}

	// Test Get non-existent
	_, err = lister.Get("non-existent")
	if err == nil {
		t.Errorf("Expected error for non-existent item, got nil")
	}
}

func TestEFSNamespaceInformer(t *testing.T) {
	// Create a fake client that implements basic List/Watch
	fakeClient := &fakeEFSNamespaceClient{}

	// Create informer
	informer := NewEFSNamespaceInformer(fakeClient, time.Minute)

	// Test that informer is created successfully
	if informer == nil {
		t.Fatal("Failed to create informer")
	}

	// Get the indexer
	indexer := informer.GetIndexer()
	if indexer == nil {
		t.Fatal("Informer has no indexer")
	}

	// Add a test object directly to the indexer
	testObj := &EFSNamespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "informer-test",
		},
		Spec: EFSNamespaceSpec{
			Namespace: "test-ns",
			Region:    "us-west-2",
		},
	}

	if err := indexer.Add(testObj); err != nil {
		t.Fatalf("Failed to add object to indexer: %v", err)
	}

	// Verify object is in indexer
	obj, exists, err := indexer.GetByKey("informer-test")
	if err != nil {
		t.Fatalf("Failed to get object from indexer: %v", err)
	}
	if !exists {
		t.Error("Object not found in indexer")
	}
	if efsNs, ok := obj.(*EFSNamespace); !ok || efsNs.Name != "informer-test" {
		t.Error("Wrong object in indexer")
	}
}

// fakeEFSNamespaceClient implements a basic fake client for testing
type fakeEFSNamespaceClient struct{}

func (f *fakeEFSNamespaceClient) Create(ctx context.Context, efsNamespace *EFSNamespace, opts metav1.CreateOptions) (*EFSNamespace, error) {
	return efsNamespace, nil
}

func (f *fakeEFSNamespaceClient) Update(ctx context.Context, efsNamespace *EFSNamespace, opts metav1.UpdateOptions) (*EFSNamespace, error) {
	return efsNamespace, nil
}

func (f *fakeEFSNamespaceClient) UpdateStatus(ctx context.Context, efsNamespace *EFSNamespace, opts metav1.UpdateOptions) (*EFSNamespace, error) {
	return efsNamespace, nil
}

func (f *fakeEFSNamespaceClient) Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error {
	return nil
}

func (f *fakeEFSNamespaceClient) Get(ctx context.Context, name string, opts metav1.GetOptions) (*EFSNamespace, error) {
	return &EFSNamespace{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}, nil
}

func (f *fakeEFSNamespaceClient) List(ctx context.Context, opts metav1.ListOptions) (*EFSNamespaceList, error) {
	return &EFSNamespaceList{
		Items: []EFSNamespace{},
	}, nil
}

func (f *fakeEFSNamespaceClient) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	return watch.NewFake(), nil
}

func (f *fakeEFSNamespaceClient) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (*EFSNamespace, error) {
	return &EFSNamespace{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}, nil
}

func TestSchemeRegistration(t *testing.T) {
	// Create a new scheme
	testScheme := runtime.NewScheme()

	// Add our types to the scheme
	if err := AddToScheme(testScheme); err != nil {
		t.Fatalf("Failed to add types to scheme: %v", err)
	}

	// Verify EFSNamespace is registered
	gvks, _, err := testScheme.ObjectKinds(&EFSNamespace{})
	if err != nil {
		t.Fatalf("Failed to get object kinds for EFSNamespace: %v", err)
	}
	if len(gvks) == 0 {
		t.Error("EFSNamespace not registered in scheme")
	}

	// Verify EFSNamespaceList is registered
	gvks, _, err = testScheme.ObjectKinds(&EFSNamespaceList{})
	if err != nil {
		t.Fatalf("Failed to get object kinds for EFSNamespaceList: %v", err)
	}
	if len(gvks) == 0 {
		t.Error("EFSNamespaceList not registered in scheme")
	}

	// Verify the correct GVK
	expectedGVK := SchemeGroupVersion.WithKind("EFSNamespace")
	obj := &EFSNamespace{}
	gvks, _, _ = testScheme.ObjectKinds(obj)
	found := false
	for _, gvk := range gvks {
		if gvk == expectedGVK {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected GVK %v not found in scheme", expectedGVK)
	}
}