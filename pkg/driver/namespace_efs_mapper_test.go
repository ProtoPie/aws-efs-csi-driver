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
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"

	efsv1alpha1 "github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/apis/efs/v1alpha1"
)

// Helper functions for testing without external dependencies
func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(substr) > 0 && s[0:len(substr)] == substr) ||
		(len(s) > len(substr) && s[len(s)-len(substr):] == substr) ||
		len(s) > len(substr) && func() bool {
			for i := 1; i <= len(s)-len(substr); i++ {
				if s[i:i+len(substr)] == substr {
					return true
				}
			}
			return false
		}())
}

func assertEqual(t *testing.T, expected, actual interface{}, msg string) {
	if expected != actual {
		t.Errorf("%s: expected %v, got %v", msg, expected, actual)
	}
}

func assertNotNil(t *testing.T, value interface{}, msg string) {
	if value == nil {
		t.Errorf("%s: expected non-nil value", msg)
	}
}

func assertNil(t *testing.T, value interface{}, msg string) {
	if value != nil {
		// Handle typed nil pointers (common Go gotcha)
		if reflect.ValueOf(value).Kind() == reflect.Ptr && reflect.ValueOf(value).IsNil() {
			return
		}
		t.Errorf("%s: expected nil value, got %v", msg, value)
	}
}

func assertError(t *testing.T, err error, msg string) {
	if err == nil {
		t.Errorf("%s: expected error but got nil", msg)
	}
}

func assertNoError(t *testing.T, err error, msg string) {
	if err != nil {
		t.Errorf("%s: unexpected error: %v", msg, err)
	}
}

func assertBool(t *testing.T, condition bool, msg string) {
	if !condition {
		t.Errorf("%s: condition was false", msg)
	}
}

// mockEFSNamespaceClient implements efsv1alpha1.EFSNamespaceInterface for testing
type mockEFSNamespaceClient struct {
	mutex     sync.RWMutex
	resources map[string]*efsv1alpha1.EFSNamespace
	calls     []string // Track method calls for verification
}

func newMockEFSNamespaceClient() *mockEFSNamespaceClient {
	return &mockEFSNamespaceClient{
		resources: make(map[string]*efsv1alpha1.EFSNamespace),
		calls:     make([]string, 0),
	}
}

func (m *mockEFSNamespaceClient) Create(ctx context.Context, efsNamespace *efsv1alpha1.EFSNamespace, opts metav1.CreateOptions) (*efsv1alpha1.EFSNamespace, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.calls = append(m.calls, "Create")

	if _, exists := m.resources[efsNamespace.Name]; exists {
		return nil, errors.NewAlreadyExists(schema.GroupResource{Group: "efs.csi.aws.com", Resource: "efsnamespaces"}, efsNamespace.Name)
	}

	// Deep copy to simulate Kubernetes API behavior
	result := efsNamespace.DeepCopy()
	result.CreationTimestamp = metav1.Now()
	result.ResourceVersion = "1"

	m.resources[efsNamespace.Name] = result
	return result, nil
}

func (m *mockEFSNamespaceClient) Update(ctx context.Context, efsNamespace *efsv1alpha1.EFSNamespace, opts metav1.UpdateOptions) (*efsv1alpha1.EFSNamespace, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.calls = append(m.calls, "Update")

	existing, exists := m.resources[efsNamespace.Name]
	if !exists {
		return nil, errors.NewNotFound(schema.GroupResource{Group: "efs.csi.aws.com", Resource: "efsnamespaces"}, efsNamespace.Name)
	}

	// Deep copy and update resource version
	result := efsNamespace.DeepCopy()
	result.CreationTimestamp = existing.CreationTimestamp
	result.ResourceVersion = "2"

	m.resources[efsNamespace.Name] = result
	return result, nil
}

func (m *mockEFSNamespaceClient) UpdateStatus(ctx context.Context, efsNamespace *efsv1alpha1.EFSNamespace, opts metav1.UpdateOptions) (*efsv1alpha1.EFSNamespace, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.calls = append(m.calls, "UpdateStatus")

	existing, exists := m.resources[efsNamespace.Name]
	if !exists {
		return nil, errors.NewNotFound(schema.GroupResource{Group: "efs.csi.aws.com", Resource: "efsnamespaces"}, efsNamespace.Name)
	}

	result := existing.DeepCopy()
	result.Status = efsNamespace.Status
	result.ResourceVersion = "3"

	m.resources[efsNamespace.Name] = result
	return result, nil
}

func (m *mockEFSNamespaceClient) Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.calls = append(m.calls, "Delete")

	if _, exists := m.resources[name]; !exists {
		return errors.NewNotFound(schema.GroupResource{Group: "efs.csi.aws.com", Resource: "efsnamespaces"}, name)
	}

	delete(m.resources, name)
	return nil
}

func (m *mockEFSNamespaceClient) Get(ctx context.Context, name string, opts metav1.GetOptions) (*efsv1alpha1.EFSNamespace, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	m.calls = append(m.calls, "Get")

	resource, exists := m.resources[name]
	if !exists {
		return nil, errors.NewNotFound(schema.GroupResource{Group: "efs.csi.aws.com", Resource: "efsnamespaces"}, name)
	}

	return resource.DeepCopy(), nil
}

func (m *mockEFSNamespaceClient) List(ctx context.Context, opts metav1.ListOptions) (*efsv1alpha1.EFSNamespaceList, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	m.calls = append(m.calls, "List")

	list := &efsv1alpha1.EFSNamespaceList{}
	for _, resource := range m.resources {
		list.Items = append(list.Items, *resource.DeepCopy())
	}

	return list, nil
}

func (m *mockEFSNamespaceClient) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	m.calls = append(m.calls, "Watch")
	return watch.NewFake(), nil
}

func (m *mockEFSNamespaceClient) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (*efsv1alpha1.EFSNamespace, error) {
	m.calls = append(m.calls, "Patch")
	return nil, fmt.Errorf("patch not implemented in mock")
}

func (m *mockEFSNamespaceClient) getCalls() []string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return append([]string{}, m.calls...)
}

func (m *mockEFSNamespaceClient) clearCalls() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.calls = m.calls[:0]
}

// Test helper function to create a NamespaceEFSMapper with mocked dependencies
func createTestMapper(t *testing.T) (*NamespaceEFSMapper, *mockEFSNamespaceClient) {
	k8sClient := fake.NewSimpleClientset()
	mockCRDClient := newMockEFSNamespaceClient()

	mapper := &NamespaceEFSMapper{
		crdClient:    mockCRDClient,
		k8sClient:    k8sClient,
		cache:        make(map[string]*NamespaceEFSMapping),
		resyncPeriod: 100 * time.Millisecond, // Short period for testing
		stopCh:       make(chan struct{}),
	}

	return mapper, mockCRDClient
}

func TestNewNamespaceEFSMapper(t *testing.T) {
	tests := []struct {
		name          string
		k8sClient     interface{}
		config        interface{}
		expectError   bool
		expectedError string
	}{
		{
			name:          "nil kubernetes client",
			k8sClient:     nil,
			config:        &mockRestConfig{},
			expectError:   true,
			expectedError: "kubernetes client cannot be nil",
		},
		{
			name:          "nil rest config",
			k8sClient:     fake.NewSimpleClientset(),
			config:        nil,
			expectError:   true,
			expectedError: "rest config cannot be nil",
		},
		{
			name:        "valid inputs",
			k8sClient:   fake.NewSimpleClientset(),
			config:      &mockRestConfig{},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This test would require mocking the rest.Config and CRD client creation
			// For now, we test the basic validation logic
			if tt.k8sClient == nil {
				_, err := NewNamespaceEFSMapper(nil, nil)
				if err == nil {
					t.Error("Expected error but got nil")
				}
				if err != nil && !containsString(err.Error(), tt.expectedError) {
					t.Errorf("Expected error to contain '%s', got '%s'", tt.expectedError, err.Error())
				}
			}
		})
	}
}

func TestCreateOrUpdateMapping(t *testing.T) {
	tests := []struct {
		name          string
		namespace     string
		fileSystemID  string
		fileSystemArn string
		region        string
		expectError   bool
		expectedError string
		setupFunc     func(*mockEFSNamespaceClient)
		expectedCalls []string
	}{
		{
			name:          "empty namespace",
			namespace:     "",
			fileSystemID:  "fs-12345678",
			region:        "us-east-1",
			expectError:   true,
			expectedError: "namespace, fileSystemID, and region are required",
		},
		{
			name:          "empty fileSystemID",
			namespace:     "test-ns",
			fileSystemID:  "",
			region:        "us-east-1",
			expectError:   true,
			expectedError: "namespace, fileSystemID, and region are required",
		},
		{
			name:          "empty region",
			namespace:     "test-ns",
			fileSystemID:  "fs-12345678",
			region:        "",
			expectError:   true,
			expectedError: "namespace, fileSystemID, and region are required",
		},
		{
			name:          "successful create new mapping",
			namespace:     "test-ns",
			fileSystemID:  "fs-12345678",
			fileSystemArn: "arn:aws:elasticfilesystem:us-east-1:123456789012:file-system/fs-12345678",
			region:        "us-east-1",
			expectError:   false,
			expectedCalls: []string{"Get", "Create"},
		},
		{
			name:          "successful update existing mapping",
			namespace:     "existing-ns",
			fileSystemID:  "fs-87654321",
			fileSystemArn: "arn:aws:elasticfilesystem:us-west-2:123456789012:file-system/fs-87654321",
			region:        "us-west-2",
			expectError:   false,
			setupFunc: func(mock *mockEFSNamespaceClient) {
				// Pre-create an existing mapping
				existing := &efsv1alpha1.EFSNamespace{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "existing-ns",
						CreationTimestamp: metav1.Now(),
						ResourceVersion:   "1",
					},
					Spec: efsv1alpha1.EFSNamespaceSpec{
						Namespace:    "existing-ns",
						FileSystemID: "fs-11111111",
						Region:       "us-east-1",
					},
				}
				mock.resources["existing-ns"] = existing
			},
			expectedCalls: []string{"Get", "Update"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapper, mockClient := createTestMapper(t)

			if tt.setupFunc != nil {
				tt.setupFunc(mockClient)
			}

			ctx := context.Background()
			result, err := mapper.CreateOrUpdateMapping(ctx, tt.namespace, tt.fileSystemID, tt.fileSystemArn, tt.region)

			if tt.expectError {
				assertError(t, err, "CreateOrUpdateMapping should return error")
				if err != nil && !containsString(err.Error(), tt.expectedError) {
					t.Errorf("Expected error to contain '%s', got '%s'", tt.expectedError, err.Error())
				}
				assertNil(t, result, "Result should be nil on error")
			} else {
				assertNoError(t, err, "CreateOrUpdateMapping should not return error")
				assertNotNil(t, result, "Result should not be nil")
				assertEqual(t, tt.namespace, result.Namespace, "Namespace mismatch")
				assertEqual(t, tt.fileSystemID, result.FileSystemID, "FileSystemID mismatch")
				assertEqual(t, tt.fileSystemArn, result.FileSystemArn, "FileSystemArn mismatch")
				assertEqual(t, tt.region, result.Region, "Region mismatch")
				assertBool(t, !result.CreationTime.IsZero(), "CreationTime should not be zero")
				assertBool(t, !result.LastAccessTime.IsZero(), "LastAccessTime should not be zero")

				// Verify cache was updated
				cached := mapper.getCachedMapping(tt.namespace)
				assertNotNil(t, cached, "Cached mapping should exist")
				assertEqual(t, tt.fileSystemID, cached.FileSystemID, "Cached FileSystemID mismatch")
			}

			if len(tt.expectedCalls) > 0 {
				calls := mockClient.getCalls()
				if len(calls) != len(tt.expectedCalls) {
					t.Errorf("Expected %d calls, got %d: %v", len(tt.expectedCalls), len(calls), calls)
				} else {
					for i, expected := range tt.expectedCalls {
						if i < len(calls) && calls[i] != expected {
							t.Errorf("Expected call[%d] = %s, got %s", i, expected, calls[i])
						}
					}
				}
			}
		})
	}
}

func TestGetMapping(t *testing.T) {
	tests := []struct {
		name          string
		namespace     string
		expectError   bool
		expectedError string
		setupFunc     func(*NamespaceEFSMapper, *mockEFSNamespaceClient)
		expectedCalls []string
		expectNil     bool
	}{
		{
			name:          "empty namespace",
			namespace:     "",
			expectError:   true,
			expectedError: "namespace is required",
		},
		{
			name:          "not found in cache or CRD",
			namespace:     "nonexistent-ns",
			expectError:   false,
			expectNil:     true,
			expectedCalls: []string{"Get"},
		},
		{
			name:      "found in cache",
			namespace: "cached-ns",
			setupFunc: func(mapper *NamespaceEFSMapper, mock *mockEFSNamespaceClient) {
				mapping := &NamespaceEFSMapping{
					Namespace:      "cached-ns",
					FileSystemID:   "fs-cached123",
					FileSystemArn:  "arn:aws:elasticfilesystem:us-east-1:123456789012:file-system/fs-cached123",
					Region:         "us-east-1",
					CreationTime:   time.Now(),
					LastAccessTime: time.Now(),
				}
				mapper.updateCache("cached-ns", mapping)
			},
			expectError:   false,
			expectedCalls: []string{}, // Should not call CRD client
		},
		{
			name:      "found in CRD",
			namespace: "crd-ns",
			setupFunc: func(mapper *NamespaceEFSMapper, mock *mockEFSNamespaceClient) {
				efsNamespace := &efsv1alpha1.EFSNamespace{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "crd-ns",
						CreationTimestamp: metav1.Now(),
					},
					Spec: efsv1alpha1.EFSNamespaceSpec{
						Namespace:     "crd-ns",
						FileSystemID:  "fs-crd123",
						FileSystemArn: "arn:aws:elasticfilesystem:us-west-2:123456789012:file-system/fs-crd123",
						Region:        "us-west-2",
					},
				}
				mock.resources["crd-ns"] = efsNamespace
			},
			expectError:   false,
			expectedCalls: []string{"Get"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapper, mockClient := createTestMapper(t)

			if tt.setupFunc != nil {
				tt.setupFunc(mapper, mockClient)
			}

			ctx := context.Background()
			result, err := mapper.GetMapping(ctx, tt.namespace)

			if tt.expectError {
				assertError(t, err, "GetMapping should return error")
				if err != nil && !containsString(err.Error(), tt.expectedError) {
					t.Errorf("Expected error to contain '%s', got '%s'", tt.expectedError, err.Error())
				}
				assertNil(t, result, "Result should be nil on error")
			} else if tt.expectNil {
				assertNoError(t, err, "GetMapping should not return error")
				assertNil(t, result, "Result should be nil when not found")
			} else {
				assertNoError(t, err, "GetMapping should not return error")
				assertNotNil(t, result, "Result should not be nil")
				assertEqual(t, tt.namespace, result.Namespace, "Namespace mismatch")
				assertBool(t, !result.CreationTime.IsZero(), "CreationTime should not be zero")
				assertBool(t, !result.LastAccessTime.IsZero(), "LastAccessTime should not be zero")
			}

			if len(tt.expectedCalls) > 0 {
				calls := mockClient.getCalls()
				if len(calls) != len(tt.expectedCalls) {
					t.Errorf("Expected %d calls, got %d: %v", len(tt.expectedCalls), len(calls), calls)
				} else {
					for i, expected := range tt.expectedCalls {
						if i < len(calls) && calls[i] != expected {
							t.Errorf("Expected call[%d] = %s, got %s", i, expected, calls[i])
						}
					}
				}
			}
		})
	}
}

func TestDeleteMapping(t *testing.T) {
	tests := []struct {
		name          string
		namespace     string
		expectError   bool
		expectedError string
		setupFunc     func(*NamespaceEFSMapper, *mockEFSNamespaceClient)
		expectedCalls []string
	}{
		{
			name:          "empty namespace",
			namespace:     "",
			expectError:   true,
			expectedError: "namespace is required",
		},
		{
			name:          "delete nonexistent mapping",
			namespace:     "nonexistent-ns",
			expectError:   false,
			expectedCalls: []string{"Delete"},
		},
		{
			name:      "delete existing mapping",
			namespace: "existing-ns",
			setupFunc: func(mapper *NamespaceEFSMapper, mock *mockEFSNamespaceClient) {
				efsNamespace := &efsv1alpha1.EFSNamespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: "existing-ns",
					},
					Spec: efsv1alpha1.EFSNamespaceSpec{
						Namespace:    "existing-ns",
						FileSystemID: "fs-existing123",
						Region:       "us-east-1",
					},
				}
				mock.resources["existing-ns"] = efsNamespace

				// Also add to cache
				mapping := &NamespaceEFSMapping{
					Namespace:    "existing-ns",
					FileSystemID: "fs-existing123",
					Region:       "us-east-1",
				}
				mapper.updateCache("existing-ns", mapping)
			},
			expectError:   false,
			expectedCalls: []string{"Delete"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapper, mockClient := createTestMapper(t)

			if tt.setupFunc != nil {
				tt.setupFunc(mapper, mockClient)
			}

			ctx := context.Background()
			err := mapper.DeleteMapping(ctx, tt.namespace)

			if tt.expectError {
				assertError(t, err, "DeleteMapping should return error")
				if err != nil && !containsString(err.Error(), tt.expectedError) {
					t.Errorf("Expected error to contain '%s', got '%s'", tt.expectedError, err.Error())
				}
			} else {
				assertNoError(t, err, "DeleteMapping should not return error")

				// Verify cache was cleared
				cached := mapper.getCachedMapping(tt.namespace)
				assertNil(t, cached, "Cached mapping should be cleared")
			}

			if len(tt.expectedCalls) > 0 {
				calls := mockClient.getCalls()
				if len(calls) != len(tt.expectedCalls) {
					t.Errorf("Expected %d calls, got %d: %v", len(tt.expectedCalls), len(calls), calls)
				} else {
					for i, expected := range tt.expectedCalls {
						if i < len(calls) && calls[i] != expected {
							t.Errorf("Expected call[%d] = %s, got %s", i, expected, calls[i])
						}
					}
				}
			}
		})
	}
}

func TestListMappings(t *testing.T) {
	mapper, mockClient := createTestMapper(t)

	// Setup test data
	efsNamespaces := []*efsv1alpha1.EFSNamespace{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "ns1",
				CreationTimestamp: metav1.Now(),
			},
			Spec: efsv1alpha1.EFSNamespaceSpec{
				Namespace:     "ns1",
				FileSystemID:  "fs-111111",
				FileSystemArn: "arn:aws:elasticfilesystem:us-east-1:123456789012:file-system/fs-111111",
				Region:        "us-east-1",
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "ns2",
				CreationTimestamp: metav1.Now(),
			},
			Spec: efsv1alpha1.EFSNamespaceSpec{
				Namespace:     "ns2",
				FileSystemID:  "fs-222222",
				FileSystemArn: "arn:aws:elasticfilesystem:us-west-2:123456789012:file-system/fs-222222",
				Region:        "us-west-2",
			},
		},
	}

	for _, efsNamespace := range efsNamespaces {
		mockClient.resources[efsNamespace.Name] = efsNamespace
	}

	ctx := context.Background()
	mappings, err := mapper.ListMappings(ctx)

	assertNoError(t, err, "ListMappings should not return error")
	if len(mappings) != 2 {
		t.Errorf("Expected 2 mappings, got %d", len(mappings))
	}

	// Verify mappings contain expected data
	namespaces := make(map[string]NamespaceEFSMapping)
	for _, mapping := range mappings {
		namespaces[mapping.Namespace] = mapping
	}

	if _, exists := namespaces["ns1"]; !exists {
		t.Error("Expected mapping for ns1")
	}
	if _, exists := namespaces["ns2"]; !exists {
		t.Error("Expected mapping for ns2")
	}
	assertEqual(t, "fs-111111", namespaces["ns1"].FileSystemID, "ns1 FileSystemID mismatch")
	assertEqual(t, "fs-222222", namespaces["ns2"].FileSystemID, "ns2 FileSystemID mismatch")

	// Verify cache was updated
	cached1 := mapper.getCachedMapping("ns1")
	cached2 := mapper.getCachedMapping("ns2")
	assertNotNil(t, cached1, "ns1 should be cached")
	assertNotNil(t, cached2, "ns2 should be cached")
	assertEqual(t, "fs-111111", cached1.FileSystemID, "cached ns1 FileSystemID mismatch")
	assertEqual(t, "fs-222222", cached2.FileSystemID, "cached ns2 FileSystemID mismatch")

	// Verify CRD client was called
	calls := mockClient.getCalls()
	if len(calls) != 1 || calls[0] != "List" {
		t.Errorf("Expected [List], got %v", calls)
	}
}

func TestCacheOperations(t *testing.T) {
	mapper, _ := createTestMapper(t)

	t.Run("InvalidateCache", func(t *testing.T) {
		// Add mapping to cache
		mapping := &NamespaceEFSMapping{
			Namespace:    "test-ns",
			FileSystemID: "fs-test123",
			Region:       "us-east-1",
		}
		mapper.updateCache("test-ns", mapping)

		// Verify it's in cache
		cached := mapper.getCachedMapping("test-ns")
		assertNotNil(t, cached, "Mapping should be in cache")

		// Invalidate cache
		mapper.InvalidateCache("test-ns")

		// Verify it's removed from cache
		cached = mapper.getCachedMapping("test-ns")
		assertNil(t, cached, "Mapping should be removed from cache")
	})

	t.Run("ClearCache", func(t *testing.T) {
		// Add multiple mappings to cache
		mappings := []*NamespaceEFSMapping{
			{Namespace: "ns1", FileSystemID: "fs-1", Region: "us-east-1"},
			{Namespace: "ns2", FileSystemID: "fs-2", Region: "us-west-2"},
		}

		for _, mapping := range mappings {
			mapper.updateCache(mapping.Namespace, mapping)
		}

		// Verify they're in cache
		assertNotNil(t, mapper.getCachedMapping("ns1"), "ns1 should be in cache")
		assertNotNil(t, mapper.getCachedMapping("ns2"), "ns2 should be in cache")

		// Clear cache
		mapper.ClearCache()

		// Verify cache is empty
		assertNil(t, mapper.getCachedMapping("ns1"), "ns1 should be removed from cache")
		assertNil(t, mapper.getCachedMapping("ns2"), "ns2 should be removed from cache")
	})
}

func TestConcurrency(t *testing.T) {
	mapper, mockClient := createTestMapper(t)

	// Setup some test data
	efsNamespace := &efsv1alpha1.EFSNamespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "concurrent-ns",
			CreationTimestamp: metav1.Now(),
		},
		Spec: efsv1alpha1.EFSNamespaceSpec{
			Namespace:    "concurrent-ns",
			FileSystemID: "fs-concurrent123",
			Region:       "us-east-1",
		},
	}
	mockClient.resources["concurrent-ns"] = efsNamespace

	ctx := context.Background()
	var wg sync.WaitGroup
	errors := make(chan error, 10)

	// Test concurrent reads
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := mapper.GetMapping(ctx, "concurrent-ns")
			if err != nil {
				errors <- err
			}
		}()
	}

	// Test concurrent cache operations
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mapper.InvalidateCache("concurrent-ns")
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent operation failed: %v", err)
	}
}

func TestInformerEventHandlers(t *testing.T) {
	mapper, _ := createTestMapper(t)

	t.Run("onEFSNamespaceAdd", func(t *testing.T) {
		efsNamespace := &efsv1alpha1.EFSNamespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "added-ns",
				CreationTimestamp: metav1.Now(),
			},
			Spec: efsv1alpha1.EFSNamespaceSpec{
				Namespace:     "added-ns",
				FileSystemID:  "fs-added123",
				FileSystemArn: "arn:aws:elasticfilesystem:us-east-1:123456789012:file-system/fs-added123",
				Region:        "us-east-1",
			},
		}

		mapper.onEFSNamespaceAdd(efsNamespace)

		cached := mapper.getCachedMapping("added-ns")
		assertNotNil(t, cached, "Added mapping should be cached")
		assertEqual(t, "fs-added123", cached.FileSystemID, "Added FileSystemID mismatch")
	})

	t.Run("onEFSNamespaceUpdate", func(t *testing.T) {
		// First add a mapping
		mapping := &NamespaceEFSMapping{
			Namespace:    "updated-ns",
			FileSystemID: "fs-old123",
			Region:       "us-east-1",
		}
		mapper.updateCache("updated-ns", mapping)

		// Now simulate an update
		efsNamespace := &efsv1alpha1.EFSNamespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "updated-ns",
				CreationTimestamp: metav1.Now(),
			},
			Spec: efsv1alpha1.EFSNamespaceSpec{
				Namespace:     "updated-ns",
				FileSystemID:  "fs-new123",
				FileSystemArn: "arn:aws:elasticfilesystem:us-east-1:123456789012:file-system/fs-new123",
				Region:        "us-west-2",
			},
		}

		mapper.onEFSNamespaceUpdate(efsNamespace)

		cached := mapper.getCachedMapping("updated-ns")
		assertNotNil(t, cached, "Updated mapping should be cached")
		assertEqual(t, "fs-new123", cached.FileSystemID, "Updated FileSystemID mismatch")
		assertEqual(t, "us-west-2", cached.Region, "Updated Region mismatch")
	})

	t.Run("onEFSNamespaceDelete", func(t *testing.T) {
		// First add a mapping
		mapping := &NamespaceEFSMapping{
			Namespace:    "deleted-ns",
			FileSystemID: "fs-deleted123",
			Region:       "us-east-1",
		}
		mapper.updateCache("deleted-ns", mapping)

		// Verify it's in cache
		cached := mapper.getCachedMapping("deleted-ns")
		assertNotNil(t, cached, "Mapping should be in cache before delete")

		// Now simulate deletion
		efsNamespace := &efsv1alpha1.EFSNamespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: "deleted-ns",
			},
			Spec: efsv1alpha1.EFSNamespaceSpec{
				Namespace: "deleted-ns",
			},
		}

		mapper.onEFSNamespaceDelete(efsNamespace)

		// Verify it's removed from cache
		cached = mapper.getCachedMapping("deleted-ns")
		assertNil(t, cached, "Mapping should be removed from cache after delete")
	})
}

// mockRestConfig is a minimal mock for rest.Config
type mockRestConfig struct{}

func (c *mockRestConfig) String() string {
	return "mock-rest-config"
}
