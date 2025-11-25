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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// mockPVCTracker implements PVCTracker for testing
type mockPVCTracker struct {
	addPVCFunc          func(ctx context.Context, namespace, pvcName, volumeID string) error
	removePVCFunc       func(ctx context.Context, namespace, pvcName string) (bool, error)
	getPVCCountFunc     func(ctx context.Context, namespace string) (int32, error)
	listPVCsFunc        func(ctx context.Context, namespace string) ([]string, error)
	syncWithClusterFunc func(ctx context.Context) error
}

func (m *mockPVCTracker) AddPVC(ctx context.Context, namespace, pvcName, volumeID string) error {
	if m.addPVCFunc != nil {
		return m.addPVCFunc(ctx, namespace, pvcName, volumeID)
	}
	return nil
}

func (m *mockPVCTracker) RemovePVC(ctx context.Context, namespace, pvcName string) (bool, error) {
	if m.removePVCFunc != nil {
		return m.removePVCFunc(ctx, namespace, pvcName)
	}
	return false, nil
}

func (m *mockPVCTracker) GetPVCCount(ctx context.Context, namespace string) (int32, error) {
	if m.getPVCCountFunc != nil {
		return m.getPVCCountFunc(ctx, namespace)
	}
	return 0, nil
}

func (m *mockPVCTracker) ListPVCsInNamespace(ctx context.Context, namespace string) ([]string, error) {
	if m.listPVCsFunc != nil {
		return m.listPVCsFunc(ctx, namespace)
	}
	return nil, nil
}

func (m *mockPVCTracker) SyncWithCluster(ctx context.Context) error {
	if m.syncWithClusterFunc != nil {
		return m.syncWithClusterFunc(ctx)
	}
	return nil
}

// mockNamespaceFileSystemManager implements NamespaceFileSystemManager for testing
type mockNamespaceFileSystemManager struct {
	createOrGetFunc       func(ctx context.Context, namespace string, options *FileSystemOptions) (*FileSystemInfo, error)
	deleteFunc            func(ctx context.Context, namespace string, volumeID string) error
	getFileSystemInfoFunc func(ctx context.Context, namespace string) (*FileSystemInfo, error)
	listFunc              func(ctx context.Context, namespace string) ([]*FileSystemInfo, error)
	syncFromAWSFunc       func(ctx context.Context) error
}

func (m *mockNamespaceFileSystemManager) CreateOrGetFileSystemForNamespace(
	ctx context.Context, namespace string, options *FileSystemOptions,
) (*FileSystemInfo, error) {
	if m.createOrGetFunc != nil {
		return m.createOrGetFunc(ctx, namespace, options)
	}
	return nil, nil
}

func (m *mockNamespaceFileSystemManager) DeleteFileSystemForNamespace(
	ctx context.Context, namespace string, volumeID string,
) error {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, namespace, volumeID)
	}
	return nil
}

func (m *mockNamespaceFileSystemManager) GetFileSystemInfo(
	ctx context.Context, namespace string,
) (*FileSystemInfo, error) {
	if m.getFileSystemInfoFunc != nil {
		return m.getFileSystemInfoFunc(ctx, namespace)
	}
	return nil, nil
}

func (m *mockNamespaceFileSystemManager) ListFileSystemsForNamespace(
	ctx context.Context, namespace string,
) ([]*FileSystemInfo, error) {
	if m.listFunc != nil {
		return m.listFunc(ctx, namespace)
	}
	return nil, nil
}

func (m *mockNamespaceFileSystemManager) SyncFromAWS(ctx context.Context) error {
	if m.syncFromAWSFunc != nil {
		return m.syncFromAWSFunc(ctx)
	}
	return nil
}

func TestNewConfigMapFinalizerManager(t *testing.T) {
	client := fake.NewSimpleClientset()
	pvcTracker := &mockPVCTracker{}
	fsMgr := &mockNamespaceFileSystemManager{}

	tests := []struct {
		name          string
		client        kubernetes.Interface
		clusterID     string
		pvcTracker    PVCTracker
		fileSystemMgr NamespaceFileSystemManager
		shouldPanic   bool
	}{
		{
			name:          "valid parameters",
			client:        client,
			clusterID:     "test-cluster",
			pvcTracker:    pvcTracker,
			fileSystemMgr: fsMgr,
			shouldPanic:   false,
		},
		{
			name:          "nil client",
			client:        nil,
			clusterID:     "test-cluster",
			pvcTracker:    pvcTracker,
			fileSystemMgr: fsMgr,
			shouldPanic:   true,
		},
		{
			name:          "empty cluster ID",
			client:        client,
			clusterID:     "",
			pvcTracker:    pvcTracker,
			fileSystemMgr: fsMgr,
			shouldPanic:   true,
		},
		{
			name:          "nil pvc tracker",
			client:        client,
			clusterID:     "test-cluster",
			pvcTracker:    nil,
			fileSystemMgr: fsMgr,
			shouldPanic:   true,
		},
		{
			name:          "nil filesystem manager",
			client:        client,
			clusterID:     "test-cluster",
			pvcTracker:    pvcTracker,
			fileSystemMgr: nil,
			shouldPanic:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.shouldPanic {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("Expected panic but didn't panic")
					}
				}()
			}

			mgr := NewConfigMapFinalizerManager(tt.client, tt.clusterID, tt.pvcTracker, tt.fileSystemMgr)

			if !tt.shouldPanic {
				if mgr == nil {
					t.Errorf("Expected non-nil manager")
				}
				if mgr.client != tt.client {
					t.Errorf("Expected client to be set correctly")
				}
				if mgr.clusterID != tt.clusterID {
					t.Errorf("Expected clusterID to be set correctly")
				}
			}
		})
	}
}

func TestAddFinalizer(t *testing.T) {
	tests := []struct {
		name          string
		pvc           *corev1.PersistentVolumeClaim
		setupClient   func() kubernetes.Interface
		expectedError bool
		errorType     EFSNSErrorType
	}{
		{
			name:          "nil PVC",
			pvc:           nil,
			setupClient:   func() kubernetes.Interface { return fake.NewSimpleClientset() },
			expectedError: true,
			errorType:     ErrInvalidParameter,
		},
		{
			name: "PVC not found",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "test-ns",
				},
			},
			setupClient: func() kubernetes.Interface {
				return fake.NewSimpleClientset()
			},
			expectedError: false,
		},
		{
			name: "finalizer already exists",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-pvc",
					Namespace:  "test-ns",
					Finalizers: []string{EFSNSFinalizerName},
				},
			},
			setupClient: func() kubernetes.Interface {
				pvc := &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:       "test-pvc",
						Namespace:  "test-ns",
						Finalizers: []string{EFSNSFinalizerName},
					},
				}
				return fake.NewSimpleClientset(pvc)
			},
			expectedError: false,
		},
		{
			name: "successful finalizer addition",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "test-ns",
				},
			},
			setupClient: func() kubernetes.Interface {
				pvc := &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pvc",
						Namespace: "test-ns",
					},
				}
				return fake.NewSimpleClientset(pvc)
			},
			expectedError: false,
		},
		{
			name: "kubernetes API error",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "test-ns",
				},
			},
			setupClient: func() kubernetes.Interface {
				pvc := &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pvc",
						Namespace: "test-ns",
					},
				}
				client := fake.NewSimpleClientset(pvc)
				client.PrependReactor("update", "persistentvolumeclaims", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
					return true, nil, fmt.Errorf("API error")
				})
				return client
			},
			expectedError: true,
			errorType:     ErrFinalizerOperationFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := tt.setupClient()
			pvcTracker := &mockPVCTracker{}
			fsMgr := &mockNamespaceFileSystemManager{}

			mgr := NewConfigMapFinalizerManager(client, "test-cluster", pvcTracker, fsMgr)

			ctx := context.Background()
			err := mgr.AddFinalizer(ctx, tt.pvc)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got nil")
				} else if tt.errorType != "" {
					if !IsEFSNSError(err, tt.errorType) {
						t.Errorf("Expected error type %v, got %v", tt.errorType, err)
					}
				}
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestRemoveFinalizer(t *testing.T) {
	tests := []struct {
		name          string
		pvc           *corev1.PersistentVolumeClaim
		setupClient   func() kubernetes.Interface
		expectedError bool
		errorType     EFSNSErrorType
	}{
		{
			name:          "nil PVC",
			pvc:           nil,
			setupClient:   func() kubernetes.Interface { return fake.NewSimpleClientset() },
			expectedError: true,
			errorType:     ErrInvalidParameter,
		},
		{
			name: "PVC not found",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "test-ns",
				},
			},
			setupClient: func() kubernetes.Interface {
				return fake.NewSimpleClientset()
			},
			expectedError: false,
		},
		{
			name: "finalizer not present",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "test-ns",
				},
			},
			setupClient: func() kubernetes.Interface {
				pvc := &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pvc",
						Namespace: "test-ns",
					},
				}
				return fake.NewSimpleClientset(pvc)
			},
			expectedError: false,
		},
		{
			name: "successful finalizer removal",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-pvc",
					Namespace:  "test-ns",
					Finalizers: []string{EFSNSFinalizerName},
				},
			},
			setupClient: func() kubernetes.Interface {
				pvc := &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:       "test-pvc",
						Namespace:  "test-ns",
						Finalizers: []string{EFSNSFinalizerName, "other-finalizer"},
					},
				}
				return fake.NewSimpleClientset(pvc)
			},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := tt.setupClient()
			pvcTracker := &mockPVCTracker{}
			fsMgr := &mockNamespaceFileSystemManager{}

			mgr := NewConfigMapFinalizerManager(client, "test-cluster", pvcTracker, fsMgr)

			ctx := context.Background()
			err := mgr.RemoveFinalizer(ctx, tt.pvc)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got nil")
				} else if tt.errorType != "" {
					if !IsEFSNSError(err, tt.errorType) {
						t.Errorf("Expected error type %v, got %v", tt.errorType, err)
					}
				}
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestAddNamespaceFinalizer(t *testing.T) {
	tests := []struct {
		name          string
		namespace     string
		setupClient   func() kubernetes.Interface
		expectedError bool
		errorType     EFSNSErrorType
	}{
		{
			name:          "empty namespace",
			namespace:     "",
			setupClient:   func() kubernetes.Interface { return fake.NewSimpleClientset() },
			expectedError: true,
			errorType:     ErrInvalidParameter,
		},
		{
			name:      "namespace not found",
			namespace: "test-ns",
			setupClient: func() kubernetes.Interface {
				return fake.NewSimpleClientset()
			},
			expectedError: false,
		},
		{
			name:      "finalizer already exists",
			namespace: "test-ns",
			setupClient: func() kubernetes.Interface {
				ns := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name:       "test-ns",
						Finalizers: []string{EFSNSNamespaceFinalizerName},
					},
				}
				return fake.NewSimpleClientset(ns)
			},
			expectedError: false,
		},
		{
			name:      "successful finalizer addition",
			namespace: "test-ns",
			setupClient: func() kubernetes.Interface {
				ns := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-ns",
					},
				}
				return fake.NewSimpleClientset(ns)
			},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := tt.setupClient()
			pvcTracker := &mockPVCTracker{}
			fsMgr := &mockNamespaceFileSystemManager{}

			mgr := NewConfigMapFinalizerManager(client, "test-cluster", pvcTracker, fsMgr)

			ctx := context.Background()
			err := mgr.AddNamespaceFinalizer(ctx, tt.namespace)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got nil")
				} else if tt.errorType != "" {
					if !IsEFSNSError(err, tt.errorType) {
						t.Errorf("Expected error type %v, got %v", tt.errorType, err)
					}
				}
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestRemoveNamespaceFinalizer(t *testing.T) {
	tests := []struct {
		name          string
		namespace     string
		setupClient   func() kubernetes.Interface
		expectedError bool
		errorType     EFSNSErrorType
	}{
		{
			name:          "empty namespace",
			namespace:     "",
			setupClient:   func() kubernetes.Interface { return fake.NewSimpleClientset() },
			expectedError: true,
			errorType:     ErrInvalidParameter,
		},
		{
			name:      "namespace not found",
			namespace: "test-ns",
			setupClient: func() kubernetes.Interface {
				return fake.NewSimpleClientset()
			},
			expectedError: false,
		},
		{
			name:      "finalizer not present",
			namespace: "test-ns",
			setupClient: func() kubernetes.Interface {
				ns := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-ns",
					},
				}
				return fake.NewSimpleClientset(ns)
			},
			expectedError: false,
		},
		{
			name:      "successful finalizer removal",
			namespace: "test-ns",
			setupClient: func() kubernetes.Interface {
				ns := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name:       "test-ns",
						Finalizers: []string{EFSNSNamespaceFinalizerName, "other-finalizer"},
					},
				}
				return fake.NewSimpleClientset(ns)
			},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := tt.setupClient()
			pvcTracker := &mockPVCTracker{}
			fsMgr := &mockNamespaceFileSystemManager{}

			mgr := NewConfigMapFinalizerManager(client, "test-cluster", pvcTracker, fsMgr)

			ctx := context.Background()
			err := mgr.RemoveNamespaceFinalizer(ctx, tt.namespace)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got nil")
				} else if tt.errorType != "" {
					if !IsEFSNSError(err, tt.errorType) {
						t.Errorf("Expected error type %v, got %v", tt.errorType, err)
					}
				}
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestProcessPVCFinalization(t *testing.T) {
	tests := []struct {
		name               string
		pvc                *corev1.PersistentVolumeClaim
		setupClient        func() kubernetes.Interface
		setupPVCTracker    func() *mockPVCTracker
		setupFileSystemMgr func() *mockNamespaceFileSystemManager
		expectedError      bool
		errorType          EFSNSErrorType
	}{
		{
			name: "PVC without our finalizer",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-pvc",
					Namespace:  "test-ns",
					Finalizers: []string{"other-finalizer"},
				},
			},
			setupClient:        func() kubernetes.Interface { return fake.NewSimpleClientset() },
			setupPVCTracker:    func() *mockPVCTracker { return &mockPVCTracker{} },
			setupFileSystemMgr: func() *mockNamespaceFileSystemManager { return &mockNamespaceFileSystemManager{} },
			expectedError:      false,
		},
		{
			name: "PVC with our finalizer - not last PVC",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-pvc",
					Namespace:  "test-ns",
					Finalizers: []string{EFSNSFinalizerName},
					Annotations: map[string]string{
						"volume.kubernetes.io/storage-provisioner": "test-volume-id",
					},
				},
			},
			setupClient: func() kubernetes.Interface {
				pvc := &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:       "test-pvc",
						Namespace:  "test-ns",
						Finalizers: []string{EFSNSFinalizerName},
					},
				}
				return fake.NewSimpleClientset(pvc)
			},
			setupPVCTracker: func() *mockPVCTracker {
				return &mockPVCTracker{
					removePVCFunc: func(ctx context.Context, namespace, pvcName string) (bool, error) {
						return false, nil // Not the last PVC
					},
				}
			},
			setupFileSystemMgr: func() *mockNamespaceFileSystemManager {
				return &mockNamespaceFileSystemManager{}
			},
			expectedError: false,
		},
		{
			name: "PVC with our finalizer - last PVC triggers cleanup",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-pvc",
					Namespace:  "test-ns",
					Finalizers: []string{EFSNSFinalizerName},
					Annotations: map[string]string{
						"volume.kubernetes.io/storage-provisioner": "test-volume-id",
					},
				},
			},
			setupClient: func() kubernetes.Interface {
				pvc := &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:       "test-pvc",
						Namespace:  "test-ns",
						Finalizers: []string{EFSNSFinalizerName},
					},
				}
				return fake.NewSimpleClientset(pvc)
			},
			setupPVCTracker: func() *mockPVCTracker {
				return &mockPVCTracker{
					removePVCFunc: func(ctx context.Context, namespace, pvcName string) (bool, error) {
						return true, nil // Last PVC
					},
				}
			},
			setupFileSystemMgr: func() *mockNamespaceFileSystemManager {
				return &mockNamespaceFileSystemManager{
					deleteFunc: func(ctx context.Context, namespace string, volumeID string) error {
						return nil
					},
				}
			},
			expectedError: false,
		},
		{
			name: "PVC cleanup with filesystem deletion error",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-pvc",
					Namespace:  "test-ns",
					Finalizers: []string{EFSNSFinalizerName},
					Annotations: map[string]string{
						"volume.kubernetes.io/storage-provisioner": "test-volume-id",
					},
				},
			},
			setupClient: func() kubernetes.Interface {
				pvc := &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:       "test-pvc",
						Namespace:  "test-ns",
						Finalizers: []string{EFSNSFinalizerName},
					},
				}
				return fake.NewSimpleClientset(pvc)
			},
			setupPVCTracker: func() *mockPVCTracker {
				return &mockPVCTracker{
					removePVCFunc: func(ctx context.Context, namespace, pvcName string) (bool, error) {
						return true, nil
					},
				}
			},
			setupFileSystemMgr: func() *mockNamespaceFileSystemManager {
				return &mockNamespaceFileSystemManager{
					deleteFunc: func(ctx context.Context, namespace string, volumeID string) error {
						return fmt.Errorf("filesystem deletion failed")
					},
				}
			},
			expectedError: false, // Should not error even if cleanup fails
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := tt.setupClient()
			pvcTracker := tt.setupPVCTracker()
			fsMgr := tt.setupFileSystemMgr()

			mgr := NewConfigMapFinalizerManager(client, "test-cluster", pvcTracker, fsMgr)

			ctx := context.Background()
			err := mgr.processPVCFinalization(ctx, tt.pvc)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got nil")
				} else if tt.errorType != "" {
					if !IsEFSNSError(err, tt.errorType) {
						t.Errorf("Expected error type %v, got %v", tt.errorType, err)
					}
				}
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestProcessNamespaceFinalization(t *testing.T) {
	tests := []struct {
		name               string
		namespace          *corev1.Namespace
		setupClient        func() kubernetes.Interface
		setupPVCTracker    func() *mockPVCTracker
		setupFileSystemMgr func() *mockNamespaceFileSystemManager
		expectedError      bool
		errorType          EFSNSErrorType
	}{
		{
			name: "namespace without our finalizer",
			namespace: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-ns",
					Finalizers: []string{"other-finalizer"},
				},
			},
			setupClient:        func() kubernetes.Interface { return fake.NewSimpleClientset() },
			setupPVCTracker:    func() *mockPVCTracker { return &mockPVCTracker{} },
			setupFileSystemMgr: func() *mockNamespaceFileSystemManager { return &mockNamespaceFileSystemManager{} },
			expectedError:      false,
		},
		{
			name: "namespace with PVCs remaining",
			namespace: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-ns",
					Finalizers: []string{EFSNSNamespaceFinalizerName},
				},
			},
			setupClient: func() kubernetes.Interface {
				ns := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name:       "test-ns",
						Finalizers: []string{EFSNSNamespaceFinalizerName},
					},
				}
				return fake.NewSimpleClientset(ns)
			},
			setupPVCTracker: func() *mockPVCTracker {
				return &mockPVCTracker{
					getPVCCountFunc: func(ctx context.Context, namespace string) (int32, error) {
						return 1, nil // Still has PVCs
					},
				}
			},
			setupFileSystemMgr: func() *mockNamespaceFileSystemManager {
				return &mockNamespaceFileSystemManager{}
			},
			expectedError: true,
			errorType:     ErrFinalizerOperationFailed,
		},
		{
			name: "namespace cleanup - no PVCs remaining",
			namespace: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-ns",
					Finalizers: []string{EFSNSNamespaceFinalizerName},
				},
			},
			setupClient: func() kubernetes.Interface {
				ns := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name:       "test-ns",
						Finalizers: []string{EFSNSNamespaceFinalizerName},
					},
				}
				return fake.NewSimpleClientset(ns)
			},
			setupPVCTracker: func() *mockPVCTracker {
				return &mockPVCTracker{
					getPVCCountFunc: func(ctx context.Context, namespace string) (int32, error) {
						return 0, nil // No PVCs remaining
					},
				}
			},
			setupFileSystemMgr: func() *mockNamespaceFileSystemManager {
				return &mockNamespaceFileSystemManager{
					getFileSystemInfoFunc: func(ctx context.Context, namespace string) (*FileSystemInfo, error) {
						return &FileSystemInfo{
							FileSystemID: "fs-123",
							Namespace:    namespace,
						}, nil
					},
					deleteFunc: func(ctx context.Context, namespace string, volumeID string) error {
						return nil
					},
				}
			},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := tt.setupClient()
			pvcTracker := tt.setupPVCTracker()
			fsMgr := tt.setupFileSystemMgr()

			mgr := NewConfigMapFinalizerManager(client, "test-cluster", pvcTracker, fsMgr)

			ctx := context.Background()
			err := mgr.processNamespaceFinalization(ctx, tt.namespace)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got nil")
				} else if tt.errorType != "" {
					if !IsEFSNSError(err, tt.errorType) {
						t.Errorf("Expected error type %v, got %v", tt.errorType, err)
					}
				}
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestProcessFinalization(t *testing.T) {
	client := fake.NewSimpleClientset()
	pvcTracker := &mockPVCTracker{}
	fsMgr := &mockNamespaceFileSystemManager{}

	mgr := NewConfigMapFinalizerManager(client, "test-cluster", pvcTracker, fsMgr)

	tests := []struct {
		name          string
		object        interface{}
		expectedError bool
		errorType     EFSNSErrorType
	}{
		{
			name:          "nil object",
			object:        nil,
			expectedError: true,
			errorType:     ErrInvalidParameter,
		},
		{
			name: "PVC object",
			object: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "test-ns",
				},
			},
			expectedError: false,
		},
		{
			name: "Namespace object",
			object: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-ns",
				},
			},
			expectedError: false,
		},
		{
			name: "unsupported object type",
			object: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "test-ns",
				},
			},
			expectedError: true,
			errorType:     ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			err := mgr.ProcessFinalization(ctx, tt.object)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got nil")
				} else if tt.errorType != "" {
					if !IsEFSNSError(err, tt.errorType) {
						t.Errorf("Expected error type %v, got %v", tt.errorType, err)
					}
				}
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestRetryOperation(t *testing.T) {
	client := fake.NewSimpleClientset()
	pvcTracker := &mockPVCTracker{}
	fsMgr := &mockNamespaceFileSystemManager{}

	mgr := NewConfigMapFinalizerManager(client, "test-cluster", pvcTracker, fsMgr)

	tests := []struct {
		name          string
		operation     func() error
		setupContext  func() (context.Context, context.CancelFunc)
		expectedError bool
		errorType     EFSNSErrorType
	}{
		{
			name: "successful operation",
			operation: func() error {
				return nil
			},
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 5*time.Second)
			},
			expectedError: false,
		},
		{
			name: "non-retryable error",
			operation: func() error {
				return fmt.Errorf("non-retryable error")
			},
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 5*time.Second)
			},
			expectedError: true,
			errorType:     ErrFinalizerOperationFailed,
		},
		{
			name: "retryable error - eventual success",
			operation: (func() func() error {
				// Use a counter to simulate success on third attempt
				count := 0
				return func() error {
					count++
					if count < 3 {
						return apierrors.NewConflict(schema.GroupResource{}, "test", fmt.Errorf("conflict"))
					}
					return nil
				}
			})(),
			setupContext: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 10*time.Second)
			},
			expectedError: false,
		},
		{
			name: "context canceled",
			operation: func() error {
				// Return a retryable error to trigger retry logic, where context cancellation will happen
				return apierrors.NewConflict(schema.GroupResource{}, "test", fmt.Errorf("conflict"))
			},
			setupContext: func() (context.Context, context.CancelFunc) {
				// Set very short timeout so context will be canceled during retry wait
				return context.WithTimeout(context.Background(), 1*time.Millisecond)
			},
			expectedError: true,
			errorType:     ErrTimeout,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := tt.setupContext()
			defer cancel()

			err := mgr.retryOperation(ctx, tt.operation)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got nil")
				} else if tt.errorType != "" {
					if !IsEFSNSError(err, tt.errorType) {
						t.Errorf("Expected error type %v, got %v", tt.errorType, err)
					}
				}
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}
