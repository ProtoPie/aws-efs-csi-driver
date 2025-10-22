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
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/cloud"
	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/driver/mocks"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

func TestNamespaceEFSMapper_DescribeFileSystems(t *testing.T) {
	testCases := []struct {
		name                  string
		mockCloudResponse     []*cloud.FileSystem
		mockCloudNextToken    string
		mockCloudError        error
		expectedCount         int
		expectNamespaceFS     bool
		expectError           bool
	}{
		{
			name: "Success: Return namespace-provisioned filesystems",
			mockCloudResponse: []*cloud.FileSystem{
				{
					FileSystemId:    "fs-namespace-1",
					LifeCycleState:  "available",
					CreationTime:    timePtr(time.Now()),
					PerformanceMode: "generalPurpose",
					ThroughputMode:  "bursting",
					Encrypted:       true,
					Tags: map[string]string{
						"kubernetes.io/provisioning-mode": "efs-ns",
						"kubernetes.io/namespace":          "default",
						"kubernetes.io/cluster/test":       "owned",
					},
				},
				{
					FileSystemId:    "fs-namespace-2",
					LifeCycleState:  "available",
					PerformanceMode: "generalPurpose",
					ThroughputMode:  "bursting",
					Encrypted:       true,
					Tags: map[string]string{
						"kubernetes.io/provisioning-mode": "efs-ns",
						"kubernetes.io/namespace":          "kube-system",
						"kubernetes.io/cluster/test":       "owned",
					},
				},
				{
					FileSystemId:    "fs-regular-1",
					LifeCycleState:  "available",
					PerformanceMode: "generalPurpose",
					ThroughputMode:  "bursting",
					Encrypted:       false,
					Tags: map[string]string{
						"Name": "regular-efs",
					},
				},
			},
			expectedCount:     2, // Only namespace-provisioned filesystems
			expectNamespaceFS: true,
			expectError:       false,
		},
		{
			name: "Success: Return all filesystems when no namespace-provisioned ones exist",
			mockCloudResponse: []*cloud.FileSystem{
				{
					FileSystemId:    "fs-regular-1",
					LifeCycleState:  "available",
					PerformanceMode: "generalPurpose",
					ThroughputMode:  "bursting",
					Tags: map[string]string{
						"Name": "regular-efs-1",
					},
				},
				{
					FileSystemId:    "fs-regular-2",
					LifeCycleState:  "creating",
					PerformanceMode: "maxIO",
					ThroughputMode:  "provisioned",
					Tags: map[string]string{
						"Name": "regular-efs-2",
					},
				},
			},
			expectedCount:     2, // All filesystems
			expectNamespaceFS: false,
			expectError:       false,
		},
		{
			name:                  "Success: Empty filesystem list",
			mockCloudResponse:     []*cloud.FileSystem{},
			expectedCount:         0,
			expectNamespaceFS:     false,
			expectError:           false,
		},
		{
			name: "Success: Filesystems with no tags",
			mockCloudResponse: []*cloud.FileSystem{
				{
					FileSystemId:    "fs-no-tags",
					LifeCycleState:  "available",
					PerformanceMode: "generalPurpose",
					ThroughputMode:  "bursting",
					Tags:            nil,
				},
			},
			expectedCount:     1,
			expectNamespaceFS: false,
			expectError:       false,
		},
		{
			name: "Success: Mixed provisioning modes",
			mockCloudResponse: []*cloud.FileSystem{
				{
					FileSystemId: "fs-efs-ap",
					Tags: map[string]string{
						"kubernetes.io/provisioning-mode": "efs-ap",
						"Name": "access-point-mode",
					},
				},
				{
					FileSystemId: "fs-efs-ns",
					Tags: map[string]string{
						"kubernetes.io/provisioning-mode": "efs-ns",
						"kubernetes.io/namespace":          "test",
					},
				},
				{
					FileSystemId: "fs-unknown",
					Tags: map[string]string{
						"kubernetes.io/provisioning-mode": "unknown",
					},
				},
			},
			expectedCount:     1, // Only efs-ns mode
			expectNamespaceFS: true,
			expectError:       false,
		},
		{
			name:              "Error: Cloud API failure",
			mockCloudError:    errors.New("AWS API error"),
			expectedCount:     0,
			expectNamespaceFS: false,
			expectError:       true,
		},
		{
			name:              "Error: Cloud access denied",
			mockCloudError:    cloud.ErrAccessDenied,
			expectedCount:     0,
			expectNamespaceFS: false,
			expectError:       true,
		},
		{
			name: "Success: Large number of filesystems",
			mockCloudResponse: func() []*cloud.FileSystem {
				var filesystems []*cloud.FileSystem
				// Create 50 namespace-provisioned filesystems
				for i := 0; i < 50; i++ {
					filesystems = append(filesystems, &cloud.FileSystem{
						FileSystemId: fmt.Sprintf("fs-namespace-%d", i),
						Tags: map[string]string{
							"kubernetes.io/provisioning-mode": "efs-ns",
							"kubernetes.io/namespace":          fmt.Sprintf("namespace-%d", i),
						},
					})
				}
				// Add 50 regular filesystems
				for i := 0; i < 50; i++ {
					filesystems = append(filesystems, &cloud.FileSystem{
						FileSystemId: fmt.Sprintf("fs-regular-%d", i),
						Tags: map[string]string{
							"Name": fmt.Sprintf("regular-%d", i),
						},
					})
				}
				return filesystems
			}(),
			expectedCount:     50, // Only namespace-provisioned ones
			expectNamespaceFS: true,
			expectError:       false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockCloud := mocks.NewMockCloud(ctrl)
			k8sClient := fake.NewSimpleClientset()

			mapper, err := NewNamespaceEFSMapper(k8sClient, &rest.Config{}, mockCloud)
			if err != nil {
				t.Fatalf("Failed to create mapper: %v", err)
			}

			ctx := context.Background()

			// Set up mock expectations
			if tc.mockCloudError != nil {
				mockCloud.EXPECT().
					DescribeFileSystems(gomock.Eq(ctx), gomock.Eq(""), gomock.Eq(int32(100))).
					Return(nil, "", tc.mockCloudError)
			} else {
				mockCloud.EXPECT().
					DescribeFileSystems(gomock.Eq(ctx), gomock.Eq(""), gomock.Eq(int32(100))).
					Return(tc.mockCloudResponse, tc.mockCloudNextToken, nil)
			}

			// Execute
			filesystems, err := mapper.DescribeFileSystems(ctx)

			// Verify
			if tc.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}

				if len(filesystems) != tc.expectedCount {
					t.Errorf("Expected %d filesystems, got %d", tc.expectedCount, len(filesystems))
				}

				// Verify that only namespace-provisioned filesystems are returned when they exist
				if tc.expectNamespaceFS && len(filesystems) > 0 {
					for _, fs := range filesystems {
						if fs.Tags == nil {
							t.Errorf("Expected namespace filesystem to have tags")
							continue
						}
						if mode, ok := fs.Tags["kubernetes.io/provisioning-mode"]; !ok || mode != "efs-ns" {
							t.Errorf("Expected all returned filesystems to be namespace-provisioned, got: %v", fs.Tags)
						}
					}
				}
			}
		})
	}
}

// TestNamespaceEFSMapper_DescribeFileSystems_Caching tests caching behavior
func TestNamespaceEFSMapper_DescribeFileSystems_Caching(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCloud := mocks.NewMockCloud(ctrl)
	k8sClient := fake.NewSimpleClientset()

	mapper, err := NewNamespaceEFSMapper(k8sClient, &rest.Config{}, mockCloud)
	if err != nil {
		t.Fatalf("Failed to create mapper: %v", err)
	}

	ctx := context.Background()

	mockResponse := []*cloud.FileSystem{
		{
			FileSystemId: "fs-cached-1",
			Tags: map[string]string{
				"kubernetes.io/provisioning-mode": "efs-ns",
				"kubernetes.io/namespace":          "cached-ns",
			},
		},
	}

	// Expect multiple calls - caching is not implemented for DescribeFileSystems
	// as it needs to provide real-time filesystem availability
	mockCloud.EXPECT().
		DescribeFileSystems(gomock.Eq(ctx), gomock.Eq(""), gomock.Eq(int32(100))).
		Return(mockResponse, "", nil).
		Times(3)

	// Make multiple calls
	for i := 0; i < 3; i++ {
		filesystems, err := mapper.DescribeFileSystems(ctx)
		if err != nil {
			t.Errorf("Call %d: Unexpected error: %v", i, err)
		}
		if len(filesystems) != 1 {
			t.Errorf("Call %d: Expected 1 filesystem, got %d", i, len(filesystems))
		}
	}
}

// TestNamespaceEFSMapper_DescribeFileSystems_ConcurrentAccess tests thread safety
func TestNamespaceEFSMapper_DescribeFileSystems_ConcurrentAccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCloud := mocks.NewMockCloud(ctrl)
	k8sClient := fake.NewSimpleClientset()

	mapper, err := NewNamespaceEFSMapper(k8sClient, &rest.Config{}, mockCloud)
	if err != nil {
		t.Fatalf("Failed to create mapper: %v", err)
	}

	ctx := context.Background()

	mockResponse := []*cloud.FileSystem{
		{
			FileSystemId: "fs-concurrent",
			Tags: map[string]string{
				"kubernetes.io/provisioning-mode": "efs-ns",
			},
		},
	}

	// Set up expectations for concurrent calls
	mockCloud.EXPECT().
		DescribeFileSystems(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(mockResponse, "", nil).
		AnyTimes()

	// Run concurrent calls
	concurrency := 10
	done := make(chan bool, concurrency)
	errors := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		go func(id int) {
			defer func() { done <- true }()

			filesystems, err := mapper.DescribeFileSystems(ctx)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: %v", id, err)
				return
			}
			if len(filesystems) != 1 {
				errors <- fmt.Errorf("goroutine %d: expected 1 filesystem, got %d", id, len(filesystems))
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < concurrency; i++ {
		<-done
	}

	// Check for errors
	close(errors)
	for err := range errors {
		t.Errorf("Concurrent access error: %v", err)
	}
}

// TestNamespaceEFSMapper_DescribeFileSystems_FilteringLogic tests the filtering logic
func TestNamespaceEFSMapper_DescribeFileSystems_FilteringLogic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCloud := mocks.NewMockCloud(ctrl)
	k8sClient := fake.NewSimpleClientset()

	mapper, err := NewNamespaceEFSMapper(k8sClient, &rest.Config{}, mockCloud)
	if err != nil {
		t.Fatalf("Failed to create mapper: %v", err)
	}

	ctx := context.Background()

	// Test case 1: Correct filtering of namespace-provisioned filesystems
	mockCloud.EXPECT().
		DescribeFileSystems(gomock.Eq(ctx), gomock.Eq(""), gomock.Eq(int32(100))).
		Return([]*cloud.FileSystem{
			{
				FileSystemId: "fs-1",
				Tags: map[string]string{
					"kubernetes.io/provisioning-mode": "efs-ns",
				},
			},
			{
				FileSystemId: "fs-2",
				Tags: map[string]string{
					"kubernetes.io/provisioning-mode": "efs-ap",
				},
			},
			{
				FileSystemId: "fs-3",
				Tags: map[string]string{
					"kubernetes.io/provisioning-mode": "efs-ns",
				},
			},
		}, "", nil)

	filesystems, err := mapper.DescribeFileSystems(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Should return only fs-1 and fs-3
	if len(filesystems) != 2 {
		t.Errorf("Expected 2 namespace-provisioned filesystems, got %d", len(filesystems))
	}

	expectedIds := map[string]bool{"fs-1": true, "fs-3": true}
	for _, fs := range filesystems {
		if !expectedIds[fs.FileSystemId] {
			t.Errorf("Unexpected filesystem ID: %s", fs.FileSystemId)
		}
	}
}

// Helper function to create time pointer
func timePtr(t time.Time) *time.Time {
	return &t
}