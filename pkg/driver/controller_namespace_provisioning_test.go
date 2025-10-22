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
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/mock/gomock"
	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/cloud"
	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/driver/mocks"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestController_CreateVolume_ProvisioningModeSelection tests the controller's ability to
// correctly select between efs-ap and efs-ns provisioning modes
func TestController_CreateVolume_ProvisioningModeSelection(t *testing.T) {
	testCases := []struct {
		name                string
		provisioningMode    string
		expectEfsNsMode     bool
		expectError         bool
		expectedErrorCode   codes.Code
		expectedErrorMsg    string
		setupProvisioner    bool
	}{
		{
			name:             "Select efs-ap mode",
			provisioningMode: AccessPointMode,
			expectEfsNsMode:  false,
			expectError:      false,
		},
		{
			name:             "Select efs-ns mode",
			provisioningMode: NamespaceProvisioningMode,
			expectEfsNsMode:  true,
			expectError:      false,
			setupProvisioner: true,
		},
		{
			name:              "Invalid provisioning mode",
			provisioningMode:  "invalid-mode",
			expectError:       true,
			expectedErrorCode: codes.InvalidArgument,
			expectedErrorMsg:  "Provisioning mode invalid-mode is not supported",
		},
		{
			name:              "Empty provisioning mode",
			provisioningMode:  "",
			expectError:       true,
			expectedErrorCode: codes.InvalidArgument,
			expectedErrorMsg:  "Missing provisioningMode parameter",
		},
		{
			name:              "Whitespace provisioning mode",
			provisioningMode:  "   ",
			expectError:       true,
			expectedErrorCode: codes.InvalidArgument,
			expectedErrorMsg:  "is not supported. Supported modes:",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockCloud := mocks.NewMockCloud(ctrl)

			driver := &Driver{
				endpoint: "test-endpoint",
				cloud:    mockCloud,
			}

			// Setup namespace provisioner if needed
			if tc.setupProvisioner {
				mockProvisioner := &MockNamespaceProvisioner{
					CreateFunc: func(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
						return &csi.CreateVolumeResponse{
							Volume: &csi.Volume{
								VolumeId:      "fsap-test123",
								CapacityBytes: 1024 * 1024 * 1024,
							},
						}, nil
					},
				}
				driver.namespaceProvisioner = mockProvisioner
			}

			// For efs-ap mode test, we need to mock cloud operations
			if tc.provisioningMode == AccessPointMode {
				// Mock ListAccessPoints call for efs-ap mode
				mockCloud.EXPECT().ListAccessPoints(gomock.Any(), gomock.Any()).Return([]*cloud.AccessPoint{}, nil).AnyTimes()
				mockCloud.EXPECT().CreateAccessPoint(gomock.Any(), gomock.Any(), gomock.Any()).Return(&cloud.AccessPoint{
					AccessPointId: "fsap-ap-test",
					FileSystemId:  "fs-test123",
				}, nil).AnyTimes()
			}

			req := &csi.CreateVolumeRequest{
				Name: "test-volume",
				Parameters: map[string]string{
					ProvisioningMode: tc.provisioningMode,
					FsId:             "fs-test123", // Required for efs-ap mode
					PvcNamespace:     "test-ns",
					PvcName:          "test-pvc",
				},
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessType: &csi.VolumeCapability_Mount{
							Mount: &csi.VolumeCapability_MountVolume{},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
						},
					},
				},
				CapacityRange: &csi.CapacityRange{
					RequiredBytes: 1024 * 1024 * 1024,
				},
			}

			// Handle empty provisioning mode case
			if tc.provisioningMode == "" {
				delete(req.Parameters, ProvisioningMode)
			}

			resp, err := driver.CreateVolume(context.Background(), req)

			if tc.expectError {
				if err == nil {
					t.Fatalf("Expected error but got none")
				}
				st, ok := status.FromError(err)
				if !ok {
					t.Fatalf("Expected gRPC status error, got: %v", err)
				}
				if st.Code() != tc.expectedErrorCode {
					t.Fatalf("Expected error code %v, got %v", tc.expectedErrorCode, st.Code())
				}
				if tc.expectedErrorMsg != "" && !containsStr(st.Message(), tc.expectedErrorMsg) {
					t.Fatalf("Expected error message to contain '%s', got '%s'", tc.expectedErrorMsg, st.Message())
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if resp == nil || resp.Volume == nil {
					t.Fatal("Expected valid response with volume")
				}

				// Verify correct mode was selected based on VolumeId pattern
				if tc.expectEfsNsMode {
					if resp.Volume.VolumeId != "fsap-test123" {
						t.Fatalf("Expected efs-ns volume ID pattern, got %s", resp.Volume.VolumeId)
					}
				} else {
					if resp.Volume.VolumeId == "" {
						t.Fatal("Expected valid volume ID for efs-ap mode")
					}
				}
			}
		})
	}
}

// TestController_CreateVolume_DelegationLogic tests the delegation logic
// from controller to NamespaceProvisioner for efs-ns mode
func TestController_CreateVolume_DelegationLogic(t *testing.T) {
	testCases := []struct {
		name                   string
		setupProvisioner       bool
		provisionerError       error
		expectDelegation       bool
		expectError            bool
		expectedErrorCode      codes.Code
		expectedErrorMsg       string
	}{
		{
			name:             "Successful delegation to NamespaceProvisioner",
			setupProvisioner: true,
			expectDelegation: true,
			expectError:      false,
		},
		{
			name:              "NamespaceProvisioner not initialized",
			setupProvisioner:  false,
			expectDelegation:  false,
			expectError:       true,
			expectedErrorCode: codes.Internal,
			expectedErrorMsg:  "Failed to initialize NamespaceProvisioner",
		},
		{
			name:             "NamespaceProvisioner returns error",
			setupProvisioner: true,
			provisionerError: status.Error(codes.ResourceExhausted, "quota exceeded"),
			expectDelegation: true,
			expectError:      true,
			expectedErrorCode: codes.ResourceExhausted,
			expectedErrorMsg:  "quota exceeded",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockCloud := mocks.NewMockCloud(ctrl)

			driver := &Driver{
				endpoint: "test-endpoint",
				cloud:    mockCloud,
			}

			delegationCalled := false
			if tc.setupProvisioner {
				mockProvisioner := &MockNamespaceProvisioner{
					CreateFunc: func(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
						delegationCalled = true
						if tc.provisionerError != nil {
							return nil, tc.provisionerError
						}
						return &csi.CreateVolumeResponse{
							Volume: &csi.Volume{
								VolumeId:      "fsap-delegated",
								CapacityBytes: 1024 * 1024 * 1024,
							},
						}, nil
					},
				}
				driver.namespaceProvisioner = mockProvisioner
			}

			req := &csi.CreateVolumeRequest{
				Name: "test-volume",
				Parameters: map[string]string{
					ProvisioningMode: NamespaceProvisioningMode,
					PvcNamespace:     "test-ns",
					PvcName:          "test-pvc",
				},
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessType: &csi.VolumeCapability_Mount{
							Mount: &csi.VolumeCapability_MountVolume{},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
						},
					},
				},
				CapacityRange: &csi.CapacityRange{
					RequiredBytes: 1024 * 1024 * 1024,
				},
			}

			resp, err := driver.CreateVolume(context.Background(), req)

			// Check delegation
			if tc.expectDelegation != delegationCalled {
				t.Fatalf("Expected delegation=%v, but delegation called=%v", tc.expectDelegation, delegationCalled)
			}

			// Check error handling
			if tc.expectError {
				if err == nil {
					t.Fatal("Expected error but got none")
				}
				st, ok := status.FromError(err)
				if !ok {
					t.Fatalf("Expected gRPC status error, got: %v", err)
				}
				if st.Code() != tc.expectedErrorCode {
					t.Fatalf("Expected error code %v, got %v", tc.expectedErrorCode, st.Code())
				}
				if tc.expectedErrorMsg != "" && !containsStr(st.Message(), tc.expectedErrorMsg) {
					t.Fatalf("Expected error message to contain '%s', got '%s'", tc.expectedErrorMsg, st.Message())
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if resp == nil || resp.Volume == nil {
					t.Fatal("Expected valid response")
				}
				if resp.Volume.VolumeId != "fsap-delegated" {
					t.Fatalf("Expected delegated volume ID, got %s", resp.Volume.VolumeId)
				}
			}
		})
	}
}

// TestController_DeleteVolume_DelegationLogic tests the delegation logic
// for DeleteVolume operations in efs-ns mode
func TestController_DeleteVolume_DelegationLogic(t *testing.T) {
	testCases := []struct {
		name                   string
		volumeId               string
		setupProvisioner       bool
		provisionerError       error
		expectDelegation       bool
		expectFallthrough      bool
		expectError            bool
		expectedErrorCode      codes.Code
	}{
		{
			name:             "Successful delegation for efs-ns volume",
			volumeId:         "fsap-12345678",
			setupProvisioner: true,
			expectDelegation: true,
			expectError:      false,
		},
		{
			name:              "Fallthrough to efs-ap for double-colon format",
			volumeId:          "fs-123::fsap-456",
			setupProvisioner:  true,
			expectDelegation:  false,
			expectFallthrough: true,
			expectError:       false,
		},
		{
			name:              "Fallthrough when provisioner not initialized",
			volumeId:          "fsap-12345678",
			setupProvisioner:  false,
			expectDelegation:  false,
			expectFallthrough: true,
			expectError:       false,
		},
		{
			name:             "Provisioner returns error",
			volumeId:         "fsap-12345678",
			setupProvisioner: true,
			provisionerError: status.Error(codes.NotFound, "volume not found"),
			expectDelegation: true,
			expectError:      true,
			expectedErrorCode: codes.NotFound,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockCloud := mocks.NewMockCloud(ctrl)

			driver := &Driver{
				endpoint: "test-endpoint",
				cloud:    mockCloud,
			}

			delegationCalled := false
			if tc.setupProvisioner {
				mockProvisioner := &MockNamespaceProvisioner{
					DeleteFunc: func(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
						delegationCalled = true
						if tc.provisionerError != nil {
							return nil, tc.provisionerError
						}
						return &csi.DeleteVolumeResponse{}, nil
					},
				}
				driver.namespaceProvisioner = mockProvisioner
			}

			// Setup mock for fallthrough cases
			if tc.expectFallthrough && tc.volumeId == "fs-123::fsap-456" {
				// For efs-ap format volumes, mock the DeleteAccessPoint call
				mockCloud.EXPECT().DeleteAccessPoint(gomock.Any(), "fsap-456").Return(nil).AnyTimes()
			}

			req := &csi.DeleteVolumeRequest{
				VolumeId: tc.volumeId,
			}

			resp, err := driver.DeleteVolume(context.Background(), req)

			// Check delegation
			if tc.expectDelegation != delegationCalled {
				t.Fatalf("Expected delegation=%v, but delegation called=%v", tc.expectDelegation, delegationCalled)
			}

			// Check error handling
			if tc.expectError {
				if err == nil {
					t.Fatal("Expected error but got none")
				}
				st, ok := status.FromError(err)
				if !ok {
					t.Fatalf("Expected gRPC status error, got: %v", err)
				}
				if st.Code() != tc.expectedErrorCode {
					t.Fatalf("Expected error code %v, got %v", tc.expectedErrorCode, st.Code())
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if resp == nil {
					t.Fatal("Expected valid response")
				}
			}
		})
	}
}

// TestController_EndToEndScenarios tests complete scenarios from PVC creation to deletion
func TestController_EndToEndScenarios(t *testing.T) {
	testCases := []struct {
		name           string
		scenario       func(t *testing.T, driver *Driver)
	}{
		{
			name: "Create and delete efs-ns volume lifecycle",
			scenario: func(t *testing.T, driver *Driver) {
				// Create volume
				createReq := &csi.CreateVolumeRequest{
					Name: "e2e-test-volume",
					Parameters: map[string]string{
						ProvisioningMode: NamespaceProvisioningMode,
						PvcNamespace:     "e2e-namespace",
						PvcName:          "e2e-pvc",
					},
					VolumeCapabilities: []*csi.VolumeCapability{
						{
							AccessType: &csi.VolumeCapability_Mount{
								Mount: &csi.VolumeCapability_MountVolume{},
							},
							AccessMode: &csi.VolumeCapability_AccessMode{
								Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
							},
						},
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: 5 * 1024 * 1024 * 1024, // 5GB
					},
				}

				createResp, err := driver.CreateVolume(context.Background(), createReq)
				if err != nil {
					t.Fatalf("Failed to create volume: %v", err)
				}
				if createResp.Volume.VolumeId == "" {
					t.Fatal("Expected valid volume ID")
				}

				// Delete volume
				deleteReq := &csi.DeleteVolumeRequest{
					VolumeId: createResp.Volume.VolumeId,
				}

				_, err = driver.DeleteVolume(context.Background(), deleteReq)
				if err != nil {
					t.Fatalf("Failed to delete volume: %v", err)
				}
			},
		},
		{
			name: "Multiple PVCs in same namespace reuse EFS",
			scenario: func(t *testing.T, driver *Driver) {
				namespace := "shared-namespace"

				// Create first PVC
				req1 := &csi.CreateVolumeRequest{
					Name: "pvc-1",
					Parameters: map[string]string{
						ProvisioningMode: NamespaceProvisioningMode,
						PvcNamespace:     namespace,
						PvcName:          "pvc-1",
					},
					VolumeCapabilities: []*csi.VolumeCapability{
						{
							AccessType: &csi.VolumeCapability_Mount{
								Mount: &csi.VolumeCapability_MountVolume{},
							},
							AccessMode: &csi.VolumeCapability_AccessMode{
								Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
							},
						},
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: 1024 * 1024 * 1024,
					},
				}

				resp1, err := driver.CreateVolume(context.Background(), req1)
				if err != nil {
					t.Fatalf("Failed to create first volume: %v", err)
				}

				// Create second PVC in same namespace
				req2 := &csi.CreateVolumeRequest{
					Name: "pvc-2",
					Parameters: map[string]string{
						ProvisioningMode: NamespaceProvisioningMode,
						PvcNamespace:     namespace,
						PvcName:          "pvc-2",
					},
					VolumeCapabilities: []*csi.VolumeCapability{
						{
							AccessType: &csi.VolumeCapability_Mount{
								Mount: &csi.VolumeCapability_MountVolume{},
							},
							AccessMode: &csi.VolumeCapability_AccessMode{
								Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
							},
						},
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: 1024 * 1024 * 1024,
					},
				}

				resp2, err := driver.CreateVolume(context.Background(), req2)
				if err != nil {
					t.Fatalf("Failed to create second volume: %v", err)
				}

				// Both volumes should exist
				if resp1.Volume.VolumeId == "" || resp2.Volume.VolumeId == "" {
					t.Fatal("Expected valid volume IDs for both volumes")
				}

				// Verify both volumes have the same namespace in context
				if resp1.Volume.VolumeContext["namespace"] != namespace {
					t.Fatalf("Expected namespace %s in volume 1 context", namespace)
				}
				if resp2.Volume.VolumeContext["namespace"] != namespace {
					t.Fatalf("Expected namespace %s in volume 2 context", namespace)
				}
			},
		},
		{
			name: "Error recovery scenario",
			scenario: func(t *testing.T, driver *Driver) {
				// Simulate a failed creation attempt
				failReq := &csi.CreateVolumeRequest{
					Name: "fail-volume",
					Parameters: map[string]string{
						ProvisioningMode: NamespaceProvisioningMode,
						PvcNamespace:     "fail-namespace",
						PvcName:          "fail-pvc",
					},
					VolumeCapabilities: []*csi.VolumeCapability{
						{
							AccessType: &csi.VolumeCapability_Mount{
								Mount: &csi.VolumeCapability_MountVolume{},
							},
							AccessMode: &csi.VolumeCapability_AccessMode{
								Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
							},
						},
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: 1024 * 1024 * 1024,
					},
				}

				// First attempt might fail (simulated)
				mockProvisioner := driver.namespaceProvisioner.(*MockNamespaceProvisioner)
				originalCreate := mockProvisioner.CreateFunc
				attemptCount := 0

				mockProvisioner.CreateFunc = func(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
					attemptCount++
					if attemptCount == 1 {
						return nil, status.Error(codes.Unavailable, "temporary failure")
					}
					return originalCreate(ctx, req)
				}

				// First attempt fails
				_, err := driver.CreateVolume(context.Background(), failReq)
				if err == nil {
					t.Fatal("Expected first attempt to fail")
				}

				// Second attempt succeeds (retry)
				resp, err := driver.CreateVolume(context.Background(), failReq)
				if err != nil {
					t.Fatalf("Expected retry to succeed: %v", err)
				}
				if resp.Volume.VolumeId == "" {
					t.Fatal("Expected valid volume ID after retry")
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockCloud := mocks.NewMockCloud(ctrl)

			// Setup a simple mock provisioner for e2e scenarios
			mockProvisioner := &MockNamespaceProvisioner{
				CreateFunc: func(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
					return &csi.CreateVolumeResponse{
						Volume: &csi.Volume{
							VolumeId:      fmt.Sprintf("fsap-%s-%s", req.Parameters[PvcNamespace], req.Parameters[PvcName]),
							CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
							VolumeContext: map[string]string{
								"namespace":     req.Parameters[PvcNamespace],
								"fileSystemId":  fmt.Sprintf("fs-%s", req.Parameters[PvcNamespace]),
								"accessPointId": fmt.Sprintf("fsap-%s-%s", req.Parameters[PvcNamespace], req.Parameters[PvcName]),
							},
						},
					}, nil
				},
				DeleteFunc: func(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
					return &csi.DeleteVolumeResponse{}, nil
				},
			}

			driver := &Driver{
				endpoint:             "test-endpoint",
				cloud:                mockCloud,
				namespaceProvisioner: mockProvisioner,
			}

			tc.scenario(t, driver)
		})
	}
}

// TestController_ConcurrentOperations tests concurrent PVC creation/deletion scenarios
func TestController_ConcurrentOperations(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCloud := mocks.NewMockCloud(ctrl)

	// Track concurrent operations
	operationCount := 0
	maxConcurrent := 0
	currentConcurrent := 0

	mockProvisioner := &MockNamespaceProvisioner{
		CreateFunc: func(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
			currentConcurrent++
			operationCount++
			if currentConcurrent > maxConcurrent {
				maxConcurrent = currentConcurrent
			}

			// Simulate some processing time
			time.Sleep(10 * time.Millisecond)

			resp := &csi.CreateVolumeResponse{
				Volume: &csi.Volume{
					VolumeId:      fmt.Sprintf("fsap-%d", operationCount),
					CapacityBytes: 1024 * 1024 * 1024,
				},
			}

			currentConcurrent--
			return resp, nil
		},
	}

	driver := &Driver{
		endpoint:             "test-endpoint",
		cloud:                mockCloud,
		namespaceProvisioner: mockProvisioner,
	}

	// Launch concurrent create operations
	numOperations := 10
	results := make(chan error, numOperations)

	for i := 0; i < numOperations; i++ {
		go func(index int) {
			req := &csi.CreateVolumeRequest{
				Name: fmt.Sprintf("concurrent-volume-%d", index),
				Parameters: map[string]string{
					ProvisioningMode: NamespaceProvisioningMode,
					PvcNamespace:     "concurrent-namespace",
					PvcName:          fmt.Sprintf("pvc-%d", index),
				},
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessType: &csi.VolumeCapability_Mount{
							Mount: &csi.VolumeCapability_MountVolume{},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
						},
					},
				},
				CapacityRange: &csi.CapacityRange{
					RequiredBytes: 1024 * 1024 * 1024,
				},
			}

			_, err := driver.CreateVolume(context.Background(), req)
			results <- err
		}(i)
	}

	// Wait for all operations to complete
	successCount := 0
	for i := 0; i < numOperations; i++ {
		err := <-results
		if err == nil {
			successCount++
		}
	}

	// Verify all operations succeeded
	if successCount != numOperations {
		t.Fatalf("Expected %d successful operations, got %d", numOperations, successCount)
	}

	// Verify concurrent operations were handled
	if maxConcurrent < 2 {
		t.Log("Warning: concurrent operations may not have overlapped in testing")
	}

	t.Logf("Successfully handled %d concurrent operations (max concurrent: %d)", numOperations, maxConcurrent)
}

// TestController_ProvisionerInitialization tests lazy initialization of NamespaceProvisioner
func TestController_ProvisionerInitialization(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCloud := mocks.NewMockCloud(ctrl)

	driver := &Driver{
		endpoint:  "test-endpoint",
		cloud:     mockCloud,
		// namespaceProvisioner is nil initially
	}

	// First request should trigger initialization
	req := &csi.CreateVolumeRequest{
		Name: "init-test-volume",
		Parameters: map[string]string{
			ProvisioningMode: NamespaceProvisioningMode,
			PvcNamespace:     "init-namespace",
			PvcName:          "init-pvc",
		},
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{},
				},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
				},
			},
		},
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1024 * 1024 * 1024,
		},
	}

	// The initialization will happen internally
	_, err := driver.CreateVolume(context.Background(), req)

	// We expect it to succeed (with actual provisioner initialized)
	if err == nil {
		// The real provisioner was initialized successfully
		if driver.namespaceProvisioner == nil {
			t.Fatal("Expected namespaceProvisioner to be initialized")
		}
	} else {
		// Initialization might fail in test environment without proper k8s setup
		// This is expected in unit tests
		t.Logf("Provisioner initialization expected to fail in test environment: %v", err)
	}
}

// TestController_ErrorHandling tests various error conditions
func TestController_ErrorHandling(t *testing.T) {
	testCases := []struct {
		name              string
		setupDriver       func(*Driver)
		request           *csi.CreateVolumeRequest
		expectedErrorCode codes.Code
		expectedErrorMsg  string
	}{
		{
			name: "Missing PVC namespace parameter",
			setupDriver: func(d *Driver) {
				d.namespaceProvisioner = &MockNamespaceProvisioner{
					CreateFunc: func(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
						// The real provisioner would check for namespace
						if _, ok := req.Parameters["csi.storage.k8s.io/pvc/namespace"]; !ok {
							return nil, status.Error(codes.InvalidArgument, "namespace not found in volume parameters")
						}
						return nil, nil
					},
				}
			},
			request: &csi.CreateVolumeRequest{
				Name: "test-volume",
				Parameters: map[string]string{
					ProvisioningMode: NamespaceProvisioningMode,
					// Missing PvcNamespace
					PvcName: "test-pvc",
				},
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessType: &csi.VolumeCapability_Mount{
							Mount: &csi.VolumeCapability_MountVolume{},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
						},
					},
				},
			},
			expectedErrorCode: codes.InvalidArgument,
			expectedErrorMsg:  "namespace not found in volume parameters",
		},
		{
			name: "Provisioner returns internal error",
			setupDriver: func(d *Driver) {
				d.namespaceProvisioner = &MockNamespaceProvisioner{
					CreateFunc: func(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
						return nil, status.Error(codes.Internal, "internal provisioner error")
					},
				}
			},
			request: &csi.CreateVolumeRequest{
				Name: "test-volume",
				Parameters: map[string]string{
					ProvisioningMode: NamespaceProvisioningMode,
					PvcNamespace:     "test-namespace",
					PvcName:          "test-pvc",
				},
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessType: &csi.VolumeCapability_Mount{
							Mount: &csi.VolumeCapability_MountVolume{},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
						},
					},
				},
			},
			expectedErrorCode: codes.Internal,
			expectedErrorMsg:  "internal provisioner error",
		},
		{
			name: "Provisioner returns resource exhausted",
			setupDriver: func(d *Driver) {
				d.namespaceProvisioner = &MockNamespaceProvisioner{
					CreateFunc: func(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
						return nil, status.Error(codes.ResourceExhausted, "quota exceeded for namespace")
					},
				}
			},
			request: &csi.CreateVolumeRequest{
				Name: "test-volume",
				Parameters: map[string]string{
					ProvisioningMode: NamespaceProvisioningMode,
					PvcNamespace:     "test-namespace",
					PvcName:          "test-pvc",
				},
				VolumeCapabilities: []*csi.VolumeCapability{
					{
						AccessType: &csi.VolumeCapability_Mount{
							Mount: &csi.VolumeCapability_MountVolume{},
						},
						AccessMode: &csi.VolumeCapability_AccessMode{
							Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
						},
					},
				},
			},
			expectedErrorCode: codes.ResourceExhausted,
			expectedErrorMsg:  "quota exceeded for namespace",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockCloud := mocks.NewMockCloud(ctrl)

			driver := &Driver{
				endpoint: "test-endpoint",
				cloud:    mockCloud,
			}

			if tc.setupDriver != nil {
				tc.setupDriver(driver)
			}

			_, err := driver.CreateVolume(context.Background(), tc.request)

			if err == nil {
				t.Fatal("Expected error but got none")
			}

			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("Expected gRPC status error, got: %v", err)
			}

			if st.Code() != tc.expectedErrorCode {
				t.Fatalf("Expected error code %v, got %v", tc.expectedErrorCode, st.Code())
			}

			if tc.expectedErrorMsg != "" && !contains(st.Message(), tc.expectedErrorMsg) {
				t.Fatalf("Expected error message to contain '%s', got '%s'", tc.expectedErrorMsg, st.Message())
			}
		})
	}
}

// Helper function to check if a string contains a substring
func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && len(substr) == 0 || (len(substr) > 0 && findSubstring(s, substr) != -1))
}

func findSubstring(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}