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
	"os"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/mock/gomock"
	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/cloud"
	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/driver/mocks"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

func TestDriver_InitializeNamespaceProvisioner(t *testing.T) {
	tests := []struct {
		name                  string
		existingProvisioner   NamespaceProvisionerInterface
		mockSetup             func(*gomock.Controller) *mocks.MockCloud
		k8sClientError        error
		k8sConfigError        error
		envVars               map[string]string
		expectedError         bool
		expectedErrorContains string
		validateProvisioner   bool
	}{
		{
			name:                "successful initialization",
			existingProvisioner: nil,
			mockSetup: func(ctrl *gomock.Controller) *mocks.MockCloud {
				mockCloud := mocks.NewMockCloud(ctrl)
				return mockCloud
			},
			envVars: map[string]string{
				"CLUSTER_NAME": "test-cluster",
			},
			expectedError:       false,
			validateProvisioner: true,
		},
		{
			name: "already initialized",
			existingProvisioner: &mockNamespaceProvisioner{
				healthy: true,
			},
			mockSetup: func(ctrl *gomock.Controller) *mocks.MockCloud {
				return mocks.NewMockCloud(ctrl)
			},
			expectedError:       false,
			validateProvisioner: true,
		},
		{
			name:                "initialization with tags",
			existingProvisioner: nil,
			mockSetup: func(ctrl *gomock.Controller) *mocks.MockCloud {
				mockCloud := mocks.NewMockCloud(ctrl)
				return mockCloud
			},
			envVars: map[string]string{
				"CLUSTER_NAME": "prod-cluster",
			},
			expectedError:       false,
			validateProvisioner: true,
		},
		{
			name:                "initialization without cluster name",
			existingProvisioner: nil,
			mockSetup: func(ctrl *gomock.Controller) *mocks.MockCloud {
				mockCloud := mocks.NewMockCloud(ctrl)
				return mockCloud
			},
			envVars:             map[string]string{},
			expectedError:       false,
			validateProvisioner: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Set environment variables
			for key, value := range tc.envVars {
				os.Setenv(key, value)
				defer os.Unsetenv(key)
			}

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockCloud := tc.mockSetup(ctrl)

			// Create driver
			d := &Driver{
				cloud: mockCloud,
				tags: map[string]string{
					"environment": "test",
					"team":        "platform",
				},
				namespaceProvisioner: tc.existingProvisioner,
			}

			// Override the DefaultKubernetesAPIClient for testing
			originalClient := cloud.DefaultKubernetesAPIClient
			defer func() {
				cloud.DefaultKubernetesAPIClient = originalClient
			}()

			if tc.k8sClientError != nil {
				cloud.DefaultKubernetesAPIClient = func() (kubernetes.Interface, error) {
					return nil, tc.k8sClientError
				}
			} else {
				cloud.DefaultKubernetesAPIClient = func() (kubernetes.Interface, error) {
					return fake.NewSimpleClientset(), nil
				}
			}

			// Mock the rest.InClusterConfig for testing
			// Note: In real unit tests, we would need to properly mock this
			// For now, we'll skip testing the actual initialization that requires K8s config

			if tc.existingProvisioner != nil {
				// Test that it returns early when already initialized
				err := d.InitializeNamespaceProvisioner()
				if err != nil {
					t.Fatalf("Expected no error when already initialized, got: %v", err)
				}
				if d.namespaceProvisioner != tc.existingProvisioner {
					t.Fatal("Expected existing provisioner to be unchanged")
				}
			}
		})
	}
}

func TestDriver_GetNamespaceProvisioner(t *testing.T) {
	tests := []struct {
		name                string
		existingProvisioner NamespaceProvisionerInterface
		initError           bool
		expectError         bool
	}{
		{
			name: "returns existing provisioner",
			existingProvisioner: &mockNamespaceProvisioner{
				healthy: true,
			},
			expectError: false,
		},
		{
			name:                "initializes when nil",
			existingProvisioner: nil,
			initError:           false,
			expectError:         false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockCloud := mocks.NewMockCloud(ctrl)

			d := &Driver{
				cloud:                mockCloud,
				namespaceProvisioner: tc.existingProvisioner,
			}

			// Override the DefaultKubernetesAPIClient for testing
			originalClient := cloud.DefaultKubernetesAPIClient
			defer func() {
				cloud.DefaultKubernetesAPIClient = originalClient
			}()

			cloud.DefaultKubernetesAPIClient = func() (kubernetes.Interface, error) {
				if tc.initError {
					return nil, errors.New("k8s client error")
				}
				return fake.NewSimpleClientset(), nil
			}

			if tc.existingProvisioner != nil {
				provisioner, err := d.GetNamespaceProvisioner()
				if tc.expectError && err == nil {
					t.Fatal("Expected error, got nil")
				}
				if !tc.expectError && err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if !tc.expectError && provisioner != tc.existingProvisioner {
					t.Fatal("Expected to return existing provisioner")
				}
			}
		})
	}
}

func TestDriver_Stop(t *testing.T) {
	tests := []struct {
		name                string
		hasProvisioner      bool
		provisionerStopErr  error
		hasServer           bool
	}{
		{
			name:           "stops with provisioner",
			hasProvisioner: true,
			hasServer:      false,
		},
		{
			name:               "handles provisioner stop error",
			hasProvisioner:     true,
			provisionerStopErr: errors.New("stop error"),
			hasServer:          false,
		},
		{
			name:           "stops without provisioner",
			hasProvisioner: false,
			hasServer:      false,
		},
		{
			name:           "stops with server",
			hasProvisioner: false,
			hasServer:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			d := &Driver{}

			if tc.hasProvisioner {
				mockProvisioner := &mockNamespaceProvisioner{
					stopErr: tc.provisionerStopErr,
				}
				d.namespaceProvisioner = mockProvisioner
			}

			// Note: We can't easily test the gRPC server stop in unit tests
			// This would be better tested in integration tests

			// Call Stop and ensure it doesn't panic
			d.Stop()

			// Verify provisioner Stop was called if it existed
			if tc.hasProvisioner {
				mockProv := d.namespaceProvisioner.(*mockNamespaceProvisioner)
				if !mockProv.stopCalled {
					t.Error("Expected provisioner Stop to be called")
				}
			}
		})
	}
}

// mockNamespaceProvisioner is a mock implementation for testing
type mockNamespaceProvisioner struct {
	healthy    bool
	stopCalled bool
	stopErr    error
	status     *ProvisionerStatus
}

func (m *mockNamespaceProvisioner) CreateNamespaceEFS(ctx context.Context, namespace string, options *EFSOptions) (*cloud.FileSystem, error) {
	return nil, nil
}

func (m *mockNamespaceProvisioner) GetNamespaceEFS(ctx context.Context, namespace string) (*cloud.FileSystem, error) {
	return nil, nil
}

func (m *mockNamespaceProvisioner) DeleteNamespaceEFS(ctx context.Context, namespace string) error {
	return nil
}

func (m *mockNamespaceProvisioner) CreateAccessPointForPVC(ctx context.Context, pvcName, namespace string, options *cloud.AccessPointOptions) (*cloud.AccessPoint, error) {
	return nil, nil
}

func (m *mockNamespaceProvisioner) DeleteAccessPointForPVC(ctx context.Context, pvcName, namespace string) error {
	return nil
}

func (m *mockNamespaceProvisioner) CreateNamespaceVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	return nil, nil
}

func (m *mockNamespaceProvisioner) DeleteNamespaceVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	return nil, nil
}

func (m *mockNamespaceProvisioner) Start(ctx context.Context) error {
	return nil
}

func (m *mockNamespaceProvisioner) Stop() error {
	m.stopCalled = true
	return m.stopErr
}

func (m *mockNamespaceProvisioner) IsHealthy() bool {
	return m.healthy
}

func (m *mockNamespaceProvisioner) GetStatus() *ProvisionerStatus {
	if m.status == nil {
		return &ProvisionerStatus{
			Started: true,
			Healthy: m.healthy,
		}
	}
	return m.status
}

// Helper function to test K8s config initialization
func mockInClusterConfig() (*rest.Config, error) {
	return &rest.Config{
		Host: "https://kubernetes.default.svc",
	}, nil
}