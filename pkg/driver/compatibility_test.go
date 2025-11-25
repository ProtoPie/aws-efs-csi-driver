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
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/cloud"
	efsns "github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/driver/efs-ns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBackwardCompatibilityValidator_ValidateVolumeID(t *testing.T) {
	// Create a minimal driver for testing
	driver := &Driver{
		cloud:   &cloud.FakeCloudProvider{},
		mounter: NewFakeMounter(),
	}

	validator := NewBackwardCompatibilityValidator(driver)

	testCases := []struct {
		name      string
		volumeID  string
		expectErr bool
		errCode   codes.Code
	}{
		{
			name:      "Empty volume ID should fail",
			volumeID:  "",
			expectErr: true,
			errCode:   codes.InvalidArgument,
		},
		{
			name:      "Valid efs-ap volume ID should pass",
			volumeID:  "fs-12345678::fsap-12345678",
			expectErr: false,
		},
		{
			name:      "Valid efs-ap volume ID with subpath should pass",
			volumeID:  "fs-12345678:subpath:fsap-12345678",
			expectErr: false,
		},
		{
			name:      "Valid efs-ns volume ID should pass",
			volumeID:  efsns.VolumeIDPrefix + efsns.VolumeIDSeparator + "test-namespace" + efsns.VolumeIDSeparator + "fs-12345678" + efsns.VolumeIDSeparator + "cluster-123",
			expectErr: false,
		},
		{
			name:      "Invalid efs-ns volume ID format should fail",
			volumeID:  efsns.VolumeIDPrefix + efsns.VolumeIDSeparator + "invalid-format",
			expectErr: true,
			errCode:   codes.InvalidArgument,
		},
		{
			name:      "Invalid efs-ap volume ID format should fail",
			volumeID:  "invalid-format",
			expectErr: true,
			errCode:   codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validator.ValidateVolumeID(tc.volumeID)

			if tc.expectErr {
				require.Error(t, err)
				if tc.errCode != codes.OK {
					st, ok := status.FromError(err)
					require.True(t, ok, "Expected gRPC status error")
					assert.Equal(t, tc.errCode, st.Code())
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackwardCompatibilityValidator_ValidateProvisioningModeParameter(t *testing.T) {
	testCases := []struct {
		name            string
		driver          *Driver
		provisioningMode string
		expectErr       bool
		errCode         codes.Code
	}{
		{
			name: "Empty provisioning mode should fail",
			driver: &Driver{
				cloud:   &cloud.FakeCloudProvider{},
				mounter: NewFakeMounter(),
			},
			provisioningMode: "",
			expectErr:        true,
			errCode:          codes.InvalidArgument,
		},
		{
			name: "efs-ap mode should always pass",
			driver: &Driver{
				cloud:   &cloud.FakeCloudProvider{},
				mounter: NewFakeMounter(),
			},
			provisioningMode: AccessPointMode,
			expectErr:        false,
		},
		{
			name: "efs-ns mode should pass when components initialized",
			driver: &Driver{
				cloud:           &cloud.FakeCloudProvider{},
				mounter:         NewFakeMounter(),
				namespaceManager: &mockNSFileSystemManager{},
			},
			provisioningMode: NamespaceMode,
			expectErr:        false,
		},
		{
			name: "efs-ns mode should fail when components not initialized",
			driver: &Driver{
				cloud:   &cloud.FakeCloudProvider{},
				mounter: NewFakeMounter(),
				// namespaceManager is nil
			},
			provisioningMode: NamespaceMode,
			expectErr:        true,
			errCode:          codes.FailedPrecondition,
		},
		{
			name: "Invalid provisioning mode should fail",
			driver: &Driver{
				cloud:   &cloud.FakeCloudProvider{},
				mounter: NewFakeMounter(),
			},
			provisioningMode: "invalid-mode",
			expectErr:        true,
			errCode:          codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			validator := NewBackwardCompatibilityValidator(tc.driver)
			err := validator.ValidateProvisioningModeParameter(tc.provisioningMode)

			if tc.expectErr {
				require.Error(t, err)
				if tc.errCode != codes.OK {
					st, ok := status.FromError(err)
					require.True(t, ok, "Expected gRPC status error")
					assert.Equal(t, tc.errCode, st.Code())
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackwardCompatibilityValidator_ValidateMixedModeOperation(t *testing.T) {
	testCases := []struct {
		name      string
		driver    *Driver
		expectErr bool
		errCode   codes.Code
	}{
		{
			name: "Valid driver with all components should pass",
			driver: &Driver{
				cloud:               &cloud.FakeCloudProvider{},
				mounter:             NewFakeMounter(),
				namespaceManager: &mockNSFileSystemManager{},
			},
			expectErr: false,
		},
		{
			name: "Driver without EFS-NS components should pass (efs-ap only)",
			driver: &Driver{
				cloud:   &cloud.FakeCloudProvider{},
				mounter: NewFakeMounter(),
				// namespaceManager is nil
			},
			expectErr: false,
		},
		{
			name: "Driver without cloud provider should fail",
			driver: &Driver{
				mounter: NewFakeMounter(),
				// cloud is nil
			},
			expectErr: true,
			errCode:   codes.Internal,
		},
		{
			name: "Driver without mounter should fail",
			driver: &Driver{
				cloud: &cloud.FakeCloudProvider{},
				// mounter is nil
			},
			expectErr: true,
			errCode:   codes.Internal,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			validator := NewBackwardCompatibilityValidator(tc.driver)
			err := validator.ValidateMixedModeOperation(context.Background())

			if tc.expectErr {
				require.Error(t, err)
				if tc.errCode != codes.OK {
					st, ok := status.FromError(err)
					require.True(t, ok, "Expected gRPC status error")
					assert.Equal(t, tc.errCode, st.Code())
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackwardCompatibilityValidator_ValidateControllerCapabilities(t *testing.T) {
	driver := &Driver{
		cloud:   &cloud.FakeCloudProvider{},
		mounter: NewFakeMounter(),
	}

	validator := NewBackwardCompatibilityValidator(driver)
	err := validator.ValidateControllerCapabilities()

	// Should always pass as controller capabilities are static
	require.NoError(t, err)
}

func TestBackwardCompatibilityValidator_ValidateVolumeCapabilities(t *testing.T) {
	driver := &Driver{
		cloud:   &cloud.FakeCloudProvider{},
		mounter: NewFakeMounter(),
	}

	validator := NewBackwardCompatibilityValidator(driver)

	testCases := []struct {
		name             string
		volCaps          []*csi.VolumeCapability
		provisioningMode string
		expectErr        bool
		errCode          codes.Code
	}{
		{
			name:             "Empty volume capabilities should fail",
			volCaps:          []*csi.VolumeCapability{},
			provisioningMode: AccessPointMode,
			expectErr:        true,
			errCode:          codes.InvalidArgument,
		},
		{
			name: "Valid volume capabilities should pass",
			volCaps: []*csi.VolumeCapability{
				{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
					},
				},
			},
			provisioningMode: AccessPointMode,
			expectErr:        false,
		},
		{
			name: "Block volume capability should fail",
			volCaps: []*csi.VolumeCapability{
				{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
			},
			provisioningMode: NamespaceMode,
			expectErr:        true,
			errCode:          codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validator.ValidateVolumeCapabilities(tc.volCaps, tc.provisioningMode)

			if tc.expectErr {
				require.Error(t, err)
				if tc.errCode != codes.OK {
					st, ok := status.FromError(err)
					require.True(t, ok, "Expected gRPC status error")
					assert.Equal(t, tc.errCode, st.Code())
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackwardCompatibilityValidator_ValidateUpgradeCompatibility(t *testing.T) {
	testCases := []struct {
		name      string
		driver    *Driver
		expectErr bool
	}{
		{
			name: "Valid driver should pass upgrade compatibility",
			driver: &Driver{
				cloud:               &cloud.FakeCloudProvider{},
				mounter:             NewFakeMounter(),
				namespaceManager: &mockNSFileSystemManager{},
			},
			expectErr: false,
		},
		{
			name: "Driver with missing components should fail",
			driver: &Driver{
				// Missing cloud and mounter
			},
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			validator := NewBackwardCompatibilityValidator(tc.driver)
			err := validator.ValidateUpgradeCompatibility(context.Background())

			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackwardCompatibilityValidator_ValidateDowngradeCompatibility(t *testing.T) {
	testCases := []struct {
		name        string
		driver      *Driver
		expectErr   bool
		description string
	}{
		{
			name: "Driver without EFS-NS should pass",
			driver: &Driver{
				cloud:   &cloud.FakeCloudProvider{},
				mounter: NewFakeMounter(),
				// namespaceManager is nil
			},
			expectErr:   false,
			description: "Without EFS-NS components, downgrade should be safe",
		},
		{
			name: "Driver with EFS-NS should pass with warning",
			driver: &Driver{
				cloud:               &cloud.FakeCloudProvider{},
				mounter:             NewFakeMounter(),
				namespaceManager: &mockNSFileSystemManager{},
			},
			expectErr:   false,
			description: "With EFS-NS components, downgrade should log warning but pass",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			validator := NewBackwardCompatibilityValidator(tc.driver)
			err := validator.ValidateDowngradeCompatibility(context.Background())

			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackwardCompatibilityValidator_GenerateCompatibilityReport(t *testing.T) {
	testCases := []struct {
		name                 string
		driver               *Driver
		expectedEFSAPSupport bool
		expectedEFSNSSupport bool
		expectedMixedMode    bool
	}{
		{
			name: "Driver with all components",
			driver: &Driver{
				cloud:               &cloud.FakeCloudProvider{},
				mounter:             NewFakeMounter(),
				namespaceManager: &mockNSFileSystemManager{},
			},
			expectedEFSAPSupport: true,
			expectedEFSNSSupport: true,
			expectedMixedMode:    true,
		},
		{
			name: "Driver with only efs-ap components",
			driver: &Driver{
				cloud:   &cloud.FakeCloudProvider{},
				mounter: NewFakeMounter(),
				// namespaceManager is nil
			},
			expectedEFSAPSupport: true,
			expectedEFSNSSupport: false,
			expectedMixedMode:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			validator := NewBackwardCompatibilityValidator(tc.driver)
			report := validator.GenerateCompatibilityReport(context.Background())

			require.NotNil(t, report)
			assert.Equal(t, tc.expectedEFSAPSupport, report.EFSAPModeSupported)
			assert.Equal(t, tc.expectedEFSNSSupport, report.EFSNSModeSupported)
			assert.Equal(t, tc.expectedMixedMode, report.MixedModeSupported)
			// DriverVersion may be empty in tests since it's injected at build time
			assert.NotNil(t, report.DriverVersion)
			assert.NotEmpty(t, report.ControllerCapabilities)
			assert.NotEmpty(t, report.ValidationResults)

			// Check that all validation results have required fields
			for _, result := range report.ValidationResults {
				assert.NotEmpty(t, result.CheckName)
				assert.NotEmpty(t, result.Description)
				// Passed can be true or false, both are valid
			}
		})
	}
}

func TestBackwardCompatibilityValidator_ValidateEFSNSVolumeID(t *testing.T) {
	driver := &Driver{
		cloud:   &cloud.FakeCloudProvider{},
		mounter: NewFakeMounter(),
	}

	validator := NewBackwardCompatibilityValidator(driver)

	testCases := []struct {
		name      string
		volumeID  string
		expectErr bool
	}{
		{
			name:      "Valid efs-ns volume ID",
			volumeID:  efsns.VolumeIDPrefix + efsns.VolumeIDSeparator + "test-ns" + efsns.VolumeIDSeparator + "fs-12345678" + efsns.VolumeIDSeparator + "cluster-123",
			expectErr: false,
		},
		{
			name:      "Invalid efs-ns volume ID - missing parts",
			volumeID:  efsns.VolumeIDPrefix + efsns.VolumeIDSeparator + "test-ns",
			expectErr: true,
		},
		{
			name:      "Invalid efs-ns volume ID - empty namespace",
			volumeID:  efsns.VolumeIDPrefix + efsns.VolumeIDSeparator + efsns.VolumeIDSeparator + "fs-12345678" + efsns.VolumeIDSeparator + "cluster-123",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validator.validateEFSNSVolumeID(tc.volumeID)

			if tc.expectErr {
				require.Error(t, err)
				st, ok := status.FromError(err)
				require.True(t, ok, "Expected gRPC status error")
				assert.Equal(t, codes.InvalidArgument, st.Code())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackwardCompatibilityValidator_ValidateEFSAPVolumeID(t *testing.T) {
	driver := &Driver{
		cloud:   &cloud.FakeCloudProvider{},
		mounter: NewFakeMounter(),
	}

	validator := NewBackwardCompatibilityValidator(driver)

	testCases := []struct {
		name      string
		volumeID  string
		expectErr bool
	}{
		{
			name:      "Valid efs-ap volume ID",
			volumeID:  "fs-12345678::fsap-12345678",
			expectErr: false,
		},
		{
			name:      "Valid efs-ap volume ID with subpath",
			volumeID:  "fs-12345678:subpath:fsap-12345678",
			expectErr: false,
		},
		{
			name:      "Invalid efs-ap volume ID",
			volumeID:  "invalid-format",
			expectErr: true,
		},
		{
			name:      "Empty volume ID",
			volumeID:  "",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validator.validateEFSAPVolumeID(tc.volumeID)

			if tc.expectErr {
				require.Error(t, err)
				st, ok := status.FromError(err)
				require.True(t, ok, "Expected gRPC status error")
				assert.Equal(t, codes.InvalidArgument, st.Code())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Mock implementations for testing

type mockNSFileSystemManager struct {
	efsns.NamespaceFileSystemManager
}

func (m *mockNSFileSystemManager) CreateOrGetFileSystemForNamespace(
	ctx context.Context,
	namespace string,
	options *efsns.FileSystemOptions,
) (*efsns.FileSystemInfo, error) {
	return &efsns.FileSystemInfo{
		FileSystemID: "fs-mock123",
		Namespace:    namespace,
		ClusterID:    "mock-cluster",
		State:        efsns.FileSystemStateAvailable,
	}, nil
}

func (m *mockNSFileSystemManager) DeleteFileSystemForNamespace(
	ctx context.Context,
	namespace string,
	volumeId string,
) error {
	return nil
}

func (m *mockNSFileSystemManager) GetFileSystemInfo(
	ctx context.Context,
	namespace string,
) (*efsns.FileSystemInfo, error) {
	return &efsns.FileSystemInfo{
		FileSystemID: "fs-mock123",
		Namespace:    namespace,
		ClusterID:    "mock-cluster",
		State:        efsns.FileSystemStateAvailable,
	}, nil
}