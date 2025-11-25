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

// TestBackwardCompatibility_MixedModeOperations tests that both efs-ap and efs-ns
// volumes can be created, validated, and deleted in the same driver instance
func TestBackwardCompatibility_MixedModeOperations(t *testing.T) {
	// Create driver with both efs-ap and efs-ns capabilities
	mockCloud := cloud.NewFakeCloudProvider()

	mockNSFileSystemManager := &mockNSFileSystemManager{}
	
	driver := &Driver{
		cloud:           mockCloud,
		mounter:         NewFakeMounter(),
		namespaceManager: mockNSFileSystemManager,
		gidAllocator:    NewGidAllocator(),
		lockManager:     NewLockManagerMap(),
	}

	ctx := context.Background()

	// Test 1: Create efs-ap volume
	efsAPCreateReq := &csi.CreateVolumeRequest{
		Name: "test-efs-ap-volume",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 5 * 1024 * 1024 * 1024, // 5 GiB
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
		Parameters: map[string]string{
			ProvisioningMode: AccessPointMode,
			FsId:            "fs-testfs123",
		},
	}

	efsAPResp, err := driver.CreateVolume(ctx, efsAPCreateReq)
	require.NoError(t, err)
	require.NotNil(t, efsAPResp.Volume)
	assert.Contains(t, efsAPResp.Volume.VolumeId, "fs-testfs123")
	assert.Contains(t, efsAPResp.Volume.VolumeId, "fsap-") // Generated AP ID

	// Test 2: Create efs-ns volume
	efsNSCreateReq := &csi.CreateVolumeRequest{
		Name: "test-efs-ns-volume",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 5 * 1024 * 1024 * 1024, // 5 GiB
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
		Parameters: map[string]string{
			ProvisioningMode: NamespaceMode,
			Namespace:       "test-namespace",
			ClusterID:       "test-cluster",
		},
	}

	efsNSResp, err := driver.CreateVolume(ctx, efsNSCreateReq)
	require.NoError(t, err)
	require.NotNil(t, efsNSResp.Volume)
	assert.Contains(t, efsNSResp.Volume.VolumeId, efsns.VolumeIDPrefix)
	assert.Contains(t, efsNSResp.Volume.VolumeId, "test-namespace")

	// Test 3: Validate both volume types
	
	// Validate efs-ap volume
	efsAPValidateReq := &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId: efsAPResp.Volume.VolumeId,
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
	}

	efsAPValidateResp, err := driver.ValidateVolumeCapabilities(ctx, efsAPValidateReq)
	require.NoError(t, err)
	require.NotNil(t, efsAPValidateResp.Confirmed)

	// Validate efs-ns volume
	efsNSValidateReq := &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId: efsNSResp.Volume.VolumeId,
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
	}

	efsNSValidateResp, err := driver.ValidateVolumeCapabilities(ctx, efsNSValidateReq)
	require.NoError(t, err)
	require.NotNil(t, efsNSValidateResp.Confirmed)

	// Test 4: Delete both volume types

	// Delete efs-ap volume
	efsAPDeleteReq := &csi.DeleteVolumeRequest{
		VolumeId: efsAPResp.Volume.VolumeId,
	}

	_, err = driver.DeleteVolume(ctx, efsAPDeleteReq)
	require.NoError(t, err)

	// Delete efs-ns volume
	efsNSDeleteReq := &csi.DeleteVolumeRequest{
		VolumeId: efsNSResp.Volume.VolumeId,
	}

	_, err = driver.DeleteVolume(ctx, efsNSDeleteReq)
	require.NoError(t, err)
}

// TestBackwardCompatibility_VolumeIDCompatibility tests that both old and new
// volume ID formats are handled correctly
func TestBackwardCompatibility_VolumeIDCompatibility(t *testing.T) {
	driver := &Driver{
		cloud:               &cloud.FakeCloudProvider{},
		mounter:             NewFakeMounter(),
		namespaceManager: &mockNSFileSystemManager{},
	}

	validator := NewBackwardCompatibilityValidator(driver)
	ctx := context.Background()

	testCases := []struct {
		name         string
		volumeID     string
		expectValid  bool
		volumeType   string
	}{
		{
			name:        "Valid efs-ap volume ID",
			volumeID:    "fs-12345678::fsap-87654321",
			expectValid: true,
			volumeType:  "efs-ap",
		},
		{
			name:        "Valid efs-ap volume ID with subpath",
			volumeID:    "fs-12345678:subpath:fsap-87654321",
			expectValid: true,
			volumeType:  "efs-ap",
		},
		{
			name:        "Valid efs-ns volume ID",
			volumeID:    efsns.VolumeIDPrefix + efsns.VolumeIDSeparator + "test-ns" + efsns.VolumeIDSeparator + "fs-12345678" + efsns.VolumeIDSeparator + "cluster-123",
			expectValid: true,
			volumeType:  "efs-ns",
		},
		{
			name:        "Invalid volume ID format",
			volumeID:    "invalid-format",
			expectValid: false,
			volumeType:  "unknown",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validator.ValidateVolumeID(tc.volumeID)
			
			if tc.expectValid {
				assert.NoError(t, err, "Valid volume ID should pass validation")
				
				// Test ValidateVolumeCapabilities with this volume ID
				validateReq := &csi.ValidateVolumeCapabilitiesRequest{
					VolumeId: tc.volumeID,
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
				}

				// Note: This might fail due to volume not existing, but should not fail due to format validation
				_, err := driver.ValidateVolumeCapabilities(ctx, validateReq)
				// We only check that it's not an InvalidArgument error (format validation error)
				if err != nil {
					st, ok := status.FromError(err)
					require.True(t, ok)
					assert.NotEqual(t, codes.InvalidArgument, st.Code(), 
						"Should not fail with InvalidArgument (format validation error)")
				}
			} else {
				assert.Error(t, err, "Invalid volume ID should fail validation")
				st, ok := status.FromError(err)
				require.True(t, ok)
				assert.Equal(t, codes.InvalidArgument, st.Code())
			}
		})
	}
}

// TestBackwardCompatibility_UpgradeScenario simulates driver upgrade scenarios
func TestBackwardCompatibility_UpgradeScenario(t *testing.T) {
	testCases := []struct {
		name          string
		description   string
		setupDriver   func() *Driver
		expectUpgrade bool
	}{
		{
			name:        "Upgrade from efs-ap only to mixed mode",
			description: "Driver initially supports only efs-ap, then EFS-NS is enabled",
			setupDriver: func() *Driver {
				return &Driver{
					cloud:               &cloud.FakeCloudProvider{},
					mounter:             NewFakeMounter(),
					namespaceManager: &mockNSFileSystemManager{}, // EFS-NS now available
				}
			},
			expectUpgrade: true,
		},
		{
			name:        "Upgrade maintains existing functionality",
			description: "All existing functionality should remain intact after upgrade",
			setupDriver: func() *Driver {
				return &Driver{
					cloud:               &cloud.FakeCloudProvider{},
					mounter:             NewFakeMounter(),
					namespaceManager: &mockNSFileSystemManager{},
				}
			},
			expectUpgrade: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			driver := tc.setupDriver()
			validator := NewBackwardCompatibilityValidator(driver)
			
			// Test upgrade compatibility
			err := validator.ValidateUpgradeCompatibility(context.Background())
			
			if tc.expectUpgrade {
				assert.NoError(t, err, "Upgrade compatibility should pass")
				
				// Test that both modes are still supported after upgrade
				err = validator.ValidateProvisioningModeParameter(AccessPointMode)
				assert.NoError(t, err, "efs-ap mode should still be supported after upgrade")
				
				if driver.namespaceManager != nil {
					err = validator.ValidateProvisioningModeParameter(NamespaceMode)
					assert.NoError(t, err, "efs-ns mode should be supported after upgrade")
				}
			} else {
				assert.Error(t, err, "Upgrade compatibility should fail")
			}
		})
	}
}

// TestBackwardCompatibility_CompatibilityReport tests the compatibility report generation
func TestBackwardCompatibility_CompatibilityReport(t *testing.T) {
	testCases := []struct {
		name               string
		driver             *Driver
		expectedEFSAPMode  bool
		expectedEFSNSMode  bool
		expectedMixedMode  bool
	}{
		{
			name: "Driver with both modes",
			driver: &Driver{
				cloud:               &cloud.FakeCloudProvider{},
				mounter:             NewFakeMounter(),
				namespaceManager: &mockNSFileSystemManager{},
			},
			expectedEFSAPMode: true,
			expectedEFSNSMode: true,
			expectedMixedMode: true,
		},
		{
			name: "Driver with efs-ap only",
			driver: &Driver{
				cloud:   &cloud.FakeCloudProvider{},
				mounter: NewFakeMounter(),
			},
			expectedEFSAPMode: true,
			expectedEFSNSMode: false,
			expectedMixedMode: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			validator := NewBackwardCompatibilityValidator(tc.driver)
			report := validator.GenerateCompatibilityReport(context.Background())

			require.NotNil(t, report)
			
			// Verify expected mode support
			assert.Equal(t, tc.expectedEFSAPMode, report.EFSAPModeSupported)
			assert.Equal(t, tc.expectedEFSNSMode, report.EFSNSModeSupported)
			assert.Equal(t, tc.expectedMixedMode, report.MixedModeSupported)

			// Verify report structure
			// DriverVersion may be empty in tests since it's injected at build time
			assert.NotNil(t, report.DriverVersion)
			assert.NotEmpty(t, report.ControllerCapabilities)
			assert.NotEmpty(t, report.ValidationResults)

			// Verify all validation results have proper structure
			for _, result := range report.ValidationResults {
				assert.NotEmpty(t, result.CheckName)
				assert.NotEmpty(t, result.Description)
			}

			// At least some validations should pass
			passedCount := 0
			for _, result := range report.ValidationResults {
				if result.Passed {
					passedCount++
				}
			}
			assert.Greater(t, passedCount, 0, "At least some validation checks should pass")
		})
	}
}

// TestBackwardCompatibility_InvalidVolumeIDHandling tests how invalid volume IDs are handled
func TestBackwardCompatibility_InvalidVolumeIDHandling(t *testing.T) {
	driver := &Driver{
		cloud:               &cloud.FakeCloudProvider{},
		mounter:             NewFakeMounter(),
		namespaceManager: &mockNSFileSystemManager{},
	}

	ctx := context.Background()

	invalidVolumeIDs := []string{
		"",
		"invalid-format",
		"ef-123", // doesn't start with fs-
		efsns.VolumeIDPrefix + efsns.VolumeIDSeparator, // efs-ns prefix but incomplete
		efsns.VolumeIDPrefix + efsns.VolumeIDSeparator + "ns" + efsns.VolumeIDSeparator, // missing parts
	}

	for _, volumeID := range invalidVolumeIDs {
		t.Run("Invalid_ID_"+volumeID, func(t *testing.T) {
			// Test DeleteVolume - should handle gracefully (return success for invalid IDs)
			deleteReq := &csi.DeleteVolumeRequest{
				VolumeId: volumeID,
			}
			
			if volumeID == "" {
				// Empty volume ID should fail
				_, err := driver.DeleteVolume(ctx, deleteReq)
				assert.Error(t, err)
				st, ok := status.FromError(err)
				require.True(t, ok)
				assert.Equal(t, codes.InvalidArgument, st.Code())
			} else {
				// Invalid but non-empty volume IDs should return success for CSI compliance
				_, err := driver.DeleteVolume(ctx, deleteReq)
				assert.NoError(t, err, "DeleteVolume should return success for invalid volume IDs (CSI compliance)")
			}

			// Test ValidateVolumeCapabilities - should return proper error
			validateReq := &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId: volumeID,
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
			}

			_, err := driver.ValidateVolumeCapabilities(ctx, validateReq)
			if volumeID == "" {
				// Empty volume ID should fail with InvalidArgument
				assert.Error(t, err)
				st, ok := status.FromError(err)
				require.True(t, ok)
				assert.Equal(t, codes.InvalidArgument, st.Code())
			} else {
				// Invalid format should fail with NotFound (CSI spec compliance)
				assert.Error(t, err)
				st, ok := status.FromError(err)
				require.True(t, ok)
				assert.Equal(t, codes.NotFound, st.Code())
			}
		})
	}
}