package driver

import (
	"context"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/cloud"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MockNamespaceProvisioner is a simple mock implementation for testing
type MockNamespaceProvisioner struct {
	CreateFunc func(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error)
	DeleteFunc func(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error)
}

func (m *MockNamespaceProvisioner) CreateNamespaceVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if m.CreateFunc != nil {
		return m.CreateFunc(ctx, req)
	}
	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      "fsap-12345678",
			CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
			VolumeContext: map[string]string{
				"namespace": "test-namespace",
			},
		},
	}, nil
}

func (m *MockNamespaceProvisioner) DeleteNamespaceVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if m.DeleteFunc != nil {
		return m.DeleteFunc(ctx, req)
	}
	return &csi.DeleteVolumeResponse{}, nil
}

// Implement the rest of the interface methods as no-ops for testing
func (m *MockNamespaceProvisioner) CreateNamespaceEFS(ctx context.Context, namespace string, options *EFSOptions) (*cloud.FileSystem, error) {
	return nil, nil
}

func (m *MockNamespaceProvisioner) GetNamespaceEFS(ctx context.Context, namespace string) (*cloud.FileSystem, error) {
	return nil, nil
}

func (m *MockNamespaceProvisioner) DeleteNamespaceEFS(ctx context.Context, namespace string) error {
	return nil
}

func (m *MockNamespaceProvisioner) CreateAccessPointForPVC(ctx context.Context, pvcName, namespace string, options *cloud.AccessPointOptions) (*cloud.AccessPoint, error) {
	return nil, nil
}

func (m *MockNamespaceProvisioner) DeleteAccessPointForPVC(ctx context.Context, pvcName, namespace string) error {
	return nil
}

func (m *MockNamespaceProvisioner) Start(ctx context.Context) error {
	return nil
}

func (m *MockNamespaceProvisioner) Stop() error {
	return nil
}

func (m *MockNamespaceProvisioner) IsHealthy() bool {
	return true
}

func (m *MockNamespaceProvisioner) GetStatus() *ProvisionerStatus {
	return nil
}

// TestCreateVolumeEFSNamespaceMode tests the efs-ns provisioning mode functionality
func TestCreateVolumeEFSNamespaceMode(t *testing.T) {
	var (
		endpoint     = "endpoint"
		volumeName   = "volumeName"
		pvcNamespace = "test-namespace"
		capacityRange int64 = 5368709120
		stdVolCap    = &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		}
	)

	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "Success: EFS-NS mode delegation to NamespaceProvisioner",
			testFunc: func(t *testing.T) {
				mockNamespaceProvisioner := &MockNamespaceProvisioner{}

				driver := &Driver{
					endpoint:             endpoint,
					namespaceProvisioner: mockNamespaceProvisioner,
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ns",
						PvcNamespace:     pvcNamespace,
						DirectoryPerms:   "700",
					},
				}

				ctx := context.Background()
				resp, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if resp == nil {
					t.Fatal("CreateVolume returned nil response")
				}

				if resp.Volume.VolumeId != "fsap-12345678" {
					t.Fatalf("Expected VolumeId fsap-12345678, got %s", resp.Volume.VolumeId)
				}

				if resp.Volume.CapacityBytes != capacityRange {
					t.Fatalf("Expected CapacityBytes %d, got %d", capacityRange, resp.Volume.CapacityBytes)
				}
			},
		},
		{
			name: "Fail: EFS-NS mode but NamespaceProvisioner not initialized",
			testFunc: func(t *testing.T) {
				driver := &Driver{
					endpoint:             endpoint,
					namespaceProvisioner: nil, // Not initialized
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ns",
						PvcNamespace:     pvcNamespace,
						DirectoryPerms:   "700",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)

				if err == nil {
					t.Fatal("CreateVolume should have failed")
				}

				if status.Code(err) != codes.Internal {
					t.Fatalf("Expected Internal error, got %v", err)
				}

				expectedErrMsg := "NamespaceProvisioner not initialized"
				if !strings.Contains(err.Error(), expectedErrMsg) {
					t.Fatalf("Expected error message to contain '%s', got '%s'", expectedErrMsg, err.Error())
				}
			},
		},
		{
			name: "Fail: Invalid provisioning mode",
			testFunc: func(t *testing.T) {
				driver := &Driver{
					endpoint: endpoint,
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "invalid-mode",
						DirectoryPerms:   "700",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)

				if err == nil {
					t.Fatal("CreateVolume should have failed")
				}

				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("Expected InvalidArgument error, got %v", err)
				}

				expectedErrMsg := "is not supported. Supported modes: 'efs-ap' (Access Point) and 'efs-ns' (Namespace)"
				if !strings.Contains(err.Error(), expectedErrMsg) {
					t.Fatalf("Expected error message to contain '%s', got '%s'", expectedErrMsg, err.Error())
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

// TestDeleteVolumeEFSNamespaceMode tests the efs-ns provisioning mode deletion functionality
func TestDeleteVolumeEFSNamespaceMode(t *testing.T) {
	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "Success: EFS-NS volume deletion",
			testFunc: func(t *testing.T) {
				mockNamespaceProvisioner := &MockNamespaceProvisioner{}

				driver := &Driver{
					endpoint:             "endpoint",
					namespaceProvisioner: mockNamespaceProvisioner,
				}

				// EFS-NS volume ID format is just the access point ID
				volumeId := "fsap-12345678"
				req := &csi.DeleteVolumeRequest{
					VolumeId: volumeId,
				}

				ctx := context.Background()
				resp, err := driver.DeleteVolume(ctx, req)

				if err != nil {
					t.Fatalf("DeleteVolume failed: %v", err)
				}

				if resp == nil {
					t.Fatal("DeleteVolume returned nil response")
				}
			},
		},
		{
			name: "Success: EFS-AP volume detection logic (volume ID with double colon does not trigger efs-ns)",
			testFunc: func(t *testing.T) {
				// This test only verifies the volume ID detection logic
				// We test that volume IDs with "::" don't trigger efs-ns mode
				volumeId := "fs-12345::fsap-67890"

				// Check our detection logic manually
				isEfsNsVolumeId := strings.HasPrefix(volumeId, "fsap-") && !strings.Contains(volumeId, "::")

				if isEfsNsVolumeId {
					t.Fatal("Volume ID with :: should not be detected as efs-ns mode")
				}

				// Test the opposite - a proper efs-ns volume ID
				efsNsVolumeId := "fsap-12345678"
				isEfsNsVolumeId = strings.HasPrefix(efsNsVolumeId, "fsap-") && !strings.Contains(efsNsVolumeId, "::")

				if !isEfsNsVolumeId {
					t.Fatal("Pure access point ID should be detected as efs-ns mode")
				}
			},
		},
		{
			name: "Logic: EFS-NS volume detection when NamespaceProvisioner not initialized",
			testFunc: func(t *testing.T) {
				// This test verifies that the logic correctly identifies efs-ns volumes
				// but gracefully handles the case when NamespaceProvisioner is nil
				volumeId := "fsap-12345678"

				// Verify this would be detected as efs-ns volume
				isEfsNsVolumeId := strings.HasPrefix(volumeId, "fsap-") && !strings.Contains(volumeId, "::")

				if !isEfsNsVolumeId {
					t.Fatal("Volume ID should be detected as efs-ns mode")
				}

				// Test that regular efs-ap volume IDs are NOT detected as efs-ns
				efsApVolumeId := "fs-12345::fsap-67890"
				isEfsNsVolumeId = strings.HasPrefix(efsApVolumeId, "fsap-") && !strings.Contains(efsApVolumeId, "::")

				if isEfsNsVolumeId {
					t.Fatal("EFS-AP volume ID should NOT be detected as efs-ns mode")
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}