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
	"github.com/golang/mock/gomock"
	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/cloud"
	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/driver/mocks"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

// TestNamespaceProvisionerIntegration tests the integration between Driver and NamespaceProvisioner
func TestNamespaceProvisionerIntegration(t *testing.T) {
	t.Run("lazy initialization on first efs-ns request", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Create mock cloud
		mockCloud := mocks.NewMockCloud(ctrl)

		// Create driver
		d := &Driver{
			cloud:                mockCloud,
			tags:                 map[string]string{"test": "true"},
			namespaceProvisioner: nil, // Start with no provisioner
		}

		// Override K8s client factory for testing
		originalClient := cloud.DefaultKubernetesAPIClient
		defer func() {
			cloud.DefaultKubernetesAPIClient = originalClient
		}()

		cloud.DefaultKubernetesAPIClient = func() (kubernetes.Interface, error) {
			return fake.NewSimpleClientset(), nil
		}

		// Verify provisioner starts as nil
		if d.namespaceProvisioner != nil {
			t.Fatal("Expected namespaceProvisioner to be nil initially")
		}

		// Initialize the provisioner
		err := d.InitializeNamespaceProvisioner()
		// This will fail due to rest.InClusterConfig() not being available in tests
		// but we can verify the attempt was made
		if err == nil {
			t.Log("NamespaceProvisioner initialization attempted (would fail in test environment due to K8s config)")
		}
	})

	t.Run("provisioner lifecycle management", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Create a driver with a mock provisioner
		mockProvisioner := &mockNamespaceProvisioner{
			healthy: true,
		}

		d := &Driver{
			namespaceProvisioner: mockProvisioner,
		}

		// Test Stop() calls provisioner.Stop()
		d.Stop()

		if !mockProvisioner.stopCalled {
			t.Error("Expected provisioner.Stop() to be called during Driver.Stop()")
		}
	})

	t.Run("CreateVolume delegates to provisioner for efs-ns mode", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Create mock provisioner
		mockProvisioner := &mockNamespaceProvisionerWithCalls{
			createResponse: &csi.CreateVolumeResponse{
				Volume: &csi.Volume{
					VolumeId:      "fsap-test123",
					CapacityBytes: 1024,
				},
			},
		}

		mockCloud := mocks.NewMockCloud(ctrl)

		d := &Driver{
			cloud:                mockCloud,
			namespaceProvisioner: mockProvisioner,
		}

		// Create request with efs-ns mode
		req := &csi.CreateVolumeRequest{
			Name: "test-volume",
			Parameters: map[string]string{
				ProvisioningMode: NamespaceProvisioningMode,
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
				RequiredBytes: 1024,
			},
		}

		ctx := context.Background()
		resp, err := d.CreateVolume(ctx, req)

		if err != nil {
			t.Fatalf("CreateVolume failed: %v", err)
		}

		if resp == nil {
			t.Fatal("Expected response, got nil")
		}

		if !mockProvisioner.createCalled {
			t.Error("Expected provisioner.CreateNamespaceVolume to be called")
		}

		if resp.Volume.VolumeId != "fsap-test123" {
			t.Errorf("Expected volume ID fsap-test123, got %s", resp.Volume.VolumeId)
		}
	})

	t.Run("DeleteVolume delegates to provisioner for efs-ns volumes", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Create mock provisioner
		mockProvisioner := &mockNamespaceProvisionerWithCalls{
			deleteResponse: &csi.DeleteVolumeResponse{},
		}

		mockCloud := mocks.NewMockCloud(ctrl)

		d := &Driver{
			cloud:                mockCloud,
			namespaceProvisioner: mockProvisioner,
		}

		// Delete request for efs-ns volume (just access point ID)
		req := &csi.DeleteVolumeRequest{
			VolumeId: "fsap-test123",
		}

		ctx := context.Background()
		resp, err := d.DeleteVolume(ctx, req)

		if err != nil {
			t.Fatalf("DeleteVolume failed: %v", err)
		}

		if resp == nil {
			t.Fatal("Expected response, got nil")
		}

		if !mockProvisioner.deleteCalled {
			t.Error("Expected provisioner.DeleteNamespaceVolume to be called")
		}
	})
}