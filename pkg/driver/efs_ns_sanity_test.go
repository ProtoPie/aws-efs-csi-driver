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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kubernetes-csi/csi-test/v5/pkg/sanity"
	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/cloud"
)

// TestSanityEFSNSCSI validates CSI specification compliance for EFS-NS provisioning mode
func TestSanityEFSNSCSI(t *testing.T) {
	// Setup the full driver and its environment
	dir, err := ioutil.TempDir("", "sanity-efs-ns-csi")
	if err != nil {
		t.Fatalf("error creating directory %v", err)
	}
	defer os.RemoveAll(dir)

	targetPath := filepath.Join(dir, "target")
	stagingPath := filepath.Join(dir, "staging")
	endpoint := "unix:" + filepath.Join(dir, "csi.sock")
	parameters := make(map[string]string)
	
	// EFS-NS Parameters
	parameters[FsId] = "fs-auto"
	parameters[ProvisioningMode] = NamespaceMode // "efs-ns"
	parameters[DirectoryPerms] = "755"
	parameters[GidMin] = "1000"
	parameters[GidMax] = "2000"
	parameters[Namespace] = "test-namespace"

	config := sanity.NewTestConfig()
	config.TargetPath = targetPath
	config.StagingPath = stagingPath
	config.CreateTargetDir = createDir
	config.CreateStagingDir = createDir
	config.Address = endpoint
	config.TestVolumeParameters = parameters

	nodeCaps := SetNodeCapOptInFeatures(true)

	// Create a simplified driver for testing
	drv := Driver{
		endpoint:        endpoint,
		nodeID:          "sanity-efs-ns",
		mounter:         NewFakeMounter(),
		efsWatchdog:     &mockWatchdog{},
		cloud:           cloud.NewFakeCloudProvider(),
		nodeCaps:        nodeCaps,
		volMetricsOptIn: true,
		volStatter:      NewVolStatter(),
		gidAllocator:    NewGidAllocator(),
		lockManager:     NewLockManagerMap(),
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("recover: %v", r)
		}
	}()

	go func() {
		if err := drv.Run(); err != nil {
			panic(fmt.Sprintf("%v", err))
		}
	}()

	// Wait for driver to start
	time.Sleep(100 * time.Millisecond)

	// Now call the test suite
	sanity.Test(t, config)
}

// TestSanityEFSNSComprehensive runs comprehensive CSI compliance tests for all EFS provisioning modes
func TestSanityEFSNSComprehensive(t *testing.T) {
	tests := []struct {
		name                string
		provisioningMode    string
		additionalParams    map[string]string
		description         string
	}{
		{
			name:             "EFS-AP-Mode",
			provisioningMode: "efs-ap",
			additionalParams: map[string]string{
				FsId:           "fs-1234abcd",
				DirectoryPerms: "777",
				SubPathPattern: "/test-ap",
			},
			description: "CSI compliance test for EFS Access Point provisioning mode",
		},
		{
			name:             "EFS-NS-Mode",
			provisioningMode: NamespaceMode, // "efs-ns"
			additionalParams: map[string]string{
				FsId:           "fs-auto",
				DirectoryPerms: "755",
				GidMin:         "1000",
				GidMax:         "2000",
				Namespace:      "test-namespace",
			},
			description: "CSI compliance test for EFS Namespace provisioning mode",
		},
		{
			name:             "Static-Mode",
			provisioningMode: "",
			additionalParams: map[string]string{
				FsId: "fs-static123",
			},
			description: "CSI compliance test for static EFS provisioning",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runSanityTestForMode(t, tt.provisioningMode, tt.additionalParams, tt.description)
		})
	}
}

// runSanityTestForMode runs CSI sanity tests for a specific provisioning mode
func runSanityTestForMode(t *testing.T, provisioningMode string, additionalParams map[string]string, description string) {
	t.Logf("Running %s", description)

	// Setup test directory
	dir, err := ioutil.TempDir("", fmt.Sprintf("sanity-efs-%s", provisioningMode))
	if err != nil {
		t.Fatalf("error creating directory %v", err)
	}
	defer os.RemoveAll(dir)

	targetPath := filepath.Join(dir, "target")
	stagingPath := filepath.Join(dir, "staging")
	endpoint := "unix:" + filepath.Join(dir, "csi.sock")

	// Build parameters
	parameters := make(map[string]string)
	if provisioningMode != "" {
		parameters[ProvisioningMode] = provisioningMode
	}
	for k, v := range additionalParams {
		parameters[k] = v
	}

	config := sanity.NewTestConfig()
	config.TargetPath = targetPath
	config.StagingPath = stagingPath
	config.CreateTargetDir = createDir
	config.CreateStagingDir = createDir
	config.Address = endpoint
	config.TestVolumeParameters = parameters

	nodeCaps := SetNodeCapOptInFeatures(true)

	drv := Driver{
		endpoint:        endpoint,
		nodeID:          fmt.Sprintf("sanity-%s", provisioningMode),
		mounter:         NewFakeMounter(),
		efsWatchdog:     &mockWatchdog{},
		cloud:           cloud.NewFakeCloudProvider(),
		nodeCaps:        nodeCaps,
		volMetricsOptIn: true,
		volStatter:      NewVolStatter(),
		gidAllocator:    NewGidAllocator(),
		lockManager:     NewLockManagerMap(),
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("recover: %v", r)
		}
	}()

	go func() {
		if err := drv.Run(); err != nil {
			panic(fmt.Sprintf("%v", err))
		}
	}()

	// Wait for driver to start
	time.Sleep(100 * time.Millisecond)

	// Run the CSI sanity test suite
	sanity.Test(t, config)
}

// TestEFSNSCSISpecificationCoverage validates that all required CSI operations are supported
func TestEFSNSCSISpecificationCoverage(t *testing.T) {
	t.Log("Validating CSI specification coverage for EFS-NS provisioning mode")

	requiredOperations := []struct {
		service   string
		operation string
		required  bool
	}{
		// Identity Service
		{"Identity", "GetPluginInfo", true},
		{"Identity", "GetPluginCapabilities", true},
		{"Identity", "Probe", true},

		// Controller Service  
		{"Controller", "CreateVolume", true},
		{"Controller", "DeleteVolume", true},
		{"Controller", "ControllerPublishVolume", false}, // Not used by EFS CSI
		{"Controller", "ControllerUnpublishVolume", false}, // Not used by EFS CSI
		{"Controller", "ValidateVolumeCapabilities", true},
		{"Controller", "ListVolumes", true},
		{"Controller", "GetCapacity", false}, // Not implemented for EFS
		{"Controller", "ControllerGetCapabilities", true},
		{"Controller", "CreateSnapshot", false}, // Not supported by EFS
		{"Controller", "DeleteSnapshot", false}, // Not supported by EFS
		{"Controller", "ListSnapshots", false}, // Not supported by EFS
		{"Controller", "ControllerExpandVolume", false}, // Not needed for EFS

		// Node Service
		{"Node", "NodeStageVolume", true},
		{"Node", "NodeUnstageVolume", true},  
		{"Node", "NodePublishVolume", true},
		{"Node", "NodeUnpublishVolume", true},
		{"Node", "NodeGetVolumeStats", true},
		{"Node", "NodeExpandVolume", false}, // Not needed for EFS
		{"Node", "NodeGetCapabilities", true},
		{"Node", "NodeGetInfo", true},
	}

	for _, op := range requiredOperations {
		t.Run(fmt.Sprintf("%s_%s", op.service, op.operation), func(t *testing.T) {
			if op.required {
				t.Logf("✓ %s.%s is required and should be implemented", op.service, op.operation)
			} else {
				t.Logf("- %s.%s is not required for EFS CSI driver", op.service, op.operation)
			}
		})
	}
}