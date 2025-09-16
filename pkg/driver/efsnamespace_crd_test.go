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
	"regexp"
	"strings"
	"testing"
)

// Validation helper functions
func isValidNamespaceName(name string) bool {
	if len(name) == 0 || len(name) > 63 {
		return false
	}
	// Kubernetes namespace name must consist of lowercase alphanumeric characters or '-'
	namespaceRegex := regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	return namespaceRegex.MatchString(name)
}

func isValidRegion(region string) bool {
	// AWS region format: us-east-1, eu-west-2, ap-southeast-1, etc.
	regionRegex := regexp.MustCompile(`^[a-z]{2}-[a-z]+-[0-9]+$`)
	return regionRegex.MatchString(region)
}

func isValidEFSFileSystemId(id string) bool {
	// AWS EFS filesystem ID format: fs-0123456789abcdef0
	fsIdRegex := regexp.MustCompile(`^fs-[0-9a-f]{17}$`)
	return fsIdRegex.MatchString(id)
}

func isValidPerformanceMode(mode string) bool {
	validModes := []string{"generalPurpose", "maxIO"}
	for _, valid := range validModes {
		if mode == valid {
			return true
		}
	}
	return false
}

func isValidThroughputMode(mode string) bool {
	validModes := []string{"bursting", "provisioned", "elastic"}
	for _, valid := range validModes {
		if mode == valid {
			return true
		}
	}
	return false
}

func isValidState(state string) bool {
	validStates := []string{"Provisioning", "Active", "Updating", "Deleting", "Failed"}
	for _, valid := range validStates {
		if state == valid {
			return true
		}
	}
	return false
}

func TestNamespaceNameValidation(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		valid     bool
	}{
		{"valid lowercase", "dev", true},
		{"valid with hyphen", "dev-namespace", true},
		{"valid with numbers", "namespace123", true},
		{"valid single char", "x", true},
		{"invalid uppercase", "Dev", false},
		{"invalid underscore", "dev_namespace", false},
		{"invalid special chars", "dev@namespace", false},
		{"invalid too long", "this-is-a-very-long-namespace-name-that-exceeds-sixty-three-chars", false},
		{"invalid empty", "", false},
		{"invalid starts with hyphen", "-namespace", false},
		{"invalid ends with hyphen", "namespace-", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidNamespaceName(tt.namespace)
			if got != tt.valid {
				t.Errorf("isValidNamespaceName(%q) = %v, want %v", tt.namespace, got, tt.valid)
			}
		})
	}
}

func TestRegionValidation(t *testing.T) {
	tests := []struct {
		name   string
		region string
		valid  bool
	}{
		{"valid us-east-1", "us-east-1", true},
		{"valid eu-west-2", "eu-west-2", true},
		{"valid ap-southeast-1", "ap-southeast-1", true},
		{"valid ca-central-1", "ca-central-1", true},
		{"invalid format", "invalid-region", false},
		{"invalid no numbers", "us-east-x", false},
		{"invalid uppercase", "US-EAST-1", false},
		{"invalid extra segment", "us-east-1-extra", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidRegion(tt.region)
			if got != tt.valid {
				t.Errorf("isValidRegion(%q) = %v, want %v", tt.region, got, tt.valid)
			}
		})
	}
}

func TestFileSystemIdValidation(t *testing.T) {
	tests := []struct {
		name string
		fsId string
		valid bool
	}{
		{"valid filesystem ID", "fs-0123456789abcdef0", true},
		{"valid all lowercase hex", "fs-abcdefabcdefabcde", true},
		{"invalid wrong prefix", "efs-0123456789abcdef0", false},
		{"invalid too short", "fs-0123456789abcde", false},
		{"invalid too long", "fs-0123456789abcdef01", false},
		{"invalid uppercase hex", "fs-0123456789ABCDEF0", false},
		{"invalid non-hex chars", "fs-0123456789ghijkl0", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidEFSFileSystemId(tt.fsId)
			if got != tt.valid {
				t.Errorf("isValidEFSFileSystemId(%q) = %v, want %v", tt.fsId, got, tt.valid)
			}
		})
	}
}

func TestPerformanceModeValidation(t *testing.T) {
	tests := []struct {
		name     string
		mode     string
		valid    bool
	}{
		{"valid generalPurpose", "generalPurpose", true},
		{"valid maxIO", "maxIO", true},
		{"invalid superFast", "superFast", false},
		{"invalid empty", "", false},
		{"invalid uppercase", "MAXIO", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidPerformanceMode(tt.mode)
			if got != tt.valid {
				t.Errorf("isValidPerformanceMode(%q) = %v, want %v", tt.mode, got, tt.valid)
			}
		})
	}
}

func TestThroughputModeValidation(t *testing.T) {
	tests := []struct {
		name  string
		mode  string
		valid bool
	}{
		{"valid bursting", "bursting", true},
		{"valid provisioned", "provisioned", true},
		{"valid elastic", "elastic", true},
		{"invalid unlimited", "unlimited", false},
		{"invalid empty", "", false},
		{"invalid uppercase", "BURSTING", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidThroughputMode(tt.mode)
			if got != tt.valid {
				t.Errorf("isValidThroughputMode(%q) = %v, want %v", tt.mode, got, tt.valid)
			}
		})
	}
}

func TestStateValidation(t *testing.T) {
	tests := []struct {
		name  string
		state string
		valid bool
	}{
		{"valid Provisioning", "Provisioning", true},
		{"valid Active", "Active", true},
		{"valid Updating", "Updating", true},
		{"valid Deleting", "Deleting", true},
		{"valid Failed", "Failed", true},
		{"invalid Running", "Running", false},
		{"invalid lowercase", "active", false},
		{"invalid empty", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidState(tt.state)
			if got != tt.valid {
				t.Errorf("isValidState(%q) = %v, want %v", tt.state, got, tt.valid)
			}
		})
	}
}

func TestMountTargetValidation(t *testing.T) {
	tests := []struct {
		name     string
		mtId     string
		subnetId string
		ip       string
		valid    bool
	}{
		{
			name:     "valid mount target",
			mtId:     "fsmt-0123456789abcdef0",
			subnetId: "subnet-0123456789abcdef0",
			ip:       "10.0.1.100",
			valid:    true,
		},
		{
			name:     "invalid mount target ID",
			mtId:     "mt-invalid",
			subnetId: "subnet-0123456789abcdef0",
			ip:       "10.0.1.100",
			valid:    false,
		},
		{
			name:     "invalid subnet ID",
			mtId:     "fsmt-0123456789abcdef0",
			subnetId: "invalid-subnet",
			ip:       "10.0.1.100",
			valid:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validMt := strings.HasPrefix(tt.mtId, "fsmt-")
			validSubnet := strings.HasPrefix(tt.subnetId, "subnet-")

			isValid := validMt && validSubnet
			if isValid != tt.valid {
				t.Errorf("Mount target validation = %v, want %v", isValid, tt.valid)
			}
		})
	}
}

func TestProvisionedThroughputValidation(t *testing.T) {
	tests := []struct {
		name       string
		throughput float64
		mode       string
		valid      bool
	}{
		{
			name:       "valid provisioned throughput",
			throughput: 100.0,
			mode:       "provisioned",
			valid:      true,
		},
		{
			name:       "throughput at min boundary",
			throughput: 1.0,
			mode:       "provisioned",
			valid:      true,
		},
		{
			name:       "throughput at max boundary",
			throughput: 1024.0,
			mode:       "provisioned",
			valid:      true,
		},
		{
			name:       "throughput too low",
			throughput: 0.5,
			mode:       "provisioned",
			valid:      false,
		},
		{
			name:       "throughput too high",
			throughput: 2048.0,
			mode:       "provisioned",
			valid:      false,
		},
		{
			name:       "throughput with bursting mode",
			throughput: 100.0,
			mode:       "bursting",
			valid:      false, // throughput should not be set for bursting mode
		},
		{
			name:       "throughput with elastic mode",
			throughput: 100.0,
			mode:       "elastic",
			valid:      false, // throughput should not be set for elastic mode
		},
		{
			name:       "no throughput with bursting",
			throughput: 0,
			mode:       "bursting",
			valid:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Validate throughput ranges (1-1024 MiBps for provisioned mode)
			valid := true
			if tt.mode == "provisioned" {
				if tt.throughput < 1.0 || tt.throughput > 1024.0 {
					valid = false
				}
			} else if tt.throughput > 0 {
				// Other modes should not have throughput set
				valid = false
			}

			if valid != tt.valid {
				t.Errorf("Throughput validation failed: got %v, want %v", valid, tt.valid)
			}
		})
	}
}

func TestEFSNamespaceFinalizers(t *testing.T) {
	finalizer := "efsnamespace.csi.aws.com/cleanup"

	// Test adding finalizer
	finalizers := []string{}
	finalizers = append(finalizers, finalizer)

	if len(finalizers) != 1 {
		t.Errorf("Expected 1 finalizer after adding, got %d", len(finalizers))
	}

	if finalizers[0] != finalizer {
		t.Errorf("Expected finalizer %s, got %s", finalizer, finalizers[0])
	}

	// Test checking for finalizer
	hasFinalizer := false
	for _, f := range finalizers {
		if f == finalizer {
			hasFinalizer = true
			break
		}
	}

	if !hasFinalizer {
		t.Error("Expected finalizer not found")
	}

	// Test removing finalizer
	var newFinalizers []string
	for _, f := range finalizers {
		if f != finalizer {
			newFinalizers = append(newFinalizers, f)
		}
	}

	if len(newFinalizers) != 0 {
		t.Errorf("Expected 0 finalizers after removal, got %d", len(newFinalizers))
	}
}

func TestEFSConditions(t *testing.T) {
	conditions := []struct {
		condType string
		status   string
		reason   string
		message  string
	}{
		{
			condType: "Ready",
			status:   "True",
			reason:   "FileSystemReady",
			message:  "EFS filesystem is ready for use",
		},
		{
			condType: "Provisioning",
			status:   "False",
			reason:   "ProvisioningComplete",
			message:  "EFS filesystem provisioning completed",
		},
		{
			condType: "Error",
			status:   "False",
			reason:   "NoErrors",
			message:  "No errors detected",
		},
	}

	for _, cond := range conditions {
		// Validate condition type is not empty
		if cond.condType == "" {
			t.Error("Condition type should not be empty")
		}

		// Validate status is either True, False, or Unknown
		validStatus := cond.status == "True" || cond.status == "False" || cond.status == "Unknown"
		if !validStatus {
			t.Errorf("Invalid condition status: %s", cond.status)
		}

		// Validate reason follows CamelCase convention
		if cond.reason != "" && !isValidCamelCase(cond.reason) {
			t.Errorf("Condition reason should be CamelCase: %s", cond.reason)
		}

		// Validate message is provided
		if cond.message == "" {
			t.Error("Condition message should not be empty")
		}
	}
}

func isValidCamelCase(s string) bool {
	// Simple check for CamelCase: starts with uppercase letter
	// and contains only letters and numbers
	camelCaseRegex := regexp.MustCompile(`^[A-Z][a-zA-Z0-9]*$`)
	return camelCaseRegex.MatchString(s)
}