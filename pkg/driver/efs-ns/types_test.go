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
	"testing"
	"time"
)

func TestEFSNSVolumeID_String(t *testing.T) {
	tests := []struct {
		name     string
		volumeID *EFSNSVolumeID
		expected string
	}{
		{
			name: "valid volume ID",
			volumeID: &EFSNSVolumeID{
				Namespace:    "default",
				FileSystemID: "fs-12345678",
				ClusterID:    "my-cluster",
			},
			expected: "efs-ns::default::fs-12345678::my-cluster",
		},
		{
			name: "volume ID with special characters",
			volumeID: &EFSNSVolumeID{
				Namespace:    "test-namespace",
				FileSystemID: "fs-abcdef12",
				ClusterID:    "cluster-with-dashes",
			},
			expected: "efs-ns::test-namespace::fs-abcdef12::cluster-with-dashes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.volumeID.String()
			if result != tt.expected {
				t.Errorf("EFSNSVolumeID.String() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestParseEFSNSVolumeID(t *testing.T) {
	tests := []struct {
		name        string
		volumeID    string
		expected    *EFSNSVolumeID
		expectError bool
		errorType   EFSNSErrorType
	}{
		{
			name:     "valid volume ID",
			volumeID: "efs-ns::default::fs-12345678::my-cluster",
			expected: &EFSNSVolumeID{
				Namespace:    "default",
				FileSystemID: "fs-12345678",
				ClusterID:    "my-cluster",
			},
			expectError: false,
		},
		{
			name:        "empty volume ID",
			volumeID:    "",
			expectError: true,
			errorType:   ErrInvalidVolumeID,
		},
		{
			name:        "invalid format - too few parts",
			volumeID:    "efs-ns::default::fs-12345678",
			expectError: true,
			errorType:   ErrInvalidVolumeID,
		},
		{
			name:        "invalid format - too many parts",
			volumeID:    "efs-ns::default::fs-12345678::my-cluster::extra",
			expectError: true,
			errorType:   ErrInvalidVolumeID,
		},
		{
			name:        "invalid prefix",
			volumeID:    "wrong-prefix::default::fs-12345678::my-cluster",
			expectError: true,
			errorType:   ErrInvalidVolumeID,
		},
		{
			name:        "empty namespace",
			volumeID:    "efs-ns::::fs-12345678::my-cluster",
			expectError: true,
			errorType:   ErrInvalidVolumeID,
		},
		{
			name:        "empty filesystem ID",
			volumeID:    "efs-ns::default::::my-cluster",
			expectError: true,
			errorType:   ErrInvalidVolumeID,
		},
		{
			name:        "empty cluster ID",
			volumeID:    "efs-ns::default::fs-12345678::",
			expectError: true,
			errorType:   ErrInvalidVolumeID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseEFSNSVolumeID(tt.volumeID)

			if tt.expectError {
				if err == nil {
					t.Errorf("ParseEFSNSVolumeID() expected error but got none")
					return
				}

				if !IsInvalidVolumeID(err) {
					t.Errorf("ParseEFSNSVolumeID() error type = %T, want EFSNSError with type %v", err, tt.errorType)
				}
				return
			}

			if err != nil {
				t.Errorf("ParseEFSNSVolumeID() unexpected error = %v", err)
				return
			}

			if result == nil {
				t.Errorf("ParseEFSNSVolumeID() returned nil result")
				return
			}

			if result.Namespace != tt.expected.Namespace {
				t.Errorf("ParseEFSNSVolumeID() namespace = %v, want %v", result.Namespace, tt.expected.Namespace)
			}
			if result.FileSystemID != tt.expected.FileSystemID {
				t.Errorf("ParseEFSNSVolumeID() filesystemID = %v, want %v", result.FileSystemID, tt.expected.FileSystemID)
			}
			if result.ClusterID != tt.expected.ClusterID {
				t.Errorf("ParseEFSNSVolumeID() clusterID = %v, want %v", result.ClusterID, tt.expected.ClusterID)
			}
		})
	}
}

func TestParseEFSNSVolumeID_RoundTrip(t *testing.T) {
	testCases := []EFSNSVolumeID{
		{
			Namespace:    "default",
			FileSystemID: "fs-12345678",
			ClusterID:    "my-cluster",
		},
		{
			Namespace:    "test-namespace-with-dashes",
			FileSystemID: "fs-abcdef12",
			ClusterID:    "cluster-with-dashes",
		},
		{
			Namespace:    "ns",
			FileSystemID: "fs-1",
			ClusterID:    "c",
		},
	}

	for _, original := range testCases {
		t.Run("roundtrip", func(t *testing.T) {
			// Convert to string and back
			volumeIDStr := original.String()
			parsed, err := ParseEFSNSVolumeID(volumeIDStr)

			if err != nil {
				t.Errorf("ParseEFSNSVolumeID() unexpected error = %v", err)
				return
			}

			if parsed.Namespace != original.Namespace ||
				parsed.FileSystemID != original.FileSystemID ||
				parsed.ClusterID != original.ClusterID {
				t.Errorf("Round trip failed: original=%+v, parsed=%+v", original, parsed)
			}
		})
	}
}

func TestFileSystemState_IsValid(t *testing.T) {
	tests := []struct {
		name  string
		state FileSystemState
		valid bool
	}{
		{"creating state", FileSystemStateCreating, true},
		{"available state", FileSystemStateAvailable, true},
		{"deleting state", FileSystemStateDeleting, true},
		{"deleted state", FileSystemStateDeleted, true},
		{"error state", FileSystemStateError, true},
		{"invalid state", FileSystemState("invalid"), false},
		{"empty state", FileSystemState(""), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.state.IsValid()
			if result != tt.valid {
				t.Errorf("FileSystemState.IsValid() = %v, want %v", result, tt.valid)
			}
		})
	}
}

func TestFileSystemInfo_IsEmpty(t *testing.T) {
	tests := []struct {
		name     string
		pvcCount int32
		expected bool
	}{
		{"empty filesystem", 0, true},
		{"filesystem with one PVC", 1, false},
		{"filesystem with multiple PVCs", 5, false},
		{"negative PVC count", -1, true}, // edge case
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &FileSystemInfo{PVCCount: tt.pvcCount}
			result := fs.IsEmpty()
			if result != tt.expected {
				t.Errorf("FileSystemInfo.IsEmpty() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestFileSystemInfo_CanBeDeleted(t *testing.T) {
	tests := []struct {
		name     string
		pvcCount int32
		state    FileSystemState
		expected bool
	}{
		{"empty available filesystem", 0, FileSystemStateAvailable, true},
		{"empty error filesystem", 0, FileSystemStateError, true},
		{"empty creating filesystem", 0, FileSystemStateCreating, false},
		{"empty deleting filesystem", 0, FileSystemStateDeleting, false},
		{"non-empty available filesystem", 1, FileSystemStateAvailable, false},
		{"non-empty error filesystem", 1, FileSystemStateError, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &FileSystemInfo{
				PVCCount: tt.pvcCount,
				State:    tt.state,
			}
			result := fs.CanBeDeleted()
			if result != tt.expected {
				t.Errorf("FileSystemInfo.CanBeDeleted() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestFileSystemOptions_Validate(t *testing.T) {
	tests := []struct {
		name        string
		options     *FileSystemOptions
		expectError bool
		errorType   EFSNSErrorType
	}{
		{
			name: "valid options - general purpose",
			options: &FileSystemOptions{
				PerformanceMode: "generalPurpose",
				ThroughputMode:  "bursting",
			},
			expectError: false,
		},
		{
			name: "valid options - max IO",
			options: &FileSystemOptions{
				PerformanceMode: "maxIO",
				ThroughputMode:  "bursting",
			},
			expectError: false,
		},
		{
			name: "valid options - provisioned throughput",
			options: &FileSystemOptions{
				PerformanceMode:              "generalPurpose",
				ThroughputMode:               "provisioned",
				ProvisionedThroughputInMibps: func(i int64) *int64 { return &i }(100),
			},
			expectError: false,
		},
		{
			name:        "empty options",
			options:     &FileSystemOptions{},
			expectError: false,
		},
		{
			name: "invalid performance mode",
			options: &FileSystemOptions{
				PerformanceMode: "invalid",
			},
			expectError: true,
			errorType:   ErrInvalidParameter,
		},
		{
			name: "invalid throughput mode",
			options: &FileSystemOptions{
				ThroughputMode: "invalid",
			},
			expectError: true,
			errorType:   ErrInvalidParameter,
		},
		{
			name: "provisioned throughput without value",
			options: &FileSystemOptions{
				ThroughputMode: "provisioned",
			},
			expectError: true,
			errorType:   ErrInvalidParameter,
		},
		{
			name: "provisioned throughput with zero value",
			options: &FileSystemOptions{
				ThroughputMode:               "provisioned",
				ProvisionedThroughputInMibps: func(i int64) *int64 { return &i }(0),
			},
			expectError: true,
			errorType:   ErrInvalidParameter,
		},
		{
			name: "provisioned throughput with negative value",
			options: &FileSystemOptions{
				ThroughputMode:               "provisioned",
				ProvisionedThroughputInMibps: func(i int64) *int64 { return &i }(-1),
			},
			expectError: true,
			errorType:   ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.options.Validate()

			if tt.expectError {
				if err == nil {
					t.Errorf("FileSystemOptions.Validate() expected error but got none")
					return
				}

				if !IsInvalidParameter(err) {
					t.Errorf("FileSystemOptions.Validate() error type want %v, got %v", tt.errorType, err)
				}
				return
			}

			if err != nil {
				t.Errorf("FileSystemOptions.Validate() unexpected error = %v", err)
			}
		})
	}
}

func TestPVCMappingEntry_Validate(t *testing.T) {
	tests := []struct {
		name        string
		entry       *PVCMappingEntry
		expectError bool
	}{
		{
			name: "valid entry",
			entry: &PVCMappingEntry{
				Namespace:    "default",
				PVCName:      "test-pvc",
				VolumeID:     "efs-ns::default::fs-12345678::my-cluster",
				FileSystemID: "fs-12345678",
				CreatedAt:    time.Now(),
			},
			expectError: false,
		},
		{
			name: "empty namespace",
			entry: &PVCMappingEntry{
				Namespace:    "",
				PVCName:      "test-pvc",
				VolumeID:     "efs-ns::default::fs-12345678::my-cluster",
				FileSystemID: "fs-12345678",
			},
			expectError: true,
		},
		{
			name: "empty PVC name",
			entry: &PVCMappingEntry{
				Namespace:    "default",
				PVCName:      "",
				VolumeID:     "efs-ns::default::fs-12345678::my-cluster",
				FileSystemID: "fs-12345678",
			},
			expectError: true,
		},
		{
			name: "empty volume ID",
			entry: &PVCMappingEntry{
				Namespace:    "default",
				PVCName:      "test-pvc",
				VolumeID:     "",
				FileSystemID: "fs-12345678",
			},
			expectError: true,
		},
		{
			name: "empty filesystem ID",
			entry: &PVCMappingEntry{
				Namespace:    "default",
				PVCName:      "test-pvc",
				VolumeID:     "efs-ns::default::fs-12345678::my-cluster",
				FileSystemID: "",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.entry.Validate()

			if tt.expectError {
				if err == nil {
					t.Errorf("PVCMappingEntry.Validate() expected error but got none")
					return
				}

				if !IsInvalidParameter(err) {
					t.Errorf("PVCMappingEntry.Validate() expected InvalidParameter error, got %v", err)
				}
				return
			}

			if err != nil {
				t.Errorf("PVCMappingEntry.Validate() unexpected error = %v", err)
			}
		})
	}
}

func TestConstants(t *testing.T) {
	// Test that constants have expected values
	if EFSNSProvisioningMode != "efs-ns" {
		t.Errorf("EFSNSProvisioningMode = %v, want 'efs-ns'", EFSNSProvisioningMode)
	}

	if VolumeIDSeparator != "::" {
		t.Errorf("VolumeIDSeparator = %v, want '::'", VolumeIDSeparator)
	}

	if VolumeIDPrefix != "efs-ns" {
		t.Errorf("VolumeIDPrefix = %v, want 'efs-ns'", VolumeIDPrefix)
	}

	if DefaultPerformanceMode != "generalPurpose" {
		t.Errorf("DefaultPerformanceMode = %v, want 'generalPurpose'", DefaultPerformanceMode)
	}

	if DefaultThroughputMode != "bursting" {
		t.Errorf("DefaultThroughputMode = %v, want 'bursting'", DefaultThroughputMode)
	}

	if !DefaultEncrypted {
		t.Errorf("DefaultEncrypted = %v, want true", DefaultEncrypted)
	}

	if !DefaultEncryptInTransit {
		t.Errorf("DefaultEncryptInTransit = %v, want true", DefaultEncryptInTransit)
	}

	if DefaultCacheTTL != 5*time.Minute {
		t.Errorf("DefaultCacheTTL = %v, want 5 minutes", DefaultCacheTTL)
	}
}
