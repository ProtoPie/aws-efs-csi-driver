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
	"context"
	"fmt"
	"strings"
	"time"
)

const (
	// EFS Namespace provisioning mode identifier
	EFSNSProvisioningMode = "efs-ns"

	// Volume ID format: efs-ns::{namespace}::{filesystem-id}::{cluster-id}
	VolumeIDSeparator = "::"
	VolumeIDPrefix    = "efs-ns"
)

// EFSNSStorageClassParameters represents StorageClass parameters for efs-ns mode
type EFSNSStorageClassParameters struct {
	// ProvisioningMode should be "efs-ns"
	ProvisioningMode string `json:"provisioningMode"`

	// PerformanceMode: "generalPurpose" or "maxIO"
	PerformanceMode string `json:"performanceMode,omitempty"`

	// ThroughputMode: "bursting" or "provisioned"
	ThroughputMode string `json:"throughputMode,omitempty"`

	// ProvisionedThroughputInMibps for provisioned throughput mode
	ProvisionedThroughputInMibps *int64 `json:"provisionedThroughputInMibps,omitempty"`

	// Encrypted indicates if the filesystem should be encrypted
	Encrypted *bool `json:"encrypted,omitempty"`

	// KmsKeyId for encryption
	KmsKeyId *string `json:"kmsKeyId,omitempty"`

	// Tags to apply to the filesystem
	Tags map[string]string `json:"tags,omitempty"`

	// EncryptInTransit for TLS mounting
	EncryptInTransit *bool `json:"encryptInTransit,omitempty"`
}

// EFSNSVolumeID represents a parsed efs-ns volume identifier
// Format: efs-ns::{namespace}::{filesystem-id}::{cluster-id}
type EFSNSVolumeID struct {
	Namespace    string
	FileSystemID string
	ClusterID    string
}

// String returns the string representation of the volume ID
func (v *EFSNSVolumeID) String() string {
	return fmt.Sprintf("%s%s%s%s%s%s%s",
		VolumeIDPrefix, VolumeIDSeparator,
		v.Namespace, VolumeIDSeparator,
		v.FileSystemID, VolumeIDSeparator,
		v.ClusterID)
}

// ParseEFSNSVolumeID parses a volume ID string into EFSNSVolumeID
func ParseEFSNSVolumeID(volumeID string) (*EFSNSVolumeID, error) {
	if volumeID == "" {
		return nil, NewEFSNSError(ErrInvalidVolumeID, "ParseEFSNSVolumeID", "", "volume ID cannot be empty", nil)
	}

	parts := strings.Split(volumeID, VolumeIDSeparator)
	if len(parts) != 4 {
		return nil, NewEFSNSError(ErrInvalidVolumeID, "ParseEFSNSVolumeID", "",
			fmt.Sprintf("invalid efs-ns volume ID format: %s, expected format: efs-ns::{namespace}::{filesystem-id}::{cluster-id}", volumeID), nil)
	}

	if parts[0] != VolumeIDPrefix {
		return nil, NewEFSNSError(ErrInvalidVolumeID, "ParseEFSNSVolumeID", "",
			fmt.Sprintf("volume ID must start with '%s', got: %s", VolumeIDPrefix, parts[0]), nil)
	}

	namespace := parts[1]
	filesystemID := parts[2]
	clusterID := parts[3]

	if namespace == "" {
		return nil, NewEFSNSError(ErrInvalidVolumeID, "ParseEFSNSVolumeID", "", "namespace cannot be empty", nil)
	}
	if filesystemID == "" {
		return nil, NewEFSNSError(ErrInvalidVolumeID, "ParseEFSNSVolumeID", "", "filesystem ID cannot be empty", nil)
	}
	if clusterID == "" {
		return nil, NewEFSNSError(ErrInvalidVolumeID, "ParseEFSNSVolumeID", "", "cluster ID cannot be empty", nil)
	}

	return &EFSNSVolumeID{
		Namespace:    namespace,
		FileSystemID: filesystemID,
		ClusterID:    clusterID,
	}, nil
}

// FileSystemState represents the state of an EFS filesystem
type FileSystemState string

const (
	FileSystemStateCreating  FileSystemState = "creating"
	FileSystemStateAvailable FileSystemState = "available"
	FileSystemStateDeleting  FileSystemState = "deleting"
	FileSystemStateDeleted   FileSystemState = "deleted"
	FileSystemStateError     FileSystemState = "error"
)

// IsValid checks if the filesystem state is valid
func (s FileSystemState) IsValid() bool {
	switch s {
	case FileSystemStateCreating, FileSystemStateAvailable, FileSystemStateDeleting, FileSystemStateDeleted, FileSystemStateError:
		return true
	}
	return false
}

// MountTargetInfo represents information about an EFS mount target
type MountTargetInfo struct {
	MountTargetID string
	SubnetID      string
	IPAddress     string
	State         string
}

// FileSystemInfo contains comprehensive information about an EFS filesystem
type FileSystemInfo struct {
	// FileSystemID is the AWS EFS filesystem identifier
	FileSystemID string

	// Namespace is the Kubernetes namespace this filesystem belongs to
	Namespace string

	// ClusterID identifies the Kubernetes cluster
	ClusterID string

	// CreatedAt is when the filesystem was created
	CreatedAt time.Time

	// MountTargets contains the mount target information for this filesystem
	MountTargets []*MountTargetInfo

	// SecurityGroupID is the ID of the security group used for this filesystem
	SecurityGroupID string

	// State represents the current state of the filesystem
	State FileSystemState

	// PVCCount is the number of PVCs currently using this filesystem
	PVCCount int32

	// Tags are the tags applied to this filesystem
	Tags map[string]string

	// PerformanceMode is the performance mode of the filesystem
	PerformanceMode string

	// ThroughputMode is the throughput mode of the filesystem
	ThroughputMode string

	// Encrypted indicates if the filesystem is encrypted
	Encrypted bool
}

// IsEmpty returns true if the filesystem has no PVCs using it
func (f *FileSystemInfo) IsEmpty() bool {
	return f.PVCCount <= 0
}

// CanBeDeleted returns true if the filesystem can be safely deleted
func (f *FileSystemInfo) CanBeDeleted() bool {
	return f.IsEmpty() && (f.State == FileSystemStateAvailable || f.State == FileSystemStateError)
}

// FileSystemOptions contains options for creating an EFS filesystem
type FileSystemOptions struct {
	// PerformanceMode: "generalPurpose" or "maxIO"
	PerformanceMode string

	// ThroughputMode: "bursting" or "provisioned"
	ThroughputMode string

	// ProvisionedThroughputInMibps for provisioned throughput mode
	ProvisionedThroughputInMibps *int64

	// Encrypted indicates if the filesystem should be encrypted
	Encrypted *bool

	// KmsKeyID for encryption
	KmsKeyID *string

	// Tags to apply to the filesystem
	Tags map[string]string
}

// Validate checks if the filesystem options are valid
func (o *FileSystemOptions) Validate() error {
	if o.PerformanceMode != "" && o.PerformanceMode != "generalPurpose" && o.PerformanceMode != "maxIO" {
		return NewEFSNSError(ErrInvalidParameter, "FileSystemOptions.Validate", "",
			fmt.Sprintf("invalid performance mode: %s, must be 'generalPurpose' or 'maxIO'", o.PerformanceMode), nil)
	}

	if o.ThroughputMode != "" && o.ThroughputMode != "bursting" && o.ThroughputMode != "provisioned" {
		return NewEFSNSError(ErrInvalidParameter, "FileSystemOptions.Validate", "",
			fmt.Sprintf("invalid throughput mode: %s, must be 'bursting' or 'provisioned'", o.ThroughputMode), nil)
	}

	if o.ThroughputMode == "provisioned" && (o.ProvisionedThroughputInMibps == nil || *o.ProvisionedThroughputInMibps <= 0) {
		return NewEFSNSError(ErrInvalidParameter, "FileSystemOptions.Validate", "",
			"provisioned throughput mode requires a positive provisionedThroughputInMibps value", nil)
	}

	return nil
}

// PVCMappingEntry represents a PVC to filesystem mapping entry
type PVCMappingEntry struct {
	Namespace    string    `json:"namespace"`
	PVCName      string    `json:"pvcName"`
	VolumeID     string    `json:"volumeId"`
	FileSystemID string    `json:"fileSystemId"`
	CreatedAt    time.Time `json:"createdAt"`
}

// Validate checks if the PVC mapping entry is valid
func (p *PVCMappingEntry) Validate() error {
	if p.Namespace == "" {
		return NewEFSNSError(ErrInvalidParameter, "PVCMappingEntry.Validate", "", "namespace cannot be empty", nil)
	}
	if p.PVCName == "" {
		return NewEFSNSError(ErrInvalidParameter, "PVCMappingEntry.Validate", "", "PVC name cannot be empty", nil)
	}
	if p.VolumeID == "" {
		return NewEFSNSError(ErrInvalidParameter, "PVCMappingEntry.Validate", "", "volume ID cannot be empty", nil)
	}
	if p.FileSystemID == "" {
		return NewEFSNSError(ErrInvalidParameter, "PVCMappingEntry.Validate", "", "filesystem ID cannot be empty", nil)
	}
	return nil
}

// Constants for finalizers
const (
	EFSNSFinalizerName          = "efs-csi.aws.com/efs-ns-cleanup"
	EFSNSNamespaceFinalizerName = "efs-csi.aws.com/efs-ns-namespace-cleanup"
)

// Constants for default values
const (
	DefaultPerformanceMode  = "generalPurpose"
	DefaultThroughputMode   = "bursting"
	DefaultEncrypted        = true
	DefaultEncryptInTransit = true
	DefaultCacheTTL         = 5 * time.Minute
)

// NamespaceFileSystemManager interface handles EFS filesystem lifecycle for namespaces
type NamespaceFileSystemManager interface {
	// CreateOrGetFileSystemForNamespace creates a new EFS filesystem for namespace or returns existing one
	CreateOrGetFileSystemForNamespace(ctx context.Context, namespace string, options *FileSystemOptions) (*FileSystemInfo, error)

	// DeleteFileSystemForNamespace deletes EFS filesystem if it's the last PVC in namespace
	DeleteFileSystemForNamespace(ctx context.Context, namespace string, volumeID string) error

	// GetFileSystemInfo retrieves filesystem info for a namespace
	GetFileSystemInfo(ctx context.Context, namespace string) (*FileSystemInfo, error)

	// ListFileSystemsForNamespace returns all filesystems for given namespace
	ListFileSystemsForNamespace(ctx context.Context, namespace string) ([]*FileSystemInfo, error)

	// SyncFromAWS synchronizes state with AWS EFS API
	SyncFromAWS(ctx context.Context) error
}
