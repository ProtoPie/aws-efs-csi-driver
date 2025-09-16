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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=efsns,categories={storage,efs,csi}
// +kubebuilder:printcolumn:name="Namespace",type="string",JSONPath=".spec.namespace"
// +kubebuilder:printcolumn:name="FileSystem",type="string",JSONPath=".status.fileSystemId"
// +kubebuilder:printcolumn:name="State",type="string",JSONPath=".status.state"
// +kubebuilder:printcolumn:name="AccessPoints",type="integer",JSONPath=".status.accessPointCount"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// EFSNamespace represents a mapping between a Kubernetes namespace and an AWS EFS filesystem
type EFSNamespace struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EFSNamespaceSpec   `json:"spec"`
	Status EFSNamespaceStatus `json:"status,omitempty"`
}

// EFSNamespaceSpec defines the desired state of EFSNamespace
type EFSNamespaceSpec struct {
	// The Kubernetes namespace to map to an EFS filesystem
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Namespace string `json:"namespace"`

	// The AWS EFS filesystem ID (optional - will be auto-created if not specified)
	// +kubebuilder:validation:Pattern=`^fs-[0-9a-f]{8,40}$`
	// +optional
	FileSystemID string `json:"fileSystemId,omitempty"`

	// The ARN of the EFS filesystem
	// +kubebuilder:validation:Pattern=`^arn:aws[a-z-]*:elasticfilesystem:[a-z0-9-]+:[0-9]{12}:file-system/fs-[0-9a-f]{8,40}$`
	// +optional
	FileSystemArn string `json:"fileSystemArn,omitempty"`

	// AWS region where the EFS filesystem exists or will be created
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^[a-z]{2}-[a-z]+-[0-9]{1}$`
	Region string `json:"region"`

	// Performance mode of the EFS filesystem
	// +kubebuilder:validation:Enum=generalPurpose;maxIO
	// +kubebuilder:default=generalPurpose
	// +optional
	PerformanceMode string `json:"performanceMode,omitempty"`

	// Throughput mode of the EFS filesystem
	// +kubebuilder:validation:Enum=bursting;provisioned;elastic
	// +kubebuilder:default=bursting
	// +optional
	ThroughputMode string `json:"throughputMode,omitempty"`

	// Provisioned throughput in MiB/s (required when throughputMode is provisioned)
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=3414
	// +optional
	ProvisionedThroughputInMibps *float64 `json:"provisionedThroughputInMibps,omitempty"`

	// Enable encryption at rest for the EFS filesystem
	// +kubebuilder:default=true
	// +optional
	Encrypted *bool `json:"encrypted,omitempty"`

	// KMS key ID or ARN for encryption (uses AWS managed key if not specified)
	// +kubebuilder:validation:Pattern=`^(arn:aws[a-z-]*:kms:[a-z0-9-]+:[0-9]{12}:(key|alias)/)?[a-zA-Z0-9-/_]+$`
	// +optional
	KmsKeyID string `json:"kmsKeyId,omitempty"`

	// Lifecycle policy for transitioning files to infrequent access storage
	// +kubebuilder:validation:Enum=AFTER_7_DAYS;AFTER_14_DAYS;AFTER_30_DAYS;AFTER_60_DAYS;AFTER_90_DAYS;AFTER_1_DAY
	// +optional
	LifecyclePolicy string `json:"lifecyclePolicy,omitempty"`

	// AWS Backup policy status
	// +kubebuilder:validation:Enum=ENABLED;DISABLED
	// +kubebuilder:default=ENABLED
	// +optional
	BackupPolicy string `json:"backupPolicy,omitempty"`

	// Cleanup policy when namespace is deleted
	// +kubebuilder:validation:Enum=retain;delete
	// +kubebuilder:default=retain
	// +optional
	CleanupPolicy string `json:"cleanupPolicy,omitempty"`

	// Mount targets configuration for the EFS filesystem
	// +optional
	MountTargets *MountTargetConfig `json:"mountTargets,omitempty"`

	// AWS tags to apply to the EFS filesystem
	// +optional
	Tags map[string]string `json:"tags,omitempty"`

	// List of access points associated with this namespace
	// +optional
	AccessPoints []AccessPointInfo `json:"accessPoints,omitempty"`
}

// MountTargetConfig defines mount target configuration
type MountTargetConfig struct {
	// List of subnet IDs for creating mount targets
	// +kubebuilder:validation:Pattern=`^subnet-[0-9a-f]{8,40}$`
	// +optional
	SubnetIDs []string `json:"subnetIds,omitempty"`

	// List of security group IDs for mount targets
	// +kubebuilder:validation:Pattern=`^sg-[0-9a-f]{8,40}$`
	// +optional
	SecurityGroupIDs []string `json:"securityGroupIds,omitempty"`

	// Optional IP addresses for mount targets
	// +kubebuilder:validation:Pattern=`^((25[0-5]|(2[0-4]|1\d|[1-9]|)\d)\.?\b){4}$`
	// +optional
	IPAddresses []string `json:"ipAddresses,omitempty"`
}

// AccessPointInfo contains information about an EFS access point
type AccessPointInfo struct {
	// AWS EFS Access Point ID
	// +kubebuilder:validation:Pattern=`^fsap-[0-9a-f]{8,40}$`
	AccessPointID string `json:"accessPointId"`

	// Name of the PVC this access point is for
	PvcName string `json:"pvcName"`

	// Namespace of the PVC
	PvcNamespace string `json:"pvcNamespace"`

	// Path within the EFS filesystem
	Path string `json:"path"`

	// POSIX user configuration
	// +optional
	PosixUser *PosixUser `json:"posixUser,omitempty"`

	// Root directory configuration
	// +optional
	RootDirectory *RootDirectory `json:"rootDirectory,omitempty"`
}

// PosixUser defines POSIX user configuration
type PosixUser struct {
	// User ID
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=4294967295
	UID int64 `json:"uid"`

	// Group ID
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=4294967295
	GID int64 `json:"gid"`

	// Secondary group IDs
	// +optional
	SecondaryGIDs []int64 `json:"secondaryGids,omitempty"`
}

// RootDirectory defines root directory configuration
type RootDirectory struct {
	// Path within the filesystem
	Path string `json:"path"`

	// Creation info for the directory
	// +optional
	CreationInfo *CreationInfo `json:"creationInfo,omitempty"`
}

// CreationInfo defines directory creation information
type CreationInfo struct {
	// Owner user ID
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=4294967295
	OwnerUID int64 `json:"ownerUid"`

	// Owner group ID
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=4294967295
	OwnerGID int64 `json:"ownerGid"`

	// Directory permissions (octal notation)
	// +kubebuilder:validation:Pattern=`^[0-7]{3,4}$`
	Permissions string `json:"permissions"`
}

// EFSNamespaceStatus defines the observed state of EFSNamespace
type EFSNamespaceStatus struct {
	// Current state of the EFS namespace mapping
	// +kubebuilder:validation:Enum=Provisioning;Active;Updating;Deleting;Failed;Unknown
	State string `json:"state,omitempty"`

	// The actual EFS filesystem ID in use
	// +kubebuilder:validation:Pattern=`^fs-[0-9a-f]{8,40}$`
	FileSystemID string `json:"fileSystemId,omitempty"`

	// The ARN of the created EFS filesystem
	FileSystemArn string `json:"fileSystemArn,omitempty"`

	// List of mount targets created for this filesystem
	// +optional
	MountTargets []MountTargetStatus `json:"mountTargets,omitempty"`

	// Number of access points created for this namespace
	// +kubebuilder:validation:Minimum=0
	AccessPointCount int `json:"accessPointCount,omitempty"`

	// Last time this resource was updated
	LastUpdated *metav1.Time `json:"lastUpdated,omitempty"`

	// Human-readable message indicating details about current state
	Message string `json:"message,omitempty"`

	// Conditions represent the latest available observations of the EFSNamespace state
	// +optional
	Conditions []EFSNamespaceCondition `json:"conditions,omitempty"`

	// ObservedGeneration reflects the generation most recently observed
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// MountTargetStatus contains status of a mount target
type MountTargetStatus struct {
	// Mount target ID
	// +kubebuilder:validation:Pattern=`^fsmt-[0-9a-f]{8,40}$`
	MountTargetID string `json:"mountTargetId"`

	// Availability zone of the mount target
	AvailabilityZone string `json:"availabilityZone"`

	// Subnet ID of the mount target
	// +kubebuilder:validation:Pattern=`^subnet-[0-9a-f]{8,40}$`
	SubnetID string `json:"subnetId"`

	// IP address of the mount target
	IPAddress string `json:"ipAddress"`

	// Network interface ID
	// +kubebuilder:validation:Pattern=`^eni-[0-9a-f]{8,40}$`
	NetworkInterfaceID string `json:"networkInterfaceId"`

	// Lifecycle state of the mount target
	// +kubebuilder:validation:Enum=creating;available;updating;deleting;deleted;error
	LifecycleState string `json:"lifecycleState"`
}

// EFSNamespaceConditionType represents condition types
type EFSNamespaceConditionType string

const (
	// EFSNamespaceReady indicates the namespace mapping is ready for use
	EFSNamespaceReady EFSNamespaceConditionType = "Ready"
	// EFSNamespaceProvisioning indicates the namespace is being provisioned
	EFSNamespaceProvisioning EFSNamespaceConditionType = "Provisioning"
	// EFSNamespaceProgressing indicates the namespace is progressing
	EFSNamespaceProgressing EFSNamespaceConditionType = "Progressing"
	// EFSNamespaceDegraded indicates the namespace is degraded
	EFSNamespaceDegraded EFSNamespaceConditionType = "Degraded"
	// EFSNamespaceError indicates an error occurred
	EFSNamespaceError EFSNamespaceConditionType = "Error"
)

// EFSNamespaceCondition describes the state of an EFSNamespace at a certain point
type EFSNamespaceCondition struct {
	// Type of condition
	// +kubebuilder:validation:Enum=Ready;Provisioning;Progressing;Degraded;Error
	Type EFSNamespaceConditionType `json:"type"`

	// Status of the condition (True, False, Unknown)
	// +kubebuilder:validation:Enum=True;False;Unknown
	Status metav1.ConditionStatus `json:"status"`

	// Last time the condition transitioned from one status to another
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`

	// Machine-readable reason for the condition's last transition
	Reason string `json:"reason,omitempty"`

	// Human-readable message indicating details about the transition
	Message string `json:"message,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EFSNamespaceList contains a list of EFSNamespace
type EFSNamespaceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EFSNamespace `json:"items"`
}