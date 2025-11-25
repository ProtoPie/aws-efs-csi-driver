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
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	// MaxWaitTime is the maximum time to wait for filesystem operations
	MaxWaitTime = 5 * time.Minute

	// FileSystemNameFormat defines the naming convention for EFS filesystems
	// Format: efs-ns-{namespace-name}-{cluster-id}
	FileSystemNameFormat = "efs-ns-%s-%s"

	// TagKeys for EFS filesystem tagging
	TagKeyNamespace   = "kubernetes.io/namespace"
	TagKeyCluster     = "kubernetes.io/cluster"
	TagKeyManagedBy   = "kubernetes.io/managed-by"
	TagKeyCreatedBy   = "kubernetes.io/created-by"
	TagKeyProvisioner = "kubernetes.io/provisioner"
	TagKeyComponent   = "kubernetes.io/component"

	// Tag values
	TagValueManagedBy   = "efs-csi-driver"
	TagValueCreatedBy   = "efs-ns-provisioner"
	TagValueProvisioner = "efs.csi.aws.com"
	TagValueComponent   = "efs-ns-filesystem"
)

// EFSClient interface abstracts the AWS EFS client for testing
type EFSClient interface {
	CreateFileSystem(ctx context.Context, params *efs.CreateFileSystemInput, optFns ...func(*efs.Options)) (*efs.CreateFileSystemOutput, error)
	DeleteFileSystem(ctx context.Context, params *efs.DeleteFileSystemInput, optFns ...func(*efs.Options)) (*efs.DeleteFileSystemOutput, error)
	DescribeFileSystems(ctx context.Context, params *efs.DescribeFileSystemsInput, optFns ...func(*efs.Options)) (*efs.DescribeFileSystemsOutput, error)
	DescribeMountTargets(ctx context.Context, params *efs.DescribeMountTargetsInput, optFns ...func(*efs.Options)) (*efs.DescribeMountTargetsOutput, error)
	CreateMountTarget(ctx context.Context, params *efs.CreateMountTargetInput, optFns ...func(*efs.Options)) (*efs.CreateMountTargetOutput, error)
	DeleteMountTarget(ctx context.Context, params *efs.DeleteMountTargetInput, optFns ...func(*efs.Options)) (*efs.DeleteMountTargetOutput, error)
	CreateTags(ctx context.Context, params *efs.CreateTagsInput, optFns ...func(*efs.Options)) (*efs.CreateTagsOutput, error)
}

// EC2Client interface abstracts the AWS EC2 client for security group management
type EC2Client interface {
	CreateSecurityGroup(ctx context.Context, params *ec2.CreateSecurityGroupInput, optFns ...func(*ec2.Options)) (*ec2.CreateSecurityGroupOutput, error)
	DeleteSecurityGroup(ctx context.Context, params *ec2.DeleteSecurityGroupInput, optFns ...func(*ec2.Options)) (*ec2.DeleteSecurityGroupOutput, error)
	AuthorizeSecurityGroupIngress(ctx context.Context, params *ec2.AuthorizeSecurityGroupIngressInput, optFns ...func(*ec2.Options)) (*ec2.AuthorizeSecurityGroupIngressOutput, error)
	DescribeSecurityGroups(ctx context.Context, params *ec2.DescribeSecurityGroupsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error)
	DescribeSubnets(ctx context.Context, params *ec2.DescribeSubnetsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error)
	DescribeVpcs(ctx context.Context, params *ec2.DescribeVpcsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeVpcsOutput, error)
}

// namespaceFileSystemManager implements the NamespaceFileSystemManager interface
type namespaceFileSystemManager struct {
	efsClient EFSClient
	ec2Client EC2Client
	k8sClient kubernetes.Interface
	cache     FileSystemCache
	tracker   PVCTracker
	clusterID string
	vpcID     string
	mutex     sync.RWMutex
}

// NewNamespaceFileSystemManager creates a new NamespaceFileSystemManager instance
func NewNamespaceFileSystemManager(
	efsClient EFSClient,
	ec2Client EC2Client,
	k8sClient kubernetes.Interface,
	cache FileSystemCache,
	tracker PVCTracker,
	clusterID string,
	vpcID string,
) NamespaceFileSystemManager {
	return &namespaceFileSystemManager{
		efsClient: efsClient,
		ec2Client: ec2Client,
		k8sClient: k8sClient,
		cache:     cache,
		tracker:   tracker,
		clusterID: clusterID,
		vpcID:     vpcID,
	}
}

// CreateOrGetFileSystemForNamespace creates a new EFS filesystem for namespace or returns existing one
func (m *namespaceFileSystemManager) CreateOrGetFileSystemForNamespace(
	ctx context.Context,
	namespace string,
	options *FileSystemOptions,
) (*FileSystemInfo, error) {
	if namespace == "" {
		return nil, NewEFSNSError(ErrInvalidParameter, "CreateOrGetFileSystemForNamespace", namespace, "namespace cannot be empty", nil)
	}

	if options == nil {
		return nil, NewEFSNSError(ErrInvalidParameter, "CreateOrGetFileSystemForNamespace", namespace, "options cannot be nil", nil)
	}

	if err := options.Validate(); err != nil {
		return nil, NewEFSNSError(ErrInvalidParameter, "CreateOrGetFileSystemForNamespace", namespace, "invalid filesystem options", err)
	}

	klog.V(4).Infof("CreateOrGetFileSystemForNamespace: namespace=%s, options=%+v", namespace, options)

	// Check cache first
	if fsInfo, found := m.cache.Get(namespace); found {
		klog.V(4).Infof("Found filesystem in cache: namespace=%s, fileSystemId=%s", namespace, fsInfo.FileSystemID)
		return fsInfo, nil
	}

	// Use mutex to prevent concurrent creation for the same namespace
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Double-check cache after acquiring lock
	if fsInfo, found := m.cache.Get(namespace); found {
		klog.V(4).Infof("Found filesystem in cache after lock: namespace=%s, fileSystemId=%s", namespace, fsInfo.FileSystemID)
		return fsInfo, nil
	}

	// Check if filesystem already exists in AWS
	existingFS, err := m.findExistingFileSystem(ctx, namespace)
	if err != nil {
		return nil, NewEFSNSError(ErrAWSAPIFailed, "CreateOrGetFileSystemForNamespace", namespace,
			"failed to check for existing filesystem", err)
	}

	if existingFS != nil {
		klog.V(4).Infof("Found existing filesystem: namespace=%s, fileSystemId=%s", namespace, existingFS.FileSystemID)
		m.cache.Set(namespace, existingFS)
		return existingFS, nil
	}

	// Create new filesystem
	klog.V(2).Infof("Creating new EFS filesystem for namespace: %s", namespace)
	fsInfo, err := m.createFileSystem(ctx, namespace, options)
	if err != nil {
		return nil, NewEFSNSError(ErrFileSystemCreationFailed, "CreateOrGetFileSystemForNamespace", namespace,
			"failed to create filesystem", err)
	}

	// Cache the new filesystem info
	m.cache.Set(namespace, fsInfo)

	klog.V(2).Infof("Successfully created filesystem: namespace=%s, fileSystemId=%s", namespace, fsInfo.FileSystemID)
	return fsInfo, nil
}

// DeleteFileSystemForNamespace deletes EFS filesystem if it's the last PVC in namespace
func (m *namespaceFileSystemManager) DeleteFileSystemForNamespace(
	ctx context.Context,
	namespace string,
	volumeID string,
) error {
	if namespace == "" {
		return NewEFSNSError(ErrInvalidParameter, "DeleteFileSystemForNamespace", namespace, "namespace cannot be empty", nil)
	}

	if volumeID == "" {
		return NewEFSNSError(ErrInvalidParameter, "DeleteFileSystemForNamespace", namespace, "volume ID cannot be empty", nil)
	}

	klog.V(4).Infof("DeleteFileSystemForNamespace: namespace=%s, volumeID=%s", namespace, volumeID)

	// Parse volume ID to get filesystem ID
	_, err := ParseEFSNSVolumeID(volumeID)
	if err != nil {
		return NewEFSNSError(ErrInvalidVolumeID, "DeleteFileSystemForNamespace", namespace,
			"invalid volume ID format", err)
	}

	// Check if there are other PVCs using this filesystem
	pvcCount, err := m.tracker.GetPVCCount(ctx, namespace)
	if err != nil {
		return NewEFSNSError(ErrKubernetesAPIFailed, "DeleteFileSystemForNamespace", namespace,
			"failed to get PVC count", err)
	}

	if pvcCount > 0 {
		klog.V(4).Infof("Filesystem still in use: namespace=%s, pvcCount=%d", namespace, pvcCount)
		return NewEFSNSError(ErrInvalidParameter, "DeleteFileSystemForNamespace", namespace,
			fmt.Sprintf("cannot delete filesystem: %d active PVC(s) still using it", pvcCount), nil)
	}

	// Get filesystem info
	fsInfo, err := m.GetFileSystemInfo(ctx, namespace)
	if err != nil {
		// If filesystem doesn't exist, consider deletion successful
		if IsEFSNSError(err, ErrFileSystemNotFound) {
			klog.V(4).Infof("Filesystem not found, considering deletion successful: namespace=%s", namespace)
			m.cache.Delete(namespace)
			return nil
		}
		return NewEFSNSError(ErrAWSAPIFailed, "DeleteFileSystemForNamespace", namespace,
			"failed to get filesystem info", err)
	}

	if !fsInfo.CanBeDeleted() {
		return NewEFSNSError(ErrResourceNotReady, "DeleteFileSystemForNamespace", namespace,
			fmt.Sprintf("filesystem cannot be deleted in current state: %s", fsInfo.State), nil)
	}

	klog.V(2).Infof("Deleting filesystem: namespace=%s, fileSystemId=%s", namespace, fsInfo.FileSystemID)

	// Delete mount targets first
	err = m.deleteMountTargets(ctx, fsInfo.FileSystemID)
	if err != nil {
		return NewEFSNSError(ErrFileSystemDeletionFailed, "DeleteFileSystemForNamespace", namespace,
			"failed to delete mount targets", err)
	}

	// Delete the filesystem
	err = m.deleteFileSystem(ctx, fsInfo.FileSystemID)
	if err != nil {
		return NewEFSNSError(ErrFileSystemDeletionFailed, "DeleteFileSystemForNamespace", namespace,
			"failed to delete filesystem", err)
	}

	// Remove from cache
	m.cache.Delete(namespace)

	klog.V(2).Infof("Successfully deleted filesystem: namespace=%s, fileSystemId=%s", namespace, fsInfo.FileSystemID)
	return nil
}

// GetFileSystemInfo retrieves filesystem info for a namespace
func (m *namespaceFileSystemManager) GetFileSystemInfo(
	ctx context.Context,
	namespace string,
) (*FileSystemInfo, error) {
	if namespace == "" {
		return nil, NewEFSNSError(ErrInvalidParameter, "GetFileSystemInfo", namespace, "namespace cannot be empty", nil)
	}

	klog.V(4).Infof("GetFileSystemInfo: namespace=%s", namespace)

	// Check cache first
	if fsInfo, found := m.cache.Get(namespace); found {
		klog.V(4).Infof("Found filesystem in cache: namespace=%s, fileSystemId=%s", namespace, fsInfo.FileSystemID)
		return fsInfo, nil
	}

	// Query AWS EFS API
	fsInfo, err := m.findExistingFileSystem(ctx, namespace)
	if err != nil {
		return nil, NewEFSNSError(ErrAWSAPIFailed, "GetFileSystemInfo", namespace,
			"failed to query filesystem", err)
	}

	if fsInfo == nil {
		return nil, NewEFSNSError(ErrFileSystemNotFound, "GetFileSystemInfo", namespace,
			"filesystem not found", nil)
	}

	// Update cache
	m.cache.Set(namespace, fsInfo)

	return fsInfo, nil
}

// ListFileSystemsForNamespace returns all filesystems for given namespace
func (m *namespaceFileSystemManager) ListFileSystemsForNamespace(
	ctx context.Context,
	namespace string,
) ([]*FileSystemInfo, error) {
	if namespace == "" {
		return nil, NewEFSNSError(ErrInvalidParameter, "ListFileSystemsForNamespace", namespace, "namespace cannot be empty", nil)
	}

	klog.V(4).Infof("ListFileSystemsForNamespace: namespace=%s", namespace)

	// For efs-ns mode, there should only be one filesystem per namespace
	fsInfo, err := m.GetFileSystemInfo(ctx, namespace)
	if err != nil {
		if IsEFSNSError(err, ErrFileSystemNotFound) {
			return []*FileSystemInfo{}, nil // Return empty list if no filesystem found
		}
		return nil, err
	}

	return []*FileSystemInfo{fsInfo}, nil
}

// SyncFromAWS synchronizes state with AWS EFS API
func (m *namespaceFileSystemManager) SyncFromAWS(ctx context.Context) error {
	klog.V(4).Infof("SyncFromAWS: syncing filesystem state with AWS")

	// Get all cached filesystems
	cached := m.cache.List()

	for namespace, fsInfo := range cached {
		// Query AWS for current state
		updatedInfo, err := m.getFileSystemFromAWS(ctx, fsInfo.FileSystemID)
		if err != nil {
			klog.Errorf("Failed to sync filesystem from AWS: namespace=%s, fileSystemId=%s, error=%v",
				namespace, fsInfo.FileSystemID, err)
			continue
		}

		if updatedInfo == nil {
			// Filesystem no longer exists in AWS, remove from cache
			m.cache.Delete(namespace)
			klog.V(4).Infof("Removed deleted filesystem from cache: namespace=%s, fileSystemId=%s",
				namespace, fsInfo.FileSystemID)
			continue
		}

		// Update cache with latest info
		m.cache.Set(namespace, updatedInfo)
		klog.V(4).Infof("Updated filesystem in cache: namespace=%s, fileSystemId=%s, state=%s",
			namespace, updatedInfo.FileSystemID, updatedInfo.State)
	}

	return nil
}

// findExistingFileSystem searches for an existing filesystem for the namespace
func (m *namespaceFileSystemManager) findExistingFileSystem(
	ctx context.Context,
	namespace string,
) (*FileSystemInfo, error) {
	// Build expected filesystem name
	expectedName := fmt.Sprintf(FileSystemNameFormat, namespace, m.clusterID)

	// Query EFS API for filesystems with the expected name
	input := &efs.DescribeFileSystemsInput{}

	resp, err := m.efsClient.DescribeFileSystems(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to describe filesystems: %w", err)
	}

	// Look for filesystem with matching name and tags
	for _, fs := range resp.FileSystems {
		if fs.Name != nil && *fs.Name == expectedName {
			// Verify this is managed by efs-ns provisioner
			if m.isEFSNSManagedFileSystem(fs.Tags) {
				fsInfo, err := m.convertAWSFileSystemToInfo(ctx, &fs, namespace)
				if err != nil {
					klog.Warningf("Failed to convert filesystem info: %v", err)
					continue
				}
				return fsInfo, nil
			}
		}
	}

	return nil, nil // Not found
}

// getFileSystemFromAWS retrieves filesystem information from AWS by ID
func (m *namespaceFileSystemManager) getFileSystemFromAWS(
	ctx context.Context,
	fileSystemID string,
) (*FileSystemInfo, error) {
	input := &efs.DescribeFileSystemsInput{
		FileSystemId: aws.String(fileSystemID),
	}

	resp, err := m.efsClient.DescribeFileSystems(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to describe filesystem %s: %w", fileSystemID, err)
	}

	if len(resp.FileSystems) == 0 {
		return nil, nil // Filesystem not found
	}

	fs := resp.FileSystems[0]

	// Extract namespace from tags
	namespace := ""
	for _, tag := range fs.Tags {
		if tag.Key != nil && *tag.Key == TagKeyNamespace {
			if tag.Value != nil {
				namespace = *tag.Value
			}
			break
		}
	}

	if namespace == "" {
		return nil, fmt.Errorf("filesystem %s missing namespace tag", fileSystemID)
	}

	return m.convertAWSFileSystemToInfo(ctx, &fs, namespace)
}

// convertAWSFileSystemToInfo converts AWS EFS filesystem to FileSystemInfo
func (m *namespaceFileSystemManager) convertAWSFileSystemToInfo(
	ctx context.Context,
	fs *efstypes.FileSystemDescription,
	namespace string,
) (*FileSystemInfo, error) {
	if fs.FileSystemId == nil {
		return nil, fmt.Errorf("filesystem ID is nil")
	}

	// Get mount targets
	mountTargets, err := m.getMountTargets(ctx, *fs.FileSystemId)
	if err != nil {
		klog.Warningf("Failed to get mount targets for filesystem %s: %v", *fs.FileSystemId, err)
	}

	// Get PVC count from tracker
	pvcCount, err := m.tracker.GetPVCCount(ctx, namespace)
	if err != nil {
		klog.Warningf("Failed to get PVC count for namespace %s: %v", namespace, err)
		pvcCount = 0
	}

	// Convert tags
	tags := make(map[string]string)
	for _, tag := range fs.Tags {
		if tag.Key != nil && tag.Value != nil {
			tags[*tag.Key] = *tag.Value
		}
	}

	// Extract security group ID from mount targets
	securityGroupID := ""
	// Note: SecurityGroupID would need to be retrieved separately via NetworkInterface
	// For now, we'll leave it empty and update via SyncFromAWS if needed

	fsInfo := &FileSystemInfo{
		FileSystemID:    *fs.FileSystemId,
		Namespace:       namespace,
		ClusterID:       m.clusterID,
		CreatedAt:       aws.ToTime(fs.CreationTime),
		MountTargets:    convertMountTargets(mountTargets),
		SecurityGroupID: securityGroupID,
		State:           convertFileSystemState(fs.LifeCycleState),
		PVCCount:        pvcCount,
		Tags:            tags,
		PerformanceMode: string(fs.PerformanceMode),
		ThroughputMode:  string(fs.ThroughputMode),
		Encrypted:       aws.ToBool(fs.Encrypted),
	}

	return fsInfo, nil
}

// getMountTargets retrieves mount targets for a filesystem
func (m *namespaceFileSystemManager) getMountTargets(
	ctx context.Context,
	fileSystemID string,
) ([]efstypes.MountTargetDescription, error) {
	input := &efs.DescribeMountTargetsInput{
		FileSystemId: aws.String(fileSystemID),
	}

	resp, err := m.efsClient.DescribeMountTargets(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to describe mount targets: %w", err)
	}

	return resp.MountTargets, nil
}

// createFileSystem creates a new EFS filesystem
func (m *namespaceFileSystemManager) createFileSystem(
	ctx context.Context,
	namespace string,
	options *FileSystemOptions,
) (*FileSystemInfo, error) {
	// Generate filesystem name
	fsName := fmt.Sprintf(FileSystemNameFormat, namespace, m.clusterID)

	// Prepare creation parameters
	input := &efs.CreateFileSystemInput{
		CreationToken: aws.String(fmt.Sprintf("%s-%d", fsName, time.Now().UnixNano())),
	}

	// Set performance mode
	if options.PerformanceMode != "" {
		input.PerformanceMode = efstypes.PerformanceMode(options.PerformanceMode)
	} else {
		input.PerformanceMode = efstypes.PerformanceMode(DefaultPerformanceMode)
	}

	// Set throughput mode
	if options.ThroughputMode != "" {
		input.ThroughputMode = efstypes.ThroughputMode(options.ThroughputMode)
	} else {
		input.ThroughputMode = efstypes.ThroughputMode(DefaultThroughputMode)
	}

	// Set provisioned throughput if specified
	if options.ThroughputMode == "provisioned" && options.ProvisionedThroughputInMibps != nil {
		throughput := float64(*options.ProvisionedThroughputInMibps)
		input.ProvisionedThroughputInMibps = &throughput
	}

	// Set encryption
	encrypted := DefaultEncrypted
	if options.Encrypted != nil {
		encrypted = *options.Encrypted
	}
	input.Encrypted = aws.Bool(encrypted)

	// Set KMS key if specified
	if encrypted && options.KmsKeyID != nil && *options.KmsKeyID != "" {
		input.KmsKeyId = options.KmsKeyID
	}

	klog.V(4).Infof("Creating EFS filesystem: name=%s, performanceMode=%s, throughputMode=%s, encrypted=%t",
		fsName, input.PerformanceMode, input.ThroughputMode, *input.Encrypted)

	// Create the filesystem
	resp, err := m.efsClient.CreateFileSystem(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to create filesystem: %w", err)
	}

	if resp.FileSystemId == nil {
		return nil, fmt.Errorf("create filesystem response missing filesystem ID")
	}

	fileSystemID := *resp.FileSystemId
	klog.V(4).Infof("Created filesystem: %s", fileSystemID)

	// Wait for filesystem to become available
	err = m.waitForFileSystemAvailable(ctx, fileSystemID)
	if err != nil {
		// Attempt cleanup on failure
		_ = m.deleteFileSystem(ctx, fileSystemID)
		return nil, fmt.Errorf("filesystem creation failed: %w", err)
	}

	// Add tags
	err = m.tagFileSystem(ctx, fileSystemID, namespace, options)
	if err != nil {
		klog.Warningf("Failed to tag filesystem %s: %v", fileSystemID, err)
	}

	// Set filesystem name
	err = m.setFileSystemName(ctx, fileSystemID, fsName)
	if err != nil {
		klog.Warningf("Failed to set filesystem name %s: %v", fileSystemID, err)
	}

	// Create mount targets
	err = m.createMountTargets(ctx, fileSystemID, namespace)
	if err != nil {
		// Attempt cleanup on failure
		_ = m.deleteFileSystem(ctx, fileSystemID)
		return nil, fmt.Errorf("failed to create mount targets: %w", err)
	}

	// Build and return filesystem info
	fsInfo := &FileSystemInfo{
		FileSystemID:    fileSystemID,
		Namespace:       namespace,
		ClusterID:       m.clusterID,
		CreatedAt:       time.Now(),
		SecurityGroupID: "", // Will be updated by mount target creation
		State:           FileSystemStateAvailable,
		PVCCount:        0,
		PerformanceMode: string(input.PerformanceMode),
		ThroughputMode:  string(input.ThroughputMode),
		Encrypted:       *input.Encrypted,
		Tags:            m.buildTags(namespace, options),
	}

	return fsInfo, nil
}

// waitForFileSystemAvailable waits for filesystem to become available
func (m *namespaceFileSystemManager) waitForFileSystemAvailable(
	ctx context.Context,
	fileSystemID string,
) error {
	klog.V(4).Infof("Waiting for filesystem to become available: %s", fileSystemID)

	timeout := time.After(MaxWaitTime)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return fmt.Errorf("timeout waiting for filesystem %s to become available", fileSystemID)
		case <-ticker.C:
			input := &efs.DescribeFileSystemsInput{
				FileSystemId: aws.String(fileSystemID),
			}

			resp, err := m.efsClient.DescribeFileSystems(ctx, input)
			if err != nil {
				return fmt.Errorf("failed to describe filesystem %s: %w", fileSystemID, err)
			}

			if len(resp.FileSystems) == 0 {
				return fmt.Errorf("filesystem %s not found", fileSystemID)
			}

			fs := resp.FileSystems[0]
			klog.V(4).Infof("Filesystem %s state: %s", fileSystemID, fs.LifeCycleState)

			if fs.LifeCycleState == efstypes.LifeCycleStateAvailable {
				return nil
			}

			if fs.LifeCycleState == efstypes.LifeCycleStateError {
				return fmt.Errorf("filesystem %s entered error state", fileSystemID)
			}
		}
	}
}

// tagFileSystem adds required tags to the filesystem
func (m *namespaceFileSystemManager) tagFileSystem(
	ctx context.Context,
	fileSystemID string,
	namespace string,
	options *FileSystemOptions,
) error {
	tags := m.buildTags(namespace, options)

	var efsTagsArray []efstypes.Tag
	for key, value := range tags {
		efsTagsArray = append(efsTagsArray, efstypes.Tag{
			Key:   aws.String(key),
			Value: aws.String(value),
		})
	}

	input := &efs.CreateTagsInput{
		FileSystemId: aws.String(fileSystemID),
		Tags:         efsTagsArray,
	}

	_, err := m.efsClient.CreateTags(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to create tags: %w", err)
	}

	return nil
}

// setFileSystemName sets the Name tag for the filesystem (used for display)
func (m *namespaceFileSystemManager) setFileSystemName(
	ctx context.Context,
	fileSystemID string,
	name string,
) error {
	input := &efs.CreateTagsInput{
		FileSystemId: aws.String(fileSystemID),
		Tags: []efstypes.Tag{
			{
				Key:   aws.String("Name"),
				Value: aws.String(name),
			},
		},
	}

	_, err := m.efsClient.CreateTags(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to set filesystem name: %w", err)
	}

	return nil
}

// buildTags builds the standard set of tags for EFS filesystems
func (m *namespaceFileSystemManager) buildTags(
	namespace string,
	options *FileSystemOptions,
) map[string]string {
	tags := map[string]string{
		TagKeyNamespace:   namespace,
		TagKeyCluster:     m.clusterID,
		TagKeyManagedBy:   TagValueManagedBy,
		TagKeyCreatedBy:   TagValueCreatedBy,
		TagKeyProvisioner: TagValueProvisioner,
		TagKeyComponent:   TagValueComponent,
	}

	// Add user-provided tags
	if options.Tags != nil {
		for key, value := range options.Tags {
			// Don't allow overriding system tags
			if !strings.HasPrefix(key, "kubernetes.io/") {
				tags[key] = value
			}
		}
	}

	return tags
}

// createMountTargets creates mount targets for the filesystem
func (m *namespaceFileSystemManager) createMountTargets(
	ctx context.Context,
	fileSystemID string,
	namespace string,
) error {
	klog.V(4).Infof("Creating mount targets for filesystem: %s", fileSystemID)

	// Get VPC subnets
	subnets, err := m.getVPCSubnets(ctx)
	if err != nil {
		return fmt.Errorf("failed to get VPC subnets: %w", err)
	}

	if len(subnets) == 0 {
		return fmt.Errorf("no subnets found in VPC %s", m.vpcID)
	}

	// Create security group for this filesystem
	securityGroupID, err := m.createSecurityGroup(ctx, fileSystemID, namespace)
	if err != nil {
		return fmt.Errorf("failed to create security group: %w", err)
	}

	// Create mount targets in parallel for better performance
	errChan := make(chan error, len(subnets))
	for _, subnet := range subnets {
		go func(subnetID string) {
			err := m.createMountTarget(ctx, fileSystemID, subnetID, securityGroupID)
			errChan <- err
		}(*subnet.SubnetId)
	}

	// Collect results
	var errors []string
	for i := 0; i < len(subnets); i++ {
		if err := <-errChan; err != nil {
			errors = append(errors, err.Error())
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to create mount targets: %s", strings.Join(errors, "; "))
	}

	// Wait for mount targets to become available
	err = m.waitForMountTargetsAvailable(ctx, fileSystemID)
	if err != nil {
		return fmt.Errorf("failed waiting for mount targets to become available: %w", err)
	}

	klog.V(4).Infof("Successfully created %d mount targets for filesystem: %s", len(subnets), fileSystemID)
	return nil
}

// createMountTarget creates a single mount target
func (m *namespaceFileSystemManager) createMountTarget(
	ctx context.Context,
	fileSystemID string,
	subnetID string,
	securityGroupID string,
) error {
	input := &efs.CreateMountTargetInput{
		FileSystemId:   aws.String(fileSystemID),
		SubnetId:       aws.String(subnetID),
		SecurityGroups: []string{securityGroupID},
	}

	_, err := m.efsClient.CreateMountTarget(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to create mount target in subnet %s: %w", subnetID, err)
	}

	return nil
}

// deleteFileSystem deletes the EFS filesystem
func (m *namespaceFileSystemManager) deleteFileSystem(
	ctx context.Context,
	fileSystemID string,
) error {
	klog.V(4).Infof("Deleting filesystem: %s", fileSystemID)

	input := &efs.DeleteFileSystemInput{
		FileSystemId: aws.String(fileSystemID),
	}

	_, err := m.efsClient.DeleteFileSystem(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to delete filesystem %s: %w", fileSystemID, err)
	}

	return nil
}

// deleteMountTargets deletes all mount targets for a filesystem
func (m *namespaceFileSystemManager) deleteMountTargets(
	ctx context.Context,
	fileSystemID string,
) error {
	klog.V(4).Infof("Deleting mount targets for filesystem: %s", fileSystemID)

	// Get existing mount targets
	mountTargets, err := m.getMountTargets(ctx, fileSystemID)
	if err != nil {
		return fmt.Errorf("failed to get mount targets: %w", err)
	}

	// Delete mount targets in parallel
	errChan := make(chan error, len(mountTargets))
	for _, mt := range mountTargets {
		go func(mountTargetID string) {
			err := m.deleteMountTarget(ctx, mountTargetID)
			errChan <- err
		}(*mt.MountTargetId)
	}

	// Collect results
	var errors []string
	for i := 0; i < len(mountTargets); i++ {
		if err := <-errChan; err != nil {
			errors = append(errors, err.Error())
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to delete mount targets: %s", strings.Join(errors, "; "))
	}

	// Wait for mount targets to be deleted
	err = m.waitForMountTargetsDeleted(ctx, fileSystemID)
	if err != nil {
		return fmt.Errorf("failed waiting for mount targets to be deleted: %w", err)
	}

	return nil
}

// deleteMountTarget deletes a single mount target
func (m *namespaceFileSystemManager) deleteMountTarget(
	ctx context.Context,
	mountTargetID string,
) error {
	input := &efs.DeleteMountTargetInput{
		MountTargetId: aws.String(mountTargetID),
	}

	_, err := m.efsClient.DeleteMountTarget(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to delete mount target %s: %w", mountTargetID, err)
	}

	return nil
}

// waitForMountTargetsDeleted waits for all mount targets to be deleted
func (m *namespaceFileSystemManager) waitForMountTargetsDeleted(
	ctx context.Context,
	fileSystemID string,
) error {
	klog.V(4).Infof("Waiting for mount targets to be deleted: %s", fileSystemID)

	// Check immediately first
	mountTargets, err := m.getMountTargets(ctx, fileSystemID)
	if err != nil {
		return fmt.Errorf("failed to check mount targets: %w", err)
	}
	if len(mountTargets) == 0 {
		klog.V(4).Infof("All mount targets are already deleted: %s", fileSystemID)
		return nil // All mount targets deleted
	}

	timeout := time.After(MaxWaitTime)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return fmt.Errorf("timeout waiting for mount targets to be deleted")
		case <-ticker.C:
			mountTargets, err := m.getMountTargets(ctx, fileSystemID)
			if err != nil {
				return fmt.Errorf("failed to check mount targets: %w", err)
			}

			if len(mountTargets) == 0 {
				return nil // All mount targets deleted
			}

			klog.V(4).Infof("Still waiting for %d mount targets to be deleted", len(mountTargets))
		}
	}
}

// waitForMountTargetsAvailable waits for all mount targets to become available
func (m *namespaceFileSystemManager) waitForMountTargetsAvailable(
	ctx context.Context,
	fileSystemID string,
) error {
	klog.V(4).Infof("Waiting for mount targets to become available: %s", fileSystemID)

	// Check immediately first
	mountTargets, err := m.getMountTargets(ctx, fileSystemID)
	if err != nil {
		return fmt.Errorf("failed to check mount targets: %w", err)
	}

	if len(mountTargets) > 0 {
		allAvailable := true
		for _, mt := range mountTargets {
			status := string(mt.LifeCycleState)
			if status == "error" {
				return fmt.Errorf("mount target %s entered error state", *mt.MountTargetId)
			}
			if status != "available" {
				allAvailable = false
			}
		}
		if allAvailable {
			klog.V(4).Infof("All mount targets are available: %s", fileSystemID)
			return nil
		}
	}

	timeout := time.After(MaxWaitTime)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return fmt.Errorf("timeout waiting for mount targets to become available")
		case <-ticker.C:
			mountTargets, err := m.getMountTargets(ctx, fileSystemID)
			if err != nil {
				return fmt.Errorf("failed to check mount targets: %w", err)
			}

			if len(mountTargets) == 0 {
				klog.V(4).Infof("No mount targets found for filesystem: %s", fileSystemID)
				continue
			}

			allAvailable := true
			var statuses []string
			for _, mt := range mountTargets {
				status := string(mt.LifeCycleState)
				statuses = append(statuses, status)

				if status == "error" {
					return fmt.Errorf("mount target %s entered error state", *mt.MountTargetId)
				}

				if status != "available" {
					allAvailable = false
				}
			}

			klog.V(4).Infof("Mount target states for filesystem %s: %v", fileSystemID, statuses)

			if allAvailable {
				klog.V(4).Infof("All mount targets available for filesystem: %s", fileSystemID)
				return nil
			}

			klog.V(4).Infof("Still waiting for mount targets to become available")
		}
	}
}

// Helper functions

// isEFSNSManagedFileSystem checks if filesystem is managed by efs-ns provisioner
func (m *namespaceFileSystemManager) isEFSNSManagedFileSystem(tags []efstypes.Tag) bool {
	for _, tag := range tags {
		if tag.Key != nil && tag.Value != nil {
			if *tag.Key == TagKeyManagedBy && *tag.Value == TagValueManagedBy {
				return true
			}
			if *tag.Key == TagKeyCreatedBy && *tag.Value == TagValueCreatedBy {
				return true
			}
		}
	}
	return false
}

// convertMountTargets converts AWS mount targets to our format
func convertMountTargets(awsMountTargets []efstypes.MountTargetDescription) []*MountTargetInfo {
	var mountTargets []*MountTargetInfo

	for _, mt := range awsMountTargets {
		mountTarget := &MountTargetInfo{
			MountTargetID: aws.ToString(mt.MountTargetId),
			SubnetID:      aws.ToString(mt.SubnetId),
			IPAddress:     aws.ToString(mt.IpAddress),
			State:         string(mt.LifeCycleState),
		}
		mountTargets = append(mountTargets, mountTarget)
	}

	return mountTargets
}

// convertFileSystemState converts AWS lifecycle state to our format
func convertFileSystemState(awsState efstypes.LifeCycleState) FileSystemState {
	switch awsState {
	case efstypes.LifeCycleStateCreating:
		return FileSystemStateCreating
	case efstypes.LifeCycleStateAvailable:
		return FileSystemStateAvailable
	case efstypes.LifeCycleStateDeleting:
		return FileSystemStateDeleting
	case efstypes.LifeCycleStateDeleted:
		return FileSystemStateDeleted
	default:
		return FileSystemStateError
	}
}

// getVPCSubnets retrieves all subnets in the configured VPC
func (m *namespaceFileSystemManager) getVPCSubnets(ctx context.Context) ([]ec2types.Subnet, error) {
	input := &ec2.DescribeSubnetsInput{
		Filters: []ec2types.Filter{
			{
				Name:   aws.String("vpc-id"),
				Values: []string{m.vpcID},
			},
		},
	}

	resp, err := m.ec2Client.DescribeSubnets(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to describe VPC subnets: %w", err)
	}

	return resp.Subnets, nil
}

// createSecurityGroup creates a security group for the EFS filesystem
func (m *namespaceFileSystemManager) createSecurityGroup(
	ctx context.Context,
	fileSystemID string,
	namespace string,
) (string, error) {
	// Security group name and description
	sgName := fmt.Sprintf("efs-ns-%s-%s", namespace, m.clusterID)
	sgDescription := fmt.Sprintf("EFS security group for namespace %s in cluster %s", namespace, m.clusterID)

	klog.V(4).Infof("Creating security group: %s", sgName)

	// Create security group
	createInput := &ec2.CreateSecurityGroupInput{
		GroupName:   aws.String(sgName),
		Description: aws.String(sgDescription),
		VpcId:       aws.String(m.vpcID),
		TagSpecifications: []ec2types.TagSpecification{
			{
				ResourceType: ec2types.ResourceTypeSecurityGroup,
				Tags: []ec2types.Tag{
					{
						Key:   aws.String("Name"),
						Value: aws.String(sgName),
					},
					{
						Key:   aws.String(TagKeyNamespace),
						Value: aws.String(namespace),
					},
					{
						Key:   aws.String(TagKeyCluster),
						Value: aws.String(m.clusterID),
					},
					{
						Key:   aws.String(TagKeyManagedBy),
						Value: aws.String(TagValueManagedBy),
					},
					{
						Key:   aws.String("efs-filesystem-id"),
						Value: aws.String(fileSystemID),
					},
				},
			},
		},
	}

	createResp, err := m.ec2Client.CreateSecurityGroup(ctx, createInput)
	if err != nil {
		return "", fmt.Errorf("failed to create security group: %w", err)
	}

	securityGroupID := *createResp.GroupId
	klog.V(4).Infof("Created security group: %s (ID: %s)", sgName, securityGroupID)

	// Add ingress rule for NFS traffic (port 2049)
	err = m.addNFSIngressRule(ctx, securityGroupID)
	if err != nil {
		// Attempt to clean up the security group on failure
		_ = m.deleteSecurityGroup(ctx, securityGroupID)
		return "", fmt.Errorf("failed to add NFS ingress rule: %w", err)
	}

	return securityGroupID, nil
}

// addNFSIngressRule adds NFS ingress rule to security group
func (m *namespaceFileSystemManager) addNFSIngressRule(
	ctx context.Context,
	securityGroupID string,
) error {
	// Allow NFS traffic (port 2049) from the VPC CIDR
	vpcCidr, err := m.getVPCCidr(ctx)
	if err != nil {
		return fmt.Errorf("failed to get VPC CIDR: %w", err)
	}

	input := &ec2.AuthorizeSecurityGroupIngressInput{
		GroupId: aws.String(securityGroupID),
		IpPermissions: []ec2types.IpPermission{
			{
				IpProtocol: aws.String("tcp"),
				FromPort:   aws.Int32(2049),
				ToPort:     aws.Int32(2049),
				IpRanges: []ec2types.IpRange{
					{
						CidrIp:      aws.String(vpcCidr),
						Description: aws.String("NFS traffic from VPC"),
					},
				},
			},
		},
	}

	_, err = m.ec2Client.AuthorizeSecurityGroupIngress(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to authorize security group ingress: %w", err)
	}

	return nil
}

// getVPCCidr retrieves the CIDR block for the VPC
func (m *namespaceFileSystemManager) getVPCCidr(ctx context.Context) (string, error) {
	// Use the existing VPC subnets to get subnet CIDR blocks
	subnets, err := m.getVPCSubnets(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get VPC subnets: %w", err)
	}

	if len(subnets) == 0 {
		return "", fmt.Errorf("no subnets found in VPC %s", m.vpcID)
	}

	// For simplicity, we'll use the first subnet's CIDR and allow the entire /16 or /8 range
	// This is a reasonable approach for most VPCs
	firstSubnetCidr := *subnets[0].CidrBlock

	// Extract the network portion (assume /16 for private VPCs)
	if strings.Contains(firstSubnetCidr, "10.") {
		return "10.0.0.0/8", nil
	} else if strings.Contains(firstSubnetCidr, "172.") {
		return "172.16.0.0/12", nil
	} else if strings.Contains(firstSubnetCidr, "192.168.") {
		return "192.168.0.0/16", nil
	}

	// Fallback to the first subnet's CIDR
	return firstSubnetCidr, nil
}

// deleteSecurityGroup deletes a security group
func (m *namespaceFileSystemManager) deleteSecurityGroup(
	ctx context.Context,
	securityGroupID string,
) error {
	klog.V(4).Infof("Deleting security group: %s", securityGroupID)

	input := &ec2.DeleteSecurityGroupInput{
		GroupId: aws.String(securityGroupID),
	}

	_, err := m.ec2Client.DeleteSecurityGroup(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to delete security group %s: %w", securityGroupID, err)
	}

	return nil
}
