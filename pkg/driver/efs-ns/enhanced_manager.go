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
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// enhancedNamespaceFileSystemManager implements enhanced NamespaceFileSystemManager with comprehensive error handling
type enhancedNamespaceFileSystemManager struct {
	efsClient        EFSClient
	ec2Client        EC2Client
	k8sClient        kubernetes.Interface
	cache            FileSystemCache
	tracker          PVCTracker
	recoveryManager  RecoveryManager
	circuitBreaker   *CircuitBreaker
	clusterID        string
	vpcID            string
	mutex            sync.RWMutex
	operationTimeout time.Duration
}

// NewEnhancedNamespaceFileSystemManager creates a new enhanced NamespaceFileSystemManager
func NewEnhancedNamespaceFileSystemManager(
	efsClient EFSClient,
	ec2Client EC2Client,
	k8sClient kubernetes.Interface,
	cache FileSystemCache,
	tracker PVCTracker,
	clusterID string,
	vpcID string,
) NamespaceFileSystemManager {
	recoveryManager := NewRecoveryManager(efsClient, ec2Client, cache, tracker, clusterID, vpcID)
	circuitBreaker := NewCircuitBreaker(CircuitBreakerFailureThreshold, CircuitBreakerRecoveryTimeout)

	return &enhancedNamespaceFileSystemManager{
		efsClient:        efsClient,
		ec2Client:        ec2Client,
		k8sClient:        k8sClient,
		cache:            cache,
		tracker:          tracker,
		recoveryManager:  recoveryManager,
		circuitBreaker:   circuitBreaker,
		clusterID:        clusterID,
		vpcID:            vpcID,
		operationTimeout: 10 * time.Minute,
	}
}

// CreateOrGetFileSystemForNamespace creates a new EFS filesystem with comprehensive error handling
func (m *enhancedNamespaceFileSystemManager) CreateOrGetFileSystemForNamespace(
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

	// Wrap the entire operation with timeout
	return m.withOperationTimeout(ctx, "CreateOrGetFileSystemForNamespace", func(ctx context.Context) (*FileSystemInfo, error) {
		return m.createOrGetFileSystemInternal(ctx, namespace, options)
	})
}

// createOrGetFileSystemInternal handles the actual filesystem creation with retry and rollback logic
func (m *enhancedNamespaceFileSystemManager) createOrGetFileSystemInternal(
	ctx context.Context,
	namespace string,
	options *FileSystemOptions,
) (*FileSystemInfo, error) {
	// Check cache first
	if fsInfo, found := m.cache.Get(namespace); found {
		klog.V(4).Infof("Found filesystem in cache: namespace=%s, fileSystemId=%s", namespace, fsInfo.FileSystemID)
		
		// Validate cached filesystem still exists in AWS
		if err := m.validateCachedFileSystem(ctx, fsInfo); err != nil {
			klog.Warningf("Cached filesystem validation failed, clearing cache: %v", err)
			m.cache.Delete(namespace)
		} else {
			return fsInfo, nil
		}
	}

	// Use mutex to prevent concurrent creation for the same namespace
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Double-check cache after acquiring lock
	if fsInfo, found := m.cache.Get(namespace); found {
		klog.V(4).Infof("Found filesystem in cache after lock: namespace=%s, fileSystemId=%s", namespace, fsInfo.FileSystemID)
		return fsInfo, nil
	}

	// Check if filesystem already exists in AWS with retry
	var existingFS *FileSystemInfo
	err := RetryableAWSOperation(ctx, m.circuitBreaker, "FindExistingFileSystem", namespace, func() error {
		var err error
		existingFS, err = m.findExistingFileSystem(ctx, namespace)
		return err
	})
	if err != nil {
		return nil, NewEFSNSError(ErrAWSAPIFailed, "CreateOrGetFileSystemForNamespace", namespace,
			"failed to check for existing filesystem", err)
	}

	if existingFS != nil {
		klog.V(4).Infof("Found existing filesystem: namespace=%s, fileSystemId=%s", namespace, existingFS.FileSystemID)
		m.cache.Set(namespace, existingFS)
		return existingFS, nil
	}

	// Create new filesystem with comprehensive error handling
	klog.V(2).Infof("Creating new EFS filesystem for namespace: %s", namespace)
	fsInfo, err := m.createFileSystemWithRecovery(ctx, namespace, options)
	if err != nil {
		return nil, NewEFSNSError(ErrFileSystemCreationFailed, "CreateOrGetFileSystemForNamespace", namespace,
			"failed to create filesystem", err)
	}

	// Cache the new filesystem info
	m.cache.Set(namespace, fsInfo)

	klog.V(2).Infof("Successfully created filesystem: namespace=%s, fileSystemId=%s", namespace, fsInfo.FileSystemID)
	return fsInfo, nil
}

// createFileSystemWithRecovery creates a filesystem with automatic rollback on failure
func (m *enhancedNamespaceFileSystemManager) createFileSystemWithRecovery(
	ctx context.Context,
	namespace string,
	options *FileSystemOptions,
) (*FileSystemInfo, error) {
	partialInfo := &PartialFileSystemInfo{
		Namespace: namespace,
		CreatedAt: time.Now(),
	}

	// Step 1: Create the EFS filesystem
	var filesystem *efstypes.FileSystemDescription
	err := RetryableAWSOperation(ctx, m.circuitBreaker, "CreateFileSystem", namespace, func() error {
		var err error
		filesystem, err = m.createEFSFileSystem(ctx, namespace, options)
		if err != nil {
			return err
		}
		partialInfo.FileSystemID = aws.ToString(filesystem.FileSystemId)
		partialInfo.State = string(filesystem.LifeCycleState)
		return nil
	})
	if err != nil {
		// No cleanup needed if filesystem creation failed
		return nil, fmt.Errorf("filesystem creation failed: %w", err)
	}

	// Step 2: Wait for filesystem to become available
	err = m.waitForFileSystemAvailable(ctx, partialInfo.FileSystemID)
	if err != nil {
		// Rollback: delete the filesystem
		klog.Errorf("Filesystem availability wait failed, initiating rollback: %v", err)
		if rollbackErr := m.recoveryManager.RollbackFileSystemCreation(ctx, namespace, partialInfo); rollbackErr != nil {
			klog.Errorf("Rollback failed: %v", rollbackErr)
		}
		return nil, fmt.Errorf("filesystem not available after creation: %w", err)
	}

	// Step 3: Create security group with retry
	var securityGroupID string
	err = RetryableAWSOperation(ctx, m.circuitBreaker, "CreateSecurityGroup", namespace, func() error {
		var err error
		securityGroupID, err = m.createSecurityGroup(ctx, namespace, partialInfo.FileSystemID)
		if err != nil {
			return err
		}
		partialInfo.SecurityGroupID = securityGroupID
		return nil
	})
	if err != nil {
		// Rollback: delete filesystem
		klog.Errorf("Security group creation failed, initiating rollback: %v", err)
		if rollbackErr := m.recoveryManager.RollbackFileSystemCreation(ctx, namespace, partialInfo); rollbackErr != nil {
			klog.Errorf("Rollback failed: %v", rollbackErr)
		}
		return nil, fmt.Errorf("security group creation failed: %w", err)
	}

	// Step 4: Create mount targets with retry
	var mountTargetIDs []string
	err = RetryableAWSOperation(ctx, m.circuitBreaker, "CreateMountTargets", namespace, func() error {
		var err error
		mountTargetIDs, err = m.createMountTargets(ctx, partialInfo.FileSystemID, securityGroupID)
		if err != nil {
			return err
		}
		partialInfo.MountTargetIDs = mountTargetIDs
		return nil
	})
	if err != nil {
		// Rollback: delete security group and filesystem
		klog.Errorf("Mount targets creation failed, initiating rollback: %v", err)
		if rollbackErr := m.recoveryManager.RollbackFileSystemCreation(ctx, namespace, partialInfo); rollbackErr != nil {
			klog.Errorf("Rollback failed: %v", rollbackErr)
		}
		return nil, fmt.Errorf("mount targets creation failed: %w", err)
	}

	// Step 5: Wait for mount targets to become available
	err = m.waitForMountTargetsAvailable(ctx, mountTargetIDs)
	if err != nil {
		// Rollback: delete mount targets, security group, and filesystem
		klog.Errorf("Mount targets availability wait failed, initiating rollback: %v", err)
		if rollbackErr := m.recoveryManager.RollbackFileSystemCreation(ctx, namespace, partialInfo); rollbackErr != nil {
			klog.Errorf("Rollback failed: %v", rollbackErr)
		}
		return nil, fmt.Errorf("mount targets not available after creation: %w", err)
	}

	// Step 6: Apply tags with retry
	err = RetryableAWSOperation(ctx, m.circuitBreaker, "CreateTags", namespace, func() error {
		return m.applyFileSystemTags(ctx, partialInfo.FileSystemID, namespace, options)
	})
	if err != nil {
		// Tags are not critical, log warning but don't fail
		klog.Warningf("Failed to apply tags to filesystem %s: %v", partialInfo.FileSystemID, err)
	}

	// Convert to FileSystemInfo
	fsInfo := &FileSystemInfo{
		FileSystemID:    partialInfo.FileSystemID,
		Namespace:       namespace,
		ClusterID:       m.clusterID,
		CreatedAt:       partialInfo.CreatedAt,
		SecurityGroupID: securityGroupID,
		State:           FileSystemStateAvailable,
		PVCCount:        0,
		Tags:            m.buildFileSystemTags(namespace, options),
	}

	// Get and set mount targets info
	mountTargets, err := m.getMountTargets(ctx, partialInfo.FileSystemID)
	if err != nil {
		klog.Warningf("Failed to get mount targets info: %v", err)
	} else {
		fsInfo.MountTargets = convertMountTargets(mountTargets)
	}

	return fsInfo, nil
}

// DeleteFileSystemForNamespace deletes EFS filesystem with comprehensive error handling
func (m *enhancedNamespaceFileSystemManager) DeleteFileSystemForNamespace(
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

	// Wrap the operation with timeout
	return m.withOperationTimeoutError(ctx, "DeleteFileSystemForNamespace", func(ctx context.Context) error {
		return m.deleteFileSystemInternal(ctx, namespace, volumeID)
	})
}

// deleteFileSystemInternal handles the actual filesystem deletion
func (m *enhancedNamespaceFileSystemManager) deleteFileSystemInternal(
	ctx context.Context,
	namespace string,
	volumeID string,
) error {
	// Parse volume ID to get filesystem ID
	_, err := ParseEFSNSVolumeID(volumeID)
	if err != nil {
		return NewEFSNSError(ErrInvalidVolumeID, "DeleteFileSystemForNamespace", namespace,
			"invalid volume ID format", err)
	}

	// Check if there are other PVCs using this filesystem with retry
	var pvcCount int32
	err = RetryableKubernetesOperation(ctx, "GetPVCCount", namespace, func() error {
		var err error
		pvcCount, err = m.tracker.GetPVCCount(ctx, namespace)
		return err
	})
	if err != nil {
		return NewEFSNSError(ErrKubernetesAPIFailed, "DeleteFileSystemForNamespace", namespace,
			"failed to get PVC count", err)
	}

	if pvcCount > 0 {
		klog.V(4).Infof("Filesystem still in use: namespace=%s, pvcCount=%d", namespace, pvcCount)
		return NewEFSNSError(ErrInvalidParameter, "DeleteFileSystemForNamespace", namespace,
			fmt.Sprintf("cannot delete filesystem: %d active PVC(s) still using it", pvcCount), nil)
	}

	// Get filesystem info with retry
	var fsInfo *FileSystemInfo
	err = RetryableAWSOperation(ctx, m.circuitBreaker, "GetFileSystemInfo", namespace, func() error {
		var err error
		fsInfo, err = m.GetFileSystemInfo(ctx, namespace)
		return err
	})
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

	// Delete mount targets first with retry
	err = RetryableAWSOperation(ctx, m.circuitBreaker, "DeleteMountTargets", namespace, func() error {
		return m.deleteMountTargets(ctx, fsInfo.FileSystemID)
	})
	if err != nil {
		return NewEFSNSError(ErrFileSystemDeletionFailed, "DeleteFileSystemForNamespace", namespace,
			"failed to delete mount targets", err)
	}

	// Delete security group with retry
	if fsInfo.SecurityGroupID != "" {
		err = RetryableAWSOperation(ctx, m.circuitBreaker, "DeleteSecurityGroup", namespace, func() error {
			return m.deleteSecurityGroup(ctx, fsInfo.SecurityGroupID)
		})
		if err != nil {
			klog.Warningf("Failed to delete security group %s: %v", fsInfo.SecurityGroupID, err)
			// Don't fail the entire operation for security group deletion
		}
	}

	// Delete the filesystem with retry
	err = RetryableAWSOperation(ctx, m.circuitBreaker, "DeleteFileSystem", namespace, func() error {
		return m.deleteFileSystem(ctx, fsInfo.FileSystemID)
	})
	if err != nil {
		return NewEFSNSError(ErrFileSystemDeletionFailed, "DeleteFileSystemForNamespace", namespace,
			"failed to delete filesystem", err)
	}

	// Remove from cache
	m.cache.Delete(namespace)

	klog.V(2).Infof("Successfully deleted filesystem: namespace=%s, fileSystemId=%s", namespace, fsInfo.FileSystemID)
	return nil
}

// Helper methods for enhanced error handling

// withOperationTimeout wraps an operation with timeout handling
func (m *enhancedNamespaceFileSystemManager) withOperationTimeout(
	ctx context.Context,
	operation string,
	fn func(context.Context) (*FileSystemInfo, error),
) (*FileSystemInfo, error) {
	return OperationWithTimeoutResult(ctx, m.operationTimeout, operation, fn)
}

// withOperationTimeoutError wraps an error-returning operation with timeout handling
func (m *enhancedNamespaceFileSystemManager) withOperationTimeoutError(
	ctx context.Context,
	operation string,
	fn func(context.Context) error,
) error {
	return OperationWithTimeout(ctx, m.operationTimeout, operation, fn)
}

// validateCachedFileSystem validates that a cached filesystem still exists and is in correct state
func (m *enhancedNamespaceFileSystemManager) validateCachedFileSystem(
	ctx context.Context,
	fsInfo *FileSystemInfo,
) error {
	return RetryableAWSOperation(ctx, m.circuitBreaker, "ValidateCachedFileSystem", fsInfo.Namespace, func() error {
		actualFS, err := m.getFileSystemFromAWS(ctx, fsInfo.FileSystemID)
		if err != nil {
			return err
		}
		if actualFS == nil {
			return NewEFSNSError(ErrFileSystemNotFound, "ValidateCachedFileSystem", fsInfo.Namespace,
				"cached filesystem no longer exists in AWS", nil)
		}
		return nil
	})
}

// waitForFileSystemAvailable waits for filesystem to become available
func (m *enhancedNamespaceFileSystemManager) waitForFileSystemAvailable(
	ctx context.Context,
	fileSystemID string,
) error {
	return OperationWithTimeout(ctx, 5*time.Minute, "WaitForFileSystemAvailable", func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Second):
				fsInfo, err := m.getFileSystemFromAWS(ctx, fileSystemID)
				if err != nil {
					return err
				}
				if fsInfo != nil && fsInfo.State == FileSystemStateAvailable {
					return nil
				}
				// Continue waiting
			}
		}
	})
}

// waitForMountTargetsAvailable waits for all mount targets to become available
func (m *enhancedNamespaceFileSystemManager) waitForMountTargetsAvailable(
	ctx context.Context,
	mountTargetIDs []string,
) error {
	return OperationWithTimeout(ctx, 5*time.Minute, "WaitForMountTargetsAvailable", func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Second):
				allAvailable := true
				for _, mtID := range mountTargetIDs {
					available, err := m.isMountTargetAvailable(ctx, mtID)
					if err != nil {
						return err
					}
					if !available {
						allAvailable = false
						break
					}
				}
				if allAvailable {
					return nil
				}
				// Continue waiting
			}
		}
	})
}

// Placeholder methods that would need full implementation
func (m *enhancedNamespaceFileSystemManager) GetFileSystemInfo(ctx context.Context, namespace string) (*FileSystemInfo, error) {
	// Implementation from original manager
	return nil, nil
}

func (m *enhancedNamespaceFileSystemManager) ListFileSystemsForNamespace(ctx context.Context, namespace string) ([]*FileSystemInfo, error) {
	// Implementation from original manager
	return nil, nil
}

func (m *enhancedNamespaceFileSystemManager) SyncFromAWS(ctx context.Context) error {
	// Implementation from original manager
	return nil
}

// Additional helper functions that would need implementation
func (m *enhancedNamespaceFileSystemManager) findExistingFileSystem(ctx context.Context, namespace string) (*FileSystemInfo, error) {
	return nil, nil
}

func (m *enhancedNamespaceFileSystemManager) createEFSFileSystem(ctx context.Context, namespace string, options *FileSystemOptions) (*efstypes.FileSystemDescription, error) {
	return nil, nil
}

func (m *enhancedNamespaceFileSystemManager) createSecurityGroup(ctx context.Context, namespace, fileSystemID string) (string, error) {
	return "", nil
}

func (m *enhancedNamespaceFileSystemManager) createMountTargets(ctx context.Context, fileSystemID, securityGroupID string) ([]string, error) {
	return nil, nil
}

func (m *enhancedNamespaceFileSystemManager) deleteMountTargets(ctx context.Context, fileSystemID string) error {
	return nil
}

func (m *enhancedNamespaceFileSystemManager) deleteSecurityGroup(ctx context.Context, securityGroupID string) error {
	return nil
}

func (m *enhancedNamespaceFileSystemManager) deleteFileSystem(ctx context.Context, fileSystemID string) error {
	return nil
}

func (m *enhancedNamespaceFileSystemManager) applyFileSystemTags(ctx context.Context, fileSystemID, namespace string, options *FileSystemOptions) error {
	return nil
}

func (m *enhancedNamespaceFileSystemManager) buildFileSystemTags(namespace string, options *FileSystemOptions) map[string]string {
	return make(map[string]string)
}

func (m *enhancedNamespaceFileSystemManager) getMountTargets(ctx context.Context, fileSystemID string) ([]efstypes.MountTargetDescription, error) {
	return nil, nil
}

func (m *enhancedNamespaceFileSystemManager) getFileSystemFromAWS(ctx context.Context, fileSystemID string) (*FileSystemInfo, error) {
	return nil, nil
}

func (m *enhancedNamespaceFileSystemManager) isMountTargetAvailable(ctx context.Context, mountTargetID string) (bool, error) {
	return false, nil
}

// OperationWithTimeoutResult executes a function that returns a result with timeout
func OperationWithTimeoutResult[T any](ctx context.Context, timeout time.Duration, operation string, fn func(context.Context) (T, error)) (T, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	type result struct {
		value T
		err   error
	}

	done := make(chan result, 1)
	go func() {
		value, err := fn(timeoutCtx)
		done <- result{value: value, err: err}
	}()

	select {
	case res := <-done:
		return res.value, res.err
	case <-timeoutCtx.Done():
		var zero T
		return zero, NewEFSNSError(ErrTimeout, operation, "",
			fmt.Sprintf("operation timed out after %v", timeout), timeoutCtx.Err())
	}
}

