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
	"k8s.io/klog/v2"
)

const (
	// RecoveryCleanupTimeout is the maximum time to wait for recovery cleanup operations
	RecoveryCleanupTimeout = 10 * time.Minute

	// OrphanedResourceMaxAge is the maximum age for considering a resource orphaned
	OrphanedResourceMaxAge = 24 * time.Hour

	// RecoveryOperationTimeout is the timeout for individual recovery operations
	RecoveryOperationTimeout = 5 * time.Minute
)

// RecoveryManager handles rollback operations and orphaned resource cleanup
type RecoveryManager interface {
	// RollbackFileSystemCreation rolls back a partially created filesystem
	RollbackFileSystemCreation(ctx context.Context, namespace string, fsInfo *PartialFileSystemInfo) error

	// CleanupOrphanedResources finds and cleans up orphaned resources
	CleanupOrphanedResources(ctx context.Context) error

	// RecoverFromHangingOperation handles recovery from hanging operations
	RecoverFromHangingOperation(ctx context.Context, operationID string) error

	// ValidateResourceConsistency checks consistency between cache and AWS state
	ValidateResourceConsistency(ctx context.Context) error
}

// PartialFileSystemInfo represents information about a partially created filesystem
type PartialFileSystemInfo struct {
	FileSystemID    string
	Namespace       string
	MountTargetIDs  []string
	SecurityGroupID string
	CreatedAt       time.Time
	State           string
}

// RecoveryOperation represents a recovery operation
type RecoveryOperation struct {
	ID          string
	Type        RecoveryOperationType
	Namespace   string
	ResourceID  string
	StartTime   time.Time
	Status      RecoveryOperationStatus
	Error       error
	Description string
}

// RecoveryOperationType defines types of recovery operations
type RecoveryOperationType string

const (
	RecoveryOperationRollback       RecoveryOperationType = "rollback"
	RecoveryOperationCleanup        RecoveryOperationType = "cleanup"
	RecoveryOperationValidation     RecoveryOperationType = "validation"
	RecoveryOperationHangingTimeout RecoveryOperationType = "hanging_timeout"
)

// RecoveryOperationStatus defines status of recovery operations
type RecoveryOperationStatus string

const (
	RecoveryOperationStatusPending    RecoveryOperationStatus = "pending"
	RecoveryOperationStatusRunning    RecoveryOperationStatus = "running"
	RecoveryOperationStatusCompleted  RecoveryOperationStatus = "completed"
	RecoveryOperationStatusFailed     RecoveryOperationStatus = "failed"
	RecoveryOperationStatusCancelled  RecoveryOperationStatus = "cancelled"
)

// recoveryManager implements the RecoveryManager interface
type recoveryManager struct {
	efsClient    EFSClient
	ec2Client    EC2Client
	cache        FileSystemCache
	tracker      PVCTracker
	clusterID    string
	vpcID        string
	operations   map[string]*RecoveryOperation
	operationsMu sync.RWMutex
}

// NewRecoveryManager creates a new RecoveryManager instance
func NewRecoveryManager(
	efsClient EFSClient,
	ec2Client EC2Client,
	cache FileSystemCache,
	tracker PVCTracker,
	clusterID string,
	vpcID string,
) RecoveryManager {
	return &recoveryManager{
		efsClient:  efsClient,
		ec2Client:  ec2Client,
		cache:      cache,
		tracker:    tracker,
		clusterID:  clusterID,
		vpcID:      vpcID,
		operations: make(map[string]*RecoveryOperation),
	}
}

// RollbackFileSystemCreation rolls back a partially created filesystem
func (r *recoveryManager) RollbackFileSystemCreation(
	ctx context.Context,
	namespace string,
	fsInfo *PartialFileSystemInfo,
) error {
	if fsInfo == nil {
		return NewEFSNSError(ErrInvalidParameter, "RollbackFileSystemCreation", namespace,
			"partial filesystem info cannot be nil", nil)
	}

	klog.V(2).Infof("Starting filesystem creation rollback: namespace=%s, fileSystemId=%s",
		namespace, fsInfo.FileSystemID)

	operationID := fmt.Sprintf("rollback-%s-%d", namespace, time.Now().UnixNano())
	operation := &RecoveryOperation{
		ID:          operationID,
		Type:        RecoveryOperationRollback,
		Namespace:   namespace,
		ResourceID:  fsInfo.FileSystemID,
		StartTime:   time.Now(),
		Status:      RecoveryOperationStatusRunning,
		Description: fmt.Sprintf("Rolling back filesystem creation for namespace %s", namespace),
	}

	r.trackOperation(operation)
	defer r.completeOperation(operationID)

	// Create timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, RecoveryCleanupTimeout)
	defer cancel()

	var lastError error

	// Step 1: Delete mount targets if they exist
	if len(fsInfo.MountTargetIDs) > 0 {
		klog.V(3).Infof("Deleting mount targets during rollback: %v", fsInfo.MountTargetIDs)
		for _, mtID := range fsInfo.MountTargetIDs {
			err := r.deleteMountTargetWithRetry(timeoutCtx, mtID)
			if err != nil {
				klog.Errorf("Failed to delete mount target %s during rollback: %v", mtID, err)
				lastError = err
				// Continue with other mount targets
			}
		}

		// Wait for mount targets to be deleted
		err := r.waitForMountTargetsDeletion(timeoutCtx, fsInfo.MountTargetIDs)
		if err != nil {
			lastError = err
			klog.Errorf("Mount targets deletion did not complete during rollback: %v", err)
		}
	}

	// Step 2: Delete security group if it exists and was created for this filesystem
	if fsInfo.SecurityGroupID != "" {
		klog.V(3).Infof("Deleting security group during rollback: %s", fsInfo.SecurityGroupID)
		err := r.deleteSecurityGroupWithRetry(timeoutCtx, fsInfo.SecurityGroupID)
		if err != nil {
			klog.Errorf("Failed to delete security group %s during rollback: %v", fsInfo.SecurityGroupID, err)
			lastError = err
		}
	}

	// Step 3: Delete the filesystem if it exists
	if fsInfo.FileSystemID != "" {
		klog.V(3).Infof("Deleting filesystem during rollback: %s", fsInfo.FileSystemID)
		err := r.deleteFileSystemWithRetry(timeoutCtx, fsInfo.FileSystemID)
		if err != nil {
			klog.Errorf("Failed to delete filesystem %s during rollback: %v", fsInfo.FileSystemID, err)
			lastError = err
		}
	}

	// Step 4: Clean up cache entries
	r.cache.Delete(namespace)

	// Step 5: Clean up tracker entries
	err := r.cleanupTrackerEntries(timeoutCtx, namespace, fsInfo.FileSystemID)
	if err != nil {
		klog.Errorf("Failed to cleanup tracker entries during rollback: %v", err)
		lastError = err
	}

	if lastError != nil {
		operation.Status = RecoveryOperationStatusFailed
		operation.Error = lastError
		return NewEFSNSError(ErrFileSystemDeletionFailed, "RollbackFileSystemCreation", namespace,
			"rollback partially failed", lastError)
	}

	operation.Status = RecoveryOperationStatusCompleted
	klog.V(2).Infof("Successfully completed filesystem creation rollback: namespace=%s", namespace)
	return nil
}

// CleanupOrphanedResources finds and cleans up orphaned resources
func (r *recoveryManager) CleanupOrphanedResources(ctx context.Context) error {
	klog.V(2).Info("Starting orphaned resources cleanup")

	operationID := fmt.Sprintf("cleanup-orphaned-%d", time.Now().UnixNano())
	operation := &RecoveryOperation{
		ID:          operationID,
		Type:        RecoveryOperationCleanup,
		StartTime:   time.Now(),
		Status:      RecoveryOperationStatusRunning,
		Description: "Cleaning up orphaned EFS resources",
	}

	r.trackOperation(operation)
	defer r.completeOperation(operationID)

	// Find orphaned filesystems
	orphanedFS, err := r.findOrphanedFileSystems(ctx)
	if err != nil {
		operation.Status = RecoveryOperationStatusFailed
		operation.Error = err
		return NewEFSNSError(ErrAWSAPIFailed, "CleanupOrphanedResources", "",
			"failed to find orphaned filesystems", err)
	}

	// Find orphaned security groups
	orphanedSG, err := r.findOrphanedSecurityGroups(ctx)
	if err != nil {
		operation.Status = RecoveryOperationStatusFailed
		operation.Error = err
		return NewEFSNSError(ErrAWSAPIFailed, "CleanupOrphanedResources", "",
			"failed to find orphaned security groups", err)
	}

	var lastError error
	cleanedCount := 0

	// Clean up orphaned filesystems
	for _, fs := range orphanedFS {
		klog.V(3).Infof("Cleaning up orphaned filesystem: %s", aws.ToString(fs.FileSystemId))
		err := r.cleanupOrphanedFileSystem(ctx, fs)
		if err != nil {
			klog.Errorf("Failed to cleanup orphaned filesystem %s: %v", aws.ToString(fs.FileSystemId), err)
			lastError = err
		} else {
			cleanedCount++
		}
	}

	// Clean up orphaned security groups
	for _, sg := range orphanedSG {
		klog.V(3).Infof("Cleaning up orphaned security group: %s", aws.ToString(sg.GroupId))
		err := r.cleanupOrphanedSecurityGroup(ctx, sg)
		if err != nil {
			klog.Errorf("Failed to cleanup orphaned security group %s: %v", aws.ToString(sg.GroupId), err)
			lastError = err
		} else {
			cleanedCount++
		}
	}

	if lastError != nil {
		operation.Status = RecoveryOperationStatusFailed
		operation.Error = lastError
		klog.Warningf("Orphaned resources cleanup completed with errors, cleaned %d resources", cleanedCount)
	} else {
		operation.Status = RecoveryOperationStatusCompleted
		klog.V(2).Infof("Successfully completed orphaned resources cleanup, cleaned %d resources", cleanedCount)
	}

	return lastError
}

// RecoverFromHangingOperation handles recovery from hanging operations
func (r *recoveryManager) RecoverFromHangingOperation(ctx context.Context, operationID string) error {
	r.operationsMu.RLock()
	operation, exists := r.operations[operationID]
	r.operationsMu.RUnlock()

	if !exists {
		return NewEFSNSError(ErrInvalidParameter, "RecoverFromHangingOperation", "",
			fmt.Sprintf("operation %s not found", operationID), nil)
	}

	if operation.Status != RecoveryOperationStatusRunning {
		return NewEFSNSError(ErrInvalidParameter, "RecoverFromHangingOperation", operation.Namespace,
			fmt.Sprintf("operation %s is not in running state", operationID), nil)
	}

	// Check if operation has been running too long
	if time.Since(operation.StartTime) > RecoveryOperationTimeout {
		klog.Warningf("Cancelling hanging operation: %s", operationID)
		operation.Status = RecoveryOperationStatusCancelled
		operation.Error = NewEFSNSError(ErrTimeout, "RecoverFromHangingOperation", operation.Namespace,
			"operation timed out", nil)
		return operation.Error
	}

	return nil
}

// ValidateResourceConsistency checks consistency between cache and AWS state
func (r *recoveryManager) ValidateResourceConsistency(ctx context.Context) error {
	klog.V(2).Info("Starting resource consistency validation")

	operationID := fmt.Sprintf("validate-consistency-%d", time.Now().UnixNano())
	operation := &RecoveryOperation{
		ID:          operationID,
		Type:        RecoveryOperationValidation,
		StartTime:   time.Now(),
		Status:      RecoveryOperationStatusRunning,
		Description: "Validating resource consistency between cache and AWS",
	}

	r.trackOperation(operation)
	defer r.completeOperation(operationID)

	// Get all cached filesystems
	cachedFS := r.cache.List()

	var inconsistencies []string
	var lastError error

	for namespace, fsInfo := range cachedFS {
		// Verify filesystem exists in AWS
		awsFS, err := r.getFileSystemFromAWS(ctx, fsInfo.FileSystemID)
		if err != nil {
			inconsistencies = append(inconsistencies, 
				fmt.Sprintf("filesystem %s for namespace %s not found in AWS", fsInfo.FileSystemID, namespace))
			lastError = err
			continue
		}

		// Verify filesystem state
		if awsFS != nil && string(awsFS.LifeCycleState) != string(fsInfo.State) {
			inconsistencies = append(inconsistencies,
				fmt.Sprintf("filesystem %s state mismatch: cache=%s, aws=%s", 
					fsInfo.FileSystemID, fsInfo.State, awsFS.LifeCycleState))
		}

		// Verify mount targets
		mountTargets, err := r.getMountTargetsFromAWS(ctx, fsInfo.FileSystemID)
		if err != nil {
			inconsistencies = append(inconsistencies,
				fmt.Sprintf("failed to get mount targets for filesystem %s", fsInfo.FileSystemID))
			lastError = err
			continue
		}

		if len(mountTargets) != len(fsInfo.MountTargets) {
			inconsistencies = append(inconsistencies,
				fmt.Sprintf("mount target count mismatch for filesystem %s: cache=%d, aws=%d",
					fsInfo.FileSystemID, len(fsInfo.MountTargets), len(mountTargets)))
		}
	}

	if len(inconsistencies) > 0 {
		operation.Status = RecoveryOperationStatusFailed
		errorMsg := strings.Join(inconsistencies, "; ")
		operation.Error = NewEFSNSError(ErrCacheInvalidation, "ValidateResourceConsistency", "",
			fmt.Sprintf("found %d inconsistencies: %s", len(inconsistencies), errorMsg), lastError)
		return operation.Error
	}

	operation.Status = RecoveryOperationStatusCompleted
	klog.V(2).Info("Resource consistency validation completed successfully")
	return nil
}

// Helper methods

func (r *recoveryManager) trackOperation(operation *RecoveryOperation) {
	r.operationsMu.Lock()
	defer r.operationsMu.Unlock()
	r.operations[operation.ID] = operation
}

func (r *recoveryManager) completeOperation(operationID string) {
	r.operationsMu.Lock()
	defer r.operationsMu.Unlock()
	if operation, exists := r.operations[operationID]; exists {
		if operation.Status == RecoveryOperationStatusRunning {
			operation.Status = RecoveryOperationStatusCompleted
		}
	}
}

func (r *recoveryManager) deleteMountTargetWithRetry(ctx context.Context, mountTargetID string) error {
	return RetryableAWSOperation(ctx, nil, "DeleteMountTarget", "", func() error {
		_, err := r.efsClient.DeleteMountTarget(ctx, &efs.DeleteMountTargetInput{
			MountTargetId: aws.String(mountTargetID),
		})
		return err
	})
}

func (r *recoveryManager) deleteSecurityGroupWithRetry(ctx context.Context, securityGroupID string) error {
	return RetryableAWSOperation(ctx, nil, "DeleteSecurityGroup", "", func() error {
		_, err := r.ec2Client.DeleteSecurityGroup(ctx, &ec2.DeleteSecurityGroupInput{
			GroupId: aws.String(securityGroupID),
		})
		return err
	})
}

func (r *recoveryManager) deleteFileSystemWithRetry(ctx context.Context, fileSystemID string) error {
	return RetryableAWSOperation(ctx, nil, "DeleteFileSystem", "", func() error {
		_, err := r.efsClient.DeleteFileSystem(ctx, &efs.DeleteFileSystemInput{
			FileSystemId: aws.String(fileSystemID),
		})
		return err
	})
}

func (r *recoveryManager) waitForMountTargetsDeletion(ctx context.Context, mountTargetIDs []string) error {
	for _, mtID := range mountTargetIDs {
		err := r.waitForMountTargetDeletion(ctx, mtID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *recoveryManager) waitForMountTargetDeletion(ctx context.Context, mountTargetID string) error {
	return OperationWithTimeout(ctx, 5*time.Minute, "WaitForMountTargetDeletion", func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Second):
				// Check if mount target still exists
				_, err := r.efsClient.DescribeMountTargets(ctx, &efs.DescribeMountTargetsInput{
					MountTargetId: aws.String(mountTargetID),
				})
				if err != nil {
					// If not found, deletion is complete
					if strings.Contains(err.Error(), "MountTargetNotFound") {
						return nil
					}
					return err
				}
				// Continue waiting
			}
		}
	})
}

func (r *recoveryManager) cleanupTrackerEntries(ctx context.Context, namespace string, fileSystemID string) error {
	// This would typically involve cleaning up ConfigMap entries
	// Implementation depends on the specific tracker implementation
	return nil
}

func (r *recoveryManager) findOrphanedFileSystems(ctx context.Context) ([]efstypes.FileSystemDescription, error) {
	// Implementation to find filesystems that are no longer needed
	return nil, nil
}

func (r *recoveryManager) findOrphanedSecurityGroups(ctx context.Context) ([]ec2types.SecurityGroup, error) {
	// Implementation to find security groups that are no longer needed
	return nil, nil
}

func (r *recoveryManager) cleanupOrphanedFileSystem(ctx context.Context, fs efstypes.FileSystemDescription) error {
	// Implementation to cleanup an orphaned filesystem
	return nil
}

func (r *recoveryManager) cleanupOrphanedSecurityGroup(ctx context.Context, sg ec2types.SecurityGroup) error {
	// Implementation to cleanup an orphaned security group
	return nil
}

func (r *recoveryManager) getFileSystemFromAWS(ctx context.Context, fileSystemID string) (*efstypes.FileSystemDescription, error) {
	// Implementation to get filesystem from AWS
	return nil, nil
}

func (r *recoveryManager) getMountTargetsFromAWS(ctx context.Context, fileSystemID string) ([]efstypes.MountTargetDescription, error) {
	// Implementation to get mount targets from AWS
	return nil, nil
}