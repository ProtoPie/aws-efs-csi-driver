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
	"errors"
	"fmt"
)

// EFSNSErrorType defines the type of efs-ns errors
type EFSNSErrorType string

const (
	// Filesystem operation errors
	ErrFileSystemCreationFailed EFSNSErrorType = "FileSystemCreationFailed"
	ErrFileSystemDeletionFailed EFSNSErrorType = "FileSystemDeletionFailed"
	ErrFileSystemNotFound       EFSNSErrorType = "FileSystemNotFound"
	ErrFileSystemAlreadyExists  EFSNSErrorType = "FileSystemAlreadyExists"

	// Security group operation errors
	ErrSecurityGroupCreationFailed EFSNSErrorType = "SecurityGroupCreationFailed"
	ErrSecurityGroupDeletionFailed EFSNSErrorType = "SecurityGroupDeletionFailed"
	ErrSecurityGroupNotFound       EFSNSErrorType = "SecurityGroupNotFound"

	// Mount target operation errors
	ErrMountTargetCreationFailed EFSNSErrorType = "MountTargetCreationFailed"
	ErrMountTargetDeletionFailed EFSNSErrorType = "MountTargetDeletionFailed"
	ErrMountTargetNotFound       EFSNSErrorType = "MountTargetNotFound"

	// Cache operation errors
	ErrCacheOperationFailed EFSNSErrorType = "CacheOperationFailed"
	ErrCacheMiss            EFSNSErrorType = "CacheMiss"
	ErrCacheInvalidation    EFSNSErrorType = "CacheInvalidation"

	// Tracker operation errors
	ErrTrackerOperationFailed EFSNSErrorType = "TrackerOperationFailed"
	ErrPVCNotFound            EFSNSErrorType = "PVCNotFound"
	ErrPVCAlreadyExists       EFSNSErrorType = "PVCAlreadyExists"

	// Validation errors
	ErrNamespaceValidationFailed EFSNSErrorType = "NamespaceValidationFailed"
	ErrInvalidVolumeID           EFSNSErrorType = "InvalidVolumeID"
	ErrInvalidParameter          EFSNSErrorType = "InvalidParameter"

	// Kubernetes API errors
	ErrKubernetesAPIFailed      EFSNSErrorType = "KubernetesAPIFailed"
	ErrFinalizerOperationFailed EFSNSErrorType = "FinalizerOperationFailed"

	// AWS API errors
	ErrAWSAPIFailed             EFSNSErrorType = "AWSAPIFailed"
	ErrAWSPermissionDenied      EFSNSErrorType = "AWSPermissionDenied"
	ErrAWSThrottled             EFSNSErrorType = "AWSThrottled"
	ErrAWSResourceLimitExceeded EFSNSErrorType = "AWSResourceLimitExceeded"

	// Metrics operation errors
	ErrMetricsRegistration EFSNSErrorType = "MetricsRegistration"
	ErrMetricsCollection   EFSNSErrorType = "MetricsCollection"

	// Namespace controller errors
	ErrNamespaceControllerFailed EFSNSErrorType = "NamespaceControllerFailed"

	// General errors
	ErrTimeout             EFSNSErrorType = "Timeout"
	ErrConcurrentOperation EFSNSErrorType = "ConcurrentOperation"
	ErrResourceNotReady    EFSNSErrorType = "ResourceNotReady"
	ErrInternalError       EFSNSErrorType = "InternalError"
)

// EFSNSError represents a structured error for efs-ns operations
type EFSNSError struct {
	Type      EFSNSErrorType
	Operation string
	Namespace string
	Message   string
	Cause     error
}

// Error implements the error interface
func (e *EFSNSError) Error() string {
	if e.Namespace != "" {
		if e.Cause != nil {
			return fmt.Sprintf("efs-ns %s operation '%s' failed for namespace '%s': %s (caused by: %v)",
				e.Type, e.Operation, e.Namespace, e.Message, e.Cause)
		}
		return fmt.Sprintf("efs-ns %s operation '%s' failed for namespace '%s': %s",
			e.Type, e.Operation, e.Namespace, e.Message)
	}

	if e.Cause != nil {
		return fmt.Sprintf("efs-ns %s operation '%s' failed: %s (caused by: %v)",
			e.Type, e.Operation, e.Message, e.Cause)
	}
	return fmt.Sprintf("efs-ns %s operation '%s' failed: %s",
		e.Type, e.Operation, e.Message)
}

// Unwrap returns the underlying cause error
func (e *EFSNSError) Unwrap() error {
	return e.Cause
}

// Is checks if the error is of a specific type
func (e *EFSNSError) Is(target error) bool {
	if targetErr, ok := target.(*EFSNSError); ok {
		return e.Type == targetErr.Type
	}
	return false
}

// NewEFSNSError creates a new EFSNSError
func NewEFSNSError(errType EFSNSErrorType, operation, namespace, message string, cause error) *EFSNSError {
	return &EFSNSError{
		Type:      errType,
		Operation: operation,
		Namespace: namespace,
		Message:   message,
		Cause:     cause,
	}
}

// Predefined errors for common cases
var (
	ErrNotFound         = errors.New("resource was not found")
	ErrAlreadyExists    = errors.New("resource already exists")
	ErrAccessDenied     = errors.New("access denied")
	ErrInvalidInput     = errors.New("invalid input")
	ErrOperationTimeout = errors.New("operation timed out")
)

// IsFileSystemNotFound checks if the error indicates a filesystem was not found
func IsFileSystemNotFound(err error) bool {
	var efsnsErr *EFSNSError
	return errors.As(err, &efsnsErr) && efsnsErr.Type == ErrFileSystemNotFound
}

// GetEFSNSErrorType extracts the error type from an EFSNSError
func GetEFSNSErrorType(err error) EFSNSErrorType {
	var efsnsErr *EFSNSError
	if errors.As(err, &efsnsErr) {
		return efsnsErr.Type
	}
	return ""
}

// IsFileSystemCreationFailed checks if the error indicates filesystem creation failed
func IsFileSystemCreationFailed(err error) bool {
	var efsnsErr *EFSNSError
	return errors.As(err, &efsnsErr) && efsnsErr.Type == ErrFileSystemCreationFailed
}

// IsFileSystemDeletionFailed checks if the error indicates filesystem deletion failed
func IsFileSystemDeletionFailed(err error) bool {
	var efsnsErr *EFSNSError
	return errors.As(err, &efsnsErr) && efsnsErr.Type == ErrFileSystemDeletionFailed
}

// IsInvalidVolumeID checks if the error indicates an invalid volume ID
func IsInvalidVolumeID(err error) bool {
	var efsnsErr *EFSNSError
	return errors.As(err, &efsnsErr) && efsnsErr.Type == ErrInvalidVolumeID
}

// IsInvalidParameter checks if the error indicates an invalid parameter
func IsInvalidParameter(err error) bool {
	var efsnsErr *EFSNSError
	return errors.As(err, &efsnsErr) && efsnsErr.Type == ErrInvalidParameter
}

// IsCacheOperationFailed checks if the error indicates a cache operation failed
func IsCacheOperationFailed(err error) bool {
	var efsnsErr *EFSNSError
	return errors.As(err, &efsnsErr) && efsnsErr.Type == ErrCacheOperationFailed
}

// IsTrackerOperationFailed checks if the error indicates a tracker operation failed
func IsTrackerOperationFailed(err error) bool {
	var efsnsErr *EFSNSError
	return errors.As(err, &efsnsErr) && efsnsErr.Type == ErrTrackerOperationFailed
}

// IsAWSAPIFailed checks if the error indicates an AWS API failure
func IsAWSAPIFailed(err error) bool {
	var efsnsErr *EFSNSError
	return errors.As(err, &efsnsErr) && efsnsErr.Type == ErrAWSAPIFailed
}

// IsKubernetesAPIFailed checks if the error indicates a Kubernetes API failure
func IsKubernetesAPIFailed(err error) bool {
	var efsnsErr *EFSNSError
	return errors.As(err, &efsnsErr) && efsnsErr.Type == ErrKubernetesAPIFailed
}

// IsRetryable checks if the error is retryable
func IsRetryable(err error) bool {
	var efsnsErr *EFSNSError
	if !errors.As(err, &efsnsErr) {
		return false
	}

	switch efsnsErr.Type {
	case ErrAWSThrottled, ErrTimeout, ErrResourceNotReady, ErrConcurrentOperation:
		return true
	case ErrAWSAPIFailed, ErrKubernetesAPIFailed, ErrCacheOperationFailed, ErrTrackerOperationFailed:
		return true
	default:
		return false
	}
}

// IsTemporary checks if the error is temporary
func IsTemporary(err error) bool {
	var efsnsErr *EFSNSError
	if !errors.As(err, &efsnsErr) {
		return false
	}

	switch efsnsErr.Type {
	case ErrTimeout, ErrResourceNotReady, ErrConcurrentOperation, ErrAWSThrottled:
		return true
	default:
		return false
	}
}

// IsEFSNSError checks if the error is an EFSNSError of a specific type
func IsEFSNSError(err error, errorType EFSNSErrorType) bool {
	var efsnsErr *EFSNSError
	return errors.As(err, &efsnsErr) && efsnsErr.Type == errorType
}
