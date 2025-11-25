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
	"testing"
)

func TestEFSNSError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *EFSNSError
		expected string
	}{
		{
			name: "error with namespace and cause",
			err: &EFSNSError{
				Type:      ErrFileSystemCreationFailed,
				Operation: "CreateFileSystem",
				Namespace: "default",
				Message:   "filesystem creation failed",
				Cause:     errors.New("AWS API error"),
			},
			expected: "efs-ns FileSystemCreationFailed operation 'CreateFileSystem' failed for namespace 'default': filesystem creation failed (caused by: AWS API error)",
		},
		{
			name: "error with namespace but no cause",
			err: &EFSNSError{
				Type:      ErrFileSystemDeletionFailed,
				Operation: "DeleteFileSystem",
				Namespace: "test-ns",
				Message:   "filesystem deletion failed",
				Cause:     nil,
			},
			expected: "efs-ns FileSystemDeletionFailed operation 'DeleteFileSystem' failed for namespace 'test-ns': filesystem deletion failed",
		},
		{
			name: "error without namespace but with cause",
			err: &EFSNSError{
				Type:      ErrInvalidVolumeID,
				Operation: "ParseVolumeID",
				Namespace: "",
				Message:   "invalid volume ID format",
				Cause:     errors.New("parsing error"),
			},
			expected: "efs-ns InvalidVolumeID operation 'ParseVolumeID' failed: invalid volume ID format (caused by: parsing error)",
		},
		{
			name: "error without namespace and no cause",
			err: &EFSNSError{
				Type:      ErrInvalidParameter,
				Operation: "ValidateOptions",
				Namespace: "",
				Message:   "invalid parameter value",
				Cause:     nil,
			},
			expected: "efs-ns InvalidParameter operation 'ValidateOptions' failed: invalid parameter value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.err.Error()
			if result != tt.expected {
				t.Errorf("EFSNSError.Error() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestEFSNSError_Unwrap(t *testing.T) {
	cause := errors.New("underlying error")
	err := &EFSNSError{
		Type:      ErrFileSystemCreationFailed,
		Operation: "CreateFileSystem",
		Namespace: "default",
		Message:   "creation failed",
		Cause:     cause,
	}

	unwrapped := err.Unwrap()
	if unwrapped != cause {
		t.Errorf("EFSNSError.Unwrap() = %v, want %v", unwrapped, cause)
	}

	// Test with no cause
	errNoCause := &EFSNSError{
		Type:      ErrFileSystemCreationFailed,
		Operation: "CreateFileSystem",
		Namespace: "default",
		Message:   "creation failed",
		Cause:     nil,
	}

	unwrappedNil := errNoCause.Unwrap()
	if unwrappedNil != nil {
		t.Errorf("EFSNSError.Unwrap() = %v, want nil", unwrappedNil)
	}
}

func TestEFSNSError_Is(t *testing.T) {
	err1 := &EFSNSError{Type: ErrFileSystemCreationFailed}
	err2 := &EFSNSError{Type: ErrFileSystemCreationFailed}
	err3 := &EFSNSError{Type: ErrFileSystemDeletionFailed}
	regularErr := errors.New("regular error")

	tests := []struct {
		name     string
		err      *EFSNSError
		target   error
		expected bool
	}{
		{
			name:     "same error type",
			err:      err1,
			target:   err2,
			expected: true,
		},
		{
			name:     "different error type",
			err:      err1,
			target:   err3,
			expected: false,
		},
		{
			name:     "non-EFSNSError target",
			err:      err1,
			target:   regularErr,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.err.Is(tt.target)
			if result != tt.expected {
				t.Errorf("EFSNSError.Is() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestNewEFSNSError(t *testing.T) {
	cause := errors.New("cause error")
	err := NewEFSNSError(ErrFileSystemCreationFailed, "CreateFileSystem", "default", "creation failed", cause)

	if err.Type != ErrFileSystemCreationFailed {
		t.Errorf("NewEFSNSError() Type = %v, want %v", err.Type, ErrFileSystemCreationFailed)
	}
	if err.Operation != "CreateFileSystem" {
		t.Errorf("NewEFSNSError() Operation = %v, want CreateFileSystem", err.Operation)
	}
	if err.Namespace != "default" {
		t.Errorf("NewEFSNSError() Namespace = %v, want default", err.Namespace)
	}
	if err.Message != "creation failed" {
		t.Errorf("NewEFSNSError() Message = %v, want creation failed", err.Message)
	}
	if err.Cause != cause {
		t.Errorf("NewEFSNSError() Cause = %v, want %v", err.Cause, cause)
	}
}

func TestIsFileSystemNotFound(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "FileSystemNotFound error",
			err:      NewEFSNSError(ErrFileSystemNotFound, "GetFileSystem", "default", "not found", nil),
			expected: true,
		},
		{
			name:     "different EFSNSError",
			err:      NewEFSNSError(ErrFileSystemCreationFailed, "CreateFileSystem", "default", "failed", nil),
			expected: false,
		},
		{
			name:     "regular error",
			err:      errors.New("regular error"),
			expected: false,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsFileSystemNotFound(tt.err)
			if result != tt.expected {
				t.Errorf("IsFileSystemNotFound() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIsFileSystemCreationFailed(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "FileSystemCreationFailed error",
			err:      NewEFSNSError(ErrFileSystemCreationFailed, "CreateFileSystem", "default", "failed", nil),
			expected: true,
		},
		{
			name:     "different EFSNSError",
			err:      NewEFSNSError(ErrFileSystemNotFound, "GetFileSystem", "default", "not found", nil),
			expected: false,
		},
		{
			name:     "regular error",
			err:      errors.New("regular error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsFileSystemCreationFailed(tt.err)
			if result != tt.expected {
				t.Errorf("IsFileSystemCreationFailed() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIsFileSystemDeletionFailed(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "FileSystemDeletionFailed error",
			err:      NewEFSNSError(ErrFileSystemDeletionFailed, "DeleteFileSystem", "default", "failed", nil),
			expected: true,
		},
		{
			name:     "different EFSNSError",
			err:      NewEFSNSError(ErrFileSystemCreationFailed, "CreateFileSystem", "default", "failed", nil),
			expected: false,
		},
		{
			name:     "regular error",
			err:      errors.New("regular error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsFileSystemDeletionFailed(tt.err)
			if result != tt.expected {
				t.Errorf("IsFileSystemDeletionFailed() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIsInvalidVolumeID(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "InvalidVolumeID error",
			err:      NewEFSNSError(ErrInvalidVolumeID, "ParseVolumeID", "", "invalid format", nil),
			expected: true,
		},
		{
			name:     "different EFSNSError",
			err:      NewEFSNSError(ErrFileSystemNotFound, "GetFileSystem", "default", "not found", nil),
			expected: false,
		},
		{
			name:     "regular error",
			err:      errors.New("regular error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsInvalidVolumeID(tt.err)
			if result != tt.expected {
				t.Errorf("IsInvalidVolumeID() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIsInvalidParameter(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "InvalidParameter error",
			err:      NewEFSNSError(ErrInvalidParameter, "ValidateOptions", "", "invalid value", nil),
			expected: true,
		},
		{
			name:     "different EFSNSError",
			err:      NewEFSNSError(ErrInvalidVolumeID, "ParseVolumeID", "", "invalid format", nil),
			expected: false,
		},
		{
			name:     "regular error",
			err:      errors.New("regular error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsInvalidParameter(tt.err)
			if result != tt.expected {
				t.Errorf("IsInvalidParameter() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIsCacheOperationFailed(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "CacheOperationFailed error",
			err:      NewEFSNSError(ErrCacheOperationFailed, "CacheSet", "default", "cache failed", nil),
			expected: true,
		},
		{
			name:     "different EFSNSError",
			err:      NewEFSNSError(ErrInvalidParameter, "ValidateOptions", "", "invalid value", nil),
			expected: false,
		},
		{
			name:     "regular error",
			err:      errors.New("regular error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsCacheOperationFailed(tt.err)
			if result != tt.expected {
				t.Errorf("IsCacheOperationFailed() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIsTrackerOperationFailed(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "TrackerOperationFailed error",
			err:      NewEFSNSError(ErrTrackerOperationFailed, "AddPVC", "default", "tracker failed", nil),
			expected: true,
		},
		{
			name:     "different EFSNSError",
			err:      NewEFSNSError(ErrCacheOperationFailed, "CacheSet", "default", "cache failed", nil),
			expected: false,
		},
		{
			name:     "regular error",
			err:      errors.New("regular error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsTrackerOperationFailed(tt.err)
			if result != tt.expected {
				t.Errorf("IsTrackerOperationFailed() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIsAWSAPIFailed(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "AWSAPIFailed error",
			err:      NewEFSNSError(ErrAWSAPIFailed, "CreateFileSystem", "default", "AWS failed", nil),
			expected: true,
		},
		{
			name:     "different EFSNSError",
			err:      NewEFSNSError(ErrKubernetesAPIFailed, "GetPVC", "default", "K8s failed", nil),
			expected: false,
		},
		{
			name:     "regular error",
			err:      errors.New("regular error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsAWSAPIFailed(tt.err)
			if result != tt.expected {
				t.Errorf("IsAWSAPIFailed() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIsKubernetesAPIFailed(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "KubernetesAPIFailed error",
			err:      NewEFSNSError(ErrKubernetesAPIFailed, "GetPVC", "default", "K8s failed", nil),
			expected: true,
		},
		{
			name:     "different EFSNSError",
			err:      NewEFSNSError(ErrAWSAPIFailed, "CreateFileSystem", "default", "AWS failed", nil),
			expected: false,
		},
		{
			name:     "regular error",
			err:      errors.New("regular error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsKubernetesAPIFailed(tt.err)
			if result != tt.expected {
				t.Errorf("IsKubernetesAPIFailed() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "throttled error - retryable",
			err:      NewEFSNSError(ErrAWSThrottled, "CreateFileSystem", "default", "throttled", nil),
			expected: true,
		},
		{
			name:     "timeout error - retryable",
			err:      NewEFSNSError(ErrTimeout, "CreateFileSystem", "default", "timeout", nil),
			expected: true,
		},
		{
			name:     "resource not ready error - retryable",
			err:      NewEFSNSError(ErrResourceNotReady, "CreateFileSystem", "default", "not ready", nil),
			expected: true,
		},
		{
			name:     "concurrent operation error - retryable",
			err:      NewEFSNSError(ErrConcurrentOperation, "CreateFileSystem", "default", "concurrent", nil),
			expected: true,
		},
		{
			name:     "AWS API error - retryable",
			err:      NewEFSNSError(ErrAWSAPIFailed, "CreateFileSystem", "default", "AWS failed", nil),
			expected: true,
		},
		{
			name:     "Kubernetes API error - retryable",
			err:      NewEFSNSError(ErrKubernetesAPIFailed, "GetPVC", "default", "K8s failed", nil),
			expected: true,
		},
		{
			name:     "invalid parameter error - not retryable",
			err:      NewEFSNSError(ErrInvalidParameter, "ValidateOptions", "", "invalid", nil),
			expected: false,
		},
		{
			name:     "filesystem not found error - not retryable",
			err:      NewEFSNSError(ErrFileSystemNotFound, "GetFileSystem", "default", "not found", nil),
			expected: false,
		},
		{
			name:     "regular error - not retryable",
			err:      errors.New("regular error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsRetryable(tt.err)
			if result != tt.expected {
				t.Errorf("IsRetryable() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIsTemporary(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "timeout error - temporary",
			err:      NewEFSNSError(ErrTimeout, "CreateFileSystem", "default", "timeout", nil),
			expected: true,
		},
		{
			name:     "resource not ready error - temporary",
			err:      NewEFSNSError(ErrResourceNotReady, "CreateFileSystem", "default", "not ready", nil),
			expected: true,
		},
		{
			name:     "concurrent operation error - temporary",
			err:      NewEFSNSError(ErrConcurrentOperation, "CreateFileSystem", "default", "concurrent", nil),
			expected: true,
		},
		{
			name:     "throttled error - temporary",
			err:      NewEFSNSError(ErrAWSThrottled, "CreateFileSystem", "default", "throttled", nil),
			expected: true,
		},
		{
			name:     "AWS API error - not temporary",
			err:      NewEFSNSError(ErrAWSAPIFailed, "CreateFileSystem", "default", "AWS failed", nil),
			expected: false,
		},
		{
			name:     "invalid parameter error - not temporary",
			err:      NewEFSNSError(ErrInvalidParameter, "ValidateOptions", "", "invalid", nil),
			expected: false,
		},
		{
			name:     "regular error - not temporary",
			err:      errors.New("regular error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsTemporary(tt.err)
			if result != tt.expected {
				t.Errorf("IsTemporary() = %v, want %v", result, tt.expected)
			}
		})
	}
}
