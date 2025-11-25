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
	"errors"
	"testing"
	"time"
)

func TestNewEFSNSLogger(t *testing.T) {
	tests := []struct {
		name         string
		component    string
		expectedComp string
	}{
		{
			name:         "with component name",
			component:    "test-component",
			expectedComp: "test-component",
		},
		{
			name:         "with empty component name",
			component:    "",
			expectedComp: "efs-ns",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := NewEFSNSLogger(tt.component)
			if logger == nil {
				t.Fatal("NewEFSNSLogger returned nil")
			}
			if logger.component != tt.expectedComp {
				t.Errorf("Expected component %s, got %s", tt.expectedComp, logger.component)
			}
		})
	}
}

func TestEFSNSLoggerFileSystemLifecycle(t *testing.T) {
	logger := NewEFSNSLogger("test-filesystem")

	// Test data
	namespace := "test-namespace"
	clusterID := "test-cluster"
	fileSystemID := "fs-12345"
	duration := 500 * time.Millisecond

	options := &FileSystemOptions{
		PerformanceMode: "generalPurpose",
		ThroughputMode:  "bursting",
		Encrypted:       BoolPtr(true),
	}

	// Test filesystem creation logging
	t.Run("LogFileSystemCreationStarted", func(t *testing.T) {
		// Should not panic
		logger.LogFileSystemCreationStarted(namespace, clusterID, options)
	})

	t.Run("LogFileSystemCreated", func(t *testing.T) {
		// Should not panic
		logger.LogFileSystemCreated(namespace, clusterID, fileSystemID, duration)
	})

	t.Run("LogFileSystemCreationFailed", func(t *testing.T) {
		// Test with EFSNSError
		efsnsErr := NewEFSNSError(ErrFileSystemCreationFailed, "CreateFileSystem", namespace, "test error", nil)
		logger.LogFileSystemCreationFailed(namespace, clusterID, duration, efsnsErr)

		// Test with regular error
		regularErr := errors.New("regular error")
		logger.LogFileSystemCreationFailed(namespace, clusterID, duration, regularErr)
	})

	t.Run("LogFileSystemDeletionStarted", func(t *testing.T) {
		// Should not panic
		logger.LogFileSystemDeletionStarted(namespace, clusterID, fileSystemID)
	})

	t.Run("LogFileSystemDeleted", func(t *testing.T) {
		// Should not panic
		logger.LogFileSystemDeleted(namespace, clusterID, fileSystemID, duration)
	})

	t.Run("LogFileSystemDeletionFailed", func(t *testing.T) {
		// Test with EFSNSError
		efsnsErr := NewEFSNSError(ErrFileSystemDeletionFailed, "DeleteFileSystem", namespace, "test error", nil)
		logger.LogFileSystemDeletionFailed(namespace, clusterID, fileSystemID, duration, efsnsErr)

		// Test with regular error
		regularErr := errors.New("regular error")
		logger.LogFileSystemDeletionFailed(namespace, clusterID, fileSystemID, duration, regularErr)
	})

	t.Run("LogFileSystemStateChanged", func(t *testing.T) {
		// Should not panic
		logger.LogFileSystemStateChanged(namespace, clusterID, fileSystemID, FileSystemStateCreating, FileSystemStateAvailable)
	})
}

func TestEFSNSLoggerMountTargetOperations(t *testing.T) {
	logger := NewEFSNSLogger("test-mount-target")

	// Test data
	namespace := "test-namespace"
	fileSystemID := "fs-12345"
	mountTargetID := "fsmt-67890"
	subnetID := "subnet-abc123"
	subnetIDs := []string{"subnet-abc123", "subnet-def456"}
	duration := 300 * time.Millisecond

	t.Run("LogMountTargetCreationStarted", func(t *testing.T) {
		// Should not panic
		logger.LogMountTargetCreationStarted(namespace, fileSystemID, subnetIDs)
	})

	t.Run("LogMountTargetCreated", func(t *testing.T) {
		// Should not panic
		logger.LogMountTargetCreated(namespace, fileSystemID, mountTargetID, subnetID, duration)
	})

	t.Run("LogMountTargetCreationFailed", func(t *testing.T) {
		// Test with EFSNSError
		efsnsErr := NewEFSNSError(ErrMountTargetCreationFailed, "CreateMountTarget", namespace, "test error", nil)
		logger.LogMountTargetCreationFailed(namespace, fileSystemID, subnetID, duration, efsnsErr)

		// Test with regular error
		regularErr := errors.New("regular error")
		logger.LogMountTargetCreationFailed(namespace, fileSystemID, subnetID, duration, regularErr)
	})

	t.Run("LogMountTargetDeleted", func(t *testing.T) {
		// Should not panic
		logger.LogMountTargetDeleted(namespace, fileSystemID, mountTargetID, duration)
	})
}

func TestEFSNSLoggerCacheOperations(t *testing.T) {
	logger := NewEFSNSLogger("test-cache")

	// Test data
	operation := "filesystem_lookup"
	namespace := "test-namespace"
	duration := 50 * time.Millisecond

	t.Run("LogCacheHit", func(t *testing.T) {
		// Should not panic
		logger.LogCacheHit(operation, namespace, duration)
	})

	t.Run("LogCacheMiss", func(t *testing.T) {
		// Should not panic
		logger.LogCacheMiss(operation, namespace, duration)
	})

	t.Run("LogCacheRefresh", func(t *testing.T) {
		// Test successful refresh
		logger.LogCacheRefresh(namespace, duration, true)

		// Test failed refresh
		logger.LogCacheRefresh(namespace, duration, false)
	})
}

func TestEFSNSLoggerPVCTracking(t *testing.T) {
	logger := NewEFSNSLogger("test-pvc-tracker")

	// Test data
	namespace := "test-namespace"
	pvcName := "test-pvc"
	volumeID := "efs-ns::test-namespace::fs-12345::test-cluster"
	operation := "add_pvc"

	t.Run("LogPVCAdded", func(t *testing.T) {
		// Should not panic
		logger.LogPVCAdded(namespace, pvcName, volumeID)
	})

	t.Run("LogPVCRemoved", func(t *testing.T) {
		// Test when namespace becomes empty
		logger.LogPVCRemoved(namespace, pvcName, true)

		// Test when namespace still has PVCs
		logger.LogPVCRemoved(namespace, pvcName, false)
	})

	t.Run("LogPVCTrackingFailed", func(t *testing.T) {
		// Test with EFSNSError
		efsnsErr := NewEFSNSError(ErrTrackerOperationFailed, operation, namespace, "test error", nil)
		logger.LogPVCTrackingFailed(operation, namespace, pvcName, efsnsErr)

		// Test with regular error
		regularErr := errors.New("regular error")
		logger.LogPVCTrackingFailed(operation, namespace, pvcName, regularErr)
	})
}

func TestEFSNSLoggerSecurityGroupOperations(t *testing.T) {
	logger := NewEFSNSLogger("test-security-group")

	// Test data
	namespace := "test-namespace"
	securityGroupID := "sg-12345"
	vpcID := "vpc-67890"
	duration := 200 * time.Millisecond

	t.Run("LogSecurityGroupCreated", func(t *testing.T) {
		// Should not panic
		logger.LogSecurityGroupCreated(namespace, securityGroupID, vpcID, duration)
	})

	t.Run("LogSecurityGroupDeleted", func(t *testing.T) {
		// Should not panic
		logger.LogSecurityGroupDeleted(namespace, securityGroupID, duration)
	})
}

func TestEFSNSLoggerVolumeOperations(t *testing.T) {
	logger := NewEFSNSLogger("test-volume")

	// Test data
	namespace := "test-namespace"
	volumeID := "efs-ns::test-namespace::fs-12345::test-cluster"
	fileSystemID := "fs-12345"
	operation := "create_volume"
	duration := 1000 * time.Millisecond

	t.Run("LogVolumeCreated", func(t *testing.T) {
		// Should not panic
		logger.LogVolumeCreated(namespace, volumeID, fileSystemID, duration)
	})

	t.Run("LogVolumeDeleted", func(t *testing.T) {
		// Should not panic
		logger.LogVolumeDeleted(namespace, volumeID, duration)
	})

	t.Run("LogVolumeOperationFailed", func(t *testing.T) {
		// Test with EFSNSError
		efsnsErr := NewEFSNSError(ErrFileSystemCreationFailed, operation, namespace, "test error", nil)
		logger.LogVolumeOperationFailed(operation, namespace, volumeID, duration, efsnsErr)

		// Test with regular error
		regularErr := errors.New("regular error")
		logger.LogVolumeOperationFailed(operation, namespace, volumeID, duration, regularErr)
	})
}

func TestEFSNSLoggerAWSAPIOperations(t *testing.T) {
	logger := NewEFSNSLogger("test-aws-api")

	// Test data
	service := "efs"
	operation := "DescribeFileSystems"
	duration := 100 * time.Millisecond
	retryCount := 3
	nextRetryAfter := 5 * time.Second

	t.Run("LogAWSAPICall", func(t *testing.T) {
		// Test successful call
		logger.LogAWSAPICall(service, operation, duration, true)

		// Test failed call
		logger.LogAWSAPICall(service, operation, duration, false)
	})

	t.Run("LogAWSAPIThrottled", func(t *testing.T) {
		// Should not panic
		logger.LogAWSAPIThrottled(service, operation, retryCount, nextRetryAfter)
	})

	t.Run("LogAWSAPIError", func(t *testing.T) {
		err := errors.New("API error")
		// Should not panic
		logger.LogAWSAPIError(service, operation, duration, err)
	})
}

func TestEFSNSLoggerKubernetesAPIOperations(t *testing.T) {
	logger := NewEFSNSLogger("test-k8s-api")

	// Test data
	resource := "persistentvolumeclaims"
	operation := "create"
	duration := 150 * time.Millisecond

	t.Run("LogKubernetesAPICall", func(t *testing.T) {
		// Test successful call
		logger.LogKubernetesAPICall(resource, operation, duration, true)

		// Test failed call
		logger.LogKubernetesAPICall(resource, operation, duration, false)
	})

	t.Run("LogKubernetesAPIError", func(t *testing.T) {
		err := errors.New("K8s API error")
		// Should not panic
		logger.LogKubernetesAPIError(resource, operation, duration, err)
	})
}

func TestEFSNSLoggerFinalizerOperations(t *testing.T) {
	logger := NewEFSNSLogger("test-finalizer")

	// Test data
	resourceType := "PersistentVolumeClaim"
	namespace := "test-namespace"
	name := "test-pvc"
	finalizerName := EFSNSFinalizerName

	t.Run("LogFinalizerAdded", func(t *testing.T) {
		// Should not panic
		logger.LogFinalizerAdded(resourceType, namespace, name, finalizerName)
	})

	t.Run("LogFinalizerRemoved", func(t *testing.T) {
		// Should not panic
		logger.LogFinalizerRemoved(resourceType, namespace, name, finalizerName)
	})
}

func TestEFSNSLoggerMetricsOperations(t *testing.T) {
	logger := NewEFSNSLogger("test-metrics")

	// Test data
	address := "0.0.0.0"
	port := "8080"
	metricName := "efs_ns_filesystem_count"
	value := 5.0
	labels := map[string]string{"namespace": "test", "state": "available"}

	t.Run("LogMetricsServerStarted", func(t *testing.T) {
		// Should not panic
		logger.LogMetricsServerStarted(address, port)
	})

	t.Run("LogMetricsServerStopped", func(t *testing.T) {
		// Should not panic
		logger.LogMetricsServerStopped()
	})

	t.Run("LogMetricsCollection", func(t *testing.T) {
		// Should not panic
		logger.LogMetricsCollection(metricName, value, labels)
	})
}

func TestEFSNSLoggerGeneralOperations(t *testing.T) {
	logger := NewEFSNSLogger("test-general")

	// Test data
	operation := "test_operation"
	namespace := "test-namespace"
	duration := 250 * time.Millisecond

	params := map[string]interface{}{
		"param1": "value1",
		"param2": 42,
	}

	result := map[string]interface{}{
		"result1": "success",
		"result2": 100,
	}

	t.Run("LogOperationStarted", func(t *testing.T) {
		// Test with context containing request ID
		ctx := context.WithValue(context.Background(), "requestId", "req-123")
		logger.LogOperationStarted(ctx, operation, namespace, params)

		// Test with nil context
		logger.LogOperationStarted(nil, operation, namespace, params)

		// Test with context without request ID
		emptyCtx := context.Background()
		logger.LogOperationStarted(emptyCtx, operation, namespace, params)
	})

	t.Run("LogOperationCompleted", func(t *testing.T) {
		// Test with context containing request ID
		ctx := context.WithValue(context.Background(), "requestId", "req-123")
		logger.LogOperationCompleted(ctx, operation, namespace, duration, result)

		// Test with nil context
		logger.LogOperationCompleted(nil, operation, namespace, duration, result)

		// Test with context without request ID
		emptyCtx := context.Background()
		logger.LogOperationCompleted(emptyCtx, operation, namespace, duration, result)
	})

	t.Run("LogDebug", func(t *testing.T) {
		// Should not panic
		logger.LogDebug("Debug message", "key1", "value1", "key2", 42)
	})

	t.Run("LogInfo", func(t *testing.T) {
		// Should not panic
		logger.LogInfo("Info message", "key1", "value1", "key2", 42)
	})

	t.Run("LogWarning", func(t *testing.T) {
		// Should not panic
		logger.LogWarning("Warning message", "key1", "value1", "key2", 42)
	})

	t.Run("LogError", func(t *testing.T) {
		// Test with EFSNSError
		efsnsErr := NewEFSNSError(ErrInternalError, "TestOperation", namespace, "test error", nil)
		logger.LogError(efsnsErr, "Error message", "key1", "value1")

		// Test with regular error
		regularErr := errors.New("regular error")
		logger.LogError(regularErr, "Error message", "key1", "value1")
	})
}

func TestGlobalLoggerInstances(t *testing.T) {
	loggers := map[string]*EFSNSLogger{
		"filesystem-manager": FileSystemManagerLogger,
		"cache":              CacheLogger,
		"tracker":            TrackerLogger,
		"finalizer":          FinalizerLogger,
		"metrics":            MetricsLogger,
		"security-group":     SecurityGroupLogger,
		"mount-target":       MountTargetLogger,
		"controller":         ControllerLogger,
		"node":               NodeLogger,
	}

	for expectedComponent, logger := range loggers {
		t.Run(expectedComponent, func(t *testing.T) {
			if logger == nil {
				t.Fatalf("Logger for %s is nil", expectedComponent)
			}
			if logger.component != expectedComponent {
				t.Errorf("Expected component %s, got %s", expectedComponent, logger.component)
			}
		})
	}
}

// Helper function for creating bool pointers
func BoolPtr(b bool) *bool {
	return &b
}
