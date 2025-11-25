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
	"time"

	"k8s.io/klog/v2"
)

const (
	// Log levels for EFS-NS operations
	LogLevelError   = 1
	LogLevelWarning = 2
	LogLevelInfo    = 3
	LogLevelDebug   = 4
	LogLevelTrace   = 5
)

// EFSNSLogger provides structured logging for EFS-NS operations
type EFSNSLogger struct {
	component string
}

// NewEFSNSLogger creates a new structured logger for a component
func NewEFSNSLogger(component string) *EFSNSLogger {
	if component == "" {
		component = "efs-ns"
	}
	return &EFSNSLogger{
		component: component,
	}
}

// FileSystemLifecycle logs filesystem lifecycle events

// LogFileSystemCreationStarted logs the start of filesystem creation
func (l *EFSNSLogger) LogFileSystemCreationStarted(namespace, clusterID string, options *FileSystemOptions) {
	klog.V(LogLevelInfo).InfoS("EFS filesystem creation started",
		"component", l.component,
		"operation", "filesystem_create",
		"namespace", namespace,
		"clusterId", clusterID,
		"performanceMode", options.PerformanceMode,
		"throughputMode", options.ThroughputMode,
		"encrypted", options.Encrypted,
	)
}

// LogFileSystemCreated logs successful filesystem creation
func (l *EFSNSLogger) LogFileSystemCreated(namespace, clusterID, fileSystemID string, duration time.Duration) {
	klog.V(LogLevelInfo).InfoS("EFS filesystem created successfully",
		"component", l.component,
		"operation", "filesystem_create",
		"namespace", namespace,
		"clusterId", clusterID,
		"fileSystemId", fileSystemID,
		"duration", duration.String(),
		"durationMs", duration.Milliseconds(),
	)
}

// LogFileSystemCreationFailed logs filesystem creation failure
func (l *EFSNSLogger) LogFileSystemCreationFailed(namespace, clusterID string, duration time.Duration, err error) {
	efsnsErr, isEFSNSError := err.(*EFSNSError)
	if isEFSNSError {
		klog.V(LogLevelError).ErrorS(err, "EFS filesystem creation failed",
			"component", l.component,
			"operation", "filesystem_create",
			"namespace", namespace,
			"clusterId", clusterID,
			"duration", duration.String(),
			"durationMs", duration.Milliseconds(),
			"errorType", string(efsnsErr.Type),
			"errorOperation", efsnsErr.Operation,
		)
	} else {
		klog.V(LogLevelError).ErrorS(err, "EFS filesystem creation failed",
			"component", l.component,
			"operation", "filesystem_create",
			"namespace", namespace,
			"clusterId", clusterID,
			"duration", duration.String(),
			"durationMs", duration.Milliseconds(),
		)
	}
}

// LogFileSystemDeletionStarted logs the start of filesystem deletion
func (l *EFSNSLogger) LogFileSystemDeletionStarted(namespace, clusterID, fileSystemID string) {
	klog.V(LogLevelInfo).InfoS("EFS filesystem deletion started",
		"component", l.component,
		"operation", "filesystem_delete",
		"namespace", namespace,
		"clusterId", clusterID,
		"fileSystemId", fileSystemID,
	)
}

// LogFileSystemDeleted logs successful filesystem deletion
func (l *EFSNSLogger) LogFileSystemDeleted(namespace, clusterID, fileSystemID string, duration time.Duration) {
	klog.V(LogLevelInfo).InfoS("EFS filesystem deleted successfully",
		"component", l.component,
		"operation", "filesystem_delete",
		"namespace", namespace,
		"clusterId", clusterID,
		"fileSystemId", fileSystemID,
		"duration", duration.String(),
		"durationMs", duration.Milliseconds(),
	)
}

// LogFileSystemDeletionFailed logs filesystem deletion failure
func (l *EFSNSLogger) LogFileSystemDeletionFailed(namespace, clusterID, fileSystemID string, duration time.Duration, err error) {
	efsnsErr, isEFSNSError := err.(*EFSNSError)
	if isEFSNSError {
		klog.V(LogLevelError).ErrorS(err, "EFS filesystem deletion failed",
			"component", l.component,
			"operation", "filesystem_delete",
			"namespace", namespace,
			"clusterId", clusterID,
			"fileSystemId", fileSystemID,
			"duration", duration.String(),
			"durationMs", duration.Milliseconds(),
			"errorType", string(efsnsErr.Type),
			"errorOperation", efsnsErr.Operation,
		)
	} else {
		klog.V(LogLevelError).ErrorS(err, "EFS filesystem deletion failed",
			"component", l.component,
			"operation", "filesystem_delete",
			"namespace", namespace,
			"clusterId", clusterID,
			"fileSystemId", fileSystemID,
			"duration", duration.String(),
			"durationMs", duration.Milliseconds(),
		)
	}
}

// LogFileSystemStateChanged logs filesystem state transitions
func (l *EFSNSLogger) LogFileSystemStateChanged(namespace, clusterID, fileSystemID string, fromState, toState FileSystemState) {
	klog.V(LogLevelInfo).InfoS("EFS filesystem state changed",
		"component", l.component,
		"operation", "filesystem_state_change",
		"namespace", namespace,
		"clusterId", clusterID,
		"fileSystemId", fileSystemID,
		"fromState", string(fromState),
		"toState", string(toState),
	)
}

// Mount Target Operations

// LogMountTargetCreationStarted logs the start of mount target creation
func (l *EFSNSLogger) LogMountTargetCreationStarted(namespace, fileSystemID string, subnetIDs []string) {
	klog.V(LogLevelInfo).InfoS("Mount target creation started",
		"component", l.component,
		"operation", "mount_target_create",
		"namespace", namespace,
		"fileSystemId", fileSystemID,
		"subnetCount", len(subnetIDs),
		"subnetIds", subnetIDs,
	)
}

// LogMountTargetCreated logs successful mount target creation
func (l *EFSNSLogger) LogMountTargetCreated(namespace, fileSystemID, mountTargetID, subnetID string, duration time.Duration) {
	klog.V(LogLevelInfo).InfoS("Mount target created successfully",
		"component", l.component,
		"operation", "mount_target_create",
		"namespace", namespace,
		"fileSystemId", fileSystemID,
		"mountTargetId", mountTargetID,
		"subnetId", subnetID,
		"duration", duration.String(),
		"durationMs", duration.Milliseconds(),
	)
}

// LogMountTargetCreationFailed logs mount target creation failure
func (l *EFSNSLogger) LogMountTargetCreationFailed(namespace, fileSystemID, subnetID string, duration time.Duration, err error) {
	efsnsErr, isEFSNSError := err.(*EFSNSError)
	if isEFSNSError {
		klog.V(LogLevelError).ErrorS(err, "Mount target creation failed",
			"component", l.component,
			"operation", "mount_target_create",
			"namespace", namespace,
			"fileSystemId", fileSystemID,
			"subnetId", subnetID,
			"duration", duration.String(),
			"durationMs", duration.Milliseconds(),
			"errorType", string(efsnsErr.Type),
			"errorOperation", efsnsErr.Operation,
		)
	} else {
		klog.V(LogLevelError).ErrorS(err, "Mount target creation failed",
			"component", l.component,
			"operation", "mount_target_create",
			"namespace", namespace,
			"fileSystemId", fileSystemID,
			"subnetId", subnetID,
			"duration", duration.String(),
			"durationMs", duration.Milliseconds(),
		)
	}
}

// LogMountTargetDeleted logs successful mount target deletion
func (l *EFSNSLogger) LogMountTargetDeleted(namespace, fileSystemID, mountTargetID string, duration time.Duration) {
	klog.V(LogLevelInfo).InfoS("Mount target deleted successfully",
		"component", l.component,
		"operation", "mount_target_delete",
		"namespace", namespace,
		"fileSystemId", fileSystemID,
		"mountTargetId", mountTargetID,
		"duration", duration.String(),
		"durationMs", duration.Milliseconds(),
	)
}

// Cache Operations

// LogCacheHit logs cache hit events
func (l *EFSNSLogger) LogCacheHit(operation, namespace string, duration time.Duration) {
	klog.V(LogLevelTrace).InfoS("Cache hit",
		"component", l.component,
		"operation", "cache_hit",
		"cacheOperation", operation,
		"namespace", namespace,
		"duration", duration.String(),
		"durationMs", duration.Milliseconds(),
	)
}

// LogCacheMiss logs cache miss events
func (l *EFSNSLogger) LogCacheMiss(operation, namespace string, duration time.Duration) {
	klog.V(LogLevelTrace).InfoS("Cache miss",
		"component", l.component,
		"operation", "cache_miss",
		"cacheOperation", operation,
		"namespace", namespace,
		"duration", duration.String(),
		"durationMs", duration.Milliseconds(),
	)
}

// LogCacheRefresh logs cache refresh operations
func (l *EFSNSLogger) LogCacheRefresh(namespace string, duration time.Duration, success bool) {
	if success {
		klog.V(LogLevelDebug).InfoS("Cache refreshed successfully",
			"component", l.component,
			"operation", "cache_refresh",
			"namespace", namespace,
			"duration", duration.String(),
			"durationMs", duration.Milliseconds(),
		)
	} else {
		klog.V(LogLevelWarning).InfoS("Cache refresh failed",
			"component", l.component,
			"operation", "cache_refresh",
			"namespace", namespace,
			"duration", duration.String(),
			"durationMs", duration.Milliseconds(),
		)
	}
}

// PVC Tracking Operations

// LogPVCAdded logs PVC addition to tracking
func (l *EFSNSLogger) LogPVCAdded(namespace, pvcName, volumeID string) {
	klog.V(LogLevelInfo).InfoS("PVC added to tracking",
		"component", l.component,
		"operation", "pvc_track_add",
		"namespace", namespace,
		"pvcName", pvcName,
		"volumeId", volumeID,
	)
}

// LogPVCRemoved logs PVC removal from tracking
func (l *EFSNSLogger) LogPVCRemoved(namespace, pvcName string, namespaceEmpty bool) {
	klog.V(LogLevelInfo).InfoS("PVC removed from tracking",
		"component", l.component,
		"operation", "pvc_track_remove",
		"namespace", namespace,
		"pvcName", pvcName,
		"namespaceEmpty", namespaceEmpty,
	)
}

// LogPVCTrackingFailed logs PVC tracking operation failures
func (l *EFSNSLogger) LogPVCTrackingFailed(operation, namespace, pvcName string, err error) {
	efsnsErr, isEFSNSError := err.(*EFSNSError)
	if isEFSNSError {
		klog.V(LogLevelError).ErrorS(err, "PVC tracking operation failed",
			"component", l.component,
			"operation", "pvc_track_error",
			"trackingOperation", operation,
			"namespace", namespace,
			"pvcName", pvcName,
			"errorType", string(efsnsErr.Type),
			"errorOperation", efsnsErr.Operation,
		)
	} else {
		klog.V(LogLevelError).ErrorS(err, "PVC tracking operation failed",
			"component", l.component,
			"operation", "pvc_track_error",
			"trackingOperation", operation,
			"namespace", namespace,
			"pvcName", pvcName,
		)
	}
}

// Security Group Operations

// LogSecurityGroupCreated logs successful security group creation
func (l *EFSNSLogger) LogSecurityGroupCreated(namespace, securityGroupID, vpcID string, duration time.Duration) {
	klog.V(LogLevelInfo).InfoS("Security group created successfully",
		"component", l.component,
		"operation", "security_group_create",
		"namespace", namespace,
		"securityGroupId", securityGroupID,
		"vpcId", vpcID,
		"duration", duration.String(),
		"durationMs", duration.Milliseconds(),
	)
}

// LogSecurityGroupDeleted logs successful security group deletion
func (l *EFSNSLogger) LogSecurityGroupDeleted(namespace, securityGroupID string, duration time.Duration) {
	klog.V(LogLevelInfo).InfoS("Security group deleted successfully",
		"component", l.component,
		"operation", "security_group_delete",
		"namespace", namespace,
		"securityGroupId", securityGroupID,
		"duration", duration.String(),
		"durationMs", duration.Milliseconds(),
	)
}

// Volume Operations

// LogVolumeCreated logs successful volume creation
func (l *EFSNSLogger) LogVolumeCreated(namespace, volumeID, fileSystemID string, duration time.Duration) {
	klog.V(LogLevelInfo).InfoS("Volume created successfully",
		"component", l.component,
		"operation", "volume_create",
		"namespace", namespace,
		"volumeId", volumeID,
		"fileSystemId", fileSystemID,
		"duration", duration.String(),
		"durationMs", duration.Milliseconds(),
	)
}

// LogVolumeDeleted logs successful volume deletion
func (l *EFSNSLogger) LogVolumeDeleted(namespace, volumeID string, duration time.Duration) {
	klog.V(LogLevelInfo).InfoS("Volume deleted successfully",
		"component", l.component,
		"operation", "volume_delete",
		"namespace", namespace,
		"volumeId", volumeID,
		"duration", duration.String(),
		"durationMs", duration.Milliseconds(),
	)
}

// LogVolumeOperationFailed logs volume operation failures
func (l *EFSNSLogger) LogVolumeOperationFailed(operation, namespace, volumeID string, duration time.Duration, err error) {
	efsnsErr, isEFSNSError := err.(*EFSNSError)
	if isEFSNSError {
		klog.V(LogLevelError).ErrorS(err, "Volume operation failed",
			"component", l.component,
			"operation", "volume_error",
			"volumeOperation", operation,
			"namespace", namespace,
			"volumeId", volumeID,
			"duration", duration.String(),
			"durationMs", duration.Milliseconds(),
			"errorType", string(efsnsErr.Type),
			"errorOperation", efsnsErr.Operation,
		)
	} else {
		klog.V(LogLevelError).ErrorS(err, "Volume operation failed",
			"component", l.component,
			"operation", "volume_error",
			"volumeOperation", operation,
			"namespace", namespace,
			"volumeId", volumeID,
			"duration", duration.String(),
			"durationMs", duration.Milliseconds(),
		)
	}
}

// AWS API Operations

// LogAWSAPICall logs AWS API call details
func (l *EFSNSLogger) LogAWSAPICall(service, operation string, duration time.Duration, success bool) {
	if success {
		klog.V(LogLevelDebug).InfoS("AWS API call successful",
			"component", l.component,
			"operation", "aws_api_call",
			"awsService", service,
			"awsOperation", operation,
			"duration", duration.String(),
			"durationMs", duration.Milliseconds(),
		)
	} else {
		klog.V(LogLevelWarning).InfoS("AWS API call failed",
			"component", l.component,
			"operation", "aws_api_call",
			"awsService", service,
			"awsOperation", operation,
			"duration", duration.String(),
			"durationMs", duration.Milliseconds(),
		)
	}
}

// LogAWSAPIThrottled logs AWS API throttling events
func (l *EFSNSLogger) LogAWSAPIThrottled(service, operation string, retryCount int, nextRetryAfter time.Duration) {
	klog.V(LogLevelWarning).InfoS("AWS API throttled, retrying",
		"component", l.component,
		"operation", "aws_api_throttled",
		"awsService", service,
		"awsOperation", operation,
		"retryCount", retryCount,
		"nextRetryAfter", nextRetryAfter.String(),
	)
}

// LogAWSAPIError logs AWS API errors
func (l *EFSNSLogger) LogAWSAPIError(service, operation string, duration time.Duration, err error) {
	klog.V(LogLevelError).ErrorS(err, "AWS API error",
		"component", l.component,
		"operation", "aws_api_error",
		"awsService", service,
		"awsOperation", operation,
		"duration", duration.String(),
		"durationMs", duration.Milliseconds(),
	)
}

// Kubernetes API Operations

// LogKubernetesAPICall logs Kubernetes API call details
func (l *EFSNSLogger) LogKubernetesAPICall(resource, operation string, duration time.Duration, success bool) {
	if success {
		klog.V(LogLevelDebug).InfoS("Kubernetes API call successful",
			"component", l.component,
			"operation", "k8s_api_call",
			"resource", resource,
			"k8sOperation", operation,
			"duration", duration.String(),
			"durationMs", duration.Milliseconds(),
		)
	} else {
		klog.V(LogLevelWarning).InfoS("Kubernetes API call failed",
			"component", l.component,
			"operation", "k8s_api_call",
			"resource", resource,
			"k8sOperation", operation,
			"duration", duration.String(),
			"durationMs", duration.Milliseconds(),
		)
	}
}

// LogKubernetesAPIError logs Kubernetes API errors
func (l *EFSNSLogger) LogKubernetesAPIError(resource, operation string, duration time.Duration, err error) {
	klog.V(LogLevelError).ErrorS(err, "Kubernetes API error",
		"component", l.component,
		"operation", "k8s_api_error",
		"resource", resource,
		"k8sOperation", operation,
		"duration", duration.String(),
		"durationMs", duration.Milliseconds(),
	)
}

// Finalizer Operations

// LogFinalizerAdded logs finalizer addition
func (l *EFSNSLogger) LogFinalizerAdded(resourceType, namespace, name, finalizerName string) {
	klog.V(LogLevelInfo).InfoS("Finalizer added",
		"component", l.component,
		"operation", "finalizer_add",
		"resourceType", resourceType,
		"namespace", namespace,
		"name", name,
		"finalizerName", finalizerName,
	)
}

// LogFinalizerRemoved logs finalizer removal
func (l *EFSNSLogger) LogFinalizerRemoved(resourceType, namespace, name, finalizerName string) {
	klog.V(LogLevelInfo).InfoS("Finalizer removed",
		"component", l.component,
		"operation", "finalizer_remove",
		"resourceType", resourceType,
		"namespace", namespace,
		"name", name,
		"finalizerName", finalizerName,
	)
}

// Metrics Operations

// LogMetricsServerStarted logs metrics server startup
func (l *EFSNSLogger) LogMetricsServerStarted(address, port string) {
	klog.V(LogLevelInfo).InfoS("Metrics server started",
		"component", l.component,
		"operation", "metrics_server_start",
		"address", address,
		"port", port,
	)
}

// LogMetricsServerStopped logs metrics server shutdown
func (l *EFSNSLogger) LogMetricsServerStopped() {
	klog.V(LogLevelInfo).InfoS("Metrics server stopped",
		"component", l.component,
		"operation", "metrics_server_stop",
	)
}

// LogMetricsCollection logs metrics collection events
func (l *EFSNSLogger) LogMetricsCollection(metricName string, value float64, labels map[string]string) {
	klog.V(LogLevelTrace).InfoS("Metric collected",
		"component", l.component,
		"operation", "metrics_collect",
		"metricName", metricName,
		"value", value,
		"labels", labels,
	)
}

// General Operations

// LogOperationStarted logs the start of any operation with context
func (l *EFSNSLogger) LogOperationStarted(ctx context.Context, operation, namespace string, params map[string]interface{}) {
	// Extract request ID from context if available
	var requestID string
	if ctx != nil {
		if id := ctx.Value("requestId"); id != nil {
			if idStr, ok := id.(string); ok {
				requestID = idStr
			}
		}
	}

	logFields := []interface{}{
		"component", l.component,
		"operation", "operation_start",
		"operationType", operation,
		"namespace", namespace,
	}

	if requestID != "" {
		logFields = append(logFields, "requestId", requestID)
	}

	for key, value := range params {
		logFields = append(logFields, key, value)
	}

	klog.V(LogLevelInfo).InfoS("Operation started", logFields...)
}

// LogOperationCompleted logs successful operation completion with context
func (l *EFSNSLogger) LogOperationCompleted(ctx context.Context, operation, namespace string, duration time.Duration, result map[string]interface{}) {
	// Extract request ID from context if available
	var requestID string
	if ctx != nil {
		if id := ctx.Value("requestId"); id != nil {
			if idStr, ok := id.(string); ok {
				requestID = idStr
			}
		}
	}

	logFields := []interface{}{
		"component", l.component,
		"operation", "operation_complete",
		"operationType", operation,
		"namespace", namespace,
		"duration", duration.String(),
		"durationMs", duration.Milliseconds(),
	}

	if requestID != "" {
		logFields = append(logFields, "requestId", requestID)
	}

	for key, value := range result {
		logFields = append(logFields, key, value)
	}

	klog.V(LogLevelInfo).InfoS("Operation completed successfully", logFields...)
}

// LogDebug logs debug information
func (l *EFSNSLogger) LogDebug(message string, keysAndValues ...interface{}) {
	logFields := []interface{}{
		"component", l.component,
		"operation", "debug",
	}
	logFields = append(logFields, keysAndValues...)
	klog.V(LogLevelDebug).InfoS(message, logFields...)
}

// LogInfo logs informational messages
func (l *EFSNSLogger) LogInfo(message string, keysAndValues ...interface{}) {
	logFields := []interface{}{
		"component", l.component,
		"operation", "info",
	}
	logFields = append(logFields, keysAndValues...)
	klog.V(LogLevelInfo).InfoS(message, logFields...)
}

// LogWarning logs warning messages
func (l *EFSNSLogger) LogWarning(message string, keysAndValues ...interface{}) {
	logFields := []interface{}{
		"component", l.component,
		"operation", "warning",
	}
	logFields = append(logFields, keysAndValues...)
	klog.V(LogLevelWarning).InfoS(message, logFields...)
}

// LogError logs error messages
func (l *EFSNSLogger) LogError(err error, message string, keysAndValues ...interface{}) {
	logFields := []interface{}{
		"component", l.component,
		"operation", "error",
	}
	logFields = append(logFields, keysAndValues...)

	if efsnsErr, ok := err.(*EFSNSError); ok {
		logFields = append(logFields, "errorType", string(efsnsErr.Type))
		logFields = append(logFields, "errorOperation", efsnsErr.Operation)
	}

	klog.V(LogLevelError).ErrorS(err, message, logFields...)
}

// Global logger instances for different components
var (
	FileSystemManagerLogger = NewEFSNSLogger("filesystem-manager")
	CacheLogger             = NewEFSNSLogger("cache")
	TrackerLogger           = NewEFSNSLogger("tracker")
	FinalizerLogger         = NewEFSNSLogger("finalizer")
	MetricsLogger           = NewEFSNSLogger("metrics")
	SecurityGroupLogger     = NewEFSNSLogger("security-group")
	MountTargetLogger       = NewEFSNSLogger("mount-target")
	ControllerLogger        = NewEFSNSLogger("controller")
	NodeLogger              = NewEFSNSLogger("node")
)
