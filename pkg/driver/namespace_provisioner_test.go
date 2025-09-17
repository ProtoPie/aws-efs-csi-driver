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

package driver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/cloud"
)

// Helper functions
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestDefaultProvisionerOptions(t *testing.T) {
	options := DefaultProvisionerOptions()

	if options == nil {
		t.Errorf("Expected options to be non-nil")
	}
	if options.CacheTimeout != DefaultCacheTimeout {
		t.Errorf("Expected CacheTimeout to be %v, got %v", DefaultCacheTimeout, options.CacheTimeout)
	}
	if options.MaxRetries != DefaultMaxRetries {
		t.Errorf("Expected MaxRetries to be %d, got %d", DefaultMaxRetries, options.MaxRetries)
	}
	if options.RetryDelay != DefaultRetryDelay {
		t.Errorf("Expected RetryDelay to be %v, got %v", DefaultRetryDelay, options.RetryDelay)
	}
	if options.CreateTimeout != 5*time.Minute {
		t.Errorf("Expected CreateTimeout to be 5m, got %v", options.CreateTimeout)
	}
	if !options.EnableAsyncCleanup {
		t.Errorf("Expected EnableAsyncCleanup to be true")
	}
	if !options.EnableCaching {
		t.Errorf("Expected EnableCaching to be true")
	}
}

func TestValidateProvisionerOptions(t *testing.T) {
	testCases := []struct {
		name          string
		options       *ProvisionerOptions
		expectError   bool
		errorContains string
	}{
		{
			name:        "Valid options",
			options:     DefaultProvisionerOptions(),
			expectError: false,
		},
		{
			name: "Invalid cache timeout",
			options: &ProvisionerOptions{
				CacheTimeout: -1 * time.Second,
				MaxRetries:   3,
				RetryDelay:   time.Second,
				CreateTimeout: time.Minute,
				DeleteTimeout: time.Minute,
				HealthCheckInterval: time.Minute,
			},
			expectError:   true,
			errorContains: "cache timeout must be positive",
		},
		{
			name: "Invalid max retries",
			options: &ProvisionerOptions{
				CacheTimeout: time.Minute,
				MaxRetries:   -1,
				RetryDelay:   time.Second,
				CreateTimeout: time.Minute,
				DeleteTimeout: time.Minute,
				HealthCheckInterval: time.Minute,
			},
			expectError:   true,
			errorContains: "max retries cannot be negative",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateProvisionerOptions(tc.options)

			if tc.expectError {
				if err == nil {
					t.Errorf("Expected error but got nil")
				}
				if err != nil && !contains(err.Error(), tc.errorContains) {
					t.Errorf("Expected error to contain '%s', got '%s'", tc.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
			}
		})
	}
}

func TestNamespaceProvisioner_Cache(t *testing.T) {
	options := DefaultProvisionerOptions()
	options.CacheTimeout = 100 * time.Millisecond

	provisioner := &NamespaceProvisioner{
		options:  options,
		efsCache: make(map[string]*CachedEFS),
	}

	namespace := "test-namespace"
	fs := &cloud.FileSystem{FileSystemId: "fs-123456"}

	// Test cache miss
	cached := provisioner.getCachedEFS(namespace)
	if cached != nil {
		t.Errorf("Expected cache miss, got %v", cached)
	}

	// Test cache set and hit
	provisioner.setCachedEFS(namespace, fs)
	cached = provisioner.getCachedEFS(namespace)
	if cached == nil {
		t.Errorf("Expected cache hit, got nil")
	}
	if cached != nil && cached.FileSystemId != fs.FileSystemId {
		t.Errorf("Expected FileSystemId %s, got %s", fs.FileSystemId, cached.FileSystemId)
	}

	// Test cache expiration
	time.Sleep(150 * time.Millisecond)
	cached = provisioner.getCachedEFS(namespace)
	if cached != nil {
		t.Errorf("Expected cache to be expired, got %v", cached)
	}

	// Test cache removal
	provisioner.setCachedEFS(namespace, fs)
	provisioner.removeCachedEFS(namespace)
	cached = provisioner.getCachedEFS(namespace)
	if cached != nil {
		t.Errorf("Expected cache to be removed, got %v", cached)
	}

	// Test cache clear
	provisioner.setCachedEFS(namespace, fs)
	provisioner.setCachedEFS("another-namespace", fs)
	provisioner.clearCache()
	if len(provisioner.efsCache) != 0 {
		t.Errorf("Expected cache to be cleared, got %d entries", len(provisioner.efsCache))
	}
}

func TestNamespaceProvisioner_CacheDisabled(t *testing.T) {
	options := DefaultProvisionerOptions()
	options.EnableCaching = false

	provisioner := &NamespaceProvisioner{
		options:  options,
		efsCache: make(map[string]*CachedEFS),
	}

	namespace := "test-namespace"
	fs := &cloud.FileSystem{FileSystemId: "fs-123456"}

	// When caching is disabled, cache operations should be no-ops
	provisioner.setCachedEFS(namespace, fs)
	if len(provisioner.efsCache) != 0 {
		t.Errorf("Expected cache to remain empty when disabled, got %d entries", len(provisioner.efsCache))
	}

	cached := provisioner.getCachedEFS(namespace)
	if cached != nil {
		t.Errorf("Expected nil when caching disabled, got %v", cached)
	}

	// removeCachedEFS should not panic
	provisioner.removeCachedEFS(namespace)
}

func TestNamespaceProvisioner_UpdateStatus(t *testing.T) {
	provisioner := &NamespaceProvisioner{
		status: &ProvisionerStatus{
			Started: false,
			Healthy: false,
		},
	}

	// Test updateStatus
	provisioner.updateStatus(func(status *ProvisionerStatus) {
		status.Started = true
		status.Healthy = true
		status.ActiveNamespaces = 10
	})

	status := provisioner.GetStatus()
	if !status.Started {
		t.Errorf("Expected Started to be true")
	}
	if !status.Healthy {
		t.Errorf("Expected Healthy to be true")
	}
	if status.ActiveNamespaces != 10 {
		t.Errorf("Expected ActiveNamespaces to be 10, got %d", status.ActiveNamespaces)
	}
}

func TestNamespaceProvisioner_IsHealthy(t *testing.T) {
	provisioner := &NamespaceProvisioner{
		status: &ProvisionerStatus{
			Started: true,
			Healthy: true,
		},
	}

	if !provisioner.IsHealthy() {
		t.Errorf("Expected provisioner to be healthy")
	}

	provisioner.updateStatus(func(status *ProvisionerStatus) {
		status.Healthy = false
	})

	if provisioner.IsHealthy() {
		t.Errorf("Expected provisioner to be unhealthy")
	}
}

func TestNoOpMetricsCollector(t *testing.T) {
	collector := &NoOpMetricsCollector{}

	// All methods should not panic
	collector.IncEFSCreated("test")
	collector.IncEFSDeleted("test")
	collector.IncAccessPointCreated("test")
	collector.IncAccessPointDeleted("test")
	collector.RecordEFSCreationTime("test", time.Second)
	collector.RecordError("test", "test", fmt.Errorf("test error"))
	collector.SetActiveNamespaces(5)
}

func TestEFSOptions(t *testing.T) {
	options := &EFSOptions{
		PerformanceMode:               "generalPurpose",
		ThroughputMode:               "bursting",
		ProvisionedThroughputInMibps: 100,
		Encrypted:                    true,
		KmsKeyId:                     "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012",
		LifecyclePolicy:              "AFTER_30_DAYS",
		BackupPolicy:                 "ENABLED",
		Tags: map[string]string{
			"Environment": "test",
			"Team":        "platform",
		},
	}

	if options == nil {
		t.Errorf("Expected options to be non-nil")
	}
	if options.PerformanceMode != "generalPurpose" {
		t.Errorf("Expected PerformanceMode to be 'generalPurpose', got %s", options.PerformanceMode)
	}
	if options.ThroughputMode != "bursting" {
		t.Errorf("Expected ThroughputMode to be 'bursting', got %s", options.ThroughputMode)
	}
	if options.ProvisionedThroughputInMibps != 100 {
		t.Errorf("Expected ProvisionedThroughputInMibps to be 100, got %d", options.ProvisionedThroughputInMibps)
	}
	if !options.Encrypted {
		t.Errorf("Expected Encrypted to be true")
	}
	if len(options.Tags) != 2 {
		t.Errorf("Expected 2 tags, got %d", len(options.Tags))
	}
}

func TestCachedEFS(t *testing.T) {
	now := time.Now()
	fs := &cloud.FileSystem{FileSystemId: "fs-123456"}

	cached := &CachedEFS{
		FileSystem:   fs,
		Namespace:    "test-namespace",
		CreatedAt:    now,
		LastAccessed: now,
		AccessCount:  1,
	}

	if cached == nil {
		t.Errorf("Expected cached to be non-nil")
	}
	if cached.FileSystem.FileSystemId != "fs-123456" {
		t.Errorf("Expected FileSystemId to be 'fs-123456', got %s", cached.FileSystem.FileSystemId)
	}
	if cached.Namespace != "test-namespace" {
		t.Errorf("Expected Namespace to be 'test-namespace', got %s", cached.Namespace)
	}
	if cached.AccessCount != 1 {
		t.Errorf("Expected AccessCount to be 1, got %d", cached.AccessCount)
	}
}

func TestProvisionerStatus(t *testing.T) {
	now := time.Now()
	testErr := fmt.Errorf("test error")

	status := &ProvisionerStatus{
		Started:          true,
		Healthy:          true,
		LastHealthCheck:  now,
		ActiveNamespaces: 5,
		TotalEFSCreated:  10,
		TotalErrors:      2,
		LastError:        testErr,
		LastErrorTime:    now,
	}

	if !status.Started {
		t.Errorf("Expected Started to be true")
	}
	if !status.Healthy {
		t.Errorf("Expected Healthy to be true")
	}
	if status.ActiveNamespaces != 5 {
		t.Errorf("Expected ActiveNamespaces to be 5, got %d", status.ActiveNamespaces)
	}
	if status.TotalEFSCreated != 10 {
		t.Errorf("Expected TotalEFSCreated to be 10, got %d", status.TotalEFSCreated)
	}
	if status.TotalErrors != 2 {
		t.Errorf("Expected TotalErrors to be 2, got %d", status.TotalErrors)
	}
	if status.LastError != testErr {
		t.Errorf("Expected LastError to be %v, got %v", testErr, status.LastError)
	}
}

func TestCleanupExpiredCache(t *testing.T) {
	options := DefaultProvisionerOptions()
	options.CacheTimeout = 50 * time.Millisecond

	provisioner := &NamespaceProvisioner{
		options:  options,
		efsCache: make(map[string]*CachedEFS),
	}

	now := time.Now()
	fs := &cloud.FileSystem{FileSystemId: "fs-123456"}

	// Add entries with different access times
	provisioner.efsCache["fresh"] = &CachedEFS{
		FileSystem:   fs,
		Namespace:    "fresh",
		LastAccessed: now,
	}

	provisioner.efsCache["expired"] = &CachedEFS{
		FileSystem:   fs,
		Namespace:    "expired",
		LastAccessed: now.Add(-100 * time.Millisecond),
	}

	// Cleanup expired cache
	provisioner.cleanupExpiredCache()

	// Fresh entry should remain, expired should be removed
	if _, exists := provisioner.efsCache["fresh"]; !exists {
		t.Errorf("Expected 'fresh' entry to remain in cache")
	}
	if _, exists := provisioner.efsCache["expired"]; exists {
		t.Errorf("Expected 'expired' entry to be removed from cache")
	}
}

// Benchmark tests
func BenchmarkNamespaceProvisioner_GetCachedEFS(b *testing.B) {
	provisioner := &NamespaceProvisioner{
		options:  DefaultProvisionerOptions(),
		efsCache: make(map[string]*CachedEFS),
	}

	fs := &cloud.FileSystem{FileSystemId: "fs-123456"}
	provisioner.setCachedEFS("test-namespace", fs)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		provisioner.getCachedEFS("test-namespace")
	}
}

func BenchmarkNamespaceProvisioner_SetCachedEFS(b *testing.B) {
	provisioner := &NamespaceProvisioner{
		options:  DefaultProvisionerOptions(),
		efsCache: make(map[string]*CachedEFS),
	}

	fs := &cloud.FileSystem{FileSystemId: "fs-123456"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		namespace := fmt.Sprintf("test-namespace-%d", i)
		provisioner.setCachedEFS(namespace, fs)
	}
}

// Mock implementations for testing

type testMockCloud struct {
	createFileSystemFunc      func(ctx context.Context, clientToken string, options *cloud.FileSystemOptions) (*cloud.FileSystem, error)
	describeFileSystemFunc    func(ctx context.Context, fileSystemId string) (*cloud.FileSystem, error)
	findFileSystemsByTagsFunc func(ctx context.Context, tags map[string]string) ([]*cloud.FileSystem, error)
}

func (m *testMockCloud) CreateFileSystem(ctx context.Context, clientToken string, options *cloud.FileSystemOptions) (*cloud.FileSystem, error) {
	if m.createFileSystemFunc != nil {
		return m.createFileSystemFunc(ctx, clientToken, options)
	}
	return &cloud.FileSystem{
		FileSystemId:    "fs-12345678",
		LifeCycleState:  "available",
		PerformanceMode: "generalPurpose",
		ThroughputMode:  "bursting",
		Encrypted:       true,
	}, nil
}

func (m *testMockCloud) DescribeFileSystem(ctx context.Context, fileSystemId string) (*cloud.FileSystem, error) {
	if m.describeFileSystemFunc != nil {
		return m.describeFileSystemFunc(ctx, fileSystemId)
	}
	return &cloud.FileSystem{FileSystemId: fileSystemId}, nil
}

func (m *testMockCloud) FindFileSystemsByTags(ctx context.Context, tags map[string]string) ([]*cloud.FileSystem, error) {
	if m.findFileSystemsByTagsFunc != nil {
		return m.findFileSystemsByTagsFunc(ctx, tags)
	}
	return []*cloud.FileSystem{}, nil
}

// Other required methods to satisfy the Cloud interface
func (m *testMockCloud) GetMetadata() cloud.MetadataService                              { return nil }
func (m *testMockCloud) CreateAccessPoint(ctx context.Context, clientToken string, accessPointOpts *cloud.AccessPointOptions) (*cloud.AccessPoint, error) { return nil, nil }
func (m *testMockCloud) DeleteAccessPoint(ctx context.Context, accessPointId string) error { return nil }
func (m *testMockCloud) DescribeAccessPoint(ctx context.Context, accessPointId string) (*cloud.AccessPoint, error) { return nil, nil }
func (m *testMockCloud) FindAccessPointByClientToken(ctx context.Context, clientToken, fileSystemId string) (*cloud.AccessPoint, error) { return nil, nil }
func (m *testMockCloud) ListAccessPoints(ctx context.Context, fileSystemId string) ([]*cloud.AccessPoint, error) { return nil, nil }
func (m *testMockCloud) DescribeMountTargets(ctx context.Context, fileSystemId, az string) (*cloud.MountTarget, error) { return nil, nil }
func (m *testMockCloud) CreateMountTarget(ctx context.Context, fileSystemId, subnetId, securityGroupId string) (*cloud.MountTarget, error) { return nil, nil }
func (m *testMockCloud) GetFileSystemTags(ctx context.Context, fileSystemId string) (map[string]string, error) { return nil, nil }

type mockMapper struct {
	createOrUpdateMappingFunc func(ctx context.Context, namespace, fileSystemID, fileSystemArn, region string) (*NamespaceEFSMapping, error)
	getMappingFunc           func(ctx context.Context, namespace string) (*NamespaceEFSMapping, error)
}

func (m *mockMapper) CreateOrUpdateMapping(ctx context.Context, namespace, fileSystemID, fileSystemArn, region string) (*NamespaceEFSMapping, error) {
	if m.createOrUpdateMappingFunc != nil {
		return m.createOrUpdateMappingFunc(ctx, namespace, fileSystemID, fileSystemArn, region)
	}
	return &NamespaceEFSMapping{
		Namespace:     namespace,
		FileSystemID:  fileSystemID,
		FileSystemArn: fileSystemArn,
		Region:        region,
	}, nil
}

func (m *mockMapper) GetMapping(ctx context.Context, namespace string) (*NamespaceEFSMapping, error) {
	if m.getMappingFunc != nil {
		return m.getMappingFunc(ctx, namespace)
	}
	return nil, fmt.Errorf("mapping not found")
}

// Other required methods to satisfy NamespaceEFSMapperInterface
func (m *mockMapper) DeleteMapping(ctx context.Context, namespace string) error { return nil }
func (m *mockMapper) ListMappings(ctx context.Context) ([]NamespaceEFSMapping, error) { return nil, nil }
func (m *mockMapper) Start(ctx context.Context) error { return nil }
func (m *mockMapper) Stop() {}
func (m *mockMapper) InvalidateCache(namespace string) {}
func (m *mockMapper) ClearCache() {}
func (m *mockMapper) RecoverFromAWSTags(ctx context.Context, clusterID string) (int, error) { return 0, nil }
func (m *mockMapper) SyncWithAWSTags(ctx context.Context, clusterID string) error { return nil }

type mockLockManager struct {
	acquireLockFunc func(ctx context.Context, key string, timeout time.Duration) (interface{}, error)
	releaseLockFunc func(ctx context.Context, key string, lock interface{}) error
}

func (m *mockLockManager) AcquireLock(ctx context.Context, key string, timeout time.Duration) (interface{}, error) {
	if m.acquireLockFunc != nil {
		return m.acquireLockFunc(ctx, key, timeout)
	}
	return "mock-lock", nil
}

func (m *mockLockManager) ReleaseLock(ctx context.Context, key string, lock interface{}) error {
	if m.releaseLockFunc != nil {
		return m.releaseLockFunc(ctx, key, lock)
	}
	return nil
}

type mockMetricsCollector struct {
	incEFSCreatedFunc        func(namespace string)
	recordEFSCreationTimeFunc func(namespace string, duration time.Duration)
	recordErrorFunc          func(operation, namespace string, err error)
}

func (m *mockMetricsCollector) IncEFSCreated(namespace string) {
	if m.incEFSCreatedFunc != nil {
		m.incEFSCreatedFunc(namespace)
	}
}

func (m *mockMetricsCollector) RecordEFSCreationTime(namespace string, duration time.Duration) {
	if m.recordEFSCreationTimeFunc != nil {
		m.recordEFSCreationTimeFunc(namespace, duration)
	}
}

func (m *mockMetricsCollector) RecordError(operation, namespace string, err error) {
	if m.recordErrorFunc != nil {
		m.recordErrorFunc(operation, namespace, err)
	}
}

// Other required methods
func (m *mockMetricsCollector) IncEFSDeleted(namespace string) {}
func (m *mockMetricsCollector) IncAccessPointCreated(namespace string) {}
func (m *mockMetricsCollector) IncAccessPointDeleted(namespace string) {}
func (m *mockMetricsCollector) SetActiveNamespaces(count int) {}

// Test cases for CreateNamespaceEFS

func TestNamespaceProvisioner_CreateNamespaceEFS_Success(t *testing.T) {
	mockCloud := &testMockCloud{}
	mapper := &mockMapper{}
	lockManager := NewLockManagerMap()
	metrics := &mockMetricsCollector{}

	options := DefaultProvisionerOptions()
	options.ClusterID = "test-cluster"
	options.DefaultTags = map[string]string{
		"Environment": "test",
	}

	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		mapper:           mapper,
		lockManager:      lockManager,
		metricsCollector: metrics,
		options:          options,
		efsCache:         make(map[string]*CachedEFS),
		status:           &ProvisionerStatus{},
	}

	namespace := "test-namespace"
	efsOptions := &EFSOptions{
		PerformanceMode: "generalPurpose",
		ThroughputMode:  "bursting",
		Encrypted:       true,
		Tags: map[string]string{
			"Team": "platform",
		},
	}

	// Verify cloud.CreateFileSystem is called with correct parameters
	var capturedOptions *cloud.FileSystemOptions
	mockCloud.createFileSystemFunc = func(ctx context.Context, clientToken string, options *cloud.FileSystemOptions) (*cloud.FileSystem, error) {
		capturedOptions = options
		return &cloud.FileSystem{
			FileSystemId:    "fs-12345678",
			LifeCycleState:  "available",
			PerformanceMode: "generalPurpose",
			ThroughputMode:  "bursting",
			Encrypted:       true,
		}, nil
	}

	// Track metrics calls
	var metricsCreated bool
	var metricsTime time.Duration
	metrics.incEFSCreatedFunc = func(ns string) {
		if ns == namespace {
			metricsCreated = true
		}
	}
	metrics.recordEFSCreationTimeFunc = func(ns string, duration time.Duration) {
		if ns == namespace {
			metricsTime = duration
		}
	}

	ctx := context.Background()
	fs, err := provisioner.CreateNamespaceEFS(ctx, namespace, efsOptions)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if fs == nil {
		t.Errorf("Expected filesystem to be non-nil")
	}
	if fs.FileSystemId != "fs-12345678" {
		t.Errorf("Expected FileSystemId to be 'fs-12345678', got %s", fs.FileSystemId)
	}

	// Verify tags were set correctly
	if capturedOptions == nil {
		t.Errorf("Expected options to be captured")
	} else {
		expectedTags := map[string]string{
			"Environment":                        "test",     // Default tag
			"Team":                               "platform", // User tag
			"kubernetes.io/namespace":            namespace,
			"kubernetes.io/provisioning-mode":    "efs-ns",
			"kubernetes.io/cluster/test-cluster": "owned",
		}
		for k, v := range expectedTags {
			if capturedOptions.Tags[k] != v {
				t.Errorf("Expected tag %s to be %s, got %s", k, v, capturedOptions.Tags[k])
			}
		}
		if !capturedOptions.Encrypted {
			t.Errorf("Expected filesystem to be encrypted")
		}
		if capturedOptions.PerformanceMode != "generalPurpose" {
			t.Errorf("Expected PerformanceMode to be 'generalPurpose', got %s", capturedOptions.PerformanceMode)
		}
	}

	// Verify metrics were recorded
	if !metricsCreated {
		t.Errorf("Expected metrics to record EFS creation")
	}
	if metricsTime == 0 {
		t.Errorf("Expected metrics to record creation time")
	}
}

func TestNamespaceProvisioner_CreateNamespaceEFS_AlreadyExists(t *testing.T) {
	mockCloud := &testMockCloud{}
	mapper := &mockMapper{
		getMappingFunc: func(ctx context.Context, namespace string) (*NamespaceEFSMapping, error) {
			return &NamespaceEFSMapping{
				Namespace:     namespace,
				FileSystemID:  "fs-existing",
				FileSystemArn: "arn:aws:elasticfilesystem:us-east-1:123456789012:file-system/fs-existing",
				Region:        "us-east-1",
			}, nil
		},
	}
	lockManager := NewLockManagerMap()
	metrics := &mockMetricsCollector{}

	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		mapper:           mapper,
		lockManager:      lockManager,
		metricsCollector: metrics,
		options:          options,
		efsCache:         make(map[string]*CachedEFS),
		status:           &ProvisionerStatus{},
	}

	// Mock DescribeFileSystem to return existing filesystem
	mockCloud.describeFileSystemFunc = func(ctx context.Context, fileSystemId string) (*cloud.FileSystem, error) {
		return &cloud.FileSystem{
			FileSystemId: fileSystemId,
		}, nil
	}

	var createCalled bool
	mockCloud.createFileSystemFunc = func(ctx context.Context, clientToken string, options *cloud.FileSystemOptions) (*cloud.FileSystem, error) {
		createCalled = true
		return nil, fmt.Errorf("should not be called")
	}

	ctx := context.Background()
	fs, err := provisioner.CreateNamespaceEFS(ctx, "test-namespace", nil)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if fs == nil {
		t.Errorf("Expected filesystem to be non-nil")
	}
	if fs.FileSystemId != "fs-existing" {
		t.Errorf("Expected FileSystemId to be 'fs-existing', got %s", fs.FileSystemId)
	}
	if createCalled {
		t.Errorf("CreateFileSystem should not have been called when EFS already exists")
	}
}

func TestNamespaceProvisioner_CreateNamespaceEFS_CloudError(t *testing.T) {
	mockCloud := &testMockCloud{
		createFileSystemFunc: func(ctx context.Context, clientToken string, options *cloud.FileSystemOptions) (*cloud.FileSystem, error) {
			return nil, fmt.Errorf("AWS API error")
		},
	}
	mapper := &mockMapper{}
	lockManager := NewLockManagerMap()
	metrics := &mockMetricsCollector{}

	var errorRecorded bool
	metrics.recordErrorFunc = func(operation, namespace string, err error) {
		if operation == "create_filesystem" && namespace == "test-namespace" {
			errorRecorded = true
		}
	}

	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		mapper:           mapper,
		lockManager:      lockManager,
		metricsCollector: metrics,
		options:          options,
		efsCache:         make(map[string]*CachedEFS),
		status:           &ProvisionerStatus{},
	}

	ctx := context.Background()
	fs, err := provisioner.CreateNamespaceEFS(ctx, "test-namespace", nil)

	if err == nil {
		t.Errorf("Expected error, got none")
	}
	if fs != nil {
		t.Errorf("Expected filesystem to be nil when error occurs")
	}
	if !errorRecorded {
		t.Errorf("Expected error to be recorded in metrics")
	}
}

func TestNamespaceProvisioner_CreateNamespaceEFS_LockError(t *testing.T) {
	mockCloud := &testMockCloud{}
	mapper := &mockMapper{}
	lockManager := NewLockManagerMap()
	metrics := &mockMetricsCollector{}

	var errorRecorded bool
	metrics.recordErrorFunc = func(operation, namespace string, err error) {
		if operation == "acquire_lock" && namespace == "test-namespace" {
			errorRecorded = true
		}
	}

	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		mapper:           mapper,
		lockManager:      lockManager,
		metricsCollector: metrics,
		options:          options,
		efsCache:         make(map[string]*CachedEFS),
		status:           &ProvisionerStatus{},
	}

	ctx := context.Background()
	fs, err := provisioner.CreateNamespaceEFS(ctx, "test-namespace", nil)

	if err == nil {
		t.Errorf("Expected error, got none")
	}
	if fs != nil {
		t.Errorf("Expected filesystem to be nil when lock fails")
	}
	if !errorRecorded {
		t.Errorf("Expected lock error to be recorded in metrics")
	}
}

func TestNamespaceProvisioner_CreateNamespaceEFS_DefaultValues(t *testing.T) {
	mockCloud := &testMockCloud{}
	mapper := &mockMapper{}
	lockManager := NewLockManagerMap()
	metrics := &mockMetricsCollector{}

	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		mapper:           mapper,
		lockManager:      lockManager,
		metricsCollector: metrics,
		options:          options,
		efsCache:         make(map[string]*CachedEFS),
		status:           &ProvisionerStatus{},
	}

	var capturedOptions *cloud.FileSystemOptions
	mockCloud.createFileSystemFunc = func(ctx context.Context, clientToken string, options *cloud.FileSystemOptions) (*cloud.FileSystem, error) {
		capturedOptions = options
		return &cloud.FileSystem{
			FileSystemId: "fs-12345678",
		}, nil
	}

	ctx := context.Background()
	_, err := provisioner.CreateNamespaceEFS(ctx, "test-namespace", nil)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify default values were applied
	if capturedOptions == nil {
		t.Errorf("Expected options to be captured")
	} else {
		if capturedOptions.PerformanceMode != "generalPurpose" {
			t.Errorf("Expected default PerformanceMode to be 'generalPurpose', got %s", capturedOptions.PerformanceMode)
		}
		if capturedOptions.ThroughputMode != "bursting" {
			t.Errorf("Expected default ThroughputMode to be 'bursting', got %s", capturedOptions.ThroughputMode)
		}
		if !capturedOptions.Encrypted {
			t.Errorf("Expected default encryption to be true")
		}
	}
}

func TestNamespaceProvisioner_GetNamespaceEFS_FromCache(t *testing.T) {
	mockCloud := &testMockCloud{}
	mapper := &mockMapper{}

	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:    mockCloud,
		mapper:   mapper,
		options:  options,
		efsCache: make(map[string]*CachedEFS),
		status:   &ProvisionerStatus{},
	}

	// Pre-populate cache
	fs := &cloud.FileSystem{FileSystemId: "fs-cached"}
	provisioner.setCachedEFS("test-namespace", fs)

	ctx := context.Background()
	result, err := provisioner.GetNamespaceEFS(ctx, "test-namespace")

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if result == nil {
		t.Errorf("Expected filesystem to be non-nil")
	}
	if result.FileSystemId != "fs-cached" {
		t.Errorf("Expected FileSystemId to be 'fs-cached', got %s", result.FileSystemId)
	}
}

func TestNamespaceProvisioner_GetNamespaceEFS_FromMapper(t *testing.T) {
	mockCloud := &testMockCloud{
		describeFileSystemFunc: func(ctx context.Context, fileSystemId string) (*cloud.FileSystem, error) {
			return &cloud.FileSystem{FileSystemId: fileSystemId}, nil
		},
	}
	mapper := &mockMapper{
		getMappingFunc: func(ctx context.Context, namespace string) (*NamespaceEFSMapping, error) {
			return &NamespaceEFSMapping{
				Namespace:     namespace,
				FileSystemID:  "fs-mapped",
				FileSystemArn: "arn:aws:elasticfilesystem:us-east-1:123456789012:file-system/fs-mapped",
				Region:        "us-east-1",
			}, nil
		},
	}

	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:    mockCloud,
		mapper:   mapper,
		options:  options,
		efsCache: make(map[string]*CachedEFS),
		status:   &ProvisionerStatus{},
	}

	ctx := context.Background()
	result, err := provisioner.GetNamespaceEFS(ctx, "test-namespace")

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if result == nil {
		t.Errorf("Expected filesystem to be non-nil")
	}
	if result.FileSystemId != "fs-mapped" {
		t.Errorf("Expected FileSystemId to be 'fs-mapped', got %s", result.FileSystemId)
	}
}

func TestNamespaceProvisioner_GetNamespaceEFS_FromTags(t *testing.T) {
	mockCloud := &testMockCloud{
		findFileSystemsByTagsFunc: func(ctx context.Context, tags map[string]string) ([]*cloud.FileSystem, error) {
			return []*cloud.FileSystem{
				{FileSystemId: "fs-from-tags"},
			}, nil
		},
	}
	mapper := &mockMapper{
		getMappingFunc: func(ctx context.Context, namespace string) (*NamespaceEFSMapping, error) {
			return nil, fmt.Errorf("not found")
		},
	}

	options := DefaultProvisionerOptions()
	options.ClusterID = "test-cluster"
	provisioner := &NamespaceProvisioner{
		cloud:    mockCloud,
		mapper:   mapper,
		options:  options,
		efsCache: make(map[string]*CachedEFS),
		status:   &ProvisionerStatus{},
	}

	ctx := context.Background()
	result, err := provisioner.GetNamespaceEFS(ctx, "test-namespace")

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if result == nil {
		t.Errorf("Expected filesystem to be non-nil")
	}
	if result.FileSystemId != "fs-from-tags" {
		t.Errorf("Expected FileSystemId to be 'fs-from-tags', got %s", result.FileSystemId)
	}
}

func TestNamespaceProvisioner_GetNamespaceEFS_NotFound(t *testing.T) {
	mockCloud := &testMockCloud{
		findFileSystemsByTagsFunc: func(ctx context.Context, tags map[string]string) ([]*cloud.FileSystem, error) {
			return []*cloud.FileSystem{}, nil
		},
	}
	mapper := &mockMapper{
		getMappingFunc: func(ctx context.Context, namespace string) (*NamespaceEFSMapping, error) {
			return nil, fmt.Errorf("not found")
		},
	}

	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:    mockCloud,
		mapper:   mapper,
		options:  options,
		efsCache: make(map[string]*CachedEFS),
		status:   &ProvisionerStatus{},
	}

	ctx := context.Background()
	result, err := provisioner.GetNamespaceEFS(ctx, "test-namespace")

	if err == nil {
		t.Errorf("Expected error, got none")
	}
	if result != nil {
		t.Errorf("Expected filesystem to be nil when not found")
	}
	if !contains(err.Error(), "no EFS filesystem found") {
		t.Errorf("Expected error message to contain 'no EFS filesystem found', got: %s", err.Error())
	}
}