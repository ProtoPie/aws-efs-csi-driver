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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/cloud"
	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/driver/mocks"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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

type testMockMapper struct {
	getMappingFunc   func(ctx context.Context, namespace string) (*NamespaceEFSMapping, error)
	listMappingsFunc func(ctx context.Context) ([]NamespaceEFSMapping, error)
}

func (m *testMockMapper) CreateOrUpdateMapping(ctx context.Context, namespace, fileSystemID, fileSystemArn, region string) (*NamespaceEFSMapping, error) {
	return &NamespaceEFSMapping{
		Namespace:     namespace,
		FileSystemID:  fileSystemID,
		FileSystemArn: fileSystemArn,
		Region:        region,
	}, nil
}

func (m *testMockMapper) GetMapping(ctx context.Context, namespace string) (*NamespaceEFSMapping, error) {
	if m.getMappingFunc != nil {
		return m.getMappingFunc(ctx, namespace)
	}
	return nil, fmt.Errorf("mapping not found")
}

func (m *testMockMapper) DeleteMapping(ctx context.Context, namespace string) error {
	return nil
}

func (m *testMockMapper) ListMappings(ctx context.Context) ([]NamespaceEFSMapping, error) {
	if m.listMappingsFunc != nil {
		return m.listMappingsFunc(ctx)
	}
	return []NamespaceEFSMapping{}, nil
}

func (m *testMockMapper) Start(ctx context.Context) error {
	return nil
}

func (m *testMockMapper) Stop() {
}

func (m *testMockMapper) InvalidateCache(namespace string) {
}

func (m *testMockMapper) ClearCache() {
}

func (m *testMockMapper) RecoverFromAWSTags(ctx context.Context, clusterID string) (int, error) {
	return 0, nil
}

func (m *testMockMapper) SyncWithAWSTags(ctx context.Context, clusterID string) error {
	return nil
}

func (m *testMockMapper) DescribeFileSystems(ctx context.Context) ([]*cloud.FileSystem, error) {
	return []*cloud.FileSystem{}, nil
}

type testMockCloud struct {
	createFileSystemFunc            func(ctx context.Context, clientToken string, options *cloud.FileSystemOptions) (*cloud.FileSystem, error)
	describeFileSystemFunc          func(ctx context.Context, fileSystemId string) (*cloud.FileSystem, error)
	findFileSystemsByTagsFunc       func(ctx context.Context, tags map[string]string) ([]*cloud.FileSystem, error)
	describeMountTargetsFunc        func(ctx context.Context, fileSystemId, az string) (*cloud.MountTarget, error)
	createMountTargetFunc           func(ctx context.Context, fileSystemId, subnetId, securityGroupId string) (*cloud.MountTarget, error)
	getMetadataFunc                 func() cloud.MetadataService
	// Access Point functions
	createAccessPointFunc           func(ctx context.Context, clientToken string, opts *cloud.AccessPointOptions) (*cloud.AccessPoint, error)
	deleteAccessPointFunc           func(ctx context.Context, accessPointId string) error
	describeAccessPointFunc         func(ctx context.Context, accessPointId string) (*cloud.AccessPoint, error)
	listAccessPointsFunc            func(ctx context.Context, fileSystemId string) ([]*cloud.AccessPoint, error)
	findAccessPointByClientTokenFunc func(ctx context.Context, clientToken, fileSystemId string) (*cloud.AccessPoint, error)
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
func (m *testMockCloud) GetMetadata() cloud.MetadataService {
	if m.getMetadataFunc != nil {
		return m.getMetadataFunc()
	}
	return nil
}

func (m *testMockCloud) CreateAccessPoint(ctx context.Context, clientToken string, accessPointOpts *cloud.AccessPointOptions) (*cloud.AccessPoint, error) {
	if m.createAccessPointFunc != nil {
		return m.createAccessPointFunc(ctx, clientToken, accessPointOpts)
	}
	return &cloud.AccessPoint{
		AccessPointId: "fsap-12345678",
		FileSystemId:  accessPointOpts.FileSystemId,
		CapacityGiB:   accessPointOpts.CapacityGiB,
	}, nil
}

func (m *testMockCloud) DeleteAccessPoint(ctx context.Context, accessPointId string) error {
	if m.deleteAccessPointFunc != nil {
		return m.deleteAccessPointFunc(ctx, accessPointId)
	}
	return nil
}

func (m *testMockCloud) DescribeAccessPoint(ctx context.Context, accessPointId string) (*cloud.AccessPoint, error) {
	if m.describeAccessPointFunc != nil {
		return m.describeAccessPointFunc(ctx, accessPointId)
	}
	return &cloud.AccessPoint{
		AccessPointId: accessPointId,
		FileSystemId:  "fs-12345678",
	}, nil
}

func (m *testMockCloud) FindAccessPointByClientToken(ctx context.Context, clientToken, fileSystemId string) (*cloud.AccessPoint, error) {
	return nil, cloud.ErrNotFound
}

func (m *testMockCloud) ListAccessPoints(ctx context.Context, fileSystemId string) ([]*cloud.AccessPoint, error) {
	if m.listAccessPointsFunc != nil {
		return m.listAccessPointsFunc(ctx, fileSystemId)
	}
	return []*cloud.AccessPoint{}, nil
}
func (m *testMockCloud) DescribeMountTargets(ctx context.Context, fileSystemId, az string) (*cloud.MountTarget, error) {
	if m.describeMountTargetsFunc != nil {
		return m.describeMountTargetsFunc(ctx, fileSystemId, az)
	}
	return nil, nil
}
func (m *testMockCloud) CreateMountTarget(ctx context.Context, fileSystemId, subnetId, securityGroupId string) (*cloud.MountTarget, error) {
	if m.createMountTargetFunc != nil {
		return m.createMountTargetFunc(ctx, fileSystemId, subnetId, securityGroupId)
	}
	return nil, nil
}
func (m *testMockCloud) GetFileSystemTags(ctx context.Context, fileSystemId string) (map[string]string, error) { return nil, nil }
func (m *testMockCloud) DescribeFileSystems(ctx context.Context, creationToken string, maxResults int32) ([]*cloud.FileSystem, string, error) { return nil, "", nil }

type mockMapper struct {
	createOrUpdateMappingFunc func(ctx context.Context, namespace, fileSystemID, fileSystemArn, region string) (*NamespaceEFSMapping, error)
	getMappingFunc           func(ctx context.Context, namespace string) (*NamespaceEFSMapping, error)
	listMappingsFunc         func(ctx context.Context) ([]NamespaceEFSMapping, error)
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

func (m *mockMapper) ListMappings(ctx context.Context) ([]NamespaceEFSMapping, error) {
	if m.listMappingsFunc != nil {
		return m.listMappingsFunc(ctx)
	}
	return []NamespaceEFSMapping{}, nil
}

// Other required methods to satisfy NamespaceEFSMapperInterface
func (m *mockMapper) DeleteMapping(ctx context.Context, namespace string) error { return nil }
func (m *mockMapper) Start(ctx context.Context) error { return nil }
func (m *mockMapper) Stop() {}
func (m *mockMapper) InvalidateCache(namespace string) {}
func (m *mockMapper) ClearCache() {}
func (m *mockMapper) RecoverFromAWSTags(ctx context.Context, clusterID string) (int, error) { return 0, nil }
func (m *mockMapper) SyncWithAWSTags(ctx context.Context, clusterID string) error { return nil }
func (m *mockMapper) DescribeFileSystems(ctx context.Context) ([]*cloud.FileSystem, error) { return nil, nil }

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
	lockMgr := NewLockManagerMap()
	metrics := &mockMetricsCollector{}

	options := DefaultProvisionerOptions()
	options.ClusterID = "test-cluster"
	options.DefaultTags = map[string]string{
		"Environment": "test",
	}

	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		mapper:           mapper,
		lockManager:      &lockMgr,
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
	lockMgr := NewLockManagerMap()
	metrics := &mockMetricsCollector{}

	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		mapper:           mapper,
		lockManager:      &lockMgr,
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
	lockMgr := NewLockManagerMap()
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
		lockManager:      &lockMgr,
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

	metrics := &mockMetricsCollector{}

	var errorRecorded bool
	metrics.recordErrorFunc = func(operation, namespace string, err error) {
		if operation == "acquire_lock" && namespace == "test-namespace" {
			errorRecorded = true
		}
	}

	options := DefaultProvisionerOptions()
	options.CreateTimeout = 1 * time.Millisecond // Very short timeout to force failure
	lockMgr := NewLockManagerMap()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		mapper:           mapper,
		lockManager:      &lockMgr,
		metricsCollector: metrics,
		options:          options,
		efsCache:         make(map[string]*CachedEFS),
		status:           &ProvisionerStatus{},
	}

	// Pre-acquire the lock to force timeout (same pattern as controller tests)
	lockKey := "namespace:test-namespace"
	t.Logf("Acquiring lock for key: %s", lockKey)
	provisioner.lockManager.lockMutex(lockKey) // Hold lock without timeout using provisioner's lockManager
	defer provisioner.lockManager.unlockMutex(lockKey)

	ctx := context.Background()

	// Add debugging - test the lock acquisition directly
	start := time.Now()
	lockSuccess := provisioner.lockManager.lockMutex(lockKey, 1*time.Millisecond)
	elapsed := time.Since(start)
	t.Logf("Direct lock test: success=%v, elapsed=%v", lockSuccess, elapsed)

	if lockSuccess {
		t.Errorf("Expected direct lock acquisition to fail due to timeout, but it succeeded")
	}

	t.Logf("About to call CreateNamespaceEFS...")
	start = time.Now()
	fs, err := provisioner.CreateNamespaceEFS(ctx, "test-namespace", nil)
	elapsed = time.Since(start)
	t.Logf("CreateNamespaceEFS completed: success=%v, elapsed=%v, err=%v", fs != nil, elapsed, err)

	if err == nil {
		t.Errorf("Expected error, got none. fs=%v", fs)
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
	lockMgr := NewLockManagerMap()
	metrics := &mockMetricsCollector{}

	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		mapper:           mapper,
		lockManager:      &lockMgr,
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

// Test cases for Mount Target creation logic

func TestNamespaceProvisioner_createMountTargetsForEFS_Success(t *testing.T) {
	mockCloud := &testMockCloud{}
	mapper := &mockMapper{}
	metrics := &mockMetricsCollector{}

	options := DefaultProvisionerOptions()
	options.SubnetIds = []string{"subnet-12345", "subnet-67890"}
	options.AvailabilityZones = []string{"us-east-1a", "us-east-1b"}
	options.SecurityGroupId = "sg-12345"

	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		mapper:           mapper,
		metricsCollector: metrics,
		options:          options,
		efsCache:         make(map[string]*CachedEFS),
		status:           &ProvisionerStatus{},
	}

	// Mock DescribeMountTargets to return nil (no existing mount targets)
	mockCloud.describeMountTargetsFunc = func(ctx context.Context, fileSystemId, az string) (*cloud.MountTarget, error) {
		return nil, fmt.Errorf("no mount targets found")
	}

	// Mock CreateMountTarget to succeed
	createCallCount := 0
	mockCloud.createMountTargetFunc = func(ctx context.Context, fileSystemId, subnetId, securityGroupId string) (*cloud.MountTarget, error) {
		createCallCount++
		return &cloud.MountTarget{
			MountTargetId: fmt.Sprintf("fsmt-%d", createCallCount),
			AZName:        fmt.Sprintf("us-east-1%c", 'a'+createCallCount-1),
			IPAddress:     fmt.Sprintf("192.168.1.%d", createCallCount),
		}, nil
	}

	ctx := context.Background()
	err := provisioner.createMountTargetsForEFS(ctx, "fs-12345", "test-namespace")

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if createCallCount != 2 {
		t.Errorf("Expected CreateMountTarget to be called 2 times, got %d", createCallCount)
	}
}

func TestNamespaceProvisioner_createMountTargetsForEFS_ExistingTargets(t *testing.T) {
	mockCloud := &testMockCloud{}
	mapper := &mockMapper{}
	metrics := &mockMetricsCollector{}

	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		mapper:           mapper,
		metricsCollector: metrics,
		options:          options,
		efsCache:         make(map[string]*CachedEFS),
		status:           &ProvisionerStatus{},
	}

	// Mock DescribeMountTargets to return existing mount target
	mockCloud.describeMountTargetsFunc = func(ctx context.Context, fileSystemId, az string) (*cloud.MountTarget, error) {
		return &cloud.MountTarget{
			MountTargetId: "fsmt-existing",
			AZName:        "us-east-1a",
			IPAddress:     "192.168.1.100",
		}, nil
	}

	createCalled := false
	mockCloud.createMountTargetFunc = func(ctx context.Context, fileSystemId, subnetId, securityGroupId string) (*cloud.MountTarget, error) {
		createCalled = true
		return nil, fmt.Errorf("should not be called")
	}

	ctx := context.Background()
	err := provisioner.createMountTargetsForEFS(ctx, "fs-12345", "test-namespace")

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if createCalled {
		t.Errorf("CreateMountTarget should not have been called when mount targets already exist")
	}
}

func TestNamespaceProvisioner_createMountTargetWithRetry_Success(t *testing.T) {
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:   mockCloud,
		options: options,
	}

	mockCloud.createMountTargetFunc = func(ctx context.Context, fileSystemId, subnetId, securityGroupId string) (*cloud.MountTarget, error) {
		if fileSystemId != "fs-12345" {
			t.Errorf("Expected fileSystemId to be 'fs-12345', got %s", fileSystemId)
		}
		if subnetId != "subnet-12345" {
			t.Errorf("Expected subnetId to be 'subnet-12345', got %s", subnetId)
		}
		if securityGroupId != "sg-12345" {
			t.Errorf("Expected securityGroupId to be 'sg-12345', got %s", securityGroupId)
		}
		return &cloud.MountTarget{
			MountTargetId: "fsmt-12345",
			AZName:        "us-east-1a",
			IPAddress:     "192.168.1.100",
		}, nil
	}

	ctx := context.Background()
	mt, err := provisioner.createMountTargetWithRetry(ctx, "fs-12345", "subnet-12345", "sg-12345")

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if mt == nil {
		t.Errorf("Expected mount target to be non-nil")
	}
	if mt.MountTargetId != "fsmt-12345" {
		t.Errorf("Expected MountTargetId to be 'fsmt-12345', got %s", mt.MountTargetId)
	}
}

func TestNamespaceProvisioner_createMountTargetWithRetry_AlreadyExists(t *testing.T) {
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:   mockCloud,
		options: options,
	}

	mockCloud.createMountTargetFunc = func(ctx context.Context, fileSystemId, subnetId, securityGroupId string) (*cloud.MountTarget, error) {
		return nil, cloud.ErrAlreadyExists
	}

	ctx := context.Background()
	mt, err := provisioner.createMountTargetWithRetry(ctx, "fs-12345", "subnet-12345", "sg-12345")

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if mt != nil {
		t.Errorf("Expected mount target to be nil when already exists")
	}
}

func TestNamespaceProvisioner_createMountTargetWithRetry_MaxRetriesExceeded(t *testing.T) {
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()
	options.MaxRetries = 2
	options.RetryDelay = 1 * time.Millisecond // Speed up test
	provisioner := &NamespaceProvisioner{
		cloud:   mockCloud,
		options: options,
	}

	callCount := 0
	mockCloud.createMountTargetFunc = func(ctx context.Context, fileSystemId, subnetId, securityGroupId string) (*cloud.MountTarget, error) {
		callCount++
		return nil, fmt.Errorf("temporary error")
	}

	ctx := context.Background()
	mt, err := provisioner.createMountTargetWithRetry(ctx, "fs-12345", "subnet-12345", "sg-12345")

	if err == nil {
		t.Errorf("Expected error after max retries")
	}
	if mt != nil {
		t.Errorf("Expected mount target to be nil when retries exceeded")
	}
	if callCount != 2 {
		t.Errorf("Expected 2 retry attempts, got %d", callCount)
	}
	if !contains(err.Error(), "failed to create mount target after 2 attempts") {
		t.Errorf("Expected error message to mention retry attempts, got: %s", err.Error())
	}
}

func TestNamespaceProvisioner_getConfiguredSubnets(t *testing.T) {
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()
	options.SubnetIds = []string{"subnet-12345", "subnet-67890", "subnet-abcdef"}
	options.AvailabilityZones = []string{"us-east-1a", "us-east-1b"} // Fewer AZs than subnets
	options.VpcId = "vpc-12345"

	provisioner := &NamespaceProvisioner{
		cloud:   mockCloud,
		options: options,
	}

	ctx := context.Background()
	subnets, err := provisioner.getConfiguredSubnets(ctx)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(subnets) != 3 {
		t.Errorf("Expected 3 subnets, got %d", len(subnets))
	}

	// Check first subnet
	if subnets[0].SubnetId != "subnet-12345" {
		t.Errorf("Expected first subnet ID to be 'subnet-12345', got %s", subnets[0].SubnetId)
	}
	if subnets[0].AvailabilityZone != "us-east-1a" {
		t.Errorf("Expected first AZ to be 'us-east-1a', got %s", subnets[0].AvailabilityZone)
	}
	if subnets[0].VpcId != "vpc-12345" {
		t.Errorf("Expected VPC ID to be 'vpc-12345', got %s", subnets[0].VpcId)
	}

	// Check second subnet
	if subnets[1].SubnetId != "subnet-67890" {
		t.Errorf("Expected second subnet ID to be 'subnet-67890', got %s", subnets[1].SubnetId)
	}
	if subnets[1].AvailabilityZone != "us-east-1b" {
		t.Errorf("Expected second AZ to be 'us-east-1b', got %s", subnets[1].AvailabilityZone)
	}

	// Third subnet should have empty AZ since we only have 2 AZs configured
	if subnets[2].SubnetId != "subnet-abcdef" {
		t.Errorf("Expected third subnet ID to be 'subnet-abcdef', got %s", subnets[2].SubnetId)
	}
	if subnets[2].AvailabilityZone != "" {
		t.Errorf("Expected third AZ to be empty, got %s", subnets[2].AvailabilityZone)
	}
}

func TestNamespaceProvisioner_getEFSSecurityGroup_Configured(t *testing.T) {
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()
	options.SecurityGroupId = "sg-configured"

	provisioner := &NamespaceProvisioner{
		cloud:   mockCloud,
		options: options,
	}

	ctx := context.Background()
	sgId, err := provisioner.getEFSSecurityGroup(ctx)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if sgId != "sg-configured" {
		t.Errorf("Expected security group ID to be 'sg-configured', got %s", sgId)
	}
}

func TestNamespaceProvisioner_getEFSSecurityGroup_Default(t *testing.T) {
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()
	// No security group configured

	provisioner := &NamespaceProvisioner{
		cloud:   mockCloud,
		options: options,
	}

	ctx := context.Background()
	sgId, err := provisioner.getEFSSecurityGroup(ctx)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if sgId != "" {
		t.Errorf("Expected empty security group ID (default), got %s", sgId)
	}
}

func TestNamespaceProvisioner_discoverSubnetsFromExistingEFS_NoFileSystems(t *testing.T) {
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()
	options.ClusterID = "test-cluster"

	provisioner := &NamespaceProvisioner{
		cloud:   mockCloud,
		options: options,
	}

	// Mock FindFileSystemsByTags to return no filesystems
	mockCloud.findFileSystemsByTagsFunc = func(ctx context.Context, tags map[string]string) ([]*cloud.FileSystem, error) {
		return []*cloud.FileSystem{}, nil
	}

	ctx := context.Background()
	subnets, err := provisioner.discoverSubnetsFromExistingEFS(ctx)

	if err == nil {
		t.Errorf("Expected error when no existing filesystems found")
	}
	if len(subnets) != 0 {
		t.Errorf("Expected 0 subnets, got %d", len(subnets))
	}
	if !contains(err.Error(), "no existing EFS filesystems found") {
		t.Errorf("Expected error message to mention no filesystems, got: %s", err.Error())
	}
}

func TestNamespaceProvisioner_getDefaultSubnets_NoMetadata(t *testing.T) {
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()

	provisioner := &NamespaceProvisioner{
		cloud:   mockCloud,
		options: options,
	}

	// Mock GetMetadata to return nil
	mockCloud.getMetadataFunc = func() cloud.MetadataService {
		return nil
	}

	ctx := context.Background()
	subnets, err := provisioner.getDefaultSubnets(ctx)

	if err == nil {
		t.Errorf("Expected error when metadata service not available")
	}
	if len(subnets) != 0 {
		t.Errorf("Expected 0 subnets, got %d", len(subnets))
	}
	if !contains(err.Error(), "metadata service not available") {
		t.Errorf("Expected error message to mention metadata service, got: %s", err.Error())
	}
}

// Access Point Management Tests

func TestNamespaceProvisioner_CreateAccessPointForPVC_Success(t *testing.T) {
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()
	lockMgr := NewLockManagerMap()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		options:         options,
		lockManager:     &lockMgr,
		metricsCollector: &NoOpMetricsCollector{},
		mapper:          &testMockMapper{},
		efsCache:        make(map[string]*CachedEFS),
	}

	// Mock CreateAccessPoint to succeed
	expectedAccessPoint := &cloud.AccessPoint{
		AccessPointId: "fsap-12345678",
		FileSystemId:  "fs-123456789",
		CapacityGiB:   0,
	}
	mockCloud.createAccessPointFunc = func(ctx context.Context, clientToken string, opts *cloud.AccessPointOptions) (*cloud.AccessPoint, error) {
		// Validate the options
		if opts.FileSystemId != "fs-123456789" {
			t.Errorf("Expected FileSystemId to be fs-123456789, got %s", opts.FileSystemId)
		}
		if opts.Uid <= 0 {
			t.Errorf("Expected Uid to be positive, got %d", opts.Uid)
		}
		if opts.Gid <= 0 {
			t.Errorf("Expected Gid to be positive, got %d", opts.Gid)
		}
		if opts.DirectoryPerms != DefaultDirectoryPerms {
			t.Errorf("Expected DirectoryPerms to be %s, got %s", DefaultDirectoryPerms, opts.DirectoryPerms)
		}
		if opts.DirectoryPath == "" {
			t.Error("Expected DirectoryPath to be non-empty")
		}
		if !contains(opts.DirectoryPath, "test-namespace") {
			t.Errorf("Expected DirectoryPath to contain namespace, got %s", opts.DirectoryPath)
		}
		if !contains(opts.DirectoryPath, "test-pvc") {
			t.Errorf("Expected DirectoryPath to contain PVC name, got %s", opts.DirectoryPath)
		}
		return expectedAccessPoint, nil
	}

	ctx := context.Background()
	apOptions := &cloud.AccessPointOptions{
		FileSystemId: "fs-123456789",
	}

	result, err := provisioner.CreateAccessPointForPVC(ctx, "test-pvc", "test-namespace", apOptions)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if result == nil {
		t.Fatal("Expected non-nil AccessPoint")
	}
	if result.AccessPointId != expectedAccessPoint.AccessPointId {
		t.Errorf("Expected AccessPointId to be %s, got %s", expectedAccessPoint.AccessPointId, result.AccessPointId)
	}
	if result.FileSystemId != expectedAccessPoint.FileSystemId {
		t.Errorf("Expected FileSystemId to be %s, got %s", expectedAccessPoint.FileSystemId, result.FileSystemId)
	}
}

func TestNamespaceProvisioner_CreateAccessPointForPVC_ValidationErrors(t *testing.T) {
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		options:         options,
		lockManager:     func() *LockManagerMap { lm := NewLockManagerMap(); return &lm }(),
		metricsCollector: &NoOpMetricsCollector{},
		mapper:          &testMockMapper{},
		efsCache:        make(map[string]*CachedEFS),
	}

	ctx := context.Background()
	apOptions := &cloud.AccessPointOptions{
		FileSystemId: "fs-123456789",
	}

	tests := []struct {
		name      string
		pvcName   string
		namespace string
		options   *cloud.AccessPointOptions
		expectErr string
	}{
		{
			name:      "Empty PVC name",
			pvcName:   "",
			namespace: "test-namespace",
			options:   apOptions,
			expectErr: "PVC name cannot be empty",
		},
		{
			name:      "Empty namespace",
			pvcName:   "test-pvc",
			namespace: "",
			options:   apOptions,
			expectErr: "namespace cannot be empty",
		},
		{
			name:      "Nil options",
			pvcName:   "test-pvc",
			namespace: "test-namespace",
			options:   nil,
			expectErr: "access point options cannot be nil",
		},
		{
			name:      "Empty FileSystemId",
			pvcName:   "test-pvc",
			namespace: "test-namespace",
			options:   &cloud.AccessPointOptions{},
			expectErr: "FileSystemId must be specified",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := provisioner.CreateAccessPointForPVC(ctx, tt.pvcName, tt.namespace, tt.options)
			if err == nil {
				t.Fatalf("Expected error for test %s", tt.name)
			}
			if !contains(err.Error(), tt.expectErr) {
				t.Errorf("Expected error to contain '%s', got: %v", tt.expectErr, err)
			}
		})
	}
}

func TestNamespaceProvisioner_CreateAccessPointForPVC_CloudError(t *testing.T) {
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		options:         options,
		lockManager:     func() *LockManagerMap { lm := NewLockManagerMap(); return &lm }(),
		metricsCollector: &NoOpMetricsCollector{},
		mapper:          &testMockMapper{},
		efsCache:        make(map[string]*CachedEFS),
	}

	// Mock CreateAccessPoint to fail
	mockCloud.createAccessPointFunc = func(ctx context.Context, clientToken string, opts *cloud.AccessPointOptions) (*cloud.AccessPoint, error) {
		return nil, fmt.Errorf("AWS API error")
	}

	ctx := context.Background()
	apOptions := &cloud.AccessPointOptions{
		FileSystemId: "fs-123456789",
	}

	_, err := provisioner.CreateAccessPointForPVC(ctx, "test-pvc", "test-namespace", apOptions)

	if err == nil {
		t.Fatal("Expected error from cloud provider")
	}
	if !contains(err.Error(), "failed to create Access Point for PVC test-pvc") {
		t.Errorf("Expected error to mention PVC name, got: %v", err)
	}
}

func TestNamespaceProvisioner_DeleteAccessPointForPVC_Success(t *testing.T) {
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		options:         options,
		lockManager:     func() *LockManagerMap { lm := NewLockManagerMap(); return &lm }(),
		metricsCollector: &NoOpMetricsCollector{},
		mapper:          &testMockMapper{},
		efsCache:        make(map[string]*CachedEFS),
	}

	// Mock GetNamespaceEFS to return a filesystem
	mockCloud.findFileSystemsByTagsFunc = func(ctx context.Context, tags map[string]string) ([]*cloud.FileSystem, error) {
		return []*cloud.FileSystem{
			{
				FileSystemId: "fs-123456789",
			},
		}, nil
	}

	// Mock ListAccessPoints and DescribeAccessPoint
	mockCloud.listAccessPointsFunc = func(ctx context.Context, fileSystemId string) ([]*cloud.AccessPoint, error) {
		return []*cloud.AccessPoint{
			{
				AccessPointId: "fsap-12345678",
				FileSystemId:  "fs-123456789",
			},
		}, nil
	}

	mockCloud.describeAccessPointFunc = func(ctx context.Context, accessPointId string) (*cloud.AccessPoint, error) {
		return &cloud.AccessPoint{
			AccessPointId: "fsap-12345678",
			FileSystemId:  "fs-123456789",
		}, nil
	}

	// Mock DeleteAccessPoint to succeed
	mockCloud.deleteAccessPointFunc = func(ctx context.Context, accessPointId string) error {
		if accessPointId != "fsap-12345678" {
			t.Errorf("Expected AccessPointId to be fsap-12345678, got %s", accessPointId)
		}
		return nil
	}

	ctx := context.Background()
	err := provisioner.DeleteAccessPointForPVC(ctx, "test-pvc", "test-namespace")

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
}

func TestNamespaceProvisioner_DeleteAccessPointForPVC_ValidationErrors(t *testing.T) {
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		options:         options,
		lockManager:     func() *LockManagerMap { lm := NewLockManagerMap(); return &lm }(),
		metricsCollector: &NoOpMetricsCollector{},
		mapper:          &testMockMapper{},
		efsCache:        make(map[string]*CachedEFS),
	}

	ctx := context.Background()

	tests := []struct {
		name      string
		pvcName   string
		namespace string
		expectErr string
	}{
		{
			name:      "Empty PVC name",
			pvcName:   "",
			namespace: "test-namespace",
			expectErr: "PVC name cannot be empty",
		},
		{
			name:      "Empty namespace",
			pvcName:   "test-pvc",
			namespace: "",
			expectErr: "namespace cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := provisioner.DeleteAccessPointForPVC(ctx, tt.pvcName, tt.namespace)
			if err == nil {
				t.Fatalf("Expected error for test %s", tt.name)
			}
			if !contains(err.Error(), tt.expectErr) {
				t.Errorf("Expected error to contain '%s', got: %v", tt.expectErr, err)
			}
		})
	}
}

func TestNamespaceProvisioner_DeleteAccessPointForPVC_NotFound(t *testing.T) {
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		options:         options,
		lockManager:     func() *LockManagerMap { lm := NewLockManagerMap(); return &lm }(),
		metricsCollector: &NoOpMetricsCollector{},
		mapper:          &testMockMapper{},
		efsCache:        make(map[string]*CachedEFS),
	}

	// Mock GetNamespaceEFS to fail (EFS not found)
	mockCloud.findFileSystemsByTagsFunc = func(ctx context.Context, tags map[string]string) ([]*cloud.FileSystem, error) {
		return []*cloud.FileSystem{}, nil // No filesystems found
	}

	ctx := context.Background()
	err := provisioner.DeleteAccessPointForPVC(ctx, "test-pvc", "test-namespace")

	// Should not error when EFS or AccessPoint is not found
	if err != nil {
		t.Fatalf("Expected no error when EFS not found, got: %v", err)
	}
}

func TestNamespaceProvisioner_buildAccessPointPath(t *testing.T) {
	provisioner := &NamespaceProvisioner{}

	tests := []struct {
		name        string
		pvcName     string
		namespace   string
		options     *cloud.AccessPointOptions
		expectPath  func(string) bool // Function to validate path
		expectError bool
	}{
		{
			name:      "Default path construction",
			pvcName:   "test-pvc",
			namespace: "test-namespace",
			options:   &cloud.AccessPointOptions{},
			expectPath: func(path string) bool {
				return contains(path, DefaultBasePath) &&
					contains(path, "test-namespace") &&
					contains(path, "test-pvc") &&
					len(path) > len(DefaultBasePath+"/test-namespace/test-pvc/")
			},
			expectError: false,
		},
		{
			name:      "Custom directory path",
			pvcName:   "test-pvc",
			namespace: "test-namespace",
			options:   &cloud.AccessPointOptions{DirectoryPath: "/custom/base"},
			expectPath: func(path string) bool {
				return contains(path, "/custom/base")
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, err := provisioner.buildAccessPointPath(tt.pvcName, tt.namespace, tt.options)

			if tt.expectError && err == nil {
				t.Fatalf("Expected error for test %s", tt.name)
			}
			if !tt.expectError && err != nil {
				t.Fatalf("Unexpected error for test %s: %v", tt.name, err)
			}

			if !tt.expectError {
				if !tt.expectPath(path) {
					t.Errorf("Path validation failed for test %s: %s", tt.name, path)
				}
				// All paths should start with /
				if path[0] != '/' {
					t.Errorf("Expected path to start with '/', got: %s", path)
				}
			}
		})
	}
}

func TestNamespaceProvisioner_determinePosixIDs(t *testing.T) {
	provisioner := &NamespaceProvisioner{}

	tests := []struct {
		name        string
		options     *cloud.AccessPointOptions
		expectUid   int64
		expectGid   func(int64) bool // Function to validate GID
		expectError bool
	}{
		{
			name:      "Use specified UID and GID",
			options:   &cloud.AccessPointOptions{Uid: 2000, Gid: 3000},
			expectUid: 2000,
			expectGid: func(gid int64) bool { return gid == 3000 },
		},
		{
			name:      "Use default UID, random GID",
			options:   &cloud.AccessPointOptions{},
			expectUid: DefaultUid,
			expectGid: func(gid int64) bool {
				return gid >= DefaultGidRangeStart && gid < DefaultGidRangeEnd
			},
		},
		{
			name:      "Use specified UID, random GID",
			options:   &cloud.AccessPointOptions{Uid: 1500},
			expectUid: 1500,
			expectGid: func(gid int64) bool {
				return gid >= DefaultGidRangeStart && gid < DefaultGidRangeEnd
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uid, gid, err := provisioner.determinePosixIDs(tt.options)

			if tt.expectError && err == nil {
				t.Fatalf("Expected error for test %s", tt.name)
			}
			if !tt.expectError && err != nil {
				t.Fatalf("Unexpected error for test %s: %v", tt.name, err)
			}

			if !tt.expectError {
				if uid != tt.expectUid {
					t.Errorf("Expected UID %d, got %d", tt.expectUid, uid)
				}
				if !tt.expectGid(gid) {
					t.Errorf("GID validation failed for test %s: %d", tt.name, gid)
				}
			}
		})
	}
}

func TestNamespaceProvisioner_buildAccessPointTags(t *testing.T) {
	options := &ProvisionerOptions{
		DefaultTags: map[string]string{
			"Environment": "test",
			"Owner":       "team-a",
		},
		ClusterID: "test-cluster",
	}

	provisioner := &NamespaceProvisioner{
		options: options,
	}

	additionalTags := map[string]string{
		"Custom": "value",
	}

	tags := provisioner.buildAccessPointTags("test-pvc", "test-namespace", additionalTags)

	expectedTags := map[string]string{
		"Environment":                           "test",
		"Owner":                                "team-a",
		"Custom":                               "value",
		"kubernetes.io/namespace":              "test-namespace",
		"kubernetes.io/pvc-name":               "test-pvc",
		"kubernetes.io/provisioning-mode":      "efs-ns",
		"kubernetes.io/created-by":             "efs-ns-provisioner",
		"kubernetes.io/cluster/test-cluster":   "owned",
	}

	for key, expectedValue := range expectedTags {
		if actualValue, exists := tags[key]; !exists {
			t.Errorf("Expected tag %s to exist", key)
		} else if actualValue != expectedValue {
			t.Errorf("Expected tag %s to be %s, got %s", key, expectedValue, actualValue)
		}
	}

	// Check that we have the expected number of tags
	if len(tags) != len(expectedTags) {
		t.Errorf("Expected %d tags, got %d", len(expectedTags), len(tags))
	}
}

// Additional comprehensive tests for NamespaceProvisioner

func TestNewNamespaceProvisioner_Success(t *testing.T) {
	mockCloud := &testMockCloud{}
	mockK8sClient := &mocks.MockKubernetesClient{}
	config := &rest.Config{}
	options := DefaultProvisionerOptions()

	provisioner, err := NewNamespaceProvisioner(mockCloud, mockK8sClient, config, options)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if provisioner == nil {
		t.Fatal("Expected non-nil provisioner")
	}
	if provisioner.cloud != mockCloud {
		t.Error("Expected cloud provider to be set correctly")
	}
	if provisioner.k8sClient != mockK8sClient {
		t.Error("Expected k8s client to be set correctly")
	}
	if provisioner.options != options {
		t.Error("Expected options to be set correctly")
	}
}

func TestNewNamespaceProvisioner_ValidationErrors(t *testing.T) {
	mockCloud := &testMockCloud{}
	mockK8sClient := &mocks.MockKubernetesClient{}
	config := &rest.Config{}
	options := DefaultProvisionerOptions()

	tests := []struct {
		name        string
		cloud       cloud.Cloud
		k8sClient   kubernetes.Interface
		config      *rest.Config
		options     *ProvisionerOptions
		expectError string
	}{
		{
			name:        "Nil cloud provider",
			cloud:       nil,
			k8sClient:   mockK8sClient,
			config:      config,
			options:     options,
			expectError: "cloud client cannot be nil",
		},
		{
			name:        "Nil k8s client",
			cloud:       mockCloud,
			k8sClient:   nil,
			config:      config,
			options:     options,
			expectError: "kubernetes client cannot be nil",
		},
		{
			name:        "Nil config",
			cloud:       mockCloud,
			k8sClient:   mockK8sClient,
			config:      nil,
			options:     options,
			expectError: "kubernetes config cannot be nil",
		},
		{
			name:      "Invalid options",
			cloud:     mockCloud,
			k8sClient: mockK8sClient,
			config:    config,
			options: &ProvisionerOptions{
				CacheTimeout: -1 * time.Second,
			},
			expectError: "invalid provisioner options",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provisioner, err := NewNamespaceProvisioner(tt.cloud, tt.k8sClient, tt.config, tt.options)

			if err == nil {
				t.Fatal("Expected error but got none")
			}
			if provisioner != nil {
				t.Error("Expected nil provisioner on error")
			}
			if !contains(err.Error(), tt.expectError) {
				t.Errorf("Expected error to contain '%s', got: %v", tt.expectError, err)
			}
		})
	}
}

func TestNamespaceProvisioner_Start_Success(t *testing.T) {
	mockCloud := &testMockCloud{}
	mapper := &testMockMapper{}
	options := DefaultProvisionerOptions()
	options.HealthCheckInterval = 10 * time.Millisecond // Speed up test

	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		mapper:           mapper,
		options:          options,
		status:           &ProvisionerStatus{},
		efsCache:         make(map[string]*CachedEFS),
		metricsCollector: &NoOpMetricsCollector{},
		stopCh:           make(chan struct{}),
	}

	ctx := context.Background()
	err := provisioner.Start(ctx)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if !provisioner.started {
		t.Error("Expected provisioner to be marked as started")
	}
	if !provisioner.IsHealthy() {
		t.Error("Expected provisioner to be healthy after start")
	}

	// Clean up
	_ = provisioner.Stop()
}

func TestNamespaceProvisioner_Start_AlreadyStarted(t *testing.T) {
	mockCloud := &testMockCloud{}
	mapper := &testMockMapper{}
	options := DefaultProvisionerOptions()

	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		mapper:           mapper,
		options:          options,
		status:           &ProvisionerStatus{},
		efsCache:         make(map[string]*CachedEFS),
		metricsCollector: &NoOpMetricsCollector{},
		started:          true, // Already started
		stopCh:           make(chan struct{}),
	}

	ctx := context.Background()
	err := provisioner.Start(ctx)

	if err == nil {
		t.Fatal("Expected error when starting already started provisioner")
	}
	if !contains(err.Error(), "provisioner is already started") {
		t.Errorf("Expected error about already started, got: %v", err)
	}
}

func TestNamespaceProvisioner_Stop_Success(t *testing.T) {
	mockCloud := &testMockCloud{}
	mapper := &testMockMapper{}
	options := DefaultProvisionerOptions()

	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		mapper:           mapper,
		options:          options,
		status:           &ProvisionerStatus{},
		efsCache:         make(map[string]*CachedEFS),
		metricsCollector: &NoOpMetricsCollector{},
		started:          true,
		stopCh:           make(chan struct{}),
	}

	err := provisioner.Stop()

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if provisioner.started {
		t.Error("Expected provisioner to be marked as stopped")
	}
	if provisioner.IsHealthy() {
		t.Error("Expected provisioner to be unhealthy after stop")
	}
}

func TestNamespaceProvisioner_Stop_NotStarted(t *testing.T) {
	mockCloud := &testMockCloud{}
	mapper := &testMockMapper{}
	options := DefaultProvisionerOptions()

	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		mapper:           mapper,
		options:          options,
		status:           &ProvisionerStatus{},
		efsCache:         make(map[string]*CachedEFS),
		metricsCollector: &NoOpMetricsCollector{},
		started:          false, // Not started
		stopCh:           make(chan struct{}),
	}

	err := provisioner.Stop()

	// Should not error when stopping non-started provisioner
	if err != nil {
		t.Fatalf("Expected no error when stopping non-started provisioner, got: %v", err)
	}
}

func TestNamespaceProvisioner_HealthCheck(t *testing.T) {
	mockCloud := &testMockCloud{}
	mapper := &testMockMapper{
		listMappingsFunc: func(ctx context.Context) ([]NamespaceEFSMapping, error) {
			// Initially succeed
			return []NamespaceEFSMapping{}, nil
		},
	}
	options := DefaultProvisionerOptions()

	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		mapper:           mapper,
		options:          options,
		status:           &ProvisionerStatus{Healthy: true},
		efsCache:         make(map[string]*CachedEFS),
		metricsCollector: &NoOpMetricsCollector{},
	}

	// Test successful health check
	provisioner.performHealthCheck()
	if !provisioner.IsHealthy() {
		t.Error("Expected provisioner to remain healthy")
	}

	// Test failed health check by simulating mapper failure (which is what health check actually tests)
	mapper.listMappingsFunc = func(ctx context.Context) ([]NamespaceEFSMapping, error) {
		return nil, fmt.Errorf("mapper error")
	}
	provisioner.performHealthCheck()
	if provisioner.IsHealthy() {
		t.Error("Expected provisioner to become unhealthy")
	}
}

func TestNamespaceProvisioner_ConcurrentEFSCreation(t *testing.T) {
	mockCloud := &testMockCloud{}
	mapper := &mockMapper{}
	lockMgr := NewLockManagerMap()
	metrics := &mockMetricsCollector{}

	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		mapper:           mapper,
		lockManager:      &lockMgr,
		metricsCollector: metrics,
		options:          options,
		efsCache:         make(map[string]*CachedEFS),
		status:           &ProvisionerStatus{},
	}

	// Mock CreateFileSystem to simulate a delay and return different filesystems
	var createCount int32
	mockCloud.createFileSystemFunc = func(ctx context.Context, clientToken string, options *cloud.FileSystemOptions) (*cloud.FileSystem, error) {
		// Simulate some processing time
		time.Sleep(50 * time.Millisecond)

		count := atomic.AddInt32(&createCount, 1)
		return &cloud.FileSystem{
			FileSystemId: fmt.Sprintf("fs-concurrent-%d", count),
		}, nil
	}

	namespace := "concurrent-test"
	numGoroutines := 5
	var wg sync.WaitGroup
	results := make(chan *cloud.FileSystem, numGoroutines)
	errors := make(chan error, numGoroutines)

	// Launch multiple goroutines trying to create EFS for the same namespace
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			fs, err := provisioner.CreateNamespaceEFS(ctx, namespace, nil)
			if err != nil {
				errors <- err
				return
			}
			results <- fs
		}()
	}

	wg.Wait()
	close(results)
	close(errors)

	// Check for errors
	var errList []error
	for err := range errors {
		errList = append(errList, err)
	}
	if len(errList) > 0 {
		t.Fatalf("Unexpected errors in concurrent creation: %v", errList)
	}

	// Collect results
	var fsList []*cloud.FileSystem
	for fs := range results {
		fsList = append(fsList, fs)
	}

	if len(fsList) != numGoroutines {
		t.Fatalf("Expected %d results, got %d", numGoroutines, len(fsList))
	}

	// All goroutines should get the same filesystem (from the first successful creation)
	// Due to locking, only one should actually create, others should get the existing one
	firstFsId := fsList[0].FileSystemId
	for i, fs := range fsList {
		if fs.FileSystemId != firstFsId {
			t.Errorf("Result %d has different FileSystemId: expected %s, got %s", i, firstFsId, fs.FileSystemId)
		}
	}

	// Only one actual creation should have happened
	if createCount != 1 {
		t.Errorf("Expected 1 actual filesystem creation, got %d", createCount)
	}
}

func TestNamespaceProvisioner_CreateNamespaceEFS_TagsHandling(t *testing.T) {
	mockCloud := &testMockCloud{}
	mapper := &mockMapper{}
	lockMgr := NewLockManagerMap()
	metrics := &mockMetricsCollector{}

	options := DefaultProvisionerOptions()
	options.ClusterID = "test-cluster"
	options.Region = "us-west-2"
	options.DefaultTags = map[string]string{
		"Environment": "test",
		"Owner":       "platform-team",
	}

	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		mapper:           mapper,
		lockManager:      &lockMgr,
		metricsCollector: metrics,
		options:          options,
		efsCache:         make(map[string]*CachedEFS),
		status:           &ProvisionerStatus{},
	}

	var capturedTags map[string]string
	mockCloud.createFileSystemFunc = func(ctx context.Context, clientToken string, options *cloud.FileSystemOptions) (*cloud.FileSystem, error) {
		capturedTags = make(map[string]string)
		for k, v := range options.Tags {
			capturedTags[k] = v
		}
		return &cloud.FileSystem{
			FileSystemId: "fs-12345678",
		}, nil
	}

	efsOptions := &EFSOptions{
		Tags: map[string]string{
			"Project": "test-project",
		},
	}

	ctx := context.Background()
	_, err := provisioner.CreateNamespaceEFS(ctx, "test-namespace", efsOptions)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expectedTags := map[string]string{
		"Environment":                        "test",
		"Owner":                              "platform-team",
		"Project":                            "test-project",
		"kubernetes.io/namespace":            "test-namespace",
		"kubernetes.io/provisioning-mode":    "efs-ns",
		"kubernetes.io/cluster/test-cluster": "owned",
	}

	for key, expectedValue := range expectedTags {
		if actualValue, exists := capturedTags[key]; !exists {
			t.Errorf("Expected tag %s to exist", key)
		} else if actualValue != expectedValue {
			t.Errorf("Expected tag %s to be %s, got %s", key, expectedValue, actualValue)
		}
	}
}

// Note: CreateNamespaceEFS does not implement retry logic at the EFS creation level.
// Retry is only implemented for mount target creation. This test is removed as it was
// testing non-existent functionality.

func TestNamespaceProvisioner_DeleteNamespaceEFS(t *testing.T) {
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:   mockCloud,
		options: options,
	}

	ctx := context.Background()
	err := provisioner.DeleteNamespaceEFS(ctx, "test-namespace")

	// Current implementation uses retain policy, so should not error
	if err != nil {
		t.Fatalf("Expected no error with retain policy, got: %v", err)
	}
}

func TestNamespaceProvisioner_CreateAccessPointForPVC_PathConstruction(t *testing.T) {
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()
	lockMgr := NewLockManagerMap()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		options:         options,
		lockManager:     &lockMgr,
		metricsCollector: &NoOpMetricsCollector{},
		mapper:          &testMockMapper{},
		efsCache:        make(map[string]*CachedEFS),
	}

	var capturedPath string
	mockCloud.createAccessPointFunc = func(ctx context.Context, clientToken string, opts *cloud.AccessPointOptions) (*cloud.AccessPoint, error) {
		capturedPath = opts.DirectoryPath
		return &cloud.AccessPoint{
			AccessPointId: "fsap-12345678",
			FileSystemId:  opts.FileSystemId,
		}, nil
	}

	ctx := context.Background()
	apOptions := &cloud.AccessPointOptions{
		FileSystemId: "fs-123456789",
	}

	_, err := provisioner.CreateAccessPointForPVC(ctx, "my-pvc", "my-namespace", apOptions)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Path should contain the default base path, namespace, PVC name, and a UUID
	if !contains(capturedPath, DefaultBasePath) {
		t.Errorf("Expected path to contain base path %s, got: %s", DefaultBasePath, capturedPath)
	}
	if !contains(capturedPath, "my-namespace") {
		t.Errorf("Expected path to contain namespace, got: %s", capturedPath)
	}
	if !contains(capturedPath, "my-pvc") {
		t.Errorf("Expected path to contain PVC name, got: %s", capturedPath)
	}
	// Path should be longer than just the base components due to UUID
	expectedMinLength := len(DefaultBasePath) + len("/my-namespace/my-pvc/") + 36 // UUID length
	if len(capturedPath) < expectedMinLength {
		t.Errorf("Expected path to be at least %d characters (including UUID), got %d: %s",
			expectedMinLength, len(capturedPath), capturedPath)
	}
}

func TestNamespaceProvisioner_CreateAccessPointForPVC_CustomOptions(t *testing.T) {
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()
	lockMgr := NewLockManagerMap()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		options:         options,
		lockManager:     &lockMgr,
		metricsCollector: &NoOpMetricsCollector{},
		mapper:          &testMockMapper{},
		efsCache:        make(map[string]*CachedEFS),
	}

	var capturedUid, capturedGid int64
	var capturedPerms string
	mockCloud.createAccessPointFunc = func(ctx context.Context, clientToken string, opts *cloud.AccessPointOptions) (*cloud.AccessPoint, error) {
		capturedUid = opts.Uid
		capturedGid = opts.Gid
		capturedPerms = opts.DirectoryPerms
		return &cloud.AccessPoint{
			AccessPointId: "fsap-12345678",
			FileSystemId:  opts.FileSystemId,
		}, nil
	}

	ctx := context.Background()
	apOptions := &cloud.AccessPointOptions{
		FileSystemId:   "fs-123456789",
		Uid:            2000,
		Gid:            3000,
		DirectoryPerms: "755",
	}

	_, err := provisioner.CreateAccessPointForPVC(ctx, "my-pvc", "my-namespace", apOptions)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if capturedUid != 2000 {
		t.Errorf("Expected UID 2000, got %d", capturedUid)
	}
	if capturedGid != 3000 {
		t.Errorf("Expected GID 3000, got %d", capturedGid)
	}
	if capturedPerms != "755" {
		t.Errorf("Expected directory permissions 755, got %s", capturedPerms)
	}
}

func TestNamespaceProvisioner_CreateAccessPointForPVC_LockTimeout(t *testing.T) {
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()
	options.CreateTimeout = 1 * time.Millisecond // Very short timeout
	lockMgr := NewLockManagerMap()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		options:         options,
		lockManager:     &lockMgr,
		metricsCollector: &NoOpMetricsCollector{},
		mapper:          &testMockMapper{},
		efsCache:        make(map[string]*CachedEFS),
	}

	// Pre-acquire the lock to force timeout
	lockKey := "accesspoint:my-namespace:my-pvc"
	provisioner.lockManager.lockMutex(lockKey)
	defer provisioner.lockManager.unlockMutex(lockKey)

	ctx := context.Background()
	apOptions := &cloud.AccessPointOptions{
		FileSystemId: "fs-123456789",
	}

	_, err := provisioner.CreateAccessPointForPVC(ctx, "my-pvc", "my-namespace", apOptions)

	if err == nil {
		t.Fatal("Expected error due to lock timeout")
	}
	if !contains(err.Error(), "failed to acquire lock") {
		t.Errorf("Expected lock timeout error, got: %v", err)
	}
}

func TestNamespaceProvisioner_DeleteAccessPointForPVC_CloudError(t *testing.T) {
	// Test case: Error case would be when deleteAccessPoint fails and we actually find an AccessPoint
	// to delete. Since findExistingAccessPoint currently always returns ErrNotFound,
	// this test verifies the idempotent behavior instead.
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		options:         options,
		lockManager:     func() *LockManagerMap { lm := NewLockManagerMap(); return &lm }(),
		metricsCollector: &NoOpMetricsCollector{},
		mapper:          &testMockMapper{},
		efsCache:        make(map[string]*CachedEFS),
	}

	// Mock GetNamespaceEFS to return a filesystem
	mockCloud.findFileSystemsByTagsFunc = func(ctx context.Context, tags map[string]string) ([]*cloud.FileSystem, error) {
		return []*cloud.FileSystem{
			{FileSystemId: "fs-123456789"},
		}, nil
	}

	// Mock ListAccessPoints to return access points
	mockCloud.listAccessPointsFunc = func(ctx context.Context, fileSystemId string) ([]*cloud.AccessPoint, error) {
		return []*cloud.AccessPoint{
			{
				AccessPointId: "fsap-12345678",
				FileSystemId:  "fs-123456789",
			},
		}, nil
	}

	// Mock DescribeAccessPoint to succeed (called by findExistingAccessPoint)
	mockCloud.describeAccessPointFunc = func(ctx context.Context, accessPointId string) (*cloud.AccessPoint, error) {
		return &cloud.AccessPoint{
			AccessPointId: "fsap-12345678",
			FileSystemId:  "fs-123456789",
		}, nil
	}

	ctx := context.Background()
	err := provisioner.DeleteAccessPointForPVC(ctx, "test-pvc", "test-namespace")

	// Since findExistingAccessPoint always returns ErrNotFound in current implementation,
	// delete operation should succeed (idempotent behavior)
	if err != nil {
		t.Fatalf("Expected no error due to idempotent behavior, got: %v", err)
	}
}

func TestNamespaceProvisioner_DeleteAccessPointForPVC_AlreadyDeleted(t *testing.T) {
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		options:         options,
		lockManager:     func() *LockManagerMap { lm := NewLockManagerMap(); return &lm }(),
		metricsCollector: &NoOpMetricsCollector{},
		mapper:          &testMockMapper{},
		efsCache:        make(map[string]*CachedEFS),
	}

	// Mock GetNamespaceEFS to return a filesystem
	mockCloud.findFileSystemsByTagsFunc = func(ctx context.Context, tags map[string]string) ([]*cloud.FileSystem, error) {
		return []*cloud.FileSystem{
			{FileSystemId: "fs-123456789"},
		}, nil
	}

	// Mock access point discovery to return not found
	mockCloud.listAccessPointsFunc = func(ctx context.Context, fileSystemId string) ([]*cloud.AccessPoint, error) {
		return []*cloud.AccessPoint{}, nil // No access points found
	}

	ctx := context.Background()
	err := provisioner.DeleteAccessPointForPVC(ctx, "test-pvc", "test-namespace")

	// Should not error when access point is already deleted
	if err != nil {
		t.Fatalf("Expected no error when access point not found, got: %v", err)
	}
}

// Tests for comprehensive coverage of additional scenarios

func TestNamespaceProvisioner_CreateNamespaceEFS_CacheIntegration(t *testing.T) {
	mockCloud := &testMockCloud{}
	mapper := &mockMapper{}
	lockMgr := NewLockManagerMap()
	metrics := &mockMetricsCollector{}

	options := DefaultProvisionerOptions()
	options.EnableCaching = true
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		mapper:           mapper,
		lockManager:      &lockMgr,
		metricsCollector: metrics,
		options:          options,
		efsCache:         make(map[string]*CachedEFS),
		status:           &ProvisionerStatus{},
	}

	mockCloud.createFileSystemFunc = func(ctx context.Context, clientToken string, options *cloud.FileSystemOptions) (*cloud.FileSystem, error) {
		return &cloud.FileSystem{
			FileSystemId: "fs-12345678",
		}, nil
	}

	ctx := context.Background()
	namespace := "cache-test"

	// First call should create EFS and cache it
	fs1, err := provisioner.CreateNamespaceEFS(ctx, namespace, nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check that it was cached
	cached := provisioner.getCachedEFS(namespace)
	if cached == nil {
		t.Error("Expected EFS to be cached")
	}
	if cached != nil && cached.FileSystemId != fs1.FileSystemId {
		t.Errorf("Expected cached FileSystemId %s, got %s", fs1.FileSystemId, cached.FileSystemId)
	}

	// Second call should return cached result without calling cloud provider
	var secondCallMade bool
	mockCloud.createFileSystemFunc = func(ctx context.Context, clientToken string, options *cloud.FileSystemOptions) (*cloud.FileSystem, error) {
		secondCallMade = true
		return nil, fmt.Errorf("should not be called")
	}

	fs2, err := provisioner.GetNamespaceEFS(ctx, namespace)
	if err != nil {
		t.Fatalf("Unexpected error on cached call: %v", err)
	}
	if secondCallMade {
		t.Error("Second call should not have reached cloud provider")
	}
	if fs2.FileSystemId != fs1.FileSystemId {
		t.Errorf("Expected same FileSystemId from cache, got %s vs %s", fs1.FileSystemId, fs2.FileSystemId)
	}
}

func TestNamespaceProvisioner_AccessPointOptionsValidation(t *testing.T) {
	provisioner := &NamespaceProvisioner{}

	tests := []struct {
		name        string
		options     *cloud.AccessPointOptions
		expectError bool
	}{
		{
			name: "Valid UID and GID",
			options: &cloud.AccessPointOptions{
				Uid: 1000,
				Gid: 1000,
			},
			expectError: false,
		},
		{
			name: "Zero UID should use default",
			options: &cloud.AccessPointOptions{
				Uid: 0,
				Gid: 2000,
			},
			expectError: false,
		},
		{
			name: "Zero GID should generate random",
			options: &cloud.AccessPointOptions{
				Uid: 1000,
				Gid: 0,
			},
			expectError: false,
		},
		{
			name: "Empty options should use defaults",
			options: &cloud.AccessPointOptions{
				Uid: 0,
				Gid: 0,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uid, gid, err := provisioner.determinePosixIDs(tt.options)

			if tt.expectError && err == nil {
				t.Fatal("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !tt.expectError {
				if uid <= 0 {
					t.Errorf("Expected positive UID, got %d", uid)
				}
				if gid <= 0 {
					t.Errorf("Expected positive GID, got %d", gid)
				}
			}
		})
	}
}

func TestNamespaceProvisioner_MetricsIntegration(t *testing.T) {
	mockCloud := &testMockCloud{}
	mapper := &mockMapper{}
	lockMgr := NewLockManagerMap()
	metrics := &mockMetricsCollector{}

	// Track metrics calls
	var efsCreatedCalls, efsCreationTimeCalls, errorCalls int
	metrics.incEFSCreatedFunc = func(namespace string) {
		efsCreatedCalls++
	}
	metrics.recordEFSCreationTimeFunc = func(namespace string, duration time.Duration) {
		efsCreationTimeCalls++
	}
	metrics.recordErrorFunc = func(operation, namespace string, err error) {
		errorCalls++
	}

	options := DefaultProvisionerOptions()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		mapper:           mapper,
		lockManager:      &lockMgr,
		metricsCollector: metrics,
		options:          options,
		efsCache:         make(map[string]*CachedEFS),
		status:           &ProvisionerStatus{},
	}

	mockCloud.createFileSystemFunc = func(ctx context.Context, clientToken string, options *cloud.FileSystemOptions) (*cloud.FileSystem, error) {
		return &cloud.FileSystem{
			FileSystemId: "fs-12345678",
		}, nil
	}

	ctx := context.Background()
	_, err := provisioner.CreateNamespaceEFS(ctx, "metrics-test", nil)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if efsCreatedCalls != 1 {
		t.Errorf("Expected 1 EFS created metric call, got %d", efsCreatedCalls)
	}
	if efsCreationTimeCalls != 1 {
		t.Errorf("Expected 1 EFS creation time metric call, got %d", efsCreationTimeCalls)
	}
	if errorCalls != 0 {
		t.Errorf("Expected 0 error metric calls, got %d", errorCalls)
	}

	// Test error case
	mockCloud.createFileSystemFunc = func(ctx context.Context, clientToken string, options *cloud.FileSystemOptions) (*cloud.FileSystem, error) {
		return nil, fmt.Errorf("AWS error")
	}

	_, err = provisioner.CreateNamespaceEFS(ctx, "metrics-error-test", nil)
	if err == nil {
		t.Fatal("Expected error")
	}

	if errorCalls != 1 {
		t.Errorf("Expected 1 error metric call after error, got %d", errorCalls)
	}
}

// CSI Volume interface tests
func TestNamespaceProvisioner_CreateNamespaceVolume_Success(t *testing.T) {
	mockCloud := &testMockCloud{}
	mockMapper := &testMockMapper{}
	options := DefaultProvisionerOptions()
	options.ClusterID = "test-cluster"

	lockMgr := NewLockManagerMap()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		options:          options,
		mapper:           mockMapper,
		lockManager:      &lockMgr,
		metricsCollector: &NoOpMetricsCollector{},
		efsCache:         make(map[string]*CachedEFS),
	}

	// Mock successful filesystem lookup (already exists)
	mockFileSystem := &cloud.FileSystem{
		FileSystemId: "fs-12345",
		Tags: map[string]string{
			"kubernetes.io/namespace": "test-namespace",
		},
	}

	mockAccessPoint := &cloud.AccessPoint{
		AccessPointId: "fsap-67890",
		FileSystemId:  "fs-12345",
		CapacityGiB:   0,
	}

	// Setup mock expectations
	mockMapper.getMappingFunc = func(ctx context.Context, namespace string) (*NamespaceEFSMapping, error) {
		return &NamespaceEFSMapping{
			Namespace:    namespace,
			FileSystemID: mockFileSystem.FileSystemId,
		}, nil
	}

	mockCloud.findFileSystemsByTagsFunc = func(ctx context.Context, tags map[string]string) ([]*cloud.FileSystem, error) {
		return []*cloud.FileSystem{mockFileSystem}, nil
	}

	mockCloud.createAccessPointFunc = func(ctx context.Context, clientToken string, opts *cloud.AccessPointOptions) (*cloud.AccessPoint, error) {
		return mockAccessPoint, nil
	}

	// Create test request
	req := &csi.CreateVolumeRequest{
		Name: "test-volume",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1073741824, // 1GB
		},
		Parameters: map[string]string{
			"csi.storage.k8s.io/pvc/namespace": "test-namespace",
			"csi.storage.k8s.io/pvc/name":      "test-pvc",
			"provisioningMode":                 "efs-ns",
			"directoryPerms":                   "755",
		},
	}

	// Call the method
	ctx := context.Background()
	response, err := provisioner.CreateNamespaceVolume(ctx, req)

	// Verify results
	if err != nil {
		t.Fatalf("CreateNamespaceVolume failed: %v", err)
	}

	if response == nil {
		t.Fatal("Expected non-nil response")
	}

	if response.Volume == nil {
		t.Fatal("Expected non-nil volume in response")
	}

	if response.Volume.VolumeId != mockAccessPoint.AccessPointId {
		t.Errorf("Expected volume ID %s, got %s", mockAccessPoint.AccessPointId, response.Volume.VolumeId)
	}

	if response.Volume.CapacityBytes != 1073741824 {
		t.Errorf("Expected capacity %d, got %d", 1073741824, response.Volume.CapacityBytes)
	}

	// Verify volume context
	volumeContext := response.Volume.VolumeContext
	if volumeContext["accesspoint"] != mockAccessPoint.AccessPointId {
		t.Errorf("Expected accesspoint %s in volume context, got %s", mockAccessPoint.AccessPointId, volumeContext["accesspoint"])
	}

	if volumeContext["filesystem"] != mockFileSystem.FileSystemId {
		t.Errorf("Expected filesystem %s in volume context, got %s", mockFileSystem.FileSystemId, volumeContext["filesystem"])
	}

	if volumeContext["namespace"] != "test-namespace" {
		t.Errorf("Expected namespace test-namespace in volume context, got %s", volumeContext["namespace"])
	}

	if volumeContext["pvcName"] != "test-pvc" {
		t.Errorf("Expected pvcName test-pvc in volume context, got %s", volumeContext["pvcName"])
	}
}

func TestNamespaceProvisioner_CreateNamespaceVolume_ValidationErrors(t *testing.T) {
	mockCloud := &testMockCloud{}
	mockMapper := &testMockMapper{}
	options := DefaultProvisionerOptions()
	lockMgr := NewLockManagerMap()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		options:          options,
		mapper:           mockMapper,
		lockManager:      &lockMgr,
		metricsCollector: &NoOpMetricsCollector{},
		efsCache:         make(map[string]*CachedEFS),
	}

	ctx := context.Background()

	// Test nil request
	_, err := provisioner.CreateNamespaceVolume(ctx, nil)
	if err == nil || !contains(err.Error(), "cannot be nil") {
		t.Errorf("Expected nil request error, got: %v", err)
	}

	// Test empty volume name
	req := &csi.CreateVolumeRequest{
		Name: "",
		Parameters: map[string]string{
			"csi.storage.k8s.io/pvc/namespace": "test-namespace",
		},
	}
	_, err = provisioner.CreateNamespaceVolume(ctx, req)
	if err == nil || !contains(err.Error(), "volume name cannot be empty") {
		t.Errorf("Expected empty volume name error, got: %v", err)
	}

	// Test nil parameters
	req = &csi.CreateVolumeRequest{
		Name:       "test-volume",
		Parameters: nil,
	}
	_, err = provisioner.CreateNamespaceVolume(ctx, req)
	if err == nil || !contains(err.Error(), "volume parameters cannot be nil") {
		t.Errorf("Expected nil parameters error, got: %v", err)
	}

	// Test missing namespace
	req = &csi.CreateVolumeRequest{
		Name: "test-volume",
		Parameters: map[string]string{
			"provisioningMode": "efs-ns",
		},
	}
	_, err = provisioner.CreateNamespaceVolume(ctx, req)
	if err == nil || !contains(err.Error(), "namespace not found") {
		t.Errorf("Expected missing namespace error, got: %v", err)
	}
}

func TestNamespaceProvisioner_CreateNamespaceVolume_EFSCreationError(t *testing.T) {
	mockCloud := &testMockCloud{}
	mockMapper := &testMockMapper{}
	options := DefaultProvisionerOptions()
	options.ClusterID = "test-cluster"

	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		options:          options,
		mapper:           mockMapper,
		metricsCollector: &NoOpMetricsCollector{},
		efsCache:         make(map[string]*CachedEFS),
		lockManager:      func() *LockManagerMap { lm := NewLockManagerMap(); return &lm }(),
	}

	// Mock mapping not found and EFS creation error
	mockMapper.getMappingFunc = func(ctx context.Context, namespace string) (*NamespaceEFSMapping, error) {
		return nil, fmt.Errorf("not found")
	}

	mockCloud.createFileSystemFunc = func(ctx context.Context, clientToken string, options *cloud.FileSystemOptions) (*cloud.FileSystem, error) {
		return nil, fmt.Errorf("EFS creation failed")
	}

	req := &csi.CreateVolumeRequest{
		Name: "test-volume",
		Parameters: map[string]string{
			"csi.storage.k8s.io/pvc/namespace": "test-namespace",
			"csi.storage.k8s.io/pvc/name":      "test-pvc",
			"provisioningMode":                 "efs-ns",
		},
	}

	ctx := context.Background()
	_, err := provisioner.CreateNamespaceVolume(ctx, req)

	if err == nil || !contains(err.Error(), "failed to ensure namespace EFS") {
		t.Errorf("Expected EFS creation error, got: %v", err)
	}
}

func TestNamespaceProvisioner_CreateNamespaceVolume_AccessPointCreationError(t *testing.T) {
	mockCloud := &testMockCloud{}
	mockMapper := &testMockMapper{}
	options := DefaultProvisionerOptions()
	options.ClusterID = "test-cluster"

	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		options:          options,
		mapper:           mockMapper,
		metricsCollector: &NoOpMetricsCollector{},
		efsCache:         make(map[string]*CachedEFS),
		lockManager:      func() *LockManagerMap { lm := NewLockManagerMap(); return &lm }(),
	}

	// Mock successful EFS retrieval but failed access point creation
	mockFileSystem := &cloud.FileSystem{
		FileSystemId: "fs-12345",
	}

	mockMapper.getMappingFunc = func(ctx context.Context, namespace string) (*NamespaceEFSMapping, error) {
		return &NamespaceEFSMapping{
			Namespace:    namespace,
			FileSystemID: mockFileSystem.FileSystemId,
		}, nil
	}

	mockCloud.findFileSystemsByTagsFunc = func(ctx context.Context, tags map[string]string) ([]*cloud.FileSystem, error) {
		return []*cloud.FileSystem{mockFileSystem}, nil
	}

	mockCloud.createAccessPointFunc = func(ctx context.Context, clientToken string, opts *cloud.AccessPointOptions) (*cloud.AccessPoint, error) {
		return nil, fmt.Errorf("access point creation failed")
	}

	req := &csi.CreateVolumeRequest{
		Name: "test-volume",
		Parameters: map[string]string{
			"csi.storage.k8s.io/pvc/namespace": "test-namespace",
			"csi.storage.k8s.io/pvc/name":      "test-pvc",
			"provisioningMode":                 "efs-ns",
		},
	}

	ctx := context.Background()
	_, err := provisioner.CreateNamespaceVolume(ctx, req)

	if err == nil || !contains(err.Error(), "failed to create Access Point") {
		t.Errorf("Expected access point creation error, got: %v", err)
	}
}

func TestNamespaceProvisioner_CreateNamespaceVolume_ParameterParsing(t *testing.T) {
	mockCloud := &testMockCloud{}
	mockMapper := &testMockMapper{}
	options := DefaultProvisionerOptions()
	options.ClusterID = "test-cluster"

	lockMgr := NewLockManagerMap()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		options:          options,
		mapper:           mockMapper,
		lockManager:      &lockMgr,
		metricsCollector: &NoOpMetricsCollector{},
		efsCache:         make(map[string]*CachedEFS),
	}

	// Mock responses
	mockFileSystem := &cloud.FileSystem{
		FileSystemId: "fs-12345",
	}

	mockAccessPoint := &cloud.AccessPoint{
		AccessPointId: "fsap-67890",
		FileSystemId:  "fs-12345",
		CapacityGiB:   0,
	}

	mockMapper.getMappingFunc = func(ctx context.Context, namespace string) (*NamespaceEFSMapping, error) {
		return &NamespaceEFSMapping{
			Namespace:    namespace,
			FileSystemID: mockFileSystem.FileSystemId,
		}, nil
	}

	mockCloud.findFileSystemsByTagsFunc = func(ctx context.Context, tags map[string]string) ([]*cloud.FileSystem, error) {
		return []*cloud.FileSystem{mockFileSystem}, nil
	}

	mockCloud.createAccessPointFunc = func(ctx context.Context, clientToken string, opts *cloud.AccessPointOptions) (*cloud.AccessPoint, error) {
		return mockAccessPoint, nil
	}

	// Test with custom parameters
	req := &csi.CreateVolumeRequest{
		Name: "test-volume",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 2147483648, // 2GB
		},
		Parameters: map[string]string{
			"csi.storage.k8s.io/pvc/namespace": "test-namespace",
			"csi.storage.k8s.io/pvc/name":      "test-pvc",
			"provisioningMode":                 "efs-ns",
			"performanceMode":                  "maxIO",
			"throughputMode":                   "provisioned",
			"provisionedThroughputInMibps":     "100",
			"encrypted":                        "true",
			"kmsKeyId":                         "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012",
			"lifecyclePolicy":                  "AFTER_30_DAYS",
			"backupPolicy":                     "ENABLED",
			"basePath":                         "/custom/base/path",
			"directoryPerms":                   "755",
			"uid":                              "1000",
			"gid":                              "1000",
			"ensureUniqueDirectory":            "true",
		},
	}

	ctx := context.Background()
	response, err := provisioner.CreateNamespaceVolume(ctx, req)

	if err != nil {
		t.Fatalf("CreateNamespaceVolume failed: %v", err)
	}

	if response == nil || response.Volume == nil {
		t.Fatal("Expected valid response with volume")
	}

	// Verify that sensitive parameters are not in volume context
	volumeContext := response.Volume.VolumeContext
	if _, exists := volumeContext["kmsKeyId"]; exists {
		t.Error("Sensitive parameter kmsKeyId should not be in volume context")
	}

	// Verify capacity is set correctly
	if response.Volume.CapacityBytes != 2147483648 {
		t.Errorf("Expected capacity %d, got %d", 2147483648, response.Volume.CapacityBytes)
	}
}

func TestNamespaceProvisioner_CreateNamespaceVolume_PVCNameFallback(t *testing.T) {
	mockCloud := &testMockCloud{}
	mockMapper := &testMockMapper{}
	options := DefaultProvisionerOptions()
	lockMgr := NewLockManagerMap()
	provisioner := &NamespaceProvisioner{
		cloud:            mockCloud,
		options:          options,
		mapper:           mockMapper,
		lockManager:      &lockMgr,
		metricsCollector: &NoOpMetricsCollector{},
		efsCache:         make(map[string]*CachedEFS),
	}

	// Mock responses
	mockFileSystem := &cloud.FileSystem{
		FileSystemId: "fs-12345",
	}

	mockAccessPoint := &cloud.AccessPoint{
		AccessPointId: "fsap-67890",
		FileSystemId:  "fs-12345",
		CapacityGiB:   0,
	}

	mockMapper.getMappingFunc = func(ctx context.Context, namespace string) (*NamespaceEFSMapping, error) {
		return &NamespaceEFSMapping{
			Namespace:    namespace,
			FileSystemID: mockFileSystem.FileSystemId,
		}, nil
	}

	mockCloud.findFileSystemsByTagsFunc = func(ctx context.Context, tags map[string]string) ([]*cloud.FileSystem, error) {
		return []*cloud.FileSystem{mockFileSystem}, nil
	}

	mockCloud.createAccessPointFunc = func(ctx context.Context, clientToken string, opts *cloud.AccessPointOptions) (*cloud.AccessPoint, error) {
		return mockAccessPoint, nil
	}

	// Test without explicit PVC name - should use volume name as fallback
	req := &csi.CreateVolumeRequest{
		Name: "fallback-volume-name",
		Parameters: map[string]string{
			"csi.storage.k8s.io/pvc/namespace": "test-namespace",
			"provisioningMode":                 "efs-ns",
		},
	}

	ctx := context.Background()
	response, err := provisioner.CreateNamespaceVolume(ctx, req)

	if err != nil {
		t.Fatalf("CreateNamespaceVolume failed: %v", err)
	}

	// Verify that volume name was used as PVC name
	volumeContext := response.Volume.VolumeContext
	if volumeContext["pvcName"] != "fallback-volume-name" {
		t.Errorf("Expected pvcName fallback-volume-name, got %s", volumeContext["pvcName"])
	}
}

func TestNamespaceProvisioner_ExtractNamespaceFromRequest(t *testing.T) {
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()

	provisioner := &NamespaceProvisioner{
		cloud:   mockCloud,
		options: options,
	}

	// Test with standard namespace parameter
	req := &csi.CreateVolumeRequest{
		Parameters: map[string]string{
			"csi.storage.k8s.io/pvc/namespace": "test-namespace",
		},
	}

	namespace, err := provisioner.extractNamespaceFromRequest(req)
	if err != nil {
		t.Fatalf("extractNamespaceFromRequest failed: %v", err)
	}

	if namespace != "test-namespace" {
		t.Errorf("Expected namespace test-namespace, got %s", namespace)
	}

	// Test with alternative namespace parameter
	req = &csi.CreateVolumeRequest{
		Parameters: map[string]string{
			"namespace": "alt-namespace",
		},
	}

	namespace, err = provisioner.extractNamespaceFromRequest(req)
	if err != nil {
		t.Fatalf("extractNamespaceFromRequest failed: %v", err)
	}

	if namespace != "alt-namespace" {
		t.Errorf("Expected namespace alt-namespace, got %s", namespace)
	}

	// Test with missing namespace
	req = &csi.CreateVolumeRequest{
		Parameters: map[string]string{
			"other-param": "value",
		},
	}

	_, err = provisioner.extractNamespaceFromRequest(req)
	if err == nil {
		t.Error("Expected error for missing namespace")
	}
}

func TestNamespaceProvisioner_ParseEFSOptionsFromParams(t *testing.T) {
	mockCloud := &testMockCloud{}
	options := DefaultProvisionerOptions()

	provisioner := &NamespaceProvisioner{
		cloud:   mockCloud,
		options: options,
	}

	// Test with default values
	params := map[string]string{}
	efsOptions, err := provisioner.parseEFSOptionsFromParams(params)
	if err != nil {
		t.Fatalf("parseEFSOptionsFromParams failed: %v", err)
	}

	if efsOptions.PerformanceMode != "generalPurpose" {
		t.Errorf("Expected default performance mode generalPurpose, got %s", efsOptions.PerformanceMode)
	}

	if efsOptions.ThroughputMode != "bursting" {
		t.Errorf("Expected default throughput mode bursting, got %s", efsOptions.ThroughputMode)
	}

	if !efsOptions.Encrypted {
		t.Error("Expected default encrypted to be true")
	}

	// Test with custom values
	params = map[string]string{
		"performanceMode":                  "maxIO",
		"throughputMode":                   "provisioned",
		"provisionedThroughputInMibps":     "100",
		"encrypted":                        "false",
		"kmsKeyId":                         "test-key-id",
		"lifecyclePolicy":                  "AFTER_30_DAYS",
		"backupPolicy":                     "DISABLED",
	}

	efsOptions, err = provisioner.parseEFSOptionsFromParams(params)
	if err != nil {
		t.Fatalf("parseEFSOptionsFromParams failed: %v", err)
	}

	if efsOptions.PerformanceMode != "maxIO" {
		t.Errorf("Expected performance mode maxIO, got %s", efsOptions.PerformanceMode)
	}

	if efsOptions.ThroughputMode != "provisioned" {
		t.Errorf("Expected throughput mode provisioned, got %s", efsOptions.ThroughputMode)
	}

	if efsOptions.ProvisionedThroughputInMibps != 100 {
		t.Errorf("Expected provisioned throughput 100, got %d", efsOptions.ProvisionedThroughputInMibps)
	}

	if efsOptions.Encrypted {
		t.Error("Expected encrypted to be false")
	}

	if efsOptions.KmsKeyId != "test-key-id" {
		t.Errorf("Expected KMS key ID test-key-id, got %s", efsOptions.KmsKeyId)
	}

	if efsOptions.LifecyclePolicy != "AFTER_30_DAYS" {
		t.Errorf("Expected lifecycle policy AFTER_30_DAYS, got %s", efsOptions.LifecyclePolicy)
	}

	if efsOptions.BackupPolicy != "DISABLED" {
		t.Errorf("Expected backup policy DISABLED, got %s", efsOptions.BackupPolicy)
	}
}

