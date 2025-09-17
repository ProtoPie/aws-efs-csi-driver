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