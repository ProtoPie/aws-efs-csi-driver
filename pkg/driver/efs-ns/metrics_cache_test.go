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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// MockCache implements FileSystemCache for testing
type MockCache struct {
	data     map[string]*FileSystemInfo
	getCalls int
	setCalls int
}

func NewMockCache() *MockCache {
	return &MockCache{
		data: make(map[string]*FileSystemInfo),
	}
}

func (m *MockCache) Get(namespace string) (*FileSystemInfo, bool) {
	m.getCalls++
	fsInfo, exists := m.data[namespace]
	return fsInfo, exists
}

func (m *MockCache) Set(namespace string, fsInfo *FileSystemInfo) {
	m.setCalls++
	m.data[namespace] = fsInfo
}

func (m *MockCache) Delete(namespace string) {
	delete(m.data, namespace)
}

func (m *MockCache) List() map[string]*FileSystemInfo {
	result := make(map[string]*FileSystemInfo)
	for k, v := range m.data {
		result[k] = v
	}
	return result
}

func (m *MockCache) Refresh(ctx context.Context, namespace string) error {
	return nil
}

func (m *MockCache) SetTTL(duration time.Duration) {}

func (m *MockCache) Clear() {
	m.data = make(map[string]*FileSystemInfo)
}

func (m *MockCache) GetSize() int {
	return len(m.data)
}

func createTestFSInfo() *FileSystemInfo {
	return &FileSystemInfo{
		FileSystemID:    "fs-12345678901234567",
		Namespace:       "test-namespace",
		ClusterID:       "test-cluster",
		CreatedAt:       time.Now(),
		MountTargets:    []*MountTargetInfo{},
		SecurityGroupID: "sg-test123",
		State:           FileSystemStateAvailable,
		PVCCount:        1,
		Tags:            map[string]string{"test": "true"},
		PerformanceMode: "generalPurpose",
		ThroughputMode:  "bursting",
	}
}

func TestMetricsAwareCache_Get_Hit(t *testing.T) {
	mockCache := NewMockCache()
	collector := NewPrometheusMetricsCollector()
	metricsCache := NewMetricsAwareCache(mockCache, collector, "test_operation")

	// Prepare test data
	fsInfo := createTestFSInfo()
	mockCache.Set("test-namespace", fsInfo)

	// Test cache hit
	result, found := metricsCache.Get("test-namespace")

	if !found {
		t.Error("Expected cache hit")
	}
	if result == nil {
		t.Error("Expected non-nil result")
	}
	if result.FileSystemID != fsInfo.FileSystemID {
		t.Errorf("Expected FileSystemID %s, got %s", fsInfo.FileSystemID, result.FileSystemID)
	}
	if mockCache.getCalls != 1 {
		t.Errorf("Expected 1 get call to underlying cache, got %d", mockCache.getCalls)
	}
}

func TestMetricsAwareCache_Get_Miss(t *testing.T) {
	mockCache := NewMockCache()
	collector := NewPrometheusMetricsCollector()
	metricsCache := NewMetricsAwareCache(mockCache, collector, "test_operation")

	// Test cache miss
	result, found := metricsCache.Get("non-existent-namespace")

	if found {
		t.Error("Expected cache miss")
	}
	if result != nil {
		t.Error("Expected nil result")
	}
	if mockCache.getCalls != 1 {
		t.Errorf("Expected 1 get call to underlying cache, got %d", mockCache.getCalls)
	}
}

func TestMetricsAwareCache_Set(t *testing.T) {
	mockCache := NewMockCache()
	collector := NewPrometheusMetricsCollector()
	registry := prometheus.NewRegistry()
	err := collector.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register collector: %v", err)
	}

	metricsCache := NewMetricsAwareCache(mockCache, collector, "test_operation")

	// Test cache set
	fsInfo := createTestFSInfo()
	metricsCache.Set("test-namespace", fsInfo)

	if mockCache.setCalls != 1 {
		t.Errorf("Expected 1 set call to underlying cache, got %d", mockCache.setCalls)
	}

	// Verify data was set in underlying cache
	storedInfo, exists := mockCache.Get("test-namespace")
	if !exists {
		t.Error("Expected data to be stored in underlying cache")
	}
	if storedInfo.FileSystemID != fsInfo.FileSystemID {
		t.Errorf("Expected stored FileSystemID %s, got %s", fsInfo.FileSystemID, storedInfo.FileSystemID)
	}
}

func TestMetricsAwareCache_Delete(t *testing.T) {
	mockCache := NewMockCache()
	collector := NewPrometheusMetricsCollector()
	metricsCache := NewMetricsAwareCache(mockCache, collector, "test_operation")

	// Prepare test data
	fsInfo := createTestFSInfo()
	mockCache.Set("test-namespace", fsInfo)

	// Test cache delete
	metricsCache.Delete("test-namespace")

	// Verify data was deleted from underlying cache
	_, exists := mockCache.Get("test-namespace")
	if exists {
		t.Error("Expected data to be deleted from underlying cache")
	}
}

func TestMetricsAwareCache_List(t *testing.T) {
	mockCache := NewMockCache()
	collector := NewPrometheusMetricsCollector()
	metricsCache := NewMetricsAwareCache(mockCache, collector, "test_operation")

	// Prepare test data
	fsInfo1 := createTestFSInfo()
	fsInfo1.Namespace = "namespace-1"
	fsInfo2 := createTestFSInfo()
	fsInfo2.Namespace = "namespace-2"
	fsInfo2.FileSystemID = "fs-87654321098765432"

	mockCache.Set("namespace-1", fsInfo1)
	mockCache.Set("namespace-2", fsInfo2)

	// Test cache list
	result := metricsCache.List()

	if len(result) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(result))
	}
	if result["namespace-1"] == nil {
		t.Error("Expected namespace-1 entry")
	}
	if result["namespace-2"] == nil {
		t.Error("Expected namespace-2 entry")
	}
	if result["namespace-1"].FileSystemID != fsInfo1.FileSystemID {
		t.Errorf("Expected FileSystemID %s for namespace-1, got %s", fsInfo1.FileSystemID, result["namespace-1"].FileSystemID)
	}
}

func TestMetricsAwareCache_Refresh_Success(t *testing.T) {
	mockCache := NewMockCache()
	collector := NewPrometheusMetricsCollector()
	metricsCache := NewMetricsAwareCache(mockCache, collector, "test_operation")

	// Test successful refresh
	err := metricsCache.Refresh(context.Background(), "test-namespace")

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestMetricsAwareCache_SetTTL(t *testing.T) {
	mockCache := NewMockCache()
	collector := NewPrometheusMetricsCollector()
	metricsCache := NewMetricsAwareCache(mockCache, collector, "test_operation")

	// Test SetTTL (should not cause errors)
	metricsCache.SetTTL(5 * time.Minute)
}

func TestMetricsAwareCache_Clear(t *testing.T) {
	mockCache := NewMockCache()
	collector := NewPrometheusMetricsCollector()
	metricsCache := NewMetricsAwareCache(mockCache, collector, "test_operation")

	// Prepare test data
	fsInfo := createTestFSInfo()
	mockCache.Set("test-namespace", fsInfo)

	// Verify data exists
	if len(mockCache.List()) != 1 {
		t.Error("Expected 1 entry before clear")
	}

	// Test clear
	metricsCache.Clear()

	// Verify data was cleared
	if len(mockCache.List()) != 0 {
		t.Error("Expected 0 entries after clear")
	}
}

func TestMetricsAwareCache_GetSize(t *testing.T) {
	mockCache := NewMockCache()
	collector := NewPrometheusMetricsCollector()
	metricsCache := NewMetricsAwareCache(mockCache, collector, "test_operation")

	// Test initial size
	if metricsCache.GetSize() != 0 {
		t.Errorf("Expected initial size 0, got %d", metricsCache.GetSize())
	}

	// Add some data
	fsInfo := createTestFSInfo()
	mockCache.Set("test-namespace", fsInfo)

	// Test size after addition
	if metricsCache.GetSize() != 1 {
		t.Errorf("Expected size 1 after addition, got %d", metricsCache.GetSize())
	}
}

func TestFileSystemMetricsCollector_RecordFileSystemCreated(t *testing.T) {
	collector := NewPrometheusMetricsCollector()
	registry := prometheus.NewRegistry()
	err := collector.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register collector: %v", err)
	}

	fsCollector := NewFileSystemMetricsCollector(collector)

	// Test recording filesystem creation
	duration := 2 * time.Second
	fsCollector.RecordFileSystemCreated("test-namespace", "test-cluster", duration)

	// Verify metrics were recorded
	metricFamily, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	foundDuration := false
	foundTotal := false
	foundCount := false

	for _, mf := range metricFamily {
		switch mf.GetName() {
		case "efs_ns_operations_duration_seconds":
			foundDuration = true
		case "efs_ns_operations_total":
			foundTotal = true
		case "efs_ns_filesystem_count":
			foundCount = true
		}
	}

	if !foundDuration {
		t.Error("Expected operation duration metric")
	}
	if !foundTotal {
		t.Error("Expected operation total metric")
	}
	if !foundCount {
		t.Error("Expected filesystem count metric")
	}
}

func TestFileSystemMetricsCollector_RecordFileSystemCreationFailed(t *testing.T) {
	collector := NewPrometheusMetricsCollector()
	registry := prometheus.NewRegistry()
	err := collector.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register collector: %v", err)
	}

	fsCollector := NewFileSystemMetricsCollector(collector)

	// Test recording filesystem creation failure
	duration := 500 * time.Millisecond
	testErr := NewEFSNSError(ErrFileSystemCreationFailed, "test_operation", "test-namespace", "creation failed", nil)
	fsCollector.RecordFileSystemCreationFailed("test-namespace", "test-cluster", duration, testErr)

	// Verify metrics were recorded
	metricFamily, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	foundDuration := false
	foundTotal := false

	for _, mf := range metricFamily {
		switch mf.GetName() {
		case "efs_ns_operations_duration_seconds":
			foundDuration = true
		case "efs_ns_operations_total":
			foundTotal = true
		}
	}

	if !foundDuration {
		t.Error("Expected operation duration metric")
	}
	if !foundTotal {
		t.Error("Expected operation total metric")
	}
}

func TestFileSystemMetricsCollector_RecordFileSystemDeleted(t *testing.T) {
	collector := NewPrometheusMetricsCollector()
	registry := prometheus.NewRegistry()
	err := collector.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register collector: %v", err)
	}

	fsCollector := NewFileSystemMetricsCollector(collector)

	// Test recording filesystem deletion
	duration := 1 * time.Second
	fsCollector.RecordFileSystemDeleted("test-namespace", "test-cluster", duration)

	// Verify metrics were recorded
	metricFamily, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	foundDuration := false
	foundTotal := false

	for _, mf := range metricFamily {
		switch mf.GetName() {
		case "efs_ns_operations_duration_seconds":
			foundDuration = true
		case "efs_ns_operations_total":
			foundTotal = true
		}
	}

	if !foundDuration {
		t.Error("Expected operation duration metric")
	}
	if !foundTotal {
		t.Error("Expected operation total metric")
	}
}

func TestFileSystemMetricsCollector_RecordFileSystemStateTransition(t *testing.T) {
	collector := NewPrometheusMetricsCollector()
	registry := prometheus.NewRegistry()
	err := collector.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register collector: %v", err)
	}

	fsCollector := NewFileSystemMetricsCollector(collector)

	// Test recording state transition
	fsCollector.RecordFileSystemStateTransition("test-namespace", "test-cluster", FileSystemStateCreating, FileSystemStateAvailable)

	// Verify metrics were recorded
	metricFamily, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	foundCount := false

	for _, mf := range metricFamily {
		if mf.GetName() == "efs_ns_filesystem_count" {
			foundCount = true
			break
		}
	}

	if !foundCount {
		t.Error("Expected filesystem count metric")
	}
}

func TestFileSystemMetricsCollector_GetMetrics(t *testing.T) {
	collector := NewPrometheusMetricsCollector()
	fsCollector := NewFileSystemMetricsCollector(collector)

	// Test getting underlying metrics collector
	underlyingCollector := fsCollector.GetMetrics()
	if underlyingCollector != collector {
		t.Error("Expected underlying collector to match")
	}
}

func TestNewMetricsAwareCache_DefaultOperation(t *testing.T) {
	mockCache := NewMockCache()
	collector := NewPrometheusMetricsCollector()

	// Test with empty operation name
	metricsCache := NewMetricsAwareCache(mockCache, collector, "")

	// Should use default operation name
	_, _ = metricsCache.Get("test-namespace")
	// No explicit verification of operation name, but ensures no panics
}

func TestMetricsAwareCache_Integration(t *testing.T) {
	mockCache := NewMockCache()
	collector := NewPrometheusMetricsCollector()
	registry := prometheus.NewRegistry()
	err := collector.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register collector: %v", err)
	}

	metricsCache := NewMetricsAwareCache(mockCache, collector, "filesystem_cache")

	// Simulate typical cache operations
	fsInfo := createTestFSInfo()

	// Cache miss
	_, found := metricsCache.Get("test-namespace")
	if found {
		t.Error("Expected cache miss")
	}

	// Cache set
	metricsCache.Set("test-namespace", fsInfo)

	// Cache hit
	result, found := metricsCache.Get("test-namespace")
	if !found {
		t.Error("Expected cache hit")
	}
	if result == nil {
		t.Error("Expected non-nil result")
	}

	// Another cache hit
	_, found = metricsCache.Get("test-namespace")
	if !found {
		t.Error("Expected cache hit")
	}

	// Cache delete
	metricsCache.Delete("test-namespace")

	// Verify metrics were recorded
	metricFamily, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	// Should have cache hit/miss metrics and operation duration metrics
	hasHitMetric := false
	hasMissMetric := false
	hasDurationMetric := false

	for _, mf := range metricFamily {
		switch mf.GetName() {
		case "efs_ns_cache_hits_total":
			hasHitMetric = true
		case "efs_ns_cache_misses_total":
			hasMissMetric = true
		case "efs_ns_operations_duration_seconds":
			hasDurationMetric = true
		}
	}

	if !hasHitMetric {
		t.Error("Expected cache hit metric")
	}
	if !hasMissMetric {
		t.Error("Expected cache miss metric")
	}
	if !hasDurationMetric {
		t.Error("Expected operation duration metric")
	}
}
