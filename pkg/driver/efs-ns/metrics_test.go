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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestPrometheusMetricsCollector_NewInstance(t *testing.T) {
	collector := NewPrometheusMetricsCollector()
	if collector == nil {
		t.Fatal("Expected collector to be non-nil")
	}

	// Type assertion to access internal fields for testing
	promCollector, ok := collector.(*PrometheusMetricsCollector)
	if !ok {
		t.Fatal("Expected collector to be PrometheusMetricsCollector")
	}

	// Verify all metrics are initialized
	if promCollector.filesystemCount == nil {
		t.Error("Expected filesystemCount to be initialized")
	}
	if promCollector.filesystemTotal == nil {
		t.Error("Expected filesystemTotal to be initialized")
	}
	if promCollector.operationDuration == nil {
		t.Error("Expected operationDuration to be initialized")
	}
	if promCollector.operationTotal == nil {
		t.Error("Expected operationTotal to be initialized")
	}
	if promCollector.cacheHitTotal == nil {
		t.Error("Expected cacheHitTotal to be initialized")
	}
	if promCollector.cacheMissTotal == nil {
		t.Error("Expected cacheMissTotal to be initialized")
	}
	if promCollector.cacheHitRatio == nil {
		t.Error("Expected cacheHitRatio to be initialized")
	}
	if promCollector.cacheStats == nil {
		t.Error("Expected cacheStats to be initialized")
	}
	if promCollector.handler == nil {
		t.Error("Expected handler to be initialized")
	}
}

func TestPrometheusMetricsCollector_RecordFileSystemCount(t *testing.T) {
	collector := NewPrometheusMetricsCollector().(*PrometheusMetricsCollector)
	registry := prometheus.NewRegistry()
	err := collector.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register collector: %v", err)
	}

	// Record filesystem count
	collector.RecordFileSystemCount("test-namespace", "test-cluster", "available", 5.0)

	// Verify the metric was recorded
	metricFamily, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	found := false
	for _, mf := range metricFamily {
		if mf.GetName() == "efs_ns_filesystem_count" {
			found = true
			metrics := mf.GetMetric()
			if len(metrics) != 1 {
				t.Errorf("Expected 1 metric, got %d", len(metrics))
				return
			}
			if metrics[0].GetGauge().GetValue() != 5.0 {
				t.Errorf("Expected value 5.0, got %f", metrics[0].GetGauge().GetValue())
			}

			// Check labels
			labels := metrics[0].GetLabel()
			if len(labels) != 3 {
				t.Errorf("Expected 3 labels, got %d", len(labels))
			}

			labelMap := make(map[string]string)
			for _, label := range labels {
				labelMap[label.GetName()] = label.GetValue()
			}

			if labelMap["namespace"] != "test-namespace" {
				t.Errorf("Expected namespace 'test-namespace', got '%s'", labelMap["namespace"])
			}
			if labelMap["cluster"] != "test-cluster" {
				t.Errorf("Expected cluster 'test-cluster', got '%s'", labelMap["cluster"])
			}
			if labelMap["state"] != "available" {
				t.Errorf("Expected state 'available', got '%s'", labelMap["state"])
			}
		}
	}
	if !found {
		t.Error("efs_ns_filesystem_count metric not found")
	}
}

func TestPrometheusMetricsCollector_RecordOperationDuration(t *testing.T) {
	collector := NewPrometheusMetricsCollector().(*PrometheusMetricsCollector)
	registry := prometheus.NewRegistry()
	err := collector.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register collector: %v", err)
	}

	// Record operation duration
	duration := 2500 * time.Millisecond
	collector.RecordOperationDuration("filesystem_create", "test-namespace", "success", duration)

	// Verify the metric was recorded
	metricFamily, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	found := false
	for _, mf := range metricFamily {
		if mf.GetName() == "efs_ns_operations_duration_seconds" {
			found = true
			metrics := mf.GetMetric()
			if len(metrics) != 1 {
				t.Errorf("Expected 1 metric, got %d", len(metrics))
				return
			}

			histogram := metrics[0].GetHistogram()
			if histogram.GetSampleCount() != 1 {
				t.Errorf("Expected sample count 1, got %d", histogram.GetSampleCount())
			}
			if histogram.GetSampleSum() != 2.5 { // 2500ms = 2.5s
				t.Errorf("Expected sample sum 2.5, got %f", histogram.GetSampleSum())
			}
		}
	}
	if !found {
		t.Error("efs_ns_operations_duration_seconds metric not found")
	}
}

func TestPrometheusMetricsCollector_RecordCacheHitMiss(t *testing.T) {
	collector := NewPrometheusMetricsCollector().(*PrometheusMetricsCollector)
	registry := prometheus.NewRegistry()
	err := collector.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register collector: %v", err)
	}

	operation := "filesystem_lookup"

	// Record cache hits and misses
	collector.RecordCacheHit(operation)
	collector.RecordCacheHit(operation)
	collector.RecordCacheMiss(operation)

	// Verify cache hit/miss metrics
	metricFamily, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	var hitCount, missCount float64
	var hitRatio float64

	for _, mf := range metricFamily {
		switch mf.GetName() {
		case "efs_ns_cache_hits_total":
			metrics := mf.GetMetric()
			if len(metrics) != 1 {
				t.Errorf("Expected 1 hit metric, got %d", len(metrics))
				return
			}
			hitCount = metrics[0].GetCounter().GetValue()
		case "efs_ns_cache_misses_total":
			metrics := mf.GetMetric()
			if len(metrics) != 1 {
				t.Errorf("Expected 1 miss metric, got %d", len(metrics))
				return
			}
			missCount = metrics[0].GetCounter().GetValue()
		case "efs_ns_cache_hit_ratio":
			metrics := mf.GetMetric()
			if len(metrics) != 1 {
				t.Errorf("Expected 1 ratio metric, got %d", len(metrics))
				return
			}
			hitRatio = metrics[0].GetGauge().GetValue()
		}
	}

	if hitCount != 2.0 {
		t.Errorf("Expected hit count 2.0, got %f", hitCount)
	}
	if missCount != 1.0 {
		t.Errorf("Expected miss count 1.0, got %f", missCount)
	}
	if hitRatio < 0.666 || hitRatio > 0.668 {
		t.Errorf("Expected hit ratio around 0.667, got %f", hitRatio)
	}
}

func TestPrometheusMetricsCollector_Register(t *testing.T) {
	collector := NewPrometheusMetricsCollector().(*PrometheusMetricsCollector)
	registry := prometheus.NewRegistry()

	// First registration should succeed
	err := collector.Register(registry)
	if err != nil {
		t.Errorf("First registration failed: %v", err)
	}
	if !collector.registered {
		t.Error("Expected collector to be marked as registered")
	}

	// Second registration should be a no-op
	err = collector.Register(registry)
	if err != nil {
		t.Errorf("Second registration failed: %v", err)
	}
}

func TestPrometheusMetricsCollector_Unregister(t *testing.T) {
	collector := NewPrometheusMetricsCollector().(*PrometheusMetricsCollector)
	registry := prometheus.NewRegistry()

	// Register first
	err := collector.Register(registry)
	if err != nil {
		t.Fatalf("Registration failed: %v", err)
	}

	// Unregister should succeed
	success := collector.Unregister(registry)
	if !success {
		t.Error("Expected unregister to succeed")
	}
	if collector.registered {
		t.Error("Expected collector to be marked as unregistered")
	}

	// Unregister when not registered should return true
	success = collector.Unregister(registry)
	if !success {
		t.Error("Expected unregister to succeed when already unregistered")
	}
}

func TestMetricsRecorder(t *testing.T) {
	collector := NewPrometheusMetricsCollector()
	registry := prometheus.NewRegistry()
	err := collector.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register collector: %v", err)
	}

	operation := "test_operation"
	namespace := "test-namespace"

	// Test successful operation recording
	recorder := NewMetricsRecorder(collector, operation, namespace)
	time.Sleep(10 * time.Millisecond) // Simulate some work
	recorder.RecordSuccess()

	// Test error operation recording
	recorder2 := NewMetricsRecorder(collector, operation, namespace)
	time.Sleep(5 * time.Millisecond) // Simulate some work
	testErr := NewEFSNSError(ErrFileSystemCreationFailed, operation, namespace, "test error", nil)
	recorder2.RecordError(testErr)

	// Verify metrics were recorded
	metricFamily, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	var totalMetrics int
	for _, mf := range metricFamily {
		if mf.GetName() == "efs_ns_operations_total" {
			metrics := mf.GetMetric()
			for _, metric := range metrics {
				totalMetrics++
				labels := make(map[string]string)
				for _, label := range metric.GetLabel() {
					labels[label.GetName()] = label.GetValue()
				}

				if labels["operation"] != operation {
					t.Errorf("Expected operation '%s', got '%s'", operation, labels["operation"])
				}
				if labels["namespace"] != namespace {
					t.Errorf("Expected namespace '%s', got '%s'", namespace, labels["namespace"])
				}
				if labels["result"] != "success" && labels["result"] != "error" {
					t.Errorf("Expected result 'success' or 'error', got '%s'", labels["result"])
				}
				if metric.GetCounter().GetValue() != 1.0 {
					t.Errorf("Expected counter value 1.0, got %f", metric.GetCounter().GetValue())
				}
			}
		}
	}
	if totalMetrics != 2 {
		t.Errorf("Expected 2 operation metrics (success and error), got %d", totalMetrics)
	}
}

func TestGlobalMetricsCollector(t *testing.T) {
	// Reset global state for test isolation
	globalMetricsCollector = nil
	metricsOnce = sync.Once{}

	// Get global collector
	collector1 := GetGlobalMetricsCollector()
	if collector1 == nil {
		t.Fatal("Expected global collector to be non-nil")
	}

	// Should return the same instance
	collector2 := GetGlobalMetricsCollector()
	if collector1 != collector2 {
		t.Error("Expected same global collector instance")
	}

	// Test setting custom collector
	customCollector := NewPrometheusMetricsCollector()
	SetGlobalMetricsCollector(customCollector)

	collector3 := GetGlobalMetricsCollector()
	if collector3 != customCollector {
		t.Error("Expected custom collector to be returned")
	}
}

func TestMetricsCollector_PerformanceImpact(t *testing.T) {
	collector := NewPrometheusMetricsCollector()
	registry := prometheus.NewRegistry()
	err := collector.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register collector: %v", err)
	}

	// Measure time for metrics operations
	operations := 1000
	startTime := time.Now()

	for i := 0; i < operations; i++ {
		collector.RecordFileSystemCount("test-namespace", "test-cluster", "available", float64(i))
		collector.RecordOperationDuration("test_op", "test-ns", "success", time.Millisecond)
		collector.RecordCacheHit("test_operation")
		if i%10 == 0 {
			collector.RecordCacheMiss("test_operation")
		}
	}

	totalTime := time.Since(startTime)
	avgTimePerOperation := totalTime / time.Duration(operations*4) // 4 operations per loop

	// Performance requirement: average operation should take less than 1ms
	if avgTimePerOperation >= time.Millisecond {
		t.Errorf("Metrics collection performance impact too high: %v per operation", avgTimePerOperation)
	}

	t.Logf("Performance test: %d operations in %v (avg: %v per operation)",
		operations*4, totalTime, avgTimePerOperation)
}

func TestMetricsCollector_ConcurrentAccess(t *testing.T) {
	collector := NewPrometheusMetricsCollector()
	registry := prometheus.NewRegistry()
	err := collector.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register collector: %v", err)
	}

	// Test concurrent access
	const goroutines = 10
	const operationsPerGoroutine = 100

	done := make(chan bool, goroutines)

	for i := 0; i < goroutines; i++ {
		go func(goroutineID int) {
			defer func() { done <- true }()

			for j := 0; j < operationsPerGoroutine; j++ {
				namespace := fmt.Sprintf("ns-%d", goroutineID)
				operation := fmt.Sprintf("op-%d", j)

				collector.RecordFileSystemCount(namespace, "cluster", "available", float64(j))
				collector.RecordOperationDuration(operation, namespace, "success", time.Millisecond)

				if j%2 == 0 {
					collector.RecordCacheHit(operation)
				} else {
					collector.RecordCacheMiss(operation)
				}
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < goroutines; i++ {
		<-done
	}

	// Verify no data races occurred and metrics were recorded
	metricFamily, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}
	if len(metricFamily) == 0 {
		t.Error("Expected metrics to be recorded from concurrent operations")
	}
}

func TestMetricsCollector_MetricsFormat(t *testing.T) {
	collector := NewPrometheusMetricsCollector()
	registry := prometheus.NewRegistry()
	err := collector.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register collector: %v", err)
	}

	// Record some sample metrics
	collector.RecordFileSystemCount("test-ns", "test-cluster", "available", 3)
	collector.RecordOperationDuration("filesystem_create", "test-ns", "success", 2*time.Second)
	collector.RecordCacheHit("filesystem_lookup")

	// Test that metrics can be gathered without errors
	metricFamily, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}
	if len(metricFamily) == 0 {
		t.Error("Expected metrics to be gathered")
	}

	// Test Prometheus text format
	problems, err := testutil.GatherAndLint(registry)
	if err != nil {
		t.Errorf("Failed to gather and lint metrics: %v", err)
	}
	if len(problems) > 0 {
		t.Errorf("Metrics should pass Prometheus linting, found problems: %v", problems)
	}

	// Verify specific metrics exist
	expectedMetrics := []string{
		"efs_ns_filesystem_count",
		"efs_ns_operations_duration_seconds",
		"efs_ns_cache_hits_total",
	}

	for _, expectedMetric := range expectedMetrics {
		found := false
		for _, mf := range metricFamily {
			if mf.GetName() == expectedMetric {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected metric %s not found", expectedMetric)
		}
	}
}

func TestMetricsCollector_MetricLabels(t *testing.T) {
	collector := NewPrometheusMetricsCollector()
	registry := prometheus.NewRegistry()
	err := collector.Register(registry)
	if err != nil {
		t.Fatalf("Failed to register collector: %v", err)
	}

	// Record metrics with various label combinations
	testCases := []struct {
		namespace string
		cluster   string
		state     string
		operation string
		result    string
	}{
		{"default", "cluster-1", "available", "filesystem_create", "success"},
		{"kube-system", "cluster-1", "creating", "filesystem_delete", "error"},
		{"test-ns", "cluster-2", "available", "cache_get", "success"},
	}

	for _, tc := range testCases {
		collector.RecordFileSystemCount(tc.namespace, tc.cluster, tc.state, 1)
		collector.RecordOperationTotal(tc.operation, tc.namespace, tc.result)
	}

	// Verify all label combinations are recorded correctly
	metricFamily, err := registry.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	for _, mf := range metricFamily {
		for _, metric := range mf.GetMetric() {
			labels := make(map[string]string)
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}

			// Verify required labels exist based on metric type
			switch mf.GetName() {
			case "efs_ns_filesystem_count":
				if _, ok := labels["namespace"]; !ok {
					t.Error("Expected namespace label in filesystem_count metric")
				}
				if _, ok := labels["cluster"]; !ok {
					t.Error("Expected cluster label in filesystem_count metric")
				}
				if _, ok := labels["state"]; !ok {
					t.Error("Expected state label in filesystem_count metric")
				}
			case "efs_ns_operations_total":
				if _, ok := labels["operation"]; !ok {
					t.Error("Expected operation label in operations_total metric")
				}
				if _, ok := labels["namespace"]; !ok {
					t.Error("Expected namespace label in operations_total metric")
				}
				if _, ok := labels["result"]; !ok {
					t.Error("Expected result label in operations_total metric")
				}
			}
		}
	}
}

// Benchmark tests for performance validation
func BenchmarkMetricsCollector_RecordFileSystemCount(b *testing.B) {
	collector := NewPrometheusMetricsCollector()
	registry := prometheus.NewRegistry()
	collector.Register(registry)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		collector.RecordFileSystemCount("test-ns", "test-cluster", "available", float64(i))
	}
}

func BenchmarkMetricsCollector_RecordOperationDuration(b *testing.B) {
	collector := NewPrometheusMetricsCollector()
	registry := prometheus.NewRegistry()
	collector.Register(registry)

	duration := 100 * time.Millisecond

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		collector.RecordOperationDuration("test_op", "test-ns", "success", duration)
	}
}

func BenchmarkMetricsCollector_RecordCacheHit(b *testing.B) {
	collector := NewPrometheusMetricsCollector()
	registry := prometheus.NewRegistry()
	collector.Register(registry)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		collector.RecordCacheHit("test_operation")
	}
}
