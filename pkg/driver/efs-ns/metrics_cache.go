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

// MetricsAwareCache wraps a FileSystemCache with metrics collection
type MetricsAwareCache struct {
	cache     FileSystemCache
	metrics   MetricsCollector
	operation string // Operation name for metrics (e.g., "filesystem_lookup", "filesystem_refresh")
}

// NewMetricsAwareCache creates a new metrics-aware cache wrapper
func NewMetricsAwareCache(cache FileSystemCache, metrics MetricsCollector, operation string) FileSystemCache {
	if operation == "" {
		operation = "filesystem_cache"
	}

	return &MetricsAwareCache{
		cache:     cache,
		metrics:   metrics,
		operation: operation,
	}
}

// Get retrieves filesystem info from cache with metrics collection
func (c *MetricsAwareCache) Get(namespace string) (*FileSystemInfo, bool) {
	startTime := time.Now()

	fsInfo, found := c.cache.Get(namespace)

	// Record cache hit/miss metrics
	if found {
		c.metrics.RecordCacheHit(c.operation)
		klog.V(5).Infof("MetricsAwareCache: cache hit for namespace %s", namespace)
	} else {
		c.metrics.RecordCacheMiss(c.operation)
		klog.V(5).Infof("MetricsAwareCache: cache miss for namespace %s", namespace)
	}

	// Record operation duration
	duration := time.Since(startTime)
	result := "success"
	if !found {
		result = "miss"
	}
	c.metrics.RecordOperationDuration("cache_get", namespace, result, duration)

	return fsInfo, found
}

// Set stores filesystem info in cache with metrics collection
func (c *MetricsAwareCache) Set(namespace string, fsInfo *FileSystemInfo) {
	startTime := time.Now()

	c.cache.Set(namespace, fsInfo)

	// Record operation duration
	duration := time.Since(startTime)
	c.metrics.RecordOperationDuration("cache_set", namespace, "success", duration)

	// Update filesystem count metrics if state information is available
	if fsInfo != nil {
		c.metrics.RecordFileSystemCount(namespace, fsInfo.ClusterID, string(fsInfo.State), 1)
		klog.V(5).Infof("MetricsAwareCache: set filesystem info for namespace %s, state %s", namespace, fsInfo.State)
	}
}

// Delete removes filesystem info from cache with metrics collection
func (c *MetricsAwareCache) Delete(namespace string) {
	startTime := time.Now()

	// Get the current info before deletion for metrics
	fsInfo, found := c.cache.Get(namespace)

	c.cache.Delete(namespace)

	// Record operation duration
	duration := time.Since(startTime)
	c.metrics.RecordOperationDuration("cache_delete", namespace, "success", duration)

	// Update filesystem count metrics (set to 0 since we're deleting)
	if found && fsInfo != nil {
		c.metrics.RecordFileSystemCount(namespace, fsInfo.ClusterID, string(fsInfo.State), 0)
		klog.V(5).Infof("MetricsAwareCache: deleted filesystem info for namespace %s, state %s", namespace, fsInfo.State)
	}
}

// List returns all cached filesystem info with metrics collection
func (c *MetricsAwareCache) List() map[string]*FileSystemInfo {
	startTime := time.Now()

	result := c.cache.List()

	// Record operation duration
	duration := time.Since(startTime)
	c.metrics.RecordOperationDuration("cache_list", "", "success", duration)

	// Update filesystem count metrics for all entries
	c.updateFileSystemCountMetrics(result)

	klog.V(5).Infof("MetricsAwareCache: listed %d cached filesystem entries", len(result))
	return result
}

// Refresh refreshes cache from AWS API with metrics collection
func (c *MetricsAwareCache) Refresh(ctx context.Context, namespace string) error {
	recorder := NewMetricsRecorder(c.metrics, "cache_refresh", namespace)

	err := c.cache.Refresh(ctx, namespace)

	if err != nil {
		recorder.RecordError(err)
		return err
	}

	recorder.RecordSuccess()

	// Update filesystem count metrics after refresh
	if fsInfo, found := c.cache.Get(namespace); found && fsInfo != nil {
		c.metrics.RecordFileSystemCount(namespace, fsInfo.ClusterID, string(fsInfo.State), 1)
	}

	return nil
}

// SetTTL sets cache TTL for entries
func (c *MetricsAwareCache) SetTTL(duration time.Duration) {
	c.cache.SetTTL(duration)
}

// Clear removes all entries from the cache with metrics collection
func (c *MetricsAwareCache) Clear() {
	startTime := time.Now()

	// Get current entries for metrics before clearing
	currentEntries := c.cache.List()

	c.cache.Clear()

	// Record operation duration
	duration := time.Since(startTime)
	c.metrics.RecordOperationDuration("cache_clear", "", "success", duration)

	// Set all filesystem counts to 0 since we cleared the cache
	for namespace, fsInfo := range currentEntries {
		if fsInfo != nil {
			c.metrics.RecordFileSystemCount(namespace, fsInfo.ClusterID, string(fsInfo.State), 0)
		}
	}

	klog.V(4).Infof("MetricsAwareCache: cleared %d cached filesystem entries", len(currentEntries))
}

// GetSize returns the number of entries in cache
func (c *MetricsAwareCache) GetSize() int {
	return c.cache.GetSize()
}

// updateFileSystemCountMetrics updates filesystem count metrics for all entries
func (c *MetricsAwareCache) updateFileSystemCountMetrics(entries map[string]*FileSystemInfo) {
	// Group by cluster and state
	stateCounts := make(map[string]map[string]int) // cluster -> state -> count

	for _, fsInfo := range entries {
		if fsInfo == nil {
			continue
		}

		clusterID := fsInfo.ClusterID
		state := string(fsInfo.State)

		if stateCounts[clusterID] == nil {
			stateCounts[clusterID] = make(map[string]int)
		}
		stateCounts[clusterID][state]++
	}

	// Record metrics for each cluster and state combination
	for clusterID, states := range stateCounts {
		for state, count := range states {
			// For list operations, we record the total count per state
			// We use an empty namespace since this represents aggregate data
			c.metrics.RecordFileSystemCount("", clusterID, state, float64(count))
		}
	}
}

// FileSystemMetricsCollector provides higher-level metrics collection for filesystem operations
type FileSystemMetricsCollector struct {
	metrics MetricsCollector
}

// NewFileSystemMetricsCollector creates a new filesystem metrics collector
func NewFileSystemMetricsCollector(metrics MetricsCollector) *FileSystemMetricsCollector {
	return &FileSystemMetricsCollector{
		metrics: metrics,
	}
}

// RecordFileSystemCreated records a filesystem creation event
func (f *FileSystemMetricsCollector) RecordFileSystemCreated(namespace, clusterID string, duration time.Duration) {
	f.metrics.RecordOperationDuration("filesystem_create", namespace, "success", duration)
	f.metrics.RecordOperationTotal("filesystem_create", namespace, "success")
	f.metrics.RecordFileSystemCount(namespace, clusterID, string(FileSystemStateAvailable), 1)
	f.metrics.RecordFileSystemStateChange(namespace, clusterID, "", string(FileSystemStateAvailable))

	klog.V(4).Infof("FileSystemMetricsCollector: recorded filesystem creation for namespace %s (duration: %v)", namespace, duration)
}

// RecordFileSystemCreationFailed records a filesystem creation failure
func (f *FileSystemMetricsCollector) RecordFileSystemCreationFailed(namespace, clusterID string, duration time.Duration, err error) {
	f.metrics.RecordOperationDuration("filesystem_create", namespace, "error", duration)
	f.metrics.RecordOperationTotal("filesystem_create", namespace, "error")

	klog.V(4).Infof("FileSystemMetricsCollector: recorded filesystem creation failure for namespace %s (duration: %v, error: %v)", namespace, duration, err)
}

// RecordFileSystemDeleted records a filesystem deletion event
func (f *FileSystemMetricsCollector) RecordFileSystemDeleted(namespace, clusterID string, duration time.Duration) {
	f.metrics.RecordOperationDuration("filesystem_delete", namespace, "success", duration)
	f.metrics.RecordOperationTotal("filesystem_delete", namespace, "success")
	f.metrics.RecordFileSystemCount(namespace, clusterID, string(FileSystemStateDeleted), 0)
	f.metrics.RecordFileSystemStateChange(namespace, clusterID, string(FileSystemStateAvailable), string(FileSystemStateDeleted))

	klog.V(4).Infof("FileSystemMetricsCollector: recorded filesystem deletion for namespace %s (duration: %v)", namespace, duration)
}

// RecordFileSystemDeletionFailed records a filesystem deletion failure
func (f *FileSystemMetricsCollector) RecordFileSystemDeletionFailed(namespace, clusterID string, duration time.Duration, err error) {
	f.metrics.RecordOperationDuration("filesystem_delete", namespace, "error", duration)
	f.metrics.RecordOperationTotal("filesystem_delete", namespace, "error")

	klog.V(4).Infof("FileSystemMetricsCollector: recorded filesystem deletion failure for namespace %s (duration: %v, error: %v)", namespace, duration, err)
}

// RecordFileSystemStateTransition records a filesystem state transition
func (f *FileSystemMetricsCollector) RecordFileSystemStateTransition(namespace, clusterID string, fromState, toState FileSystemState) {
	f.metrics.RecordFileSystemStateChange(namespace, clusterID, string(fromState), string(toState))
	f.metrics.RecordFileSystemCount(namespace, clusterID, string(toState), 1)

	klog.V(4).Infof("FileSystemMetricsCollector: recorded filesystem state transition for namespace %s: %s -> %s", namespace, fromState, toState)
}

// GetMetrics returns the underlying metrics collector
func (f *FileSystemMetricsCollector) GetMetrics() MetricsCollector {
	return f.metrics
}
