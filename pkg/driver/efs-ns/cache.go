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
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// FileSystemCache provides thread-safe caching for EFS filesystem information
type FileSystemCache interface {
	// Get retrieves filesystem info from cache
	Get(namespace string) (*FileSystemInfo, bool)

	// Set stores filesystem info in cache
	Set(namespace string, fsInfo *FileSystemInfo)

	// Delete removes filesystem info from cache
	Delete(namespace string)

	// List returns all cached filesystem info
	List() map[string]*FileSystemInfo

	// Refresh refreshes cache from AWS API
	Refresh(ctx context.Context, namespace string) error

	// SetTTL sets cache TTL for entries
	SetTTL(duration time.Duration)

	// Clear removes all entries from the cache
	Clear()

	// GetSize returns the number of entries in cache
	GetSize() int
}

// cacheEntry represents a cache entry with TTL support
type cacheEntry struct {
	data      *FileSystemInfo
	createdAt time.Time
	ttl       time.Duration
}

// isExpired checks if the cache entry has expired
func (e *cacheEntry) isExpired() bool {
	if e.ttl <= 0 {
		return false // No expiration if TTL is 0 or negative
	}
	return time.Since(e.createdAt) > e.ttl
}

// memoryCache implements FileSystemCache using in-memory storage
type memoryCache struct {
	// mutex provides thread-safe access to the cache
	mutex sync.RWMutex

	// data stores the actual cache entries
	data map[string]*cacheEntry

	// ttl is the default time-to-live for cache entries
	ttl time.Duration

	// refreshFunc is the function to refresh data from AWS API
	refreshFunc RefreshFunction

	// cleanupTicker runs periodic cleanup of expired entries
	cleanupTicker *time.Ticker

	// stopCleanup channel to stop the cleanup routine
	stopCleanup chan bool
}

// RefreshFunction defines the signature for AWS API refresh function
type RefreshFunction func(ctx context.Context, namespace string) (*FileSystemInfo, error)

// NewFileSystemCache creates a new FileSystemCache instance
func NewFileSystemCache(refreshFunc RefreshFunction) FileSystemCache {
	cache := &memoryCache{
		data:        make(map[string]*cacheEntry),
		ttl:         DefaultCacheTTL,
		refreshFunc: refreshFunc,
		stopCleanup: make(chan bool),
	}

	// Start cleanup routine that runs every minute
	cache.cleanupTicker = time.NewTicker(time.Minute)
	go cache.cleanupRoutine()

	klog.V(4).Info("FileSystemCache initialized with TTL", "ttl", DefaultCacheTTL)
	return cache
}

// Get retrieves filesystem info from cache
func (c *memoryCache) Get(namespace string) (*FileSystemInfo, bool) {
	if namespace == "" {
		klog.V(4).Info("FileSystemCache.Get called with empty namespace")
		return nil, false
	}

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	entry, exists := c.data[namespace]
	if !exists {
		klog.V(4).Info("FileSystemCache.Get: entry not found", "namespace", namespace)
		return nil, false
	}

	if entry.isExpired() {
		klog.V(4).Info("FileSystemCache.Get: entry expired", "namespace", namespace, "age", time.Since(entry.createdAt))
		return nil, false
	}

	klog.V(5).Info("FileSystemCache.Get: cache hit", "namespace", namespace, "filesystemId", entry.data.FileSystemID)
	return entry.data, true
}

// Set stores filesystem info in cache
func (c *memoryCache) Set(namespace string, fsInfo *FileSystemInfo) {
	if namespace == "" {
		klog.Warning("FileSystemCache.Set called with empty namespace")
		return
	}

	if fsInfo == nil {
		klog.Warning("FileSystemCache.Set called with nil fsInfo", "namespace", namespace)
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.data[namespace] = &cacheEntry{
		data:      fsInfo,
		createdAt: time.Now(),
		ttl:       c.ttl,
	}

	klog.V(4).Info("FileSystemCache.Set: entry cached", "namespace", namespace, "filesystemId", fsInfo.FileSystemID, "ttl", c.ttl)
}

// Delete removes filesystem info from cache
func (c *memoryCache) Delete(namespace string) {
	if namespace == "" {
		klog.V(4).Info("FileSystemCache.Delete called with empty namespace")
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, exists := c.data[namespace]; exists {
		delete(c.data, namespace)
		klog.V(4).Info("FileSystemCache.Delete: entry removed", "namespace", namespace)
	} else {
		klog.V(4).Info("FileSystemCache.Delete: entry not found", "namespace", namespace)
	}
}

// List returns all cached filesystem info (excluding expired entries)
func (c *memoryCache) List() map[string]*FileSystemInfo {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	result := make(map[string]*FileSystemInfo)
	now := time.Now()

	for namespace, entry := range c.data {
		if !entry.isExpired() {
			result[namespace] = entry.data
		} else {
			klog.V(5).Info("FileSystemCache.List: skipping expired entry", "namespace", namespace, "age", now.Sub(entry.createdAt))
		}
	}

	klog.V(5).Info("FileSystemCache.List: returning entries", "count", len(result), "totalCached", len(c.data))
	return result
}

// Refresh refreshes cache from AWS API
func (c *memoryCache) Refresh(ctx context.Context, namespace string) error {
	if namespace == "" {
		return NewEFSNSError(ErrInvalidParameter, "FileSystemCache.Refresh", namespace, "namespace cannot be empty", nil)
	}

	if c.refreshFunc == nil {
		return NewEFSNSError(ErrCacheOperationFailed, "FileSystemCache.Refresh", namespace, "refresh function not configured", nil)
	}

	klog.V(4).Info("FileSystemCache.Refresh: refreshing from AWS API", "namespace", namespace)

	fsInfo, err := c.refreshFunc(ctx, namespace)
	if err != nil {
		return NewEFSNSError(ErrCacheOperationFailed, "FileSystemCache.Refresh", namespace, "failed to refresh from AWS API", err)
	}

	if fsInfo != nil {
		c.Set(namespace, fsInfo)
		klog.V(4).Info("FileSystemCache.Refresh: successfully refreshed", "namespace", namespace, "filesystemId", fsInfo.FileSystemID)
	} else {
		// Remove from cache if it doesn't exist in AWS
		c.Delete(namespace)
		klog.V(4).Info("FileSystemCache.Refresh: filesystem not found in AWS, removed from cache", "namespace", namespace)
	}

	return nil
}

// SetTTL sets cache TTL for entries
func (c *memoryCache) SetTTL(duration time.Duration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	oldTTL := c.ttl
	c.ttl = duration

	klog.V(4).Info("FileSystemCache.SetTTL: TTL updated", "oldTTL", oldTTL, "newTTL", duration)
}

// Clear removes all entries from the cache
func (c *memoryCache) Clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	entryCount := len(c.data)
	c.data = make(map[string]*cacheEntry)

	klog.V(4).Info("FileSystemCache.Clear: cache cleared", "removedEntries", entryCount)
}

// GetSize returns the number of entries in cache
func (c *memoryCache) GetSize() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return len(c.data)
}

// cleanupRoutine periodically removes expired entries
func (c *memoryCache) cleanupRoutine() {
	for {
		select {
		case <-c.cleanupTicker.C:
			c.cleanupExpiredEntries()
		case <-c.stopCleanup:
			c.cleanupTicker.Stop()
			klog.V(4).Info("FileSystemCache cleanup routine stopped")
			return
		}
	}
}

// cleanupExpiredEntries removes expired entries from the cache
func (c *memoryCache) cleanupExpiredEntries() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	expiredCount := 0
	for namespace, entry := range c.data {
		if entry.isExpired() {
			delete(c.data, namespace)
			expiredCount++
		}
	}

	if expiredCount > 0 {
		klog.V(4).Info("FileSystemCache cleanup: removed expired entries", "count", expiredCount, "remaining", len(c.data))
	}
}

// Close stops the cache cleanup routine
func (c *memoryCache) Close() {
	close(c.stopCleanup)
}
