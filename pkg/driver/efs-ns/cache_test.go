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
	"fmt"
	"sync"
	"testing"
	"time"
)

// Test data
var (
	testNamespace1    = "test-namespace-1"
	testNamespace2    = "test-namespace-2"
	testNamespace3    = "test-namespace-3"
	testFileSystemID1 = "fs-12345678901234567"
	testFileSystemID2 = "fs-87654321098765432"
	testClusterID     = "test-cluster-123"
)

func createTestFileSystemInfo(namespace, fsID string) *FileSystemInfo {
	return &FileSystemInfo{
		FileSystemID:    fsID,
		Namespace:       namespace,
		ClusterID:       testClusterID,
		CreatedAt:       time.Now(),
		MountTargets:    []*MountTargetInfo{},
		SecurityGroupID: "sg-test123",
		State:           FileSystemStateAvailable,
		PVCCount:        1,
		Tags:            map[string]string{"test": "true"},
		PerformanceMode: "generalPurpose",
		ThroughputMode:  "bursting",
		Encrypted:       true,
	}
}

// Mock refresh function that always succeeds
func mockRefreshFunc(ctx context.Context, namespace string) (*FileSystemInfo, error) {
	return createTestFileSystemInfo(namespace, testFileSystemID1), nil
}

// Mock refresh function that always fails
func mockRefreshFuncFail(ctx context.Context, namespace string) (*FileSystemInfo, error) {
	return nil, errors.New("AWS API error")
}

// Mock refresh function that returns nil (filesystem not found)
func mockRefreshFuncNotFound(ctx context.Context, namespace string) (*FileSystemInfo, error) {
	return nil, nil
}

func TestNewFileSystemCache(t *testing.T) {
	cache := NewFileSystemCache(mockRefreshFunc)
	if cache == nil {
		t.Fatal("Expected cache to be non-nil")
	}

	// Test that the cache starts empty
	if cache.GetSize() != 0 {
		t.Errorf("Expected cache size to be 0, got %d", cache.GetSize())
	}

	// Clean up
	if memCache, ok := cache.(*memoryCache); ok {
		memCache.Close()
	}
}

func TestFileSystemCache_SetAndGet(t *testing.T) {
	cache := NewFileSystemCache(mockRefreshFunc)
	defer func() {
		if memCache, ok := cache.(*memoryCache); ok {
			memCache.Close()
		}
	}()

	fsInfo := createTestFileSystemInfo(testNamespace1, testFileSystemID1)

	// Test Set
	cache.Set(testNamespace1, fsInfo)

	// Test Get - should find the entry
	retrieved, found := cache.Get(testNamespace1)
	if !found {
		t.Fatal("Expected to find cached entry")
	}
	if retrieved.FileSystemID != fsInfo.FileSystemID {
		t.Errorf("Expected FileSystemID %s, got %s", fsInfo.FileSystemID, retrieved.FileSystemID)
	}
	if retrieved.Namespace != fsInfo.Namespace {
		t.Errorf("Expected Namespace %s, got %s", fsInfo.Namespace, retrieved.Namespace)
	}

	// Test Get with non-existent namespace
	_, found = cache.Get("non-existent")
	if found {
		t.Error("Expected not to find non-existent entry")
	}
}

func TestFileSystemCache_SetWithInvalidParameters(t *testing.T) {
	cache := NewFileSystemCache(mockRefreshFunc)
	defer func() {
		if memCache, ok := cache.(*memoryCache); ok {
			memCache.Close()
		}
	}()

	fsInfo := createTestFileSystemInfo(testNamespace1, testFileSystemID1)

	// Test Set with empty namespace - should not crash and should not store
	cache.Set("", fsInfo)
	if cache.GetSize() != 0 {
		t.Error("Expected cache to remain empty when setting with empty namespace")
	}

	// Test Set with nil fsInfo - should not crash and should not store
	cache.Set(testNamespace1, nil)
	if cache.GetSize() != 0 {
		t.Error("Expected cache to remain empty when setting with nil fsInfo")
	}
}

func TestFileSystemCache_GetWithInvalidParameters(t *testing.T) {
	cache := NewFileSystemCache(mockRefreshFunc)
	defer func() {
		if memCache, ok := cache.(*memoryCache); ok {
			memCache.Close()
		}
	}()

	// Test Get with empty namespace
	_, found := cache.Get("")
	if found {
		t.Error("Expected not to find entry with empty namespace")
	}
}

func TestFileSystemCache_Delete(t *testing.T) {
	cache := NewFileSystemCache(mockRefreshFunc)
	defer func() {
		if memCache, ok := cache.(*memoryCache); ok {
			memCache.Close()
		}
	}()

	fsInfo := createTestFileSystemInfo(testNamespace1, testFileSystemID1)

	// Add entry
	cache.Set(testNamespace1, fsInfo)
	if cache.GetSize() != 1 {
		t.Fatal("Expected cache size to be 1")
	}

	// Delete entry
	cache.Delete(testNamespace1)
	if cache.GetSize() != 0 {
		t.Error("Expected cache size to be 0 after delete")
	}

	// Test Get after delete - should not find the entry
	_, found := cache.Get(testNamespace1)
	if found {
		t.Error("Expected not to find deleted entry")
	}

	// Test delete non-existent entry - should not crash
	cache.Delete("non-existent")

	// Test delete with empty namespace - should not crash
	cache.Delete("")
}

func TestFileSystemCache_List(t *testing.T) {
	cache := NewFileSystemCache(mockRefreshFunc)
	defer func() {
		if memCache, ok := cache.(*memoryCache); ok {
			memCache.Close()
		}
	}()

	fsInfo1 := createTestFileSystemInfo(testNamespace1, testFileSystemID1)
	fsInfo2 := createTestFileSystemInfo(testNamespace2, testFileSystemID2)

	// Test empty list
	list := cache.List()
	if len(list) != 0 {
		t.Errorf("Expected empty list, got %d entries", len(list))
	}

	// Add entries
	cache.Set(testNamespace1, fsInfo1)
	cache.Set(testNamespace2, fsInfo2)

	// Test list with entries
	list = cache.List()
	if len(list) != 2 {
		t.Errorf("Expected 2 entries in list, got %d", len(list))
	}

	// Check that both entries are present
	if entry1, exists := list[testNamespace1]; !exists {
		t.Error("Expected to find entry for testNamespace1")
	} else if entry1.FileSystemID != testFileSystemID1 {
		t.Errorf("Expected FileSystemID %s for testNamespace1, got %s", testFileSystemID1, entry1.FileSystemID)
	}

	if entry2, exists := list[testNamespace2]; !exists {
		t.Error("Expected to find entry for testNamespace2")
	} else if entry2.FileSystemID != testFileSystemID2 {
		t.Errorf("Expected FileSystemID %s for testNamespace2, got %s", testFileSystemID2, entry2.FileSystemID)
	}
}

func TestFileSystemCache_Clear(t *testing.T) {
	cache := NewFileSystemCache(mockRefreshFunc)
	defer func() {
		if memCache, ok := cache.(*memoryCache); ok {
			memCache.Close()
		}
	}()

	fsInfo1 := createTestFileSystemInfo(testNamespace1, testFileSystemID1)
	fsInfo2 := createTestFileSystemInfo(testNamespace2, testFileSystemID2)

	// Add entries
	cache.Set(testNamespace1, fsInfo1)
	cache.Set(testNamespace2, fsInfo2)
	if cache.GetSize() != 2 {
		t.Fatal("Expected cache size to be 2")
	}

	// Clear cache
	cache.Clear()
	if cache.GetSize() != 0 {
		t.Error("Expected cache size to be 0 after clear")
	}

	// Verify entries are gone
	_, found1 := cache.Get(testNamespace1)
	_, found2 := cache.Get(testNamespace2)
	if found1 || found2 {
		t.Error("Expected not to find any entries after clear")
	}
}

func TestFileSystemCache_TTL(t *testing.T) {
	cache := NewFileSystemCache(mockRefreshFunc)
	defer func() {
		if memCache, ok := cache.(*memoryCache); ok {
			memCache.Close()
		}
	}()

	// Set a very short TTL for testing
	shortTTL := 100 * time.Millisecond
	cache.SetTTL(shortTTL)

	fsInfo := createTestFileSystemInfo(testNamespace1, testFileSystemID1)

	// Add entry
	cache.Set(testNamespace1, fsInfo)

	// Should be able to get it immediately
	_, found := cache.Get(testNamespace1)
	if !found {
		t.Fatal("Expected to find recently cached entry")
	}

	// Wait for TTL to expire
	time.Sleep(shortTTL + 50*time.Millisecond)

	// Should not find it after expiration
	_, found = cache.Get(testNamespace1)
	if found {
		t.Error("Expected not to find expired entry")
	}

	// List should also not include expired entries
	list := cache.List()
	if len(list) != 0 {
		t.Errorf("Expected empty list after TTL expiration, got %d entries", len(list))
	}
}

func TestFileSystemCache_TTLDisabled(t *testing.T) {
	cache := NewFileSystemCache(mockRefreshFunc)
	defer func() {
		if memCache, ok := cache.(*memoryCache); ok {
			memCache.Close()
		}
	}()

	// Set TTL to 0 (disabled)
	cache.SetTTL(0)

	fsInfo := createTestFileSystemInfo(testNamespace1, testFileSystemID1)

	// Add entry
	cache.Set(testNamespace1, fsInfo)

	// Wait a bit
	time.Sleep(50 * time.Millisecond)

	// Should still be able to get it (no expiration)
	_, found := cache.Get(testNamespace1)
	if !found {
		t.Error("Expected to find entry when TTL is disabled")
	}
}

func TestFileSystemCache_Refresh(t *testing.T) {
	cache := NewFileSystemCache(mockRefreshFunc)
	defer func() {
		if memCache, ok := cache.(*memoryCache); ok {
			memCache.Close()
		}
	}()

	ctx := context.Background()

	// Test refresh with empty namespace
	err := cache.Refresh(ctx, "")
	if err == nil {
		t.Error("Expected error when refreshing with empty namespace")
	}

	// Test successful refresh
	err = cache.Refresh(ctx, testNamespace1)
	if err != nil {
		t.Fatalf("Expected successful refresh, got error: %v", err)
	}

	// Should be able to get the refreshed entry
	retrieved, found := cache.Get(testNamespace1)
	if !found {
		t.Fatal("Expected to find entry after refresh")
	}
	if retrieved.Namespace != testNamespace1 {
		t.Errorf("Expected namespace %s, got %s", testNamespace1, retrieved.Namespace)
	}
}

func TestFileSystemCache_RefreshWithFailure(t *testing.T) {
	cache := NewFileSystemCache(mockRefreshFuncFail)
	defer func() {
		if memCache, ok := cache.(*memoryCache); ok {
			memCache.Close()
		}
	}()

	ctx := context.Background()

	// Test refresh that fails
	err := cache.Refresh(ctx, testNamespace1)
	if err == nil {
		t.Error("Expected error when refresh function fails")
	}

	// Should not add anything to cache
	if cache.GetSize() != 0 {
		t.Error("Expected cache to remain empty after failed refresh")
	}
}

func TestFileSystemCache_RefreshNotFound(t *testing.T) {
	cache := NewFileSystemCache(mockRefreshFunc)
	defer func() {
		if memCache, ok := cache.(*memoryCache); ok {
			memCache.Close()
		}
	}()

	fsInfo := createTestFileSystemInfo(testNamespace1, testFileSystemID1)

	// Add entry to cache first
	cache.Set(testNamespace1, fsInfo)
	if cache.GetSize() != 1 {
		t.Fatal("Expected cache size to be 1")
	}

	// Change refresh function to return not found
	cache.(*memoryCache).refreshFunc = mockRefreshFuncNotFound

	ctx := context.Background()

	// Refresh should succeed but remove the entry
	err := cache.Refresh(ctx, testNamespace1)
	if err != nil {
		t.Fatalf("Expected successful refresh, got error: %v", err)
	}

	// Entry should be removed from cache
	if cache.GetSize() != 0 {
		t.Error("Expected cache size to be 0 after refresh with not found")
	}
}

func TestFileSystemCache_RefreshWithNoRefreshFunction(t *testing.T) {
	cache := NewFileSystemCache(nil)
	defer func() {
		if memCache, ok := cache.(*memoryCache); ok {
			memCache.Close()
		}
	}()

	ctx := context.Background()

	// Test refresh with no refresh function configured
	err := cache.Refresh(ctx, testNamespace1)
	if err == nil {
		t.Error("Expected error when no refresh function is configured")
	}
}

func TestFileSystemCache_ConcurrentAccess(t *testing.T) {
	cache := NewFileSystemCache(mockRefreshFunc)
	defer func() {
		if memCache, ok := cache.(*memoryCache); ok {
			memCache.Close()
		}
	}()

	const numGoroutines = 100
	const numOperations = 10

	var wg sync.WaitGroup

	// Test concurrent Set and Get operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			namespace := fmt.Sprintf("namespace-%d", id)
			fsInfo := createTestFileSystemInfo(namespace, fmt.Sprintf("fs-%d", id))

			for j := 0; j < numOperations; j++ {
				// Set entry
				cache.Set(namespace, fsInfo)

				// Get entry
				retrieved, found := cache.Get(namespace)
				if found && retrieved.Namespace != namespace {
					t.Errorf("Concurrent access error: expected namespace %s, got %s", namespace, retrieved.Namespace)
				}

				// Delete entry
				if j%2 == 0 {
					cache.Delete(namespace)
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestFileSystemCache_ConcurrentRefresh(t *testing.T) {
	cache := NewFileSystemCache(mockRefreshFunc)
	defer func() {
		if memCache, ok := cache.(*memoryCache); ok {
			memCache.Close()
		}
	}()

	const numGoroutines = 20

	var wg sync.WaitGroup
	ctx := context.Background()

	// Test concurrent refresh operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			namespace := fmt.Sprintf("namespace-%d", id)
			err := cache.Refresh(ctx, namespace)
			if err != nil {
				t.Errorf("Concurrent refresh error for namespace %s: %v", namespace, err)
			}
		}(i)
	}

	wg.Wait()

	// Should have entries for all namespaces
	if cache.GetSize() != numGoroutines {
		t.Errorf("Expected cache size %d after concurrent refresh, got %d", numGoroutines, cache.GetSize())
	}
}

func TestFileSystemCache_ConcurrentListAndModify(t *testing.T) {
	cache := NewFileSystemCache(mockRefreshFunc)
	defer func() {
		if memCache, ok := cache.(*memoryCache); ok {
			memCache.Close()
		}
	}()

	const numGoroutines = 50

	var wg sync.WaitGroup

	// Test concurrent List and modify operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			if id%3 == 0 {
				// List operation
				list := cache.List()
				_ = len(list) // Use the result to prevent optimization
			} else if id%3 == 1 {
				// Set operation
				namespace := fmt.Sprintf("namespace-%d", id)
				fsInfo := createTestFileSystemInfo(namespace, fmt.Sprintf("fs-%d", id))
				cache.Set(namespace, fsInfo)
			} else {
				// Delete operation
				namespace := fmt.Sprintf("namespace-%d", id-1)
				cache.Delete(namespace)
			}
		}(i)
	}

	wg.Wait()
}

func TestFileSystemCache_CleanupExpiredEntries(t *testing.T) {
	cache := NewFileSystemCache(mockRefreshFunc)
	memCache := cache.(*memoryCache)
	defer memCache.Close()

	// Set a very short TTL
	shortTTL := 50 * time.Millisecond
	cache.SetTTL(shortTTL)

	fsInfo1 := createTestFileSystemInfo(testNamespace1, testFileSystemID1)
	fsInfo2 := createTestFileSystemInfo(testNamespace2, testFileSystemID2)

	// Add entries
	cache.Set(testNamespace1, fsInfo1)
	cache.Set(testNamespace2, fsInfo2)

	if cache.GetSize() != 2 {
		t.Fatal("Expected cache size to be 2")
	}

	// Wait for entries to expire
	time.Sleep(shortTTL + 50*time.Millisecond)

	// Manually trigger cleanup
	memCache.cleanupExpiredEntries()

	// Should have no entries
	if cache.GetSize() != 0 {
		t.Errorf("Expected cache size to be 0 after cleanup, got %d", cache.GetSize())
	}
}

func TestFileSystemCache_PerformanceBenchmark(t *testing.T) {
	cache := NewFileSystemCache(mockRefreshFunc)
	defer func() {
		if memCache, ok := cache.(*memoryCache); ok {
			memCache.Close()
		}
	}()

	const numEntries = 1000

	// Benchmark Set operations
	start := time.Now()
	for i := 0; i < numEntries; i++ {
		namespace := fmt.Sprintf("namespace-%d", i)
		fsInfo := createTestFileSystemInfo(namespace, fmt.Sprintf("fs-%d", i))
		cache.Set(namespace, fsInfo)
	}
	setDuration := time.Since(start)

	// Benchmark Get operations
	start = time.Now()
	for i := 0; i < numEntries; i++ {
		namespace := fmt.Sprintf("namespace-%d", i)
		_, _ = cache.Get(namespace)
	}
	getDuration := time.Since(start)

	// Performance should be reasonable (<1ms per operation for 1000 entries)
	if setDuration > time.Millisecond*numEntries {
		t.Errorf("Set operations too slow: %v for %d entries", setDuration, numEntries)
	}

	if getDuration > time.Millisecond*numEntries {
		t.Errorf("Get operations too slow: %v for %d entries", getDuration, numEntries)
	}

	t.Logf("Performance: Set %v for %d entries (avg: %v per op), Get %v for %d entries (avg: %v per op)",
		setDuration, numEntries, setDuration/numEntries,
		getDuration, numEntries, getDuration/numEntries)
}

func TestCacheEntry_IsExpired(t *testing.T) {
	// Test entry with no TTL (should never expire)
	entry := &cacheEntry{
		data:      createTestFileSystemInfo(testNamespace1, testFileSystemID1),
		createdAt: time.Now(),
		ttl:       0,
	}
	if entry.isExpired() {
		t.Error("Entry with 0 TTL should not expire")
	}

	// Test entry with negative TTL (should never expire)
	entry.ttl = -1 * time.Hour
	if entry.isExpired() {
		t.Error("Entry with negative TTL should not expire")
	}

	// Test entry that should be expired
	entry.ttl = 10 * time.Millisecond
	entry.createdAt = time.Now().Add(-100 * time.Millisecond)
	if !entry.isExpired() {
		t.Error("Entry should be expired")
	}

	// Test entry that should not be expired
	entry.createdAt = time.Now()
	if entry.isExpired() {
		t.Error("Fresh entry should not be expired")
	}
}
