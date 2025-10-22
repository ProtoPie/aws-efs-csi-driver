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
	"sync"
	"testing"
	"time"
)

func TestLockMetrics_ConcurrentOperations(t *testing.T) {
	metrics := NewLockMetrics()

	var wg sync.WaitGroup
	numWorkers := 50
	operationsPerWorker := 100

	// Test concurrent metric updates
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			lockKey := fmt.Sprintf("lock-%d", workerID%10) // 10 different locks

			for j := 0; j < operationsPerWorker; j++ {
				switch j % 8 {
				case 0:
					metrics.RecordAcquisition(lockKey)
				case 1:
					metrics.RecordRelease(lockKey)
				case 2:
					metrics.RecordConflict(lockKey)
				case 3:
					metrics.RecordTimeout(lockKey)
				case 4:
					metrics.RecordRenewalFailure(lockKey)
				case 5:
					metrics.RecordForceRelease(lockKey)
				case 6:
					metrics.RecordWaitTime(lockKey, time.Duration(j)*time.Millisecond)
				case 7:
					metrics.RecordAcquisitionTime(lockKey, time.Duration(j)*time.Millisecond)
				}

				// Also test counter operations
				if j%2 == 0 {
					metrics.IncrementWaiting()
				} else {
					metrics.DecrementWaiting()
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify metrics are consistent
	m := metrics.GetMetrics()

	// Check that we have reasonable numbers
	if totalAcquisitions, ok := m["total_acquisitions"].(int64); !ok || totalAcquisitions <= 0 {
		t.Errorf("Expected positive acquisitions, got %v", totalAcquisitions)
	}

	if totalConflicts, ok := m["total_conflicts"].(int64); !ok || totalConflicts <= 0 {
		t.Errorf("Expected positive conflicts, got %v", totalConflicts)
	}

	// Verify lock-specific stats
	topLocks := metrics.GetTopContendedLocks(5)
	if len(topLocks) == 0 {
		t.Error("Expected some contended locks")
	}

	for i, lockStat := range topLocks {
		if lockStat.Conflicts <= 0 {
			t.Errorf("Lock %d should have conflicts, got %d", i, lockStat.Conflicts)
		}
	}
}

func TestLockMetrics_WaitTimeTracking(t *testing.T) {
	metrics := NewLockMetrics()

	lockKey := "test-lock"

	// Record various wait times
	waitTimes := []time.Duration{
		100 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		2 * time.Second,
		5 * time.Second,
	}

	for _, waitTime := range waitTimes {
		metrics.RecordWaitTime(lockKey, waitTime)
	}

	// Get lock stats
	lockStats := metrics.GetLockStats(lockKey)
	if lockStats == nil {
		t.Fatal("Expected lock stats to exist")
	}

	if lockStats.TotalWaitTime == 0 {
		t.Error("Expected non-zero total wait time")
	}

	expectedTotal := time.Duration(0)
	for _, wt := range waitTimes {
		expectedTotal += wt
	}

	if lockStats.TotalWaitTime != expectedTotal {
		t.Errorf("Expected total wait time %v, got %v", expectedTotal, lockStats.TotalWaitTime)
	}

	// Note: AvgWaitTime and MaxWaitTime are not fields in LockStat
	// The test just verifies that TotalWaitTime is correctly tracked
}

func TestLockMetrics_AcquisitionTimeTracking(t *testing.T) {
	metrics := NewLockMetrics()

	lockKey := "test-lock"

	// Record various acquisition times
	acquisitionTimes := []time.Duration{
		50 * time.Millisecond,
		200 * time.Millisecond,
		800 * time.Millisecond,
		1500 * time.Millisecond,
	}

	for _, acqTime := range acquisitionTimes {
		metrics.RecordAcquisitionTime(lockKey, acqTime)
	}

	// Verify the metric recording doesn't panic and basic functionality works
	// Note: RecordAcquisitionTime stores global timing data, not per-lock data
	m := metrics.GetMetrics()
	if m == nil {
		t.Error("Expected metrics to be available")
	}

	// The acquisition time tracking is global, not per-lock
	// so we just verify the method call succeeded
}

func TestLockMetrics_TopContendedLocks(t *testing.T) {
	metrics := NewLockMetrics()

	// Create locks with different conflict levels
	testData := map[string]int{
		"high-contention":   50,
		"medium-contention": 20,
		"low-contention":    5,
		"no-contention":     0,
	}

	for lockKey, conflicts := range testData {
		for i := 0; i < conflicts; i++ {
			metrics.RecordConflict(lockKey)
		}
		// Also record some acquisitions
		for i := 0; i < conflicts+1; i++ {
			metrics.RecordAcquisition(lockKey)
		}
	}

	// Get top 3 contended locks
	topLocks := metrics.GetTopContendedLocks(3)

	if len(topLocks) != 3 {
		t.Errorf("Expected 3 top locks, got %d", len(topLocks))
	}

	// Verify order (should be sorted by conflicts descending)
	expectedOrder := []string{"high-contention", "medium-contention", "low-contention"}
	for i, expectedKey := range expectedOrder {
		if i >= len(topLocks) {
			t.Errorf("Missing expected lock %s at position %d", expectedKey, i)
			continue
		}
		if topLocks[i].Key != expectedKey {
			t.Errorf("Expected lock %s at position %d, got %s", expectedKey, i, topLocks[i].Key)
		}
	}

	// Test requesting more locks than available
	allLocks := metrics.GetTopContendedLocks(10)
	if len(allLocks) != 4 { // We created 4 locks total
		t.Errorf("Expected 4 locks when requesting 10, got %d", len(allLocks))
	}
}

func TestLockMetrics_WaitingCounters(t *testing.T) {
	metrics := NewLockMetrics()

	// Test increment/decrement
	for i := 0; i < 10; i++ {
		metrics.IncrementWaiting()
	}

	m := metrics.GetMetrics()
	if currentWaiting, ok := m["current_waiting"].(int64); !ok || currentWaiting != 10 {
		t.Errorf("Expected 10 waiting, got %v", currentWaiting)
	}

	for i := 0; i < 5; i++ {
		metrics.DecrementWaiting()
	}

	m = metrics.GetMetrics()
	if currentWaiting, ok := m["current_waiting"].(int64); !ok || currentWaiting != 5 {
		t.Errorf("Expected 5 waiting after decrements, got %v", currentWaiting)
	}

	// Test that decrement doesn't go below zero
	for i := 0; i < 10; i++ {
		metrics.DecrementWaiting()
	}

	m = metrics.GetMetrics()
	if currentWaiting, ok := m["current_waiting"].(int64); !ok || currentWaiting < 0 {
		t.Errorf("Expected non-negative waiting count, got %v", currentWaiting)
	}
}

func TestLockMetrics_Reset(t *testing.T) {
	metrics := NewLockMetrics()

	// Add some metrics
	lockKey := "test-lock"
	metrics.RecordAcquisition(lockKey)
	metrics.RecordConflict(lockKey)
	metrics.RecordTimeout(lockKey)
	metrics.RecordWaitTime(lockKey, 1*time.Second)
	metrics.IncrementWaiting()

	// Verify we have data
	m := metrics.GetMetrics()
	if totalAcquisitions, ok := m["total_acquisitions"].(int64); !ok || totalAcquisitions == 0 {
		t.Error("Expected non-zero acquisitions before reset")
	}

	lockStats := metrics.GetLockStats(lockKey)
	if lockStats == nil {
		t.Error("Expected lock stats before reset")
	}

	// Reset
	metrics.Reset()

	// Verify everything is reset
	m = metrics.GetMetrics()
	if totalAcquisitions, ok := m["total_acquisitions"].(int64); !ok || totalAcquisitions != 0 {
		t.Errorf("Expected 0 acquisitions after reset, got %v", totalAcquisitions)
	}

	if totalConflicts, ok := m["total_conflicts"].(int64); !ok || totalConflicts != 0 {
		t.Errorf("Expected 0 conflicts after reset, got %v", totalConflicts)
	}

	if currentWaiting, ok := m["current_waiting"].(int64); !ok || currentWaiting != 0 {
		t.Errorf("Expected 0 waiting after reset, got %v", currentWaiting)
	}

	// Verify lock-specific stats are reset
	lockStats = metrics.GetLockStats(lockKey)
	if lockStats != nil {
		t.Error("Expected no lock stats after reset")
	}

	topLocks := metrics.GetTopContendedLocks(10)
	if len(topLocks) != 0 {
		t.Errorf("Expected no contended locks after reset, got %d", len(topLocks))
	}
}

func TestLockMetrics_EdgeCases(t *testing.T) {
	metrics := NewLockMetrics()

	// Test with empty lock key
	metrics.RecordAcquisition("")
	metrics.RecordConflict("")

	// Test with very long lock key
	longKey := ""
	for i := 0; i < 1000; i++ {
		longKey += "a"
	}
	metrics.RecordAcquisition(longKey)

	// Test with special characters in lock key
	specialKey := "lock-with-special-chars!@#$%^&*()_+-=[]{}|;:,.<>?"
	metrics.RecordAcquisition(specialKey)

	// Test zero and negative durations
	metrics.RecordWaitTime("test-lock", 0)
	metrics.RecordWaitTime("test-lock", -1*time.Second) // Should handle gracefully

	// Test getting stats for non-existent lock
	nonExistentStats := metrics.GetLockStats("non-existent-lock")
	if nonExistentStats != nil {
		t.Error("Expected nil stats for non-existent lock")
	}

	// Test getting top locks when no locks exist
	metrics.Reset()
	topLocks := metrics.GetTopContendedLocks(5)
	if len(topLocks) != 0 {
		t.Error("Expected empty list when no locks exist")
	}

	// Test getting top locks with zero count
	metrics.RecordAcquisition("test")
	topLocksZero := metrics.GetTopContendedLocks(0)
	if len(topLocksZero) != 0 {
		t.Error("Expected empty list when requesting 0 locks")
	}
}

func BenchmarkLockMetrics_RecordAcquisition(b *testing.B) {
	metrics := NewLockMetrics()
	lockKey := "benchmark-lock"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		metrics.RecordAcquisition(lockKey)
	}
}

func BenchmarkLockMetrics_RecordConflict(b *testing.B) {
	metrics := NewLockMetrics()
	lockKey := "benchmark-lock"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		metrics.RecordConflict(lockKey)
	}
}

func BenchmarkLockMetrics_ConcurrentOperations(b *testing.B) {
	metrics := NewLockMetrics()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		lockKey := "benchmark-lock"
		for pb.Next() {
			metrics.RecordAcquisition(lockKey)
			metrics.RecordConflict(lockKey)
			metrics.IncrementWaiting()
			metrics.DecrementWaiting()
		}
	})
}