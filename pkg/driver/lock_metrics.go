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
	"sync"
	"time"
)

// LockMetrics tracks metrics for distributed locks
type LockMetrics struct {
	mu sync.RWMutex

	// Counters
	acquisitions      int64
	releases          int64
	conflicts         int64
	timeouts          int64
	renewalFailures   int64
	forceReleases     int64

	// Histograms (simplified - in production use prometheus.Histogram)
	acquisitionTimes  []time.Duration
	holdTimes         []time.Duration
	waitTimes         []time.Duration

	// Gauges
	activeLocks       int64
	waitingLocks      int64

	// Per-lock metrics
	lockMetrics       map[string]*LockStat
}

// LockStat contains statistics for a specific lock
type LockStat struct {
	Key              string
	Acquisitions     int64
	Releases         int64
	Conflicts        int64
	TotalHoldTime    time.Duration
	TotalWaitTime    time.Duration
	LastAcquired     time.Time
	LastReleased     time.Time
	CurrentHolder    string
	IsHeld           bool
}

// NewLockMetrics creates a new lock metrics collector
func NewLockMetrics() *LockMetrics {
	return &LockMetrics{
		acquisitionTimes: make([]time.Duration, 0),
		holdTimes:        make([]time.Duration, 0),
		waitTimes:        make([]time.Duration, 0),
		lockMetrics:      make(map[string]*LockStat),
	}
}

// RecordAcquisition records a successful lock acquisition
func (m *LockMetrics) RecordAcquisition(lockKey string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.acquisitions++
	m.activeLocks++

	// Update per-lock stats
	stat, exists := m.lockMetrics[lockKey]
	if !exists {
		stat = &LockStat{Key: lockKey}
		m.lockMetrics[lockKey] = stat
	}

	stat.Acquisitions++
	stat.LastAcquired = time.Now()
	stat.IsHeld = true
}

// RecordRelease records a lock release
func (m *LockMetrics) RecordRelease(lockKey string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.releases++
	if m.activeLocks > 0 {
		m.activeLocks--
	}

	// Update per-lock stats
	if stat, exists := m.lockMetrics[lockKey]; exists {
		stat.Releases++
		stat.LastReleased = time.Now()
		stat.IsHeld = false

		// Calculate hold time
		if !stat.LastAcquired.IsZero() {
			holdTime := stat.LastReleased.Sub(stat.LastAcquired)
			stat.TotalHoldTime += holdTime
			m.holdTimes = append(m.holdTimes, holdTime)
		}
	}
}

// RecordConflict records a lock conflict
func (m *LockMetrics) RecordConflict(lockKey string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.conflicts++

	if stat, exists := m.lockMetrics[lockKey]; exists {
		stat.Conflicts++
	} else {
		m.lockMetrics[lockKey] = &LockStat{
			Key:       lockKey,
			Conflicts: 1,
		}
	}
}

// RecordTimeout records a lock acquisition timeout
func (m *LockMetrics) RecordTimeout(lockKey string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.timeouts++
}

// RecordRenewalFailure records a lease renewal failure
func (m *LockMetrics) RecordRenewalFailure(lockKey string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.renewalFailures++
}

// RecordForceRelease records a forced lock release
func (m *LockMetrics) RecordForceRelease(lockKey string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.forceReleases++
}

// RecordWaitTime records the time spent waiting for a lock
func (m *LockMetrics) RecordWaitTime(lockKey string, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.waitTimes = append(m.waitTimes, duration)

	if stat, exists := m.lockMetrics[lockKey]; exists {
		stat.TotalWaitTime += duration
	}
}

// RecordAcquisitionTime records the time taken to acquire a lock
func (m *LockMetrics) RecordAcquisitionTime(lockKey string, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.acquisitionTimes = append(m.acquisitionTimes, duration)
}

// IncrementWaiting increments the waiting locks counter
func (m *LockMetrics) IncrementWaiting() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waitingLocks++
}

// DecrementWaiting decrements the waiting locks counter
func (m *LockMetrics) DecrementWaiting() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.waitingLocks > 0 {
		m.waitingLocks--
	}
}

// GetMetrics returns all metrics
func (m *LockMetrics) GetMetrics() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Calculate percentiles for timing metrics
	p50Acquisition := calculatePercentile(m.acquisitionTimes, 50)
	p95Acquisition := calculatePercentile(m.acquisitionTimes, 95)
	p99Acquisition := calculatePercentile(m.acquisitionTimes, 99)

	p50Hold := calculatePercentile(m.holdTimes, 50)
	p95Hold := calculatePercentile(m.holdTimes, 95)
	p99Hold := calculatePercentile(m.holdTimes, 99)

	p50Wait := calculatePercentile(m.waitTimes, 50)
	p95Wait := calculatePercentile(m.waitTimes, 95)
	p99Wait := calculatePercentile(m.waitTimes, 99)

	return map[string]interface{}{
		// Counters
		"total_acquisitions":    m.acquisitions,
		"total_releases":        m.releases,
		"total_conflicts":       m.conflicts,
		"total_timeouts":        m.timeouts,
		"renewal_failures":      m.renewalFailures,
		"force_releases":        m.forceReleases,

		// Gauges
		"active_locks":          m.activeLocks,
		"waiting_locks":         m.waitingLocks,

		// Timing percentiles
		"acquisition_time_p50": p50Acquisition,
		"acquisition_time_p95": p95Acquisition,
		"acquisition_time_p99": p99Acquisition,

		"hold_time_p50":         p50Hold,
		"hold_time_p95":         p95Hold,
		"hold_time_p99":         p99Hold,

		"wait_time_p50":         p50Wait,
		"wait_time_p95":         p95Wait,
		"wait_time_p99":         p99Wait,

		// Lock count
		"unique_locks":          len(m.lockMetrics),
	}
}

// GetLockStats returns statistics for a specific lock
func (m *LockMetrics) GetLockStats(lockKey string) *LockStat {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if stat, exists := m.lockMetrics[lockKey]; exists {
		// Return a copy to avoid race conditions
		return &LockStat{
			Key:           stat.Key,
			Acquisitions:  stat.Acquisitions,
			Releases:      stat.Releases,
			Conflicts:     stat.Conflicts,
			TotalHoldTime: stat.TotalHoldTime,
			TotalWaitTime: stat.TotalWaitTime,
			LastAcquired:  stat.LastAcquired,
			LastReleased:  stat.LastReleased,
			CurrentHolder: stat.CurrentHolder,
			IsHeld:        stat.IsHeld,
		}
	}

	return nil
}

// GetTopContendedLocks returns the most contended locks
func (m *LockMetrics) GetTopContendedLocks(count int) []*LockStat {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Create a slice of all lock stats
	stats := make([]*LockStat, 0, len(m.lockMetrics))
	for _, stat := range m.lockMetrics {
		stats = append(stats, &LockStat{
			Key:           stat.Key,
			Acquisitions:  stat.Acquisitions,
			Releases:      stat.Releases,
			Conflicts:     stat.Conflicts,
			TotalHoldTime: stat.TotalHoldTime,
			TotalWaitTime: stat.TotalWaitTime,
			LastAcquired:  stat.LastAcquired,
			LastReleased:  stat.LastReleased,
			CurrentHolder: stat.CurrentHolder,
			IsHeld:        stat.IsHeld,
		})
	}

	// Sort by conflict count (simple bubble sort for small datasets)
	for i := 0; i < len(stats)-1; i++ {
		for j := 0; j < len(stats)-i-1; j++ {
			if stats[j].Conflicts < stats[j+1].Conflicts {
				stats[j], stats[j+1] = stats[j+1], stats[j]
			}
		}
	}

	// Return top N
	if count > len(stats) {
		count = len(stats)
	}

	return stats[:count]
}

// Reset resets all metrics
func (m *LockMetrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.acquisitions = 0
	m.releases = 0
	m.conflicts = 0
	m.timeouts = 0
	m.renewalFailures = 0
	m.forceReleases = 0
	m.activeLocks = 0
	m.waitingLocks = 0

	m.acquisitionTimes = make([]time.Duration, 0)
	m.holdTimes = make([]time.Duration, 0)
	m.waitTimes = make([]time.Duration, 0)
	m.lockMetrics = make(map[string]*LockStat)
}

// calculatePercentile calculates the percentile value from a slice of durations
func calculatePercentile(durations []time.Duration, percentile float64) time.Duration {
	if len(durations) == 0 {
		return 0
	}

	// Create a copy and sort
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)

	// Simple bubble sort (fine for small datasets)
	for i := 0; i < len(sorted)-1; i++ {
		for j := 0; j < len(sorted)-i-1; j++ {
			if sorted[j] > sorted[j+1] {
				sorted[j], sorted[j+1] = sorted[j+1], sorted[j]
			}
		}
	}

	// Calculate the index for the percentile
	index := int(float64(len(sorted)-1) * percentile / 100)
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}

	return sorted[index]
}