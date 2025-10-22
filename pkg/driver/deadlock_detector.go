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
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// DeadlockDetector detects and resolves deadlock situations in distributed locks
type DeadlockDetector struct {
	manager *DistributedLockManager

	// Lock dependency tracking
	dependencies     map[string][]string // lock -> waiting for locks
	dependenciesMutex sync.RWMutex

	// Wait chain tracking
	waitChains       map[string]*WaitChain
	waitChainsMutex  sync.RWMutex

	// Detection state
	lastCheckTime    time.Time
	deadlockCount    int64
	resolvedCount    int64

	// Configuration
	checkInterval    time.Duration
	warningThreshold time.Duration
	criticalThreshold time.Duration
}

// WaitChain represents a chain of lock dependencies
type WaitChain struct {
	StartLock    string
	Chain        []string
	CreatedAt    time.Time
	LastUpdated  time.Time
	IsCycle      bool
}

// LockDependency represents a lock waiting relationship
type LockDependency struct {
	Holder       string
	Waiter       string
	LockKey      string
	WaitStarted  time.Time
	Priority     int
}

// DeadlockInfo contains information about a detected deadlock
type DeadlockInfo struct {
	Cycle        []string
	DetectedAt   time.Time
	AffectedLocks []string
	Resolution   string
}

// NewDeadlockDetector creates a new deadlock detector
func NewDeadlockDetector(manager *DistributedLockManager) *DeadlockDetector {
	return &DeadlockDetector{
		manager:          manager,
		dependencies:     make(map[string][]string),
		waitChains:       make(map[string]*WaitChain),
		checkInterval:    DeadlockCheckInterval,
		warningThreshold: DeadlockWarningAge,
		criticalThreshold: DeadlockCriticalAge,
	}
}

// Start starts the deadlock detection loop
func (d *DeadlockDetector) Start(stopCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(d.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.detectAndResolve()
			d.checkLongRunningLocks()
		case <-stopCh:
			return
		}
	}
}

// RegisterWait registers that holder is waiting for target lock
func (d *DeadlockDetector) RegisterWait(holder string, targetLock string) {
	d.dependenciesMutex.Lock()
	defer d.dependenciesMutex.Unlock()

	if d.dependencies[holder] == nil {
		d.dependencies[holder] = make([]string, 0)
	}

	// Check if already waiting for this lock
	for _, lock := range d.dependencies[holder] {
		if lock == targetLock {
			return
		}
	}

	d.dependencies[holder] = append(d.dependencies[holder], targetLock)

	// Update wait chain
	d.updateWaitChain(holder, targetLock)
}

// UnregisterWait removes a wait registration
func (d *DeadlockDetector) UnregisterWait(holder string, targetLock string) {
	d.dependenciesMutex.Lock()
	defer d.dependenciesMutex.Unlock()

	if deps, exists := d.dependencies[holder]; exists {
		newDeps := make([]string, 0)
		for _, lock := range deps {
			if lock != targetLock {
				newDeps = append(newDeps, lock)
			}
		}

		if len(newDeps) == 0 {
			delete(d.dependencies, holder)
		} else {
			d.dependencies[holder] = newDeps
		}
	}

	// Clear wait chain if no more dependencies
	if len(d.dependencies[holder]) == 0 {
		d.waitChainsMutex.Lock()
		delete(d.waitChains, holder)
		d.waitChainsMutex.Unlock()
	}
}

// detectAndResolve performs deadlock detection and resolution
func (d *DeadlockDetector) detectAndResolve() {
	d.lastCheckTime = time.Now()

	// Detect cycles in wait graph
	cycles := d.detectCycles()
	if len(cycles) == 0 {
		return
	}

	// Log detected deadlocks
	for _, cycle := range cycles {
		klog.Warningf("Deadlock detected: %v", cycle)
		d.deadlockCount++

		// Try to resolve the deadlock
		if resolved := d.resolveDeadlock(cycle); resolved {
			d.resolvedCount++
			klog.Infof("Deadlock resolved: %v", cycle)
		} else {
			klog.Errorf("Failed to resolve deadlock: %v", cycle)
		}
	}
}

// detectCycles uses DFS to detect cycles in the wait graph
func (d *DeadlockDetector) detectCycles() [][]string {
	d.dependenciesMutex.RLock()
	defer d.dependenciesMutex.RUnlock()

	visited := make(map[string]bool)
	recursionStack := make(map[string]bool)
	var cycles [][]string

	for node := range d.dependencies {
		if !visited[node] {
			if cycle := d.dfsDetectCycle(node, visited, recursionStack, []string{}); cycle != nil {
				cycles = append(cycles, cycle)
			}
		}
	}

	return cycles
}

// dfsDetectCycle performs depth-first search to detect cycles
func (d *DeadlockDetector) dfsDetectCycle(node string, visited map[string]bool, recursionStack map[string]bool, path []string) []string {
	visited[node] = true
	recursionStack[node] = true
	path = append(path, node)

	// Check all dependencies of current node
	for _, dep := range d.dependencies[node] {
		// Check if we found a cycle
		if recursionStack[dep] {
			// Find the cycle start in path
			for i, n := range path {
				if n == dep {
					return path[i:]
				}
			}
		}

		// Continue DFS if not visited
		if !visited[dep] {
			if cycle := d.dfsDetectCycle(dep, visited, recursionStack, path); cycle != nil {
				return cycle
			}
		}
	}

	recursionStack[node] = false
	return nil
}

// resolveDeadlock attempts to resolve a detected deadlock
func (d *DeadlockDetector) resolveDeadlock(cycle []string) bool {
	if len(cycle) == 0 {
		return false
	}

	// Strategy 1: Find the youngest lock in the cycle and release it
	youngestLock := d.findYoungestLock(cycle)
	if youngestLock == "" {
		klog.Warning("Could not find youngest lock in deadlock cycle")
		return false
	}

	// Try to release the youngest lock
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get the appropriate lock based on backend
	lock, err := d.manager.CreateLock(youngestLock, LockTypeEFS)
	if err != nil {
		klog.Errorf("Failed to create lock for deadlock resolution: %v", err)
		return false
	}

	// Force release the lock
	if err := lock.Release(ctx); err != nil {
		klog.Errorf("Failed to release lock %s for deadlock resolution: %v", youngestLock, err)
		return false
	}

	klog.Infof("Released lock %s to resolve deadlock", youngestLock)

	// Clear dependencies for the released lock
	d.dependenciesMutex.Lock()
	delete(d.dependencies, youngestLock)
	d.dependenciesMutex.Unlock()

	return true
}

// findYoungestLock finds the most recently acquired lock in a cycle
func (d *DeadlockDetector) findYoungestLock(cycle []string) string {
	d.waitChainsMutex.RLock()
	defer d.waitChainsMutex.RUnlock()

	var youngestLock string
	var youngestTime time.Time

	for _, lock := range cycle {
		if chain, exists := d.waitChains[lock]; exists {
			if youngestLock == "" || chain.CreatedAt.After(youngestTime) {
				youngestLock = lock
				youngestTime = chain.CreatedAt
			}
		}
	}

	// If no wait chain info, just return the first lock
	if youngestLock == "" && len(cycle) > 0 {
		return cycle[0]
	}

	return youngestLock
}

// updateWaitChain updates the wait chain information
func (d *DeadlockDetector) updateWaitChain(holder string, targetLock string) {
	d.waitChainsMutex.Lock()
	defer d.waitChainsMutex.Unlock()

	chain, exists := d.waitChains[holder]
	if !exists {
		chain = &WaitChain{
			StartLock:   holder,
			Chain:       []string{targetLock},
			CreatedAt:   time.Now(),
			LastUpdated: time.Now(),
		}
	} else {
		chain.Chain = append(chain.Chain, targetLock)
		chain.LastUpdated = time.Now()
	}

	// Check if this creates a cycle
	chain.IsCycle = d.checkCycleInChain(chain.Chain)

	d.waitChains[holder] = chain
}

// checkCycleInChain checks if there's a cycle in the given chain
func (d *DeadlockDetector) checkCycleInChain(chain []string) bool {
	seen := make(map[string]bool)
	for _, lock := range chain {
		if seen[lock] {
			return true
		}
		seen[lock] = true
	}
	return false
}

// checkLongRunningLocks checks for locks that have been held for too long
func (d *DeadlockDetector) checkLongRunningLocks() {
	d.waitChainsMutex.RLock()
	defer d.waitChainsMutex.RUnlock()

	now := time.Now()
	var warningLocks []string
	var criticalLocks []string

	for lock, chain := range d.waitChains {
		age := now.Sub(chain.CreatedAt)

		if age > d.criticalThreshold {
			criticalLocks = append(criticalLocks, lock)
		} else if age > d.warningThreshold {
			warningLocks = append(warningLocks, lock)
		}
	}

	// Log warnings
	if len(warningLocks) > 0 {
		klog.Warningf("Long-running locks detected (>%s): %v", d.warningThreshold, warningLocks)
	}

	// Log critical locks
	if len(criticalLocks) > 0 {
		klog.Errorf("Critical long-running locks detected (>%s): %v", d.criticalThreshold, criticalLocks)

		// Consider force-releasing critical locks
		for _, lock := range criticalLocks {
			d.considerForceRelease(lock)
		}
	}
}

// considerForceRelease evaluates whether to force-release a long-running lock
func (d *DeadlockDetector) considerForceRelease(lockKey string) {
	d.waitChainsMutex.RLock()
	chain, exists := d.waitChains[lockKey]
	d.waitChainsMutex.RUnlock()

	if !exists {
		return
	}

	// Only force release if lock is in a cycle or has been held for critical time
	if chain.IsCycle || time.Since(chain.CreatedAt) > d.criticalThreshold*2 {
		klog.Warningf("Considering force release of lock %s (age: %s, cycle: %v)",
			lockKey, time.Since(chain.CreatedAt), chain.IsCycle)

		// Create a deadlock info for metrics/audit
		info := &DeadlockInfo{
			Cycle:        chain.Chain,
			DetectedAt:   time.Now(),
			AffectedLocks: []string{lockKey},
			Resolution:   "force_release",
		}

		// Log the force release attempt
		klog.Infof("Force releasing lock due to deadlock detection: %+v", info)

		// Attempt force release
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		lock, err := d.manager.CreateLock(lockKey, LockTypeEFS)
		if err != nil {
			klog.Errorf("Failed to create lock for force release: %v", err)
			return
		}

		if err := lock.Release(ctx); err != nil {
			klog.Errorf("Failed to force release lock %s: %v", lockKey, err)
		} else {
			klog.Infof("Successfully force released lock %s", lockKey)
			d.resolvedCount++
		}
	}
}

// GetStatistics returns deadlock detection statistics
func (d *DeadlockDetector) GetStatistics() map[string]interface{} {
	d.dependenciesMutex.RLock()
	depCount := len(d.dependencies)
	d.dependenciesMutex.RUnlock()

	d.waitChainsMutex.RLock()
	chainCount := len(d.waitChains)
	var cycleCount int
	for _, chain := range d.waitChains {
		if chain.IsCycle {
			cycleCount++
		}
	}
	d.waitChainsMutex.RUnlock()

	return map[string]interface{}{
		"deadlocks_detected":    d.deadlockCount,
		"deadlocks_resolved":    d.resolvedCount,
		"active_dependencies":   depCount,
		"active_wait_chains":    chainCount,
		"active_cycles":         cycleCount,
		"last_check_time":       d.lastCheckTime,
		"check_interval":        d.checkInterval,
		"warning_threshold":     d.warningThreshold,
		"critical_threshold":    d.criticalThreshold,
	}
}

// Reset clears all deadlock detection state
func (d *DeadlockDetector) Reset() {
	d.dependenciesMutex.Lock()
	d.dependencies = make(map[string][]string)
	d.dependenciesMutex.Unlock()

	d.waitChainsMutex.Lock()
	d.waitChains = make(map[string]*WaitChain)
	d.waitChainsMutex.Unlock()

	d.deadlockCount = 0
	d.resolvedCount = 0
	d.lastCheckTime = time.Time{}
}