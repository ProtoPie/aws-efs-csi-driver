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

	"k8s.io/klog/v2"
)

// NamespaceProvisionerMetrics implements MetricsCollector for namespace provisioner
type NamespaceProvisionerMetrics struct {
	mu sync.RWMutex

	// Counters
	efsCreated           map[string]int64
	efsDeleted           map[string]int64
	accessPointCreated   map[string]int64
	accessPointDeleted   map[string]int64
	errors               map[string]int64

	// Timings
	efsCreationTimes     map[string][]time.Duration

	// Gauges
	activeNamespaces     int

	// Error tracking
	lastErrors           map[string]error
	lastErrorTimes       map[string]time.Time
}

// NewNamespaceProvisionerMetrics creates a new metrics collector
func NewNamespaceProvisionerMetrics() *NamespaceProvisionerMetrics {
	return &NamespaceProvisionerMetrics{
		efsCreated:           make(map[string]int64),
		efsDeleted:           make(map[string]int64),
		accessPointCreated:   make(map[string]int64),
		accessPointDeleted:   make(map[string]int64),
		errors:               make(map[string]int64),
		efsCreationTimes:     make(map[string][]time.Duration),
		lastErrors:           make(map[string]error),
		lastErrorTimes:       make(map[string]time.Time),
	}
}

// IncEFSCreated increments the EFS created counter for a namespace
func (m *NamespaceProvisionerMetrics) IncEFSCreated(namespace string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.efsCreated[namespace]++
	klog.V(4).Infof("Metrics: EFS created for namespace %s, total: %d", namespace, m.efsCreated[namespace])
}

// IncEFSDeleted increments the EFS deleted counter for a namespace
func (m *NamespaceProvisionerMetrics) IncEFSDeleted(namespace string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.efsDeleted[namespace]++
	klog.V(4).Infof("Metrics: EFS deleted for namespace %s, total: %d", namespace, m.efsDeleted[namespace])
}

// IncAccessPointCreated increments the access point created counter for a namespace
func (m *NamespaceProvisionerMetrics) IncAccessPointCreated(namespace string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.accessPointCreated[namespace]++
	klog.V(4).Infof("Metrics: Access point created for namespace %s, total: %d", namespace, m.accessPointCreated[namespace])
}

// IncAccessPointDeleted increments the access point deleted counter for a namespace
func (m *NamespaceProvisionerMetrics) IncAccessPointDeleted(namespace string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.accessPointDeleted[namespace]++
	klog.V(4).Infof("Metrics: Access point deleted for namespace %s, total: %d", namespace, m.accessPointDeleted[namespace])
}

// RecordEFSCreationTime records the time taken to create an EFS filesystem
func (m *NamespaceProvisionerMetrics) RecordEFSCreationTime(namespace string, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.efsCreationTimes[namespace] == nil {
		m.efsCreationTimes[namespace] = make([]time.Duration, 0)
	}

	// Keep only the last 100 measurements per namespace to avoid memory growth
	times := m.efsCreationTimes[namespace]
	if len(times) >= 100 {
		times = times[1:]
	}

	times = append(times, duration)
	m.efsCreationTimes[namespace] = times

	klog.V(4).Infof("Metrics: EFS creation time for namespace %s: %v", namespace, duration)
}

// RecordError records an error for a specific operation and namespace
func (m *NamespaceProvisionerMetrics) RecordError(operation, namespace string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := operation + ":" + namespace
	m.errors[key]++
	m.lastErrors[key] = err
	m.lastErrorTimes[key] = time.Now()

	klog.V(4).Infof("Metrics: Error recorded for operation %s in namespace %s: %v", operation, namespace, err)
}

// SetActiveNamespaces sets the number of active namespaces
func (m *NamespaceProvisionerMetrics) SetActiveNamespaces(count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activeNamespaces = count
	klog.V(4).Infof("Metrics: Active namespaces: %d", count)
}

// GetEFSCreated returns the number of EFS filesystems created for a namespace
func (m *NamespaceProvisionerMetrics) GetEFSCreated(namespace string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.efsCreated[namespace]
}

// GetEFSDeleted returns the number of EFS filesystems deleted for a namespace
func (m *NamespaceProvisionerMetrics) GetEFSDeleted(namespace string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.efsDeleted[namespace]
}

// GetAccessPointCreated returns the number of access points created for a namespace
func (m *NamespaceProvisionerMetrics) GetAccessPointCreated(namespace string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.accessPointCreated[namespace]
}

// GetAccessPointDeleted returns the number of access points deleted for a namespace
func (m *NamespaceProvisionerMetrics) GetAccessPointDeleted(namespace string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.accessPointDeleted[namespace]
}

// GetActiveNamespaces returns the number of active namespaces
func (m *NamespaceProvisionerMetrics) GetActiveNamespaces() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.activeNamespaces
}

// GetAverageEFSCreationTime returns the average EFS creation time for a namespace
func (m *NamespaceProvisionerMetrics) GetAverageEFSCreationTime(namespace string) time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	times := m.efsCreationTimes[namespace]
	if len(times) == 0 {
		return 0
	}

	var total time.Duration
	for _, duration := range times {
		total += duration
	}

	return total / time.Duration(len(times))
}

// GetErrorCount returns the number of errors for a specific operation and namespace
func (m *NamespaceProvisionerMetrics) GetErrorCount(operation, namespace string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := operation + ":" + namespace
	return m.errors[key]
}

// GetLastError returns the last error for a specific operation and namespace
func (m *NamespaceProvisionerMetrics) GetLastError(operation, namespace string) (error, time.Time) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := operation + ":" + namespace
	return m.lastErrors[key], m.lastErrorTimes[key]
}

// Reset resets all metrics
func (m *NamespaceProvisionerMetrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.efsCreated = make(map[string]int64)
	m.efsDeleted = make(map[string]int64)
	m.accessPointCreated = make(map[string]int64)
	m.accessPointDeleted = make(map[string]int64)
	m.errors = make(map[string]int64)
	m.efsCreationTimes = make(map[string][]time.Duration)
	m.lastErrors = make(map[string]error)
	m.lastErrorTimes = make(map[string]time.Time)
	m.activeNamespaces = 0
}

// GetSummary returns a summary of all metrics
func (m *NamespaceProvisionerMetrics) GetSummary() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	summary := make(map[string]interface{})

	// Copy counters
	summary["efs_created"] = copyInt64Map(m.efsCreated)
	summary["efs_deleted"] = copyInt64Map(m.efsDeleted)
	summary["access_point_created"] = copyInt64Map(m.accessPointCreated)
	summary["access_point_deleted"] = copyInt64Map(m.accessPointDeleted)
	summary["errors"] = copyInt64Map(m.errors)

	// Active namespaces
	summary["active_namespaces"] = m.activeNamespaces

	// Average creation times
	avgTimes := make(map[string]time.Duration)
	for namespace, times := range m.efsCreationTimes {
		if len(times) > 0 {
			var total time.Duration
			for _, duration := range times {
				total += duration
			}
			avgTimes[namespace] = total / time.Duration(len(times))
		}
	}
	summary["avg_efs_creation_times"] = avgTimes

	return summary
}

// Helper function to copy int64 maps
func copyInt64Map(src map[string]int64) map[string]int64 {
	dst := make(map[string]int64)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}