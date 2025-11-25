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
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog/v2"
)

// NamespaceControllerMetrics collects metrics for the namespace controller
type NamespaceControllerMetrics struct {
	// Counter metrics
	namespaceDeletionAttempts   *prometheus.CounterVec
	successfulNamespaceCleanups *prometheus.CounterVec
	namespaceCleanupErrors      *prometheus.CounterVec
	namespaceWatcherRestarts    prometheus.Counter

	// Histogram metrics
	namespaceCleanupDuration *prometheus.HistogramVec

	// Gauge metrics
	activeNamespaceCleanups prometheus.Gauge
	namespaceWatcherStatus  prometheus.Gauge

	// Internal state
	registry *prometheus.Registry
	mutex    sync.RWMutex
	started  bool
}

// NewNamespaceControllerMetrics creates a new metrics collector for namespace controller
func NewNamespaceControllerMetrics() *NamespaceControllerMetrics {
	metrics := &NamespaceControllerMetrics{
		// Counter metrics
		namespaceDeletionAttempts: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "efs_ns",
				Subsystem: "namespace_controller",
				Name:      "deletion_attempts_total",
				Help:      "Total number of namespace deletion attempts processed",
			},
			[]string{"namespace"},
		),

		successfulNamespaceCleanups: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "efs_ns",
				Subsystem: "namespace_controller",
				Name:      "successful_cleanups_total",
				Help:      "Total number of successful namespace cleanups",
			},
			[]string{"namespace"},
		),

		namespaceCleanupErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "efs_ns",
				Subsystem: "namespace_controller",
				Name:      "cleanup_errors_total",
				Help:      "Total number of namespace cleanup errors",
			},
			[]string{"namespace", "error_type"},
		),

		namespaceWatcherRestarts: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "efs_ns",
				Subsystem: "namespace_controller",
				Name:      "watcher_restarts_total",
				Help:      "Total number of namespace watcher restarts",
			},
		),

		// Histogram metrics
		namespaceCleanupDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "efs_ns",
				Subsystem: "namespace_controller",
				Name:      "cleanup_duration_seconds",
				Help:      "Duration of namespace cleanup operations in seconds",
				Buckets:   prometheus.ExponentialBuckets(0.1, 2, 10), // 0.1s to ~100s
			},
			[]string{"namespace"},
		),

		// Gauge metrics
		activeNamespaceCleanups: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "efs_ns",
				Subsystem: "namespace_controller",
				Name:      "active_cleanups",
				Help:      "Number of namespace cleanups currently in progress",
			},
		),

		namespaceWatcherStatus: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "efs_ns",
				Subsystem: "namespace_controller",
				Name:      "watcher_status",
				Help:      "Status of namespace watcher (1=running, 0=stopped)",
			},
		),

		registry: prometheus.NewRegistry(),
	}

	return metrics
}

// Start initializes and registers all metrics
func (m *NamespaceControllerMetrics) Start() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.started {
		return nil
	}

	klog.V(4).InfoS("Starting namespace controller metrics collection")

	// Register all metrics with the registry
	metrics := []prometheus.Collector{
		m.namespaceDeletionAttempts,
		m.successfulNamespaceCleanups,
		m.namespaceCleanupErrors,
		m.namespaceWatcherRestarts,
		m.namespaceCleanupDuration,
		m.activeNamespaceCleanups,
		m.namespaceWatcherStatus,
	}

	for _, metric := range metrics {
		if err := m.registry.Register(metric); err != nil {
			// Check if already registered
			if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
				return err
			}
		}
	}

	// Initialize gauge values
	m.activeNamespaceCleanups.Set(0)
	m.namespaceWatcherStatus.Set(0)

	m.started = true

	klog.V(4).InfoS("Namespace controller metrics collection started")
	return nil
}

// Stop stops metrics collection
func (m *NamespaceControllerMetrics) Stop() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if !m.started {
		return
	}

	klog.V(4).InfoS("Stopping namespace controller metrics collection")

	// Reset gauge values
	m.activeNamespaceCleanups.Set(0)
	m.namespaceWatcherStatus.Set(0)

	m.started = false

	klog.V(4).InfoS("Namespace controller metrics collection stopped")
}

// Registry returns the Prometheus registry for this metrics collector
func (m *NamespaceControllerMetrics) Registry() *prometheus.Registry {
	return m.registry
}

// IncNamespaceDeletionAttempts increments the namespace deletion attempts counter
func (m *NamespaceControllerMetrics) IncNamespaceDeletionAttempts(namespace string) {
	if !m.started {
		return
	}

	m.namespaceDeletionAttempts.WithLabelValues(namespace).Inc()
	klog.V(5).InfoS("Incremented namespace deletion attempts", "namespace", namespace)
}

// IncSuccessfulNamespaceCleanups increments the successful namespace cleanups counter
func (m *NamespaceControllerMetrics) IncSuccessfulNamespaceCleanups(namespace string) {
	if !m.started {
		return
	}

	m.successfulNamespaceCleanups.WithLabelValues(namespace).Inc()
	klog.V(5).InfoS("Incremented successful namespace cleanups", "namespace", namespace)
}

// IncNamespaceCleanupErrors increments the namespace cleanup errors counter
func (m *NamespaceControllerMetrics) IncNamespaceCleanupErrors(namespace, errorType string) {
	if !m.started {
		return
	}

	m.namespaceCleanupErrors.WithLabelValues(namespace, errorType).Inc()
	klog.V(5).InfoS("Incremented namespace cleanup errors", "namespace", namespace, "errorType", errorType)
}

// IncNamespaceWatcherRestarts increments the namespace watcher restarts counter
func (m *NamespaceControllerMetrics) IncNamespaceWatcherRestarts() {
	if !m.started {
		return
	}

	m.namespaceWatcherRestarts.Inc()
	klog.V(5).InfoS("Incremented namespace watcher restarts")
}

// ObserveNamespaceCleanupDuration observes a namespace cleanup duration
func (m *NamespaceControllerMetrics) ObserveNamespaceCleanupDuration(namespace string, duration time.Duration) {
	if !m.started {
		return
	}

	m.namespaceCleanupDuration.WithLabelValues(namespace).Observe(duration.Seconds())
	klog.V(5).InfoS("Observed namespace cleanup duration", "namespace", namespace, "duration", duration)
}

// SetActiveNamespaceCleanups sets the number of active namespace cleanups
func (m *NamespaceControllerMetrics) SetActiveNamespaceCleanups(count float64) {
	if !m.started {
		return
	}

	m.activeNamespaceCleanups.Set(count)
	klog.V(5).InfoS("Set active namespace cleanups", "count", count)
}

// SetNamespaceWatcherStatus sets the namespace watcher status
func (m *NamespaceControllerMetrics) SetNamespaceWatcherStatus(running bool) {
	if !m.started {
		return
	}

	status := float64(0)
	if running {
		status = 1
	}

	m.namespaceWatcherStatus.Set(status)
	klog.V(5).InfoS("Set namespace watcher status", "running", running)
}

// GetMetricsSnapshot returns a snapshot of current metric values for testing
func (m *NamespaceControllerMetrics) GetMetricsSnapshot() map[string]interface{} {
	if !m.started {
		return nil
	}

	m.mutex.RLock()
	defer m.mutex.RUnlock()

	snapshot := make(map[string]interface{})

	// Gather metrics from registry
	metricFamilies, err := m.registry.Gather()
	if err != nil {
		klog.ErrorS(err, "Failed to gather metrics")
		return snapshot
	}

	for _, mf := range metricFamilies {
		name := mf.GetName()
		snapshot[name] = mf.GetMetric()
	}

	return snapshot
}
