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
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/klog/v2"
)

const (
	// Metric namespace for EFS-NS components
	MetricsNamespace = "efs_ns"
	MetricsSubsystem = "filesystem"
)

// MetricsCollector interface for EFS-NS metrics collection
type MetricsCollector interface {
	// Filesystem metrics
	RecordFileSystemCount(namespace, cluster, state string, count float64)
	RecordFileSystemStateChange(namespace, cluster, fromState, toState string)

	// Operation metrics
	RecordOperationDuration(operation, namespace, result string, duration time.Duration)
	RecordOperationTotal(operation, namespace, result string)

	// Cache metrics
	RecordCacheHit(operation string)
	RecordCacheMiss(operation string)
	RecordCacheHitRatio(operation string, ratio float64)

	// Handler returns the HTTP handler for metrics endpoint
	Handler() http.Handler

	// Register registers all metrics with the provided registry
	Register(registry prometheus.Registerer) error

	// Unregister removes all metrics from the registry
	Unregister(registry prometheus.Registerer) bool
}

// PrometheusMetricsCollector implements MetricsCollector using Prometheus
type PrometheusMetricsCollector struct {
	// Filesystem metrics
	filesystemCount *prometheus.GaugeVec
	filesystemTotal *prometheus.CounterVec

	// Operation metrics
	operationDuration *prometheus.HistogramVec
	operationTotal    *prometheus.CounterVec

	// Cache metrics
	cacheHitTotal  *prometheus.CounterVec
	cacheMissTotal *prometheus.CounterVec
	cacheHitRatio  *prometheus.GaugeVec

	// Internal tracking
	mutex      sync.RWMutex
	cacheStats map[string]*cacheMetrics
	registered bool
	handler    http.Handler
}

// cacheMetrics tracks cache hit/miss counts for ratio calculation
type cacheMetrics struct {
	hits   int64
	misses int64
}

// NewPrometheusMetricsCollector creates a new Prometheus-based metrics collector
func NewPrometheusMetricsCollector() MetricsCollector {
	collector := &PrometheusMetricsCollector{
		cacheStats: make(map[string]*cacheMetrics),
	}

	collector.initializeMetrics()
	collector.handler = promhttp.Handler()

	return collector
}

// initializeMetrics initializes all Prometheus metrics
func (c *PrometheusMetricsCollector) initializeMetrics() {
	// Filesystem count by namespace and state
	c.filesystemCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Subsystem: MetricsSubsystem,
			Name:      "count",
			Help:      "Number of EFS filesystems by namespace and state",
		},
		[]string{"namespace", "cluster", "state"},
	)

	// Total filesystem operations
	c.filesystemTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: MetricsNamespace,
			Subsystem: MetricsSubsystem,
			Name:      "operations_total",
			Help:      "Total number of filesystem state changes",
		},
		[]string{"namespace", "cluster", "from_state", "to_state"},
	)

	// Operation duration histogram
	c.operationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: MetricsNamespace,
			Subsystem: "operations",
			Name:      "duration_seconds",
			Help:      "Duration of EFS-NS operations in seconds",
			Buckets:   []float64{0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0},
		},
		[]string{"operation", "namespace", "result"},
	)

	// Total operations counter
	c.operationTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: MetricsNamespace,
			Subsystem: "operations",
			Name:      "total",
			Help:      "Total number of EFS-NS operations",
		},
		[]string{"operation", "namespace", "result"},
	)

	// Cache hit counter
	c.cacheHitTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: MetricsNamespace,
			Subsystem: "cache",
			Name:      "hits_total",
			Help:      "Total number of cache hits",
		},
		[]string{"operation"},
	)

	// Cache miss counter
	c.cacheMissTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: MetricsNamespace,
			Subsystem: "cache",
			Name:      "misses_total",
			Help:      "Total number of cache misses",
		},
		[]string{"operation"},
	)

	// Cache hit ratio gauge
	c.cacheHitRatio = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: MetricsNamespace,
			Subsystem: "cache",
			Name:      "hit_ratio",
			Help:      "Cache hit ratio (hits / (hits + misses))",
		},
		[]string{"operation"},
	)
}

// RecordFileSystemCount records the number of filesystems by namespace and state
func (c *PrometheusMetricsCollector) RecordFileSystemCount(namespace, cluster, state string, count float64) {
	c.filesystemCount.WithLabelValues(namespace, cluster, state).Set(count)
}

// RecordFileSystemStateChange records a filesystem state transition
func (c *PrometheusMetricsCollector) RecordFileSystemStateChange(namespace, cluster, fromState, toState string) {
	c.filesystemTotal.WithLabelValues(namespace, cluster, fromState, toState).Inc()
}

// RecordOperationDuration records the duration of an operation
func (c *PrometheusMetricsCollector) RecordOperationDuration(operation, namespace, result string, duration time.Duration) {
	c.operationDuration.WithLabelValues(operation, namespace, result).Observe(duration.Seconds())
}

// RecordOperationTotal increments the total count of operations
func (c *PrometheusMetricsCollector) RecordOperationTotal(operation, namespace, result string) {
	c.operationTotal.WithLabelValues(operation, namespace, result).Inc()
}

// RecordCacheHit records a cache hit and updates hit ratio
func (c *PrometheusMetricsCollector) RecordCacheHit(operation string) {
	c.cacheHitTotal.WithLabelValues(operation).Inc()
	c.updateCacheStats(operation, true)
}

// RecordCacheMiss records a cache miss and updates hit ratio
func (c *PrometheusMetricsCollector) RecordCacheMiss(operation string) {
	c.cacheMissTotal.WithLabelValues(operation).Inc()
	c.updateCacheStats(operation, false)
}

// RecordCacheHitRatio directly sets the cache hit ratio
func (c *PrometheusMetricsCollector) RecordCacheHitRatio(operation string, ratio float64) {
	c.cacheHitRatio.WithLabelValues(operation).Set(ratio)
}

// updateCacheStats updates internal cache statistics and calculates hit ratio
func (c *PrometheusMetricsCollector) updateCacheStats(operation string, hit bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.cacheStats[operation] == nil {
		c.cacheStats[operation] = &cacheMetrics{}
	}

	stats := c.cacheStats[operation]
	if hit {
		stats.hits++
	} else {
		stats.misses++
	}

	total := stats.hits + stats.misses
	if total > 0 {
		ratio := float64(stats.hits) / float64(total)
		c.cacheHitRatio.WithLabelValues(operation).Set(ratio)
	}
}

// Handler returns the HTTP handler for metrics endpoint
func (c *PrometheusMetricsCollector) Handler() http.Handler {
	return c.handler
}

// Register registers all metrics with the provided registry
func (c *PrometheusMetricsCollector) Register(registry prometheus.Registerer) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.registered {
		return nil // Already registered
	}

	metrics := []prometheus.Collector{
		c.filesystemCount,
		c.filesystemTotal,
		c.operationDuration,
		c.operationTotal,
		c.cacheHitTotal,
		c.cacheMissTotal,
		c.cacheHitRatio,
	}

	for _, metric := range metrics {
		if err := registry.Register(metric); err != nil {
			// If registration fails, try to unregister already registered metrics
			for i := 0; i < len(metrics); i++ {
				registry.Unregister(metrics[i])
			}
			return NewEFSNSError(ErrMetricsRegistration, "Register", "",
				"failed to register metrics", err)
		}
	}

	c.registered = true
	klog.V(4).Infof("EFS-NS metrics registered successfully")
	return nil
}

// Unregister removes all metrics from the registry
func (c *PrometheusMetricsCollector) Unregister(registry prometheus.Registerer) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.registered {
		return true // Nothing to unregister
	}

	metrics := []prometheus.Collector{
		c.filesystemCount,
		c.filesystemTotal,
		c.operationDuration,
		c.operationTotal,
		c.cacheHitTotal,
		c.cacheMissTotal,
		c.cacheHitRatio,
	}

	success := true
	for _, metric := range metrics {
		if !registry.Unregister(metric) {
			success = false
			klog.Warningf("Failed to unregister EFS-NS metric: %T", metric)
		}
	}

	if success {
		c.registered = false
		klog.V(4).Infof("EFS-NS metrics unregistered successfully")
	}

	return success
}

// MetricsRecorder provides helper methods for recording metrics with timing
type MetricsRecorder struct {
	collector MetricsCollector
	operation string
	namespace string
	startTime time.Time
}

// NewMetricsRecorder creates a new metrics recorder for an operation
func NewMetricsRecorder(collector MetricsCollector, operation, namespace string) *MetricsRecorder {
	return &MetricsRecorder{
		collector: collector,
		operation: operation,
		namespace: namespace,
		startTime: time.Now(),
	}
}

// RecordSuccess records a successful operation completion
func (r *MetricsRecorder) RecordSuccess() {
	duration := time.Since(r.startTime)
	r.collector.RecordOperationDuration(r.operation, r.namespace, "success", duration)
	r.collector.RecordOperationTotal(r.operation, r.namespace, "success")
}

// RecordError records a failed operation completion
func (r *MetricsRecorder) RecordError(err error) {
	duration := time.Since(r.startTime)
	r.collector.RecordOperationDuration(r.operation, r.namespace, "error", duration)
	r.collector.RecordOperationTotal(r.operation, r.namespace, "error")

	if efsErr, ok := err.(*EFSNSError); ok {
		klog.V(4).Infof("EFS-NS operation %s failed in namespace %s: %s (type: %s, duration: %v)",
			r.operation, r.namespace, efsErr.Message, efsErr.Type, duration)
	} else {
		klog.V(4).Infof("EFS-NS operation %s failed in namespace %s: %v (duration: %v)",
			r.operation, r.namespace, err, duration)
	}
}

// GlobalMetricsCollector is the global instance of the metrics collector
var (
	globalMetricsCollector MetricsCollector
	metricsOnce            sync.Once
)

// GetGlobalMetricsCollector returns the global metrics collector instance
func GetGlobalMetricsCollector() MetricsCollector {
	metricsOnce.Do(func() {
		globalMetricsCollector = NewPrometheusMetricsCollector()
	})
	return globalMetricsCollector
}

// SetGlobalMetricsCollector sets a custom global metrics collector (mainly for testing)
func SetGlobalMetricsCollector(collector MetricsCollector) {
	globalMetricsCollector = collector
}
