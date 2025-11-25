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
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	"k8s.io/client-go/kubernetes"
)

const (
	// DefaultHealthCheckTimeout is the default timeout for health checks
	DefaultHealthCheckTimeout = 10 * time.Second
)

// HealthChecker interface for health checks
type HealthChecker interface {
	Check(ctx context.Context) error
	Name() string
}

// ReadinessChecker interface for readiness checks
type ReadinessChecker interface {
	IsReady(ctx context.Context) bool
	Name() string
}

// EFSAPIHealthChecker checks if AWS EFS API is accessible
type EFSAPIHealthChecker struct {
	efsClient EFSClient
	logger    *EFSNSLogger
	timeout   time.Duration
}

// NewEFSAPIHealthChecker creates a new EFS API health checker
func NewEFSAPIHealthChecker(efsClient EFSClient) *EFSAPIHealthChecker {
	return &EFSAPIHealthChecker{
		efsClient: efsClient,
		logger:    NewEFSNSLogger("health-check-efs"),
		timeout:   10 * time.Second,
	}
}

// Check performs EFS API health check
func (h *EFSAPIHealthChecker) Check(ctx context.Context) error {
	startTime := time.Now()

	// Create context with timeout
	checkCtx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	// Try to call EFS DescribeFileSystems with minimal parameters
	input := &efs.DescribeFileSystemsInput{
		MaxItems: aws.Int32(1), // Only need to check if API is responsive
	}

	_, err := h.efsClient.DescribeFileSystems(checkCtx, input)
	duration := time.Since(startTime)

	if err != nil {
		h.logger.LogAWSAPIError("efs", "DescribeFileSystems", duration, err)
		return NewEFSNSError(ErrAWSAPIFailed, "EFSAPIHealthCheck", "",
			fmt.Sprintf("EFS API health check failed: %v", err), err)
	}

	h.logger.LogAWSAPICall("efs", "DescribeFileSystems", duration, true)
	return nil
}

// Name returns the name of this health checker
func (h *EFSAPIHealthChecker) Name() string {
	return "efs-api"
}

// SetTimeout sets the timeout for health checks
func (h *EFSAPIHealthChecker) SetTimeout(timeout time.Duration) {
	h.timeout = timeout
}

// KubernetesAPIHealthChecker checks if Kubernetes API is accessible
type KubernetesAPIHealthChecker struct {
	client  kubernetes.Interface
	logger  *EFSNSLogger
	timeout time.Duration
}

// NewKubernetesAPIHealthChecker creates a new Kubernetes API health checker
func NewKubernetesAPIHealthChecker(client kubernetes.Interface) *KubernetesAPIHealthChecker {
	return &KubernetesAPIHealthChecker{
		client:  client,
		logger:  NewEFSNSLogger("health-check-k8s"),
		timeout: 10 * time.Second,
	}
}

// Check performs Kubernetes API health check
func (h *KubernetesAPIHealthChecker) Check(ctx context.Context) error {
	if h.client == nil {
		return NewEFSNSError(ErrKubernetesAPIFailed, "KubernetesAPIHealthCheck", "",
			"Kubernetes client is not initialized", nil)
	}

	startTime := time.Now()

	// Try to perform a simple API call (get server version)
	_, err := h.client.Discovery().ServerVersion()
	duration := time.Since(startTime)

	if err != nil {
		h.logger.LogKubernetesAPIError("discovery", "server-version", duration, err)
		return NewEFSNSError(ErrKubernetesAPIFailed, "KubernetesAPIHealthCheck", "",
			fmt.Sprintf("Kubernetes API health check failed: %v", err), err)
	}

	h.logger.LogKubernetesAPICall("discovery", "server-version", duration, true)
	return nil
}

// Name returns the name of this health checker
func (h *KubernetesAPIHealthChecker) Name() string {
	return "kubernetes-api"
}

// SetTimeout sets the timeout for health checks
func (h *KubernetesAPIHealthChecker) SetTimeout(timeout time.Duration) {
	h.timeout = timeout
}

// CacheHealthChecker checks if cache operations are working
type CacheHealthChecker struct {
	cache   FileSystemCache
	logger  *EFSNSLogger
	timeout time.Duration
}

// NewCacheHealthChecker creates a new cache health checker
func NewCacheHealthChecker(cache FileSystemCache) *CacheHealthChecker {
	return &CacheHealthChecker{
		cache:   cache,
		logger:  NewEFSNSLogger("health-check-cache"),
		timeout: 5 * time.Second,
	}
}

// Check performs cache health check
func (h *CacheHealthChecker) Check(ctx context.Context) error {
	if h.cache == nil {
		return NewEFSNSError(ErrCacheOperationFailed, "CacheHealthCheck", "",
			"cache is not initialized", nil)
	}

	startTime := time.Now()

	// Create context with timeout
	checkCtx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	// Test cache operations
	testNamespace := "__health_check__"
	testFSInfo := &FileSystemInfo{
		FileSystemID: "fs-healthcheck",
		Namespace:    testNamespace,
		ClusterID:    "test-cluster",
		CreatedAt:    time.Now(),
		State:        FileSystemStateAvailable,
		PVCCount:     0,
	}

	// Test set operation
	h.cache.Set(testNamespace, testFSInfo)

	// Test get operation
	retrievedInfo, found := h.cache.Get(testNamespace)
	if !found {
		h.cache.Delete(testNamespace) // Cleanup
		return NewEFSNSError(ErrCacheOperationFailed, "CacheHealthCheck", "",
			"cache get operation failed after set", nil)
	}

	if retrievedInfo.FileSystemID != testFSInfo.FileSystemID {
		h.cache.Delete(testNamespace) // Cleanup
		return NewEFSNSError(ErrCacheOperationFailed, "CacheHealthCheck", "",
			"cache data integrity check failed", nil)
	}

	// Test delete operation
	h.cache.Delete(testNamespace)

	// Verify deletion
	_, stillFound := h.cache.Get(testNamespace)
	if stillFound {
		return NewEFSNSError(ErrCacheOperationFailed, "CacheHealthCheck", "",
			"cache delete operation failed", nil)
	}

	duration := time.Since(startTime)
	h.logger.LogDebug("Cache health check completed successfully",
		"duration", duration.String(),
		"durationMs", duration.Milliseconds())

	// Check if context was cancelled during operations
	select {
	case <-checkCtx.Done():
		return NewEFSNSError(ErrTimeout, "CacheHealthCheck", "",
			"cache health check timed out", checkCtx.Err())
	default:
		return nil
	}
}

// Name returns the name of this health checker
func (h *CacheHealthChecker) Name() string {
	return "cache"
}

// SetTimeout sets the timeout for health checks
func (h *CacheHealthChecker) SetTimeout(timeout time.Duration) {
	h.timeout = timeout
}

// MetricsCollectorHealthChecker checks if metrics collection is working
type MetricsCollectorHealthChecker struct {
	collector MetricsCollector
	logger    *EFSNSLogger
	timeout   time.Duration
}

// NewMetricsCollectorHealthChecker creates a new metrics collector health checker
func NewMetricsCollectorHealthChecker(collector MetricsCollector) *MetricsCollectorHealthChecker {
	return &MetricsCollectorHealthChecker{
		collector: collector,
		logger:    NewEFSNSLogger("health-check-metrics"),
		timeout:   5 * time.Second,
	}
}

// Check performs metrics collector health check
func (h *MetricsCollectorHealthChecker) Check(ctx context.Context) error {
	if h.collector == nil {
		return NewEFSNSError(ErrMetricsCollection, "MetricsCollectorHealthCheck", "",
			"metrics collector is not initialized", nil)
	}

	startTime := time.Now()

	// Create context with timeout
	checkCtx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	// Test metrics recording
	testNamespace := "__health_check__"
	testDuration := 100 * time.Millisecond

	// Test operation metrics
	h.collector.RecordOperationDuration("health_check", testNamespace, "success", testDuration)
	h.collector.RecordOperationTotal("health_check", testNamespace, "success")

	// Test filesystem metrics
	h.collector.RecordFileSystemCount(testNamespace, "test-cluster", "available", 1)
	h.collector.RecordFileSystemStateChange(testNamespace, "test-cluster", "creating", "available")

	// Test cache metrics
	h.collector.RecordCacheHit("health_check")
	h.collector.RecordCacheMiss("health_check")
	h.collector.RecordCacheHitRatio("health_check", 0.5)

	duration := time.Since(startTime)
	h.logger.LogDebug("Metrics collector health check completed successfully",
		"duration", duration.String(),
		"durationMs", duration.Milliseconds())

	// Check if context was cancelled during operations
	select {
	case <-checkCtx.Done():
		return NewEFSNSError(ErrTimeout, "MetricsCollectorHealthCheck", "",
			"metrics collector health check timed out", checkCtx.Err())
	default:
		return nil
	}
}

// Name returns the name of this health checker
func (h *MetricsCollectorHealthChecker) Name() string {
	return "metrics-collector"
}

// SetTimeout sets the timeout for health checks
func (h *MetricsCollectorHealthChecker) SetTimeout(timeout time.Duration) {
	h.timeout = timeout
}

// FileSystemManagerReadinessChecker checks if filesystem manager is ready
type FileSystemManagerReadinessChecker struct {
	manager NamespaceFileSystemManager
	logger  *EFSNSLogger
	timeout time.Duration
}

// NewFileSystemManagerReadinessChecker creates a new filesystem manager readiness checker
func NewFileSystemManagerReadinessChecker(manager NamespaceFileSystemManager) *FileSystemManagerReadinessChecker {
	return &FileSystemManagerReadinessChecker{
		manager: manager,
		logger:  NewEFSNSLogger("readiness-check-fs-manager"),
		timeout: 10 * time.Second,
	}
}

// IsReady checks if the filesystem manager is ready
func (r *FileSystemManagerReadinessChecker) IsReady(ctx context.Context) bool {
	if r.manager == nil {
		r.logger.LogWarning("Filesystem manager is not initialized")
		return false
	}

	// Create context with timeout
	checkCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// Try to sync from AWS to check connectivity
	err := r.manager.SyncFromAWS(checkCtx)
	if err != nil {
		r.logger.LogError(err, "Filesystem manager readiness check failed")
		return false
	}

	r.logger.LogDebug("Filesystem manager readiness check passed")
	return true
}

// Name returns the name of this readiness checker
func (r *FileSystemManagerReadinessChecker) Name() string {
	return "filesystem-manager"
}

// SetTimeout sets the timeout for readiness checks
func (r *FileSystemManagerReadinessChecker) SetTimeout(timeout time.Duration) {
	r.timeout = timeout
}

// PVCTrackerReadinessChecker checks if PVC tracker is ready
type PVCTrackerReadinessChecker struct {
	tracker PVCTracker
	logger  *EFSNSLogger
	timeout time.Duration
}

// NewPVCTrackerReadinessChecker creates a new PVC tracker readiness checker
func NewPVCTrackerReadinessChecker(tracker PVCTracker) *PVCTrackerReadinessChecker {
	return &PVCTrackerReadinessChecker{
		tracker: tracker,
		logger:  NewEFSNSLogger("readiness-check-pvc-tracker"),
		timeout: 10 * time.Second,
	}
}

// IsReady checks if the PVC tracker is ready
func (r *PVCTrackerReadinessChecker) IsReady(ctx context.Context) bool {
	if r.tracker == nil {
		r.logger.LogWarning("PVC tracker is not initialized")
		return false
	}

	// Create context with timeout
	checkCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// Try to sync with cluster to check connectivity
	err := r.tracker.SyncWithCluster(checkCtx)
	if err != nil {
		r.logger.LogError(err, "PVC tracker readiness check failed")
		return false
	}

	r.logger.LogDebug("PVC tracker readiness check passed")
	return true
}

// Name returns the name of this readiness checker
func (r *PVCTrackerReadinessChecker) Name() string {
	return "pvc-tracker"
}

// SetTimeout sets the timeout for readiness checks
func (r *PVCTrackerReadinessChecker) SetTimeout(timeout time.Duration) {
	r.timeout = timeout
}

// HealthCheckManager manages all health and readiness checks
type HealthCheckManager struct {
	healthCheckers    []HealthChecker
	readinessCheckers []ReadinessChecker
	logger            *EFSNSLogger
	timeout           time.Duration
	mutex             *sync.RWMutex
}

// NewHealthCheckManager creates a new health check manager
func NewHealthCheckManager(timeout time.Duration) *HealthCheckManager {
	if timeout <= 0 {
		timeout = DefaultHealthCheckTimeout
	}

	return &HealthCheckManager{
		healthCheckers:    make([]HealthChecker, 0),
		readinessCheckers: make([]ReadinessChecker, 0),
		logger:            NewEFSNSLogger("health-check-manager"),
		timeout:           timeout,
		mutex:             &sync.RWMutex{},
	}
}

// AddHealthChecker adds a health checker
func (m *HealthCheckManager) AddHealthChecker(checker HealthChecker) {
	if checker == nil {
		return
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.healthCheckers = append(m.healthCheckers, checker)
	m.logger.LogInfo("Health checker added", "checkerName", checker.Name())
}

// AddReadinessChecker adds a readiness checker
func (m *HealthCheckManager) AddReadinessChecker(checker ReadinessChecker) {
	if checker == nil {
		return
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.readinessCheckers = append(m.readinessCheckers, checker)
	m.logger.LogInfo("Readiness checker added", "checkerName", checker.Name())
}

// GetHealthCheckers returns all health checkers
func (m *HealthCheckManager) GetHealthCheckers() []HealthChecker {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	checkers := make([]HealthChecker, len(m.healthCheckers))
	copy(checkers, m.healthCheckers)
	return checkers
}

// GetReadinessCheckers returns all readiness checkers
func (m *HealthCheckManager) GetReadinessCheckers() []ReadinessChecker {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	checkers := make([]ReadinessChecker, len(m.readinessCheckers))
	copy(checkers, m.readinessCheckers)
	return checkers
}

// CheckHealth runs all health checks
func (m *HealthCheckManager) CheckHealth(ctx context.Context) (bool, error) {
	m.mutex.RLock()
	checkers := make([]HealthChecker, len(m.healthCheckers))
	copy(checkers, m.healthCheckers)
	m.mutex.RUnlock()

	if len(checkers) == 0 {
		m.logger.LogDebug("No health checkers configured, returning healthy")
		return true, nil
	}

	for _, checker := range checkers {
		if err := checker.Check(ctx); err != nil {
			m.logger.LogError(err, "Health check failed", "checkerName", checker.Name())
			return false, err
		}
	}

	m.logger.LogDebug("All health checks passed", "checkerCount", len(checkers))
	return true, nil
}

// CheckReadiness runs all readiness checks
func (m *HealthCheckManager) CheckReadiness(ctx context.Context) bool {
	m.mutex.RLock()
	checkers := make([]ReadinessChecker, len(m.readinessCheckers))
	copy(checkers, m.readinessCheckers)
	m.mutex.RUnlock()

	if len(checkers) == 0 {
		m.logger.LogDebug("No readiness checkers configured, returning ready")
		return true
	}

	for _, checker := range checkers {
		if !checker.IsReady(ctx) {
			m.logger.LogWarning("Readiness check failed", "checkerName", checker.Name())
			return false
		}
	}

	m.logger.LogDebug("All readiness checks passed", "checkerCount", len(checkers))
	return true
}

// SetupDefaultHealthChecks sets up default health and readiness checks
func (m *HealthCheckManager) SetupDefaultHealthChecks(
	efsClient EFSClient,
	kubernetesClient kubernetes.Interface,
	cache FileSystemCache,
	metricsCollector MetricsCollector,
	fsManager NamespaceFileSystemManager,
	pvcTracker PVCTracker,
) {
	m.logger.LogInfo("Setting up default health and readiness checks")

	// Health checks
	if efsClient != nil {
		m.AddHealthChecker(NewEFSAPIHealthChecker(efsClient))
	}

	if kubernetesClient != nil {
		m.AddHealthChecker(NewKubernetesAPIHealthChecker(kubernetesClient))
	}

	if cache != nil {
		m.AddHealthChecker(NewCacheHealthChecker(cache))
	}

	if metricsCollector != nil {
		m.AddHealthChecker(NewMetricsCollectorHealthChecker(metricsCollector))
	}

	// Readiness checks
	if fsManager != nil {
		m.AddReadinessChecker(NewFileSystemManagerReadinessChecker(fsManager))
	}

	if pvcTracker != nil {
		m.AddReadinessChecker(NewPVCTrackerReadinessChecker(pvcTracker))
	}

	m.logger.LogInfo("Default health and readiness checks setup completed",
		"healthCheckers", len(m.healthCheckers),
		"readinessCheckers", len(m.readinessCheckers))
}
