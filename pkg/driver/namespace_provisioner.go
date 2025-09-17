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
	"fmt"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/cloud"
)

const (
	// NamespaceProvisioningMode is the provisioning mode for namespace-level EFS
	NamespaceProvisioningMode = "efs-ns"

	// Default configuration values
	DefaultCacheTimeout     = 5 * time.Minute
	DefaultMaxRetries       = 3
	DefaultRetryDelay       = 1 * time.Second
	DefaultCleanupTimeout   = 10 * time.Minute
	DefaultHealthCheckInterval = 30 * time.Second
)

// EFSOptions contains options for creating EFS filesystems
type EFSOptions struct {
	PerformanceMode               string
	ThroughputMode               string
	ProvisionedThroughputInMibps int64
	Encrypted                    bool
	KmsKeyId                     string
	LifecyclePolicy              string
	BackupPolicy                 string
	Tags                         map[string]string
}

// NamespaceProvisionerInterface defines the interface for namespace-level EFS provisioning
type NamespaceProvisionerInterface interface {
	// EFS filesystem management
	CreateNamespaceEFS(ctx context.Context, namespace string, options *EFSOptions) (*cloud.FileSystem, error)
	GetNamespaceEFS(ctx context.Context, namespace string) (*cloud.FileSystem, error)
	DeleteNamespaceEFS(ctx context.Context, namespace string) error

	// Access Point management
	CreateAccessPointForPVC(ctx context.Context, pvcName, namespace string, options *cloud.AccessPointOptions) (*cloud.AccessPoint, error)
	DeleteAccessPointForPVC(ctx context.Context, pvcName, namespace string) error

	// Volume lifecycle operations - called from controller.go
	CreateNamespaceVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error)
	DeleteNamespaceVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error)

	// Lifecycle management
	Start(ctx context.Context) error
	Stop() error

	// Health and status
	IsHealthy() bool
	GetStatus() *ProvisionerStatus
}

// ProvisionerStatus represents the current status of the provisioner
type ProvisionerStatus struct {
	Started           bool
	Healthy           bool
	LastHealthCheck   time.Time
	ActiveNamespaces  int
	TotalEFSCreated   int64
	TotalErrors       int64
	LastError         error
	LastErrorTime     time.Time
}

// NamespaceProvisioner manages EFS filesystem provisioning at the namespace level
type NamespaceProvisioner struct {
	// Core dependencies
	cloud      cloud.Cloud
	k8sClient  kubernetes.Interface
	config     *rest.Config

	// Component dependencies
	mapper      NamespaceEFSMapperInterface
	lockManager LockManagerMap

	// Configuration
	options *ProvisionerOptions

	// Internal state
	status     *ProvisionerStatus
	statusLock sync.RWMutex

	// Cache for performance optimization
	efsCache    map[string]*CachedEFS
	cacheMutex  sync.RWMutex

	// Lifecycle management
	started    bool
	stopCh     chan struct{}
	wg         sync.WaitGroup
	startMutex sync.Mutex

	// Metrics and monitoring
	metricsCollector MetricsCollector
}

// ProvisionerOptions contains configuration options for NamespaceProvisioner
type ProvisionerOptions struct {
	// Cache configuration
	CacheTimeout time.Duration

	// Retry configuration
	MaxRetries       int
	RetryDelay       time.Duration
	RetryBackoffMax  time.Duration

	// Timeout configuration
	CreateTimeout    time.Duration
	DeleteTimeout    time.Duration
	CleanupTimeout   time.Duration

	// Health check configuration
	HealthCheckInterval time.Duration

	// Feature flags
	EnableAsyncCleanup    bool
	EnableCaching         bool
	EnableMetrics         bool
	EnableHealthCheck     bool

	// AWS-specific settings
	DefaultTags           map[string]string
	ClusterID             string
	Region                string
}

// CachedEFS represents a cached EFS filesystem with metadata
type CachedEFS struct {
	FileSystem   *cloud.FileSystem
	Namespace    string
	CreatedAt    time.Time
	LastAccessed time.Time
	AccessCount  int64
}

// MetricsCollector interface for collecting metrics
type MetricsCollector interface {
	IncEFSCreated(namespace string)
	IncEFSDeleted(namespace string)
	IncAccessPointCreated(namespace string)
	IncAccessPointDeleted(namespace string)
	RecordEFSCreationTime(namespace string, duration time.Duration)
	RecordError(operation, namespace string, err error)
	SetActiveNamespaces(count int)
}

// NewNamespaceProvisioner creates a new NamespaceProvisioner instance
func NewNamespaceProvisioner(cloud cloud.Cloud, k8sClient kubernetes.Interface, config *rest.Config, options *ProvisionerOptions) (*NamespaceProvisioner, error) {
	if cloud == nil {
		return nil, fmt.Errorf("cloud client cannot be nil")
	}
	if k8sClient == nil {
		return nil, fmt.Errorf("kubernetes client cannot be nil")
	}
	if config == nil {
		return nil, fmt.Errorf("kubernetes config cannot be nil")
	}
	if options == nil {
		options = DefaultProvisionerOptions()
	}

	// Validate required options
	if err := validateProvisionerOptions(options); err != nil {
		return nil, fmt.Errorf("invalid provisioner options: %w", err)
	}

	// Initialize namespace EFS mapper
	mapper, err := NewNamespaceEFSMapper(k8sClient, config, cloud)
	if err != nil {
		return nil, fmt.Errorf("failed to create namespace EFS mapper: %w", err)
	}

	// Initialize metrics collector if enabled
	var metricsCollector MetricsCollector
	if options.EnableMetrics {
		metricsCollector = NewNamespaceProvisionerMetrics()
	} else {
		metricsCollector = &NoOpMetricsCollector{}
	}

	provisioner := &NamespaceProvisioner{
		cloud:      cloud,
		k8sClient:  k8sClient,
		config:     config,
		mapper:     mapper,
		lockManager: NewLockManagerMap(),
		options:    options,
		status: &ProvisionerStatus{
			Started:         false,
			Healthy:         false,
			LastHealthCheck: time.Now(),
		},
		efsCache:         make(map[string]*CachedEFS),
		stopCh:           make(chan struct{}),
		metricsCollector: metricsCollector,
	}

	klog.V(2).Infof("NamespaceProvisioner created successfully with options: cacheTimeout=%v, maxRetries=%d",
		options.CacheTimeout, options.MaxRetries)

	return provisioner, nil
}

// DefaultProvisionerOptions returns default configuration options
func DefaultProvisionerOptions() *ProvisionerOptions {
	return &ProvisionerOptions{
		CacheTimeout:          DefaultCacheTimeout,
		MaxRetries:            DefaultMaxRetries,
		RetryDelay:            DefaultRetryDelay,
		RetryBackoffMax:       DefaultRetryDelay * 8,
		CreateTimeout:         5 * time.Minute,
		DeleteTimeout:         3 * time.Minute,
		CleanupTimeout:        DefaultCleanupTimeout,
		HealthCheckInterval:   DefaultHealthCheckInterval,
		EnableAsyncCleanup:    true,
		EnableCaching:         true,
		EnableMetrics:         true,
		EnableHealthCheck:     true,
		DefaultTags:           make(map[string]string),
	}
}

// validateProvisionerOptions validates the provisioner options
func validateProvisionerOptions(options *ProvisionerOptions) error {
	if options.CacheTimeout <= 0 {
		return fmt.Errorf("cache timeout must be positive")
	}
	if options.MaxRetries < 0 {
		return fmt.Errorf("max retries cannot be negative")
	}
	if options.RetryDelay <= 0 {
		return fmt.Errorf("retry delay must be positive")
	}
	if options.CreateTimeout <= 0 {
		return fmt.Errorf("create timeout must be positive")
	}
	if options.DeleteTimeout <= 0 {
		return fmt.Errorf("delete timeout must be positive")
	}
	if options.HealthCheckInterval <= 0 {
		return fmt.Errorf("health check interval must be positive")
	}
	return nil
}

// Start initializes and starts the NamespaceProvisioner
func (np *NamespaceProvisioner) Start(ctx context.Context) error {
	np.startMutex.Lock()
	defer np.startMutex.Unlock()

	if np.started {
		return fmt.Errorf("provisioner is already started")
	}

	klog.V(2).Infof("Starting NamespaceProvisioner...")

	// Start the namespace EFS mapper
	if err := np.mapper.Start(ctx); err != nil {
		return fmt.Errorf("failed to start namespace EFS mapper: %w", err)
	}

	// Start health checking if enabled
	if np.options.EnableHealthCheck {
		np.wg.Add(1)
		go np.healthCheckLoop()
	}

	// Start cache cleanup if enabled
	if np.options.EnableCaching {
		np.wg.Add(1)
		go np.cacheCleanupLoop()
	}

	// Update status
	np.updateStatus(func(status *ProvisionerStatus) {
		status.Started = true
		status.Healthy = true
		status.LastHealthCheck = time.Now()
	})

	np.started = true
	klog.V(2).Infof("NamespaceProvisioner started successfully")

	return nil
}

// Stop gracefully shuts down the NamespaceProvisioner
func (np *NamespaceProvisioner) Stop() error {
	np.startMutex.Lock()
	defer np.startMutex.Unlock()

	if !np.started {
		return nil
	}

	klog.V(2).Infof("Stopping NamespaceProvisioner...")

	// Signal all goroutines to stop
	close(np.stopCh)

	// Wait for all goroutines to finish
	np.wg.Wait()

	// Stop the namespace EFS mapper
	np.mapper.Stop()

	// Clear cache
	np.clearCache()

	// Update status
	np.updateStatus(func(status *ProvisionerStatus) {
		status.Started = false
		status.Healthy = false
	})

	np.started = false
	klog.V(2).Infof("NamespaceProvisioner stopped successfully")

	return nil
}

// IsHealthy returns the current health status
func (np *NamespaceProvisioner) IsHealthy() bool {
	np.statusLock.RLock()
	defer np.statusLock.RUnlock()
	return np.status.Healthy
}

// GetStatus returns a copy of the current status
func (np *NamespaceProvisioner) GetStatus() *ProvisionerStatus {
	np.statusLock.RLock()
	defer np.statusLock.RUnlock()

	// Return a copy to avoid race conditions
	statusCopy := *np.status
	return &statusCopy
}

// updateStatus safely updates the provisioner status
func (np *NamespaceProvisioner) updateStatus(updater func(*ProvisionerStatus)) {
	np.statusLock.Lock()
	defer np.statusLock.Unlock()
	updater(np.status)
}

// healthCheckLoop performs periodic health checks
func (np *NamespaceProvisioner) healthCheckLoop() {
	defer np.wg.Done()

	ticker := time.NewTicker(np.options.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-np.stopCh:
			return
		case <-ticker.C:
			np.performHealthCheck()
		}
	}
}

// performHealthCheck checks the health of the provisioner
func (np *NamespaceProvisioner) performHealthCheck() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	healthy := true

	// Check if mapper is responsive
	if _, err := np.mapper.ListMappings(ctx); err != nil {
		klog.Warningf("Health check failed: mapper not responsive: %v", err)
		healthy = false
	}

	// Update status
	np.updateStatus(func(status *ProvisionerStatus) {
		status.Healthy = healthy
		status.LastHealthCheck = time.Now()
		if !healthy {
			status.TotalErrors++
		}
	})
}

// cacheCleanupLoop performs periodic cache cleanup
func (np *NamespaceProvisioner) cacheCleanupLoop() {
	defer np.wg.Done()

	ticker := time.NewTicker(np.options.CacheTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-np.stopCh:
			return
		case <-ticker.C:
			np.cleanupExpiredCache()
		}
	}
}

// cleanupExpiredCache removes expired entries from the cache
func (np *NamespaceProvisioner) cleanupExpiredCache() {
	np.cacheMutex.Lock()
	defer np.cacheMutex.Unlock()

	now := time.Now()
	expiredCount := 0

	for namespace, cached := range np.efsCache {
		if now.Sub(cached.LastAccessed) > np.options.CacheTimeout {
			delete(np.efsCache, namespace)
			expiredCount++
		}
	}

	if expiredCount > 0 {
		klog.V(4).Infof("Cleaned up %d expired cache entries", expiredCount)
	}
}

// clearCache clears all cache entries
func (np *NamespaceProvisioner) clearCache() {
	np.cacheMutex.Lock()
	defer np.cacheMutex.Unlock()
	np.efsCache = make(map[string]*CachedEFS)
}

// getCachedEFS retrieves EFS from cache if available and not expired
func (np *NamespaceProvisioner) getCachedEFS(namespace string) *cloud.FileSystem {
	if !np.options.EnableCaching {
		return nil
	}

	np.cacheMutex.RLock()
	defer np.cacheMutex.RUnlock()

	cached, exists := np.efsCache[namespace]
	if !exists {
		return nil
	}

	// Check if cache entry is expired
	if time.Since(cached.LastAccessed) > np.options.CacheTimeout {
		return nil
	}

	// Update access time (this is safe as we only modify LastAccessed and AccessCount)
	cached.LastAccessed = time.Now()
	cached.AccessCount++

	return cached.FileSystem
}

// setCachedEFS stores EFS in cache
func (np *NamespaceProvisioner) setCachedEFS(namespace string, fs *cloud.FileSystem) {
	if !np.options.EnableCaching || fs == nil {
		return
	}

	np.cacheMutex.Lock()
	defer np.cacheMutex.Unlock()

	now := time.Now()
	np.efsCache[namespace] = &CachedEFS{
		FileSystem:   fs,
		Namespace:    namespace,
		CreatedAt:    now,
		LastAccessed: now,
		AccessCount:  1,
	}
}

// removeCachedEFS removes EFS from cache
func (np *NamespaceProvisioner) removeCachedEFS(namespace string) {
	if !np.options.EnableCaching {
		return
	}

	np.cacheMutex.Lock()
	defer np.cacheMutex.Unlock()
	delete(np.efsCache, namespace)
}

// NoOpMetricsCollector is a no-op implementation of MetricsCollector
type NoOpMetricsCollector struct{}

func (n *NoOpMetricsCollector) IncEFSCreated(namespace string)                              {}
func (n *NoOpMetricsCollector) IncEFSDeleted(namespace string)                              {}
func (n *NoOpMetricsCollector) IncAccessPointCreated(namespace string)                     {}
func (n *NoOpMetricsCollector) IncAccessPointDeleted(namespace string)                     {}
func (n *NoOpMetricsCollector) RecordEFSCreationTime(namespace string, duration time.Duration) {}
func (n *NoOpMetricsCollector) RecordError(operation, namespace string, err error)         {}
func (n *NoOpMetricsCollector) SetActiveNamespaces(count int)                               {}