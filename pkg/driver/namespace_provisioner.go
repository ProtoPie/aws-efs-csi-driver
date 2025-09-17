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
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
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

	// Access Point defaults
	DefaultBasePath         = "/dynamic_provisioning"
	DefaultDirectoryPerms   = "700"
	DefaultUid              = 1001
	DefaultGid              = 1001
	DefaultGidRangeStart    = 1000
	DefaultGidRangeEnd      = 2000
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

// AccessPointCreationOptions contains options for creating Access Points
type AccessPointCreationOptions struct {
	FileSystemId          string
	BasePath              string
	SubPathPattern        string
	EnsureUniqueDirectory bool
	DirectoryPerms        string
	Uid                   *int64
	Gid                   *int64
	GidRangeStart         *int64
	GidRangeEnd           *int64
	ReuseAccessPoint      bool
	Tags                  map[string]string
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
	lockManager *LockManagerMap

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

	// Network configuration for mount targets
	SubnetIds             []string          // Configured subnet IDs for mount target creation
	AvailabilityZones     []string          // Availability zones corresponding to subnet IDs
	VpcId                 string            // VPC ID for the cluster
	SecurityGroupId       string            // Security group ID for EFS mount targets
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

	// Initialize lock manager
	lockManager := NewLockManagerMap()

	provisioner := &NamespaceProvisioner{
		cloud:       cloud,
		k8sClient:   k8sClient,
		config:      config,
		mapper:      mapper,
		lockManager: &lockManager,
		options:     options,
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

// CreateNamespaceEFS creates an EFS filesystem for the given namespace
func (np *NamespaceProvisioner) CreateNamespaceEFS(ctx context.Context, namespace string, options *EFSOptions) (*cloud.FileSystem, error) {
	startTime := time.Now()

	// Check if EFS already exists for this namespace
	existing, err := np.GetNamespaceEFS(ctx, namespace)
	if err == nil && existing != nil {
		klog.V(2).Infof("EFS filesystem already exists for namespace %s: %s", namespace, existing.FileSystemId)
		return existing, nil
	}

	// Acquire namespace-level lock to prevent concurrent EFS creation for the same namespace
	lockKey := fmt.Sprintf("namespace:%s", namespace)
	if !np.lockManager.lockMutex(lockKey, np.options.CreateTimeout) {
		err := fmt.Errorf("failed to acquire lock for namespace %s within timeout", namespace)
		np.metricsCollector.RecordError("acquire_lock", namespace, err)
		return nil, err
	}
	defer func() {
		np.lockManager.unlockMutex(lockKey)
	}()

	// Double-check if EFS was created while waiting for lock
	existing, err = np.GetNamespaceEFS(ctx, namespace)
	if err == nil && existing != nil {
		klog.V(2).Infof("EFS filesystem was created by another process for namespace %s: %s", namespace, existing.FileSystemId)
		return existing, nil
	}

	// Generate client token for idempotency
	clientToken := fmt.Sprintf("efs-ns-%s-%d", namespace, time.Now().UnixNano())

	// Build tags including default tags and namespace-specific tags
	tags := make(map[string]string)

	// Add default tags from options
	for k, v := range np.options.DefaultTags {
		tags[k] = v
	}

	// Add user-provided tags
	if options != nil && options.Tags != nil {
		for k, v := range options.Tags {
			tags[k] = v
		}
	}

	// Add required namespace provisioning tags
	tags["kubernetes.io/namespace"] = namespace
	tags["kubernetes.io/provisioning-mode"] = "efs-ns"
	if np.options.ClusterID != "" {
		tags[fmt.Sprintf("kubernetes.io/cluster/%s", np.options.ClusterID)] = "owned"
	}

	// Build FileSystemOptions
	fsOptions := &cloud.FileSystemOptions{
		Tags: tags,
	}

	// Apply EFS configuration options if provided
	if options != nil {
		fsOptions.PerformanceMode = options.PerformanceMode
		fsOptions.ThroughputMode = options.ThroughputMode
		fsOptions.ProvisionedThroughputInMibps = options.ProvisionedThroughputInMibps
		fsOptions.Encrypted = options.Encrypted
		fsOptions.KmsKeyId = options.KmsKeyId
		fsOptions.LifecyclePolicy = options.LifecyclePolicy
		fsOptions.BackupPolicy = options.BackupPolicy
	}

	// Set default values if not specified
	if fsOptions.PerformanceMode == "" {
		fsOptions.PerformanceMode = "generalPurpose"
	}
	if fsOptions.ThroughputMode == "" {
		fsOptions.ThroughputMode = "bursting"
	}
	if !fsOptions.Encrypted {
		fsOptions.Encrypted = true // Default to encrypted for security
	}

	klog.V(2).Infof("Creating EFS filesystem for namespace %s with options: %+v", namespace, fsOptions)

	// Create the EFS filesystem
	filesystem, err := np.cloud.CreateFileSystem(ctx, clientToken, fsOptions)
	if err != nil {
		np.metricsCollector.RecordError("create_filesystem", namespace, err)
		return nil, fmt.Errorf("failed to create EFS filesystem for namespace %s: %w", namespace, err)
	}

	// Record creation time metric
	np.metricsCollector.RecordEFSCreationTime(namespace, time.Since(startTime))
	np.metricsCollector.IncEFSCreated(namespace)

	klog.V(2).Infof("Successfully created EFS filesystem %s for namespace %s in %v",
		filesystem.FileSystemId, namespace, time.Since(startTime))

	// Store the mapping in the namespace EFS mapper
	// TODO: Replace with actual account ID from STS GetCallerIdentity
	fileSystemArn := fmt.Sprintf("arn:aws:elasticfilesystem:%s:123456789012:file-system/%s", np.options.Region, filesystem.FileSystemId)
	if _, err := np.mapper.CreateOrUpdateMapping(ctx, namespace, filesystem.FileSystemId, fileSystemArn, np.options.Region); err != nil {
		klog.Warningf("Failed to store namespace mapping for %s -> %s: %v", namespace, filesystem.FileSystemId, err)
		// Don't fail the entire operation, the mapping can be recovered from tags
	}

	// Create mount targets for the EFS filesystem
	if err := np.createMountTargetsForEFS(ctx, filesystem.FileSystemId, namespace); err != nil {
		klog.Warningf("Failed to create mount targets for EFS %s in namespace %s: %v", filesystem.FileSystemId, namespace, err)
		// Don't fail the entire operation as mount targets can be created later
		// The node service will handle cases where mount targets don't exist yet
	}

	// Cache the filesystem
	np.setCachedEFS(namespace, filesystem)

	// Update provisioner status
	np.updateStatus(func(status *ProvisionerStatus) {
		status.TotalEFSCreated++
		status.ActiveNamespaces = len(np.efsCache)
	})

	return filesystem, nil
}

// GetNamespaceEFS retrieves the EFS filesystem for the given namespace
func (np *NamespaceProvisioner) GetNamespaceEFS(ctx context.Context, namespace string) (*cloud.FileSystem, error) {
	// Check cache first
	if cached := np.getCachedEFS(namespace); cached != nil {
		klog.V(4).Infof("Found cached EFS for namespace %s: %s", namespace, cached.FileSystemId)
		return cached, nil
	}

	// Check the namespace EFS mapper
	mapping, err := np.mapper.GetMapping(ctx, namespace)
	if err == nil && mapping != nil && mapping.FileSystemID != "" {
		// Retrieve filesystem details from AWS
		fs, err := np.cloud.DescribeFileSystem(ctx, mapping.FileSystemID)
		if err == nil {
			np.setCachedEFS(namespace, fs)
			return fs, nil
		}
		klog.Warningf("Failed to describe EFS %s for namespace %s: %v", mapping.FileSystemID, namespace, err)
	}

	// Fallback: search by tags
	tags := map[string]string{
		"kubernetes.io/namespace":         namespace,
		"kubernetes.io/provisioning-mode": "efs-ns",
	}
	if np.options.ClusterID != "" {
		tags[fmt.Sprintf("kubernetes.io/cluster/%s", np.options.ClusterID)] = "owned"
	}

	fileSystems, err := np.cloud.FindFileSystemsByTags(ctx, tags)
	if err != nil {
		return nil, fmt.Errorf("failed to find EFS for namespace %s: %w", namespace, err)
	}

	if len(fileSystems) == 0 {
		return nil, fmt.Errorf("no EFS filesystem found for namespace %s", namespace)
	}

	if len(fileSystems) > 1 {
		klog.Warningf("Multiple EFS filesystems found for namespace %s, using the first one", namespace)
	}

	filesystem := fileSystems[0]

	// Update the mapping if it was missing
	// TODO: Replace with actual account ID from STS GetCallerIdentity
	fileSystemArn := fmt.Sprintf("arn:aws:elasticfilesystem:%s:123456789012:file-system/%s", np.options.Region, filesystem.FileSystemId)
	if _, err := np.mapper.CreateOrUpdateMapping(ctx, namespace, filesystem.FileSystemId, fileSystemArn, np.options.Region); err != nil {
		klog.Warningf("Failed to restore namespace mapping for %s -> %s: %v", namespace, filesystem.FileSystemId, err)
	}

	// Cache the filesystem
	np.setCachedEFS(namespace, filesystem)

	return filesystem, nil
}

// createMountTargetsForEFS creates mount targets for the given EFS filesystem
// This ensures multi-AZ availability for the EFS filesystem
func (np *NamespaceProvisioner) createMountTargetsForEFS(ctx context.Context, fileSystemId, namespace string) error {
	klog.V(2).Infof("Creating mount targets for EFS %s in namespace %s", fileSystemId, namespace)

	// Check if mount targets already exist
	existingMT, err := np.cloud.DescribeMountTargets(ctx, fileSystemId, "")
	if err == nil && existingMT != nil {
		klog.V(2).Infof("Mount targets already exist for EFS %s", fileSystemId)
		return nil
	}

	// Get available subnets for mount target creation
	subnets, err := np.getAvailableSubnets(ctx)
	if err != nil {
		return fmt.Errorf("failed to get available subnets: %w", err)
	}

	if len(subnets) == 0 {
		return fmt.Errorf("no available subnets found for mount target creation")
	}

	// Get default security group for EFS
	securityGroupId, err := np.getEFSSecurityGroup(ctx)
	if err != nil {
		klog.Warningf("Failed to get EFS security group, proceeding without security group: %v", err)
		securityGroupId = "" // EFS will use the default VPC security group
	}

	// Create mount targets in multiple subnets for high availability
	// Limit to first 3 subnets to avoid hitting EFS mount target limits
	maxMountTargets := 3
	if len(subnets) < maxMountTargets {
		maxMountTargets = len(subnets)
	}

	var createdMountTargets []*cloud.MountTarget
	var createErrors []error

	for i := 0; i < maxMountTargets; i++ {
		subnet := subnets[i]
		klog.V(4).Infof("Creating mount target for EFS %s in subnet %s (AZ: %s)", fileSystemId, subnet.SubnetId, subnet.AvailabilityZone)

		// Create mount target with retry logic
		mountTarget, err := np.createMountTargetWithRetry(ctx, fileSystemId, subnet.SubnetId, securityGroupId)
		if err != nil {
			createErrors = append(createErrors, fmt.Errorf("failed to create mount target in subnet %s: %w", subnet.SubnetId, err))
			continue
		}

		createdMountTargets = append(createdMountTargets, mountTarget)
		klog.V(2).Infof("Successfully created mount target %s for EFS %s in AZ %s",
			mountTarget.MountTargetId, fileSystemId, mountTarget.AZName)
	}

	// If we couldn't create any mount targets, return the errors
	if len(createdMountTargets) == 0 {
		return fmt.Errorf("failed to create any mount targets: %v", createErrors)
	}

	// Log warnings for any failed mount target creations
	if len(createErrors) > 0 {
		for _, err := range createErrors {
			klog.Warningf("Mount target creation warning: %v", err)
		}
	}

	klog.V(2).Infof("Successfully created %d mount targets for EFS %s", len(createdMountTargets), fileSystemId)
	return nil
}

// createMountTargetWithRetry creates a mount target with retry logic
func (np *NamespaceProvisioner) createMountTargetWithRetry(ctx context.Context, fileSystemId, subnetId, securityGroupId string) (*cloud.MountTarget, error) {
	var lastErr error

	for attempt := 0; attempt < np.options.MaxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff with jitter
			backoffDelay := np.options.RetryDelay * time.Duration(1<<uint(attempt-1))
			if backoffDelay > np.options.RetryBackoffMax {
				backoffDelay = np.options.RetryBackoffMax
			}

			// Add jitter to prevent thundering herd
			jitter := time.Duration(rand.Int63n(int64(backoffDelay / 4)))
			time.Sleep(backoffDelay + jitter)
		}

		mountTarget, err := np.cloud.CreateMountTarget(ctx, fileSystemId, subnetId, securityGroupId)
		if err == nil {
			return mountTarget, nil
		}

		// Check if it's a retryable error
		if err == cloud.ErrAlreadyExists {
			klog.V(4).Infof("Mount target already exists in subnet %s", subnetId)
			return nil, nil // Not an error, just skip this subnet
		}

		lastErr = err
		klog.V(4).Infof("Mount target creation attempt %d failed: %v", attempt+1, err)
	}

	return nil, fmt.Errorf("failed to create mount target after %d attempts: %w", np.options.MaxRetries, lastErr)
}

// SubnetInfo represents subnet information for mount target creation
type SubnetInfo struct {
	SubnetId         string
	AvailabilityZone string
	VpcId            string
}

// getAvailableSubnets gets available subnets for mount target creation
// This implementation provides multiple strategies for subnet discovery
func (np *NamespaceProvisioner) getAvailableSubnets(ctx context.Context) ([]*SubnetInfo, error) {
	klog.V(4).Infof("Getting available subnets for mount target creation")

	// Strategy 1: Check for configured subnets in options
	if len(np.options.SubnetIds) > 0 {
		return np.getConfiguredSubnets(ctx)
	}

	// Strategy 2: Auto-discover using existing EFS mount targets
	subnets, err := np.discoverSubnetsFromExistingEFS(ctx)
	if err == nil && len(subnets) > 0 {
		klog.V(2).Infof("Found %d subnets from existing EFS mount targets", len(subnets))
		return subnets, nil
	}

	// Strategy 3: Use default subnets based on cluster configuration
	return np.getDefaultSubnets(ctx)
}

// getConfiguredSubnets returns subnets configured in provisioner options
func (np *NamespaceProvisioner) getConfiguredSubnets(ctx context.Context) ([]*SubnetInfo, error) {
	var subnets []*SubnetInfo

	for i, subnetId := range np.options.SubnetIds {
		// For configured subnets, derive AZ from instance metadata if available
		az := ""
		if i < len(np.options.AvailabilityZones) {
			az = np.options.AvailabilityZones[i]
		} else if np.cloud.GetMetadata() != nil {
			// Use metadata service to get current AZ as fallback
			az = np.cloud.GetMetadata().GetAvailabilityZone()
		}

		subnet := &SubnetInfo{
			SubnetId:         subnetId,
			AvailabilityZone: az,
			VpcId:            np.options.VpcId,
		}
		subnets = append(subnets, subnet)
	}

	klog.V(2).Infof("Using %d configured subnets for mount target creation", len(subnets))
	return subnets, nil
}

// discoverSubnetsFromExistingEFS discovers subnets by examining existing EFS mount targets
func (np *NamespaceProvisioner) discoverSubnetsFromExistingEFS(ctx context.Context) ([]*SubnetInfo, error) {
	klog.V(4).Infof("Attempting to discover subnets from existing EFS mount targets")

	// Find existing EFS filesystems with the same cluster tag
	tags := map[string]string{}
	if np.options.ClusterID != "" {
		tags[fmt.Sprintf("kubernetes.io/cluster/%s", np.options.ClusterID)] = "owned"
	}

	fileSystems, err := np.cloud.FindFileSystemsByTags(ctx, tags)
	if err != nil || len(fileSystems) == 0 {
		klog.V(4).Infof("No existing EFS filesystems found for subnet discovery")
		return nil, fmt.Errorf("no existing EFS filesystems found")
	}

	// Collect unique subnets from existing mount targets
	subnetMap := make(map[string]*SubnetInfo)

	for _, fs := range fileSystems {
		// Get mount targets for this filesystem
		mountTarget, err := np.cloud.DescribeMountTargets(ctx, fs.FileSystemId, "")
		if err != nil {
			continue
		}

		if mountTarget != nil {
			// Extract subnet information from mount target
			// Note: The current DescribeMountTargets doesn't return subnet ID
			// This is a limitation we'll note for future enhancement
			subnet := &SubnetInfo{
				SubnetId:         "", // Not available from current API
				AvailabilityZone: mountTarget.AZName,
				VpcId:            "", // Not available from current API
			}

			key := mountTarget.AZName
			if _, exists := subnetMap[key]; !exists {
				subnetMap[key] = subnet
			}
		}
	}

	var subnets []*SubnetInfo
	for _, subnet := range subnetMap {
		subnets = append(subnets, subnet)
	}

	if len(subnets) == 0 {
		return nil, fmt.Errorf("no subnets discovered from existing EFS mount targets")
	}

	return subnets, nil
}

// getDefaultSubnets returns default subnets based on cluster metadata
func (np *NamespaceProvisioner) getDefaultSubnets(ctx context.Context) ([]*SubnetInfo, error) {
	klog.V(4).Infof("Using default subnet strategy based on instance metadata")

	// Get current instance availability zone from metadata
	metadata := np.cloud.GetMetadata()
	if metadata == nil {
		return nil, fmt.Errorf("metadata service not available for subnet detection")
	}

	currentAZ := metadata.GetAvailabilityZone()
	if currentAZ == "" {
		return nil, fmt.Errorf("current availability zone not available from metadata")
	}

	// Create a placeholder subnet for the current AZ
	// In a real implementation, this would:
	// 1. Query EC2 to find subnets in the current VPC
	// 2. Filter by availability zones
	// 3. Return actual subnet IDs

	// For now, return placeholder that will cause graceful handling
	klog.V(2).Infof("Current AZ from metadata: %s", currentAZ)
	klog.Warningf("Default subnet strategy requires EC2 integration - mount targets must be created manually or configured via options")

	return []*SubnetInfo{}, fmt.Errorf("automatic subnet discovery requires EC2 integration - please configure subnets manually")
}

// getEFSSecurityGroup gets the security group for EFS mount targets
func (np *NamespaceProvisioner) getEFSSecurityGroup(ctx context.Context) (string, error) {
	klog.V(4).Infof("Getting EFS security group configuration")

	// Strategy 1: Use configured security group if available
	if np.options.SecurityGroupId != "" {
		klog.V(2).Infof("Using configured security group: %s", np.options.SecurityGroupId)
		return np.options.SecurityGroupId, nil
	}

	// Strategy 2: Look for existing EFS security groups by tags
	// TODO: Implement EC2 security group discovery
	// This would search for security groups with specific tags like:
	// - kubernetes.io/cluster/<cluster-id>
	// - kubernetes.io/service/efs

	// Strategy 3: Use default VPC security group
	klog.V(2).Infof("No security group configured, using default VPC security group")
	klog.Warningf("Using default security group may require manual NFS rule configuration")

	return "", nil // Empty string will use default VPC security group
}

// DeleteNamespaceEFS deletes the EFS filesystem for the given namespace
func (np *NamespaceProvisioner) DeleteNamespaceEFS(ctx context.Context, namespace string) error {
	// This implementation would be added later as part of the cleanup logic
	// For now, we'll just log and return nil (retain policy)
	klog.V(2).Infof("Delete EFS for namespace %s requested - using retain policy", namespace)
	return nil
}

// DeleteNamespaceVolume implements the namespace-level EFS volume deletion logic
// This method is called from controller.go when provisioningMode is "efs-ns"
func (np *NamespaceProvisioner) DeleteNamespaceVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	klog.V(4).Infof("DeleteNamespaceVolume: called with request: %+v", req)

	// Validate input parameters
	if req == nil {
		return nil, fmt.Errorf("DeleteVolumeRequest cannot be nil")
	}

	volumeId := req.GetVolumeId()
	if volumeId == "" {
		return nil, fmt.Errorf("volume ID cannot be empty")
	}

	// In namespace provisioning mode, the volumeId is the Access Point ID
	accessPointId := volumeId

	klog.V(2).Infof("Deleting namespace volume with Access Point ID: %s", accessPointId)

	// Acquire lock to prevent concurrent operations on the same Access Point
	lockKey := fmt.Sprintf("accesspoint-delete:%s", accessPointId)
	if !np.lockManager.lockMutex(lockKey, np.options.DeleteTimeout) {
		err := fmt.Errorf("failed to acquire lock for deleting Access Point %s within timeout", accessPointId)
		np.metricsCollector.RecordError("acquire_delete_lock", "unknown", err)
		return nil, err
	}
	defer func() {
		np.lockManager.unlockMutex(lockKey)
	}()

	// Get Access Point details to extract namespace and PVC information
	accessPoint, err := np.cloud.DescribeAccessPoint(ctx, accessPointId)
	if err != nil {
		if err == cloud.ErrNotFound {
			klog.V(2).Infof("Access Point %s not found - assuming already deleted", accessPointId)
			return &csi.DeleteVolumeResponse{}, nil
		}
		np.metricsCollector.RecordError("describe_accesspoint", "unknown", err)
		return nil, fmt.Errorf("failed to describe Access Point %s: %w", accessPointId, err)
	}

	// Extract namespace and PVC name from Access Point tags
	namespace, pvcName, err := np.extractMetadataFromAccessPoint(accessPoint)
	if err != nil {
		klog.Warningf("Failed to extract metadata from Access Point %s: %v", accessPointId, err)
		// Continue with deletion even if we can't extract metadata
		namespace = "unknown"
		pvcName = "unknown"
	}

	klog.V(2).Infof("Deleting Access Point %s for PVC %s in namespace %s", accessPointId, pvcName, namespace)

	// Delete the Access Point
	if err := np.cloud.DeleteAccessPoint(ctx, accessPointId); err != nil {
		if err == cloud.ErrNotFound {
			klog.V(2).Infof("Access Point %s not found - assuming already deleted", accessPointId)
		} else {
			np.metricsCollector.RecordError("delete_accesspoint", namespace, err)
			return nil, fmt.Errorf("failed to delete Access Point %s: %w", accessPointId, err)
		}
	} else {
		// Record successful deletion metrics
		np.metricsCollector.IncAccessPointDeleted(namespace)
		klog.V(2).Infof("Successfully deleted Access Point %s for PVC %s in namespace %s", accessPointId, pvcName, namespace)
	}

	// Check if this was the last Access Point in the namespace
	// and handle EFS cleanup according to cleanup policy
	if namespace != "unknown" {
		if err := np.handleNamespaceCleanup(ctx, namespace, accessPointId); err != nil {
			klog.Warningf("Failed to handle namespace cleanup for %s: %v", namespace, err)
			// Don't fail the entire operation for cleanup issues
		}
	}

	return &csi.DeleteVolumeResponse{}, nil
}

// extractMetadataFromAccessPoint extracts namespace and PVC name from Access Point tags
func (np *NamespaceProvisioner) extractMetadataFromAccessPoint(accessPoint *cloud.AccessPoint) (namespace, pvcName string, err error) {
	// Strategy 1: Find namespace by filesystem ID using the namespace EFS mapper
	if accessPoint.FileSystemId != "" {
		namespace, err = np.findNamespaceByFileSystemID(context.Background(), accessPoint.FileSystemId)
		if err == nil && namespace != "" {
			// Extract PVC name from Access Point path if possible
			pvcName = np.extractPVCNameFromPath(accessPoint.AccessPointRootDir)
			if pvcName == "" {
				pvcName = "unknown-pvc"
			}
			return namespace, pvcName, nil
		}
		klog.V(4).Infof("Failed to find namespace by filesystem ID %s: %v", accessPoint.FileSystemId, err)
	}

	// Strategy 2: Extract from Access Point path structure if it follows our convention
	// Our convention: /dynamic_provisioning/namespace/pvcName/uuid
	namespace, pvcName = np.extractFromPathStructure(accessPoint.AccessPointRootDir)
	if namespace != "" && pvcName != "" {
		return namespace, pvcName, nil
	}

	// Strategy 3: Access Point tags (requires cloud interface extension)
	// This would be the preferred method but requires extending the cloud.AccessPoint struct
	// to include tags or adding a new method to get Access Point tags
	klog.V(4).Infof("Could not extract metadata from Access Point path: %s", accessPoint.AccessPointRootDir)

	return "", "", fmt.Errorf("unable to extract namespace and PVC name from Access Point %s", accessPoint.AccessPointId)
}

// findNamespaceByFileSystemID finds the namespace that owns a given filesystem ID
func (np *NamespaceProvisioner) findNamespaceByFileSystemID(ctx context.Context, fileSystemId string) (string, error) {
	// Get all namespace mappings
	mappings, err := np.mapper.ListMappings(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to list namespace mappings: %w", err)
	}

	// Find the mapping with the matching filesystem ID
	for _, mapping := range mappings {
		if mapping.FileSystemID == fileSystemId {
			return mapping.Namespace, nil
		}
	}

	return "", fmt.Errorf("no namespace found for filesystem ID %s", fileSystemId)
}

// extractPVCNameFromPath attempts to extract PVC name from the Access Point path
func (np *NamespaceProvisioner) extractPVCNameFromPath(path string) string {
	if path == "" {
		return ""
	}

	// Our convention creates paths like: /dynamic_provisioning/namespace/pvc-name/uuid
	// Try to extract the PVC name from this structure
	pathParts := strings.Split(strings.Trim(path, "/"), "/")

	// Expected structure: [dynamic_provisioning, namespace, pvc-name, uuid]
	if len(pathParts) >= 3 {
		// The third part should be the PVC name (or volume name)
		pvcName := pathParts[2]

		// Remove UUID suffix if it exists (UUIDs are 36 characters with dashes)
		if len(pathParts) == 4 && len(pathParts[3]) == 36 {
			return pvcName
		}

		// If no UUID part, try to extract from the pvc name itself if it has UUID suffix
		if strings.Contains(pvcName, "-") {
			parts := strings.Split(pvcName, "-")
			if len(parts) >= 2 {
				// Remove the last part if it looks like a UUID (length > 30)
				lastPart := parts[len(parts)-1]
				if len(lastPart) > 30 {
					return strings.Join(parts[:len(parts)-1], "-")
				}
			}
		}

		return pvcName
	}

	return ""
}

// extractFromPathStructure attempts to extract namespace and PVC name from path structure
func (np *NamespaceProvisioner) extractFromPathStructure(path string) (namespace, pvcName string) {
	if path == "" {
		return "", ""
	}

	// Our convention creates paths like: /dynamic_provisioning/namespace/pvc-name/uuid
	pathParts := strings.Split(strings.Trim(path, "/"), "/")

	// Expected structure: [dynamic_provisioning, namespace, pvc-name, uuid] or similar
	if len(pathParts) >= 3 {
		// Skip the first part (base path like "dynamic_provisioning")
		namespace = pathParts[1]
		pvcName = pathParts[2]

		// Validate that namespace looks reasonable (basic validation)
		if len(namespace) > 0 && !strings.Contains(namespace, ".") && len(namespace) < 64 {
			// Clean up PVC name if it has UUID suffix
			if len(pathParts) == 4 && len(pathParts[3]) == 36 {
				// UUID is separate, use pvcName as-is
				return namespace, pvcName
			}

			// Try to clean UUID from PVC name if needed
			cleanPVCName := np.extractPVCNameFromPath(path)
			if cleanPVCName != "" {
				pvcName = cleanPVCName
			}

			return namespace, pvcName
		}
	}

	return "", ""
}

// handleNamespaceCleanup handles cleanup logic for a namespace after Access Point deletion
func (np *NamespaceProvisioner) handleNamespaceCleanup(ctx context.Context, namespace, deletedAccessPointId string) error {
	klog.V(4).Infof("Handling namespace cleanup for %s after deleting Access Point %s", namespace, deletedAccessPointId)

	// Get the namespace EFS filesystem
	filesystem, err := np.GetNamespaceEFS(ctx, namespace)
	if err != nil {
		if err.Error() == fmt.Sprintf("no EFS filesystem found for namespace %s", namespace) {
			klog.V(2).Infof("No EFS filesystem found for namespace %s - cleanup not needed", namespace)
			return nil
		}
		return fmt.Errorf("failed to get namespace EFS for cleanup check: %w", err)
	}

	// Check if there are any remaining Access Points for this filesystem
	accessPoints, err := np.cloud.ListAccessPoints(ctx, filesystem.FileSystemId)
	if err != nil {
		return fmt.Errorf("failed to list remaining Access Points for filesystem %s: %w", filesystem.FileSystemId, err)
	}

	// Filter out the Access Point we just deleted (in case of eventual consistency)
	var remainingAccessPoints []*cloud.AccessPoint
	for _, ap := range accessPoints {
		if ap.AccessPointId != deletedAccessPointId {
			remainingAccessPoints = append(remainingAccessPoints, ap)
		}
	}

	klog.V(4).Infof("Found %d remaining Access Points for namespace %s (filesystem %s)",
		len(remainingAccessPoints), namespace, filesystem.FileSystemId)

	// If no more Access Points remain, handle EFS cleanup according to policy
	if len(remainingAccessPoints) == 0 {
		klog.V(2).Infof("No remaining Access Points for namespace %s - considering EFS cleanup", namespace)

		// For now, we implement a "retain" policy by default
		// In a full implementation, this would check the cleanup policy from storage class parameters
		cleanupPolicy := "retain" // This should come from storage class parameters

		if cleanupPolicy == "delete" {
			klog.V(2).Infof("Cleanup policy is 'delete' - would delete EFS %s for namespace %s",
				filesystem.FileSystemId, namespace)
			// TODO: Implement EFS deletion logic
			// This would involve:
			// 1. Deleting all mount targets
			// 2. Waiting for mount targets to be deleted
			// 3. Deleting the EFS filesystem
			// 4. Removing the namespace mapping
			// 5. Clearing cache
		} else {
			klog.V(2).Infof("Cleanup policy is 'retain' - preserving EFS %s for namespace %s",
				filesystem.FileSystemId, namespace)
		}

		// Remove from cache regardless of cleanup policy
		np.removeCachedEFS(namespace)

		// Update metrics
		np.updateStatus(func(status *ProvisionerStatus) {
			if status.ActiveNamespaces > 0 {
				status.ActiveNamespaces--
			}
		})
	}

	return nil
}

// CreateAccessPointForPVC creates an Access Point for the given PVC within the namespace EFS
func (np *NamespaceProvisioner) CreateAccessPointForPVC(ctx context.Context, pvcName, namespace string, options *cloud.AccessPointOptions) (*cloud.AccessPoint, error) {
	startTime := time.Now()

	// Validate input parameters
	if pvcName == "" {
		return nil, fmt.Errorf("PVC name cannot be empty")
	}
	if namespace == "" {
		return nil, fmt.Errorf("namespace cannot be empty")
	}
	if options == nil {
		return nil, fmt.Errorf("access point options cannot be nil")
	}
	if options.FileSystemId == "" {
		return nil, fmt.Errorf("FileSystemId must be specified in access point options")
	}

	klog.V(2).Infof("Creating Access Point for PVC %s in namespace %s with FileSystem %s",
		pvcName, namespace, options.FileSystemId)

	// Acquire lock to prevent concurrent access point creation for the same PVC
	lockKey := fmt.Sprintf("accesspoint:%s:%s", namespace, pvcName)
	if !np.lockManager.lockMutex(lockKey, np.options.CreateTimeout) {
		err := fmt.Errorf("failed to acquire lock for PVC %s in namespace %s within timeout", pvcName, namespace)
		np.metricsCollector.RecordError("acquire_accesspoint_lock", namespace, err)
		return nil, err
	}
	defer func() {
		np.lockManager.unlockMutex(lockKey)
	}()

	// Check if Access Point should be reused (if reuseAccessPoint is enabled)
	if np.shouldReuseAccessPoint(options) {
		existing, err := np.findExistingAccessPoint(ctx, pvcName, namespace, options.FileSystemId)
		if err == nil && existing != nil {
			klog.V(2).Infof("Reusing existing Access Point %s for PVC %s", existing.AccessPointId, pvcName)
			return existing, nil
		}
		if err != nil {
			klog.V(4).Infof("Failed to find existing Access Point for reuse: %v", err)
		}
	}

	// Build Access Point path
	accessPointPath, err := np.buildAccessPointPath(pvcName, namespace, options)
	if err != nil {
		np.metricsCollector.RecordError("build_accesspoint_path", namespace, err)
		return nil, fmt.Errorf("failed to build access point path: %w", err)
	}

	// Set POSIX user and group IDs
	uid, gid, err := np.determinePosixIDs(options)
	if err != nil {
		np.metricsCollector.RecordError("determine_posix_ids", namespace, err)
		return nil, fmt.Errorf("failed to determine POSIX IDs: %w", err)
	}

	// Set directory permissions
	directoryPerms := options.DirectoryPerms
	if directoryPerms == "" {
		directoryPerms = DefaultDirectoryPerms
	}

	// Build tags for the Access Point
	tags := np.buildAccessPointTags(pvcName, namespace, options.Tags)

	// Generate client token for idempotency
	clientToken := fmt.Sprintf("ap-%s-%s-%d", namespace, pvcName, time.Now().UnixNano())

	// Create the cloud Access Point options
	cloudOptions := &cloud.AccessPointOptions{
		FileSystemId:   options.FileSystemId,
		DirectoryPath:  accessPointPath,
		DirectoryPerms: directoryPerms,
		Uid:            uid,
		Gid:            gid,
		Tags:           tags,
		CapacityGiB:    options.CapacityGiB, // Pass through for testing purposes
	}

	klog.V(2).Infof("Creating Access Point with path %s, uid=%d, gid=%d, perms=%s",
		accessPointPath, uid, gid, directoryPerms)

	// Create the Access Point via cloud provider
	accessPoint, err := np.cloud.CreateAccessPoint(ctx, clientToken, cloudOptions)
	if err != nil {
		np.metricsCollector.RecordError("create_accesspoint", namespace, err)
		return nil, fmt.Errorf("failed to create Access Point for PVC %s: %w", pvcName, err)
	}

	// Record metrics
	np.metricsCollector.RecordEFSCreationTime(namespace, time.Since(startTime))
	np.metricsCollector.IncAccessPointCreated(namespace)

	klog.V(2).Infof("Successfully created Access Point %s for PVC %s in namespace %s in %v",
		accessPoint.AccessPointId, pvcName, namespace, time.Since(startTime))

	return accessPoint, nil
}

// DeleteAccessPointForPVC deletes the Access Point for the given PVC
func (np *NamespaceProvisioner) DeleteAccessPointForPVC(ctx context.Context, pvcName, namespace string) error {
	if pvcName == "" {
		return fmt.Errorf("PVC name cannot be empty")
	}
	if namespace == "" {
		return fmt.Errorf("namespace cannot be empty")
	}

	klog.V(2).Infof("Deleting Access Point for PVC %s in namespace %s", pvcName, namespace)

	// Get the namespace EFS to search for the Access Point
	fs, err := np.GetNamespaceEFS(ctx, namespace)
	if err != nil {
		klog.Warningf("Failed to get namespace EFS for %s: %v", namespace, err)
		// Don't fail the operation if we can't find the EFS - the Access Point might already be deleted
		return nil
	}

	// Find the Access Point by tags
	accessPoint, err := np.findExistingAccessPoint(ctx, pvcName, namespace, fs.FileSystemId)
	if err != nil {
		if err == cloud.ErrNotFound {
			klog.V(2).Infof("Access Point for PVC %s in namespace %s not found - assuming already deleted", pvcName, namespace)
			return nil
		}
		return fmt.Errorf("failed to find Access Point for PVC %s: %w", pvcName, err)
	}

	if accessPoint == nil {
		klog.V(2).Infof("No Access Point found for PVC %s in namespace %s", pvcName, namespace)
		return nil
	}

	// Acquire lock to prevent concurrent operations on the same Access Point
	lockKey := fmt.Sprintf("accesspoint:%s:%s", namespace, pvcName)
	if !np.lockManager.lockMutex(lockKey, np.options.DeleteTimeout) {
		err := fmt.Errorf("failed to acquire lock for deleting Access Point of PVC %s in namespace %s within timeout", pvcName, namespace)
		np.metricsCollector.RecordError("acquire_delete_accesspoint_lock", namespace, err)
		return err
	}
	defer func() {
		np.lockManager.unlockMutex(lockKey)
	}()

	// Delete the Access Point
	if err := np.cloud.DeleteAccessPoint(ctx, accessPoint.AccessPointId); err != nil {
		if err == cloud.ErrNotFound {
			klog.V(2).Infof("Access Point %s for PVC %s already deleted", accessPoint.AccessPointId, pvcName)
			return nil
		}
		np.metricsCollector.RecordError("delete_accesspoint", namespace, err)
		return fmt.Errorf("failed to delete Access Point %s for PVC %s: %w", accessPoint.AccessPointId, pvcName, err)
	}

	// Record metrics
	np.metricsCollector.IncAccessPointDeleted(namespace)

	klog.V(2).Infof("Successfully deleted Access Point %s for PVC %s in namespace %s",
		accessPoint.AccessPointId, pvcName, namespace)

	return nil
}

// buildAccessPointPath constructs the directory path for an Access Point
func (np *NamespaceProvisioner) buildAccessPointPath(pvcName, namespace string, options *cloud.AccessPointOptions) (string, error) {
	// Use base path from options or default
	basePath := DefaultBasePath
	if options.DirectoryPath != "" {
		// If DirectoryPath is already set in options, use it directly
		basePath = options.DirectoryPath
	}

	// Apply subPath pattern if specified
	// For now, we'll use PVC name as subPath pattern
	subPath := pvcName

	// Ensure unique directory if requested
	// UUID ensures uniqueness across the entire cluster
	uniqueId := uuid.New().String()

	// Build the full path
	var fullPath string
	if strings.Contains(subPath, "${.PV.name}") || strings.Contains(subPath, "${.PVC.name}") {
		// Handle template patterns
		subPath = strings.ReplaceAll(subPath, "${.PVC.name}", pvcName)
		subPath = strings.ReplaceAll(subPath, "${.PV.name}", pvcName) // For compatibility
		fullPath = filepath.Join(basePath, subPath, uniqueId)
	} else {
		// Simple path construction
		fullPath = filepath.Join(basePath, namespace, subPath, uniqueId)
	}

	// Ensure path starts with / and is clean
	fullPath = filepath.Clean("/" + strings.TrimPrefix(fullPath, "/"))

	klog.V(4).Infof("Built Access Point path: %s for PVC %s in namespace %s", fullPath, pvcName, namespace)
	return fullPath, nil
}

// determinePosixIDs determines the UID and GID to use for the Access Point
func (np *NamespaceProvisioner) determinePosixIDs(options *cloud.AccessPointOptions) (uid, gid int64, err error) {
	// Use specified UID if provided
	if options.Uid > 0 {
		uid = options.Uid
	} else {
		uid = DefaultUid
	}

	// Use specified GID if provided
	if options.Gid > 0 {
		gid = options.Gid
	} else {
		// If GID range is specified, pick a random GID within the range
		gidRangeStart := int64(DefaultGidRangeStart)
		gidRangeEnd := int64(DefaultGidRangeEnd)

		// Generate a random GID within the range for multi-tenancy
		if gidRangeEnd > gidRangeStart {
			gid = gidRangeStart + rand.Int63n(gidRangeEnd-gidRangeStart)
		} else {
			gid = DefaultGid
		}
	}

	// Validate IDs
	if uid <= 0 || gid <= 0 {
		return 0, 0, fmt.Errorf("invalid POSIX IDs: uid=%d, gid=%d (must be positive)", uid, gid)
	}

	return uid, gid, nil
}

// buildAccessPointTags builds the tags map for an Access Point
func (np *NamespaceProvisioner) buildAccessPointTags(pvcName, namespace string, additionalTags map[string]string) map[string]string {
	tags := make(map[string]string)

	// Add default tags from provisioner options
	for k, v := range np.options.DefaultTags {
		tags[k] = v
	}

	// Add additional tags passed in options
	for k, v := range additionalTags {
		tags[k] = v
	}

	// Add required namespace provisioning tags
	tags["kubernetes.io/namespace"] = namespace
	tags["kubernetes.io/pvc-name"] = pvcName
	tags["kubernetes.io/provisioning-mode"] = "efs-ns"
	tags["kubernetes.io/created-by"] = "efs-ns-provisioner"

	if np.options.ClusterID != "" {
		tags[fmt.Sprintf("kubernetes.io/cluster/%s", np.options.ClusterID)] = "owned"
	}

	return tags
}

// shouldReuseAccessPoint determines if Access Point reuse is enabled
func (np *NamespaceProvisioner) shouldReuseAccessPoint(options *cloud.AccessPointOptions) bool {
	// This could be controlled by a parameter in the future
	// For now, return false to always create new Access Points
	return false
}

// findExistingAccessPoint searches for an existing Access Point by PVC name and namespace
func (np *NamespaceProvisioner) findExistingAccessPoint(ctx context.Context, pvcName, namespace, fileSystemId string) (*cloud.AccessPoint, error) {
	// List all Access Points for the filesystem
	accessPoints, err := np.cloud.ListAccessPoints(ctx, fileSystemId)
	if err != nil {
		return nil, fmt.Errorf("failed to list Access Points for filesystem %s: %w", fileSystemId, err)
	}

	// Search for Access Point with matching tags
	for _, ap := range accessPoints {
		// We would need to get the Access Point details to check tags
		// For now, we'll use the DescribeAccessPoint method if available
		detailedAP, err := np.cloud.DescribeAccessPoint(ctx, ap.AccessPointId)
		if err != nil {
			klog.V(4).Infof("Failed to describe Access Point %s: %v", ap.AccessPointId, err)
			continue
		}

		// Check if this Access Point matches our PVC
		// Since the cloud.AccessPoint struct doesn't include tags directly,
		// we would need to extend the interface to get tags, or use a different approach
		// For now, we'll return nil to indicate no existing Access Point found
		_ = detailedAP
	}

	return nil, cloud.ErrNotFound
}

// CreateNamespaceVolume implements the namespace-level EFS volume creation logic
// This method is called from controller.go when provisioningMode is "efs-ns"
func (np *NamespaceProvisioner) CreateNamespaceVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	klog.V(4).Infof("CreateNamespaceVolume: called with request: %+v", req)

	// Validate input parameters
	if req == nil {
		return nil, fmt.Errorf("CreateVolumeRequest cannot be nil")
	}

	volName := req.GetName()
	if volName == "" {
		return nil, fmt.Errorf("volume name cannot be empty")
	}

	volumeParams := req.GetParameters()
	if volumeParams == nil {
		return nil, fmt.Errorf("volume parameters cannot be nil")
	}

	// Extract namespace from PVC context
	namespace, err := np.extractNamespaceFromRequest(req)
	if err != nil {
		return nil, fmt.Errorf("failed to extract namespace: %w", err)
	}

	// Parse EFS options from volume parameters
	efsOptions, err := np.parseEFSOptionsFromParams(volumeParams)
	if err != nil {
		return nil, fmt.Errorf("failed to parse EFS options: %w", err)
	}

	// Parse Access Point options from volume parameters
	apOptions, err := np.parseAccessPointOptionsFromParams(volumeParams, namespace, volName)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Access Point options: %w", err)
	}

	// Ensure namespace EFS exists (create if necessary)
	fileSystem, err := np.ensureNamespaceEFS(ctx, namespace, efsOptions)
	if err != nil {
		np.metricsCollector.RecordError("create_namespace_efs", namespace, err)
		return nil, fmt.Errorf("failed to ensure namespace EFS: %w", err)
	}

	// Extract PVC name for Access Point creation
	pvcName, err := np.extractPVCNameFromRequest(req)
	if err != nil {
		return nil, fmt.Errorf("failed to extract PVC name: %w", err)
	}

	// Update Access Point options with the correct filesystem ID
	apOptions.FileSystemId = fileSystem.FileSystemId

	// Create Access Point for this PVC
	accessPoint, err := np.CreateAccessPointForPVC(ctx, pvcName, namespace, apOptions)
	if err != nil {
		np.metricsCollector.RecordError("create_access_point", namespace, err)
		return nil, fmt.Errorf("failed to create Access Point for PVC %s: %w", pvcName, err)
	}

	// Get volume size from request
	volSize := req.GetCapacityRange().GetRequiredBytes()

	// Create the volume response
	volumeId := accessPoint.AccessPointId
	volumeContext := map[string]string{
		"accesspoint": accessPoint.AccessPointId,
		"filesystem":  fileSystem.FileSystemId,
		"namespace":   namespace,
		"pvcName":     pvcName,
	}

	// Add any additional context from the original request
	if req.GetParameters() != nil {
		for key, value := range req.GetParameters() {
			// Only add non-sensitive parameters to volume context
			if !np.isSensitiveParameter(key) {
				volumeContext[key] = value
			}
		}
	}

	response := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeId,
			CapacityBytes: volSize,
			VolumeContext: volumeContext,
		},
	}

	klog.V(2).Infof("Successfully created namespace volume %s for PVC %s in namespace %s (EFS: %s, AP: %s)",
		volName, pvcName, namespace, fileSystem.FileSystemId, accessPoint.AccessPointId)

	return response, nil
}

// extractNamespaceFromRequest extracts the namespace from the CreateVolumeRequest
func (np *NamespaceProvisioner) extractNamespaceFromRequest(req *csi.CreateVolumeRequest) (string, error) {
	// First try to get namespace from volume parameters
	if namespace, ok := req.GetParameters()["csi.storage.k8s.io/pvc/namespace"]; ok && namespace != "" {
		return namespace, nil
	}

	// Try alternative parameter names
	if namespace, ok := req.GetParameters()["namespace"]; ok && namespace != "" {
		return namespace, nil
	}

	return "", fmt.Errorf("namespace not found in volume parameters")
}

// extractPVCNameFromRequest extracts the PVC name from the CreateVolumeRequest
func (np *NamespaceProvisioner) extractPVCNameFromRequest(req *csi.CreateVolumeRequest) (string, error) {
	// First try to get PVC name from volume parameters
	if pvcName, ok := req.GetParameters()["csi.storage.k8s.io/pvc/name"]; ok && pvcName != "" {
		return pvcName, nil
	}

	// Try alternative parameter names
	if pvcName, ok := req.GetParameters()["pvcName"]; ok && pvcName != "" {
		return pvcName, nil
	}

	// Fallback to volume name if no PVC name is found
	volName := req.GetName()
	if volName != "" {
		klog.V(4).Infof("Using volume name %s as PVC name fallback", volName)
		return volName, nil
	}

	return "", fmt.Errorf("PVC name not found in volume parameters")
}

// parseEFSOptionsFromParams parses EFS creation options from volume parameters
func (np *NamespaceProvisioner) parseEFSOptionsFromParams(params map[string]string) (*EFSOptions, error) {
	options := &EFSOptions{
		PerformanceMode:  "generalPurpose", // Default
		ThroughputMode:   "bursting",       // Default
		Encrypted:        true,             // Default to encrypted
		Tags:             make(map[string]string),
	}

	// Parse optional parameters
	if performanceMode, ok := params["performanceMode"]; ok {
		options.PerformanceMode = performanceMode
	}

	if throughputMode, ok := params["throughputMode"]; ok {
		options.ThroughputMode = throughputMode
	}

	if provisionedThroughput, ok := params["provisionedThroughputInMibps"]; ok {
		if throughput, err := strconv.ParseInt(provisionedThroughput, 10, 64); err == nil {
			options.ProvisionedThroughputInMibps = throughput
		}
	}

	if encrypted, ok := params["encrypted"]; ok {
		if enc, err := strconv.ParseBool(encrypted); err == nil {
			options.Encrypted = enc
		}
	}

	if kmsKeyId, ok := params["kmsKeyId"]; ok {
		options.KmsKeyId = kmsKeyId
	}

	if lifecyclePolicy, ok := params["lifecyclePolicy"]; ok {
		options.LifecyclePolicy = lifecyclePolicy
	}

	if backupPolicy, ok := params["backupPolicy"]; ok {
		options.BackupPolicy = backupPolicy
	}

	// Add default tags
	if np.options != nil && np.options.DefaultTags != nil {
		for k, v := range np.options.DefaultTags {
			options.Tags[k] = v
		}
	}

	return options, nil
}

// parseAccessPointOptionsFromParams parses Access Point creation options from volume parameters
func (np *NamespaceProvisioner) parseAccessPointOptionsFromParams(params map[string]string, namespace, volName string) (*cloud.AccessPointOptions, error) {
	options := &cloud.AccessPointOptions{
		DirectoryPath:  DefaultBasePath,
		DirectoryPerms: DefaultDirectoryPerms,
		Uid:            DefaultUid,
		Gid:            DefaultGid,
		Tags:           make(map[string]string),
	}

	// Parse optional parameters
	if basePath, ok := params["basePath"]; ok {
		options.DirectoryPath = basePath
	}

	if directoryPerms, ok := params["directoryPerms"]; ok {
		options.DirectoryPerms = directoryPerms
	}

	if uid, ok := params["uid"]; ok {
		if uidVal, err := strconv.ParseInt(uid, 10, 64); err == nil {
			options.Uid = uidVal
		}
	}

	if gid, ok := params["gid"]; ok {
		if gidVal, err := strconv.ParseInt(gid, 10, 64); err == nil {
			options.Gid = gidVal
		}
	}

	// Construct the full path: basePath/namespace/volName/uuid
	if ensureUnique, ok := params["ensureUniqueDirectory"]; ok {
		if unique, err := strconv.ParseBool(ensureUnique); err == nil && unique {
			options.DirectoryPath = filepath.Join(options.DirectoryPath, namespace, volName, uuid.New().String())
		} else {
			options.DirectoryPath = filepath.Join(options.DirectoryPath, namespace, volName)
		}
	} else {
		// Default to unique directories
		options.DirectoryPath = filepath.Join(options.DirectoryPath, namespace, volName, uuid.New().String())
	}

	// Add tags for identification
	options.Tags["kubernetes.io/namespace"] = namespace
	options.Tags["kubernetes.io/pvc"] = volName
	options.Tags["provisioning-mode"] = "efs-ns"

	// Add cluster ID if available
	if np.options != nil && np.options.ClusterID != "" {
		options.Tags[fmt.Sprintf("kubernetes.io/cluster/%s", np.options.ClusterID)] = "owned"
	}

	return options, nil
}

// ensureNamespaceEFS ensures that an EFS filesystem exists for the given namespace
func (np *NamespaceProvisioner) ensureNamespaceEFS(ctx context.Context, namespace string, options *EFSOptions) (*cloud.FileSystem, error) {
	// First try to get existing EFS for the namespace
	existing, err := np.GetNamespaceEFS(ctx, namespace)
	if err == nil && existing != nil {
		klog.V(4).Infof("Found existing EFS %s for namespace %s", existing.FileSystemId, namespace)
		return existing, nil
	}

	// If not found, create a new EFS filesystem
	klog.V(2).Infof("Creating new EFS filesystem for namespace %s", namespace)
	return np.CreateNamespaceEFS(ctx, namespace, options)
}

// isSensitiveParameter checks if a parameter should be excluded from volume context
func (np *NamespaceProvisioner) isSensitiveParameter(key string) bool {
	sensitiveParams := []string{
		"kmsKeyId",
		"awsRoleArn",
		"csi.storage.k8s.io/provisioner-secret-name",
		"csi.storage.k8s.io/provisioner-secret-namespace",
	}

	for _, param := range sensitiveParams {
		if key == param {
			return true
		}
	}

	return false
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