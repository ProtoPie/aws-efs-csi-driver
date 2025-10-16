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
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"

	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/cloud"
	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/retry"
)

const (
	// NamespaceProvisioningMode is the provisioning mode for namespace-level EFS
	NamespaceProvisioningMode = "efs-ns"

	// EFS CSI Driver finalizer for namespace cleanup
	EFSNamespaceFinalizer = "efs.csi.aws.com/namespace-cleanup"
	// EFS CSI Driver finalizer for PV cleanup - ensures access point deletion
	EFSPVFinalizer = "efs.csi.aws.com/pv-cleanup"

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

	// Volume status tracking constants
	StatusCheckInterval     = 5 * time.Second
	StatusEventSource       = "aws-efs-csi-driver"
	EventReasonProvisioning = "Provisioning"
	EventReasonProvisioned  = "Provisioned"
	EventReasonProvisioningFailed = "ProvisioningFailed"
	EventReasonDeleting     = "Deleting"
	EventReasonDeleted      = "Deleted"
	EventReasonDeletionFailed = "DeletionFailed"
	EventReasonEFSCreating  = "EFSCreating"
	EventReasonEFSCreated   = "EFSCreated"
	EventReasonEFSReused    = "EFSReused"
	EventReasonAccessPointCreating = "AccessPointCreating"
	EventReasonAccessPointCreated  = "AccessPointCreated"
	EventReasonMountTargetsCreating = "MountTargetsCreating"
	EventReasonMountTargetsCreated  = "MountTargetsCreated"
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

// VolumeStatus represents the current status of a volume being provisioned
type VolumeStatus struct {
	VolumeID            string
	PVCName             string
	Namespace           string
	Phase               VolumePhase
	Message             string
	Reason              string
	StartTime           time.Time
	LastUpdateTime      time.Time
	EFSFileSystemID     string
	AccessPointID       string
	Error               error
	RetryCount          int
	ProgressPercentage  int
	Operations          []VolumeOperation
}

// VolumePhase represents the phase of volume provisioning
type VolumePhase string

const (
	VolumePhaseInitializing     VolumePhase = "Initializing"
	VolumePhaseEFSCreating      VolumePhase = "EFSCreating"
	VolumePhaseEFSCreated       VolumePhase = "EFSCreated"
	VolumePhaseEFSReused        VolumePhase = "EFSReused"
	VolumePhaseMountTargets     VolumePhase = "MountTargetsCreating"
	VolumePhaseAccessPoint      VolumePhase = "AccessPointCreating"
	VolumePhaseCompleted        VolumePhase = "Completed"
	VolumePhaseFailed          VolumePhase = "Failed"
	VolumePhaseDeleting        VolumePhase = "Deleting"
	VolumePhaseDeleted         VolumePhase = "Deleted"
)

// VolumeOperation represents an operation performed during volume provisioning
type VolumeOperation struct {
	Operation   string
	StartTime   time.Time
	EndTime     *time.Time
	Status      string
	Message     string
	Error       error
}

// VolumeStatusTracker manages the status of volume operations
type VolumeStatusTracker struct {
	statuses     map[string]*VolumeStatus
	statusLock   sync.RWMutex
	eventRecorder record.EventRecorder
	k8sClient    kubernetes.Interface
}

// VolumeProgressEvent represents a progress event during volume provisioning
type VolumeProgressEvent struct {
	VolumeID    string
	PVCName     string
	Namespace   string
	EventType   string
	Reason      string
	Message     string
	Progress    int
	Timestamp   time.Time
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
	accountID  string
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

	// Volume status tracking
	statusTracker *VolumeStatusTracker

	// Lifecycle management
	started    bool
	stopCh     chan struct{}
	wg         sync.WaitGroup

	// Namespace deletion monitoring
	nsWatcher    watch.Interface
	nsStopCh     chan struct{}
	startMutex sync.Mutex

	// PV deletion monitoring
	pvWatcher    watch.Interface
	pvStopCh     chan struct{}

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

	// Initialize volume status tracker
	statusTracker := NewVolumeStatusTracker(nil, k8sClient) // EventRecorder will be set later

	// Extract account ID from metadata or environment
	accountID := ""
	if roleArn := os.Getenv("AWS_ROLE_ARN"); roleArn != "" {
		// Extract account ID from role ARN: arn:aws:iam::ACCOUNT_ID:role/...
		parts := strings.Split(roleArn, ":")
		if len(parts) >= 5 {
			accountID = parts[4]
		}
	}
	if accountID == "" {
		// Default to the account ID we know is being used
		accountID = "310455165573"
		klog.Warningf("Could not determine AWS account ID from environment, using default: %s", accountID)
	}

	provisioner := &NamespaceProvisioner{
		cloud:       cloud,
		accountID:   accountID,
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
		statusTracker:    statusTracker,
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

	// Start namespace watcher for automatic EFS cleanup
	if err := np.startNamespaceWatcher(ctx); err != nil {
		klog.Errorf("Failed to start namespace watcher: %v", err)
		// Don't fail the entire start process, but retry in background
		go func() {
			retryInterval := 30 * time.Second
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(retryInterval):
					klog.V(2).Info("Retrying namespace watcher start...")
					if err := np.startNamespaceWatcher(ctx); err == nil {
						klog.V(2).Info("Successfully started namespace watcher after retry")
						return
					}
					klog.Errorf("Failed to restart namespace watcher: %v", err)
				}
			}
		}()
	}

	// Start PV watcher for automatic Access Point cleanup
	if err := np.startPVWatcher(ctx); err != nil {
		klog.Errorf("Failed to start PV watcher: %v", err)
		// Don't fail the entire start process, but log the error
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

	// Stop namespace watcher
	if np.nsStopCh != nil {
		close(np.nsStopCh)
	}
	if np.nsWatcher != nil {
		np.nsWatcher.Stop()
	}

	// Stop PV watcher
	if np.pvStopCh != nil {
		close(np.pvStopCh)
	}
	if np.pvWatcher != nil {
		np.pvWatcher.Stop()
	}

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

// addNamespaceFinalizer adds the EFS finalizer to a namespace
func (np *NamespaceProvisioner) addNamespaceFinalizer(ctx context.Context, namespace string) error {
	ns, err := np.k8sClient.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get namespace %s: %w", namespace, err)
	}

	// Check if finalizer already exists
	for _, finalizer := range ns.Finalizers {
		if finalizer == EFSNamespaceFinalizer {
			return nil // Already has the finalizer
		}
	}

	// Add the finalizer
	ns.Finalizers = append(ns.Finalizers, EFSNamespaceFinalizer)

	_, err = np.k8sClient.CoreV1().Namespaces().Update(ctx, ns, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to add finalizer to namespace %s: %w", namespace, err)
	}

	klog.V(2).Infof("Added EFS finalizer to namespace %s", namespace)
	return nil
}

// removeNamespaceFinalizer removes the EFS finalizer from a namespace
func (np *NamespaceProvisioner) removeNamespaceFinalizer(ctx context.Context, namespace string) error {
	ns, err := np.k8sClient.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get namespace %s: %w", namespace, err)
	}

	// Remove the finalizer
	var updatedFinalizers []string
	for _, finalizer := range ns.Finalizers {
		if finalizer != EFSNamespaceFinalizer {
			updatedFinalizers = append(updatedFinalizers, finalizer)
		}
	}

	ns.Finalizers = updatedFinalizers

	_, err = np.k8sClient.CoreV1().Namespaces().Update(ctx, ns, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to remove finalizer from namespace %s: %w", namespace, err)
	}

	klog.V(2).Infof("Removed EFS finalizer from namespace %s", namespace)
	return nil
}

// startNamespaceWatcher starts watching for namespace deletion events
func (np *NamespaceProvisioner) startNamespaceWatcher(ctx context.Context) error {
	klog.V(2).Info("Starting namespace watcher for EFS cleanup")

	// Create a watch for namespace events
	watchOptions := metav1.ListOptions{
		Watch: true,
	}

	watcher, err := np.k8sClient.CoreV1().Namespaces().Watch(ctx, watchOptions)
	if err != nil {
		return fmt.Errorf("failed to create namespace watcher: %w", err)
	}

	np.nsWatcher = watcher
	np.nsStopCh = make(chan struct{})

	// Start the watch loop in a goroutine
	np.wg.Add(1)
	go func() {
		defer np.wg.Done()
		defer watcher.Stop()

		for {
			select {
			case event, ok := <-watcher.ResultChan():
				if !ok {
					klog.V(4).Info("Namespace watcher channel closed, restarting...")
					// Try to restart the watcher
					if err := np.restartNamespaceWatcher(ctx); err != nil {
						klog.Errorf("Failed to restart namespace watcher: %v", err)
					}
					return
				}

				if event.Type == watch.Modified || event.Type == watch.Deleted {
					ns, ok := event.Object.(*corev1.Namespace)
					if !ok {
						continue
					}

					// Check if namespace has our finalizer and is being deleted
					if ns.DeletionTimestamp != nil && np.hasEFSFinalizer(ns) {
						klog.V(2).Infof("Detected namespace deletion with EFS finalizer: %s", ns.Name)
						go np.handleNamespaceDeletion(ctx, ns.Name)
					}
				}

			case <-np.nsStopCh:
				klog.V(2).Info("Namespace watcher stopped")
				return

			case <-ctx.Done():
				klog.V(2).Info("Namespace watcher context done")
				return
			}
		}
	}()

	return nil
}

// restartNamespaceWatcher restarts the namespace watcher after a failure
func (np *NamespaceProvisioner) restartNamespaceWatcher(ctx context.Context) error {
	if np.nsWatcher != nil {
		np.nsWatcher.Stop()
	}

	// Wait a bit before restarting
	time.Sleep(5 * time.Second)

	return np.startNamespaceWatcher(ctx)
}

// hasEFSFinalizer checks if a namespace has the EFS finalizer
func (np *NamespaceProvisioner) hasEFSFinalizer(ns *corev1.Namespace) bool {
	for _, finalizer := range ns.Finalizers {
		if finalizer == EFSNamespaceFinalizer {
			return true
		}
	}
	return false
}

// startPVWatcher starts monitoring PersistentVolumes for deletion events
func (np *NamespaceProvisioner) startPVWatcher(ctx context.Context) error {
	klog.V(2).Infof("Starting PV watcher for EFS access point cleanup")

	// Create stop channel for this watcher
	np.pvStopCh = make(chan struct{})

	// Start watching PVs
	var err error
	np.pvWatcher, err = np.k8sClient.CoreV1().PersistentVolumes().Watch(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to start PV watcher: %w", err)
	}

	// Start the watcher goroutine
	np.wg.Add(1)
	go func() {
		defer np.wg.Done()
		defer func() {
			if np.pvWatcher != nil {
				np.pvWatcher.Stop()
			}
		}()

		for {
			select {
			case <-np.pvStopCh:
				klog.V(2).Infof("PV watcher stopping")
				return

			case event, ok := <-np.pvWatcher.ResultChan():
				if !ok {
					klog.Warningf("PV watcher channel closed, attempting to restart")
					if err := np.restartPVWatcher(ctx); err != nil {
						klog.Errorf("Failed to restart PV watcher: %v", err)
						return
					}
					continue
				}

				if event.Type == watch.Modified || event.Type == watch.Deleted {
					pv, ok := event.Object.(*corev1.PersistentVolume)
					if !ok {
						klog.Warningf("Unexpected object type in PV watch: %T", event.Object)
						continue
					}

					// Only handle PVs that are:
					// 1. Using our EFS CSI driver
					// 2. In namespace provisioning mode (efs-ns)
					// 3. Have Delete reclaim policy
					if np.isEFSNamespaceProvisionedPV(pv) {
						if pv.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimDelete {
							// Check if PV is being deleted and has our finalizer
							if pv.DeletionTimestamp != nil && np.hasEFSPVFinalizer(pv) {
								klog.V(2).Infof("Detected PV deletion with EFS finalizer: %s", pv.Name)
								go np.handlePVDeletion(ctx, pv)
							} else if event.Type == watch.Modified && pv.DeletionTimestamp == nil {
								// For newly created PVs, add finalizer if they don't have it
								if !np.hasEFSPVFinalizer(pv) {
									go np.addPVFinalizer(ctx, pv.Name)
								}
							}
						}
					}
				}
			}
		}
	}()

	return nil
}

// restartPVWatcher restarts the PV watcher after a failure
func (np *NamespaceProvisioner) restartPVWatcher(ctx context.Context) error {
	if np.pvWatcher != nil {
		np.pvWatcher.Stop()
	}

	// Wait a bit before restarting
	time.Sleep(5 * time.Second)

	return np.startPVWatcher(ctx)
}

// isEFSNamespaceProvisionedPV checks if a PV is provisioned by EFS CSI driver in namespace mode
func (np *NamespaceProvisioner) isEFSNamespaceProvisionedPV(pv *corev1.PersistentVolume) bool {
	if pv.Spec.CSI == nil {
		return false
	}

	// Check if it's our EFS CSI driver
	if pv.Spec.CSI.Driver != driverName {
		return false
	}

	// Check if it's namespace provisioning mode
	if provisioningMode, ok := pv.Spec.CSI.VolumeAttributes["provisioningMode"]; ok {
		return provisioningMode == NamespaceProvisioningMode
	}

	// Also check if the volume handle contains an access point ID
	// In efs-ns mode, volume handle format is "filesystem::accesspoint"
	volumeHandle := pv.Spec.CSI.VolumeHandle
	if strings.Contains(volumeHandle, "::") {
		parts := strings.Split(volumeHandle, "::")
		if len(parts) == 2 && strings.HasPrefix(parts[1], "fsap-") {
			return true
		}
	}
	// For backward compatibility, also check if volumeHandle is just an access point ID
	return strings.HasPrefix(volumeHandle, "fsap-") && !strings.Contains(volumeHandle, "::")
}

// hasEFSPVFinalizer checks if a PV has the EFS PV finalizer
func (np *NamespaceProvisioner) hasEFSPVFinalizer(pv *corev1.PersistentVolume) bool {
	for _, finalizer := range pv.Finalizers {
		if finalizer == EFSPVFinalizer {
			return true
		}
	}
	return false
}

// addPVFinalizer adds the EFS PV finalizer to a PersistentVolume
func (np *NamespaceProvisioner) addPVFinalizer(ctx context.Context, pvName string) error {
	pv, err := np.k8sClient.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get PV %s: %w", pvName, err)
	}

	// Check if finalizer already exists
	if np.hasEFSPVFinalizer(pv) {
		return nil // Already has the finalizer
	}

	// Add the finalizer
	pv.Finalizers = append(pv.Finalizers, EFSPVFinalizer)

	_, err = np.k8sClient.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to add finalizer to PV %s: %w", pvName, err)
	}

	klog.V(2).Infof("Added EFS PV finalizer to PV %s", pvName)
	return nil
}

// removePVFinalizer removes the EFS PV finalizer from a PersistentVolume
func (np *NamespaceProvisioner) removePVFinalizer(ctx context.Context, pvName string) error {
	pv, err := np.k8sClient.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get PV %s: %w", pvName, err)
	}

	// Remove the finalizer
	var updatedFinalizers []string
	for _, finalizer := range pv.Finalizers {
		if finalizer != EFSPVFinalizer {
			updatedFinalizers = append(updatedFinalizers, finalizer)
		}
	}

	pv.Finalizers = updatedFinalizers

	_, err = np.k8sClient.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to remove finalizer from PV %s: %w", pvName, err)
	}

	klog.V(2).Infof("Removed EFS PV finalizer from PV %s", pvName)
	return nil
}

// handlePVDeletion handles the cleanup when a PV with EFS finalizer is being deleted
func (np *NamespaceProvisioner) handlePVDeletion(ctx context.Context, pv *corev1.PersistentVolume) {
	pvName := pv.Name
	klog.V(2).Infof("Starting EFS access point cleanup for PV deletion: %s", pvName)

	// Extract the access point ID from the volume handle
	volumeHandle := pv.Spec.CSI.VolumeHandle
	if volumeHandle == "" {
		klog.Errorf("PV %s has no volume handle", pvName)
		// Remove finalizer anyway to not block PV deletion
		if err := np.removePVFinalizer(ctx, pvName); err != nil {
			klog.Errorf("Failed to remove finalizer from PV %s: %v", pvName, err)
		}
		return
	}

	// Parse the volume handle to extract access point ID
	// In efs-ns mode, format is "filesystem::accesspoint"
	var accessPointId string
	if strings.Contains(volumeHandle, "::") {
		parts := strings.Split(volumeHandle, "::")
		if len(parts) == 2 {
			accessPointId = parts[1]
		}
	} else {
		// For backward compatibility, handle cases where volumeHandle is just the access point ID
		accessPointId = volumeHandle
	}

	if accessPointId == "" || !strings.HasPrefix(accessPointId, "fsap-") {
		klog.Errorf("PV %s has invalid access point ID in volume handle: %s", pvName, volumeHandle)
		// Remove finalizer anyway to not block PV deletion
		if err := np.removePVFinalizer(ctx, pvName); err != nil {
			klog.Errorf("Failed to remove finalizer from PV %s: %v", pvName, err)
		}
		return
	}

	// Create a cleanup context with timeout
	cleanupCtx, cancel := context.WithTimeout(ctx, np.options.CleanupTimeout)
	defer cancel()

	// Delete the access point
	klog.V(2).Infof("Deleting access point %s for PV %s", accessPointId, pvName)
	if err := np.cloud.DeleteAccessPoint(cleanupCtx, accessPointId); err != nil {
		if err == cloud.ErrNotFound {
			klog.V(2).Infof("Access point %s not found - assuming already deleted", accessPointId)
		} else {
			klog.Errorf("Failed to delete access point %s for PV %s: %v", accessPointId, pvName, err)
			// Don't remove finalizer if cleanup failed - will retry on next reconciliation
			return
		}
	} else {
		klog.V(2).Infof("Successfully deleted access point %s for PV %s", accessPointId, pvName)
	}

	// Remove the finalizer to allow PV deletion to proceed
	if err := np.removePVFinalizer(cleanupCtx, pvName); err != nil {
		klog.Errorf("Failed to remove finalizer from PV %s: %v", pvName, err)
	} else {
		klog.V(2).Infof("Successfully cleaned up EFS access point and removed finalizer for PV: %s", pvName)
	}
}

// hasActivePVsInNamespace checks if there are any active PVCs in the namespace that would use EFS resources
func (np *NamespaceProvisioner) hasActivePVsInNamespace(ctx context.Context, namespace string) (bool, error) {
	// List all PVCs in the namespace (including those being deleted)
	pvcList, err := np.k8sClient.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		// If namespace is not found, it's safe to say there are no PVCs
		if apierrors.IsNotFound(err) {
			klog.V(3).Infof("Namespace %s not found when checking PVCs, assuming no active PVs", namespace)
			return false, nil
		}
		return false, fmt.Errorf("failed to list PVCs in namespace %s: %w", namespace, err)
	}

	// Check if any PVCs exist (even if they're terminating)
	for _, pvc := range pvcList.Items {
		// Check if this PVC uses our EFS storage class
		if pvc.Spec.StorageClassName != nil {
			var storageClass *storagev1.StorageClass
			storageClass, err = np.k8sClient.StorageV1().StorageClasses().Get(ctx, *pvc.Spec.StorageClassName, metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) {
					klog.V(4).Infof("StorageClass %s not found for PVC %s/%s", *pvc.Spec.StorageClassName, namespace, pvc.Name)
					continue
				}
				return false, fmt.Errorf("failed to get StorageClass %s: %w", *pvc.Spec.StorageClassName, err)
			}

			// Check if this is an EFS CSI driver storage class with namespace provisioning
			if storageClass.Provisioner == "efs.csi.aws.com" {
				if provisioningMode, exists := storageClass.Parameters["provisioningMode"]; exists && provisioningMode == "efs-ns" {
					// Don't skip terminating PVCs - they still need the EFS filesystem
					if pvc.DeletionTimestamp != nil {
						klog.V(2).Infof("Found terminating EFS namespace-provisioned PVC %s/%s, waiting for it to be fully deleted before removing EFS", namespace, pvc.Name)
					} else {
						klog.V(2).Infof("Found active EFS namespace-provisioned PVC %s/%s", namespace, pvc.Name)
					}
					return true, nil
				}
			}
		}
	}

	// Also check if any PVs are still referencing this namespace's EFS
	pvList, err := np.k8sClient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Errorf("failed to list PVs: %w", err)
	}

	// Get the EFS filesystem ID for this namespace
	filesystem, err := np.GetNamespaceEFS(ctx, namespace)
	if err != nil && err != cloud.ErrNotFound {
		return false, fmt.Errorf("failed to get EFS filesystem for namespace %s: %w", namespace, err)
	}

	if filesystem != nil {
		filesystemId := filesystem.FileSystemId

		for _, pv := range pvList.Items {
			// Check if this PV is for our namespace's EFS
			if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == "efs.csi.aws.com" {
				if fsId, exists := pv.Spec.CSI.VolumeAttributes["filesystem"]; exists && fsId == filesystemId {
					// Check if the PV is still bound or being deleted
					if pv.Status.Phase == corev1.VolumeBound || pv.Status.Phase == corev1.VolumeReleased || pv.DeletionTimestamp != nil {
						klog.V(2).Infof("Found PV %s still using EFS %s for namespace %s (phase: %s, deletion: %v)",
							pv.Name, filesystemId, namespace, pv.Status.Phase, pv.DeletionTimestamp != nil)
						return true, nil
					}
				}
			}
		}
	}

	klog.V(2).Infof("No active PVCs or PVs found for namespace %s, safe to delete EFS", namespace)
	return false, nil
}

// handleNamespaceDeletion handles the cleanup when a namespace with EFS finalizer is being deleted
func (np *NamespaceProvisioner) handleNamespaceDeletion(ctx context.Context, namespace string) {
	klog.V(2).Infof("Starting EFS cleanup check for namespace deletion: %s", namespace)

	// Create a timeout context for cleanup operations
	cleanupCtx, cancel := context.WithTimeout(context.Background(), np.options.CleanupTimeout)
	defer cancel()

	// Retry logic for handling transient failures
	retryCount := 0
	maxRetries := 3
	retryDelay := 5 * time.Second

	for retryCount < maxRetries {
		// Check if there are any PVs still claiming resources in this namespace
		hasActivePVs, err := np.hasActivePVsInNamespace(cleanupCtx, namespace)
		if err != nil {
			klog.Errorf("Failed to check active PVs for namespace %s (attempt %d/%d): %v", namespace, retryCount+1, maxRetries, err)

			// If we're in the last retry and still failing, force cleanup
			if retryCount == maxRetries-1 {
				klog.Warningf("Unable to verify PV status after %d attempts, forcing namespace cleanup for %s", maxRetries, namespace)
				// Try to check if namespace is really terminating
				ns, nsErr := np.k8sClient.CoreV1().Namespaces().Get(cleanupCtx, namespace, metav1.GetOptions{})
				if nsErr != nil && apierrors.IsNotFound(nsErr) {
					klog.V(2).Infof("Namespace %s no longer exists, skipping cleanup", namespace)
					return
				}
				if ns != nil && ns.DeletionTimestamp == nil {
					klog.V(2).Infof("Namespace %s is not being deleted, skipping cleanup", namespace)
					return
				}
				// Force cleanup since namespace is terminating but we can't verify PVs
				hasActivePVs = false
			} else {
				retryCount++
				time.Sleep(retryDelay)
				continue
			}
		}

		if hasActivePVs {
			klog.V(2).Infof("Namespace %s still has active PVs, will retry cleanup later. Finalizer will remain until all PVs are deleted.", namespace)
			// Schedule another check after some delay
			go func() {
				time.Sleep(30 * time.Second)
				np.handleNamespaceDeletion(context.Background(), namespace)
			}()
			return
		}

		klog.V(2).Infof("No active PVs found in namespace %s, proceeding with EFS cleanup", namespace)

		// Clean up EFS resources for the namespace
		if err := np.DeleteNamespaceEFS(cleanupCtx, namespace); err != nil {
			if err == cloud.ErrNotFound {
				klog.V(2).Infof("EFS not found for namespace %s, proceeding to remove finalizer", namespace)
			} else {
				klog.Errorf("Failed to delete EFS for namespace %s: %v", namespace, err)
				// Retry cleanup if not the last attempt
				if retryCount < maxRetries-1 {
					retryCount++
					time.Sleep(retryDelay)
					continue
				}
				// On final failure, still try to remove finalizer if namespace is terminating
				klog.Warningf("Failed to delete EFS after %d attempts, attempting to remove finalizer anyway for namespace %s", maxRetries, namespace)
			}
		}

		// Remove the finalizer to allow namespace deletion to proceed
		if err := np.removeNamespaceFinalizer(cleanupCtx, namespace); err != nil {
			if apierrors.IsNotFound(err) {
				klog.V(2).Infof("Namespace %s no longer exists, cleanup complete", namespace)
			} else {
				klog.Errorf("Failed to remove finalizer from namespace %s: %v", namespace, err)
				// Retry if not the last attempt
				if retryCount < maxRetries-1 {
					retryCount++
					time.Sleep(retryDelay)
					continue
				}
			}
		} else {
			klog.V(2).Infof("Successfully cleaned up EFS and removed finalizer for namespace: %s", namespace)
		}

		// Success - exit the retry loop
		break
	}
}

// CreateNamespaceEFS creates an EFS filesystem for the given namespace
func (np *NamespaceProvisioner) CreateNamespaceEFS(ctx context.Context, namespace string, options *EFSOptions) (*cloud.FileSystem, error) {
	startTime := time.Now()
	
	// Create a new context with a longer timeout for AWS operations
	awsCtx, awsCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer awsCancel()

	// Check if EFS already exists for this namespace
	existing, err := np.GetNamespaceEFS(awsCtx, namespace)
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
	existing, err = np.GetNamespaceEFS(awsCtx, namespace)
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

	// Build FileSystemOptions with namespace as Name
	fsOptions := &cloud.FileSystemOptions{
		Name: namespace, // Set filesystem Name to namespace
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

	// Create the EFS filesystem with retry logic
	strategy := retry.EFSProvisioningStrategy()
	filesystem, err := retry.DoWithResultAndName(awsCtx, strategy,
		fmt.Sprintf("create-efs-%s", namespace),
		func() (*cloud.FileSystem, error) {
			return np.cloud.CreateFileSystem(awsCtx, clientToken, fsOptions)
		})
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
	// Create ARN with actual account ID
	fileSystemArn := fmt.Sprintf("arn:aws:elasticfilesystem:%s:%s:file-system/%s", np.options.Region, np.accountID, filesystem.FileSystemId)
	if _, err := np.mapper.CreateOrUpdateMapping(awsCtx, namespace, filesystem.FileSystemId, fileSystemArn, np.options.Region); err != nil {
		klog.Warningf("Failed to store namespace mapping for %s -> %s: %v", namespace, filesystem.FileSystemId, err)
		// Don't fail the entire operation, the mapping can be recovered from tags
	}

	// Add finalizer to namespace to enable EFS cleanup on namespace deletion
	if err := np.addNamespaceFinalizer(ctx, namespace); err != nil {
		klog.Warningf("Failed to add finalizer to namespace %s: %v", namespace, err)
		// Don't fail the operation, but log the warning as this affects cleanup
	}

	// Create mount targets asynchronously to avoid blocking volume creation
	go func() {
		// Create a new background context with longer timeout for mount target creation
		mountTargetCtx, mountTargetCancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer mountTargetCancel()

		// Wait for EFS to be available before creating mount targets
		klog.V(2).Infof("Waiting for EFS filesystem %s to become available for mount target creation", filesystem.FileSystemId)
		if err := np.cloud.WaitForFileSystemAvailable(mountTargetCtx, filesystem.FileSystemId); err != nil {
			klog.Errorf("Failed to wait for EFS filesystem %s to be available: %v", filesystem.FileSystemId, err)
			return
		}

		klog.Infof("EFS filesystem %s is available, initiating mount target creation", filesystem.FileSystemId)
		if err := np.createMountTargetsForEFS(mountTargetCtx, filesystem.FileSystemId, namespace); err != nil {
			klog.Errorf("Failed to create mount targets for EFS %s in namespace %s: %v", filesystem.FileSystemId, namespace, err)
		} else {
			klog.Infof("Successfully initiated mount target creation for EFS %s", filesystem.FileSystemId)
		}
	}()

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
	fileSystemArn := fmt.Sprintf("arn:aws:elasticfilesystem:%s:%s:file-system/%s", np.options.Region, np.accountID, filesystem.FileSystemId)
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
	klog.Infof("Creating mount targets for EFS %s in namespace %s", fileSystemId, namespace)

	// Check if mount targets already exist
	existingMT, err := np.cloud.DescribeMountTargets(ctx, fileSystemId, "")
	if err != nil {
		klog.Warningf("Failed to describe existing mount targets for EFS %s: %v", fileSystemId, err)
		// Continue anyway - might be cross-account issue
	} else if existingMT != nil {
		klog.Infof("Mount targets already exist for EFS %s", fileSystemId)
		return nil
	}

	// Get available subnets for mount target creation
	klog.Infof("Getting available subnets for mount target creation")
	subnets, err := np.getAvailableSubnets(ctx)
	if err != nil {
		klog.Errorf("Failed to get available subnets: %v", err)
		return fmt.Errorf("failed to get available subnets: %w", err)
	}

	klog.Infof("Found %d available subnets for mount target creation", len(subnets))
	if len(subnets) == 0 {
		return fmt.Errorf("no available subnets found for mount target creation")
	}

	// Get default security group for EFS
	klog.Infof("Getting EFS security group")
	securityGroupId, err := np.getEFSSecurityGroup(ctx)
	if err != nil {
		klog.Warningf("Failed to get EFS security group, proceeding without security group: %v", err)
		securityGroupId = "" // EFS will use the default VPC security group
	} else {
		klog.Infof("Using security group %s for mount targets", securityGroupId)
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
		klog.Infof("Creating mount target for EFS %s in subnet %s (AZ: %s)", fileSystemId, subnet.SubnetId, subnet.AvailabilityZone)

		// Create mount target with retry logic
		mountTarget, err := np.createMountTargetWithRetry(ctx, fileSystemId, subnet.SubnetId, securityGroupId)
		if err != nil {
			klog.Errorf("Failed to create mount target in subnet %s: %v", subnet.SubnetId, err)
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

// createMountTargetWithRetry creates a mount target with retry logic using the retry package
func (np *NamespaceProvisioner) createMountTargetWithRetry(ctx context.Context, fileSystemId, subnetId, securityGroupId string) (*cloud.MountTarget, error) {
	// Use AWS API strategy for mount target creation
	strategy := retry.AWSAPIStrategy()

	// Override with configured values if provided
	if np.options.MaxRetries > 0 {
		strategy.MaxAttempts = np.options.MaxRetries
	}
	if np.options.RetryDelay > 0 {
		strategy.InitialDelay = np.options.RetryDelay
	}
	if np.options.RetryBackoffMax > 0 {
		strategy.MaxDelay = np.options.RetryBackoffMax
	}

	// Custom retry condition for mount target creation
	strategy.RetryableErrors = func(err error) bool {
		// Don't retry if mount target already exists
		if err == cloud.ErrAlreadyExists {
			return false
		}
		// Use AWS-specific retry logic
		return retry.IsAWSRetryableError(err)
	}

	return retry.DoWithResultAndName(ctx, strategy,
		fmt.Sprintf("create-mount-target-%s-%s", fileSystemId, subnetId),
		func() (*cloud.MountTarget, error) {
			mountTarget, err := np.cloud.CreateMountTarget(ctx, fileSystemId, subnetId, securityGroupId)
			if err == cloud.ErrAlreadyExists {
				klog.V(4).Infof("Mount target already exists in subnet %s", subnetId)
				return nil, nil // Not an error, just skip this subnet
			}
			return mountTarget, err
		})
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
	klog.V(4).Infof("Using automatic subnet discovery from EC2")

	// Use cloud interface to automatically discover cluster subnets
	subnetIds, err := np.cloud.GetClusterSubnets(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to discover cluster subnets: %w", err)
	}

	// Convert string slice to SubnetInfo structs
	var subnets []*SubnetInfo
	for _, subnetId := range subnetIds {
		subnets = append(subnets, &SubnetInfo{
			SubnetId: subnetId,
		})
	}

	klog.V(2).Infof("Automatically discovered %d subnets from cluster", len(subnets))
	return subnets, nil
}

// getSubnetsFromInstance queries EC2 to get subnet information from the instance
func (np *NamespaceProvisioner) getSubnetsFromInstance(ctx context.Context, instanceId, region string) ([]*SubnetInfo, error) {
	// For now, we'll use environment variables or configuration to get subnet information
	// In a production environment, this would query EC2 using the AWS SDK

	// Check if we have subnet configuration from environment
	if envSubnets := os.Getenv("EFS_CSI_SUBNETS"); envSubnets != "" {
		subnets := strings.Split(envSubnets, ",")
		var subnetInfos []*SubnetInfo
		for _, subnetId := range subnets {
			subnetInfos = append(subnetInfos, &SubnetInfo{
				SubnetId: strings.TrimSpace(subnetId),
			})
		}
		klog.V(2).Infof("Using subnets from environment: %v", subnets)
		return subnetInfos, nil
	}

	// Fall back to getting subnet from instance metadata
	// This would normally use EC2 DescribeInstances API
	klog.V(4).Infof("Would query EC2 for instance %s in region %s to get subnet info", instanceId, region)

	// Return error to fall back to manual configuration
	return nil, fmt.Errorf("EC2 integration not available - please configure subnets via environment variable EFS_CSI_SUBNETS")
}

// getSecurityGroupFromInstance gets the security group from the current instance
func (np *NamespaceProvisioner) getSecurityGroupFromInstance(ctx context.Context, instanceId, region string) (string, error) {
	// Check if we have security group configuration from environment
	if envSG := os.Getenv("EFS_CSI_SECURITY_GROUP"); envSG != "" {
		klog.V(2).Infof("Using security group from environment: %s", envSG)
		return envSG, nil
	}

	// This would normally use EC2 DescribeInstances API
	klog.V(4).Infof("Would query EC2 for instance %s in region %s to get security group", instanceId, region)

	return "", fmt.Errorf("EC2 integration not available - please configure security group via environment variable EFS_CSI_SECURITY_GROUP")
}

// getEFSSecurityGroup gets the security group for EFS mount targets
func (np *NamespaceProvisioner) getEFSSecurityGroup(ctx context.Context) (string, error) {
	klog.V(4).Infof("Getting EFS security group configuration")

	// Strategy 1: Use configured security group if available
	if np.options.SecurityGroupId != "" {
		klog.V(2).Infof("Using configured security group: %s", np.options.SecurityGroupId)
		return np.options.SecurityGroupId, nil
	}

	// Strategy 2: Use automatic discovery from cloud interface
	securityGroupId, err := np.cloud.GetClusterSecurityGroup(ctx)
	if err != nil {
		klog.Warningf("Failed to discover/create security group: %v", err)
		klog.Warningf("Using default security group - manual NFS rule configuration may be required")
		return "", nil
	}

	klog.V(2).Infof("Using automatically discovered/created security group: %s", securityGroupId)
	return securityGroupId, nil
}

// DeleteNamespaceEFS deletes the EFS filesystem for the given namespace
func (np *NamespaceProvisioner) DeleteNamespaceEFS(ctx context.Context, namespace string) error {
	klog.V(2).Infof("Starting EFS deletion for namespace: %s", namespace)

	// Get EFS filesystem for this namespace
	filesystem, err := np.GetNamespaceEFS(ctx, namespace)
	if err != nil {
		if err == cloud.ErrNotFound {
			klog.V(2).Infof("EFS filesystem not found for namespace %s - assuming already deleted", namespace)
			return nil
		}
		return fmt.Errorf("failed to get EFS filesystem for namespace %s: %w", namespace, err)
	}

	if filesystem == nil {
		klog.V(2).Infof("No EFS filesystem found for namespace %s", namespace)
		return nil
	}

	fileSystemId := filesystem.FileSystemId
	klog.V(2).Infof("Found EFS filesystem %s for namespace %s, proceeding with deletion", fileSystemId, namespace)

	// First, delete all Access Points for this filesystem
	if err := np.deleteAllAccessPointsForFileSystem(ctx, fileSystemId); err != nil {
		klog.Warningf("Failed to delete some Access Points for EFS %s: %v", fileSystemId, err)
		// Continue with deletion as Access Points might be orphaned
	}

	// Delete all mount targets
	if err := np.deleteAllMountTargetsForFileSystem(ctx, fileSystemId); err != nil {
		return fmt.Errorf("failed to delete mount targets for EFS %s: %w", fileSystemId, err)
	}

	// Wait for mount targets to be deleted
	if err := np.waitForMountTargetsDeletion(ctx, fileSystemId); err != nil {
		return fmt.Errorf("timeout waiting for mount targets deletion for EFS %s: %w", fileSystemId, err)
	}

	// Finally, delete the EFS filesystem
	klog.V(2).Infof("Deleting EFS filesystem %s for namespace %s", fileSystemId, namespace)
	if err := np.cloud.DeleteFileSystem(ctx, fileSystemId); err != nil {
		return fmt.Errorf("failed to delete EFS filesystem %s: %w", fileSystemId, err)
	}

	// Remove from cache
	np.removeCachedEFS(namespace)

	// Remove mapping
	if err := np.mapper.DeleteMapping(ctx, namespace); err != nil {
		klog.Warningf("Failed to delete namespace mapping for %s: %v", namespace, err)
		// Don't fail the operation for this
	}

	// Record metrics
	np.metricsCollector.IncEFSDeleted(namespace)

	klog.V(2).Infof("Successfully deleted EFS filesystem %s for namespace %s", fileSystemId, namespace)
	return nil
}

// deleteAllAccessPointsForFileSystem deletes all Access Points for a given EFS filesystem
func (np *NamespaceProvisioner) deleteAllAccessPointsForFileSystem(ctx context.Context, fileSystemId string) error {
	klog.V(2).Infof("Deleting all Access Points for EFS filesystem: %s", fileSystemId)

	// List all Access Points for this filesystem
	accessPoints, err := np.cloud.ListAccessPoints(ctx, fileSystemId)
	if err != nil {
		return fmt.Errorf("failed to list Access Points for EFS %s: %w", fileSystemId, err)
	}

	// Delete each Access Point
	for _, ap := range accessPoints {
		klog.V(2).Infof("Deleting Access Point: %s", ap.AccessPointId)
		if err := np.cloud.DeleteAccessPoint(ctx, ap.AccessPointId); err != nil {
			klog.Warningf("Failed to delete Access Point %s: %v", ap.AccessPointId, err)
			// Continue with other Access Points
		}
	}

	// Wait for all Access Points to be deleted
	for _, ap := range accessPoints {
		if err := np.waitForAccessPointDeletion(ctx, ap.AccessPointId); err != nil {
			klog.Warningf("Timeout waiting for Access Point %s deletion: %v", ap.AccessPointId, err)
			// Continue anyway
		}
	}

	klog.V(2).Infof("Completed deletion of %d Access Points for EFS %s", len(accessPoints), fileSystemId)
	return nil
}

// deleteAllMountTargetsForFileSystem deletes all mount targets for a given EFS filesystem
func (np *NamespaceProvisioner) deleteAllMountTargetsForFileSystem(ctx context.Context, fileSystemId string) error {
	klog.V(2).Infof("Deleting all mount targets for EFS filesystem: %s", fileSystemId)

	// List all mount targets for this filesystem
	mountTargets, err := np.cloud.ListMountTargets(ctx, fileSystemId)
	if err != nil {
		return fmt.Errorf("failed to list mount targets for EFS %s: %w", fileSystemId, err)
	}

	// Delete each mount target
	for _, mt := range mountTargets {
		klog.V(2).Infof("Deleting mount target: %s", mt.MountTargetId)
		if err := np.cloud.DeleteMountTarget(ctx, mt.MountTargetId); err != nil {
			return fmt.Errorf("failed to delete mount target %s: %w", mt.MountTargetId, err)
		}
	}

	klog.V(2).Infof("Completed deletion of %d mount targets for EFS %s", len(mountTargets), fileSystemId)
	return nil
}

// waitForMountTargetsDeletion waits for all mount targets to be deleted
func (np *NamespaceProvisioner) waitForMountTargetsDeletion(ctx context.Context, fileSystemId string) error {
	klog.V(2).Infof("Waiting for mount targets deletion for EFS: %s", fileSystemId)

	timeout := 5 * time.Minute
	checkInterval := 10 * time.Second

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutCtx.Done():
			return fmt.Errorf("timeout waiting for mount targets deletion for EFS %s", fileSystemId)
		case <-ticker.C:
			mountTargets, err := np.cloud.ListMountTargets(ctx, fileSystemId)
			if err != nil {
				// Check if the filesystem itself was deleted
				if err == cloud.ErrNotFound {
					klog.V(2).Infof("EFS filesystem %s not found, assuming mount targets are deleted", fileSystemId)
					return nil
				}
				klog.Warningf("Error checking mount targets for EFS %s: %v", fileSystemId, err)
				continue
			}

			if len(mountTargets) == 0 {
				klog.V(2).Infof("All mount targets deleted for EFS: %s", fileSystemId)
				return nil
			}

			klog.V(4).Infof("Still waiting for %d mount targets to be deleted for EFS %s", len(mountTargets), fileSystemId)
		}
	}
}

// waitForAccessPointDeletion waits for a specific Access Point to be deleted
func (np *NamespaceProvisioner) waitForAccessPointDeletion(ctx context.Context, accessPointId string) error {
	timeout := 2 * time.Minute
	checkInterval := 5 * time.Second

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutCtx.Done():
			return fmt.Errorf("timeout waiting for Access Point %s deletion", accessPointId)
		case <-ticker.C:
			_, err := np.cloud.DescribeAccessPoint(ctx, accessPointId)
			if err == cloud.ErrNotFound {
				return nil // Access Point is deleted
			}
			if err != nil {
				klog.Warningf("Error checking Access Point %s: %v", accessPointId, err)
				continue
			}
			// Access Point still exists, continue waiting
		}
	}
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

	// Get Access Point details first to extract namespace and PVC information for status tracking
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

	// Start volume deletion tracking
	np.statusTracker.StartVolumeDeletion(volumeId, pvcName, namespace)

	// Helper function to handle errors with status tracking
	handleError := func(err error, message string) (*csi.DeleteVolumeResponse, error) {
		np.statusTracker.FailVolumeDeletion(volumeId, err)
		np.metricsCollector.RecordError("delete_namespace_volume", namespace, err)
		return nil, err
	}

	// Acquire lock to prevent concurrent operations on the same Access Point
	lockKey := fmt.Sprintf("accesspoint-delete:%s", accessPointId)
	if !np.lockManager.lockMutex(lockKey, np.options.DeleteTimeout) {
		err := fmt.Errorf("failed to acquire lock for deleting Access Point %s within timeout", accessPointId)
		return handleError(err, "Failed to acquire deletion lock")
	}
	defer func() {
		np.lockManager.unlockMutex(lockKey)
	}()

	klog.V(2).Infof("Deleting Access Point %s for PVC %s in namespace %s", accessPointId, pvcName, namespace)

	// Delete the Access Point
	np.statusTracker.RecordVolumeOperation(volumeId, "delete_access_point", "started", fmt.Sprintf("Deleting Access Point %s", accessPointId), nil)
	if err := np.cloud.DeleteAccessPoint(ctx, accessPointId); err != nil {
		if err == cloud.ErrNotFound {
			klog.V(2).Infof("Access Point %s not found - assuming already deleted", accessPointId)
			np.statusTracker.RecordVolumeOperation(volumeId, "delete_access_point", "completed", "Access Point already deleted", nil)
		} else {
			np.statusTracker.RecordVolumeOperation(volumeId, "delete_access_point", "failed", "Failed to delete Access Point", err)
			return handleError(fmt.Errorf("failed to delete Access Point %s: %w", accessPointId, err), "Failed to delete Access Point")
		}
	} else {
		// Record successful deletion metrics
		np.metricsCollector.IncAccessPointDeleted(namespace)
		np.statusTracker.RecordVolumeOperation(volumeId, "delete_access_point", "completed", "Access Point deleted successfully", nil)
		klog.V(2).Infof("Successfully deleted Access Point %s for PVC %s in namespace %s", accessPointId, pvcName, namespace)
	}

	// Check if this was the last Access Point in the namespace
	// and handle EFS cleanup according to cleanup policy
	if namespace != "unknown" {
		np.statusTracker.RecordVolumeOperation(volumeId, "namespace_cleanup", "started", "Checking for namespace cleanup", nil)
		if err := np.handleNamespaceCleanup(ctx, namespace, accessPointId); err != nil {
			klog.Warningf("Failed to handle namespace cleanup for %s: %v", namespace, err)
			np.statusTracker.RecordVolumeOperation(volumeId, "namespace_cleanup", "warning", "Namespace cleanup had issues", err)
			// Don't fail the entire operation for cleanup issues
		} else {
			np.statusTracker.RecordVolumeOperation(volumeId, "namespace_cleanup", "completed", "Namespace cleanup completed", nil)
		}
	}

	// Complete volume deletion tracking
	np.statusTracker.CompleteVolumeDeletion(volumeId)

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

	// Create the cloud Access Point options with PVC name as Name tag
	cloudOptions := &cloud.AccessPointOptions{
		Name:           pvcName, // Set Access Point Name to PVC name
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

	// Wait for EFS filesystem and mount targets to be available before creating Access Point
	klog.V(2).Infof("Waiting for EFS filesystem %s to be fully available before creating Access Point", options.FileSystemId)

	// Check if this is a cross-account scenario
	// In cross-account scenarios, we may not be able to describe the filesystem
	// Skip waiting if we get permission errors
	if err := np.cloud.WaitForFileSystemAvailable(ctx, options.FileSystemId); err != nil {
		// If it's a not found error, it might be cross-account
		// Check both the error type and the error message
		if errors.Is(err, cloud.ErrNotFound) || strings.Contains(err.Error(), "Resource was not found") || strings.Contains(err.Error(), "failed to describe filesystem") {
			klog.Warningf("Cannot describe filesystem %s - might be cross-account, skipping wait: %v", options.FileSystemId, err)
			// Continue without waiting - the filesystem was created successfully
		} else {
			return nil, fmt.Errorf("failed to wait for EFS filesystem %s to be available: %w", options.FileSystemId, err)
		}
	}

	// Wait for mount targets to be available
	// Skip for cross-account scenarios where we can't describe
	if err := np.cloud.WaitForMountTargetsAvailable(ctx, options.FileSystemId); err != nil {
		if errors.Is(err, cloud.ErrNotFound) || strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "failed to list mount targets") {
			klog.Warningf("Cannot describe mount targets for %s - might be cross-account, skipping wait: %v", options.FileSystemId, err)
			// Continue without waiting
		} else {
			return nil, fmt.Errorf("failed to wait for mount targets of EFS filesystem %s to be available: %w", options.FileSystemId, err)
		}
	}

	klog.V(2).Infof("EFS filesystem %s and its mount targets are fully available for Access Point creation", options.FileSystemId)

	// Create the Access Point via cloud provider with retry logic
	strategy := retry.EFSProvisioningStrategy()
	accessPoint, err := retry.DoWithResultAndName(ctx, strategy,
		fmt.Sprintf("create-access-point-%s-%s", namespace, pvcName),
		func() (*cloud.AccessPoint, error) {
			return np.cloud.CreateAccessPoint(ctx, clientToken, cloudOptions)
		})
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

	// Delete the Access Point with retry logic
	strategy := retry.AWSAPIStrategy()
	err = strategy.DoWithName(ctx,
		fmt.Sprintf("delete-access-point-%s", accessPoint.AccessPointId),
		func() error {
			deleteErr := np.cloud.DeleteAccessPoint(ctx, accessPoint.AccessPointId)
			if deleteErr == cloud.ErrNotFound {
				klog.V(2).Infof("Access Point %s for PVC %s already deleted", accessPoint.AccessPointId, pvcName)
				return nil // Not an error, already deleted
			}
			return deleteErr
		})
	if err != nil {
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
	// Simple path: /namespace/pvcname-shortid
	// AWS EFS constraints: max 100 chars, max 4 segments
	
	// Create short unique ID (8 chars)
	uniqueId := uuid.New().String()[:8]
	
	// Truncate namespace and PVC names if needed
	ns := namespace
	if len(namespace) > 30 {
		ns = namespace[:30]
	}
	
	pvc := pvcName  
	if len(pvcName) > 30 {
		pvc = pvcName[:30]
	}
	
	// Simple 2-segment path: /namespace/pvcname-id
	fullPath := fmt.Sprintf("/%s/%s-%s", ns, pvc, uniqueId)
	
	// Validate constraints
	if len(fullPath) > 100 {
		// Further truncate if needed
		maxPvcLen := 100 - len(ns) - len(uniqueId) - 3 // account for slashes and dash
		if maxPvcLen > 0 && maxPvcLen < len(pvc) {
			pvc = pvc[:maxPvcLen]
		}
		fullPath = fmt.Sprintf("/%s/%s-%s", ns, pvc, uniqueId)
	}
	
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

	// Extract PVC name for tracking and Access Point creation
	pvcName, err := np.extractPVCNameFromRequest(req)
	if err != nil {
		return nil, fmt.Errorf("failed to extract PVC name: %w", err)
	}

	// Start volume status tracking
	volumeID := volName
	np.statusTracker.StartVolumeProvisioning(volumeID, pvcName, namespace)

	// Helper function to handle errors with status tracking
	handleError := func(err error, phase VolumePhase, message string) (*csi.CreateVolumeResponse, error) {
		np.statusTracker.FailVolumeProvisioning(volumeID, err, 0)
		np.metricsCollector.RecordError("create_namespace_volume", namespace, err)
		return nil, err
	}

	// Parse EFS options from volume parameters
	np.statusTracker.UpdateVolumePhase(volumeID, VolumePhaseInitializing, "Parsing volume parameters", 10)
	efsOptions, err := np.parseEFSOptionsFromParams(volumeParams)
	if err != nil {
		return handleError(fmt.Errorf("failed to parse EFS options: %w", err), VolumePhaseFailed, "Failed to parse EFS options")
	}

	// Parse Access Point options from volume parameters
	apOptions, err := np.parseAccessPointOptionsFromParams(volumeParams, namespace, volName)
	if err != nil {
		return handleError(fmt.Errorf("failed to parse Access Point options: %w", err), VolumePhaseFailed, "Failed to parse Access Point options")
	}

	// Ensure namespace EFS exists (create if necessary)
	np.statusTracker.UpdateVolumePhase(volumeID, VolumePhaseEFSCreating, "Ensuring namespace EFS exists", 25)
	np.statusTracker.RecordVolumeOperation(volumeID, "ensure_namespace_efs", "started", "Checking for existing EFS or creating new one", nil)

	fileSystem, err := np.ensureNamespaceEFS(ctx, namespace, efsOptions)
	if err != nil {
		np.statusTracker.RecordVolumeOperation(volumeID, "ensure_namespace_efs", "failed", "Failed to ensure namespace EFS", err)
		return handleError(fmt.Errorf("failed to ensure namespace EFS: %w", err), VolumePhaseFailed, "Failed to ensure namespace EFS")
	}

	np.statusTracker.RecordVolumeOperation(volumeID, "ensure_namespace_efs", "completed", fmt.Sprintf("EFS %s ready", fileSystem.FileSystemId), nil)
	np.statusTracker.UpdateVolumePhase(volumeID, VolumePhaseEFSCreated, fmt.Sprintf("EFS %s ready", fileSystem.FileSystemId), 60)

	// Update Access Point options with the correct filesystem ID
	apOptions.FileSystemId = fileSystem.FileSystemId

	// Create Access Point for this PVC
	np.statusTracker.UpdateVolumePhase(volumeID, VolumePhaseAccessPoint, "Creating Access Point", 80)
	np.statusTracker.RecordVolumeOperation(volumeID, "create_access_point", "started", fmt.Sprintf("Creating Access Point for PVC %s", pvcName), nil)

	accessPoint, err := np.CreateAccessPointForPVC(ctx, pvcName, namespace, apOptions)
	if err != nil {
		np.statusTracker.RecordVolumeOperation(volumeID, "create_access_point", "failed", "Failed to create Access Point", err)
		return handleError(fmt.Errorf("failed to create Access Point for PVC %s: %w", pvcName, err), VolumePhaseFailed, "Failed to create Access Point")
	}

	np.statusTracker.RecordVolumeOperation(volumeID, "create_access_point", "completed", fmt.Sprintf("Access Point %s created", accessPoint.AccessPointId), nil)

	// Get volume size from request
	volSize := req.GetCapacityRange().GetRequiredBytes()

	// Create the volume response
	// Format: filesystem::accesspoint for proper parsing by node driver
	volumeId := fmt.Sprintf("%s::%s", fileSystem.FileSystemId, accessPoint.AccessPointId)
	volumeContext := map[string]string{
		"accesspoint":      accessPoint.AccessPointId,
		"filesystem":       fileSystem.FileSystemId,
		"namespace":        namespace,
		"pvcName":          pvcName,
		"provisioningMode": NamespaceProvisioningMode, // Mark as efs-ns mode for PV watcher
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

	// Complete volume provisioning tracking
	np.statusTracker.CompleteVolumeProvisioning(volumeID, fileSystem.FileSystemId, accessPoint.AccessPointId)

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

// NewVolumeStatusTracker creates a new VolumeStatusTracker
func NewVolumeStatusTracker(eventRecorder record.EventRecorder, k8sClient kubernetes.Interface) *VolumeStatusTracker {
	return &VolumeStatusTracker{
		statuses:      make(map[string]*VolumeStatus),
		eventRecorder: eventRecorder,
		k8sClient:     k8sClient,
	}
}

// StartVolumeProvisioning starts tracking a new volume provisioning operation
func (vst *VolumeStatusTracker) StartVolumeProvisioning(volumeID, pvcName, namespace string) {
	vst.statusLock.Lock()
	defer vst.statusLock.Unlock()

	status := &VolumeStatus{
		VolumeID:           volumeID,
		PVCName:            pvcName,
		Namespace:          namespace,
		Phase:              VolumePhaseInitializing,
		Message:            "Starting volume provisioning",
		Reason:             EventReasonProvisioning,
		StartTime:          time.Now(),
		LastUpdateTime:     time.Now(),
		ProgressPercentage: 0,
		Operations:         []VolumeOperation{},
	}

	vst.statuses[volumeID] = status
	vst.emitProgressEvent(status, corev1.EventTypeNormal, EventReasonProvisioning, "Starting volume provisioning")
	klog.V(2).Infof("Started volume provisioning tracking for PVC %s/%s (volume: %s)", namespace, pvcName, volumeID)
}

// UpdateVolumePhase updates the phase of a volume provisioning operation
func (vst *VolumeStatusTracker) UpdateVolumePhase(volumeID string, phase VolumePhase, message string, progressPercentage int) {
	vst.statusLock.Lock()
	defer vst.statusLock.Unlock()

	status, exists := vst.statuses[volumeID]
	if !exists {
		klog.Warningf("Attempted to update non-existent volume status: %s", volumeID)
		return
	}

	oldPhase := status.Phase
	status.Phase = phase
	status.Message = message
	status.LastUpdateTime = time.Now()
	status.ProgressPercentage = progressPercentage

	// Determine event type and reason based on phase
	eventType := corev1.EventTypeNormal
	reason := string(phase)

	switch phase {
	case VolumePhaseEFSCreating:
		reason = EventReasonEFSCreating
	case VolumePhaseEFSCreated:
		reason = EventReasonEFSCreated
	case VolumePhaseEFSReused:
		reason = EventReasonEFSReused
	case VolumePhaseMountTargets:
		reason = EventReasonMountTargetsCreating
	case VolumePhaseAccessPoint:
		reason = EventReasonAccessPointCreating
	case VolumePhaseCompleted:
		reason = EventReasonProvisioned
	case VolumePhaseFailed:
		eventType = corev1.EventTypeWarning
		reason = EventReasonProvisioningFailed
	case VolumePhaseDeleting:
		reason = EventReasonDeleting
	case VolumePhaseDeleted:
		reason = EventReasonDeleted
	}

	vst.emitProgressEvent(status, eventType, reason, message)
	klog.V(3).Infof("Updated volume %s phase: %s -> %s (progress: %d%%, message: %s)",
		volumeID, oldPhase, phase, progressPercentage, message)
}

// RecordVolumeOperation records an operation performed during volume provisioning
func (vst *VolumeStatusTracker) RecordVolumeOperation(volumeID, operation, status, message string, err error) {
	vst.statusLock.Lock()
	defer vst.statusLock.Unlock()

	volumeStatus, exists := vst.statuses[volumeID]
	if !exists {
		klog.Warningf("Attempted to record operation for non-existent volume: %s", volumeID)
		return
	}

	now := time.Now()
	op := VolumeOperation{
		Operation: operation,
		StartTime: now,
		Status:    status,
		Message:   message,
		Error:     err,
	}

	if status == "completed" || status == "failed" {
		op.EndTime = &now
	}

	volumeStatus.Operations = append(volumeStatus.Operations, op)
	klog.V(4).Infof("Recorded operation for volume %s: %s - %s (%s)", volumeID, operation, status, message)
}

// CompleteVolumeProvisioning marks a volume provisioning operation as completed
func (vst *VolumeStatusTracker) CompleteVolumeProvisioning(volumeID, efsID, accessPointID string) {
	vst.statusLock.Lock()
	defer vst.statusLock.Unlock()

	status, exists := vst.statuses[volumeID]
	if !exists {
		klog.Warningf("Attempted to complete non-existent volume provisioning: %s", volumeID)
		return
	}

	status.Phase = VolumePhaseCompleted
	status.Message = "Volume provisioning completed successfully"
	status.Reason = EventReasonProvisioned
	status.LastUpdateTime = time.Now()
	status.EFSFileSystemID = efsID
	status.AccessPointID = accessPointID
	status.ProgressPercentage = 100

	vst.emitProgressEvent(status, corev1.EventTypeNormal, EventReasonProvisioned, status.Message)
	klog.V(2).Infof("Completed volume provisioning for PVC %s/%s (volume: %s, EFS: %s, AP: %s)",
		status.Namespace, status.PVCName, volumeID, efsID, accessPointID)
}

// FailVolumeProvisioning marks a volume provisioning operation as failed
func (vst *VolumeStatusTracker) FailVolumeProvisioning(volumeID string, err error, retryCount int) {
	vst.statusLock.Lock()
	defer vst.statusLock.Unlock()

	status, exists := vst.statuses[volumeID]
	if !exists {
		klog.Warningf("Attempted to fail non-existent volume provisioning: %s", volumeID)
		return
	}

	status.Phase = VolumePhaseFailed
	status.Message = fmt.Sprintf("Volume provisioning failed: %v", err)
	status.Reason = EventReasonProvisioningFailed
	status.LastUpdateTime = time.Now()
	status.Error = err
	status.RetryCount = retryCount

	vst.emitProgressEvent(status, corev1.EventTypeWarning, EventReasonProvisioningFailed, status.Message)
	klog.Errorf("Failed volume provisioning for PVC %s/%s (volume: %s, retry: %d): %v",
		status.Namespace, status.PVCName, volumeID, retryCount, err)
}

// StartVolumeDeletion starts tracking a volume deletion operation
func (vst *VolumeStatusTracker) StartVolumeDeletion(volumeID, pvcName, namespace string) {
	vst.statusLock.Lock()
	defer vst.statusLock.Unlock()

	status := &VolumeStatus{
		VolumeID:           volumeID,
		PVCName:            pvcName,
		Namespace:          namespace,
		Phase:              VolumePhaseDeleting,
		Message:            "Starting volume deletion",
		Reason:             EventReasonDeleting,
		StartTime:          time.Now(),
		LastUpdateTime:     time.Now(),
		ProgressPercentage: 0,
		Operations:         []VolumeOperation{},
	}

	vst.statuses[volumeID] = status
	vst.emitProgressEvent(status, corev1.EventTypeNormal, EventReasonDeleting, "Starting volume deletion")
	klog.V(2).Infof("Started volume deletion tracking for PVC %s/%s (volume: %s)", namespace, pvcName, volumeID)
}

// CompleteVolumeDeletion marks a volume deletion operation as completed
func (vst *VolumeStatusTracker) CompleteVolumeDeletion(volumeID string) {
	vst.statusLock.Lock()
	defer vst.statusLock.Unlock()

	status, exists := vst.statuses[volumeID]
	if !exists {
		klog.Warningf("Attempted to complete non-existent volume deletion: %s", volumeID)
		return
	}

	status.Phase = VolumePhaseDeleted
	status.Message = "Volume deletion completed successfully"
	status.Reason = EventReasonDeleted
	status.LastUpdateTime = time.Now()
	status.ProgressPercentage = 100

	vst.emitProgressEvent(status, corev1.EventTypeNormal, EventReasonDeleted, status.Message)
	klog.V(2).Infof("Completed volume deletion for PVC %s/%s (volume: %s)",
		status.Namespace, status.PVCName, volumeID)

	// Clean up the status after successful deletion
	delete(vst.statuses, volumeID)
}

// FailVolumeDeletion marks a volume deletion operation as failed
func (vst *VolumeStatusTracker) FailVolumeDeletion(volumeID string, err error) {
	vst.statusLock.Lock()
	defer vst.statusLock.Unlock()

	status, exists := vst.statuses[volumeID]
	if !exists {
		klog.Warningf("Attempted to fail non-existent volume deletion: %s", volumeID)
		return
	}

	status.Message = fmt.Sprintf("Volume deletion failed: %v", err)
	status.Reason = EventReasonDeletionFailed
	status.LastUpdateTime = time.Now()
	status.Error = err

	vst.emitProgressEvent(status, corev1.EventTypeWarning, EventReasonDeletionFailed, status.Message)
	klog.Errorf("Failed volume deletion for PVC %s/%s (volume: %s): %v",
		status.Namespace, status.PVCName, volumeID, err)
}

// GetVolumeStatus returns the current status of a volume
func (vst *VolumeStatusTracker) GetVolumeStatus(volumeID string) (*VolumeStatus, bool) {
	vst.statusLock.RLock()
	defer vst.statusLock.RUnlock()

	status, exists := vst.statuses[volumeID]
	if !exists {
		return nil, false
	}

	// Return a copy to avoid race conditions
	statusCopy := *status
	statusCopy.Operations = make([]VolumeOperation, len(status.Operations))
	copy(statusCopy.Operations, status.Operations)

	return &statusCopy, true
}

// ListVolumeStatuses returns all tracked volume statuses
func (vst *VolumeStatusTracker) ListVolumeStatuses() map[string]*VolumeStatus {
	vst.statusLock.RLock()
	defer vst.statusLock.RUnlock()

	result := make(map[string]*VolumeStatus)
	for id, status := range vst.statuses {
		statusCopy := *status
		statusCopy.Operations = make([]VolumeOperation, len(status.Operations))
		copy(statusCopy.Operations, status.Operations)
		result[id] = &statusCopy
	}

	return result
}

// CleanupCompletedStatuses removes completed or old failed statuses
func (vst *VolumeStatusTracker) CleanupCompletedStatuses(maxAge time.Duration) {
	vst.statusLock.Lock()
	defer vst.statusLock.Unlock()

	cutoff := time.Now().Add(-maxAge)
	var removed []string

	for id, status := range vst.statuses {
		shouldRemove := false

		// Remove completed statuses that are old enough
		if status.Phase == VolumePhaseCompleted && status.LastUpdateTime.Before(cutoff) {
			shouldRemove = true
		}

		// Remove very old failed statuses
		if status.Phase == VolumePhaseFailed && status.LastUpdateTime.Before(cutoff) {
			shouldRemove = true
		}

		if shouldRemove {
			delete(vst.statuses, id)
			removed = append(removed, id)
		}
	}

	if len(removed) > 0 {
		klog.V(4).Infof("Cleaned up %d old volume statuses: %v", len(removed), removed)
	}
}

// emitProgressEvent emits a Kubernetes event for volume progress
func (vst *VolumeStatusTracker) emitProgressEvent(status *VolumeStatus, eventType, reason, message string) {
	if vst.eventRecorder == nil {
		klog.V(4).Infof("No event recorder available, skipping event: %s/%s - %s: %s",
			status.Namespace, status.PVCName, reason, message)
		return
	}

	// Try to get the PVC object to attach the event to
	pvc, err := vst.k8sClient.CoreV1().PersistentVolumeClaims(status.Namespace).Get(
		context.TODO(), status.PVCName, metav1.GetOptions{})
	if err != nil {
		klog.V(4).Infof("Could not get PVC %s/%s for event emission: %v",
			status.Namespace, status.PVCName, err)
		return
	}

	// Create enhanced message with progress information
	enhancedMessage := message
	if status.ProgressPercentage > 0 {
		enhancedMessage = fmt.Sprintf("%s (progress: %d%%)", message, status.ProgressPercentage)
	}

	// Add operation details if available
	if len(status.Operations) > 0 {
		lastOp := status.Operations[len(status.Operations)-1]
		if lastOp.Operation != "" {
			enhancedMessage = fmt.Sprintf("%s - %s", enhancedMessage, lastOp.Operation)
		}
	}

	// Emit the event
	vst.eventRecorder.Event(pvc, eventType, reason, enhancedMessage)
	klog.V(3).Infof("Emitted event for PVC %s/%s: %s - %s: %s",
		status.Namespace, status.PVCName, eventType, reason, enhancedMessage)
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