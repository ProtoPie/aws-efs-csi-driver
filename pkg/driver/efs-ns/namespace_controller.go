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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	// NamespaceControllerName is the component name for logging and metrics
	NamespaceControllerName = "efs-ns-namespace-controller"

	// DefaultResyncPeriod is the default period for namespace controller resync
	DefaultResyncPeriod = 10 * time.Minute

	// DefaultWatchTimeout is the default timeout for namespace watch operations
	DefaultWatchTimeout = 30 * time.Minute

	// CleanupTimeout is the maximum time to wait for namespace cleanup
	CleanupTimeout = 5 * time.Minute

	// RetryDelay is the delay between retry attempts for failed operations
	RetryDelay = 30 * time.Second

	// MaxRetries is the maximum number of retry attempts for cleanup operations
	MaxRetries = 5
)

// NamespaceController interface defines the namespace controller for efs-ns cleanup orchestration
type NamespaceController interface {
	// Start starts the namespace controller with the given context
	Start(ctx context.Context) error

	// Stop gracefully stops the namespace controller
	Stop() error

	// ProcessNamespaceDeletion handles immediate namespace deletion processing
	ProcessNamespaceDeletion(ctx context.Context, namespace string) error

	// IsRunning returns true if the controller is currently running
	IsRunning() bool
}

// NamespaceControllerConfig contains configuration for the namespace controller
type NamespaceControllerConfig struct {
	// ResyncPeriod is the period for namespace controller resync
	ResyncPeriod time.Duration

	// WatchTimeout is the timeout for namespace watch operations
	WatchTimeout time.Duration

	// CleanupTimeout is the maximum time to wait for namespace cleanup
	CleanupTimeout time.Duration

	// RetryDelay is the delay between retry attempts
	RetryDelay time.Duration

	// MaxRetries is the maximum number of retry attempts
	MaxRetries int

	// EnableMetrics enables metrics collection for the controller
	EnableMetrics bool
}

// DefaultNamespaceControllerConfig returns default configuration for namespace controller
func DefaultNamespaceControllerConfig() *NamespaceControllerConfig {
	return &NamespaceControllerConfig{
		ResyncPeriod:   DefaultResyncPeriod,
		WatchTimeout:   DefaultWatchTimeout,
		CleanupTimeout: CleanupTimeout,
		RetryDelay:     RetryDelay,
		MaxRetries:     MaxRetries,
		EnableMetrics:  true,
	}
}

// namespaceController implements the NamespaceController interface
type namespaceController struct {
	client            kubernetes.Interface
	finalizerManager  FinalizerManager
	fileSystemManager NamespaceFileSystemManager
	pvcTracker        PVCTracker
	config            *NamespaceControllerConfig
	clusterID         string

	// Runtime state
	running          bool
	stopCh           chan struct{}
	mutex            sync.RWMutex
	watcher          watch.Interface
	metricsCollector *NamespaceControllerMetrics
}

// NewNamespaceController creates a new namespace controller for efs-ns cleanup orchestration
func NewNamespaceController(
	client kubernetes.Interface,
	finalizerManager FinalizerManager,
	fileSystemManager NamespaceFileSystemManager,
	pvcTracker PVCTracker,
	clusterID string,
	config *NamespaceControllerConfig,
) NamespaceController {
	if client == nil {
		panic("kubernetes client cannot be nil")
	}
	if finalizerManager == nil {
		panic("finalizer manager cannot be nil")
	}
	if fileSystemManager == nil {
		panic("filesystem manager cannot be nil")
	}
	if pvcTracker == nil {
		panic("PVC tracker cannot be nil")
	}
	if clusterID == "" {
		panic("cluster ID cannot be empty")
	}
	if config == nil {
		config = DefaultNamespaceControllerConfig()
	}

	controller := &namespaceController{
		client:            client,
		finalizerManager:  finalizerManager,
		fileSystemManager: fileSystemManager,
		pvcTracker:        pvcTracker,
		config:            config,
		clusterID:         clusterID,
		stopCh:            make(chan struct{}),
	}

	if config.EnableMetrics {
		controller.metricsCollector = NewNamespaceControllerMetrics()
	}

	return controller
}

// Start starts the namespace controller with the given context
func (c *namespaceController) Start(ctx context.Context) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.running {
		return NewEFSNSError(ErrInvalidParameter, "Start", "", "controller is already running", nil)
	}

	klog.V(2).InfoS("Starting namespace controller", "component", NamespaceControllerName, "clusterId", c.clusterID)

	// Initialize metrics if enabled
	if c.metricsCollector != nil {
		c.metricsCollector.Start()
	}

	// Start watching namespaces
	err := c.startNamespaceWatcher(ctx)
	if err != nil {
		return NewEFSNSError(ErrNamespaceControllerFailed, "Start", "",
			"failed to start namespace watcher", err)
	}

	c.running = true

	// Start background cleanup reconciler
	go c.reconcileLoop(ctx)

	klog.V(2).InfoS("Namespace controller started successfully", "component", NamespaceControllerName)
	return nil
}

// Stop gracefully stops the namespace controller
func (c *namespaceController) Stop() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.running {
		return nil
	}

	klog.V(2).InfoS("Stopping namespace controller", "component", NamespaceControllerName)

	// Signal stop to all goroutines
	close(c.stopCh)

	// Stop the namespace watcher
	if c.watcher != nil {
		c.watcher.Stop()
		c.watcher = nil
	}

	// Stop metrics collection
	if c.metricsCollector != nil {
		c.metricsCollector.Stop()
	}

	c.running = false

	klog.V(2).InfoS("Namespace controller stopped", "component", NamespaceControllerName)
	return nil
}

// ProcessNamespaceDeletion handles immediate namespace deletion processing
func (c *namespaceController) ProcessNamespaceDeletion(ctx context.Context, namespace string) error {
	if namespace == "" {
		return NewEFSNSError(ErrInvalidParameter, "ProcessNamespaceDeletion", namespace,
			"namespace cannot be empty", nil)
	}

	klog.V(4).InfoS("Processing namespace deletion", "namespace", namespace, "component", NamespaceControllerName)

	// Create timeout context for cleanup operations
	cleanupCtx, cancel := context.WithTimeout(ctx, c.config.CleanupTimeout)
	defer cancel()

	// Record metrics
	if c.metricsCollector != nil {
		c.metricsCollector.IncNamespaceDeletionAttempts(namespace)
		startTime := time.Now()
		defer func() {
			c.metricsCollector.ObserveNamespaceCleanupDuration(namespace, time.Since(startTime))
		}()
	}

	// Check if namespace has any efs-ns PVCs
	pvcCount, err := c.pvcTracker.GetPVCCount(cleanupCtx, namespace)
	if err != nil {
		klog.ErrorS(err, "Failed to get PVC count for namespace", "namespace", namespace)
		if c.metricsCollector != nil {
			c.metricsCollector.IncNamespaceCleanupErrors(namespace, "pvc_count_failed")
		}
		// Continue with cleanup attempt even if count fails
	}

	klog.V(4).InfoS("Namespace PVC count", "namespace", namespace, "pvcCount", pvcCount)

	// If there are still PVCs, we cannot complete cleanup yet
	if pvcCount > 0 {
		klog.V(4).InfoS("Namespace still has active PVCs, deferring cleanup",
			"namespace", namespace, "pvcCount", pvcCount)

		// Ensure namespace has our finalizer to prevent deletion
		err = c.finalizerManager.AddNamespaceFinalizer(cleanupCtx, namespace)
		if err != nil {
			klog.ErrorS(err, "Failed to add namespace finalizer", "namespace", namespace)
			if c.metricsCollector != nil {
				c.metricsCollector.IncNamespaceCleanupErrors(namespace, "finalizer_add_failed")
			}
			return NewEFSNSError(ErrNamespaceControllerFailed, "ProcessNamespaceDeletion", namespace,
				"failed to add namespace finalizer", err)
		}

		return NewEFSNSError(ErrResourceNotReady, "ProcessNamespaceDeletion", namespace,
			fmt.Sprintf("namespace still has %d active PVCs, cleanup deferred", pvcCount), nil)
	}

	// No PVCs remain, proceed with filesystem cleanup
	err = c.cleanupNamespaceFilesystem(cleanupCtx, namespace)
	if err != nil {
		klog.ErrorS(err, "Failed to cleanup namespace filesystem", "namespace", namespace)
		if c.metricsCollector != nil {
			c.metricsCollector.IncNamespaceCleanupErrors(namespace, "filesystem_cleanup_failed")
		}
		return NewEFSNSError(ErrNamespaceControllerFailed, "ProcessNamespaceDeletion", namespace,
			"failed to cleanup namespace filesystem", err)
	}

	// Remove our finalizer to allow namespace deletion
	err = c.finalizerManager.RemoveNamespaceFinalizer(cleanupCtx, namespace)
	if err != nil {
		klog.ErrorS(err, "Failed to remove namespace finalizer", "namespace", namespace)
		if c.metricsCollector != nil {
			c.metricsCollector.IncNamespaceCleanupErrors(namespace, "finalizer_remove_failed")
		}
		return NewEFSNSError(ErrNamespaceControllerFailed, "ProcessNamespaceDeletion", namespace,
			"failed to remove namespace finalizer", err)
	}

	// Record successful cleanup
	if c.metricsCollector != nil {
		c.metricsCollector.IncSuccessfulNamespaceCleanups(namespace)
	}

	klog.V(2).InfoS("Successfully processed namespace deletion", "namespace", namespace)
	return nil
}

// IsRunning returns true if the controller is currently running
func (c *namespaceController) IsRunning() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.running
}

// startNamespaceWatcher starts watching namespace events
func (c *namespaceController) startNamespaceWatcher(ctx context.Context) error {
	// Create a field selector to watch namespace deletion events
	fieldSelector := fields.Everything()

	watchOptions := metav1.ListOptions{
		FieldSelector:  fieldSelector.String(),
		TimeoutSeconds: &[]int64{int64(c.config.WatchTimeout.Seconds())}[0],
	}

	watcher, err := c.client.CoreV1().Namespaces().Watch(ctx, watchOptions)
	if err != nil {
		return fmt.Errorf("failed to create namespace watcher: %w", err)
	}

	c.watcher = watcher

	// Start processing watch events
	go c.processNamespaceEvents(ctx, watcher.ResultChan())

	klog.V(4).InfoS("Namespace watcher started", "component", NamespaceControllerName)
	return nil
}

// processNamespaceEvents processes namespace watch events
func (c *namespaceController) processNamespaceEvents(ctx context.Context, eventCh <-chan watch.Event) {
	defer func() {
		if r := recover(); r != nil {
			klog.ErrorS(fmt.Errorf("panic in namespace event processor: %v", r),
				"Namespace event processor panicked", "component", NamespaceControllerName)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			klog.V(4).InfoS("Context cancelled, stopping namespace event processor")
			return
		case <-c.stopCh:
			klog.V(4).InfoS("Stop signal received, stopping namespace event processor")
			return
		case event, ok := <-eventCh:
			if !ok {
				klog.V(4).InfoS("Namespace watch channel closed, restarting watcher")
				// Restart watcher after a delay
				time.Sleep(c.config.RetryDelay)
				if err := c.restartNamespaceWatcher(ctx); err != nil {
					klog.ErrorS(err, "Failed to restart namespace watcher")
				}
				return
			}

			if err := c.handleNamespaceEvent(ctx, event); err != nil {
				klog.ErrorS(err, "Failed to handle namespace event", "eventType", event.Type)
			}
		}
	}
}

// handleNamespaceEvent handles a single namespace event
func (c *namespaceController) handleNamespaceEvent(ctx context.Context, event watch.Event) error {
	namespace, ok := event.Object.(*corev1.Namespace)
	if !ok {
		return fmt.Errorf("unexpected object type: %T", event.Object)
	}

	klog.V(4).InfoS("Received namespace event", "eventType", event.Type, "namespace", namespace.Name)

	switch event.Type {
	case watch.Added:
		// Check if this namespace uses efs-ns and should have our finalizer
		return c.handleNamespaceAdded(ctx, namespace)
	case watch.Modified:
		// Check if namespace is being deleted
		return c.handleNamespaceModified(ctx, namespace)
	case watch.Deleted:
		// Namespace was deleted, perform any final cleanup
		return c.handleNamespaceDeleted(ctx, namespace)
	default:
		klog.V(4).InfoS("Ignoring namespace event", "eventType", event.Type, "namespace", namespace.Name)
	}

	return nil
}

// handleNamespaceAdded handles namespace addition events
func (c *namespaceController) handleNamespaceAdded(ctx context.Context, namespace *corev1.Namespace) error {
	// For now, we don't need to do anything special when namespaces are added
	// The finalizer will be added when the first efs-ns PVC is created
	klog.V(4).InfoS("Namespace added", "namespace", namespace.Name)
	return nil
}

// handleNamespaceModified handles namespace modification events
func (c *namespaceController) handleNamespaceModified(ctx context.Context, namespace *corev1.Namespace) error {
	// Check if namespace has our finalizer and is being deleted
	if namespace.DeletionTimestamp != nil {
		hasOurFinalizer := false
		for _, finalizer := range namespace.Finalizers {
			if finalizer == EFSNSNamespaceFinalizerName {
				hasOurFinalizer = true
				break
			}
		}

		if hasOurFinalizer {
			klog.V(4).InfoS("Namespace with our finalizer is being deleted, processing cleanup",
				"namespace", namespace.Name)

			// Process the deletion (this may take time and involve retries)
			go c.processNamespaceDeletionAsync(ctx, namespace.Name)
		}
	}

	return nil
}

// handleNamespaceDeleted handles namespace deletion events
func (c *namespaceController) handleNamespaceDeleted(ctx context.Context, namespace *corev1.Namespace) error {
	klog.V(4).InfoS("Namespace deleted", "namespace", namespace.Name)

	// Clean up any remaining tracking data
	// This is a safety net in case the finalizer cleanup didn't work properly
	_ = c.cleanupTrackingData(ctx, namespace.Name)

	return nil
}

// processNamespaceDeletionAsync processes namespace deletion asynchronously with retries
func (c *namespaceController) processNamespaceDeletionAsync(ctx context.Context, namespace string) {
	for attempt := 1; attempt <= c.config.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		klog.V(4).InfoS("Attempting namespace deletion cleanup", "namespace", namespace, "attempt", attempt)

		err := c.ProcessNamespaceDeletion(ctx, namespace)
		if err == nil {
			klog.V(2).InfoS("Successfully processed namespace deletion", "namespace", namespace)
			return
		}

		// Check if this is a retryable error
		if IsEFSNSError(err, ErrResourceNotReady) {
			klog.V(4).InfoS("Namespace cleanup deferred due to active PVCs", "namespace", namespace)
			// Wait longer for PVCs to be cleaned up
			time.Sleep(c.config.RetryDelay * 2)
			continue
		}

		klog.ErrorS(err, "Failed to process namespace deletion", "namespace", namespace, "attempt", attempt)

		if attempt < c.config.MaxRetries {
			klog.V(4).InfoS("Retrying namespace deletion cleanup", "namespace", namespace, "nextAttempt", attempt+1)
			time.Sleep(c.config.RetryDelay)
		}
	}

	klog.ErrorS(fmt.Errorf("max retries exceeded"), "Failed to process namespace deletion after retries",
		"namespace", namespace, "maxRetries", c.config.MaxRetries)

	if c.metricsCollector != nil {
		c.metricsCollector.IncNamespaceCleanupErrors(namespace, "max_retries_exceeded")
	}
}

// reconcileLoop runs a periodic reconciliation to ensure namespace cleanup consistency
func (c *namespaceController) reconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(c.config.ResyncPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-ticker.C:
			if err := c.reconcileNamespaces(ctx); err != nil {
				klog.ErrorS(err, "Failed to reconcile namespaces")
			}
		}
	}
}

// reconcileNamespaces performs periodic reconciliation of namespace cleanup state
func (c *namespaceController) reconcileNamespaces(ctx context.Context) error {
	klog.V(4).InfoS("Starting namespace reconciliation", "component", NamespaceControllerName)

	// Get all namespaces with our finalizer
	namespaces, err := c.client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list namespaces: %w", err)
	}

	reconciledCount := 0
	for _, namespace := range namespaces.Items {
		// Check if namespace has our finalizer and is being deleted
		if namespace.DeletionTimestamp != nil {
			hasOurFinalizer := false
			for _, finalizer := range namespace.Finalizers {
				if finalizer == EFSNSNamespaceFinalizerName {
					hasOurFinalizer = true
					break
				}
			}

			if hasOurFinalizer {
				klog.V(4).InfoS("Found namespace with finalizer during reconciliation", "namespace", namespace.Name)

				// Attempt cleanup
				err := c.ProcessNamespaceDeletion(ctx, namespace.Name)
				if err != nil {
					klog.ErrorS(err, "Failed to process namespace during reconciliation", "namespace", namespace.Name)
				} else {
					reconciledCount++
				}
			}
		}
	}

	klog.V(4).InfoS("Completed namespace reconciliation", "component", NamespaceControllerName,
		"totalNamespaces", len(namespaces.Items), "reconciledNamespaces", reconciledCount)

	return nil
}

// restartNamespaceWatcher restarts the namespace watcher after a failure
func (c *namespaceController) restartNamespaceWatcher(ctx context.Context) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.running {
		return nil // Controller is stopping
	}

	klog.V(4).InfoS("Restarting namespace watcher", "component", NamespaceControllerName)

	// Stop existing watcher if any
	if c.watcher != nil {
		c.watcher.Stop()
		c.watcher = nil
	}

	// Start new watcher
	return c.startNamespaceWatcher(ctx)
}

// cleanupNamespaceFilesystem performs filesystem cleanup for a namespace
func (c *namespaceController) cleanupNamespaceFilesystem(ctx context.Context, namespace string) error {
	klog.V(4).InfoS("Cleaning up filesystem for namespace", "namespace", namespace)

	// Get filesystem info for the namespace
	fsInfo, err := c.fileSystemManager.GetFileSystemInfo(ctx, namespace)
	if err != nil {
		if IsEFSNSError(err, ErrFileSystemNotFound) {
			klog.V(4).InfoS("No filesystem found for namespace, cleanup complete", "namespace", namespace)
			return nil
		}
		return fmt.Errorf("failed to get filesystem info for namespace %s: %w", namespace, err)
	}

	// Construct volume ID for filesystem deletion
	volumeID := &EFSNSVolumeID{
		Namespace:    namespace,
		FileSystemID: fsInfo.FileSystemID,
		ClusterID:    c.clusterID,
	}

	// Delete the filesystem
	err = c.fileSystemManager.DeleteFileSystemForNamespace(ctx, namespace, volumeID.String())
	if err != nil {
		return fmt.Errorf("failed to delete filesystem for namespace %s: %w", namespace, err)
	}

	klog.V(2).InfoS("Successfully cleaned up filesystem for namespace",
		"namespace", namespace, "filesystemId", fsInfo.FileSystemID)

	return nil
}

// cleanupTrackingData cleans up any remaining tracking data for a namespace
func (c *namespaceController) cleanupTrackingData(ctx context.Context, namespace string) error {
	klog.V(4).InfoS("Cleaning up tracking data for namespace", "namespace", namespace)

	// Sync the tracker to clean up any orphaned data
	err := c.pvcTracker.SyncWithCluster(ctx)
	if err != nil {
		klog.ErrorS(err, "Failed to sync PVC tracker during cleanup", "namespace", namespace)
		return err
	}

	return nil
}
