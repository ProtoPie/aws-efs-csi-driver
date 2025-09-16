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

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	efsv1alpha1 "github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/apis/efs/v1alpha1"
)

// NamespaceEFSMapping represents the mapping between a namespace and EFS filesystem
type NamespaceEFSMapping struct {
	Namespace      string    `json:"namespace"`
	FileSystemID   string    `json:"fileSystemId"`
	FileSystemArn  string    `json:"fileSystemArn"`
	Region         string    `json:"region"`
	CreationTime   time.Time `json:"creationTime"`
	LastAccessTime time.Time `json:"lastAccessTime"`
}

// NamespaceEFSMapperInterface defines the interface for namespace-to-EFS mapping operations
type NamespaceEFSMapperInterface interface {
	// CRUD operations
	CreateOrUpdateMapping(ctx context.Context, namespace, fileSystemID, fileSystemArn, region string) (*NamespaceEFSMapping, error)
	GetMapping(ctx context.Context, namespace string) (*NamespaceEFSMapping, error)
	DeleteMapping(ctx context.Context, namespace string) error
	ListMappings(ctx context.Context) ([]NamespaceEFSMapping, error)

	// Lifecycle management
	Start(ctx context.Context) error
	Stop()

	// Cache operations
	InvalidateCache(namespace string)
	ClearCache()
}

// NamespaceEFSMapper manages the mapping between Kubernetes namespaces and EFS filesystems
// using CRD-based persistence with local caching for performance
type NamespaceEFSMapper struct {
	// CRD client for persistent storage
	crdClient efsv1alpha1.EFSNamespaceInterface

	// Kubernetes client for general operations
	k8sClient kubernetes.Interface

	// Local cache for performance optimization
	cache      map[string]*NamespaceEFSMapping
	cacheMutex sync.RWMutex

	// Informer for real-time CRD updates
	informer cache.SharedIndexInformer
	stopCh   chan struct{}

	// Configuration
	resyncPeriod time.Duration

	// Initialization state
	initialized bool
	initMutex   sync.Mutex
}

// NewNamespaceEFSMapper creates a new instance of NamespaceEFSMapper
func NewNamespaceEFSMapper(k8sClient kubernetes.Interface, config *rest.Config) (*NamespaceEFSMapper, error) {
	if k8sClient == nil {
		return nil, fmt.Errorf("kubernetes client cannot be nil")
	}
	if config == nil {
		return nil, fmt.Errorf("rest config cannot be nil")
	}

	// Create CRD client
	crdClient, err := efsv1alpha1.NewEFSNamespaceClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create EFSNamespace CRD client: %w", err)
	}

	mapper := &NamespaceEFSMapper{
		crdClient:    crdClient,
		k8sClient:    k8sClient,
		cache:        make(map[string]*NamespaceEFSMapping),
		resyncPeriod: 5 * time.Minute, // Default resync period
		stopCh:       make(chan struct{}),
	}

	return mapper, nil
}

// Start initializes the mapper and starts the informer for real-time updates
func (m *NamespaceEFSMapper) Start(ctx context.Context) error {
	m.initMutex.Lock()
	defer m.initMutex.Unlock()

	if m.initialized {
		return nil
	}

	klog.V(2).InfoS("Starting NamespaceEFSMapper")

	// Create informer for real-time CRD updates
	m.informer = efsv1alpha1.NewEFSNamespaceInformer(m.crdClient, m.resyncPeriod)

	// Add event handlers for cache synchronization
	m.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if efsNamespace, ok := obj.(*efsv1alpha1.EFSNamespace); ok {
				m.onEFSNamespaceAdd(efsNamespace)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if efsNamespace, ok := newObj.(*efsv1alpha1.EFSNamespace); ok {
				m.onEFSNamespaceUpdate(efsNamespace)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if efsNamespace, ok := obj.(*efsv1alpha1.EFSNamespace); ok {
				m.onEFSNamespaceDelete(efsNamespace)
			}
		},
	})

	// Start the informer in a goroutine
	go m.informer.Run(m.stopCh)

	// Wait for cache synchronization
	if !cache.WaitForCacheSync(ctx.Done(), m.informer.HasSynced) {
		return fmt.Errorf("failed to sync EFSNamespace informer cache")
	}

	// Load initial cache from CRD
	if err := m.loadCacheFromCRD(ctx); err != nil {
		klog.ErrorS(err, "Failed to load initial cache from CRD, continuing with empty cache")
	}

	m.initialized = true
	klog.V(2).InfoS("NamespaceEFSMapper started successfully")
	return nil
}

// Stop gracefully shuts down the mapper
func (m *NamespaceEFSMapper) Stop() {
	m.initMutex.Lock()
	defer m.initMutex.Unlock()

	if !m.initialized {
		return
	}

	klog.V(2).InfoS("Stopping NamespaceEFSMapper")
	close(m.stopCh)
	m.initialized = false
	klog.V(2).InfoS("NamespaceEFSMapper stopped")
}

// CreateOrUpdateMapping creates or updates a namespace-to-EFS mapping
func (m *NamespaceEFSMapper) CreateOrUpdateMapping(ctx context.Context, namespace, fileSystemID, fileSystemArn, region string) (*NamespaceEFSMapping, error) {
	if namespace == "" || fileSystemID == "" || region == "" {
		return nil, fmt.Errorf("namespace, fileSystemID, and region are required")
	}

	klog.V(4).InfoS("Creating or updating namespace EFS mapping",
		"namespace", namespace,
		"fileSystemID", fileSystemID,
		"region", region)

	// Create or update the CRD resource
	efsNamespace := &efsv1alpha1.EFSNamespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace, // CRD name matches namespace name
		},
		Spec: efsv1alpha1.EFSNamespaceSpec{
			Namespace:     namespace,
			FileSystemID:  fileSystemID,
			FileSystemArn: fileSystemArn,
			Region:        region,
		},
	}

	var result *efsv1alpha1.EFSNamespace
	var err error

	// Try to get existing resource first
	existing, getErr := m.crdClient.Get(ctx, namespace, metav1.GetOptions{})
	if getErr != nil && !errors.IsNotFound(getErr) {
		return nil, fmt.Errorf("failed to check existing EFSNamespace: %w", getErr)
	}

	if errors.IsNotFound(getErr) {
		// Create new resource
		result, err = m.crdClient.Create(ctx, efsNamespace, metav1.CreateOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to create EFSNamespace CRD: %w", err)
		}
		klog.V(2).InfoS("Created new EFSNamespace CRD", "namespace", namespace, "fileSystemID", fileSystemID)
	} else {
		// Update existing resource
		existing.Spec.FileSystemID = fileSystemID
		existing.Spec.FileSystemArn = fileSystemArn
		existing.Spec.Region = region

		result, err = m.crdClient.Update(ctx, existing, metav1.UpdateOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to update EFSNamespace CRD: %w", err)
		}
		klog.V(2).InfoS("Updated existing EFSNamespace CRD", "namespace", namespace, "fileSystemID", fileSystemID)
	}

	// Create mapping object
	mapping := &NamespaceEFSMapping{
		Namespace:      namespace,
		FileSystemID:   fileSystemID,
		FileSystemArn:  fileSystemArn,
		Region:         region,
		CreationTime:   result.CreationTimestamp.Time,
		LastAccessTime: time.Now(),
	}

	// Update local cache
	m.updateCache(namespace, mapping)

	return mapping, nil
}

// GetMapping retrieves a namespace-to-EFS mapping
func (m *NamespaceEFSMapper) GetMapping(ctx context.Context, namespace string) (*NamespaceEFSMapping, error) {
	if namespace == "" {
		return nil, fmt.Errorf("namespace is required")
	}

	klog.V(4).InfoS("Getting namespace EFS mapping", "namespace", namespace)

	// Try cache first for performance
	if mapping := m.getCachedMapping(namespace); mapping != nil {
		// Update last access time
		mapping.LastAccessTime = time.Now()
		m.updateCache(namespace, mapping)
		klog.V(4).InfoS("Found mapping in cache", "namespace", namespace, "fileSystemID", mapping.FileSystemID)
		return mapping, nil
	}

	// Fallback to CRD if not in cache
	efsNamespace, err := m.crdClient.Get(ctx, namespace, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			klog.V(4).InfoS("No EFS mapping found for namespace", "namespace", namespace)
			return nil, nil // Return nil without error for not found
		}
		return nil, fmt.Errorf("failed to get EFSNamespace CRD: %w", err)
	}

	// Create mapping from CRD data
	mapping := &NamespaceEFSMapping{
		Namespace:      efsNamespace.Spec.Namespace,
		FileSystemID:   efsNamespace.Spec.FileSystemID,
		FileSystemArn:  efsNamespace.Spec.FileSystemArn,
		Region:         efsNamespace.Spec.Region,
		CreationTime:   efsNamespace.CreationTimestamp.Time,
		LastAccessTime: time.Now(),
	}

	// Update cache for future requests
	m.updateCache(namespace, mapping)

	klog.V(4).InfoS("Found mapping in CRD", "namespace", namespace, "fileSystemID", mapping.FileSystemID)
	return mapping, nil
}

// DeleteMapping deletes a namespace-to-EFS mapping
func (m *NamespaceEFSMapper) DeleteMapping(ctx context.Context, namespace string) error {
	if namespace == "" {
		return fmt.Errorf("namespace is required")
	}

	klog.V(2).InfoS("Deleting namespace EFS mapping", "namespace", namespace)

	// Delete from CRD
	err := m.crdClient.Delete(ctx, namespace, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete EFSNamespace CRD: %w", err)
	}

	// Remove from cache
	m.invalidateCache(namespace)

	klog.V(2).InfoS("Deleted namespace EFS mapping", "namespace", namespace)
	return nil
}

// ListMappings returns all namespace-to-EFS mappings
func (m *NamespaceEFSMapper) ListMappings(ctx context.Context) ([]NamespaceEFSMapping, error) {
	klog.V(4).InfoS("Listing all namespace EFS mappings")

	// Get all EFSNamespace CRDs
	efsNamespaceList, err := m.crdClient.List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list EFSNamespace CRDs: %w", err)
	}

	mappings := make([]NamespaceEFSMapping, 0, len(efsNamespaceList.Items))
	for _, efsNamespace := range efsNamespaceList.Items {
		mapping := NamespaceEFSMapping{
			Namespace:      efsNamespace.Spec.Namespace,
			FileSystemID:   efsNamespace.Spec.FileSystemID,
			FileSystemArn:  efsNamespace.Spec.FileSystemArn,
			Region:         efsNamespace.Spec.Region,
			CreationTime:   efsNamespace.CreationTimestamp.Time,
			LastAccessTime: time.Now(),
		}
		mappings = append(mappings, mapping)

		// Update cache as we go
		m.updateCache(efsNamespace.Spec.Namespace, &mapping)
	}

	klog.V(4).InfoS("Listed namespace EFS mappings", "count", len(mappings))
	return mappings, nil
}

// InvalidateCache removes a specific namespace from the cache
func (m *NamespaceEFSMapper) InvalidateCache(namespace string) {
	m.invalidateCache(namespace)
}

// ClearCache removes all entries from the cache
func (m *NamespaceEFSMapper) ClearCache() {
	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()

	m.cache = make(map[string]*NamespaceEFSMapping)
	klog.V(4).InfoS("Cleared namespace EFS mapping cache")
}

// Private helper methods

func (m *NamespaceEFSMapper) getCachedMapping(namespace string) *NamespaceEFSMapping {
	m.cacheMutex.RLock()
	defer m.cacheMutex.RUnlock()

	if mapping, exists := m.cache[namespace]; exists {
		// Return a copy to prevent external modifications
		mappingCopy := *mapping
		return &mappingCopy
	}
	return nil
}

func (m *NamespaceEFSMapper) updateCache(namespace string, mapping *NamespaceEFSMapping) {
	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()

	// Store a copy to prevent external modifications
	mappingCopy := *mapping
	m.cache[namespace] = &mappingCopy
}

func (m *NamespaceEFSMapper) invalidateCache(namespace string) {
	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()

	delete(m.cache, namespace)
	klog.V(4).InfoS("Invalidated cache for namespace", "namespace", namespace)
}

func (m *NamespaceEFSMapper) loadCacheFromCRD(ctx context.Context) error {
	klog.V(4).InfoS("Loading initial cache from CRD")

	mappings, err := m.ListMappings(ctx)
	if err != nil {
		return fmt.Errorf("failed to load mappings from CRD: %w", err)
	}

	klog.V(4).InfoS("Loaded initial cache from CRD", "mappingCount", len(mappings))
	return nil
}

// Informer event handlers

func (m *NamespaceEFSMapper) onEFSNamespaceAdd(efsNamespace *efsv1alpha1.EFSNamespace) {
	klog.V(4).InfoS("EFSNamespace added", "namespace", efsNamespace.Spec.Namespace)

	mapping := &NamespaceEFSMapping{
		Namespace:      efsNamespace.Spec.Namespace,
		FileSystemID:   efsNamespace.Spec.FileSystemID,
		FileSystemArn:  efsNamespace.Spec.FileSystemArn,
		Region:         efsNamespace.Spec.Region,
		CreationTime:   efsNamespace.CreationTimestamp.Time,
		LastAccessTime: time.Now(),
	}

	m.updateCache(efsNamespace.Spec.Namespace, mapping)
}

func (m *NamespaceEFSMapper) onEFSNamespaceUpdate(efsNamespace *efsv1alpha1.EFSNamespace) {
	klog.V(4).InfoS("EFSNamespace updated", "namespace", efsNamespace.Spec.Namespace)

	mapping := &NamespaceEFSMapping{
		Namespace:      efsNamespace.Spec.Namespace,
		FileSystemID:   efsNamespace.Spec.FileSystemID,
		FileSystemArn:  efsNamespace.Spec.FileSystemArn,
		Region:         efsNamespace.Spec.Region,
		CreationTime:   efsNamespace.CreationTimestamp.Time,
		LastAccessTime: time.Now(),
	}

	m.updateCache(efsNamespace.Spec.Namespace, mapping)
}

func (m *NamespaceEFSMapper) onEFSNamespaceDelete(efsNamespace *efsv1alpha1.EFSNamespace) {
	klog.V(4).InfoS("EFSNamespace deleted", "namespace", efsNamespace.Spec.Namespace)
	m.invalidateCache(efsNamespace.Spec.Namespace)
}
