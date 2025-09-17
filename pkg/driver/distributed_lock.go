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

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	"k8s.io/utils/pointer"
)

const (
	// Lock backend types
	LockBackendConfigMap = "configmap"
	LockBackendLease     = "lease"

	// Deadlock detection
	DeadlockCheckInterval   = 30 * time.Second
	DeadlockWarningAge      = 3 * time.Minute
	DeadlockCriticalAge     = 5 * time.Minute
	MaxWaitingLocks         = 100

	// Lease-specific constants
	LeaseDurationSeconds    = 30
	LeaseRenewSeconds       = 10
	LeaseTransitionTime     = 2 * time.Second
)

// DistributedLock represents a distributed lock interface
type DistributedLock interface {
	// Acquire tries to acquire the lock with the given timeout
	Acquire(ctx context.Context, timeout time.Duration) error
	// Release releases the lock
	Release(ctx context.Context) error
	// Refresh extends the lock lease/expiration
	Refresh(ctx context.Context) error
	// IsHeld returns true if the lock is currently held by this instance
	IsHeld() bool
	// GetHolder returns the current lock holder identity
	GetHolder() (string, error)
}

// DistributedLockManager manages different types of distributed locks
type DistributedLockManager struct {
	backend       string
	k8sClient     kubernetes.Interface
	namespace     string
	identity      string

	// ConfigMap backend (existing implementation)
	configMapLock *NamespaceLockManager

	// Lease backend (new implementation)
	leaseLocks    map[string]*LeaseLock
	leaseMutex    sync.RWMutex

	// Deadlock detection
	deadlockDetector *DeadlockDetector

	// Metrics
	metrics       *LockMetrics

	stopCh        chan struct{}
	wg            sync.WaitGroup
}

// DistributedLockOptions contains options for the distributed lock manager
type DistributedLockOptions struct {
	Backend           string
	Namespace         string
	Identity          string
	LockTimeout       time.Duration
	RenewalPeriod     time.Duration
	MaxRetries        int
	EnableDeadlockDetection bool
}

// NewDistributedLockManager creates a new distributed lock manager
func NewDistributedLockManager(k8sClient kubernetes.Interface, opts *DistributedLockOptions) (*DistributedLockManager, error) {
	if opts == nil {
		opts = &DistributedLockOptions{
			Backend:           LockBackendConfigMap,
			Namespace:         "kube-system",
			EnableDeadlockDetection: true,
		}
	}

	manager := &DistributedLockManager{
		backend:    opts.Backend,
		k8sClient:  k8sClient,
		namespace:  opts.Namespace,
		identity:   opts.Identity,
		leaseLocks: make(map[string]*LeaseLock),
		metrics:    NewLockMetrics(),
		stopCh:     make(chan struct{}),
	}

	// Initialize backend-specific components
	switch opts.Backend {
	case LockBackendConfigMap:
		manager.configMapLock = NewNamespaceLockManager(k8sClient, opts.Identity, &NamespaceLockManagerOptions{
			LockTimeout:   opts.LockTimeout,
			RenewalPeriod: opts.RenewalPeriod,
			MaxRetries:    opts.MaxRetries,
			LockNamespace: opts.Namespace,
		})
	case LockBackendLease:
		// Lease locks are created on-demand
	default:
		return nil, fmt.Errorf("unsupported lock backend: %s", opts.Backend)
	}

	// Initialize deadlock detector if enabled
	if opts.EnableDeadlockDetection {
		manager.deadlockDetector = NewDeadlockDetector(manager)
		manager.wg.Add(1)
		go manager.deadlockDetector.Start(manager.stopCh, &manager.wg)
	}

	return manager, nil
}

// CreateLock creates a new distributed lock for the given key
func (m *DistributedLockManager) CreateLock(key string, lockType string) (DistributedLock, error) {
	switch m.backend {
	case LockBackendConfigMap:
		return &ConfigMapLock{
			manager:  m.configMapLock,
			key:      key,
			lockType: lockType,
		}, nil

	case LockBackendLease:
		m.leaseMutex.Lock()
		defer m.leaseMutex.Unlock()

		if lock, exists := m.leaseLocks[key]; exists {
			return lock, nil
		}

		lock := &LeaseLock{
			k8sClient: m.k8sClient,
			namespace: m.namespace,
			name:      sanitizeLeaseName(key),
			identity:  m.identity,
			key:       key,
			lockType:  lockType,
			metrics:   m.metrics,
		}
		m.leaseLocks[key] = lock
		return lock, nil

	default:
		return nil, fmt.Errorf("unsupported lock backend: %s", m.backend)
	}
}

// ConfigMapLock wraps the existing NamespaceLockManager for the DistributedLock interface
type ConfigMapLock struct {
	manager  *NamespaceLockManager
	key      string
	lockType string
}

func (c *ConfigMapLock) Acquire(ctx context.Context, timeout time.Duration) error {
	if c.lockType == LockTypeNamespace {
		return c.manager.AcquireNamespaceLock(ctx, c.key, timeout)
	}
	return c.manager.AcquireEFSLock(ctx, c.key, timeout)
}

func (c *ConfigMapLock) Release(ctx context.Context) error {
	if c.lockType == LockTypeNamespace {
		return c.manager.ReleaseNamespaceLock(ctx, c.key)
	}
	return c.manager.ReleaseEFSLock(ctx, c.key)
}

func (c *ConfigMapLock) Refresh(ctx context.Context) error {
	// ConfigMap locks are auto-renewed in background
	return nil
}

func (c *ConfigMapLock) IsHeld() bool {
	// Check if lock is in active locks
	_, ok := c.manager.activeLocks.Load(c.key)
	return ok
}

func (c *ConfigMapLock) GetHolder() (string, error) {
	// This would require accessing the ConfigMap, simplified for now
	return c.manager.identity, nil
}

// LeaseLock implements distributed locking using Kubernetes Leases
type LeaseLock struct {
	k8sClient    kubernetes.Interface
	namespace    string
	name         string
	identity     string
	key          string
	lockType     string

	lease        *coordinationv1.Lease
	leaseMutex   sync.RWMutex
	renewStop    chan struct{}
	isHeld       bool
	lastRenewed  time.Time

	metrics      *LockMetrics
}

// Acquire tries to acquire the lease lock
func (l *LeaseLock) Acquire(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	backoff := retry.DefaultBackoff
	backoff.Cap = 5 * time.Second

	var lastErr error
	err := retry.OnError(backoff, errors.IsConflict, func() error {
		if time.Now().After(deadline) {
			if lastErr != nil {
				return lastErr
			}
			return fmt.Errorf("timeout acquiring lease lock %s", l.key)
		}

		// Try to get existing lease
		lease, err := l.k8sClient.CoordinationV1().Leases(l.namespace).Get(ctx, l.name, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				// Create new lease
				newLease := &coordinationv1.Lease{
					ObjectMeta: metav1.ObjectMeta{
						Name:      l.name,
						Namespace: l.namespace,
					},
					Spec: coordinationv1.LeaseSpec{
						HolderIdentity:       pointer.String(l.identity),
						LeaseDurationSeconds: pointer.Int32(LeaseDurationSeconds),
						AcquireTime:          &metav1.MicroTime{Time: time.Now()},
						RenewTime:            &metav1.MicroTime{Time: time.Now()},
					},
				}

				created, err := l.k8sClient.CoordinationV1().Leases(l.namespace).Create(ctx, newLease, metav1.CreateOptions{})
				if err != nil {
					if errors.IsAlreadyExists(err) {
						lastErr = fmt.Errorf("lease %s already exists", l.name)
						return errors.NewConflict(coordinationv1.Resource("leases"), l.name, err)
					}
					return err
				}

				l.leaseMutex.Lock()
				l.lease = created
				l.isHeld = true
				l.lastRenewed = time.Now()
				l.leaseMutex.Unlock()

				l.startRenewal()
				l.metrics.RecordAcquisition(l.key)
				return nil
			}
			return err
		}

		// Check if we already hold it
		if lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity == l.identity {
			// Refresh the lease
			lease.Spec.RenewTime = &metav1.MicroTime{Time: time.Now()}
			updated, err := l.k8sClient.CoordinationV1().Leases(l.namespace).Update(ctx, lease, metav1.UpdateOptions{})
			if err != nil {
				return err
			}

			l.leaseMutex.Lock()
			l.lease = updated
			l.isHeld = true
			l.lastRenewed = time.Now()
			l.leaseMutex.Unlock()

			l.startRenewal()
			return nil
		}

		// Check if lease is expired
		if lease.Spec.RenewTime != nil {
			expiryTime := lease.Spec.RenewTime.Add(time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second)
			if time.Now().After(expiryTime) {
				// Try to take over expired lease
				lease.Spec.HolderIdentity = pointer.String(l.identity)
				lease.Spec.AcquireTime = &metav1.MicroTime{Time: time.Now()}
				lease.Spec.RenewTime = &metav1.MicroTime{Time: time.Now()}
				lease.Spec.LeaseTransitions = pointer.Int32(getLeaseTransitions(lease) + 1)

				updated, err := l.k8sClient.CoordinationV1().Leases(l.namespace).Update(ctx, lease, metav1.UpdateOptions{})
				if err != nil {
					if errors.IsConflict(err) {
						lastErr = fmt.Errorf("conflict acquiring expired lease %s", l.name)
						return err
					}
					return err
				}

				l.leaseMutex.Lock()
				l.lease = updated
				l.isHeld = true
				l.lastRenewed = time.Now()
				l.leaseMutex.Unlock()

				klog.V(2).Infof("Acquired expired lease %s (previous holder: %s)", l.name, *lease.Spec.HolderIdentity)
				l.startRenewal()
				l.metrics.RecordAcquisition(l.key)
				return nil
			}
		}

		// Lease is held by someone else
		holder := "unknown"
		if lease.Spec.HolderIdentity != nil {
			holder = *lease.Spec.HolderIdentity
		}
		lastErr = fmt.Errorf("lease %s is held by %s", l.name, holder)

		// Wait a bit before retrying
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(LeaseTransitionTime):
		}

		return errors.NewConflict(coordinationv1.Resource("leases"), l.name, lastErr)
	})

	if err != nil {
		l.metrics.RecordConflict(l.key)
		return err
	}

	return nil
}

// Release releases the lease lock
func (l *LeaseLock) Release(ctx context.Context) error {
	l.leaseMutex.Lock()
	defer l.leaseMutex.Unlock()

	if !l.isHeld {
		return nil
	}

	// Stop renewal
	if l.renewStop != nil {
		close(l.renewStop)
		l.renewStop = nil
	}

	// Delete the lease
	err := l.k8sClient.CoordinationV1().Leases(l.namespace).Delete(ctx, l.name, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete lease %s: %v", l.name, err)
	}

	l.isHeld = false
	l.lease = nil
	l.metrics.RecordRelease(l.key)

	klog.V(2).Infof("Released lease lock %s", l.name)
	return nil
}

// Refresh manually refreshes the lease
func (l *LeaseLock) Refresh(ctx context.Context) error {
	l.leaseMutex.RLock()
	if !l.isHeld || l.lease == nil {
		l.leaseMutex.RUnlock()
		return fmt.Errorf("lease %s not held", l.name)
	}
	lease := l.lease.DeepCopy()
	l.leaseMutex.RUnlock()

	lease.Spec.RenewTime = &metav1.MicroTime{Time: time.Now()}

	updated, err := l.k8sClient.CoordinationV1().Leases(l.namespace).Update(ctx, lease, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to refresh lease %s: %v", l.name, err)
	}

	l.leaseMutex.Lock()
	l.lease = updated
	l.lastRenewed = time.Now()
	l.leaseMutex.Unlock()

	return nil
}

// IsHeld returns true if the lease is currently held
func (l *LeaseLock) IsHeld() bool {
	l.leaseMutex.RLock()
	defer l.leaseMutex.RUnlock()
	return l.isHeld
}

// GetHolder returns the current lease holder
func (l *LeaseLock) GetHolder() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lease, err := l.k8sClient.CoordinationV1().Leases(l.namespace).Get(ctx, l.name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return "", nil
		}
		return "", err
	}

	if lease.Spec.HolderIdentity != nil {
		return *lease.Spec.HolderIdentity, nil
	}

	return "", nil
}

// startRenewal starts a background goroutine to renew the lease
func (l *LeaseLock) startRenewal() {
	l.leaseMutex.Lock()
	if l.renewStop != nil {
		// Renewal already running
		l.leaseMutex.Unlock()
		return
	}

	l.renewStop = make(chan struct{})
	l.leaseMutex.Unlock()

	go func() {
		ticker := time.NewTicker(time.Duration(LeaseRenewSeconds) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := l.Refresh(ctx); err != nil {
					klog.Errorf("Failed to renew lease %s: %v", l.name, err)
					l.metrics.RecordRenewalFailure(l.key)

					// Mark as not held if renewal fails
					l.leaseMutex.Lock()
					l.isHeld = false
					l.leaseMutex.Unlock()
				}
				cancel()

			case <-l.renewStop:
				return
			}
		}
	}()
}

// getLeaseTransitions safely gets the lease transitions count
func getLeaseTransitions(lease *coordinationv1.Lease) int32 {
	if lease.Spec.LeaseTransitions != nil {
		return *lease.Spec.LeaseTransitions
	}
	return 0
}

// sanitizeLeaseName converts a lock key to a valid Kubernetes lease name
func sanitizeLeaseName(key string) string {
	// Similar to sanitizeConfigMapName but for leases
	result := ""
	for _, ch := range key {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-' {
			result += string(ch)
		} else if ch >= 'A' && ch <= 'Z' {
			result += string(ch + 32)
		} else {
			result += "-"
		}
	}

	// Ensure valid Kubernetes name - remove leading/trailing dashes
	for len(result) > 0 && result[0] == '-' {
		result = result[1:]
	}
	for len(result) > 0 && result[len(result)-1] == '-' {
		result = result[:len(result)-1]
	}

	// Add prefix after cleaning
	result = "efs-lease-" + result

	// Truncate if too long, but preserve the structure
	if len(result) > 63 {
		result = result[:63]
		// Remove trailing dash if truncation created one
		for len(result) > 0 && result[len(result)-1] == '-' {
			result = result[:len(result)-1]
		}
	}

	return result
}

// Stop gracefully stops the distributed lock manager
func (m *DistributedLockManager) Stop(ctx context.Context) error {
	close(m.stopCh)

	// Stop backend-specific components
	switch m.backend {
	case LockBackendConfigMap:
		if m.configMapLock != nil {
			return m.configMapLock.Stop(ctx)
		}

	case LockBackendLease:
		m.leaseMutex.Lock()
		defer m.leaseMutex.Unlock()

		var releaseErrors []error
		for _, lock := range m.leaseLocks {
			if err := lock.Release(ctx); err != nil {
				releaseErrors = append(releaseErrors, err)
			}
		}

		if len(releaseErrors) > 0 {
			return fmt.Errorf("errors releasing lease locks: %v", releaseErrors)
		}
	}

	// Wait for background goroutines
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		klog.Warning("Timeout waiting for distributed lock manager to stop")
	}

	return nil
}