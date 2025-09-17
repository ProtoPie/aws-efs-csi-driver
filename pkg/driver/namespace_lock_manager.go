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
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
)

const (
	// Lock types
	LockTypeNamespace = "namespace"
	LockTypeEFS       = "efs"

	// Default configuration
	DefaultLockTimeout       = 30 * time.Second
	DefaultLockRenewalPeriod = 10 * time.Second
	DefaultLockMaxRetries    = 3
	DefaultBackoffBase       = 100 * time.Millisecond
	DefaultBackoffMax        = 10 * time.Second
	DefaultJitterFactor      = 0.2

	// ConfigMap/Lease metadata
	LockAnnotationPrefix     = "efs-csi-driver.kubernetes.io/"
	LockHolderAnnotation     = LockAnnotationPrefix + "holder"
	LockTimestampAnnotation  = LockAnnotationPrefix + "timestamp"
	LockTypeAnnotation       = LockAnnotationPrefix + "type"
	LockNamespaceAnnotation  = LockAnnotationPrefix + "namespace"
	LockDeadlockAnnotation   = LockAnnotationPrefix + "deadlock-detection"

	// Lock states
	LockStateAcquired  = "acquired"
	LockStateReleased  = "released"
	LockStateTimeout   = "timeout"
	LockStateDeadlock  = "deadlock"
)

// LockInfo contains information about a distributed lock
type LockInfo struct {
	Holder    string    `json:"holder"`
	Timestamp time.Time `json:"timestamp"`
	Type      string    `json:"type"`
	Namespace string    `json:"namespace,omitempty"`
	ExpiresAt time.Time `json:"expires_at"`
}

// NamespaceLockManager manages distributed locks for namespace and EFS operations
type NamespaceLockManager struct {
	k8sClient   kubernetes.Interface
	namespace   string // Kubernetes namespace where ConfigMaps/Leases are stored
	localLocks  *LockManagerMap
	lockTimeout time.Duration
	renewPeriod time.Duration
	maxRetries  int
	identity    string // Unique identifier for this instance

	// Backoff configuration
	backoffBase   time.Duration
	backoffMax    time.Duration
	jitterFactor  float64

	// Active locks tracking
	activeLocks sync.Map // map[string]*activelock
	stopCh      chan struct{}
	wg          sync.WaitGroup

	// Metrics
	metricsLock     sync.RWMutex
	lockAcquisitions int64
	lockReleases    int64
	lockTimeouts    int64
	lockConflicts   int64
}

// activeLock tracks an active distributed lock with renewal
type activeLock struct {
	key       string
	lockType  string
	holder    string
	expiresAt time.Time
	renewCh   chan struct{}
	doneCh    chan struct{}
}

// NamespaceLockManagerOptions contains configuration for the lock manager
type NamespaceLockManagerOptions struct {
	LockTimeout      time.Duration
	RenewalPeriod    time.Duration
	MaxRetries       int
	BackoffBase      time.Duration
	BackoffMax       time.Duration
	JitterFactor     float64
	LockNamespace    string
}

// NewNamespaceLockManager creates a new distributed lock manager
func NewNamespaceLockManager(k8sClient kubernetes.Interface, identity string, options *NamespaceLockManagerOptions) *NamespaceLockManager {
	if options == nil {
		options = &NamespaceLockManagerOptions{}
	}

	// Apply defaults
	if options.LockTimeout == 0 {
		options.LockTimeout = DefaultLockTimeout
	}
	if options.RenewalPeriod == 0 {
		options.RenewalPeriod = DefaultLockRenewalPeriod
	}
	if options.MaxRetries == 0 {
		options.MaxRetries = DefaultLockMaxRetries
	}
	if options.BackoffBase == 0 {
		options.BackoffBase = DefaultBackoffBase
	}
	if options.BackoffMax == 0 {
		options.BackoffMax = DefaultBackoffMax
	}
	if options.JitterFactor == 0 {
		options.JitterFactor = DefaultJitterFactor
	}
	if options.LockNamespace == "" {
		options.LockNamespace = "kube-system"
	}

	manager := &NamespaceLockManager{
		k8sClient:    k8sClient,
		namespace:    options.LockNamespace,
		localLocks:   &LockManagerMap{locks: sync.Map{}},
		lockTimeout:  options.LockTimeout,
		renewPeriod:  options.RenewalPeriod,
		maxRetries:   options.MaxRetries,
		identity:     identity,
		backoffBase:  options.BackoffBase,
		backoffMax:   options.BackoffMax,
		jitterFactor: options.JitterFactor,
		stopCh:       make(chan struct{}),
	}

	// Start background lock renewal and cleanup
	manager.wg.Add(1)
	go manager.backgroundMaintenance()

	return manager
}

// AcquireNamespaceLock acquires a distributed lock for namespace operations
func (m *NamespaceLockManager) AcquireNamespaceLock(ctx context.Context, namespace string, timeout time.Duration) error {
	lockKey := fmt.Sprintf("%s:%s", LockTypeNamespace, namespace)
	return m.acquireDistributedLock(ctx, lockKey, LockTypeNamespace, namespace, timeout)
}

// ReleaseNamespaceLock releases a namespace lock
func (m *NamespaceLockManager) ReleaseNamespaceLock(ctx context.Context, namespace string) error {
	lockKey := fmt.Sprintf("%s:%s", LockTypeNamespace, namespace)
	return m.releaseDistributedLock(ctx, lockKey)
}

// AcquireEFSLock acquires a distributed lock for EFS operations
func (m *NamespaceLockManager) AcquireEFSLock(ctx context.Context, efsId string, timeout time.Duration) error {
	lockKey := fmt.Sprintf("%s:%s", LockTypeEFS, efsId)
	return m.acquireDistributedLock(ctx, lockKey, LockTypeEFS, "", timeout)
}

// ReleaseEFSLock releases an EFS lock
func (m *NamespaceLockManager) ReleaseEFSLock(ctx context.Context, efsId string) error {
	lockKey := fmt.Sprintf("%s:%s", LockTypeEFS, efsId)
	return m.releaseDistributedLock(ctx, lockKey)
}

// acquireDistributedLock acquires a distributed lock using ConfigMap
func (m *NamespaceLockManager) acquireDistributedLock(ctx context.Context, key string, lockType string, namespace string, timeout time.Duration) error {
	// First acquire local lock to prevent multiple goroutines from same instance
	if !m.localLocks.lockMutex(key, timeout) {
		return fmt.Errorf("failed to acquire local lock for %s", key)
	}

	configMapName := m.getLockConfigMapName(key)

	// Use exponential backoff with jitter for retries
	backoff := retry.DefaultBackoff
	backoff.Duration = m.backoffBase
	backoff.Cap = m.backoffMax
	backoff.Steps = m.maxRetries

	var lastErr error
	err := retry.OnError(backoff, errors.IsConflict, func() error {
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Try to acquire the distributed lock
		cm, err := m.k8sClient.CoreV1().ConfigMaps(m.namespace).Get(ctx, configMapName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				// Create new ConfigMap for the lock
				lockInfo := &LockInfo{
					Holder:    m.identity,
					Timestamp: time.Now(),
					Type:      lockType,
					Namespace: namespace,
					ExpiresAt: time.Now().Add(m.lockTimeout),
				}

				data, _ := json.Marshal(lockInfo)
				newCM := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      configMapName,
						Namespace: m.namespace,
						Annotations: map[string]string{
							LockHolderAnnotation:    m.identity,
							LockTimestampAnnotation: lockInfo.Timestamp.Format(time.RFC3339),
							LockTypeAnnotation:      lockType,
						},
					},
					Data: map[string]string{
						"lock": string(data),
					},
				}

				if namespace != "" {
					newCM.Annotations[LockNamespaceAnnotation] = namespace
				}

				_, err = m.k8sClient.CoreV1().ConfigMaps(m.namespace).Create(ctx, newCM, metav1.CreateOptions{})
				if err != nil {
					if errors.IsAlreadyExists(err) {
						// Someone else created it, retry
						lastErr = fmt.Errorf("lock %s is held by another process", key)
						return errors.NewConflict(corev1.Resource("configmaps"), configMapName, err)
					}
					return err
				}

				// Successfully acquired lock, start renewal
				m.startLockRenewal(key, lockType, configMapName)
				return nil
			}
			return err
		}

		// ConfigMap exists, check if lock is expired or held by us
		var lockInfo LockInfo
		if lockData, ok := cm.Data["lock"]; ok {
			if err := json.Unmarshal([]byte(lockData), &lockInfo); err != nil {
				klog.Warningf("Failed to unmarshal lock data for %s: %v", key, err)
			}
		}

		// Check if we already hold the lock
		if lockInfo.Holder == m.identity {
			// Update expiration time
			lockInfo.ExpiresAt = time.Now().Add(m.lockTimeout)
			data, _ := json.Marshal(lockInfo)
			cm.Data["lock"] = string(data)
			_, err = m.k8sClient.CoreV1().ConfigMaps(m.namespace).Update(ctx, cm, metav1.UpdateOptions{})
			if err != nil {
				return err
			}
			m.startLockRenewal(key, lockType, configMapName)
			return nil
		}

		// Check if lock is expired
		if time.Now().After(lockInfo.ExpiresAt) {
			// Try to acquire expired lock
			lockInfo = LockInfo{
				Holder:    m.identity,
				Timestamp: time.Now(),
				Type:      lockType,
				Namespace: namespace,
				ExpiresAt: time.Now().Add(m.lockTimeout),
			}

			data, _ := json.Marshal(lockInfo)
			// Initialize data map if nil
			if cm.Data == nil {
				cm.Data = make(map[string]string)
			}
			cm.Data["lock"] = string(data)
			// Initialize annotations map if nil
			if cm.Annotations == nil {
				cm.Annotations = make(map[string]string)
			}
			cm.Annotations[LockHolderAnnotation] = m.identity
			cm.Annotations[LockTimestampAnnotation] = lockInfo.Timestamp.Format(time.RFC3339)

			_, err = m.k8sClient.CoreV1().ConfigMaps(m.namespace).Update(ctx, cm, metav1.UpdateOptions{})
			if err != nil {
				if errors.IsConflict(err) {
					lastErr = fmt.Errorf("lock %s conflict during acquisition", key)
					return err
				}
				return err
			}

			klog.V(2).Infof("Acquired expired lock %s (previous holder: %s)", key, lockInfo.Holder)
			m.startLockRenewal(key, lockType, configMapName)
			return nil
		}

		// Lock is held by someone else
		lastErr = fmt.Errorf("lock %s is held by %s until %s", key, lockInfo.Holder, lockInfo.ExpiresAt.Format(time.RFC3339))

		// Add jitter to backoff to prevent thundering herd
		jitter := time.Duration(float64(m.backoffBase) * m.jitterFactor * rand.Float64())
		time.Sleep(jitter)

		return errors.NewConflict(corev1.Resource("configmaps"), configMapName, lastErr)
	})

	if err != nil {
		// Release local lock if we failed to acquire distributed lock
		m.localLocks.unlockMutex(key)
		if lastErr != nil {
			return lastErr
		}
		return err
	}

	// Update metrics
	m.metricsLock.Lock()
	m.lockAcquisitions++
	m.metricsLock.Unlock()

	return nil
}

// releaseDistributedLock releases a distributed lock
func (m *NamespaceLockManager) releaseDistributedLock(ctx context.Context, key string) error {
	// Check if we have a local lock first
	if _, ok := m.localLocks.locks.Load(key); ok {
		defer m.localLocks.unlockMutex(key)
	}

	// Stop renewal if active
	if val, ok := m.activeLocks.Load(key); ok {
		lock := val.(*activeLock)
		if lock.doneCh != nil {
			close(lock.doneCh)
		}
		m.activeLocks.Delete(key)
	}

	configMapName := m.getLockConfigMapName(key)

	// Get the ConfigMap to verify we hold the lock
	cm, err := m.k8sClient.CoreV1().ConfigMaps(m.namespace).Get(ctx, configMapName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			// Lock doesn't exist, nothing to release
			return nil
		}
		return fmt.Errorf("failed to get lock ConfigMap %s: %v", configMapName, err)
	}

	// Verify we hold the lock
	var lockInfo LockInfo
	if lockData, ok := cm.Data["lock"]; ok {
		if err := json.Unmarshal([]byte(lockData), &lockInfo); err != nil {
			return fmt.Errorf("failed to unmarshal lock data: %v", err)
		}
	}

	if lockInfo.Holder != m.identity {
		return fmt.Errorf("cannot release lock %s held by %s", key, lockInfo.Holder)
	}

	// Delete the ConfigMap to release the lock
	err = m.k8sClient.CoreV1().ConfigMaps(m.namespace).Delete(ctx, configMapName, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete lock ConfigMap %s: %v", configMapName, err)
	}

	// Update metrics
	m.metricsLock.Lock()
	m.lockReleases++
	m.metricsLock.Unlock()

	klog.V(2).Infof("Released lock %s", key)
	return nil
}

// startLockRenewal starts a background goroutine to renew the lock
func (m *NamespaceLockManager) startLockRenewal(key string, lockType string, configMapName string) {
	// Check if renewal is already active
	if _, ok := m.activeLocks.Load(key); ok {
		return
	}

	lock := &activeLock{
		key:       key,
		lockType:  lockType,
		holder:    m.identity,
		expiresAt: time.Now().Add(m.lockTimeout),
		renewCh:   make(chan struct{}),
		doneCh:    make(chan struct{}),
	}

	m.activeLocks.Store(key, lock)

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(m.renewPeriod)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := m.renewLock(configMapName, lock); err != nil {
					klog.Errorf("Failed to renew lock %s: %v", key, err)
					// If renewal fails, stop trying
					m.activeLocks.Delete(key)
					return
				}
			case <-lock.doneCh:
				return
			case <-m.stopCh:
				return
			}
		}
	}()
}

// renewLock renews a distributed lock
func (m *NamespaceLockManager) renewLock(configMapName string, lock *activeLock) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cm, err := m.k8sClient.CoreV1().ConfigMaps(m.namespace).Get(ctx, configMapName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	var lockInfo LockInfo
	if lockData, ok := cm.Data["lock"]; ok {
		if err := json.Unmarshal([]byte(lockData), &lockInfo); err != nil {
			return err
		}
	}

	// Verify we still hold the lock
	if lockInfo.Holder != m.identity {
		return fmt.Errorf("lock %s is now held by %s", lock.key, lockInfo.Holder)
	}

	// Update expiration time
	lockInfo.ExpiresAt = time.Now().Add(m.lockTimeout)
	lock.expiresAt = lockInfo.ExpiresAt

	data, _ := json.Marshal(lockInfo)
	cm.Data["lock"] = string(data)

	_, err = m.k8sClient.CoreV1().ConfigMaps(m.namespace).Update(ctx, cm, metav1.UpdateOptions{})
	return err
}

// backgroundMaintenance performs periodic cleanup and deadlock detection
func (m *NamespaceLockManager) backgroundMaintenance() {
	defer m.wg.Done()
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanupExpiredLocks()
			m.detectDeadlocks()
		case <-m.stopCh:
			return
		}
	}
}

// cleanupExpiredLocks removes expired lock ConfigMaps
func (m *NamespaceLockManager) cleanupExpiredLocks() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// List all ConfigMaps in the namespace that might be locks
	// We identify them by having the "lock" data key
	cmList, err := m.k8sClient.CoreV1().ConfigMaps(m.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Failed to list lock ConfigMaps: %v", err)
		return
	}

	for _, cm := range cmList.Items {
		// Check if this is a lock ConfigMap (must have "lock" data key)
		lockData, ok := cm.Data["lock"]
		if !ok {
			// Not a lock ConfigMap, skip
			continue
		}

		var lockInfo LockInfo
		if err := json.Unmarshal([]byte(lockData), &lockInfo); err != nil {
			klog.V(4).Infof("Failed to unmarshal lock data for ConfigMap %s: %v", cm.Name, err)
			continue
		}

		// Check if lock is expired and not held by us
		if time.Now().After(lockInfo.ExpiresAt) && lockInfo.Holder != m.identity {
			// Delete expired lock
			err := m.k8sClient.CoreV1().ConfigMaps(m.namespace).Delete(ctx, cm.Name, metav1.DeleteOptions{})
			if err != nil && !errors.IsNotFound(err) {
				klog.Errorf("Failed to delete expired lock %s: %v", cm.Name, err)
			} else {
				klog.V(2).Infof("Cleaned up expired lock %s (holder: %s)", cm.Name, lockInfo.Holder)
			}
		}
	}
}

// detectDeadlocks detects potential deadlock situations
func (m *NamespaceLockManager) detectDeadlocks() {
	// Simple deadlock detection: check if multiple locks are held for too long
	var activeLockCount int
	var oldestLock *activeLock
	var oldestAge time.Duration

	m.activeLocks.Range(func(key, value interface{}) bool {
		lock := value.(*activeLock)
		activeLockCount++

		age := time.Since(lock.expiresAt.Add(-m.lockTimeout))
		if oldestLock == nil || age > oldestAge {
			oldestLock = lock
			oldestAge = age
		}
		return true
	})

	// If we have multiple locks held for more than 5 minutes, log a warning
	if activeLockCount > 5 && oldestAge > 5*time.Minute {
		klog.Warningf("Potential deadlock detected: %d locks held, oldest lock %s held for %v",
			activeLockCount, oldestLock.key, oldestAge)
	}
}

// getLockConfigMapName generates a ConfigMap name for the lock
func (m *NamespaceLockManager) getLockConfigMapName(key string) string {
	// Replace colons with dashes for valid Kubernetes names
	safeName := fmt.Sprintf("efs-lock-%s", key)
	safeName = sanitizeConfigMapName(safeName)
	return safeName
}

// sanitizeConfigMapName ensures the name is valid for Kubernetes
func sanitizeConfigMapName(name string) string {
	// Replace invalid characters with dashes
	result := ""
	for _, ch := range name {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-' {
			result += string(ch)
		} else if ch >= 'A' && ch <= 'Z' {
			// Convert to lowercase
			result += string(ch + 32)
		} else {
			result += "-"
		}
	}

	// Ensure it doesn't start or end with dash
	for len(result) > 0 && result[0] == '-' {
		result = result[1:]
	}
	for len(result) > 0 && result[len(result)-1] == '-' {
		result = result[:len(result)-1]
	}

	// Limit length to 63 characters (Kubernetes limit)
	if len(result) > 63 {
		result = result[:63]
	}

	return result
}

// CalculateBackoff calculates exponential backoff with jitter
func (m *NamespaceLockManager) CalculateBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		return m.backoffBase
	}

	// Exponential backoff: base * 2^attempt
	backoff := m.backoffBase * time.Duration(math.Pow(2, float64(attempt)))

	// Cap at maximum
	if backoff > m.backoffMax {
		backoff = m.backoffMax
	}

	// Add jitter (±jitterFactor%)
	jitterRange := float64(backoff) * m.jitterFactor
	jitter := time.Duration((rand.Float64() - 0.5) * 2 * jitterRange)

	finalBackoff := backoff + jitter
	if finalBackoff < 0 {
		finalBackoff = m.backoffBase
	}

	return finalBackoff
}

// GetMetrics returns current lock manager metrics
func (m *NamespaceLockManager) GetMetrics() map[string]int64 {
	m.metricsLock.RLock()
	defer m.metricsLock.RUnlock()

	activeLockCount := 0
	m.activeLocks.Range(func(key, value interface{}) bool {
		activeLockCount++
		return true
	})

	return map[string]int64{
		"acquisitions":  m.lockAcquisitions,
		"releases":      m.lockReleases,
		"timeouts":      m.lockTimeouts,
		"conflicts":     m.lockConflicts,
		"active_locks":  int64(activeLockCount),
	}
}

// Stop stops the lock manager and releases all locks
func (m *NamespaceLockManager) Stop(ctx context.Context) error {
	close(m.stopCh)

	// Release all active locks
	var releaseErrors []error
	m.activeLocks.Range(func(key, value interface{}) bool {
		lock := value.(*activeLock)
		if err := m.releaseDistributedLock(ctx, lock.key); err != nil {
			releaseErrors = append(releaseErrors, fmt.Errorf("failed to release lock %s: %v", lock.key, err))
		}
		return true
	})

	// Wait for background goroutines to finish
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines finished
	case <-time.After(10 * time.Second):
		klog.Warning("Timeout waiting for lock manager goroutines to finish")
	}

	if len(releaseErrors) > 0 {
		return fmt.Errorf("errors releasing locks: %v", releaseErrors)
	}

	return nil
}