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

package testenv

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"k8s.io/klog/v2"
)

// FailureSimulator provides methods to simulate various failure scenarios
type FailureSimulator struct {
	mu                 sync.RWMutex
	timeoutConfigs     map[string]*TimeoutConfig
	partialFailures    map[string]*PartialFailureConfig
	cascadingFailures  []CascadingFailureRule
	crdFailureMode     CRDFailureMode
	rollbackEnabled    bool
	cleanupTracking    bool
	failureHistory     []FailureEvent
	recoveryCallbacks  []RecoveryCallback
	operationDelays    map[string]time.Duration
	errorInjections    map[string]error
	resourceStates     map[string]ResourceState
}

// TimeoutConfig defines timeout simulation parameters
type TimeoutConfig struct {
	Operation      string
	InitialTimeout time.Duration
	RecoveryTime   time.Duration
	MaxRetries     int
	FailureRate    float64 // 0.0 to 1.0
	currentRetries atomic.Int32
	isRecovered    atomic.Bool
}

// PartialFailureConfig defines partial failure parameters
type PartialFailureConfig struct {
	Operation       string
	FailurePattern  string
	AffectedPercent float64
	Recoverable     bool
	RecoveryDelay   time.Duration
}

// CascadingFailureRule defines cascading failure behavior
type CascadingFailureRule struct {
	TriggerOperation   string
	AffectedOperations []string
	PropagationDelay   time.Duration
	RecoveryOrder      []string
}

// CRDFailureMode defines CRD failure scenarios
type CRDFailureMode int

const (
	CRDFailureNone CRDFailureMode = iota
	CRDFailureDelete
	CRDFailureCorrupt
	CRDFailureOutOfSync
	CRDFailureRandom
)

// FailureEvent records a failure occurrence
type FailureEvent struct {
	Timestamp   time.Time
	Operation   string
	Error       error
	Recovered   bool
	RecoveryTime time.Duration
}

// RecoveryCallback is called when recovery occurs
type RecoveryCallback func(operation string, duration time.Duration)

// ResourceState tracks resource state during failures
type ResourceState struct {
	ResourceID   string
	ResourceType string
	State        string
	LastModified time.Time
	IsOrphaned   bool
}

// NewFailureSimulator creates a new failure simulator
func NewFailureSimulator() *FailureSimulator {
	return &FailureSimulator{
		timeoutConfigs:    make(map[string]*TimeoutConfig),
		partialFailures:   make(map[string]*PartialFailureConfig),
		cascadingFailures: []CascadingFailureRule{},
		operationDelays:   make(map[string]time.Duration),
		errorInjections:   make(map[string]error),
		resourceStates:    make(map[string]ResourceState),
		failureHistory:    []FailureEvent{},
	}
}

// SimulateTimeout simulates a timeout for an operation
func (fs *FailureSimulator) SimulateTimeout(ctx context.Context, operation string) error {
	fs.mu.RLock()
	config, exists := fs.timeoutConfigs[operation]
	fs.mu.RUnlock()

	if !exists {
		return nil // No timeout configured
	}

	// Check if we should fail based on failure rate
	if rand.Float64() > config.FailureRate {
		return nil // Operation succeeds
	}

	retries := config.currentRetries.Add(1)

	// Check if recovered
	if config.isRecovered.Load() {
		klog.V(4).Infof("Operation %s recovered after timeout", operation)
		return nil
	}

	// Check if within recovery time
	if retries > int32(config.MaxRetries/2) {
		// Start recovery process
		go fs.startRecovery(config, operation)
	}

	// Simulate timeout
	select {
	case <-time.After(config.InitialTimeout):
		err := fmt.Errorf("operation %s timed out (attempt %d)", operation, retries)
		fs.recordFailure(operation, err, false)
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SimulatePartialFailure simulates a partial failure scenario
func (fs *FailureSimulator) SimulatePartialFailure(operation string, resources []string) ([]string, []string, error) {
	fs.mu.RLock()
	config, exists := fs.partialFailures[operation]
	fs.mu.RUnlock()

	if !exists {
		// No partial failure configured, all succeed
		return resources, []string{}, nil
	}

	succeeded := []string{}
	failed := []string{}

	for _, resource := range resources {
		// Determine if this resource should fail
		shouldFail := false

		if config.FailurePattern != "" {
			// Pattern-based failure
			shouldFail = resource == config.FailurePattern
		} else {
			// Random failure based on percentage
			shouldFail = rand.Float64() < config.AffectedPercent
		}

		if shouldFail {
			failed = append(failed, resource)
			klog.V(4).Infof("Resource %s failed for operation %s", resource, operation)
		} else {
			succeeded = append(succeeded, resource)
		}
	}

	// Handle recovery if configured
	if config.Recoverable && len(failed) > 0 {
		go fs.schedulePartialRecovery(operation, failed, config.RecoveryDelay)
	}

	if len(failed) > 0 {
		err := fmt.Errorf("partial failure: %d of %d resources failed", len(failed), len(resources))
		fs.recordFailure(operation, err, config.Recoverable)
		return succeeded, failed, err
	}

	return succeeded, failed, nil
}

// SimulateCascadingFailure simulates cascading failures
func (fs *FailureSimulator) SimulateCascadingFailure(triggerOp string) error {
	fs.mu.RLock()
	rules := fs.cascadingFailures
	fs.mu.RUnlock()

	for _, rule := range rules {
		if rule.TriggerOperation == triggerOp {
			klog.V(3).Infof("Triggering cascading failure from %s", triggerOp)

			// Propagate failure to affected operations
			go fs.propagateFailures(rule)

			return fmt.Errorf("cascading failure initiated from %s", triggerOp)
		}
	}

	return nil
}

// SimulateCRDFailure simulates CRD-related failures
func (fs *FailureSimulator) SimulateCRDFailure(namespace string) error {
	fs.mu.RLock()
	mode := fs.crdFailureMode
	fs.mu.RUnlock()

	switch mode {
	case CRDFailureDelete:
		return fs.simulateCRDDeletion(namespace)
	case CRDFailureCorrupt:
		return fs.simulateCRDCorruption(namespace)
	case CRDFailureOutOfSync:
		return fs.simulateCRDOutOfSync(namespace)
	case CRDFailureRandom:
		return fs.simulateRandomCRDFailure(namespace)
	default:
		return nil
	}
}

// InjectError injects a specific error for an operation
func (fs *FailureSimulator) InjectError(operation string, err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if err == nil {
		delete(fs.errorInjections, operation)
	} else {
		fs.errorInjections[operation] = err
	}
}

// GetInjectedError returns any injected error for an operation
func (fs *FailureSimulator) GetInjectedError(operation string) error {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	return fs.errorInjections[operation]
}

// SetOperationDelay sets a delay for an operation
func (fs *FailureSimulator) SetOperationDelay(operation string, delay time.Duration) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if delay == 0 {
		delete(fs.operationDelays, operation)
	} else {
		fs.operationDelays[operation] = delay
	}
}

// ApplyOperationDelay applies any configured delay for an operation
func (fs *FailureSimulator) ApplyOperationDelay(operation string) {
	fs.mu.RLock()
	delay, exists := fs.operationDelays[operation]
	fs.mu.RUnlock()

	if exists && delay > 0 {
		klog.V(4).Infof("Applying delay of %v for operation %s", delay, operation)
		time.Sleep(delay)
	}
}

// EnableRollback enables rollback tracking
func (fs *FailureSimulator) EnableRollback(enabled bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.rollbackEnabled = enabled
}

// EnableCleanupTracking enables cleanup tracking
func (fs *FailureSimulator) EnableCleanupTracking(enabled bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.cleanupTracking = enabled
}

// TrackResourceState tracks the state of a resource
func (fs *FailureSimulator) TrackResourceState(resourceID, resourceType, state string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.resourceStates[resourceID] = ResourceState{
		ResourceID:   resourceID,
		ResourceType: resourceType,
		State:        state,
		LastModified: time.Now(),
		IsOrphaned:   false,
	}
}

// MarkResourceOrphaned marks a resource as orphaned
func (fs *FailureSimulator) MarkResourceOrphaned(resourceID string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if state, exists := fs.resourceStates[resourceID]; exists {
		state.IsOrphaned = true
		state.LastModified = time.Now()
		fs.resourceStates[resourceID] = state
	}
}

// GetOrphanedResources returns all orphaned resources
func (fs *FailureSimulator) GetOrphanedResources() []ResourceState {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	orphaned := []ResourceState{}
	for _, state := range fs.resourceStates {
		if state.IsOrphaned {
			orphaned = append(orphaned, state)
		}
	}
	return orphaned
}

// AddRecoveryCallback adds a recovery callback
func (fs *FailureSimulator) AddRecoveryCallback(callback RecoveryCallback) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.recoveryCallbacks = append(fs.recoveryCallbacks, callback)
}

// GetFailureHistory returns the failure history
func (fs *FailureSimulator) GetFailureHistory() []FailureEvent {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	history := make([]FailureEvent, len(fs.failureHistory))
	copy(history, fs.failureHistory)
	return history
}

// Reset resets all failure configurations
func (fs *FailureSimulator) Reset() {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.timeoutConfigs = make(map[string]*TimeoutConfig)
	fs.partialFailures = make(map[string]*PartialFailureConfig)
	fs.cascadingFailures = []CascadingFailureRule{}
	fs.crdFailureMode = CRDFailureNone
	fs.operationDelays = make(map[string]time.Duration)
	fs.errorInjections = make(map[string]error)
	fs.resourceStates = make(map[string]ResourceState)
	fs.failureHistory = []FailureEvent{}
}

// AddTimeoutConfig adds a timeout configuration
func (fs *FailureSimulator) AddTimeoutConfig(config *TimeoutConfig) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.timeoutConfigs[config.Operation] = config
}

// AddPartialFailureConfig adds a partial failure configuration
func (fs *FailureSimulator) AddPartialFailureConfig(config *PartialFailureConfig) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.partialFailures[config.Operation] = config
}

// AddCascadingFailureRule adds a cascading failure rule
func (fs *FailureSimulator) AddCascadingFailureRule(rule CascadingFailureRule) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.cascadingFailures = append(fs.cascadingFailures, rule)
}

// SetCRDFailureMode sets the CRD failure mode
func (fs *FailureSimulator) SetCRDFailureMode(mode CRDFailureMode) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.crdFailureMode = mode
}

// Private helper methods

func (fs *FailureSimulator) startRecovery(config *TimeoutConfig, operation string) {
	time.Sleep(config.RecoveryTime)
	config.isRecovered.Store(true)

	klog.V(3).Infof("Operation %s recovered after %v", operation, config.RecoveryTime)

	fs.mu.RLock()
	callbacks := fs.recoveryCallbacks
	fs.mu.RUnlock()

	for _, cb := range callbacks {
		cb(operation, config.RecoveryTime)
	}

	// Update failure history
	fs.recordFailure(operation, nil, true)
}

func (fs *FailureSimulator) schedulePartialRecovery(operation string, failed []string, delay time.Duration) {
	time.Sleep(delay)

	klog.V(3).Infof("Recovering %d failed resources for operation %s", len(failed), operation)

	fs.mu.RLock()
	callbacks := fs.recoveryCallbacks
	fs.mu.RUnlock()

	for _, cb := range callbacks {
		cb(operation, delay)
	}
}

func (fs *FailureSimulator) propagateFailures(rule CascadingFailureRule) {
	time.Sleep(rule.PropagationDelay)

	for _, affectedOp := range rule.AffectedOperations {
		fs.InjectError(affectedOp, fmt.Errorf("cascading failure from %s", rule.TriggerOperation))
		klog.V(3).Infof("Propagated failure to operation %s", affectedOp)
	}

	// Schedule recovery if defined
	if len(rule.RecoveryOrder) > 0 {
		go fs.scheduleRecoverySequence(rule.RecoveryOrder)
	}
}

func (fs *FailureSimulator) scheduleRecoverySequence(operations []string) {
	for _, op := range operations {
		time.Sleep(2 * time.Second) // Recovery delay between operations
		fs.InjectError(op, nil) // Clear the error
		klog.V(3).Infof("Recovered operation %s in sequence", op)
	}
}

func (fs *FailureSimulator) simulateCRDDeletion(namespace string) error {
	klog.V(3).Infof("Simulating CRD deletion for namespace %s", namespace)
	return errors.New("CRD deleted")
}

func (fs *FailureSimulator) simulateCRDCorruption(namespace string) error {
	klog.V(3).Infof("Simulating CRD corruption for namespace %s", namespace)
	return errors.New("CRD data corrupted")
}

func (fs *FailureSimulator) simulateCRDOutOfSync(namespace string) error {
	klog.V(3).Infof("Simulating CRD out of sync for namespace %s", namespace)
	return errors.New("CRD out of sync with AWS state")
}

func (fs *FailureSimulator) simulateRandomCRDFailure(namespace string) error {
	failures := []func(string) error{
		fs.simulateCRDDeletion,
		fs.simulateCRDCorruption,
		fs.simulateCRDOutOfSync,
	}

	// Pick a random failure
	failure := failures[rand.Intn(len(failures))]
	return failure(namespace)
}

func (fs *FailureSimulator) recordFailure(operation string, err error, recovered bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	event := FailureEvent{
		Timestamp: time.Now(),
		Operation: operation,
		Error:     err,
		Recovered: recovered,
	}

	fs.failureHistory = append(fs.failureHistory, event)
}

// FailureInjector provides methods to inject failures into AWS operations
type FailureInjector struct {
	simulator *FailureSimulator
	mu        sync.RWMutex
	enabled   bool
}

// NewFailureInjector creates a new failure injector
func NewFailureInjector(simulator *FailureSimulator) *FailureInjector {
	return &FailureInjector{
		simulator: simulator,
		enabled:   true,
	}
}

// Enable enables or disables failure injection
func (fi *FailureInjector) Enable(enabled bool) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	fi.enabled = enabled
}

// ShouldInjectFailure checks if a failure should be injected
func (fi *FailureInjector) ShouldInjectFailure(operation string) (bool, error) {
	fi.mu.RLock()
	defer fi.mu.RUnlock()

	if !fi.enabled {
		return false, nil
	}

	// Check for injected error
	if err := fi.simulator.GetInjectedError(operation); err != nil {
		return true, err
	}

	return false, nil
}

// WrapEFSClient wraps an EFS client with failure injection
func (fi *FailureInjector) WrapEFSClient(client *efs.Client) *FailureInjectingEFSClient {
	return &FailureInjectingEFSClient{
		Client:   client,
		injector: fi,
	}
}

// FailureInjectingEFSClient wraps EFS client with failure injection
type FailureInjectingEFSClient struct {
	*efs.Client
	injector *FailureInjector
}

// CreateFileSystem with failure injection
func (c *FailureInjectingEFSClient) CreateFileSystem(ctx context.Context, params *efs.CreateFileSystemInput, optFns ...func(*efs.Options)) (*efs.CreateFileSystemOutput, error) {
	// Apply operation delay
	c.injector.simulator.ApplyOperationDelay("CreateFileSystem")

	// Check for injected failure
	if should, err := c.injector.ShouldInjectFailure("CreateFileSystem"); should {
		return nil, err
	}

	// Check for timeout simulation
	if err := c.injector.simulator.SimulateTimeout(ctx, "CreateFileSystem"); err != nil {
		return nil, err
	}

	// Call actual operation
	output, err := c.Client.CreateFileSystem(ctx, params, optFns...)

	if err == nil && output.FileSystemId != nil {
		// Track resource state
		c.injector.simulator.TrackResourceState(*output.FileSystemId, "FileSystem", string(output.LifeCycleState))
	}

	return output, err
}

// CreateMountTarget with failure injection
func (c *FailureInjectingEFSClient) CreateMountTarget(ctx context.Context, params *efs.CreateMountTargetInput, optFns ...func(*efs.Options)) (*efs.CreateMountTargetOutput, error) {
	// Apply operation delay
	c.injector.simulator.ApplyOperationDelay("CreateMountTarget")

	// Check for injected failure
	if should, err := c.injector.ShouldInjectFailure("CreateMountTarget"); should {
		return nil, err
	}

	// Check for timeout simulation
	if err := c.injector.simulator.SimulateTimeout(ctx, "CreateMountTarget"); err != nil {
		return nil, err
	}

	// Call actual operation
	output, err := c.Client.CreateMountTarget(ctx, params, optFns...)

	if err == nil && output.MountTargetId != nil {
		// Track resource state
		c.injector.simulator.TrackResourceState(*output.MountTargetId, "MountTarget", string(output.LifeCycleState))
	}

	return output, err
}

// CreateAccessPoint with failure injection
func (c *FailureInjectingEFSClient) CreateAccessPoint(ctx context.Context, params *efs.CreateAccessPointInput, optFns ...func(*efs.Options)) (*efs.CreateAccessPointOutput, error) {
	// Apply operation delay
	c.injector.simulator.ApplyOperationDelay("CreateAccessPoint")

	// Check for injected failure
	if should, err := c.injector.ShouldInjectFailure("CreateAccessPoint"); should {
		return nil, err
	}

	// Check for timeout simulation
	if err := c.injector.simulator.SimulateTimeout(ctx, "CreateAccessPoint"); err != nil {
		return nil, err
	}

	// Call actual operation
	output, err := c.Client.CreateAccessPoint(ctx, params, optFns...)

	if err == nil && output.AccessPointId != nil {
		// Track resource state
		c.injector.simulator.TrackResourceState(*output.AccessPointId, "AccessPoint", string(output.LifeCycleState))
	}

	return output, err
}

// RecoveryTester provides methods to test recovery mechanisms
type RecoveryTester struct {
	simulator  *FailureSimulator
	metrics    RecoveryMetrics
	mu         sync.Mutex
}

// RecoveryMetrics tracks recovery statistics
type RecoveryMetrics struct {
	TotalFailures      int
	SuccessfulRecovery int
	FailedRecovery     int
	AverageRecoveryTime time.Duration
	MaxRecoveryTime    time.Duration
	MinRecoveryTime    time.Duration
}

// NewRecoveryTester creates a new recovery tester
func NewRecoveryTester(simulator *FailureSimulator) *RecoveryTester {
	return &RecoveryTester{
		simulator: simulator,
		metrics:   RecoveryMetrics{},
	}
}

// TestRecovery tests recovery from a specific failure
func (rt *RecoveryTester) TestRecovery(ctx context.Context, operation string, testFunc func() error) error {
	start := time.Now()

	// Attempt operation
	err := testFunc()

	rt.mu.Lock()
	defer rt.mu.Unlock()

	rt.metrics.TotalFailures++

	if err == nil {
		// Successful recovery
		rt.metrics.SuccessfulRecovery++
		recoveryTime := time.Since(start)

		// Update metrics
		rt.updateRecoveryTime(recoveryTime)

		klog.V(3).Infof("Recovery successful for %s in %v", operation, recoveryTime)
	} else {
		// Failed recovery
		rt.metrics.FailedRecovery++
		klog.V(3).Infof("Recovery failed for %s: %v", operation, err)
	}

	return err
}

// GetMetrics returns recovery metrics
func (rt *RecoveryTester) GetMetrics() RecoveryMetrics {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.metrics
}

// Reset resets recovery metrics
func (rt *RecoveryTester) Reset() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.metrics = RecoveryMetrics{}
}

func (rt *RecoveryTester) updateRecoveryTime(duration time.Duration) {
	if rt.metrics.MaxRecoveryTime == 0 || duration > rt.metrics.MaxRecoveryTime {
		rt.metrics.MaxRecoveryTime = duration
	}

	if rt.metrics.MinRecoveryTime == 0 || duration < rt.metrics.MinRecoveryTime {
		rt.metrics.MinRecoveryTime = duration
	}

	// Update average (simple moving average)
	total := rt.metrics.AverageRecoveryTime * time.Duration(rt.metrics.SuccessfulRecovery-1)
	rt.metrics.AverageRecoveryTime = (total + duration) / time.Duration(rt.metrics.SuccessfulRecovery)
}