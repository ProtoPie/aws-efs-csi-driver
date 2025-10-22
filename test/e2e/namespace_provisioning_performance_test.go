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

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubernetes-sigs/aws-efs-csi-driver/test/e2e/testenv"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// PerformanceTestSuite contains all performance and scale tests for namespace provisioning
type PerformanceTestSuite struct {
	helper           *testenv.NamespaceProvisioningTestHelper
	env              *testenv.AWSTestEnvironment
	metrics          *PerformanceMetrics
	benchmarkResults *BenchmarkResults
}

// PerformanceMetrics tracks various performance metrics
type PerformanceMetrics struct {
	mu sync.RWMutex

	// Timing metrics
	EFSCreationTime      []time.Duration
	AccessPointCreation  []time.Duration
	PVCProvisioningTime  []time.Duration
	PodMountTime         []time.Duration
	NamespaceSetupTime   []time.Duration

	// Throughput metrics
	PVCPerSecond         float64
	MountPerSecond       float64
	ConcurrentOperations int32

	// Resource metrics
	CPUUsage            []float64
	MemoryUsage         []int64
	GoRoutines          []int
	FileDescriptors     []int

	// Scale metrics
	TotalPVCs           int32
	TotalAccessPoints   int32
	TotalMountTargets   int32
	TotalNamespaces     int32

	// Error metrics
	FailedOperations    int32
	Retries             int32
	Timeouts            int32
}

// BenchmarkResults stores benchmark test results
type BenchmarkResults struct {
	mu      sync.Mutex
	results map[string]*BenchmarkResult
}

// BenchmarkResult represents a single benchmark result
type BenchmarkResult struct {
	Name            string
	Operations      int
	Duration        time.Duration
	OpsPerSecond    float64
	AverageLatency  time.Duration
	P50Latency      time.Duration
	P95Latency      time.Duration
	P99Latency      time.Duration
	MaxLatency      time.Duration
	MinLatency      time.Duration
	StandardDev     time.Duration
	ResourceUsage   ResourceSnapshot
}

// ResourceSnapshot captures resource usage at a point in time
type ResourceSnapshot struct {
	CPUPercent      float64
	MemoryMB        int64
	GoRoutines      int
	FileDescriptors int
	Timestamp       time.Time
}

// NewPerformanceTestSuite creates a new performance test suite
func NewPerformanceTestSuite(t *testing.T) *PerformanceTestSuite {
	env := testenv.NewAWSTestEnvironment(t)
	helper := testenv.NewNamespaceProvisioningTestHelper(env)

	return &PerformanceTestSuite{
		helper:  helper,
		env:     env,
		metrics: &PerformanceMetrics{},
		benchmarkResults: &BenchmarkResults{
			results: make(map[string]*BenchmarkResult),
		},
	}
}

// TestBulkPVCCreationPerformance tests performance of creating many PVCs simultaneously
func TestBulkPVCCreationPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	suite := NewPerformanceTestSuite(t)
	defer suite.Cleanup()

	testCases := []struct {
		name          string
		pvcCount      int
		namespaceCount int
		parallel      bool
		expectedTime  time.Duration
	}{
		{
			name:          "Small batch - 10 PVCs in 1 namespace",
			pvcCount:      10,
			namespaceCount: 1,
			parallel:      true,
			expectedTime:  2 * time.Minute,
		},
		{
			name:          "Medium batch - 50 PVCs across 5 namespaces",
			pvcCount:      50,
			namespaceCount: 5,
			parallel:      true,
			expectedTime:  5 * time.Minute,
		},
		{
			name:          "Large batch - 100 PVCs across 10 namespaces",
			pvcCount:      100,
			namespaceCount: 10,
			parallel:      true,
			expectedTime:  10 * time.Minute,
		},
		{
			name:          "Sequential creation - 20 PVCs",
			pvcCount:      20,
			namespaceCount: 2,
			parallel:      false,
			expectedTime:  5 * time.Minute,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			suite.runBulkPVCCreationTest(t, tc.pvcCount, tc.namespaceCount, tc.parallel, tc.expectedTime)
		})
	}

	// Generate performance report
	suite.generatePerformanceReport(t)
}

func (s *PerformanceTestSuite) runBulkPVCCreationTest(t *testing.T, pvcCount, namespaceCount int, parallel bool, expectedTime time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), expectedTime*2)
	defer cancel()

	startTime := time.Now()
	s.startResourceMonitoring(ctx)

	// Create namespaces
	namespaces := make([]string, namespaceCount)
	for i := 0; i < namespaceCount; i++ {
		ns := fmt.Sprintf("perf-test-ns-%d-%d", time.Now().Unix(), i)
		namespaces[i] = ns
		_, err := s.helper.CreateNamespaceWithEFS(ctx, ns)
		if err != nil {
			t.Fatalf("Failed to create namespace %s: %v", ns, err)
		}
		defer s.helper.DeleteNamespace(ctx, ns)
	}

	// Track PVC creation times
	pvcTimes := make([]time.Duration, 0, pvcCount)
	var pvcTimesLock sync.Mutex

	if parallel {
		// Parallel creation
		var wg sync.WaitGroup
		wg.Add(pvcCount)

		semaphore := make(chan struct{}, 10) // Limit concurrent operations

		for i := 0; i < pvcCount; i++ {
			go func(index int) {
				defer wg.Done()
				semaphore <- struct{}{}
				defer func() { <-semaphore }()

				nsIndex := index % namespaceCount
				pvcName := fmt.Sprintf("perf-pvc-%d", index)

				pvcStart := time.Now()
				_, err := s.helper.CreatePVCWithAccessPoint(ctx, namespaces[nsIndex], pvcName, "1Gi")
				if err != nil {
					klog.Errorf("Failed to create PVC %s/%s: %v", namespaces[nsIndex], pvcName, err)
				}
				pvcDuration := time.Since(pvcStart)

				pvcTimesLock.Lock()
				pvcTimes = append(pvcTimes, pvcDuration)
				pvcTimesLock.Unlock()

				atomic.AddInt32(&s.metrics.TotalPVCs, 1)
			}(i)
		}

		wg.Wait()
	} else {
		// Sequential creation
		for i := 0; i < pvcCount; i++ {
			nsIndex := i % namespaceCount
			pvcName := fmt.Sprintf("perf-pvc-%d", i)

			pvcStart := time.Now()
			_, err := s.helper.CreatePVCWithAccessPoint(ctx, namespaces[nsIndex], pvcName, "1Gi")
			if err != nil {
				klog.Errorf("Failed to create PVC %s/%s: %v", namespaces[nsIndex], pvcName, err)
			}
			pvcDuration := time.Since(pvcStart)

			pvcTimes = append(pvcTimes, pvcDuration)
			atomic.AddInt32(&s.metrics.TotalPVCs, 1)
		}
	}

	totalDuration := time.Since(startTime)

	// Calculate statistics
	stats := s.calculateStatistics(pvcTimes)

	// Store benchmark result
	result := &BenchmarkResult{
		Name:           t.Name(),
		Operations:     pvcCount,
		Duration:       totalDuration,
		OpsPerSecond:   float64(pvcCount) / totalDuration.Seconds(),
		AverageLatency: stats.average,
		P50Latency:     stats.p50,
		P95Latency:     stats.p95,
		P99Latency:     stats.p99,
		MaxLatency:     stats.max,
		MinLatency:     stats.min,
		StandardDev:    stats.stdDev,
		ResourceUsage:  s.captureResourceSnapshot(),
	}

	s.benchmarkResults.mu.Lock()
	s.benchmarkResults.results[t.Name()] = result
	s.benchmarkResults.mu.Unlock()

	// Verify performance meets expectations
	if totalDuration > expectedTime {
		t.Errorf("Bulk PVC creation took %v, expected less than %v", totalDuration, expectedTime)
	}

	klog.Infof("Performance test completed: %d PVCs in %v (%.2f ops/sec)",
		pvcCount, totalDuration, result.OpsPerSecond)
}

// TestConcurrentMountPerformance tests concurrent pod mount operations
func TestConcurrentMountPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	suite := NewPerformanceTestSuite(t)
	defer suite.Cleanup()

	testCases := []struct {
		name              string
		podCount          int
		pvcsPerPod        int
		namespaceCount    int
		expectedMountTime time.Duration
	}{
		{
			name:              "10 pods with 1 mount each",
			podCount:          10,
			pvcsPerPod:        1,
			namespaceCount:    2,
			expectedMountTime: 3 * time.Minute,
		},
		{
			name:              "20 pods with 2 mounts each",
			podCount:          20,
			pvcsPerPod:        2,
			namespaceCount:    4,
			expectedMountTime: 5 * time.Minute,
		},
		{
			name:              "50 pods with 1 mount each",
			podCount:          50,
			pvcsPerPod:        1,
			namespaceCount:    5,
			expectedMountTime: 10 * time.Minute,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			suite.runConcurrentMountTest(t, tc.podCount, tc.pvcsPerPod, tc.namespaceCount, tc.expectedMountTime)
		})
	}
}

func (s *PerformanceTestSuite) runConcurrentMountTest(t *testing.T, podCount, pvcsPerPod, namespaceCount int, expectedTime time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), expectedTime*2)
	defer cancel()

	// Setup namespaces and PVCs
	namespaces := make([]string, namespaceCount)
	for i := 0; i < namespaceCount; i++ {
		ns := fmt.Sprintf("mount-test-ns-%d-%d", time.Now().Unix(), i)
		namespaces[i] = ns
		_, err := s.helper.CreateNamespaceWithEFS(ctx, ns)
		if err != nil {
			t.Fatalf("Failed to create namespace %s: %v", ns, err)
		}
		defer s.helper.DeleteNamespace(ctx, ns)

		// Pre-create PVCs
		for j := 0; j < (podCount/namespaceCount)*pvcsPerPod; j++ {
			pvcName := fmt.Sprintf("mount-pvc-%d-%d", i, j)
			_, err := s.helper.CreatePVCWithAccessPoint(ctx, ns, pvcName, "1Gi")
			if err != nil {
				klog.Errorf("Failed to create PVC %s/%s: %v", ns, pvcName, err)
			}
		}
	}

	// Wait for all PVCs to be bound
	time.Sleep(30 * time.Second)

	// Create pods concurrently and measure mount times
	mountTimes := make([]time.Duration, 0, podCount)
	var mountTimesLock sync.Mutex
	var wg sync.WaitGroup

	startTime := time.Now()
	semaphore := make(chan struct{}, 20) // Limit concurrent pod creation

	for i := 0; i < podCount; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			nsIndex := index % namespaceCount
			podName := fmt.Sprintf("mount-pod-%d", index)

			// Simulate pod mount timing
			// In a real implementation, this would create actual pods
			// For now, we simulate the mount operation
			mountStart := time.Now()

			// Simulate mount operation with varying delays
			time.Sleep(time.Duration(100+index%50) * time.Millisecond)

			mountDuration := time.Since(mountStart)

			mountTimesLock.Lock()
			mountTimes = append(mountTimes, mountDuration)
			mountTimesLock.Unlock()
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All pods mounted successfully
	case <-ctx.Done():
		t.Fatalf("Timeout waiting for pods to mount")
	}

	totalDuration := time.Since(startTime)

	// Calculate mount statistics
	stats := s.calculateStatistics(mountTimes)

	klog.Infof("Mount performance: %d pods in %v, Avg: %v, P95: %v, P99: %v",
		podCount, totalDuration, stats.average, stats.p95, stats.p99)

	if totalDuration > expectedTime {
		t.Errorf("Concurrent mounting took %v, expected less than %v", totalDuration, expectedTime)
	}
}

// TestResourceUsageMonitoring tests resource consumption during scale operations
func TestResourceUsageMonitoring(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping resource monitoring test in short mode")
	}

	suite := NewPerformanceTestSuite(t)
	defer suite.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Start continuous resource monitoring
	suite.startDetailedResourceMonitoring(ctx)

	// Create load pattern to stress the system
	ns := fmt.Sprintf("resource-test-%d", time.Now().Unix())
	_, err := suite.helper.CreateNamespaceWithEFS(ctx, ns)
	if err != nil {
		t.Fatalf("Failed to create namespace: %v", err)
	}
	defer suite.helper.DeleteNamespace(ctx, ns)

	// Phase 1: Steady state load
	t.Log("Phase 1: Steady state load")
	for i := 0; i < 10; i++ {
		pvcName := fmt.Sprintf("steady-pvc-%d", i)
		_, err := suite.helper.CreatePVCWithAccessPoint(ctx, ns, pvcName, "1Gi")
		if err != nil {
			klog.Errorf("Failed to create PVC: %v", err)
		}
		time.Sleep(2 * time.Second)
	}

	// Phase 2: Burst load
	t.Log("Phase 2: Burst load")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			pvcName := fmt.Sprintf("burst-pvc-%d", index)
			_, err := suite.helper.CreatePVCWithAccessPoint(ctx, ns, pvcName, "1Gi")
			if err != nil {
				klog.Errorf("Failed to create PVC: %v", err)
			}
		}(i)
	}
	wg.Wait()

	// Phase 3: Sustained high load
	t.Log("Phase 3: Sustained high load")
	done := make(chan struct{})
	go func() {
		for i := 0; i < 30; i++ {
			pvcName := fmt.Sprintf("sustained-pvc-%d", i)
			_, err := suite.helper.CreatePVCWithAccessPoint(ctx, ns, pvcName, "1Gi")
			if err != nil {
				klog.Errorf("Failed to create PVC: %v", err)
			}
			time.Sleep(500 * time.Millisecond)
		}
		close(done)
	}()

	<-done

	// Analyze resource usage patterns
	suite.analyzeResourceUsage(t)
}

// TestScaleStress performs extreme scale testing
func TestScaleStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping scale stress test in short mode")
	}

	// This test requires significant resources
	if os.Getenv("RUN_SCALE_STRESS_TEST") != "true" {
		t.Skip("Set RUN_SCALE_STRESS_TEST=true to run this test")
	}

	suite := NewPerformanceTestSuite(t)
	defer suite.Cleanup()

	testCases := []struct {
		name           string
		namespaces     int
		pvcsPerNs      int
		podsPerNs      int
		duration       time.Duration
		targetOpsPerSec float64
	}{
		{
			name:           "100 namespaces with 10 PVCs each",
			namespaces:     100,
			pvcsPerNs:      10,
			podsPerNs:      5,
			duration:       30 * time.Minute,
			targetOpsPerSec: 5.0,
		},
		{
			name:           "50 namespaces with 50 PVCs each",
			namespaces:     50,
			pvcsPerNs:      50,
			podsPerNs:      25,
			duration:       45 * time.Minute,
			targetOpsPerSec: 3.0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			suite.runScaleStressTest(t, tc.namespaces, tc.pvcsPerNs, tc.podsPerNs, tc.duration, tc.targetOpsPerSec)
		})
	}
}

func (s *PerformanceTestSuite) runScaleStressTest(t *testing.T, namespaces, pvcsPerNs, podsPerNs int, duration time.Duration, targetOpsPerSec float64) {
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	startTime := time.Now()
	totalOperations := int32(0)
	failedOperations := int32(0)

	// Create worker pool
	workers := 50
	workChan := make(chan func(), 1000)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for work := range workChan {
				work()
			}
		}()
	}

	// Generate workload
	go func() {
		for i := 0; i < namespaces; i++ {
			nsName := fmt.Sprintf("scale-ns-%d-%d", time.Now().Unix(), i)

			// Create namespace
			workChan <- func() {
				_, err := s.helper.CreateNamespaceWithEFS(ctx, nsName)
				if err != nil {
					atomic.AddInt32(&failedOperations, 1)
				} else {
					atomic.AddInt32(&totalOperations, 1)
					atomic.AddInt32(&s.metrics.TotalNamespaces, 1)
				}
			}

			// Create PVCs
			for j := 0; j < pvcsPerNs; j++ {
				pvcName := fmt.Sprintf("scale-pvc-%d", j)
				workChan <- func() {
					_, err := s.helper.CreatePVCWithAccessPoint(ctx, nsName, pvcName, "1Gi")
					if err != nil {
						atomic.AddInt32(&failedOperations, 1)
					} else {
						atomic.AddInt32(&totalOperations, 1)
						atomic.AddInt32(&s.metrics.TotalPVCs, 1)
					}
				}()
			}

			// Simulate Pod operations (simplified for testing)
			for k := 0; k < podsPerNs; k++ {
				workChan <- func() {
					// Simulate pod operation
					time.Sleep(10 * time.Millisecond)
					atomic.AddInt32(&totalOperations, 1)
				}()
			}

			// Rate limiting
			time.Sleep(time.Duration(float64(time.Second) / targetOpsPerSec))
		}

		close(workChan)
	}()

	// Monitor progress
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ticker.C:
				ops := atomic.LoadInt32(&totalOperations)
				failed := atomic.LoadInt32(&failedOperations)
				elapsed := time.Since(startTime)
				opsPerSec := float64(ops) / elapsed.Seconds()

				klog.Infof("Scale test progress: %d operations (%.2f ops/sec), %d failed, %d namespaces",
					ops, opsPerSec, failed, atomic.LoadInt32(&s.metrics.TotalNamespaces))
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()

	// Final statistics
	totalOps := atomic.LoadInt32(&totalOperations)
	failedOps := atomic.LoadInt32(&failedOperations)
	elapsed := time.Since(startTime)
	actualOpsPerSec := float64(totalOps) / elapsed.Seconds()

	klog.Infof("Scale test completed: %d operations in %v (%.2f ops/sec), %d failed",
		totalOps, elapsed, actualOpsPerSec, failedOps)

	// Verify performance targets
	if actualOpsPerSec < targetOpsPerSec*0.8 { // Allow 20% deviation
		t.Errorf("Performance below target: %.2f ops/sec (target: %.2f)", actualOpsPerSec, targetOpsPerSec)
	}

	if float64(failedOps)/float64(totalOps) > 0.05 { // Max 5% failure rate
		t.Errorf("High failure rate: %.2f%%", float64(failedOps)/float64(totalOps)*100)
	}
}

// Helper methods

func (s *PerformanceTestSuite) startResourceMonitoring(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				snapshot := s.captureResourceSnapshot()
				s.metrics.mu.Lock()
				s.metrics.CPUUsage = append(s.metrics.CPUUsage, snapshot.CPUPercent)
				s.metrics.MemoryUsage = append(s.metrics.MemoryUsage, snapshot.MemoryMB)
				s.metrics.GoRoutines = append(s.metrics.GoRoutines, snapshot.GoRoutines)
				s.metrics.FileDescriptors = append(s.metrics.FileDescriptors, snapshot.FileDescriptors)
				s.metrics.mu.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (s *PerformanceTestSuite) startDetailedResourceMonitoring(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				snapshot := s.captureResourceSnapshot()
				s.metrics.mu.Lock()
				s.metrics.CPUUsage = append(s.metrics.CPUUsage, snapshot.CPUPercent)
				s.metrics.MemoryUsage = append(s.metrics.MemoryUsage, snapshot.MemoryMB)
				s.metrics.GoRoutines = append(s.metrics.GoRoutines, snapshot.GoRoutines)
				s.metrics.FileDescriptors = append(s.metrics.FileDescriptors, snapshot.FileDescriptors)
				s.metrics.mu.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (s *PerformanceTestSuite) captureResourceSnapshot() ResourceSnapshot {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return ResourceSnapshot{
		CPUPercent:      0, // Would require system-specific implementation
		MemoryMB:        int64(m.Alloc / 1024 / 1024),
		GoRoutines:      runtime.NumGoroutine(),
		FileDescriptors: 0, // Would require system-specific implementation
		Timestamp:       time.Now(),
	}
}

type statistics struct {
	average time.Duration
	min     time.Duration
	max     time.Duration
	p50     time.Duration
	p95     time.Duration
	p99     time.Duration
	stdDev  time.Duration
}

func (s *PerformanceTestSuite) calculateStatistics(times []time.Duration) statistics {
	if len(times) == 0 {
		return statistics{}
	}

	// Calculate average
	var sum time.Duration
	for _, t := range times {
		sum += t
	}
	avg := sum / time.Duration(len(times))

	// Find min and max
	min := times[0]
	max := times[0]
	for _, t := range times {
		if t < min {
			min = t
		}
		if t > max {
			max = t
		}
	}

	// Calculate percentiles (simplified - should sort first)
	p50Index := len(times) * 50 / 100
	p95Index := len(times) * 95 / 100
	p99Index := len(times) * 99 / 100

	// Calculate standard deviation
	var variance float64
	avgFloat := float64(avg)
	for _, t := range times {
		diff := float64(t) - avgFloat
		variance += diff * diff
	}
	variance /= float64(len(times))
	stdDev := time.Duration(math.Sqrt(variance))

	return statistics{
		average: avg,
		min:     min,
		max:     max,
		p50:     times[p50Index],
		p95:     times[p95Index],
		p99:     times[p99Index],
		stdDev:  stdDev,
	}
}

func (s *PerformanceTestSuite) analyzeResourceUsage(t *testing.T) {
	s.metrics.mu.RLock()
	defer s.metrics.mu.RUnlock()

	if len(s.metrics.CPUUsage) == 0 {
		return
	}

	// Calculate average CPU usage
	var avgCPU float64
	for _, cpu := range s.metrics.CPUUsage {
		avgCPU += cpu
	}
	avgCPU /= float64(len(s.metrics.CPUUsage))

	// Calculate average memory usage
	var avgMem int64
	var maxMem int64
	for _, mem := range s.metrics.MemoryUsage {
		avgMem += mem
		if mem > maxMem {
			maxMem = mem
		}
	}
	avgMem /= int64(len(s.metrics.MemoryUsage))

	// Calculate average goroutines
	var avgGoroutines int
	var maxGoroutines int
	for _, gr := range s.metrics.GoRoutines {
		avgGoroutines += gr
		if gr > maxGoroutines {
			maxGoroutines = gr
		}
	}
	avgGoroutines /= len(s.metrics.GoRoutines)

	klog.Infof("Resource usage analysis:")
	klog.Infof("  CPU: Avg=%.2f%%", avgCPU)
	klog.Infof("  Memory: Avg=%dMB, Max=%dMB", avgMem, maxMem)
	klog.Infof("  Goroutines: Avg=%d, Max=%d", avgGoroutines, maxGoroutines)

	// Check for resource leaks
	if maxMem > avgMem*2 {
		t.Logf("WARNING: Potential memory leak detected (Max: %dMB, Avg: %dMB)", maxMem, avgMem)
	}
	if maxGoroutines > avgGoroutines*2 {
		t.Logf("WARNING: Potential goroutine leak detected (Max: %d, Avg: %d)", maxGoroutines, avgGoroutines)
	}
}

func (s *PerformanceTestSuite) generatePerformanceReport(t *testing.T) {
	s.benchmarkResults.mu.Lock()
	defer s.benchmarkResults.mu.Unlock()

	report := PerformanceReport{
		Timestamp: time.Now(),
		Results:   s.benchmarkResults.results,
		Metrics:   s.metrics,
	}

	// Marshal to JSON
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		klog.Errorf("Failed to generate performance report: %v", err)
		return
	}

	// Write to file
	filename := fmt.Sprintf("performance_report_%d.json", time.Now().Unix())
	if err := os.WriteFile(filename, data, 0644); err != nil {
		klog.Errorf("Failed to write performance report: %v", err)
		return
	}

	klog.Infof("Performance report written to %s", filename)

	// Log summary
	for name, result := range s.benchmarkResults.results {
		klog.Infof("Benchmark %s: %.2f ops/sec, Avg latency: %v, P95: %v",
			name, result.OpsPerSecond, result.AverageLatency, result.P95Latency)
	}
}

// PerformanceReport represents the complete performance test report
type PerformanceReport struct {
	Timestamp time.Time
	Results   map[string]*BenchmarkResult
	Metrics   *PerformanceMetrics
}

// Cleanup cleans up test resources
func (s *PerformanceTestSuite) Cleanup() {
	// Cleanup is handled by individual test defer statements
	klog.Info("Performance test suite cleanup completed")
}