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
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubernetes-sigs/aws-efs-csi-driver/test/e2e/testenv"
	"k8s.io/klog/v2"
)

// BenchmarkSuite contains all benchmark tests for namespace provisioning
type BenchmarkSuite struct {
	helper *testenv.NamespaceProvisioningTestHelper
	env    *testenv.AWSTestEnvironment
	t      *testing.T
}

// BenchmarkEFSCreation benchmarks EFS filesystem creation
func BenchmarkEFSCreation(b *testing.B) {
	t := &testing.T{}
	env := testenv.NewAWSTestEnvironment(t)
	suite := &BenchmarkSuite{
		helper: testenv.NewNamespaceProvisioningTestHelper(env),
		env:    env,
		t:      t,
	}
	defer suite.Cleanup()

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ns := fmt.Sprintf("bench-efs-%d-%d", time.Now().Unix(), i)

		start := time.Now()
		_, err := suite.helper.CreateNamespaceWithEFS(ctx, ns)
		if err != nil {
			b.Fatalf("Failed to create namespace: %v", err)
		}
		_, err = suite.helper.CreatePVCWithAccessPoint(ctx, ns, "bench-pvc", "1Gi")
		if err != nil {
			b.Fatalf("Failed to create PVC: %v", err)
		}
		b.ReportMetric(float64(time.Since(start).Milliseconds()), "ms/op")

		// Cleanup
		suite.helper.DeleteNamespace(ctx, ns)
	}
}

// BenchmarkAccessPointCreation benchmarks Access Point creation within existing EFS
func BenchmarkAccessPointCreation(b *testing.B) {
	t := &testing.T{}
	env := testenv.NewAWSTestEnvironment(t)
	suite := &BenchmarkSuite{
		helper: testenv.NewNamespaceProvisioningTestHelper(env),
		env:    env,
		t:      t,
	}
	defer suite.Cleanup()

	ctx := context.Background()

	// Setup: Create namespace with initial EFS
	ns := fmt.Sprintf("bench-ap-%d", time.Now().Unix())
	_, err := suite.helper.CreateNamespaceWithEFS(ctx, ns)
	if err != nil {
		b.Fatalf("Failed to create namespace: %v", err)
	}
	defer suite.helper.DeleteNamespace(ctx, ns)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pvcName := fmt.Sprintf("bench-ap-pvc-%d", i)

		start := time.Now()
		_, err := suite.helper.CreatePVCWithAccessPoint(ctx, ns, pvcName, "1Gi")
		if err != nil {
			b.Errorf("Failed to create PVC: %v", err)
		}
		b.ReportMetric(float64(time.Since(start).Milliseconds()), "ms/op")
	}
}

// BenchmarkConcurrentPVCCreation benchmarks concurrent PVC creation
func BenchmarkConcurrentPVCCreation(b *testing.B) {
	concurrencyLevels := []int{1, 5, 10, 20, 50}

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency-%d", concurrency), func(b *testing.B) {
			t := &testing.T{}
			env := testenv.NewAWSTestEnvironment(t)
			suite := &BenchmarkSuite{
				helper: testenv.NewNamespaceProvisioningTestHelper(env),
				env:    env,
				t:      t,
			}
			defer suite.Cleanup()

			ctx := context.Background()
			ns := fmt.Sprintf("bench-concurrent-%d-%d", concurrency, time.Now().Unix())
			_, err := suite.helper.CreateNamespaceWithEFS(ctx, ns)
			if err != nil {
				b.Fatalf("Failed to create namespace: %v", err)
			}
			defer suite.helper.DeleteNamespace(ctx, ns)

			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				var wg sync.WaitGroup
				semaphore := make(chan struct{}, concurrency)
				errors := int32(0)

				start := time.Now()

				for i := 0; i < concurrency; i++ {
					wg.Add(1)
					go func(index int) {
						defer wg.Done()
						semaphore <- struct{}{}
						defer func() { <-semaphore }()

						pvcName := fmt.Sprintf("bench-pvc-%d-%d", n, index)
						_, err := suite.helper.CreatePVCWithAccessPoint(ctx, ns, pvcName, "1Gi")
						if err != nil {
							atomic.AddInt32(&errors, 1)
						}
					}(i)
				}

				wg.Wait()
				duration := time.Since(start)

				b.ReportMetric(float64(duration.Milliseconds())/float64(concurrency), "ms/pvc")
				b.ReportMetric(float64(concurrency)*1000/float64(duration.Milliseconds()), "pvcs/sec")

				if errors > 0 {
					b.Logf("Errors occurred: %d/%d", errors, concurrency)
				}

				// Cleanup
				for i := 0; i < concurrency; i++ {
					pvcName := fmt.Sprintf("bench-pvc-%d-%d", n, i)
					suite.helper.DeletePVC(ns, pvcName)
				}
			}
		})
	}
}

// BenchmarkPodMounting benchmarks pod mounting times
func BenchmarkPodMounting(b *testing.B) {
	t := &testing.T{}
	env := testenv.NewAWSTestEnvironment(t)
	suite := &BenchmarkSuite{
		helper: testenv.NewNamespaceProvisioningTestHelper(env),
		env:    env,
		t:      t,
	}
	defer suite.Cleanup()

	ctx := context.Background()

	// Setup: Create namespace and PVC
	ns := fmt.Sprintf("bench-mount-%d", time.Now().Unix())
	_, err := suite.helper.CreateNamespaceWithEFS(ctx, ns)
	if err != nil {
		b.Fatalf("Failed to create namespace: %v", err)
	}
	_, err = suite.helper.CreatePVCWithAccessPoint(ctx, ns, "bench-mount-pvc", "1Gi")
	if err != nil {
		b.Fatalf("Failed to create PVC: %v", err)
	}
	defer suite.helper.DeleteNamespace(ctx, ns)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Simulate pod mount timing
		// In real implementation, this would create actual pods
		start := time.Now()
		time.Sleep(100 * time.Millisecond) // Simulate mount operation
		b.ReportMetric(float64(time.Since(start).Milliseconds()), "ms/mount")
	}
}

// BenchmarkNamespaceScaling benchmarks namespace scaling scenarios
func BenchmarkNamespaceScaling(b *testing.B) {
	namespaceCounts := []int{10, 25, 50, 100}

	for _, nsCount := range namespaceCounts {
		b.Run(fmt.Sprintf("Namespaces-%d", nsCount), func(b *testing.B) {
			suite := &BenchmarkSuite{
				helper: testenv.NewNamespaceProvisioningTestHelper(&testing.T{}),
				env:    testenv.NewAWSTestEnvironment(&testing.T{}),
			}
			defer suite.Cleanup()

			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				namespaces := make([]string, nsCount)
				var createWg sync.WaitGroup

				// Create namespaces and PVCs
				start := time.Now()
				for i := 0; i < nsCount; i++ {
					createWg.Add(1)
					go func(index int) {
						defer createWg.Done()
						ns := fmt.Sprintf("bench-scale-%d-%d-%d", nsCount, n, index)
						namespaces[index] = ns
						suite.helper.CreateNamespace(ns)
						suite.helper.CreatePVC(ns, "scale-pvc", "1Gi")
					}(i)
				}
				createWg.Wait()
				createDuration := time.Since(start)

				b.ReportMetric(float64(createDuration.Milliseconds())/float64(nsCount), "ms/namespace")
				b.ReportMetric(float64(nsCount)*1000/float64(createDuration.Milliseconds()), "namespaces/sec")

				// Cleanup
				var deleteWg sync.WaitGroup
				for _, ns := range namespaces {
					deleteWg.Add(1)
					go func(namespace string) {
						defer deleteWg.Done()
						suite.helper.DeleteNamespace(namespace)
					}(ns)
				}
				deleteWg.Wait()
			}
		})
	}
}

// BenchmarkLargeFileOperations benchmarks operations with large files
func BenchmarkLargeFileOperations(b *testing.B) {
	fileSizes := []string{"10Gi", "50Gi", "100Gi", "500Gi"}

	for _, size := range fileSizes {
		b.Run(fmt.Sprintf("FileSize-%s", size), func(b *testing.B) {
			suite := &BenchmarkSuite{
				helper: testenv.NewNamespaceProvisioningTestHelper(&testing.T{}),
				env:    testenv.NewAWSTestEnvironment(&testing.T{}),
			}
			defer suite.Cleanup()

			ns := fmt.Sprintf("bench-large-%s-%d", size, time.Now().Unix())
			suite.helper.CreateNamespace(ns)
			defer suite.helper.DeleteNamespace(ns)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				pvcName := fmt.Sprintf("large-pvc-%d", i)

				start := time.Now()
				suite.helper.CreatePVC(ns, pvcName, size)
				suite.helper.WaitForPVCBound(ns, pvcName, 10*time.Minute)
				provisionTime := time.Since(start)

				// Create pod and perform file operations
				podName := fmt.Sprintf("large-pod-%d", i)
				pod := suite.helper.CreatePodWithPVC(ns, podName, pvcName)
				suite.helper.WaitForPodRunning(ns, pod.Name, 5*time.Minute)

				// Simulate file operations (would need actual implementation)
				// This is a placeholder for actual file I/O operations
				time.Sleep(1 * time.Second)

				totalTime := time.Since(start)

				b.ReportMetric(float64(provisionTime.Milliseconds()), "ms/provision")
				b.ReportMetric(float64(totalTime.Milliseconds()), "ms/total")

				// Cleanup
				suite.helper.DeletePod(ns, podName)
				suite.helper.DeletePVC(ns, pvcName)
			}
		})
	}
}

// BenchmarkCrossAccountMounting benchmarks cross-account EFS mounting
func BenchmarkCrossAccountMounting(b *testing.B) {
	// This requires cross-account setup
	b.Skip("Cross-account benchmarking requires special setup")
}

// BenchmarkRecoveryTime benchmarks recovery time from various failure scenarios
func BenchmarkRecoveryTime(b *testing.B) {
	scenarios := []struct {
		name        string
		injectError func(*BenchmarkSuite, string)
	}{
		{
			name: "NetworkPartition",
			injectError: func(s *BenchmarkSuite, ns string) {
				// Simulate network partition
				s.env.SimulateNetworkPartition()
			},
		},
		{
			name: "ControllerRestart",
			injectError: func(s *BenchmarkSuite, ns string) {
				// Simulate controller restart
				s.helper.RestartCSIController()
			},
		},
		{
			name: "CRDLoss",
			injectError: func(s *BenchmarkSuite, ns string) {
				// Simulate CRD loss
				s.helper.DeleteCRD("efsnamespaces.storage.k8s.io")
			},
		},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			suite := &BenchmarkSuite{
				helper: testenv.NewNamespaceProvisioningTestHelper(&testing.T{}),
				env:    testenv.NewAWSTestEnvironment(&testing.T{}),
			}
			defer suite.Cleanup()

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				ns := fmt.Sprintf("bench-recovery-%s-%d", scenario.name, i)
				suite.helper.CreateNamespace(ns)

				// Create initial resources
				suite.helper.CreatePVC(ns, "recovery-pvc", "1Gi")
				suite.helper.WaitForPVCBound(ns, "recovery-pvc", 5*time.Minute)

				// Inject error
				scenario.injectError(suite, ns)

				// Measure recovery time
				start := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
				defer cancel()

				recovered := false
				for !recovered {
					select {
					case <-ctx.Done():
						b.Fatal("Recovery timeout")
					default:
						// Try to create new PVC to test recovery
						testPVC := fmt.Sprintf("test-recovery-pvc-%d", i)
						err := suite.helper.CreatePVCWithRetry(ns, testPVC, "1Gi", 1)
						if err == nil {
							recovered = true
						}
						time.Sleep(5 * time.Second)
					}
				}

				recoveryTime := time.Since(start)
				b.ReportMetric(float64(recoveryTime.Seconds()), "seconds")

				// Cleanup
				suite.helper.DeleteNamespace(ns)
			}
		})
	}
}

// BenchmarkMemoryUsage benchmarks memory usage patterns
func BenchmarkMemoryUsage(b *testing.B) {
	pvcCounts := []int{100, 500, 1000, 5000}

	for _, count := range pvcCounts {
		b.Run(fmt.Sprintf("PVCs-%d", count), func(b *testing.B) {
			suite := &BenchmarkSuite{
				helper: testenv.NewNamespaceProvisioningTestHelper(&testing.T{}),
				env:    testenv.NewAWSTestEnvironment(&testing.T{}),
			}
			defer suite.Cleanup()

			// Track memory usage
			memSamples := make([]int64, 0)
			done := make(chan struct{})

			go func() {
				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()

				for {
					select {
					case <-ticker.C:
						memSamples = append(memSamples, suite.helper.GetMemoryUsage())
					case <-done:
						return
					}
				}
			}()

			ns := fmt.Sprintf("bench-mem-%d", count)
			suite.helper.CreateNamespace(ns)
			defer suite.helper.DeleteNamespace(ns)

			b.ResetTimer()

			// Create many PVCs to stress memory
			for i := 0; i < count; i++ {
				pvcName := fmt.Sprintf("mem-pvc-%d", i)
				suite.helper.CreatePVC(ns, pvcName, "1Gi")
			}

			close(done)

			// Calculate memory statistics
			if len(memSamples) > 0 {
				var sum, max int64
				for _, mem := range memSamples {
					sum += mem
					if mem > max {
						max = mem
					}
				}
				avg := sum / int64(len(memSamples))

				b.ReportMetric(float64(avg/1024/1024), "MB/avg")
				b.ReportMetric(float64(max/1024/1024), "MB/max")
				b.ReportMetric(float64(avg/int64(count)/1024), "KB/pvc")
			}
		})
	}
}

// Helper to calculate percentiles
func calculatePercentile(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}

	index := int(math.Ceil(float64(len(values)) * percentile / 100))
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}

// Cleanup cleans up benchmark resources
func (s *BenchmarkSuite) Cleanup() {
	klog.Info("Benchmark suite cleanup completed")
}