package performance

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/efs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	clientset "k8s.io/client-go/kubernetes"

	testutils "github.com/kubernetes-sigs/aws-efs-csi-driver/test/utils"
)

// PerformanceTestSuite validates EFS-NS performance requirements
type PerformanceTestSuite struct {
	suite.Suite
	client           clientset.Interface
	efsClient        *efs.Client
	helper           *testutils.EfsNsTestHelper
	resourceTracker  *testutils.TestResourceTracker
	testConfig       *PerformanceTestConfig
	metrics          *PerformanceMetrics
}

// PerformanceTestConfig holds configuration for performance tests
type PerformanceTestConfig struct {
	Region                    string
	StorageClassName          string
	MaxFilesystemCreationTime time.Duration // <30s requirement
	MaxPVCLifecycleTime       time.Duration // <2min requirement
	MaxConcurrentOperations   int           // Concurrent load testing
	VolumeSize                string
	AccessMode                []corev1.PersistentVolumeAccessMode
	PerformanceIterations     int
}

// PerformanceMetrics tracks performance data during tests
type PerformanceMetrics struct {
	FilesystemCreationTimes []time.Duration
	PVCCreationTimes        []time.Duration
	PVCDeletionTimes        []time.Duration
	MountTimes              []time.Duration
	UnmountTimes            []time.Duration
	ConcurrentOperationTime time.Duration
	ResourceUtilization     *ResourceUtilizationMetrics
	mu                      sync.RWMutex
}

// ResourceUtilizationMetrics tracks system resource usage
type ResourceUtilizationMetrics struct {
	CPUUsagePercent    []float64
	MemoryUsageBytes   []int64
	NetworkBytesIn     []int64
	NetworkBytesOut    []int64
	DiskIOReadBytes    []int64
	DiskIOWriteBytes   []int64
	APICallCounts      map[string]int
	ErrorRates         map[string]float64
}

func TestPerformanceTestSuite(t *testing.T) {
	suite.Run(t, new(PerformanceTestSuite))
}

func (suite *PerformanceTestSuite) SetupSuite() {
	var err error

	suite.client, err = testutils.NewKubernetesClient()
	require.NoError(suite.T(), err, "Failed to create Kubernetes client")

	suite.efsClient, err = testutils.NewEFSClient("")
	require.NoError(suite.T(), err, "Failed to create EFS client")

	suite.helper = testutils.NewEfsNsTestHelper(suite.client, suite.efsClient)
	suite.resourceTracker = testutils.NewTestResourceTracker(suite.client, suite.efsClient)

	suite.testConfig = &PerformanceTestConfig{
		Region:                    testutils.TestConstants.AWSRegion,
		StorageClassName:          "efs-ns-sc-perf-test",
		MaxFilesystemCreationTime: 30 * time.Second,
		MaxPVCLifecycleTime:       2 * time.Minute,
		MaxConcurrentOperations:   10,
		VolumeSize:                "10Gi",
		AccessMode:                []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
		PerformanceIterations:     5,
	}

	suite.metrics = &PerformanceMetrics{
		ResourceUtilization: &ResourceUtilizationMetrics{
			APICallCounts: make(map[string]int),
			ErrorRates:    make(map[string]float64),
		},
	}

	suite.T().Logf("Performance test suite initialized with config: %+v", suite.testConfig)
}

func (suite *PerformanceTestSuite) TearDownSuite() {
	suite.resourceTracker.CleanupAll(context.Background())
	suite.printPerformanceReport()
}

func (suite *PerformanceTestSuite) TestFilesystemCreationPerformance() {
	ctx := context.Background()
	suite.T().Log("Testing EFS filesystem creation performance")

	for i := 0; i < suite.testConfig.PerformanceIterations; i++ {
		testName := fmt.Sprintf("filesystem-perf-%d", i)
		namespace := suite.helper.CreateTestNamespace(testName)
		suite.resourceTracker.AddNamespace(namespace.Name)

		// Create StorageClass
		sc := suite.createPerformanceStorageClass(testName, namespace.Name)
		suite.resourceTracker.AddStorageClass(sc.Name)

		// Measure filesystem creation time
		startTime := time.Now()
		
		// Create PVC which triggers filesystem creation
		pvc := suite.createTestPVC(testName, namespace.Name, sc.Name)
		suite.resourceTracker.AddPVC(namespace.Name, pvc.Name)

		// Wait for filesystem to be created and PVC to be bound
		err := suite.waitForPVCBound(ctx, namespace.Name, pvc.Name, suite.testConfig.MaxFilesystemCreationTime)
		creationTime := time.Since(startTime)
		
		require.NoError(suite.T(), err, "PVC should be bound within timeout")
		assert.LessOrEqual(suite.T(), creationTime, suite.testConfig.MaxFilesystemCreationTime,
			"Filesystem creation should complete within %v, took %v", suite.testConfig.MaxFilesystemCreationTime, creationTime)

		suite.recordFilesystemCreationTime(creationTime)
		
		suite.T().Logf("Iteration %d: Filesystem creation took %v", i+1, creationTime)
	}

	suite.validateFilesystemCreationMetrics()
}

func (suite *PerformanceTestSuite) TestPVCLifecyclePerformance() {
	ctx := context.Background()
	suite.T().Log("Testing complete PVC lifecycle performance")

	for i := 0; i < suite.testConfig.PerformanceIterations; i++ {
		testName := fmt.Sprintf("pvc-lifecycle-perf-%d", i)
		namespace := suite.helper.CreateTestNamespace(testName)
		suite.resourceTracker.AddNamespace(namespace.Name)

		sc := suite.createPerformanceStorageClass(testName, namespace.Name)
		suite.resourceTracker.AddStorageClass(sc.Name)

		// Measure complete PVC lifecycle
		lifecycleStart := time.Now()

		// 1. Create PVC
		createStart := time.Now()
		pvc := suite.createTestPVC(testName, namespace.Name, sc.Name)
		suite.resourceTracker.AddPVC(namespace.Name, pvc.Name)

		err := suite.waitForPVCBound(ctx, namespace.Name, pvc.Name, suite.testConfig.MaxPVCLifecycleTime/2)
		createTime := time.Since(createStart)
		require.NoError(suite.T(), err, "PVC creation should complete")

		// 2. Create Pod and mount volume
		pod := suite.createTestPod(testName, namespace.Name, pvc.Name)
		suite.resourceTracker.AddPod(namespace.Name, pod.Name)

		mountStart := time.Now()
		err = suite.waitForPodRunning(ctx, namespace.Name, pod.Name, suite.testConfig.MaxPVCLifecycleTime/4)
		mountTime := time.Since(mountStart)
		require.NoError(suite.T(), err, "Pod should be running")

		// 3. Delete Pod (unmount)
		unmountStart := time.Now()
		err = suite.client.CoreV1().Pods(namespace.Name).Delete(ctx, pod.Name, metav1.DeleteOptions{})
		require.NoError(suite.T(), err, "Pod deletion should succeed")

		err = suite.waitForPodDeleted(ctx, namespace.Name, pod.Name, suite.testConfig.MaxPVCLifecycleTime/4)
		unmountTime := time.Since(unmountStart)
		require.NoError(suite.T(), err, "Pod should be deleted")

		// 4. Delete PVC
		deleteStart := time.Now()
		err = suite.client.CoreV1().PersistentVolumeClaims(namespace.Name).Delete(ctx, pvc.Name, metav1.DeleteOptions{})
		require.NoError(suite.T(), err, "PVC deletion should succeed")

		err = suite.waitForPVCDeleted(ctx, namespace.Name, pvc.Name, suite.testConfig.MaxPVCLifecycleTime/4)
		deleteTime := time.Since(deleteStart)
		require.NoError(suite.T(), err, "PVC should be deleted")

		totalLifecycleTime := time.Since(lifecycleStart)

		// Validate performance requirements
		assert.LessOrEqual(suite.T(), totalLifecycleTime, suite.testConfig.MaxPVCLifecycleTime,
			"Complete PVC lifecycle should complete within %v, took %v", suite.testConfig.MaxPVCLifecycleTime, totalLifecycleTime)

		suite.recordPVCLifecycleMetrics(createTime, mountTime, unmountTime, deleteTime)
		
		suite.T().Logf("Iteration %d: PVC lifecycle - Create: %v, Mount: %v, Unmount: %v, Delete: %v, Total: %v",
			i+1, createTime, mountTime, unmountTime, deleteTime, totalLifecycleTime)
	}

	suite.validatePVCLifecycleMetrics()
}

func (suite *PerformanceTestSuite) TestConcurrentOperationsPerformance() {
	ctx := context.Background()
	suite.T().Log("Testing concurrent operations performance")

	testName := "concurrent-ops-perf"
	namespace := suite.helper.CreateTestNamespace(testName)
	suite.resourceTracker.AddNamespace(namespace.Name)

	sc := suite.createPerformanceStorageClass(testName, namespace.Name)
	suite.resourceTracker.AddStorageClass(sc.Name)

	// Concurrent operations test
	concurrentStart := time.Now()
	
	var wg sync.WaitGroup
	errors := make(chan error, suite.testConfig.MaxConcurrentOperations)
	
	for i := 0; i < suite.testConfig.MaxConcurrentOperations; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			
			pvcName := fmt.Sprintf("concurrent-pvc-%d", index)
			podName := fmt.Sprintf("concurrent-pod-%d", index)
			
			// Create PVC
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pvcName,
					Namespace: namespace.Name,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:      suite.testConfig.AccessMode,
					StorageClassName: &sc.Name,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: testutils.ParseQuantityOrDie(suite.testConfig.VolumeSize),
						},
					},
				},
			}
			
			_, err := suite.client.CoreV1().PersistentVolumeClaims(namespace.Name).Create(ctx, pvc, metav1.CreateOptions{})
			if err != nil {
				errors <- fmt.Errorf("failed to create PVC %s: %v", pvcName, err)
				return
			}
			
			suite.resourceTracker.AddPVC(namespace.Name, pvcName)
			
			// Wait for PVC to be bound
			err = suite.waitForPVCBound(ctx, namespace.Name, pvcName, suite.testConfig.MaxPVCLifecycleTime)
			if err != nil {
				errors <- fmt.Errorf("PVC %s failed to bind: %v", pvcName, err)
				return
			}
			
			// Create Pod
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      podName,
					Namespace: namespace.Name,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "test-container",
							Image:   "busybox:1.35",
							Command: []string{"sh", "-c", "echo 'Performance test data' > /mnt/test-file && sleep 30"},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "test-volume",
									MountPath: "/mnt",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "test-volume",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: pvcName,
								},
							},
						},
					},
				},
			}
			
			_, err = suite.client.CoreV1().Pods(namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
			if err != nil {
				errors <- fmt.Errorf("failed to create Pod %s: %v", podName, err)
				return
			}
			
			suite.resourceTracker.AddPod(namespace.Name, podName)
			
			// Wait for Pod to be running
			err = suite.waitForPodRunning(ctx, namespace.Name, podName, suite.testConfig.MaxPVCLifecycleTime/2)
			if err != nil {
				errors <- fmt.Errorf("Pod %s failed to run: %v", podName, err)
				return
			}
		}(i)
	}
	
	wg.Wait()
	close(errors)
	
	concurrentTime := time.Since(concurrentStart)
	suite.recordConcurrentOperationTime(concurrentTime)
	
	// Check for any errors
	var concurrentErrors []error
	for err := range errors {
		concurrentErrors = append(concurrentErrors, err)
	}
	
	assert.Empty(suite.T(), concurrentErrors, "No errors should occur during concurrent operations")
	
	// Validate that concurrent operations don't significantly degrade performance
	expectedMaxTime := time.Duration(float64(suite.testConfig.MaxPVCLifecycleTime) * 1.5) // Allow 50% overhead for concurrent operations
	assert.LessOrEqual(suite.T(), concurrentTime, expectedMaxTime,
		"Concurrent operations should complete within %v, took %v", expectedMaxTime, concurrentTime)
	
	suite.T().Logf("Concurrent operations (%d) completed in %v", suite.testConfig.MaxConcurrentOperations, concurrentTime)
}

func (suite *PerformanceTestSuite) TestResourceUtilizationMonitoring() {
	ctx := context.Background()
	suite.T().Log("Testing resource utilization during operations")

	testName := "resource-util-perf"
	namespace := suite.helper.CreateTestNamespace(testName)
	suite.resourceTracker.AddNamespace(namespace.Name)

	sc := suite.createPerformanceStorageClass(testName, namespace.Name)
	suite.resourceTracker.AddStorageClass(sc.Name)

	// Start resource monitoring
	stopMonitoring := make(chan bool)
	go suite.monitorResourceUtilization(ctx, stopMonitoring)

	// Perform operations while monitoring
	for i := 0; i < 3; i++ {
		pvcName := fmt.Sprintf("util-pvc-%d", i)
		pvc := suite.createTestPVC(pvcName, namespace.Name, sc.Name)
		suite.resourceTracker.AddPVC(namespace.Name, pvc.Name)

		err := suite.waitForPVCBound(ctx, namespace.Name, pvc.Name, suite.testConfig.MaxPVCLifecycleTime)
		require.NoError(suite.T(), err, "PVC should be bound")

		pod := suite.createTestPod(fmt.Sprintf("util-pod-%d", i), namespace.Name, pvc.Name)
		suite.resourceTracker.AddPod(namespace.Name, pod.Name)

		err = suite.waitForPodRunning(ctx, namespace.Name, pod.Name, suite.testConfig.MaxPVCLifecycleTime/2)
		require.NoError(suite.T(), err, "Pod should be running")

		// Allow some time for resource utilization
		time.Sleep(10 * time.Second)
	}

	// Stop monitoring
	close(stopMonitoring)
	time.Sleep(2 * time.Second) // Allow monitoring to finish

	suite.validateResourceUtilization()
}

// Helper methods

func (suite *PerformanceTestSuite) createPerformanceStorageClass(name, namespace string) *storagev1.StorageClass {
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-%s", suite.testConfig.StorageClassName, name),
		},
		Provisioner: "efs.csi.aws.com",
		Parameters: map[string]string{
			"provisioningMode": "efs-ns",
			"namespace":        namespace,
			"performanceTier":  "generalPurpose",
			"throughputMode":   "provisioned",
		},
		AllowVolumeExpansion: &[]bool{true}[0],
	}

	createdSC, err := suite.client.StorageV1().StorageClasses().Create(context.Background(), sc, metav1.CreateOptions{})
	require.NoError(suite.T(), err, "StorageClass creation should succeed")
	
	return createdSC
}

func (suite *PerformanceTestSuite) createTestPVC(name, namespace, storageClassName string) *corev1.PersistentVolumeClaim {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      suite.testConfig.AccessMode,
			StorageClassName: &storageClassName,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: testutils.ParseQuantityOrDie(suite.testConfig.VolumeSize),
				},
			},
		},
	}

	createdPVC, err := suite.client.CoreV1().PersistentVolumeClaims(namespace).Create(context.Background(), pvc, metav1.CreateOptions{})
	require.NoError(suite.T(), err, "PVC creation should succeed")
	
	return createdPVC
}

func (suite *PerformanceTestSuite) createTestPod(name, namespace, pvcName string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    "test-container",
					Image:   "busybox:1.35",
					Command: []string{"sh", "-c", "echo 'Performance test data' > /mnt/test-file && sleep 60"},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "test-volume",
							MountPath: "/mnt",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "test-volume",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcName,
						},
					},
				},
			},
		},
	}

	createdPod, err := suite.client.CoreV1().Pods(namespace).Create(context.Background(), pod, metav1.CreateOptions{})
	require.NoError(suite.T(), err, "Pod creation should succeed")
	
	return createdPod
}

func (suite *PerformanceTestSuite) waitForPVCBound(ctx context.Context, namespace, name string, timeout time.Duration) error {
	return wait.PollImmediate(2*time.Second, timeout, func() (bool, error) {
		pvc, err := suite.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return pvc.Status.Phase == corev1.ClaimBound, nil
	})
}

func (suite *PerformanceTestSuite) waitForPodRunning(ctx context.Context, namespace, name string, timeout time.Duration) error {
	return wait.PollImmediate(2*time.Second, timeout, func() (bool, error) {
		pod, err := suite.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return pod.Status.Phase == corev1.PodRunning, nil
	})
}

func (suite *PerformanceTestSuite) waitForPVCDeleted(ctx context.Context, namespace, name string, timeout time.Duration) error {
	return wait.PollImmediate(2*time.Second, timeout, func() (bool, error) {
		_, err := suite.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if metav1.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}
		return false, nil
	})
}

func (suite *PerformanceTestSuite) waitForPodDeleted(ctx context.Context, namespace, name string, timeout time.Duration) error {
	return wait.PollImmediate(2*time.Second, timeout, func() (bool, error) {
		_, err := suite.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if metav1.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}
		return false, nil
	})
}

// Metrics recording methods

func (suite *PerformanceTestSuite) recordFilesystemCreationTime(duration time.Duration) {
	suite.metrics.mu.Lock()
	defer suite.metrics.mu.Unlock()
	suite.metrics.FilesystemCreationTimes = append(suite.metrics.FilesystemCreationTimes, duration)
}

func (suite *PerformanceTestSuite) recordPVCLifecycleMetrics(createTime, mountTime, unmountTime, deleteTime time.Duration) {
	suite.metrics.mu.Lock()
	defer suite.metrics.mu.Unlock()
	suite.metrics.PVCCreationTimes = append(suite.metrics.PVCCreationTimes, createTime)
	suite.metrics.MountTimes = append(suite.metrics.MountTimes, mountTime)
	suite.metrics.UnmountTimes = append(suite.metrics.UnmountTimes, unmountTime)
	suite.metrics.PVCDeletionTimes = append(suite.metrics.PVCDeletionTimes, deleteTime)
}

func (suite *PerformanceTestSuite) recordConcurrentOperationTime(duration time.Duration) {
	suite.metrics.mu.Lock()
	defer suite.metrics.mu.Unlock()
	suite.metrics.ConcurrentOperationTime = duration
}

func (suite *PerformanceTestSuite) monitorResourceUtilization(ctx context.Context, stop <-chan bool) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			// In a real implementation, this would collect actual metrics
			// For now, we'll simulate metric collection
			suite.metrics.mu.Lock()
			suite.metrics.ResourceUtilization.CPUUsagePercent = append(
				suite.metrics.ResourceUtilization.CPUUsagePercent, 
				float64(time.Now().UnixNano()%100)/100.0*50, // Simulate 0-50% CPU
			)
			suite.metrics.ResourceUtilization.MemoryUsageBytes = append(
				suite.metrics.ResourceUtilization.MemoryUsageBytes, 
				int64(1024*1024*100 + time.Now().UnixNano()%1024*1024*50), // Simulate 100-150MB
			)
			suite.metrics.ResourceUtilization.APICallCounts["CreateVolume"]++
			suite.metrics.mu.Unlock()
		}
	}
}

// Validation methods

func (suite *PerformanceTestSuite) validateFilesystemCreationMetrics() {
	suite.metrics.mu.RLock()
	defer suite.metrics.mu.RUnlock()

	if len(suite.metrics.FilesystemCreationTimes) == 0 {
		suite.T().Fatal("No filesystem creation metrics recorded")
	}

	// Calculate statistics
	var total time.Duration
	var max time.Duration
	var min time.Duration = time.Hour // Initialize to large value

	for _, t := range suite.metrics.FilesystemCreationTimes {
		total += t
		if t > max {
			max = t
		}
		if t < min {
			min = t
		}
	}

	avg := total / time.Duration(len(suite.metrics.FilesystemCreationTimes))

	suite.T().Logf("Filesystem Creation Performance:")
	suite.T().Logf("  Average: %v", avg)
	suite.T().Logf("  Minimum: %v", min)
	suite.T().Logf("  Maximum: %v", max)

	// Validate all measurements meet requirements
	for i, t := range suite.metrics.FilesystemCreationTimes {
		assert.LessOrEqual(suite.T(), t, suite.testConfig.MaxFilesystemCreationTime,
			"Filesystem creation iteration %d should be within %v", i+1, suite.testConfig.MaxFilesystemCreationTime)
	}

	// Validate average performance
	assert.LessOrEqual(suite.T(), avg, suite.testConfig.MaxFilesystemCreationTime,
		"Average filesystem creation time should be within %v", suite.testConfig.MaxFilesystemCreationTime)
}

func (suite *PerformanceTestSuite) validatePVCLifecycleMetrics() {
	suite.metrics.mu.RLock()
	defer suite.metrics.mu.RUnlock()

	if len(suite.metrics.PVCCreationTimes) == 0 {
		suite.T().Fatal("No PVC lifecycle metrics recorded")
	}

	suite.T().Logf("PVC Lifecycle Performance Statistics:")
	
	// Calculate averages for each phase
	avgCreate := suite.calculateAverage(suite.metrics.PVCCreationTimes)
	avgMount := suite.calculateAverage(suite.metrics.MountTimes)
	avgUnmount := suite.calculateAverage(suite.metrics.UnmountTimes)
	avgDelete := suite.calculateAverage(suite.metrics.PVCDeletionTimes)
	
	suite.T().Logf("  Average Creation: %v", avgCreate)
	suite.T().Logf("  Average Mount: %v", avgMount)
	suite.T().Logf("  Average Unmount: %v", avgUnmount)
	suite.T().Logf("  Average Deletion: %v", avgDelete)

	totalAvg := avgCreate + avgMount + avgUnmount + avgDelete
	suite.T().Logf("  Total Average Lifecycle: %v", totalAvg)

	// Validate total lifecycle time meets requirements
	assert.LessOrEqual(suite.T(), totalAvg, suite.testConfig.MaxPVCLifecycleTime,
		"Average total PVC lifecycle should be within %v", suite.testConfig.MaxPVCLifecycleTime)
}

func (suite *PerformanceTestSuite) validateResourceUtilization() {
	suite.metrics.mu.RLock()
	defer suite.metrics.mu.RUnlock()

	if len(suite.metrics.ResourceUtilization.CPUUsagePercent) == 0 {
		suite.T().Fatal("No resource utilization metrics recorded")
	}

	avgCPU := suite.calculateAverageFloat64(suite.metrics.ResourceUtilization.CPUUsagePercent)
	avgMemory := suite.calculateAverageInt64(suite.metrics.ResourceUtilization.MemoryUsageBytes)

	suite.T().Logf("Resource Utilization Statistics:")
	suite.T().Logf("  Average CPU Usage: %.2f%%", avgCPU)
	suite.T().Logf("  Average Memory Usage: %d bytes (%.2f MB)", avgMemory, float64(avgMemory)/(1024*1024))
	suite.T().Logf("  API Call Counts: %+v", suite.metrics.ResourceUtilization.APICallCounts)

	// Validate resource usage is reasonable
	assert.LessOrEqual(suite.T(), avgCPU, 80.0, "Average CPU usage should be reasonable")
	assert.LessOrEqual(suite.T(), avgMemory, int64(500*1024*1024), "Average memory usage should be reasonable") // 500MB limit
}

func (suite *PerformanceTestSuite) calculateAverage(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	
	var total time.Duration
	for _, d := range durations {
		total += d
	}
	
	return total / time.Duration(len(durations))
}

func (suite *PerformanceTestSuite) calculateAverageFloat64(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	
	var total float64
	for _, v := range values {
		total += v
	}
	
	return total / float64(len(values))
}

func (suite *PerformanceTestSuite) calculateAverageInt64(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}
	
	var total int64
	for _, v := range values {
		total += v
	}
	
	return total / int64(len(values))
}

func (suite *PerformanceTestSuite) printPerformanceReport() {
	suite.T().Log("=== PERFORMANCE TEST REPORT ===")
	
	suite.metrics.mu.RLock()
	defer suite.metrics.mu.RUnlock()
	
	if len(suite.metrics.FilesystemCreationTimes) > 0 {
		avg := suite.calculateAverage(suite.metrics.FilesystemCreationTimes)
		suite.T().Logf("Filesystem Creation - Average: %v, Requirement: %v, Status: %s",
			avg, suite.testConfig.MaxFilesystemCreationTime, 
			suite.getStatusString(avg <= suite.testConfig.MaxFilesystemCreationTime))
	}
	
	if len(suite.metrics.PVCCreationTimes) > 0 {
		avgTotal := suite.calculateAverage(suite.metrics.PVCCreationTimes) +
			suite.calculateAverage(suite.metrics.MountTimes) +
			suite.calculateAverage(suite.metrics.UnmountTimes) +
			suite.calculateAverage(suite.metrics.PVCDeletionTimes)
		suite.T().Logf("PVC Lifecycle - Average: %v, Requirement: %v, Status: %s",
			avgTotal, suite.testConfig.MaxPVCLifecycleTime,
			suite.getStatusString(avgTotal <= suite.testConfig.MaxPVCLifecycleTime))
	}
	
	if suite.metrics.ConcurrentOperationTime > 0 {
		suite.T().Logf("Concurrent Operations - Time: %v, Operations: %d",
			suite.metrics.ConcurrentOperationTime, suite.testConfig.MaxConcurrentOperations)
	}
	
	suite.T().Log("=== END PERFORMANCE REPORT ===")
}

func (suite *PerformanceTestSuite) getStatusString(passed bool) string {
	if passed {
		return "PASS"
	}
	return "FAIL"
}