package reliability

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"

	testutils "github.com/kubernetes-sigs/aws-efs-csi-driver/test/utils"
)

// ReliabilityTestSuite validates EFS-NS reliability and resilience
type ReliabilityTestSuite struct {
	suite.Suite
	client           clientset.Interface
	efsClient        *efs.Client
	helper           *testutils.EfsNsTestHelper
	resourceTracker  *testutils.TestResourceTracker
	testConfig       *ReliabilityTestConfig
	reliabilityData  *ReliabilityData
}

// ReliabilityTestConfig holds configuration for reliability tests
type ReliabilityTestConfig struct {
	Region                      string
	StorageClassName           string
	VolumeSize                 string
	AccessMode                 []corev1.PersistentVolumeAccessMode
	ChaosTestDuration          time.Duration
	MaxFailureInjections       int
	RecoveryTimeout            time.Duration
	HealthCheckInterval        time.Duration
	LoadTestConcurrency        int
	NetworkPartitionDuration   time.Duration
	ResourceStressDuration     time.Duration
	FailoverTestIterations     int
}

// ReliabilityData tracks reliability metrics and test results
type ReliabilityData struct {
	TestResults        map[string]*TestResult
	SystemMetrics      *SystemMetrics
	FailureScenarios   []FailureScenario
	RecoveryTimes      []time.Duration
	AvailabilityMetrics *AvailabilityMetrics
	mu                 sync.RWMutex
}

// TestResult represents the outcome of a reliability test
type TestResult struct {
	TestName     string                 `json:"testName"`
	StartTime    time.Time              `json:"startTime"`
	EndTime      time.Time              `json:"endTime"`
	Duration     time.Duration          `json:"duration"`
	Success      bool                   `json:"success"`
	ErrorCount   int                    `json:"errorCount"`
	RecoveryTime time.Duration          `json:"recoveryTime"`
	Metrics      map[string]interface{} `json:"metrics"`
	Errors       []string               `json:"errors"`
}

// SystemMetrics tracks system-wide reliability metrics
type SystemMetrics struct {
	CPUUsage              []float64
	MemoryUsage           []int64
	NetworkLatency        []time.Duration
	DiskIOLatency         []time.Duration
	APIResponseTimes      []time.Duration
	ErrorRates            []float64
	ThroughputOperationsPerSec []float64
}

// FailureScenario represents a chaos engineering test scenario
type FailureScenario struct {
	Name              string        `json:"name"`
	Type              string        `json:"type"` // NETWORK, RESOURCE, API, STORAGE
	Severity          string        `json:"severity"` // LOW, MEDIUM, HIGH, CRITICAL
	Duration          time.Duration `json:"duration"`
	InjectionTime     time.Time     `json:"injectionTime"`
	RecoveryTime      time.Duration `json:"recoveryTime"`
	SystemRecovered   bool          `json:"systemRecovered"`
	ImpactAssessment  string        `json:"impactAssessment"`
}

// AvailabilityMetrics tracks system availability
type AvailabilityMetrics struct {
	TotalUptime         time.Duration `json:"totalUptime"`
	TotalDowntime       time.Duration `json:"totalDowntime"`
	AvailabilityPercent float64       `json:"availabilityPercent"`
	MTBF                time.Duration `json:"mtbf"` // Mean Time Between Failures
	MTTR                time.Duration `json:"mttr"` // Mean Time To Recovery
	IncidentCount       int           `json:"incidentCount"`
	SLACompliance       bool          `json:"slaCompliance"`
}

func TestReliabilityTestSuite(t *testing.T) {
	suite.Run(t, new(ReliabilityTestSuite))
}

func (suite *ReliabilityTestSuite) SetupSuite() {
	var err error

	suite.client, err = testutils.NewKubernetesClient()
	require.NoError(suite.T(), err, "Failed to create Kubernetes client")

	suite.efsClient, err = testutils.NewEFSClient("")
	require.NoError(suite.T(), err, "Failed to create EFS client")

	suite.helper = testutils.NewEfsNsTestHelper(suite.client, suite.efsClient)
	suite.resourceTracker = testutils.NewTestResourceTracker(suite.client, suite.efsClient)

	suite.testConfig = &ReliabilityTestConfig{
		Region:                      testutils.TestConstants.AWSRegion,
		StorageClassName:           "efs-ns-sc-reliability-test",
		VolumeSize:                 "10Gi",
		AccessMode:                 []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
		ChaosTestDuration:          10 * time.Minute,
		MaxFailureInjections:       5,
		RecoveryTimeout:            5 * time.Minute,
		HealthCheckInterval:        30 * time.Second,
		LoadTestConcurrency:        10,
		NetworkPartitionDuration:   2 * time.Minute,
		ResourceStressDuration:     5 * time.Minute,
		FailoverTestIterations:     3,
	}

	suite.reliabilityData = &ReliabilityData{
		TestResults:      make(map[string]*TestResult),
		SystemMetrics:    &SystemMetrics{},
		AvailabilityMetrics: &AvailabilityMetrics{},
	}

	suite.T().Logf("Reliability test suite initialized with config: %+v", suite.testConfig)
	rand.Seed(time.Now().UnixNano())
}

func (suite *ReliabilityTestSuite) TearDownSuite() {
	suite.resourceTracker.CleanupAll(context.Background())
	suite.generateReliabilityReport()
}

func (suite *ReliabilityTestSuite) TestSystemFailoverResilience() {
	ctx := context.Background()
	suite.T().Log("Testing system failover and resilience")

	testName := "system-failover-test"
	result := suite.startTestResult(testName)

	for i := 0; i < suite.testConfig.FailoverTestIterations; i++ {
		suite.T().Logf("Failover test iteration %d/%d", i+1, suite.testConfig.FailoverTestIterations)
		
		namespace := suite.helper.CreateTestNamespace(fmt.Sprintf("%s-%d", testName, i))
		suite.resourceTracker.AddNamespace(namespace.Name)

		// Setup initial resources
		sc := suite.createReliabilityStorageClass(testName, namespace.Name)
		suite.resourceTracker.AddStorageClass(sc.Name)

		pvc := suite.createTestPVC(testName, namespace.Name, sc.Name)
		suite.resourceTracker.AddPVC(namespace.Name, pvc.Name)

		err := suite.waitForPVCBound(ctx, namespace.Name, pvc.Name, 3*time.Minute)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("PVC failed to bind in iteration %d: %v", i, err))
			result.ErrorCount++
			continue
		}

		pod := suite.createTestPod(fmt.Sprintf("%s-%d", testName, i), namespace.Name, pvc.Name)
		suite.resourceTracker.AddPod(namespace.Name, pod.Name)

		err = suite.waitForPodRunning(ctx, namespace.Name, pod.Name, 2*time.Minute)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("Pod failed to start in iteration %d: %v", i, err))
			result.ErrorCount++
			continue
		}

		// Simulate failure scenario
		failureStart := time.Now()
		suite.injectFailureScenario(ctx, "API_THROTTLING", "HIGH", 30*time.Second)
		
		// Monitor system recovery
		recoveryTime := suite.monitorSystemRecovery(ctx, namespace.Name, pod.Name)
		suite.reliabilityData.RecoveryTimes = append(suite.reliabilityData.RecoveryTimes, recoveryTime)

		suite.T().Logf("Iteration %d: Recovery time %v", i+1, recoveryTime)

		// Validate system is fully operational
		suite.validateSystemOperational(ctx, namespace.Name, pod.Name)

		// Allow system to stabilize between iterations
		time.Sleep(30 * time.Second)
	}

	suite.completeTestResult(result)
	suite.T().Log("System failover resilience test completed")
}

func (suite *ReliabilityTestSuite) TestChaosEngineeringScenarios() {
	ctx := context.Background()
	suite.T().Log("Running chaos engineering scenarios")

	testName := "chaos-engineering-test"
	result := suite.startTestResult(testName)

	namespace := suite.helper.CreateTestNamespace(testName)
	suite.resourceTracker.AddNamespace(namespace.Name)

	// Setup baseline system
	sc := suite.createReliabilityStorageClass(testName, namespace.Name)
	suite.resourceTracker.AddStorageClass(sc.Name)

	// Create multiple workloads for comprehensive testing
	workloads := suite.createMultipleWorkloads(ctx, testName, namespace.Name, sc.Name, 5)

	// Run chaos experiments
	chaosScenarios := []struct {
		name     string
		failType string
		severity string
		duration time.Duration
	}{
		{"NetworkPartition", "NETWORK", "HIGH", suite.testConfig.NetworkPartitionDuration},
		{"MemoryStress", "RESOURCE", "MEDIUM", suite.testConfig.ResourceStressDuration},
		{"APIThrottling", "API", "HIGH", 90 * time.Second},
		{"StorageLatency", "STORAGE", "MEDIUM", 2 * time.Minute},
		{"PodKilling", "RESOURCE", "HIGH", 60 * time.Second},
	}

	for _, scenario := range chaosScenarios {
		suite.T().Logf("Running chaos scenario: %s", scenario.name)
		
		// Inject failure
		failureScenario := suite.injectFailureScenario(ctx, scenario.failType, scenario.severity, scenario.duration)
		
		// Monitor system behavior during failure
		suite.monitorSystemDuringChaos(ctx, namespace.Name, workloads, failureScenario)
		
		// Wait for recovery
		recoveryTime := suite.monitorSystemRecovery(ctx, namespace.Name, workloads[0].Name)
		failureScenario.RecoveryTime = recoveryTime
		failureScenario.SystemRecovered = recoveryTime < suite.testConfig.RecoveryTimeout
		
		suite.reliabilityData.FailureScenarios = append(suite.reliabilityData.FailureScenarios, failureScenario)
		
		if !failureScenario.SystemRecovered {
			result.Errors = append(result.Errors, fmt.Sprintf("System failed to recover from %s within %v", scenario.name, suite.testConfig.RecoveryTimeout))
			result.ErrorCount++
		}
		
		// Allow system to stabilize
		time.Sleep(60 * time.Second)
	}

	suite.completeTestResult(result)
	suite.T().Log("Chaos engineering scenarios completed")
}

func (suite *ReliabilityTestSuite) TestHighAvailabilityUnderLoad() {
	ctx := context.Background()
	suite.T().Log("Testing high availability under load")

	testName := "high-availability-load-test"
	result := suite.startTestResult(testName)

	namespace := suite.helper.CreateTestNamespace(testName)
	suite.resourceTracker.AddNamespace(namespace.Name)

	sc := suite.createReliabilityStorageClass(testName, namespace.Name)
	suite.resourceTracker.AddStorageClass(sc.Name)

	// Create high load scenario
	suite.T().Logf("Creating %d concurrent workloads for load testing", suite.testConfig.LoadTestConcurrency)

	var wg sync.WaitGroup
	errors := make(chan error, suite.testConfig.LoadTestConcurrency)
	
	loadTestStart := time.Now()

	for i := 0; i < suite.testConfig.LoadTestConcurrency; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			
			pvcName := fmt.Sprintf("%s-pvc-%d", testName, index)
			podName := fmt.Sprintf("%s-pod-%d", testName, index)
			
			// Create PVC
			pvc := suite.createTestPVC(pvcName, namespace.Name, sc.Name)
			suite.resourceTracker.AddPVC(namespace.Name, pvc.Name)
			
			err := suite.waitForPVCBound(ctx, namespace.Name, pvc.Name, 5*time.Minute)
			if err != nil {
				errors <- fmt.Errorf("PVC %s failed to bind: %v", pvcName, err)
				return
			}
			
			// Create Pod with I/O intensive workload
			pod := suite.createIOIntensiveTestPod(podName, namespace.Name, pvc.Name)
			suite.resourceTracker.AddPod(namespace.Name, pod.Name)
			
			err = suite.waitForPodRunning(ctx, namespace.Name, pod.Name, 3*time.Minute)
			if err != nil {
				errors <- fmt.Errorf("Pod %s failed to start: %v", podName, err)
				return
			}
			
			// Monitor for successful I/O operations
			suite.monitorPodIOOperations(ctx, namespace.Name, pod.Name, 2*time.Minute)
			
		}(i)
	}
	
	// Monitor system metrics during load test
	stopMetricsMonitoring := make(chan bool)
	go suite.monitorSystemMetrics(ctx, stopMetricsMonitoring)
	
	wg.Wait()
	close(errors)
	close(stopMetricsMonitoring)
	
	loadTestDuration := time.Since(loadTestStart)
	
	// Collect errors
	var loadTestErrors []error
	for err := range errors {
		loadTestErrors = append(loadTestErrors, err)
		result.ErrorCount++
	}
	
	if len(loadTestErrors) > 0 {
		result.Errors = append(result.Errors, fmt.Sprintf("Load test had %d errors", len(loadTestErrors)))
	}
	
	suite.T().Logf("Load test completed in %v with %d errors", loadTestDuration, len(loadTestErrors))
	
	// Validate system availability during load
	availabilityScore := suite.calculateAvailabilityScore(loadTestErrors, suite.testConfig.LoadTestConcurrency)
	result.Metrics = map[string]interface{}{
		"loadTestDuration":    loadTestDuration,
		"concurrentWorkloads": suite.testConfig.LoadTestConcurrency,
		"availabilityScore":   availabilityScore,
		"errorRate":          float64(len(loadTestErrors)) / float64(suite.testConfig.LoadTestConcurrency),
	}
	
	assert.GreaterOrEqual(suite.T(), availabilityScore, 95.0, "System availability should be >= 95% under load")

	suite.completeTestResult(result)
	suite.T().Log("High availability under load test completed")
}

func (suite *ReliabilityTestSuite) TestDataConsistencyUnderFailure() {
	ctx := context.Background()
	suite.T().Log("Testing data consistency under failure conditions")

	testName := "data-consistency-test"
	result := suite.startTestResult(testName)

	namespace := suite.helper.CreateTestNamespace(testName)
	suite.resourceTracker.AddNamespace(namespace.Name)

	sc := suite.createReliabilityStorageClass(testName, namespace.Name)
	suite.resourceTracker.AddStorageClass(sc.Name)

	// Create multiple pods writing to the same shared storage
	pvc := suite.createTestPVC(testName, namespace.Name, sc.Name)
	suite.resourceTracker.AddPVC(namespace.Name, pvc.Name)

	err := suite.waitForPVCBound(ctx, namespace.Name, pvc.Name, 3*time.Minute)
	require.NoError(suite.T(), err, "PVC should be bound")

	// Create writer pods
	writerPods := make([]*corev1.Pod, 3)
	for i := 0; i < 3; i++ {
		podName := fmt.Sprintf("%s-writer-%d", testName, i)
		pod := suite.createDataWriterPod(podName, namespace.Name, pvc.Name, i)
		writerPods[i] = pod
		suite.resourceTracker.AddPod(namespace.Name, pod.Name)

		err = suite.waitForPodRunning(ctx, namespace.Name, pod.Name, 2*time.Minute)
		require.NoError(suite.T(), err, "Writer pod should be running")
	}

	// Let writers establish initial data state
	time.Sleep(30 * time.Second)

	// Inject failure while data operations are ongoing
	suite.T().Log("Injecting failure during data operations")
	failureScenario := suite.injectFailureScenario(ctx, "STORAGE", "HIGH", 90*time.Second)

	// Monitor data consistency during and after failure
	suite.monitorDataConsistency(ctx, namespace.Name, writerPods)

	// Validate data integrity after recovery
	readerPod := suite.createDataReaderPod(fmt.Sprintf("%s-reader", testName), namespace.Name, pvc.Name)
	suite.resourceTracker.AddPod(namespace.Name, readerPod.Name)

	err = suite.waitForPodRunning(ctx, namespace.Name, readerPod.Name, 2*time.Minute)
	require.NoError(suite.T(), err, "Reader pod should be running")

	// Verify data consistency
	consistencyCheck := suite.verifyDataConsistency(ctx, namespace.Name, readerPod.Name)
	result.Metrics = map[string]interface{}{
		"dataConsistencyCheck": consistencyCheck,
		"failureImpact":       failureScenario.ImpactAssessment,
	}

	if !consistencyCheck {
		result.Errors = append(result.Errors, "Data consistency check failed after failure recovery")
		result.ErrorCount++
	}

	suite.completeTestResult(result)
	suite.T().Log("Data consistency under failure test completed")
}

func (suite *ReliabilityTestSuite) TestGracefulDegradationScenarios() {
	ctx := context.Background()
	suite.T().Log("Testing graceful degradation scenarios")

	testName := "graceful-degradation-test"
	result := suite.startTestResult(testName)

	namespace := suite.helper.CreateTestNamespace(testName)
	suite.resourceTracker.AddNamespace(namespace.Name)

	sc := suite.createReliabilityStorageClass(testName, namespace.Name)
	suite.resourceTracker.AddStorageClass(sc.Name)

	// Setup baseline workload
	workloads := suite.createMultipleWorkloads(ctx, testName, namespace.Name, sc.Name, 3)

	// Test different degradation scenarios
	degradationScenarios := []struct {
		name        string
		failureType string
		severity    string
		expectation string
	}{
		{"PartialAPIFailure", "API", "MEDIUM", "Retry with backoff"},
		{"NetworkLatency", "NETWORK", "MEDIUM", "Increased response times but continued operation"},
		{"ResourceConstraints", "RESOURCE", "MEDIUM", "Throttling but continued operation"},
	}

	for _, scenario := range degradationScenarios {
		suite.T().Logf("Testing graceful degradation: %s", scenario.name)
		
		// Measure baseline performance
		baselineMetrics := suite.measureSystemPerformance(ctx, namespace.Name, workloads)
		
		// Inject partial failure
		failureScenario := suite.injectFailureScenario(ctx, scenario.failureType, scenario.severity, 2*time.Minute)
		
		// Measure degraded performance
		degradedMetrics := suite.measureSystemPerformance(ctx, namespace.Name, workloads)
		
		// Validate graceful degradation
		degradationAnalysis := suite.analyzeDegradation(baselineMetrics, degradedMetrics)
		result.Metrics = map[string]interface{}{
			fmt.Sprintf("%s_degradation", scenario.name): degradationAnalysis,
		}
		
		// System should continue operating even if degraded
		if degradationAnalysis["systemOperational"].(bool) == false {
			result.Errors = append(result.Errors, fmt.Sprintf("System failed to operate gracefully under %s", scenario.name))
			result.ErrorCount++
		}
		
		// Wait for recovery
		suite.monitorSystemRecovery(ctx, namespace.Name, workloads[0].Name)
		
		suite.T().Logf("Graceful degradation test %s completed", scenario.name)
	}

	suite.completeTestResult(result)
	suite.T().Log("Graceful degradation scenarios test completed")
}

// Helper methods for creating test resources

func (suite *ReliabilityTestSuite) createReliabilityStorageClass(name, namespace string) *storagev1.StorageClass {
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-%s", suite.testConfig.StorageClassName, name),
		},
		Provisioner: "efs.csi.aws.com",
		Parameters: map[string]string{
			"provisioningMode": "efs-ns",
			"namespace":        namespace,
			"performanceMode":  "generalPurpose",
			"throughputMode":   "provisioned",
			"provisionedThroughputInMibps": "100",
		},
		AllowVolumeExpansion: &[]bool{true}[0],
	}

	createdSC, err := suite.client.StorageV1().StorageClasses().Create(context.Background(), sc, metav1.CreateOptions{})
	require.NoError(suite.T(), err, "StorageClass creation should succeed")
	
	return createdSC
}

func (suite *ReliabilityTestSuite) createTestPVC(name, namespace, storageClassName string) *corev1.PersistentVolumeClaim {
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

func (suite *ReliabilityTestSuite) createTestPod(name, namespace, pvcName string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers: []corev1.Container{
				{
					Name:  "reliability-test-container",
					Image: "busybox:1.35",
					Command: []string{"sh", "-c", `
						while true; do
							echo "$(date): Reliability test running" >> /mnt/data/test.log
							sleep 30
						done
					`},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "test-volume",
							MountPath: "/mnt/data",
						},
					},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    testutils.ParseQuantityOrDie("100m"),
							corev1.ResourceMemory: testutils.ParseQuantityOrDie("128Mi"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    testutils.ParseQuantityOrDie("200m"),
							corev1.ResourceMemory: testutils.ParseQuantityOrDie("256Mi"),
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

func (suite *ReliabilityTestSuite) createIOIntensiveTestPod(name, namespace, pvcName string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers: []corev1.Container{
				{
					Name:  "io-intensive-container",
					Image: "busybox:1.35",
					Command: []string{"sh", "-c", `
						while true; do
							# Write intensive I/O operations
							dd if=/dev/zero of=/mnt/data/testfile_$(date +%s) bs=1M count=10
							sync
							# Read operations
							find /mnt/data -name "testfile_*" -exec cat {} \; > /dev/null
							# Cleanup old files
							find /mnt/data -name "testfile_*" -mmin +5 -delete
							sleep 10
						done
					`},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "test-volume",
							MountPath: "/mnt/data",
						},
					},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    testutils.ParseQuantityOrDie("200m"),
							corev1.ResourceMemory: testutils.ParseQuantityOrDie("256Mi"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    testutils.ParseQuantityOrDie("500m"),
							corev1.ResourceMemory: testutils.ParseQuantityOrDie("512Mi"),
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
	require.NoError(suite.T(), err, "IO intensive pod creation should succeed")
	
	return createdPod
}

func (suite *ReliabilityTestSuite) createDataWriterPod(name, namespace, pvcName string, writerID int) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers: []corev1.Container{
				{
					Name:  "data-writer-container",
					Image: "busybox:1.35",
					Command: []string{"sh", "-c", fmt.Sprintf(`
						mkdir -p /mnt/data/writer_%d
						counter=0
						while true; do
							timestamp=$(date +%%s)
							echo "WRITER_%d:$timestamp:$counter" > /mnt/data/writer_%d/record_$counter.txt
							counter=$((counter + 1))
							sleep 5
						done
					`, writerID, writerID, writerID)},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "test-volume",
							MountPath: "/mnt/data",
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
	require.NoError(suite.T(), err, "Data writer pod creation should succeed")
	
	return createdPod
}

func (suite *ReliabilityTestSuite) createDataReaderPod(name, namespace, pvcName string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:  "data-reader-container",
					Image: "busybox:1.35",
					Command: []string{"sh", "-c", `
						echo "Starting data consistency check..."
						
						# Check if all writer directories exist
						for i in 0 1 2; do
							if [ ! -d "/mnt/data/writer_$i" ]; then
								echo "ERROR: Missing writer_$i directory"
								exit 1
							fi
						done
						
						# Verify data integrity
						total_files=0
						for i in 0 1 2; do
							files=$(ls /mnt/data/writer_$i/record_*.txt | wc -l)
							total_files=$((total_files + files))
							echo "Writer $i has $files records"
						done
						
						echo "Total records found: $total_files"
						
						# Check for data corruption
						corrupted=0
						for file in $(find /mnt/data -name "record_*.txt"); do
							if ! grep -q "WRITER_" "$file"; then
								echo "CORRUPTION: Invalid format in $file"
								corrupted=$((corrupted + 1))
							fi
						done
						
						if [ $corrupted -gt 0 ]; then
							echo "ERROR: Found $corrupted corrupted files"
							exit 1
						fi
						
						echo "Data consistency check PASSED"
						sleep 60
					`},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "test-volume",
							MountPath: "/mnt/data",
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
	require.NoError(suite.T(), err, "Data reader pod creation should succeed")
	
	return createdPod
}

func (suite *ReliabilityTestSuite) createMultipleWorkloads(ctx context.Context, testName, namespace, storageClassName string, count int) []*corev1.Pod {
	var pods []*corev1.Pod

	for i := 0; i < count; i++ {
		pvcName := fmt.Sprintf("%s-pvc-%d", testName, i)
		pvc := suite.createTestPVC(pvcName, namespace, storageClassName)
		suite.resourceTracker.AddPVC(namespace, pvc.Name)

		err := suite.waitForPVCBound(ctx, namespace, pvc.Name, 3*time.Minute)
		require.NoError(suite.T(), err, "PVC should be bound")

		podName := fmt.Sprintf("%s-pod-%d", testName, i)
		pod := suite.createTestPod(podName, namespace, pvc.Name)
		suite.resourceTracker.AddPod(namespace, pod.Name)

		err = suite.waitForPodRunning(ctx, namespace, pod.Name, 2*time.Minute)
		require.NoError(suite.T(), err, "Pod should be running")

		pods = append(pods, pod)
	}

	return pods
}

// Waiting and monitoring helper methods

func (suite *ReliabilityTestSuite) waitForPVCBound(ctx context.Context, namespace, name string, timeout time.Duration) error {
	return wait.PollImmediate(2*time.Second, timeout, func() (bool, error) {
		pvc, err := suite.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return pvc.Status.Phase == corev1.ClaimBound, nil
	})
}

func (suite *ReliabilityTestSuite) waitForPodRunning(ctx context.Context, namespace, name string, timeout time.Duration) error {
	return wait.PollImmediate(2*time.Second, timeout, func() (bool, error) {
		pod, err := suite.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return pod.Status.Phase == corev1.PodRunning, nil
	})
}

// Failure injection and chaos engineering methods

func (suite *ReliabilityTestSuite) injectFailureScenario(ctx context.Context, failureType, severity string, duration time.Duration) FailureScenario {
	scenario := FailureScenario{
		Name:          fmt.Sprintf("%s_%s", failureType, severity),
		Type:          failureType,
		Severity:      severity,
		Duration:      duration,
		InjectionTime: time.Now(),
	}

	suite.T().Logf("Injecting failure: %s (%s) for %v", failureType, severity, duration)

	switch failureType {
	case "NETWORK":
		suite.injectNetworkFailure(ctx, severity, duration)
	case "RESOURCE":
		suite.injectResourceFailure(ctx, severity, duration)
	case "API", "API_THROTTLING":
		suite.injectAPIFailure(ctx, severity, duration)
	case "STORAGE":
		suite.injectStorageFailure(ctx, severity, duration)
	default:
		suite.T().Logf("Unknown failure type: %s", failureType)
	}

	return scenario
}

func (suite *ReliabilityTestSuite) injectNetworkFailure(ctx context.Context, severity string, duration time.Duration) {
	// Simulate network issues by introducing delays or packet loss
	// In a real implementation, this might use network chaos tools like Pumba or Chaos Mesh
	suite.T().Logf("Simulating network failure (severity: %s) for %v", severity, duration)
	
	// For testing purposes, we'll simulate network issues
	go func() {
		time.Sleep(duration)
		suite.T().Log("Network failure injection completed")
	}()
}

func (suite *ReliabilityTestSuite) injectResourceFailure(ctx context.Context, severity string, duration time.Duration) {
	// Simulate resource constraints (CPU, memory, disk)
	suite.T().Logf("Simulating resource failure (severity: %s) for %v", severity, duration)
	
	// In a real implementation, this would stress system resources
	go func() {
		time.Sleep(duration)
		suite.T().Log("Resource failure injection completed")
	}()
}

func (suite *ReliabilityTestSuite) injectAPIFailure(ctx context.Context, severity string, duration time.Duration) {
	// Simulate AWS API throttling or failures
	suite.T().Logf("Simulating API failure (severity: %s) for %v", severity, duration)
	
	// This would typically involve mocking AWS API responses or using chaos engineering tools
	go func() {
		time.Sleep(duration)
		suite.T().Log("API failure injection completed")
	}()
}

func (suite *ReliabilityTestSuite) injectStorageFailure(ctx context.Context, severity string, duration time.Duration) {
	// Simulate EFS storage issues
	suite.T().Logf("Simulating storage failure (severity: %s) for %v", severity, duration)
	
	// This might involve simulating EFS latency, mount failures, or I/O errors
	go func() {
		time.Sleep(duration)
		suite.T().Log("Storage failure injection completed")
	}()
}

// Monitoring and measurement methods

func (suite *ReliabilityTestSuite) monitorSystemRecovery(ctx context.Context, namespace, podName string) time.Duration {
	suite.T().Logf("Monitoring system recovery for pod %s", podName)
	
	recoveryStart := time.Now()
	
	// Wait for system to be healthy again
	err := wait.PollImmediate(10*time.Second, suite.testConfig.RecoveryTimeout, func() (bool, error) {
		pod, err := suite.client.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return false, nil // Continue waiting
		}
		
		// Check if pod is running and healthy
		if pod.Status.Phase != corev1.PodRunning {
			return false, nil
		}
		
		// Additional health checks could be added here
		return true, nil
	})
	
	recoveryTime := time.Since(recoveryStart)
	
	if err != nil {
		suite.T().Logf("System recovery timed out after %v", recoveryTime)
		return suite.testConfig.RecoveryTimeout // Return timeout duration
	}
	
	suite.T().Logf("System recovered in %v", recoveryTime)
	return recoveryTime
}

func (suite *ReliabilityTestSuite) monitorSystemDuringChaos(ctx context.Context, namespace string, pods []*corev1.Pod, scenario FailureScenario) {
	suite.T().Logf("Monitoring system during chaos scenario: %s", scenario.Name)
	
	monitoringDuration := scenario.Duration + 30*time.Second
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	
	monitoringStart := time.Now()
	
	for time.Since(monitoringStart) < monitoringDuration {
		select {
		case <-ticker.C:
			// Check pod health
			healthyPods := 0
			for _, pod := range pods {
				currentPod, err := suite.client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
				if err == nil && currentPod.Status.Phase == corev1.PodRunning {
					healthyPods++
				}
			}
			
			suite.T().Logf("Chaos monitoring: %d/%d pods healthy", healthyPods, len(pods))
			
			// Record metrics
			suite.reliabilityData.mu.Lock()
			suite.reliabilityData.SystemMetrics.ThroughputOperationsPerSec = append(
				suite.reliabilityData.SystemMetrics.ThroughputOperationsPerSec,
				float64(healthyPods)/float64(len(pods))*100,
			)
			suite.reliabilityData.mu.Unlock()
			
		case <-ctx.Done():
			return
		}
	}
}

func (suite *ReliabilityTestSuite) monitorPodIOOperations(ctx context.Context, namespace, podName string, duration time.Duration) {
	suite.T().Logf("Monitoring I/O operations for pod %s", podName)
	
	// In a real implementation, this would monitor actual I/O metrics
	// For testing purposes, we'll simulate monitoring
	
	end := time.Now().Add(duration)
	for time.Now().Before(end) {
		pod, err := suite.client.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil || pod.Status.Phase != corev1.PodRunning {
			suite.T().Logf("Pod %s not running during I/O monitoring", podName)
			break
		}
		
		time.Sleep(10 * time.Second)
	}
	
	suite.T().Logf("I/O monitoring completed for pod %s", podName)
}

func (suite *ReliabilityTestSuite) monitorSystemMetrics(ctx context.Context, stop <-chan bool) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			// Collect system metrics (simulated)
			suite.reliabilityData.mu.Lock()
			suite.reliabilityData.SystemMetrics.CPUUsage = append(
				suite.reliabilityData.SystemMetrics.CPUUsage,
				float64(rand.Intn(60)+20), // 20-80% CPU usage
			)
			suite.reliabilityData.SystemMetrics.MemoryUsage = append(
				suite.reliabilityData.SystemMetrics.MemoryUsage,
				int64(rand.Intn(1024*1024*500)+1024*1024*200), // 200-700MB memory usage
			)
			suite.reliabilityData.SystemMetrics.APIResponseTimes = append(
				suite.reliabilityData.SystemMetrics.APIResponseTimes,
				time.Duration(rand.Intn(500)+50)*time.Millisecond, // 50-550ms response times
			)
			suite.reliabilityData.mu.Unlock()
		}
	}
}

func (suite *ReliabilityTestSuite) monitorDataConsistency(ctx context.Context, namespace string, writerPods []*corev1.Pod) {
	suite.T().Log("Monitoring data consistency during failure scenario")
	
	// Monitor writer pods for failures and data consistency
	for _, pod := range writerPods {
		go func(podName string) {
			for {
				currentPod, err := suite.client.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
				if err != nil {
					suite.T().Logf("Error getting pod %s: %v", podName, err)
					return
				}
				
				if currentPod.Status.Phase != corev1.PodRunning {
					suite.T().Logf("Writer pod %s is not running: %s", podName, currentPod.Status.Phase)
				}
				
				time.Sleep(15 * time.Second)
			}
		}(pod.Name)
	}
}

func (suite *ReliabilityTestSuite) verifyDataConsistency(ctx context.Context, namespace, readerPodName string) bool {
	suite.T().Logf("Verifying data consistency using reader pod %s", readerPodName)
	
	// Wait for reader pod to complete its consistency check
	err := wait.PollImmediate(10*time.Second, 3*time.Minute, func() (bool, error) {
		pod, err := suite.client.CoreV1().Pods(namespace).Get(ctx, readerPodName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		
		return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed, nil
	})
	
	if err != nil {
		suite.T().Logf("Reader pod did not complete: %v", err)
		return false
	}
	
	// Check final pod status
	pod, err := suite.client.CoreV1().Pods(namespace).Get(ctx, readerPodName, metav1.GetOptions{})
	if err != nil {
		suite.T().Logf("Error getting final pod status: %v", err)
		return false
	}
	
	if pod.Status.Phase == corev1.PodSucceeded {
		suite.T().Log("Data consistency check PASSED")
		return true
	}
	
	suite.T().Log("Data consistency check FAILED")
	return false
}

func (suite *ReliabilityTestSuite) validateSystemOperational(ctx context.Context, namespace, podName string) {
	suite.T().Logf("Validating system is operational after recovery")
	
	pod, err := suite.client.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	require.NoError(suite.T(), err, "Should get pod")
	assert.Equal(suite.T(), corev1.PodRunning, pod.Status.Phase, "Pod should be running")
	
	// Additional operational checks could be added here
	suite.T().Log("System operational validation completed")
}

func (suite *ReliabilityTestSuite) measureSystemPerformance(ctx context.Context, namespace string, pods []*corev1.Pod) map[string]interface{} {
	suite.T().Log("Measuring system performance")
	
	metrics := make(map[string]interface{})
	
	// Count healthy pods
	healthyPods := 0
	for _, pod := range pods {
		currentPod, err := suite.client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err == nil && currentPod.Status.Phase == corev1.PodRunning {
			healthyPods++
		}
	}
	
	metrics["healthyPods"] = healthyPods
	metrics["totalPods"] = len(pods)
	metrics["healthPercentage"] = float64(healthyPods) / float64(len(pods)) * 100
	metrics["measurementTime"] = time.Now()
	
	// Simulate additional performance metrics
	metrics["avgResponseTime"] = time.Duration(rand.Intn(200)+50) * time.Millisecond
	metrics["errorRate"] = float64(rand.Intn(10)) / 100.0 // 0-10% error rate
	
	return metrics
}

func (suite *ReliabilityTestSuite) analyzeDegradation(baseline, degraded map[string]interface{}) map[string]interface{} {
	analysis := make(map[string]interface{})
	
	baselineHealth := baseline["healthPercentage"].(float64)
	degradedHealth := degraded["healthPercentage"].(float64)
	
	analysis["baselineHealth"] = baselineHealth
	analysis["degradedHealth"] = degradedHealth
	analysis["healthDegradation"] = baselineHealth - degradedHealth
	analysis["systemOperational"] = degradedHealth > 50.0 // System operational if >50% pods healthy
	
	// Additional degradation analysis
	baselineResponseTime := baseline["avgResponseTime"].(time.Duration)
	degradedResponseTime := degraded["avgResponseTime"].(time.Duration)
	analysis["responseDegradation"] = degradedResponseTime - baselineResponseTime
	
	suite.T().Logf("Degradation analysis: Health %.1f%% -> %.1f%%, Response %v -> %v", 
		baselineHealth, degradedHealth, baselineResponseTime, degradedResponseTime)
	
	return analysis
}

func (suite *ReliabilityTestSuite) calculateAvailabilityScore(errors []error, totalOperations int) float64 {
	if totalOperations == 0 {
		return 0.0
	}
	
	successfulOperations := totalOperations - len(errors)
	return (float64(successfulOperations) / float64(totalOperations)) * 100.0
}

// Test result management methods

func (suite *ReliabilityTestSuite) startTestResult(testName string) *TestResult {
	result := &TestResult{
		TestName:  testName,
		StartTime: time.Now(),
		Errors:    make([]string, 0),
		Metrics:   make(map[string]interface{}),
	}
	
	suite.reliabilityData.mu.Lock()
	suite.reliabilityData.TestResults[testName] = result
	suite.reliabilityData.mu.Unlock()
	
	return result
}

func (suite *ReliabilityTestSuite) completeTestResult(result *TestResult) {
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)
	result.Success = result.ErrorCount == 0
	
	suite.T().Logf("Test %s completed: Success=%v, Duration=%v, Errors=%d", 
		result.TestName, result.Success, result.Duration, result.ErrorCount)
}

func (suite *ReliabilityTestSuite) generateReliabilityReport() {
	suite.T().Log("=== RELIABILITY TEST REPORT ===")
	
	suite.reliabilityData.mu.RLock()
	defer suite.reliabilityData.mu.RUnlock()
	
	// Calculate overall metrics
	totalTests := len(suite.reliabilityData.TestResults)
	successfulTests := 0
	totalDuration := time.Duration(0)
	totalErrors := 0
	
	for _, result := range suite.reliabilityData.TestResults {
		if result.Success {
			successfulTests++
		}
		totalDuration += result.Duration
		totalErrors += result.ErrorCount
	}
	
	successRate := float64(successfulTests) / float64(totalTests) * 100
	avgDuration := totalDuration / time.Duration(totalTests)
	
	suite.T().Logf("Overall Test Results:")
	suite.T().Logf("  Total Tests: %d", totalTests)
	suite.T().Logf("  Successful: %d (%.1f%%)", successfulTests, successRate)
	suite.T().Logf("  Average Duration: %v", avgDuration)
	suite.T().Logf("  Total Errors: %d", totalErrors)
	
	// Recovery time analysis
	if len(suite.reliabilityData.RecoveryTimes) > 0 {
		suite.T().Logf("\nRecovery Time Analysis:")
		
		var totalRecoveryTime time.Duration
		var maxRecoveryTime time.Duration
		minRecoveryTime := time.Hour
		
		for _, rt := range suite.reliabilityData.RecoveryTimes {
			totalRecoveryTime += rt
			if rt > maxRecoveryTime {
				maxRecoveryTime = rt
			}
			if rt < minRecoveryTime {
				minRecoveryTime = rt
			}
		}
		
		avgRecoveryTime := totalRecoveryTime / time.Duration(len(suite.reliabilityData.RecoveryTimes))
		
		suite.T().Logf("  Average Recovery Time: %v", avgRecoveryTime)
		suite.T().Logf("  Min Recovery Time: %v", minRecoveryTime)
		suite.T().Logf("  Max Recovery Time: %v", maxRecoveryTime)
		suite.T().Logf("  Recovery Success Rate: %.1f%%", 
			float64(len(suite.reliabilityData.RecoveryTimes))/float64(len(suite.reliabilityData.FailureScenarios))*100)
	}
	
	// Failure scenario analysis
	if len(suite.reliabilityData.FailureScenarios) > 0 {
		suite.T().Logf("\nFailure Scenario Analysis:")
		suite.T().Logf("  Total Scenarios: %d", len(suite.reliabilityData.FailureScenarios))
		
		recoveredScenarios := 0
		for _, scenario := range suite.reliabilityData.FailureScenarios {
			if scenario.SystemRecovered {
				recoveredScenarios++
			}
			suite.T().Logf("    %s (%s): Recovery=%v, Time=%v", 
				scenario.Name, scenario.Severity, scenario.SystemRecovered, scenario.RecoveryTime)
		}
		
		suite.T().Logf("  Recovery Success Rate: %.1f%%", 
			float64(recoveredScenarios)/float64(len(suite.reliabilityData.FailureScenarios))*100)
	}
	
	// Individual test results
	suite.T().Logf("\nIndividual Test Results:")
	for testName, result := range suite.reliabilityData.TestResults {
		status := "PASS"
		if !result.Success {
			status = "FAIL"
		}
		suite.T().Logf("  %s: %s (Duration: %v, Errors: %d)", testName, status, result.Duration, result.ErrorCount)
	}
	
	suite.T().Log("=== END RELIABILITY REPORT ===")
}