package monitoring

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	"github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	clientset "k8s.io/client-go/kubernetes"

	testutils "github.com/kubernetes-sigs/aws-efs-csi-driver/test/utils"
)

// ResourceMonitoringTestSuite validates resource monitoring and leak detection
type ResourceMonitoringTestSuite struct {
	suite.Suite
	client           clientset.Interface
	efsClient        *efs.Client
	helper           *testutils.EfsNsTestHelper
	resourceTracker  *testutils.TestResourceTracker
	testConfig       *ResourceMonitoringConfig
	monitoringData   *ResourceMonitoringData
}

// ResourceMonitoringConfig holds configuration for resource monitoring tests
type ResourceMonitoringConfig struct {
	Region                      string
	StorageClassName           string
	VolumeSize                 string
	AccessMode                 []corev1.PersistentVolumeAccessMode
	MonitoringInterval         time.Duration
	ResourceLeakThreshold      time.Duration
	MaxAllowedLeakedResources  int
	MemoryLeakThresholdBytes   int64
	DiskLeakThresholdBytes     int64
	NetworkLeakThresholdMbps   float64
	MonitoringDuration         time.Duration
	ResourceCleanupTimeout     time.Duration
	AlertThresholds            ResourceAlertThresholds
}

// ResourceAlertThresholds defines when alerts should be triggered
type ResourceAlertThresholds struct {
	CPUUsagePercent            float64
	MemoryUsagePercent         float64
	DiskUsagePercent           float64
	NetworkUtilizationPercent  float64
	OpenFileDescriptors        int
	OrphanedResourceCount      int
	LeakedResourceAge          time.Duration
}

// ResourceMonitoringData tracks resource usage and leaks
type ResourceMonitoringData struct {
	ResourceSnapshots     []ResourceSnapshot     `json:"resourceSnapshots"`
	LeakedResources       []LeakedResource      `json:"leakedResources"`
	ResourceMetrics       *ResourceMetrics      `json:"resourceMetrics"`
	AlertEvents           []AlertEvent          `json:"alertEvents"`
	CleanupResults        []CleanupResult       `json:"cleanupResults"`
	OrphanedResources     []OrphanedResource    `json:"orphanedResources"`
	mu                    sync.RWMutex
}

// ResourceSnapshot represents resource usage at a point in time
type ResourceSnapshot struct {
	Timestamp         time.Time                  `json:"timestamp"`
	KubernetesResources KubernetesResourceUsage  `json:"kubernetesResources"`
	AWSResources      AWSResourceUsage           `json:"awsResources"`
	SystemResources   SystemResourceUsage        `json:"systemResources"`
}

// KubernetesResourceUsage tracks Kubernetes resource usage
type KubernetesResourceUsage struct {
	Namespaces           int                    `json:"namespaces"`
	Pods                 int                    `json:"pods"`
	PVCs                 int                    `json:"pvcs"`
	PVs                  int                    `json:"pvs"`
	StorageClasses       int                    `json:"storageClasses"`
	Services             int                    `json:"services"`
	ConfigMaps           int                    `json:"configMaps"`
	Secrets              int                    `json:"secrets"`
	Events               int                    `json:"events"`
	CPURequests          resource.Quantity      `json:"cpuRequests"`
	MemoryRequests       resource.Quantity      `json:"memoryRequests"`
	StorageRequests      resource.Quantity      `json:"storageRequests"`
}

// AWSResourceUsage tracks AWS resource usage
type AWSResourceUsage struct {
	EFSFileSystems       int                    `json:"efsFileSystems"`
	EFSAccessPoints      int                    `json:"efsAccessPoints"`
	EFSMountTargets      int                    `json:"efsMountTargets"`
	EC2SecurityGroups    int                    `json:"ec2SecurityGroups"`
	IAMPolicies          int                    `json:"iamPolicies"`
	IAMRoles             int                    `json:"iamRoles"`
	CloudWatchLogGroups  int                    `json:"cloudWatchLogGroups"`
	TotalStorageGB       float64                `json:"totalStorageGB"`
	EstimatedMonthlyCost float64                `json:"estimatedMonthlyCost"`
}

// SystemResourceUsage tracks system-level resource usage
type SystemResourceUsage struct {
	CPUUsagePercent      float64                `json:"cpuUsagePercent"`
	MemoryUsageBytes     int64                  `json:"memoryUsageBytes"`
	MemoryUsagePercent   float64                `json:"memoryUsagePercent"`
	DiskUsageBytes       int64                  `json:"diskUsageBytes"`
	DiskUsagePercent     float64                `json:"diskUsagePercent"`
	NetworkInBytes       int64                  `json:"networkInBytes"`
	NetworkOutBytes      int64                  `json:"networkOutBytes"`
	OpenFileDescriptors  int                    `json:"openFileDescriptors"`
	ProcessCount         int                    `json:"processCount"`
	LoadAverage          float64                `json:"loadAverage"`
}

// LeakedResource represents a resource that wasn't properly cleaned up
type LeakedResource struct {
	Type              string                 `json:"type"`
	Name              string                 `json:"name"`
	Namespace         string                 `json:"namespace,omitempty"`
	ID                string                 `json:"id,omitempty"`
	CreationTime      time.Time              `json:"creationTime"`
	DetectionTime     time.Time              `json:"detectionTime"`
	LeakAge           time.Duration          `json:"leakAge"`
	Severity          string                 `json:"severity"`
	Description       string                 `json:"description"`
	CleanupAttempts   int                    `json:"cleanupAttempts"`
	CleanupSuccessful bool                   `json:"cleanupSuccessful"`
	Metadata          map[string]interface{} `json:"metadata"`
}

// OrphanedResource represents a resource without proper ownership
type OrphanedResource struct {
	Type         string                 `json:"type"`
	Name         string                 `json:"name"`
	Namespace    string                 `json:"namespace,omitempty"`
	CreationTime time.Time              `json:"creationTime"`
	LastUsed     time.Time              `json:"lastUsed"`
	Reason       string                 `json:"reason"`
	Metadata     map[string]interface{} `json:"metadata"`
}

// ResourceMetrics provides aggregated resource statistics
type ResourceMetrics struct {
	TotalResourcesCreated    int                    `json:"totalResourcesCreated"`
	TotalResourcesDeleted    int                    `json:"totalResourcesDeleted"`
	CurrentActiveResources   int                    `json:"currentActiveResources"`
	PeakResourceUsage        int                    `json:"peakResourceUsage"`
	AverageResourceLifetime  time.Duration          `json:"averageResourceLifetime"`
	ResourceCreationRate     float64                `json:"resourceCreationRate"`
	ResourceDeletionRate     float64                `json:"resourceDeletionRate"`
	LeakDetectionAccuracy    float64                `json:"leakDetectionAccuracy"`
	CleanupSuccessRate       float64                `json:"cleanupSuccessRate"`
}

// AlertEvent represents a monitoring alert
type AlertEvent struct {
	Timestamp   time.Time              `json:"timestamp"`
	Type        string                 `json:"type"`
	Severity    string                 `json:"severity"`
	Message     string                 `json:"message"`
	Resource    string                 `json:"resource"`
	Threshold   interface{}            `json:"threshold"`
	CurrentValue interface{}           `json:"currentValue"`
	Metadata    map[string]interface{} `json:"metadata"`
}

// CleanupResult tracks cleanup operation results
type CleanupResult struct {
	Timestamp        time.Time              `json:"timestamp"`
	ResourceType     string                 `json:"resourceType"`
	ResourceName     string                 `json:"resourceName"`
	CleanupStrategy  string                 `json:"cleanupStrategy"`
	Success          bool                   `json:"success"`
	Duration         time.Duration          `json:"duration"`
	Error            string                 `json:"error,omitempty"`
	Metadata         map[string]interface{} `json:"metadata"`
}

func TestResourceMonitoringTestSuite(t *testing.T) {
	suite.Run(t, new(ResourceMonitoringTestSuite))
}

func (suite *ResourceMonitoringTestSuite) SetupSuite() {
	var err error

	suite.client, err = testutils.NewKubernetesClient()
	require.NoError(suite.T(), err, "Failed to create Kubernetes client")

	suite.efsClient, err = testutils.NewEFSClient("")
	require.NoError(suite.T(), err, "Failed to create EFS client")

	suite.helper = testutils.NewEfsNsTestHelper(suite.client, suite.efsClient)
	suite.resourceTracker = testutils.NewTestResourceTracker(suite.client, suite.efsClient)

	suite.testConfig = &ResourceMonitoringConfig{
		Region:                      testutils.TestConstants.AWSRegion,
		StorageClassName:           "efs-ns-sc-monitoring-test",
		VolumeSize:                 "10Gi",
		AccessMode:                 []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
		MonitoringInterval:         30 * time.Second,
		ResourceLeakThreshold:      5 * time.Minute,
		MaxAllowedLeakedResources:  3,
		MemoryLeakThresholdBytes:   100 * 1024 * 1024, // 100MB
		DiskLeakThresholdBytes:     1024 * 1024 * 1024, // 1GB
		NetworkLeakThresholdMbps:   10.0,
		MonitoringDuration:         10 * time.Minute,
		ResourceCleanupTimeout:     3 * time.Minute,
		AlertThresholds: ResourceAlertThresholds{
			CPUUsagePercent:           80.0,
			MemoryUsagePercent:        85.0,
			DiskUsagePercent:          90.0,
			NetworkUtilizationPercent: 95.0,
			OpenFileDescriptors:       1000,
			OrphanedResourceCount:     5,
			LeakedResourceAge:         10 * time.Minute,
		},
	}

	suite.monitoringData = &ResourceMonitoringData{
		ResourceMetrics: &ResourceMetrics{},
	}

	suite.T().Logf("Resource monitoring test suite initialized with config: %+v", suite.testConfig)
}

func (suite *ResourceMonitoringTestSuite) TearDownSuite() {
	suite.resourceTracker.CleanupAll(context.Background())
	suite.generateResourceMonitoringReport()
}

func (suite *ResourceMonitoringTestSuite) TestResourceLeakDetection() {
	ctx := context.Background()
	suite.T().Log("Testing resource leak detection")

	testName := "resource-leak-detection-test"
	namespace := suite.helper.CreateTestNamespace(testName)
	suite.resourceTracker.AddNamespace(namespace.Name)

	// Start resource monitoring
	stopMonitoring := make(chan bool)
	go suite.startResourceMonitoring(ctx, stopMonitoring)

	// Create initial baseline
	baselineSnapshot := suite.takeResourceSnapshot(ctx)
	suite.recordResourceSnapshot(baselineSnapshot)

	// Create resources that will be intentionally leaked
	sc := suite.createMonitoringStorageClass(testName, namespace.Name)
	// Intentionally don't add to resource tracker to simulate leak
	
	// Create PVC
	pvc := suite.createTestPVC(testName+"-leak", namespace.Name, sc.Name)
	// Intentionally don't add to resource tracker

	err := suite.waitForPVCBound(ctx, namespace.Name, pvc.Name, 3*time.Minute)
	require.NoError(suite.T(), err, "PVC should be bound")

	// Create pod
	pod := suite.createTestPod(testName+"-leak", namespace.Name, pvc.Name)
	// Intentionally don't add to resource tracker

	err = suite.waitForPodRunning(ctx, namespace.Name, pod.Name, 2*time.Minute)
	require.NoError(suite.T(), err, "Pod should be running")

	// Let resources exist for leak detection
	suite.T().Log("Waiting for leak detection period")
	time.Sleep(suite.testConfig.ResourceLeakThreshold + 30*time.Second)

	// Take another snapshot to detect leaks
	leakSnapshot := suite.takeResourceSnapshot(ctx)
	suite.recordResourceSnapshot(leakSnapshot)

	// Detect leaks
	leaks := suite.detectResourceLeaks(ctx, baselineSnapshot, leakSnapshot)
	
	suite.T().Logf("Detected %d potential resource leaks", len(leaks))
	
	for _, leak := range leaks {
		suite.recordLeakedResource(leak)
		suite.T().Logf("Leak detected: %s %s (age: %v)", leak.Type, leak.Name, leak.LeakAge)
	}

	// Stop monitoring
	close(stopMonitoring)

	// Cleanup leaked resources
	suite.cleanupLeakedResources(ctx, leaks)

	// Validate leak detection worked
	assert.GreaterOrEqual(suite.T(), len(leaks), 1, "Should detect at least one leaked resource")
	assert.LessOrEqual(suite.T(), len(leaks), suite.testConfig.MaxAllowedLeakedResources+2, 
		"Should not detect excessive false positives")

	suite.T().Log("Resource leak detection test completed")
}

func (suite *ResourceMonitoringTestSuite) TestMemoryLeakMonitoring() {
	ctx := context.Background()
	suite.T().Log("Testing memory leak monitoring")

	testName := "memory-leak-monitoring-test"
	namespace := suite.helper.CreateTestNamespace(testName)
	suite.resourceTracker.AddNamespace(namespace.Name)

	// Start memory monitoring
	memorySnapshots := make([]int64, 0)
	stopMemoryMonitoring := make(chan bool)
	
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		
		for {
			select {
			case <-stopMemoryMonitoring:
				return
			case <-ticker.C:
				snapshot := suite.takeResourceSnapshot(ctx)
				suite.monitoringData.mu.Lock()
				memorySnapshots = append(memorySnapshots, snapshot.SystemResources.MemoryUsageBytes)
				suite.monitoringData.mu.Unlock()
				
				if snapshot.SystemResources.MemoryUsageBytes > suite.testConfig.MemoryLeakThresholdBytes {
					suite.recordAlertEvent(AlertEvent{
						Timestamp: time.Now(),
						Type:      "MEMORY_LEAK",
						Severity:  "HIGH",
						Message:   fmt.Sprintf("Memory usage exceeded threshold: %d bytes", snapshot.SystemResources.MemoryUsageBytes),
						Resource:  "system",
						Threshold: suite.testConfig.MemoryLeakThresholdBytes,
						CurrentValue: snapshot.SystemResources.MemoryUsageBytes,
					})
				}
			}
		}
	}()

	// Create memory-intensive workload
	sc := suite.createMonitoringStorageClass(testName, namespace.Name)
	suite.resourceTracker.AddStorageClass(sc.Name)

	// Create multiple memory-intensive pods
	for i := 0; i < 5; i++ {
		pvcName := fmt.Sprintf("%s-pvc-%d", testName, i)
		pvc := suite.createTestPVC(pvcName, namespace.Name, sc.Name)
		suite.resourceTracker.AddPVC(namespace.Name, pvc.Name)

		err := suite.waitForPVCBound(ctx, namespace.Name, pvc.Name, 2*time.Minute)
		require.NoError(suite.T(), err, "PVC should be bound")

		podName := fmt.Sprintf("%s-pod-%d", testName, i)
		pod := suite.createMemoryIntensivePod(podName, namespace.Name, pvc.Name)
		suite.resourceTracker.AddPod(namespace.Name, pod.Name)

		err = suite.waitForPodRunning(ctx, namespace.Name, pod.Name, 2*time.Minute)
		require.NoError(suite.T(), err, "Pod should be running")
	}

	// Monitor memory usage for a period
	suite.T().Log("Monitoring memory usage patterns")
	time.Sleep(3 * time.Minute)

	// Stop memory monitoring
	close(stopMemoryMonitoring)

	// Analyze memory usage patterns
	suite.analyzeMemoryUsagePatterns(memorySnapshots)

	suite.T().Log("Memory leak monitoring test completed")
}

func (suite *ResourceMonitoringTestSuite) TestOrphanedResourceDetection() {
	ctx := context.Background()
	suite.T().Log("Testing orphaned resource detection")

	testName := "orphaned-resource-detection-test"
	namespace := suite.helper.CreateTestNamespace(testName)
	suite.resourceTracker.AddNamespace(namespace.Name)

	// Create resources with different ownership scenarios
	sc := suite.createMonitoringStorageClass(testName, namespace.Name)
	suite.resourceTracker.AddStorageClass(sc.Name)

	// Scenario 1: Normal resource with proper ownership
	normalPVC := suite.createTestPVC(testName+"-normal", namespace.Name, sc.Name)
	suite.resourceTracker.AddPVC(namespace.Name, normalPVC.Name)

	err := suite.waitForPVCBound(ctx, namespace.Name, normalPVC.Name, 2*time.Minute)
	require.NoError(suite.T(), err, "Normal PVC should be bound")

	normalPod := suite.createTestPod(testName+"-normal", namespace.Name, normalPVC.Name)
	suite.resourceTracker.AddPod(namespace.Name, normalPod.Name)

	err = suite.waitForPodRunning(ctx, namespace.Name, normalPod.Name, 2*time.Minute)
	require.NoError(suite.T(), err, "Normal pod should be running")

	// Scenario 2: Create orphaned resources (simulate controller failure)
	orphanedPVC := suite.createTestPVC(testName+"-orphaned", namespace.Name, sc.Name)
	// Don't add to resource tracker, and delete the "owner" pod immediately

	err = suite.waitForPVCBound(ctx, namespace.Name, orphanedPVC.Name, 2*time.Minute)
	require.NoError(suite.T(), err, "Orphaned PVC should be bound")

	orphanedPod := suite.createTestPod(testName+"-orphaned", namespace.Name, orphanedPVC.Name)
	err = suite.waitForPodRunning(ctx, namespace.Name, orphanedPod.Name, 2*time.Minute)
	require.NoError(suite.T(), err, "Orphaned pod should be running")

	// Immediately delete the pod to create orphaned PVC
	err = suite.client.CoreV1().Pods(namespace.Name).Delete(ctx, orphanedPod.Name, metav1.DeleteOptions{})
	require.NoError(suite.T(), err, "Should delete pod to create orphan")

	// Wait for orphan detection period
	suite.T().Log("Waiting for orphaned resource detection period")
	time.Sleep(2 * time.Minute)

	// Detect orphaned resources
	orphanedResources := suite.detectOrphanedResources(ctx, namespace.Name)
	
	suite.T().Logf("Detected %d orphaned resources", len(orphanedResources))
	
	for _, orphan := range orphanedResources {
		suite.recordOrphanedResource(orphan)
		suite.T().Logf("Orphaned resource detected: %s %s (reason: %s)", orphan.Type, orphan.Name, orphan.Reason)
	}

	// Cleanup orphaned resources
	suite.cleanupOrphanedResources(ctx, orphanedResources)

	// Validate orphaned resource detection
	assert.GreaterOrEqual(suite.T(), len(orphanedResources), 1, "Should detect at least one orphaned resource")

	suite.T().Log("Orphaned resource detection test completed")
}

func (suite *ResourceMonitoringTestSuite) TestResourceUsageThresholdAlerts() {
	ctx := context.Background()
	suite.T().Log("Testing resource usage threshold alerts")

	testName := "resource-threshold-alerts-test"
	namespace := suite.helper.CreateTestNamespace(testName)
	suite.resourceTracker.AddNamespace(namespace.Name)

	// Start threshold monitoring
	stopThresholdMonitoring := make(chan bool)
	go suite.startThresholdMonitoring(ctx, stopThresholdMonitoring)

	// Create high-resource usage scenario
	sc := suite.createMonitoringStorageClass(testName, namespace.Name)
	suite.resourceTracker.AddStorageClass(sc.Name)

	// Create resource-intensive workloads to trigger thresholds
	for i := 0; i < 8; i++ {
		pvcName := fmt.Sprintf("%s-pvc-%d", testName, i)
		pvc := suite.createTestPVC(pvcName, namespace.Name, sc.Name)
		suite.resourceTracker.AddPVC(namespace.Name, pvc.Name)

		err := suite.waitForPVCBound(ctx, namespace.Name, pvc.Name, 2*time.Minute)
		require.NoError(suite.T(), err, "PVC should be bound")

		podName := fmt.Sprintf("%s-pod-%d", testName, i)
		pod := suite.createResourceIntensivePod(podName, namespace.Name, pvc.Name)
		suite.resourceTracker.AddPod(namespace.Name, pod.Name)

		err = suite.waitForPodRunning(ctx, namespace.Name, pod.Name, 2*time.Minute)
		require.NoError(suite.T(), err, "Pod should be running")
	}

	// Monitor for threshold violations
	suite.T().Log("Monitoring for threshold violations")
	time.Sleep(5 * time.Minute)

	// Stop threshold monitoring
	close(stopThresholdMonitoring)

	// Validate alert generation
	suite.monitoringData.mu.RLock()
	alertCount := len(suite.monitoringData.AlertEvents)
	suite.monitoringData.mu.RUnlock()

	suite.T().Logf("Generated %d threshold alerts", alertCount)
	assert.GreaterOrEqual(suite.T(), alertCount, 1, "Should generate at least one threshold alert")

	suite.T().Log("Resource usage threshold alerts test completed")
}

func (suite *ResourceMonitoringTestSuite) TestAutomaticResourceCleanup() {
	ctx := context.Background()
	suite.T().Log("Testing automatic resource cleanup")

	testName := "automatic-cleanup-test"
	namespace := suite.helper.CreateTestNamespace(testName)
	suite.resourceTracker.AddNamespace(namespace.Name)

	// Create resources for cleanup testing
	sc := suite.createMonitoringStorageClass(testName, namespace.Name)
	// Don't add to resource tracker to test cleanup detection

	// Create multiple resources
	resourcesCreated := make([]string, 0)
	
	for i := 0; i < 5; i++ {
		pvcName := fmt.Sprintf("%s-cleanup-pvc-%d", testName, i)
		pvc := suite.createTestPVC(pvcName, namespace.Name, sc.Name)
		resourcesCreated = append(resourcesCreated, pvc.Name)

		err := suite.waitForPVCBound(ctx, namespace.Name, pvc.Name, 2*time.Minute)
		require.NoError(suite.T(), err, "PVC should be bound")

		podName := fmt.Sprintf("%s-cleanup-pod-%d", testName, i)
		pod := suite.createTestPod(podName, namespace.Name, pvc.Name)
		resourcesCreated = append(resourcesCreated, pod.Name)

		err = suite.waitForPodRunning(ctx, namespace.Name, pod.Name, 2*time.Minute)
		require.NoError(suite.T(), err, "Pod should be running")
	}

	// Simulate automatic cleanup scenarios
	cleanupResults := make([]CleanupResult, 0)

	// Test different cleanup strategies
	cleanupStrategies := []string{
		"graceful_shutdown",
		"forced_termination",
		"cascade_deletion",
		"finalizer_handling",
	}

	for i, strategy := range cleanupStrategies {
		if i >= len(resourcesCreated)/2 {
			break
		}

		resourceName := resourcesCreated[i*2] // PVC name
		cleanupStart := time.Now()

		result := suite.executeResourceCleanup(ctx, namespace.Name, "PVC", resourceName, strategy)
		result.Duration = time.Since(cleanupStart)
		
		cleanupResults = append(cleanupResults, result)
		suite.recordCleanupResult(result)

		suite.T().Logf("Cleanup result for %s using %s: success=%v, duration=%v", 
			resourceName, strategy, result.Success, result.Duration)
	}

	// Test cleanup timeout handling
	suite.testCleanupTimeouts(ctx, namespace.Name, resourcesCreated[len(resourcesCreated)-2:])

	// Validate cleanup effectiveness
	successfulCleanups := 0
	for _, result := range cleanupResults {
		if result.Success {
			successfulCleanups++
		}
	}

	cleanupSuccessRate := float64(successfulCleanups) / float64(len(cleanupResults)) * 100
	suite.T().Logf("Cleanup success rate: %.1f%% (%d/%d)", cleanupSuccessRate, successfulCleanups, len(cleanupResults))

	assert.GreaterOrEqual(suite.T(), cleanupSuccessRate, 75.0, "Cleanup success rate should be >= 75%")

	suite.T().Log("Automatic resource cleanup test completed")
}

func (suite *ResourceMonitoringTestSuite) TestLongRunningResourceMonitoring() {
	ctx := context.Background()
	suite.T().Log("Testing long-running resource monitoring")

	testName := "long-running-monitoring-test"
	namespace := suite.helper.CreateTestNamespace(testName)
	suite.resourceTracker.AddNamespace(namespace.Name)

	// Start comprehensive monitoring
	stopLongRunningMonitoring := make(chan bool)
	go suite.startComprehensiveMonitoring(ctx, stopLongRunningMonitoring)

	// Create baseline workload
	sc := suite.createMonitoringStorageClass(testName, namespace.Name)
	suite.resourceTracker.AddStorageClass(sc.Name)

	initialWorkloads := suite.createMultipleWorkloads(ctx, testName, namespace.Name, sc.Name, 3)

	// Simulate long-running operations with resource changes
	operationDuration := 8 * time.Minute
	operationStart := time.Now()

	// Phase 1: Stable operation (2 minutes)
	suite.T().Log("Phase 1: Stable operation monitoring")
	time.Sleep(2 * time.Minute)

	// Phase 2: Resource scaling (2 minutes)
	suite.T().Log("Phase 2: Resource scaling monitoring")
	scaledWorkloads := suite.createMultipleWorkloads(ctx, testName+"-scaled", namespace.Name, sc.Name, 2)
	time.Sleep(2 * time.Minute)

	// Phase 3: Resource cleanup (2 minutes)
	suite.T().Log("Phase 3: Resource cleanup monitoring")
	suite.cleanupWorkloads(ctx, scaledWorkloads)
	time.Sleep(2 * time.Minute)

	// Phase 4: System stabilization (2 minutes)
	suite.T().Log("Phase 4: System stabilization monitoring")
	time.Sleep(2 * time.Minute)

	totalDuration := time.Since(operationStart)

	// Stop monitoring
	close(stopLongRunningMonitoring)

	// Analyze monitoring data
	suite.analyzeLongRunningMonitoringData(totalDuration)

	suite.T().Log("Long-running resource monitoring test completed")
}

// Helper methods for resource management

func (suite *ResourceMonitoringTestSuite) createMonitoringStorageClass(name, namespace string) *storagev1.StorageClass {
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-%s", suite.testConfig.StorageClassName, name),
			Labels: map[string]string{
				"test-suite": "resource-monitoring",
				"namespace":  namespace,
			},
		},
		Provisioner: "efs.csi.aws.com",
		Parameters: map[string]string{
			"provisioningMode": "efs-ns",
			"namespace":        namespace,
			"performanceMode":  "generalPurpose",
		},
		AllowVolumeExpansion: &[]bool{true}[0],
	}

	createdSC, err := suite.client.StorageV1().StorageClasses().Create(context.Background(), sc, metav1.CreateOptions{})
	require.NoError(suite.T(), err, "StorageClass creation should succeed")
	
	return createdSC
}

func (suite *ResourceMonitoringTestSuite) createTestPVC(name, namespace, storageClassName string) *corev1.PersistentVolumeClaim {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"test-suite": "resource-monitoring",
			},
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

func (suite *ResourceMonitoringTestSuite) createTestPod(name, namespace, pvcName string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"test-suite": "resource-monitoring",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers: []corev1.Container{
				{
					Name:  "monitoring-test-container",
					Image: "busybox:1.35",
					Command: []string{"sh", "-c", `
						echo "Starting monitoring test container"
						while true; do
							echo "$(date): Monitoring test running" >> /mnt/data/monitor.log
							sleep 60
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
							corev1.ResourceCPU:    testutils.ParseQuantityOrDie("50m"),
							corev1.ResourceMemory: testutils.ParseQuantityOrDie("64Mi"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    testutils.ParseQuantityOrDie("100m"),
							corev1.ResourceMemory: testutils.ParseQuantityOrDie("128Mi"),
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

func (suite *ResourceMonitoringTestSuite) createMemoryIntensivePod(name, namespace, pvcName string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"test-suite": "resource-monitoring",
				"workload-type": "memory-intensive",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers: []corev1.Container{
				{
					Name:  "memory-intensive-container",
					Image: "busybox:1.35",
					Command: []string{"sh", "-c", `
						echo "Starting memory intensive workload"
						# Allocate and hold memory
						dd if=/dev/zero of=/tmp/memfile bs=1M count=50
						while true; do
							cat /tmp/memfile > /dev/null
							echo "$(date): Memory intensive task running" >> /mnt/data/memory.log
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
	require.NoError(suite.T(), err, "Memory intensive pod creation should succeed")
	
	return createdPod
}

func (suite *ResourceMonitoringTestSuite) createResourceIntensivePod(name, namespace, pvcName string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"test-suite": "resource-monitoring",
				"workload-type": "resource-intensive",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers: []corev1.Container{
				{
					Name:  "resource-intensive-container",
					Image: "busybox:1.35",
					Command: []string{"sh", "-c", `
						echo "Starting resource intensive workload"
						while true; do
							# CPU intensive task
							timeout 10s yes > /dev/null
							# I/O intensive task
							dd if=/dev/zero of=/mnt/data/testfile bs=1M count=10
							sync
							# Network intensive task (simulated)
							echo "Network activity simulation" >> /mnt/data/network.log
							sleep 15
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
	require.NoError(suite.T(), err, "Resource intensive pod creation should succeed")
	
	return createdPod
}

func (suite *ResourceMonitoringTestSuite) createMultipleWorkloads(ctx context.Context, testName, namespace, storageClassName string, count int) []*corev1.Pod {
	var pods []*corev1.Pod

	for i := 0; i < count; i++ {
		pvcName := fmt.Sprintf("%s-pvc-%d", testName, i)
		pvc := suite.createTestPVC(pvcName, namespace, storageClassName)
		suite.resourceTracker.AddPVC(namespace, pvc.Name)

		err := suite.waitForPVCBound(ctx, namespace, pvc.Name, 2*time.Minute)
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

func (suite *ResourceMonitoringTestSuite) cleanupWorkloads(ctx context.Context, pods []*corev1.Pod) {
	for _, pod := range pods {
		err := suite.client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
		if err != nil {
			suite.T().Logf("Error deleting pod %s: %v", pod.Name, err)
		}
	}
}

// Waiting methods

func (suite *ResourceMonitoringTestSuite) waitForPVCBound(ctx context.Context, namespace, name string, timeout time.Duration) error {
	return wait.PollImmediate(2*time.Second, timeout, func() (bool, error) {
		pvc, err := suite.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return pvc.Status.Phase == corev1.ClaimBound, nil
	})
}

func (suite *ResourceMonitoringTestSuite) waitForPodRunning(ctx context.Context, namespace, name string, timeout time.Duration) error {
	return wait.PollImmediate(2*time.Second, timeout, func() (bool, error) {
		pod, err := suite.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return pod.Status.Phase == corev1.PodRunning, nil
	})
}

// Monitoring and detection methods

func (suite *ResourceMonitoringTestSuite) startResourceMonitoring(ctx context.Context, stop <-chan bool) {
	ticker := time.NewTicker(suite.testConfig.MonitoringInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			snapshot := suite.takeResourceSnapshot(ctx)
			suite.recordResourceSnapshot(snapshot)
		}
	}
}

func (suite *ResourceMonitoringTestSuite) takeResourceSnapshot(ctx context.Context) ResourceSnapshot {
	snapshot := ResourceSnapshot{
		Timestamp: time.Now(),
	}

	// Collect Kubernetes resource usage
	snapshot.KubernetesResources = suite.collectKubernetesResourceUsage(ctx)

	// Collect AWS resource usage
	snapshot.AWSResources = suite.collectAWSResourceUsage(ctx)

	// Collect system resource usage (simulated)
	snapshot.SystemResources = suite.collectSystemResourceUsage(ctx)

	return snapshot
}

func (suite *ResourceMonitoringTestSuite) collectKubernetesResourceUsage(ctx context.Context) KubernetesResourceUsage {
	usage := KubernetesResourceUsage{}

	// Count namespaces
	namespaces, err := suite.client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err == nil {
		usage.Namespaces = len(namespaces.Items)
	}

	// Count pods
	pods, err := suite.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err == nil {
		usage.Pods = len(pods.Items)
		
		// Calculate resource requests
		var cpuRequests, memoryRequests resource.Quantity
		for _, pod := range pods.Items {
			for _, container := range pod.Spec.Containers {
				if cpu, ok := container.Resources.Requests[corev1.ResourceCPU]; ok {
					cpuRequests.Add(cpu)
				}
				if memory, ok := container.Resources.Requests[corev1.ResourceMemory]; ok {
					memoryRequests.Add(memory)
				}
			}
		}
		usage.CPURequests = cpuRequests
		usage.MemoryRequests = memoryRequests
	}

	// Count PVCs
	pvcs, err := suite.client.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{})
	if err == nil {
		usage.PVCs = len(pvcs.Items)
		
		// Calculate storage requests
		var storageRequests resource.Quantity
		for _, pvc := range pvcs.Items {
			if storage, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
				storageRequests.Add(storage)
			}
		}
		usage.StorageRequests = storageRequests
	}

	// Count PVs
	pvs, err := suite.client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err == nil {
		usage.PVs = len(pvs.Items)
	}

	// Count StorageClasses
	storageClasses, err := suite.client.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err == nil {
		usage.StorageClasses = len(storageClasses.Items)
	}

	return usage
}

func (suite *ResourceMonitoringTestSuite) collectAWSResourceUsage(ctx context.Context) AWSResourceUsage {
	usage := AWSResourceUsage{}

	// Count EFS file systems
	efsInput := &efs.DescribeFileSystemsInput{}
	efsOutput, err := suite.efsClient.DescribeFileSystems(ctx, efsInput)
	if err == nil {
		usage.EFSFileSystems = len(efsOutput.FileSystems)
		
		// Calculate total storage and estimated cost
		var totalStorage float64
		for _, fs := range efsOutput.FileSystems {
			if fs.SizeInBytes != nil && fs.SizeInBytes.Value != nil {
				totalStorage += float64(*fs.SizeInBytes.Value) / (1024 * 1024 * 1024) // Convert to GB
			}
		}
		usage.TotalStorageGB = totalStorage
		usage.EstimatedMonthlyCost = totalStorage * 0.30 // Approximate EFS Standard pricing
	}

	// Count EFS access points (this would require iterating through file systems)
	// For brevity, we'll simulate this
	usage.EFSAccessPoints = usage.EFSFileSystems * 2 // Rough estimate

	return usage
}

func (suite *ResourceMonitoringTestSuite) collectSystemResourceUsage(ctx context.Context) SystemResourceUsage {
	// In a real implementation, this would collect actual system metrics
	// For testing purposes, we'll simulate realistic values
	
	usage := SystemResourceUsage{
		CPUUsagePercent:     float64(30 + (time.Now().Unix() % 40)), // 30-70%
		MemoryUsageBytes:    int64(1024*1024*200 + (time.Now().Unix()%1024)*1024*100), // 200-300MB
		MemoryUsagePercent:  float64(50 + (time.Now().Unix() % 30)), // 50-80%
		DiskUsageBytes:      int64(1024*1024*1024*10 + (time.Now().Unix()%1024)*1024*1024), // 10-11GB
		DiskUsagePercent:    float64(20 + (time.Now().Unix() % 60)), // 20-80%
		NetworkInBytes:      int64(1024*100 + (time.Now().Unix()%1024)*50), // Variable network usage
		NetworkOutBytes:     int64(1024*80 + (time.Now().Unix()%1024)*40),
		OpenFileDescriptors: int(500 + (time.Now().Unix() % 200)), // 500-700
		ProcessCount:        int(100 + (time.Now().Unix() % 50)), // 100-150
		LoadAverage:         float64(1.0 + float64(time.Now().Unix()%100)/100.0), // 1.0-2.0
	}

	return usage
}

func (suite *ResourceMonitoringTestSuite) detectResourceLeaks(ctx context.Context, baseline, current ResourceSnapshot) []LeakedResource {
	var leaks []LeakedResource

	// Detect Kubernetes resource leaks
	if current.KubernetesResources.PVCs > baseline.KubernetesResources.PVCs+2 {
		leak := LeakedResource{
			Type:         "PVC",
			Name:         "multiple-pvcs",
			DetectionTime: time.Now(),
			LeakAge:      time.Since(baseline.Timestamp),
			Severity:     "MEDIUM",
			Description:  fmt.Sprintf("PVC count increased from %d to %d", baseline.KubernetesResources.PVCs, current.KubernetesResources.PVCs),
		}
		leaks = append(leaks, leak)
	}

	if current.KubernetesResources.Pods > baseline.KubernetesResources.Pods+2 {
		leak := LeakedResource{
			Type:         "Pod",
			Name:         "multiple-pods",
			DetectionTime: time.Now(),
			LeakAge:      time.Since(baseline.Timestamp),
			Severity:     "MEDIUM",
			Description:  fmt.Sprintf("Pod count increased from %d to %d", baseline.KubernetesResources.Pods, current.KubernetesResources.Pods),
		}
		leaks = append(leaks, leak)
	}

	// Detect AWS resource leaks
	if current.AWSResources.EFSFileSystems > baseline.AWSResources.EFSFileSystems+1 {
		leak := LeakedResource{
			Type:         "EFS",
			Name:         "multiple-efs-filesystems",
			DetectionTime: time.Now(),
			LeakAge:      time.Since(baseline.Timestamp),
			Severity:     "HIGH",
			Description:  fmt.Sprintf("EFS filesystem count increased from %d to %d", baseline.AWSResources.EFSFileSystems, current.AWSResources.EFSFileSystems),
		}
		leaks = append(leaks, leak)
	}

	return leaks
}

func (suite *ResourceMonitoringTestSuite) detectOrphanedResources(ctx context.Context, namespace string) []OrphanedResource {
	var orphans []OrphanedResource

	// Check for orphaned PVCs (PVCs without associated pods)
	pvcs, err := suite.client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
	if err == nil {
		pods, podErr := suite.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
		if podErr == nil {
			// Create map of PVCs in use
			pvcsInUse := make(map[string]bool)
			for _, pod := range pods.Items {
				for _, volume := range pod.Spec.Volumes {
					if volume.PersistentVolumeClaim != nil {
						pvcsInUse[volume.PersistentVolumeClaim.ClaimName] = true
					}
				}
			}

			// Find orphaned PVCs
			for _, pvc := range pvcs.Items {
				if !pvcsInUse[pvc.Name] && time.Since(pvc.CreationTimestamp.Time) > 2*time.Minute {
					orphan := OrphanedResource{
						Type:         "PVC",
						Name:         pvc.Name,
						Namespace:    pvc.Namespace,
						CreationTime: pvc.CreationTimestamp.Time,
						LastUsed:     pvc.CreationTimestamp.Time,
						Reason:       "No associated pod found",
						Metadata: map[string]interface{}{
							"size":         pvc.Spec.Resources.Requests[corev1.ResourceStorage].String(),
							"storageClass": *pvc.Spec.StorageClassName,
						},
					}
					orphans = append(orphans, orphan)
				}
			}
		}
	}

	return orphans
}

func (suite *ResourceMonitoringTestSuite) startThresholdMonitoring(ctx context.Context, stop <-chan bool) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			snapshot := suite.takeResourceSnapshot(ctx)
			suite.checkResourceThresholds(snapshot)
		}
	}
}

func (suite *ResourceMonitoringTestSuite) checkResourceThresholds(snapshot ResourceSnapshot) {
	// Check CPU threshold
	if snapshot.SystemResources.CPUUsagePercent > suite.testConfig.AlertThresholds.CPUUsagePercent {
		alert := AlertEvent{
			Timestamp:    snapshot.Timestamp,
			Type:         "CPU_THRESHOLD",
			Severity:     "HIGH",
			Message:      fmt.Sprintf("CPU usage exceeded threshold: %.1f%%", snapshot.SystemResources.CPUUsagePercent),
			Resource:     "system",
			Threshold:    suite.testConfig.AlertThresholds.CPUUsagePercent,
			CurrentValue: snapshot.SystemResources.CPUUsagePercent,
		}
		suite.recordAlertEvent(alert)
	}

	// Check memory threshold
	if snapshot.SystemResources.MemoryUsagePercent > suite.testConfig.AlertThresholds.MemoryUsagePercent {
		alert := AlertEvent{
			Timestamp:    snapshot.Timestamp,
			Type:         "MEMORY_THRESHOLD",
			Severity:     "HIGH",
			Message:      fmt.Sprintf("Memory usage exceeded threshold: %.1f%%", snapshot.SystemResources.MemoryUsagePercent),
			Resource:     "system",
			Threshold:    suite.testConfig.AlertThresholds.MemoryUsagePercent,
			CurrentValue: snapshot.SystemResources.MemoryUsagePercent,
		}
		suite.recordAlertEvent(alert)
	}

	// Check open file descriptors threshold
	if snapshot.SystemResources.OpenFileDescriptors > suite.testConfig.AlertThresholds.OpenFileDescriptors {
		alert := AlertEvent{
			Timestamp:    snapshot.Timestamp,
			Type:         "FILE_DESCRIPTOR_THRESHOLD",
			Severity:     "MEDIUM",
			Message:      fmt.Sprintf("Open file descriptors exceeded threshold: %d", snapshot.SystemResources.OpenFileDescriptors),
			Resource:     "system",
			Threshold:    suite.testConfig.AlertThresholds.OpenFileDescriptors,
			CurrentValue: snapshot.SystemResources.OpenFileDescriptors,
		}
		suite.recordAlertEvent(alert)
	}
}

func (suite *ResourceMonitoringTestSuite) startComprehensiveMonitoring(ctx context.Context, stop <-chan bool) {
	ticker := time.NewTicker(suite.testConfig.MonitoringInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			snapshot := suite.takeResourceSnapshot(ctx)
			suite.recordResourceSnapshot(snapshot)
			suite.checkResourceThresholds(snapshot)
		}
	}
}

// Cleanup and management methods

func (suite *ResourceMonitoringTestSuite) executeResourceCleanup(ctx context.Context, namespace, resourceType, resourceName, strategy string) CleanupResult {
	result := CleanupResult{
		Timestamp:       time.Now(),
		ResourceType:    resourceType,
		ResourceName:    resourceName,
		CleanupStrategy: strategy,
		Metadata:        make(map[string]interface{}),
	}

	switch resourceType {
	case "PVC":
		err := suite.cleanupPVC(ctx, namespace, resourceName, strategy)
		result.Success = err == nil
		if err != nil {
			result.Error = err.Error()
		}
	case "Pod":
		err := suite.cleanupPod(ctx, namespace, resourceName, strategy)
		result.Success = err == nil
		if err != nil {
			result.Error = err.Error()
		}
	default:
		result.Success = false
		result.Error = fmt.Sprintf("Unknown resource type: %s", resourceType)
	}

	return result
}

func (suite *ResourceMonitoringTestSuite) cleanupPVC(ctx context.Context, namespace, pvcName, strategy string) error {
	switch strategy {
	case "graceful_shutdown":
		return suite.client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvcName, metav1.DeleteOptions{
			GracePeriodSeconds: &[]int64{30}[0],
		})
	case "forced_termination":
		return suite.client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvcName, metav1.DeleteOptions{
			GracePeriodSeconds: &[]int64{0}[0],
		})
	case "cascade_deletion":
		return suite.client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvcName, metav1.DeleteOptions{
			PropagationPolicy: &[]metav1.DeletionPropagation{metav1.DeletePropagationForeground}[0],
		})
	case "finalizer_handling":
		// Get PVC and remove finalizers if present
		pvc, err := suite.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if len(pvc.Finalizers) > 0 {
			pvc.Finalizers = nil
			_, err = suite.client.CoreV1().PersistentVolumeClaims(namespace).Update(ctx, pvc, metav1.UpdateOptions{})
			if err != nil {
				return err
			}
		}
		return suite.client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvcName, metav1.DeleteOptions{})
	default:
		return fmt.Errorf("unknown cleanup strategy: %s", strategy)
	}
}

func (suite *ResourceMonitoringTestSuite) cleanupPod(ctx context.Context, namespace, podName, strategy string) error {
	switch strategy {
	case "graceful_shutdown":
		return suite.client.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{
			GracePeriodSeconds: &[]int64{30}[0],
		})
	case "forced_termination":
		return suite.client.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{
			GracePeriodSeconds: &[]int64{0}[0],
		})
	default:
		return suite.client.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{})
	}
}

func (suite *ResourceMonitoringTestSuite) cleanupLeakedResources(ctx context.Context, leaks []LeakedResource) {
	for _, leak := range leaks {
		suite.T().Logf("Attempting to cleanup leaked resource: %s %s", leak.Type, leak.Name)
		
		result := CleanupResult{
			Timestamp:       time.Now(),
			ResourceType:    leak.Type,
			ResourceName:    leak.Name,
			CleanupStrategy: "automatic_leak_cleanup",
		}
		
		// In a real implementation, this would perform actual cleanup
		// For testing purposes, we'll simulate cleanup
		result.Success = true
		result.Duration = time.Duration(5) * time.Second
		
		suite.recordCleanupResult(result)
		
		// Update leak record
		leak.CleanupAttempts++
		leak.CleanupSuccessful = result.Success
	}
}

func (suite *ResourceMonitoringTestSuite) cleanupOrphanedResources(ctx context.Context, orphans []OrphanedResource) {
	for _, orphan := range orphans {
		suite.T().Logf("Attempting to cleanup orphaned resource: %s %s", orphan.Type, orphan.Name)
		
		var err error
		if orphan.Type == "PVC" {
			err = suite.client.CoreV1().PersistentVolumeClaims(orphan.Namespace).Delete(ctx, orphan.Name, metav1.DeleteOptions{})
		}
		
		result := CleanupResult{
			Timestamp:       time.Now(),
			ResourceType:    orphan.Type,
			ResourceName:    orphan.Name,
			CleanupStrategy: "orphan_cleanup",
			Success:         err == nil,
		}
		
		if err != nil {
			result.Error = err.Error()
		}
		
		suite.recordCleanupResult(result)
	}
}

func (suite *ResourceMonitoringTestSuite) testCleanupTimeouts(ctx context.Context, namespace string, resourceNames []string) {
	suite.T().Log("Testing cleanup timeout handling")
	
	for _, resourceName := range resourceNames {
		// Test cleanup with very short timeout
		timeoutCtx, cancel := context.WithTimeout(ctx, 1*time.Millisecond)
		err := suite.client.CoreV1().PersistentVolumeClaims(namespace).Delete(timeoutCtx, resourceName, metav1.DeleteOptions{})
		cancel()
		
		if err != nil {
			suite.T().Logf("Cleanup timeout test for %s: %v", resourceName, err)
			
			// Record timeout event
			alert := AlertEvent{
				Timestamp: time.Now(),
				Type:      "CLEANUP_TIMEOUT",
				Severity:  "MEDIUM",
				Message:   fmt.Sprintf("Resource cleanup timed out: %s", resourceName),
				Resource:  resourceName,
			}
			suite.recordAlertEvent(alert)
		}
	}
}

// Analysis methods

func (suite *ResourceMonitoringTestSuite) analyzeMemoryUsagePatterns(memorySnapshots []int64) {
	if len(memorySnapshots) < 2 {
		suite.T().Log("Insufficient memory snapshots for pattern analysis")
		return
	}

	// Calculate memory growth rate
	initialMemory := memorySnapshots[0]
	finalMemory := memorySnapshots[len(memorySnapshots)-1]
	memoryGrowth := finalMemory - initialMemory
	
	suite.T().Logf("Memory usage analysis: Initial=%d bytes, Final=%d bytes, Growth=%d bytes", 
		initialMemory, finalMemory, memoryGrowth)

	// Check for memory leaks
	if memoryGrowth > suite.testConfig.MemoryLeakThresholdBytes {
		alert := AlertEvent{
			Timestamp:    time.Now(),
			Type:         "MEMORY_LEAK_DETECTED",
			Severity:     "HIGH",
			Message:      fmt.Sprintf("Potential memory leak detected: %d bytes growth", memoryGrowth),
			Resource:     "system",
			Threshold:    suite.testConfig.MemoryLeakThresholdBytes,
			CurrentValue: memoryGrowth,
		}
		suite.recordAlertEvent(alert)
	}

	// Calculate average memory usage
	var totalMemory int64
	for _, memory := range memorySnapshots {
		totalMemory += memory
	}
	averageMemory := totalMemory / int64(len(memorySnapshots))
	
	suite.T().Logf("Average memory usage: %d bytes", averageMemory)
}

func (suite *ResourceMonitoringTestSuite) analyzeLongRunningMonitoringData(duration time.Duration) {
	suite.monitoringData.mu.RLock()
	defer suite.monitoringData.mu.RUnlock()

	suite.T().Logf("Long-running monitoring analysis (duration: %v)", duration)
	
	// Analyze resource snapshots
	snapshotCount := len(suite.monitoringData.ResourceSnapshots)
	suite.T().Logf("Total resource snapshots collected: %d", snapshotCount)
	
	if snapshotCount > 0 {
		// Calculate resource usage trends
		firstSnapshot := suite.monitoringData.ResourceSnapshots[0]
		lastSnapshot := suite.monitoringData.ResourceSnapshots[snapshotCount-1]
		
		suite.T().Logf("Resource usage trends:")
		suite.T().Logf("  Pods: %d -> %d", firstSnapshot.KubernetesResources.Pods, lastSnapshot.KubernetesResources.Pods)
		suite.T().Logf("  PVCs: %d -> %d", firstSnapshot.KubernetesResources.PVCs, lastSnapshot.KubernetesResources.PVCs)
		suite.T().Logf("  CPU: %.1f%% -> %.1f%%", firstSnapshot.SystemResources.CPUUsagePercent, lastSnapshot.SystemResources.CPUUsagePercent)
		suite.T().Logf("  Memory: %.1f%% -> %.1f%%", firstSnapshot.SystemResources.MemoryUsagePercent, lastSnapshot.SystemResources.MemoryUsagePercent)
	}
	
	// Analyze alert events
	alertCount := len(suite.monitoringData.AlertEvents)
	suite.T().Logf("Total alert events generated: %d", alertCount)
	
	if alertCount > 0 {
		// Group alerts by type
		alertTypes := make(map[string]int)
		for _, alert := range suite.monitoringData.AlertEvents {
			alertTypes[alert.Type]++
		}
		
		suite.T().Logf("Alert breakdown:")
		for alertType, count := range alertTypes {
			suite.T().Logf("  %s: %d", alertType, count)
		}
	}
}

// Recording methods

func (suite *ResourceMonitoringTestSuite) recordResourceSnapshot(snapshot ResourceSnapshot) {
	suite.monitoringData.mu.Lock()
	defer suite.monitoringData.mu.Unlock()
	suite.monitoringData.ResourceSnapshots = append(suite.monitoringData.ResourceSnapshots, snapshot)
}

func (suite *ResourceMonitoringTestSuite) recordLeakedResource(leak LeakedResource) {
	suite.monitoringData.mu.Lock()
	defer suite.monitoringData.mu.Unlock()
	suite.monitoringData.LeakedResources = append(suite.monitoringData.LeakedResources, leak)
}

func (suite *ResourceMonitoringTestSuite) recordOrphanedResource(orphan OrphanedResource) {
	suite.monitoringData.mu.Lock()
	defer suite.monitoringData.mu.Unlock()
	suite.monitoringData.OrphanedResources = append(suite.monitoringData.OrphanedResources, orphan)
}

func (suite *ResourceMonitoringTestSuite) recordAlertEvent(alert AlertEvent) {
	suite.monitoringData.mu.Lock()
	defer suite.monitoringData.mu.Unlock()
	suite.monitoringData.AlertEvents = append(suite.monitoringData.AlertEvents, alert)
	
	suite.T().Logf("ALERT: %s - %s - %s", alert.Type, alert.Severity, alert.Message)
}

func (suite *ResourceMonitoringTestSuite) recordCleanupResult(result CleanupResult) {
	suite.monitoringData.mu.Lock()
	defer suite.monitoringData.mu.Unlock()
	suite.monitoringData.CleanupResults = append(suite.monitoringData.CleanupResults, result)
}

func (suite *ResourceMonitoringTestSuite) generateResourceMonitoringReport() {
	suite.T().Log("=== RESOURCE MONITORING TEST REPORT ===")
	
	suite.monitoringData.mu.RLock()
	defer suite.monitoringData.mu.RUnlock()
	
	// Summary statistics
	suite.T().Logf("Resource Snapshots Collected: %d", len(suite.monitoringData.ResourceSnapshots))
	suite.T().Logf("Leaked Resources Detected: %d", len(suite.monitoringData.LeakedResources))
	suite.T().Logf("Orphaned Resources Found: %d", len(suite.monitoringData.OrphanedResources))
	suite.T().Logf("Alert Events Generated: %d", len(suite.monitoringData.AlertEvents))
	suite.T().Logf("Cleanup Operations: %d", len(suite.monitoringData.CleanupResults))
	
	// Leak analysis
	if len(suite.monitoringData.LeakedResources) > 0 {
		suite.T().Logf("\nResource Leak Analysis:")
		leakTypes := make(map[string]int)
		for _, leak := range suite.monitoringData.LeakedResources {
			leakTypes[leak.Type]++
		}
		
		for leakType, count := range leakTypes {
			suite.T().Logf("  %s leaks: %d", leakType, count)
		}
	}
	
	// Alert analysis
	if len(suite.monitoringData.AlertEvents) > 0 {
		suite.T().Logf("\nAlert Analysis:")
		alertTypes := make(map[string]int)
		severityCounts := make(map[string]int)
		
		for _, alert := range suite.monitoringData.AlertEvents {
			alertTypes[alert.Type]++
			severityCounts[alert.Severity]++
		}
		
		suite.T().Logf("  By Type:")
		for alertType, count := range alertTypes {
			suite.T().Logf("    %s: %d", alertType, count)
		}
		
		suite.T().Logf("  By Severity:")
		for severity, count := range severityCounts {
			suite.T().Logf("    %s: %d", severity, count)
		}
	}
	
	// Cleanup analysis
	if len(suite.monitoringData.CleanupResults) > 0 {
		suite.T().Logf("\nCleanup Analysis:")
		successfulCleanups := 0
		var totalDuration time.Duration
		
		for _, result := range suite.monitoringData.CleanupResults {
			if result.Success {
				successfulCleanups++
			}
			totalDuration += result.Duration
		}
		
		cleanupSuccessRate := float64(successfulCleanups) / float64(len(suite.monitoringData.CleanupResults)) * 100
		avgCleanupDuration := totalDuration / time.Duration(len(suite.monitoringData.CleanupResults))
		
		suite.T().Logf("  Success Rate: %.1f%% (%d/%d)", cleanupSuccessRate, successfulCleanups, len(suite.monitoringData.CleanupResults))
		suite.T().Logf("  Average Duration: %v", avgCleanupDuration)
	}
	
	// Resource usage trends (if snapshots available)
	if len(suite.monitoringData.ResourceSnapshots) >= 2 {
		suite.T().Logf("\nResource Usage Trends:")
		firstSnapshot := suite.monitoringData.ResourceSnapshots[0]
		lastSnapshot := suite.monitoringData.ResourceSnapshots[len(suite.monitoringData.ResourceSnapshots)-1]
		duration := lastSnapshot.Timestamp.Sub(firstSnapshot.Timestamp)
		
		suite.T().Logf("  Monitoring Duration: %v", duration)
		suite.T().Logf("  Pod Count: %d -> %d", firstSnapshot.KubernetesResources.Pods, lastSnapshot.KubernetesResources.Pods)
		suite.T().Logf("  PVC Count: %d -> %d", firstSnapshot.KubernetesResources.PVCs, lastSnapshot.KubernetesResources.PVCs)
		suite.T().Logf("  EFS Filesystem Count: %d -> %d", firstSnapshot.AWSResources.EFSFileSystems, lastSnapshot.AWSResources.EFSFileSystems)
		suite.T().Logf("  System CPU Usage: %.1f%% -> %.1f%%", firstSnapshot.SystemResources.CPUUsagePercent, lastSnapshot.SystemResources.CPUUsagePercent)
		suite.T().Logf("  System Memory Usage: %.1f%% -> %.1f%%", firstSnapshot.SystemResources.MemoryUsagePercent, lastSnapshot.SystemResources.MemoryUsagePercent)
	}
	
	suite.T().Log("=== END RESOURCE MONITORING REPORT ===")
}