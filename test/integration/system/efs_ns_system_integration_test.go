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

package system

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	"github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	testutils "github.com/kubernetes-sigs/aws-efs-csi-driver/test/utils"
)

// SystemIntegrationTestSuite provides comprehensive system integration testing for EFS-NS
type SystemIntegrationTestSuite struct {
	suite.Suite
	client           clientset.Interface
	efsClient        *efs.Client
	helper           *testutils.EfsNsTestHelper
	resourceTracker  *testutils.TestResourceTracker
	testConfig       *SystemTestConfig
	createdResources *TestResourceRegistry
}

// SystemTestConfig holds configuration for system tests
type SystemTestConfig struct {
	Region                  string        `json:"region"`
	ClusterName             string        `json:"clusterName"`
	TestTimeout             time.Duration `json:"testTimeout"`
	CleanupTimeout          time.Duration `json:"cleanupTimeout"`
	PerformanceTestEnabled  bool          `json:"performanceTestEnabled"`
	SecurityTestEnabled     bool          `json:"securityTestEnabled"`
	ChaosTestEnabled        bool          `json:"chaosTestEnabled"`
	MaxConcurrentOperations int           `json:"maxConcurrentOperations"`
	EnableMetricsValidation bool          `json:"enableMetricsValidation"`
	SkipCleanup             bool          `json:"skipCleanup"`
}

// TestResourceRegistry tracks all resources created during testing
type TestResourceRegistry struct {
	mutex              sync.RWMutex
	FileSystems        map[string]*types.FileSystemDescription
	StorageClasses     []string
	TestNamespaces     []string
	PVCs               map[string][]string // namespace -> pvc names
	Pods               map[string][]string // namespace -> pod names
	SystemMetrics      *SystemMetrics
	PerformanceMetrics *PerformanceMetrics
}

// SystemMetrics tracks system-level metrics during tests
type SystemMetrics struct {
	FilesystemCreateCount    int           `json:"filesystemCreateCount"`
	FilesystemDeleteCount    int           `json:"filesystemDeleteCount"`
	PVCCreateCount          int           `json:"pvcCreateCount"`
	PVCDeleteCount          int           `json:"pvcDeleteCount"`
	AvgFilesystemCreateTime time.Duration `json:"avgFilesystemCreateTime"`
	AvgPVCBindTime          time.Duration `json:"avgPvcBindTime"`
	ErrorCount              int           `json:"errorCount"`
	MemoryUsage             []int64       `json:"memoryUsage"`
	CPUUsage                []float64     `json:"cpuUsage"`
}

// PerformanceMetrics tracks performance-related metrics
type PerformanceMetrics struct {
	ThroughputMBps          float64       `json:"throughputMBps"`
	Latency95thPercentile   time.Duration `json:"latency95thPercentile"`
	Latency99thPercentile   time.Duration `json:"latency99thPercentile"`
	ConcurrentOperationsMax int           `json:"concurrentOperationsMax"`
	ResourceUtilization     map[string]interface{} `json:"resourceUtilization"`
}

// SetupSuite initializes the test suite
func (suite *SystemIntegrationTestSuite) SetupSuite() {
	// Load test configuration
	suite.loadTestConfig()

	// Initialize Kubernetes client
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = os.Getenv("HOME") + "/.kube/config"
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	require.NoError(suite.T(), err, "Failed to load kubeconfig")

	suite.client, err = clientset.NewForConfig(config)
	require.NoError(suite.T(), err, "Failed to create Kubernetes client")

	// Initialize AWS EFS client
	awsConfig, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(suite.testConfig.Region))
	require.NoError(suite.T(), err, "Failed to load AWS config")
	suite.efsClient = efs.NewFromConfig(awsConfig)

	// Initialize test helpers
	suite.helper = testutils.NewEfsNsTestHelper(suite.client)
	suite.resourceTracker = testutils.NewTestResourceTracker()
	
	// Initialize resource registry
	suite.createdResources = &TestResourceRegistry{
		FileSystems:        make(map[string]*types.FileSystemDescription),
		PVCs:              make(map[string][]string),
		Pods:              make(map[string][]string),
		SystemMetrics:     &SystemMetrics{},
		PerformanceMetrics: &PerformanceMetrics{},
	}

	suite.T().Logf("System Integration Test Suite initialized for region: %s, cluster: %s", 
		suite.testConfig.Region, suite.testConfig.ClusterName)
}

// TearDownSuite cleans up after all tests
func (suite *SystemIntegrationTestSuite) TearDownSuite() {
	if !suite.testConfig.SkipCleanup {
		suite.T().Log("Starting comprehensive cleanup...")
		
		ctx, cancel := context.WithTimeout(context.Background(), suite.testConfig.CleanupTimeout)
		defer cancel()
		
		suite.performComprehensiveCleanup(ctx)
		
		// Generate final test report
		suite.generateTestReport()
	}
}

// TestFullSystemIntegration tests the complete EFS-NS system end-to-end
func (suite *SystemIntegrationTestSuite) TestFullSystemIntegration() {
	ctx := context.Background()
	
	suite.Run("CompleteEfsNsLifecycle", func() {
		// Test complete PVC lifecycle across multiple namespaces
		suite.testCompleteLifecycle(ctx)
	})
	
	suite.Run("MultiNamespaceIsolation", func() {
		// Test strict isolation between namespaces
		suite.testMultiNamespaceIsolation(ctx)
	})
	
	suite.Run("ConcurrentOperations", func() {
		// Test concurrent operations across namespaces
		suite.testConcurrentOperations(ctx)
	})
	
	suite.Run("NamespaceCleanupIntegration", func() {
		// Test complete namespace deletion and cleanup
		suite.testNamespaceCleanupIntegration(ctx)
	})
	
	suite.Run("DataPersistenceValidation", func() {
		// Test data persistence across various scenarios
		suite.testDataPersistence(ctx)
	})
}

// testCompleteLifecycle tests the complete EFS-NS lifecycle
func (suite *SystemIntegrationTestSuite) testCompleteLifecycle(ctx context.Context) {
	// Create EFS-NS StorageClass
	sc, err := suite.helper.CreateRandomEfsNsStorageClass(ctx)
	require.NoError(suite.T(), err, "Failed to create EFS-NS StorageClass")
	suite.createdResources.StorageClasses = append(suite.createdResources.StorageClasses, sc.Name)

	// Create test namespace
	ns, err := suite.helper.CreateRandomNamespace(ctx, "lifecycle-test")
	require.NoError(suite.T(), err, "Failed to create test namespace")
	suite.createdResources.TestNamespaces = append(suite.createdResources.TestNamespaces, ns.Name)

	// Track timing for performance metrics
	startTime := time.Now()

	// Create PVC
	pvc, err := suite.helper.CreateRandomPVC(ctx, ns.Name, sc.Name)
	require.NoError(suite.T(), err, "Failed to create PVC")
	suite.trackPVC(ns.Name, pvc.Name)

	// Wait for PVC to be bound and measure time
	boundPVC, err := suite.helper.WaitForPVCBound(ctx, ns.Name, pvc.Name, suite.testConfig.TestTimeout)
	require.NoError(suite.T(), err, "PVC should be bound within timeout")
	
	bindTime := time.Since(startTime)
	suite.createdResources.SystemMetrics.AvgPVCBindTime = bindTime
	suite.createdResources.SystemMetrics.PVCCreateCount++

	// Verify PV exists and has correct driver
	pv, err := suite.helper.GetPVForPVC(ctx, ns.Name, boundPVC.Name)
	require.NoError(suite.T(), err, "Should get PV for bound PVC")
	
	assert.Equal(suite.T(), testutils.TestConstants.DefaultDriverName, pv.Spec.CSI.Driver)
	assert.NotEmpty(suite.T(), pv.Spec.CSI.VolumeHandle)

	// Extract and verify filesystem
	fsId, err := suite.helper.ExtractFileSystemIdFromPV(pv)
	require.NoError(suite.T(), err, "Should extract filesystem ID")
	
	fs, err := suite.describeFilesystem(ctx, fsId)
	require.NoError(suite.T(), err, "Filesystem should exist in AWS")
	
	suite.createdResources.mutex.Lock()
	suite.createdResources.FileSystems[fsId] = fs
	suite.createdResources.mutex.Unlock()

	// Test pod creation and data operations
	suite.testPodOperations(ctx, ns.Name, pvc.Name)

	// Test volume cleanup
	suite.testVolumeCleanup(ctx, ns.Name, pvc.Name, fsId)
}

// testMultiNamespaceIsolation tests isolation between namespaces
func (suite *SystemIntegrationTestSuite) testMultiNamespaceIsolation(ctx context.Context) {
	const numNamespaces = 3
	
	// Create StorageClass
	sc, err := suite.helper.CreateRandomEfsNsStorageClass(ctx)
	require.NoError(suite.T(), err, "Failed to create StorageClass")
	suite.createdResources.StorageClasses = append(suite.createdResources.StorageClasses, sc.Name)

	// Create multiple namespaces and PVCs
	var namespaces []*v1.Namespace
	var pvcs []*v1.PersistentVolumeClaim
	var filesystemIds []string

	for i := 0; i < numNamespaces; i++ {
		// Create namespace
		ns, err := suite.helper.CreateRandomNamespace(ctx, fmt.Sprintf("isolation-test-%d", i))
		require.NoError(suite.T(), err, "Failed to create namespace %d", i)
		namespaces = append(namespaces, ns)
		suite.createdResources.TestNamespaces = append(suite.createdResources.TestNamespaces, ns.Name)

		// Create PVC
		pvc, err := suite.helper.CreateRandomPVC(ctx, ns.Name, sc.Name)
		require.NoError(suite.T(), err, "Failed to create PVC in namespace %d", i)
		pvcs = append(pvcs, pvc)
		suite.trackPVC(ns.Name, pvc.Name)

		// Wait for binding
		_, err = suite.helper.WaitForPVCBound(ctx, ns.Name, pvc.Name, suite.testConfig.TestTimeout)
		require.NoError(suite.T(), err, "PVC should be bound in namespace %d", i)

		// Get filesystem ID
		pv, err := suite.helper.GetPVForPVC(ctx, ns.Name, pvc.Name)
		require.NoError(suite.T(), err, "Should get PV for namespace %d", i)
		
		fsId, err := suite.helper.ExtractFileSystemIdFromPV(pv)
		require.NoError(suite.T(), err, "Should extract filesystem ID for namespace %d", i)
		filesystemIds = append(filesystemIds, fsId)
	}

	// Verify each namespace has a unique filesystem
	fsSet := make(map[string]bool)
	for i, fsId := range filesystemIds {
		assert.False(suite.T(), fsSet[fsId], "Filesystem ID %s should be unique to namespace %d", fsId, i)
		fsSet[fsId] = true
		
		// Track filesystem for cleanup
		fs, err := suite.describeFilesystem(ctx, fsId)
		require.NoError(suite.T(), err, "Filesystem should exist for namespace %d", i)
		
		suite.createdResources.mutex.Lock()
		suite.createdResources.FileSystems[fsId] = fs
		suite.createdResources.mutex.Unlock()
	}

	// Test data isolation
	suite.testDataIsolation(ctx, namespaces, pvcs)
}

// testConcurrentOperations tests concurrent operations
func (suite *SystemIntegrationTestSuite) testConcurrentOperations(ctx context.Context) {
	const numConcurrent = 5
	
	sc, err := suite.helper.CreateRandomEfsNsStorageClass(ctx)
	require.NoError(suite.T(), err, "Failed to create StorageClass")
	suite.createdResources.StorageClasses = append(suite.createdResources.StorageClasses, sc.Name)

	var wg sync.WaitGroup
	results := make(chan error, numConcurrent)

	startTime := time.Now()
	
	// Launch concurrent operations
	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			
			// Create namespace
			ns, err := suite.helper.CreateRandomNamespace(ctx, fmt.Sprintf("concurrent-test-%d", idx))
			if err != nil {
				results <- fmt.Errorf("failed to create namespace %d: %w", idx, err)
				return
			}
			
			suite.createdResources.mutex.Lock()
			suite.createdResources.TestNamespaces = append(suite.createdResources.TestNamespaces, ns.Name)
			suite.createdResources.mutex.Unlock()

			// Create PVC
			pvc, err := suite.helper.CreateRandomPVC(ctx, ns.Name, sc.Name)
			if err != nil {
				results <- fmt.Errorf("failed to create PVC in namespace %d: %w", idx, err)
				return
			}
			
			suite.trackPVC(ns.Name, pvc.Name)

			// Wait for binding
			_, err = suite.helper.WaitForPVCBound(ctx, ns.Name, pvc.Name, suite.testConfig.TestTimeout)
			if err != nil {
				results <- fmt.Errorf("PVC failed to bind in namespace %d: %w", idx, err)
				return
			}

			// Verify filesystem creation
			pv, err := suite.helper.GetPVForPVC(ctx, ns.Name, pvc.Name)
			if err != nil {
				results <- fmt.Errorf("failed to get PV in namespace %d: %w", idx, err)
				return
			}
			
			fsId, err := suite.helper.ExtractFileSystemIdFromPV(pv)
			if err != nil {
				results <- fmt.Errorf("failed to extract filesystem ID in namespace %d: %w", idx, err)
				return
			}

			// Verify filesystem exists
			fs, err := suite.describeFilesystem(ctx, fsId)
			if err != nil {
				results <- fmt.Errorf("filesystem does not exist for namespace %d: %w", idx, err)
				return
			}
			
			suite.createdResources.mutex.Lock()
			suite.createdResources.FileSystems[fsId] = fs
			suite.createdResources.mutex.Unlock()

			results <- nil
		}(i)
	}

	// Wait for all operations to complete
	wg.Wait()
	close(results)

	// Check results
	var errors []error
	for err := range results {
		if err != nil {
			errors = append(errors, err)
		}
	}

	duration := time.Since(startTime)
	suite.createdResources.PerformanceMetrics.ConcurrentOperationsMax = numConcurrent
	
	assert.Empty(suite.T(), errors, "All concurrent operations should succeed")
	suite.T().Logf("Concurrent operations completed in %v", duration)
	
	// Verify performance metrics
	avgTimePerOperation := duration / numConcurrent
	assert.Less(suite.T(), avgTimePerOperation, 2*time.Minute, "Average operation time should be under 2 minutes")
}

// testNamespaceCleanupIntegration tests complete namespace cleanup
func (suite *SystemIntegrationTestSuite) testNamespaceCleanupIntegration(ctx context.Context) {
	sc, err := suite.helper.CreateRandomEfsNsStorageClass(ctx)
	require.NoError(suite.T(), err, "Failed to create StorageClass")
	suite.createdResources.StorageClasses = append(suite.createdResources.StorageClasses, sc.Name)

	// Create namespace
	ns, err := suite.helper.CreateRandomNamespace(ctx, "cleanup-test")
	require.NoError(suite.T(), err, "Failed to create namespace")

	// Create PVC
	pvc, err := suite.helper.CreateRandomPVC(ctx, ns.Name, sc.Name)
	require.NoError(suite.T(), err, "Failed to create PVC")

	// Wait for binding
	boundPVC, err := suite.helper.WaitForPVCBound(ctx, ns.Name, pvc.Name, suite.testConfig.TestTimeout)
	require.NoError(suite.T(), err, "PVC should be bound")

	// Get filesystem ID
	pv, err := suite.helper.GetPVForPVC(ctx, ns.Name, boundPVC.Name)
	require.NoError(suite.T(), err, "Should get PV")
	
	fsId, err := suite.helper.ExtractFileSystemIdFromPV(pv)
	require.NoError(suite.T(), err, "Should extract filesystem ID")

	// Verify filesystem exists
	_, err = suite.describeFilesystem(ctx, fsId)
	require.NoError(suite.T(), err, "Filesystem should exist before deletion")

	// Delete PVC
	err = suite.client.CoreV1().PersistentVolumeClaims(ns.Name).Delete(ctx, pvc.Name, metav1.DeleteOptions{})
	require.NoError(suite.T(), err, "Should delete PVC")

	// Wait for PVC to be deleted
	err = suite.helper.WaitForPVCDeleted(ctx, ns.Name, pvc.Name, suite.testConfig.CleanupTimeout)
	require.NoError(suite.T(), err, "PVC should be deleted")

	// Wait for PV to be deleted
	err = suite.helper.WaitForPVDeleted(ctx, pv.Name, suite.testConfig.CleanupTimeout)
	require.NoError(suite.T(), err, "PV should be deleted")

	// Verify filesystem is cleaned up
	eventually := assert.Eventually(suite.T(), func() bool {
		_, err := suite.describeFilesystem(ctx, fsId)
		return suite.isNotFoundError(err)
	}, suite.testConfig.CleanupTimeout, 10*time.Second)
	
	assert.True(suite.T(), eventually, "Filesystem should be cleaned up after PVC deletion")

	// Clean up namespace
	err = suite.client.CoreV1().Namespaces().Delete(ctx, ns.Name, metav1.DeleteOptions{})
	require.NoError(suite.T(), err, "Should delete namespace")
}

// testDataPersistence tests data persistence across various scenarios
func (suite *SystemIntegrationTestSuite) testDataPersistence(ctx context.Context) {
	sc, err := suite.helper.CreateRandomEfsNsStorageClass(ctx)
	require.NoError(suite.T(), err, "Failed to create StorageClass")
	suite.createdResources.StorageClasses = append(suite.createdResources.StorageClasses, sc.Name)

	ns, err := suite.helper.CreateRandomNamespace(ctx, "persistence-test")
	require.NoError(suite.T(), err, "Failed to create namespace")
	suite.createdResources.TestNamespaces = append(suite.createdResources.TestNamespaces, ns.Name)

	pvc, err := suite.helper.CreateRandomPVC(ctx, ns.Name, sc.Name)
	require.NoError(suite.T(), err, "Failed to create PVC")
	suite.trackPVC(ns.Name, pvc.Name)

	_, err = suite.helper.WaitForPVCBound(ctx, ns.Name, pvc.Name, suite.testConfig.TestTimeout)
	require.NoError(suite.T(), err, "PVC should be bound")

	// Test data persistence across pod restarts
	testData := fmt.Sprintf("persistence-test-data-%d", time.Now().Unix())
	
	// Write data in first pod
	writePod, err := suite.helper.CreateRandomTestPod(ctx, ns.Name, pvc.Name, 
		fmt.Sprintf("echo '%s' > /mnt/volume/test.txt && sync", testData))
	require.NoError(suite.T(), err, "Failed to create write pod")
	suite.trackPod(ns.Name, writePod.Name)

	err = suite.helper.WaitForPodSuccess(ctx, ns.Name, writePod.Name, testutils.TestConstants.DefaultTimeout)
	require.NoError(suite.T(), err, "Write pod should succeed")

	// Read data in second pod
	readPod, err := suite.helper.CreateRandomTestPod(ctx, ns.Name, pvc.Name,
		fmt.Sprintf("cat /mnt/volume/test.txt | grep '%s'", testData))
	require.NoError(suite.T(), err, "Failed to create read pod")
	suite.trackPod(ns.Name, readPod.Name)

	err = suite.helper.WaitForPodSuccess(ctx, ns.Name, readPod.Name, testutils.TestConstants.DefaultTimeout)
	require.NoError(suite.T(), err, "Read pod should succeed and find the data")

	// Get filesystem for cleanup
	pv, err := suite.helper.GetPVForPVC(ctx, ns.Name, pvc.Name)
	require.NoError(suite.T(), err, "Should get PV")
	
	fsId, err := suite.helper.ExtractFileSystemIdFromPV(pv)
	require.NoError(suite.T(), err, "Should extract filesystem ID")
	
	fs, err := suite.describeFilesystem(ctx, fsId)
	require.NoError(suite.T(), err, "Filesystem should exist")
	
	suite.createdResources.mutex.Lock()
	suite.createdResources.FileSystems[fsId] = fs
	suite.createdResources.mutex.Unlock()
}

// Helper methods

func (suite *SystemIntegrationTestSuite) testPodOperations(ctx context.Context, namespace, pvcName string) {
	// Test basic read/write operations
	pod, err := suite.helper.CreateRandomTestPod(ctx, namespace, pvcName,
		"echo 'test-data' > /mnt/volume/test.txt && cat /mnt/volume/test.txt")
	require.NoError(suite.T(), err, "Failed to create test pod")
	suite.trackPod(namespace, pod.Name)

	err = suite.helper.WaitForPodSuccess(ctx, namespace, pod.Name, testutils.TestConstants.DefaultTimeout)
	require.NoError(suite.T(), err, "Pod should complete successfully")
}

func (suite *SystemIntegrationTestSuite) testVolumeCleanup(ctx context.Context, namespace, pvcName, expectedFsId string) {
	// Delete PVC and verify cleanup
	err := suite.client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvcName, metav1.DeleteOptions{})
	require.NoError(suite.T(), err, "Should delete PVC")

	// Wait for cleanup
	err = suite.helper.WaitForPVCDeleted(ctx, namespace, pvcName, suite.testConfig.CleanupTimeout)
	require.NoError(suite.T(), err, "PVC should be deleted")

	// Verify filesystem cleanup
	eventually := assert.Eventually(suite.T(), func() bool {
		_, err := suite.describeFilesystem(ctx, expectedFsId)
		return suite.isNotFoundError(err)
	}, suite.testConfig.CleanupTimeout, 10*time.Second)
	
	assert.True(suite.T(), eventually, "Filesystem should be cleaned up")
}

func (suite *SystemIntegrationTestSuite) testDataIsolation(ctx context.Context, namespaces []*v1.Namespace, pvcs []*v1.PersistentVolumeClaim) {
	// Write different data in each namespace
	for i, ns := range namespaces {
		testData := fmt.Sprintf("ns-%d-data", i)
		
		pod, err := suite.helper.CreateRandomTestPod(ctx, ns.Name, pvcs[i].Name,
			fmt.Sprintf("echo '%s' > /mnt/volume/data.txt", testData))
		require.NoError(suite.T(), err, "Failed to create writer pod for namespace %d", i)
		suite.trackPod(ns.Name, pod.Name)

		err = suite.helper.WaitForPodSuccess(ctx, ns.Name, pod.Name, testutils.TestConstants.DefaultTimeout)
		require.NoError(suite.T(), err, "Writer pod should succeed for namespace %d", i)
	}

	// Verify each namespace only sees its own data
	for i, ns := range namespaces {
		expectedData := fmt.Sprintf("ns-%d-data", i)
		
		pod, err := suite.helper.CreateRandomTestPod(ctx, ns.Name, pvcs[i].Name,
			fmt.Sprintf("cat /mnt/volume/data.txt | grep '%s'", expectedData))
		require.NoError(suite.T(), err, "Failed to create reader pod for namespace %d", i)
		suite.trackPod(ns.Name, pod.Name)

		err = suite.helper.WaitForPodSuccess(ctx, ns.Name, pod.Name, testutils.TestConstants.DefaultTimeout)
		require.NoError(suite.T(), err, "Reader pod should find correct data for namespace %d", i)
	}
}

func (suite *SystemIntegrationTestSuite) describeFilesystem(ctx context.Context, fsId string) (*types.FileSystemDescription, error) {
	input := &efs.DescribeFileSystemsInput{
		FileSystemId: &fsId,
	}
	
	output, err := suite.efsClient.DescribeFileSystems(ctx, input)
	if err != nil {
		return nil, err
	}
	
	if len(output.FileSystems) == 0 {
		return nil, fmt.Errorf("filesystem %s does not exist", fsId)
	}
	
	return &output.FileSystems[0], nil
}

func (suite *SystemIntegrationTestSuite) isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "does not exist") ||
		   strings.Contains(errMsg, "not found") ||
		   strings.Contains(errMsg, "notfound")
}

func (suite *SystemIntegrationTestSuite) trackPVC(namespace, pvcName string) {
	suite.createdResources.mutex.Lock()
	defer suite.createdResources.mutex.Unlock()
	
	if suite.createdResources.PVCs[namespace] == nil {
		suite.createdResources.PVCs[namespace] = []string{}
	}
	suite.createdResources.PVCs[namespace] = append(suite.createdResources.PVCs[namespace], pvcName)
}

func (suite *SystemIntegrationTestSuite) trackPod(namespace, podName string) {
	suite.createdResources.mutex.Lock()
	defer suite.createdResources.mutex.Unlock()
	
	if suite.createdResources.Pods[namespace] == nil {
		suite.createdResources.Pods[namespace] = []string{}
	}
	suite.createdResources.Pods[namespace] = append(suite.createdResources.Pods[namespace], podName)
}

func (suite *SystemIntegrationTestSuite) loadTestConfig() {
	// Set default configuration
	suite.testConfig = &SystemTestConfig{
		Region:                  getEnvOrDefault("AWS_REGION", "us-west-2"),
		ClusterName:             getEnvOrDefault("CLUSTER_NAME", "test-cluster"),
		TestTimeout:             getDurationEnvOrDefault("TEST_TIMEOUT", 300*time.Second),
		CleanupTimeout:          getDurationEnvOrDefault("CLEANUP_TIMEOUT", 180*time.Second),
		PerformanceTestEnabled:  getBoolEnvOrDefault("ENABLE_PERFORMANCE_TESTS", true),
		SecurityTestEnabled:     getBoolEnvOrDefault("ENABLE_SECURITY_TESTS", true),
		ChaosTestEnabled:        getBoolEnvOrDefault("ENABLE_CHAOS_TESTS", false),
		MaxConcurrentOperations: getIntEnvOrDefault("MAX_CONCURRENT_OPERATIONS", 10),
		EnableMetricsValidation: getBoolEnvOrDefault("ENABLE_METRICS_VALIDATION", true),
		SkipCleanup:             getBoolEnvOrDefault("SKIP_CLEANUP", false),
	}
}

func (suite *SystemIntegrationTestSuite) performComprehensiveCleanup(ctx context.Context) {
	errors := suite.helper.CleanupResources(ctx)
	
	// Clean up tracked resources
	suite.createdResources.mutex.RLock()
	defer suite.createdResources.mutex.RUnlock()
	
	// Clean up EFS filesystems
	for fsId := range suite.createdResources.FileSystems {
		suite.cleanupFilesystem(ctx, fsId)
	}

	// Clean up storage classes
	for _, scName := range suite.createdResources.StorageClasses {
		err := suite.client.StorageV1().StorageClasses().Delete(ctx, scName, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			errors = append(errors, fmt.Errorf("failed to delete StorageClass %s: %w", scName, err))
		}
	}

	// Clean up test namespaces
	for _, nsName := range suite.createdResources.TestNamespaces {
		err := suite.client.CoreV1().Namespaces().Delete(ctx, nsName, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			errors = append(errors, fmt.Errorf("failed to delete namespace %s: %w", nsName, err))
		}
	}

	if len(errors) > 0 {
		suite.T().Logf("Cleanup completed with %d errors:", len(errors))
		for _, err := range errors {
			suite.T().Logf("  - %v", err)
		}
	} else {
		suite.T().Log("Cleanup completed successfully")
	}
}

func (suite *SystemIntegrationTestSuite) cleanupFilesystem(ctx context.Context, fsId string) {
	// Delete mount targets first
	mountTargets, err := suite.efsClient.DescribeMountTargets(ctx, &efs.DescribeMountTargetsInput{
		FileSystemId: &fsId,
	})
	
	if err == nil {
		for _, mt := range mountTargets.MountTargets {
			_, err := suite.efsClient.DeleteMountTarget(ctx, &efs.DeleteMountTargetInput{
				MountTargetId: mt.MountTargetId,
			})
			if err != nil {
				suite.T().Logf("Warning: Failed to delete mount target %s: %v", *mt.MountTargetId, err)
			}
		}
		
		// Wait for mount targets to be deleted
		time.Sleep(30 * time.Second)
	}

	// Delete filesystem
	_, err = suite.efsClient.DeleteFileSystem(ctx, &efs.DeleteFileSystemInput{
		FileSystemId: &fsId,
	})
	if err != nil && !suite.isNotFoundError(err) {
		suite.T().Logf("Warning: Failed to delete filesystem %s: %v", fsId, err)
	}
}

func (suite *SystemIntegrationTestSuite) generateTestReport() {
	report := map[string]interface{}{
		"testConfig":         suite.testConfig,
		"systemMetrics":      suite.createdResources.SystemMetrics,
		"performanceMetrics": suite.createdResources.PerformanceMetrics,
		"resourcesSummary": map[string]interface{}{
			"filesystemsCreated":     len(suite.createdResources.FileSystems),
			"storageClassesCreated":  len(suite.createdResources.StorageClasses),
			"namespacesCreated":      len(suite.createdResources.TestNamespaces),
			"totalPVCsCreated":       suite.getTotalPVCsCreated(),
			"totalPodsCreated":       suite.getTotalPodsCreated(),
		},
	}

	reportJSON, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		suite.T().Logf("Failed to generate test report: %v", err)
		return
	}

	// Write report to file
	reportFile := fmt.Sprintf("efs-ns-system-test-report-%d.json", time.Now().Unix())
	err = os.WriteFile(reportFile, reportJSON, 0644)
	if err != nil {
		suite.T().Logf("Failed to write test report to file: %v", err)
	} else {
		suite.T().Logf("Test report written to: %s", reportFile)
	}

	// Log summary
	suite.T().Logf("System Integration Test Summary:")
	suite.T().Logf("  - Filesystems created: %d", len(suite.createdResources.FileSystems))
	suite.T().Logf("  - Namespaces tested: %d", len(suite.createdResources.TestNamespaces))
	suite.T().Logf("  - Total PVCs tested: %d", suite.getTotalPVCsCreated())
	suite.T().Logf("  - Average PVC bind time: %v", suite.createdResources.SystemMetrics.AvgPVCBindTime)
}

func (suite *SystemIntegrationTestSuite) getTotalPVCsCreated() int {
	total := 0
	for _, pvcs := range suite.createdResources.PVCs {
		total += len(pvcs)
	}
	return total
}

func (suite *SystemIntegrationTestSuite) getTotalPodsCreated() int {
	total := 0
	for _, pods := range suite.createdResources.Pods {
		total += len(pods)
	}
	return total
}

// Environment helper functions
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getDurationEnvOrDefault(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

func getBoolEnvOrDefault(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if b, err := strconv.ParseBool(value); err == nil {
			return b
		}
	}
	return defaultValue
}

func getIntEnvOrDefault(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

// TestSystemIntegration is the main test entry point
func TestSystemIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping system integration tests in short mode")
	}

	suite.Run(t, new(SystemIntegrationTestSuite))
}