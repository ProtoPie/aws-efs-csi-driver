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

package utils

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	clientset "k8s.io/client-go/kubernetes"
)

// TestResourceTracker helps track resources created during tests for cleanup
type TestResourceTracker struct {
	FileSystems    []string
	MountTargets   []string
	SecurityGroups []string
	Namespaces     []string
	StorageClasses []string
	PVCs           map[string][]string // namespace -> pvc names
	ConfigMaps     map[string][]string // namespace -> configmap names
}

// NewTestResourceTracker creates a new resource tracker
func NewTestResourceTracker() *TestResourceTracker {
	return &TestResourceTracker{
		PVCs:       make(map[string][]string),
		ConfigMaps: make(map[string][]string),
	}
}

// AddFileSystem adds a filesystem ID for tracking
func (t *TestResourceTracker) AddFileSystem(fsID string) {
	t.FileSystems = append(t.FileSystems, fsID)
}

// AddMountTarget adds a mount target ID for tracking
func (t *TestResourceTracker) AddMountTarget(mtID string) {
	t.MountTargets = append(t.MountTargets, mtID)
}

// AddSecurityGroup adds a security group ID for tracking
func (t *TestResourceTracker) AddSecurityGroup(sgID string) {
	t.SecurityGroups = append(t.SecurityGroups, sgID)
}

// AddNamespace adds a namespace for tracking
func (t *TestResourceTracker) AddNamespace(namespace string) {
	t.Namespaces = append(t.Namespaces, namespace)
}

// AddStorageClass adds a storage class for tracking
func (t *TestResourceTracker) AddStorageClass(scName string) {
	t.StorageClasses = append(t.StorageClasses, scName)
}

// AddPVC adds a PVC for tracking
func (t *TestResourceTracker) AddPVC(namespace, pvcName string) {
	if t.PVCs[namespace] == nil {
		t.PVCs[namespace] = []string{}
	}
	t.PVCs[namespace] = append(t.PVCs[namespace], pvcName)
}

// AddConfigMap adds a ConfigMap for tracking
func (t *TestResourceTracker) AddConfigMap(namespace, cmName string) {
	if t.ConfigMaps[namespace] == nil {
		t.ConfigMaps[namespace] = []string{}
	}
	t.ConfigMaps[namespace] = append(t.ConfigMaps[namespace], cmName)
}

// TestConstants contains common test constants
var TestConstants = struct {
	DefaultTimeout     time.Duration
	LongTimeout        time.Duration
	CleanupTimeout     time.Duration
	PollingInterval    time.Duration
	EfsNsProvisioning  string
	EfsApProvisioning  string
	DefaultDriverName  string
	TestImageName      string
}{
	DefaultTimeout:     120 * time.Second,
	LongTimeout:        300 * time.Second,
	CleanupTimeout:     180 * time.Second,
	PollingInterval:    5 * time.Second,
	EfsNsProvisioning:  "efs-ns",
	EfsApProvisioning:  "efs-ap",
	DefaultDriverName:  "efs.csi.aws.com",
	TestImageName:      "busybox:1.35",
}

// TestNamespace contains helper functions for namespace operations
type TestNamespace struct {
	client clientset.Interface
}

// NewTestNamespace creates a new namespace helper
func NewTestNamespace(client clientset.Interface) *TestNamespace {
	return &TestNamespace{client: client}
}

// Create creates a test namespace with a random suffix
func (tn *TestNamespace) Create(ctx context.Context, namePrefix string) (*v1.Namespace, error) {
	namespace := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namePrefix + "-" + rand.String(6),
			Labels: map[string]string{
				"test-type": "efs-csi-test",
			},
		},
	}

	return tn.client.CoreV1().Namespaces().Create(ctx, namespace, metav1.CreateOptions{})
}

// Delete deletes a namespace
func (tn *TestNamespace) Delete(ctx context.Context, name string) error {
	return tn.client.CoreV1().Namespaces().Delete(ctx, name, metav1.DeleteOptions{})
}

// WaitForDeletion waits for namespace deletion
func (tn *TestNamespace) WaitForDeletion(ctx context.Context, name string, timeout time.Duration) error {
	return WaitForCondition(ctx, timeout, TestConstants.PollingInterval, func() (bool, error) {
		_, err := tn.client.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if IsNotFoundError(err) {
				return true, nil
			}
			return false, err
		}
		return false, nil
	})
}

// ConditionFunc is a function that returns true when a condition is met
type ConditionFunc func() (bool, error)

// WaitForCondition waits for a condition to be met within timeout
func WaitForCondition(ctx context.Context, timeout, interval time.Duration, condition ConditionFunc) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	timeoutTimer := time.NewTimer(timeout)
	defer timeoutTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeoutTimer.C:
			return fmt.Errorf("timeout after %v waiting for condition", timeout)
		case <-ticker.C:
			done, err := condition()
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		}
	}
}

// IsNotFoundError checks if error is a not found error
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	// Check for various not found error patterns
	errMsg := err.Error()
	return containsAny(errMsg, []string{
		"not found",
		"NotFound",
		"does not exist",
		"DoesNotExist",
	})
}

// containsAny checks if a string contains any of the given substrings
func containsAny(s string, substrings []string) bool {
	for _, substr := range substrings {
		if contains(s, substr) {
			return true
		}
	}
	return false
}

// contains checks if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || indexString(s, substr) >= 0)
}

// indexString returns the index of the first occurrence of substr in s
func indexString(s, substr string) int {
	if len(substr) == 0 {
		return 0
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// RandomString generates a random string of given length
func RandomString(length int) string {
	return rand.String(length)
}

// RandomStringWithPrefix generates a random string with a prefix
func RandomStringWithPrefix(prefix string, length int) string {
	return prefix + "-" + RandomString(length)
}

// TestConfigMapBuilder helps build ConfigMaps for testing
type TestConfigMapBuilder struct {
	namespace string
	name      string
	data      map[string]string
	labels    map[string]string
}

// NewTestConfigMapBuilder creates a new ConfigMap builder
func NewTestConfigMapBuilder(namespace, name string) *TestConfigMapBuilder {
	return &TestConfigMapBuilder{
		namespace: namespace,
		name:      name,
		data:      make(map[string]string),
		labels:    make(map[string]string),
	}
}

// WithData adds data to the ConfigMap
func (b *TestConfigMapBuilder) WithData(key, value string) *TestConfigMapBuilder {
	b.data[key] = value
	return b
}

// WithLabel adds a label to the ConfigMap
func (b *TestConfigMapBuilder) WithLabel(key, value string) *TestConfigMapBuilder {
	b.labels[key] = value
	return b
}

// Build builds the ConfigMap
func (b *TestConfigMapBuilder) Build() *v1.ConfigMap {
	return &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      b.name,
			Namespace: b.namespace,
			Labels:    b.labels,
		},
		Data: b.data,
	}
}

// MockEFSClient provides a mock EFS client for testing
type MockEFSClient struct {
	mock.Mock
}

// CreateFileSystem mock implementation
func (m *MockEFSClient) CreateFileSystem(ctx context.Context, input *efs.CreateFileSystemInput, opts ...func(*efs.Options)) (*efs.CreateFileSystemOutput, error) {
	args := m.Called(ctx, input, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*efs.CreateFileSystemOutput), args.Error(1)
}

// DeleteFileSystem mock implementation
func (m *MockEFSClient) DeleteFileSystem(ctx context.Context, input *efs.DeleteFileSystemInput, opts ...func(*efs.Options)) (*efs.DeleteFileSystemOutput, error) {
	args := m.Called(ctx, input, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*efs.DeleteFileSystemOutput), args.Error(1)
}

// DescribeFileSystems mock implementation
func (m *MockEFSClient) DescribeFileSystems(ctx context.Context, input *efs.DescribeFileSystemsInput, opts ...func(*efs.Options)) (*efs.DescribeFileSystemsOutput, error) {
	args := m.Called(ctx, input, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*efs.DescribeFileSystemsOutput), args.Error(1)
}

// CreateMountTarget mock implementation
func (m *MockEFSClient) CreateMountTarget(ctx context.Context, input *efs.CreateMountTargetInput, opts ...func(*efs.Options)) (*efs.CreateMountTargetOutput, error) {
	args := m.Called(ctx, input, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*efs.CreateMountTargetOutput), args.Error(1)
}

// DescribeMountTargets mock implementation
func (m *MockEFSClient) DescribeMountTargets(ctx context.Context, input *efs.DescribeMountTargetsInput, opts ...func(*efs.Options)) (*efs.DescribeMountTargetsOutput, error) {
	args := m.Called(ctx, input, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*efs.DescribeMountTargetsOutput), args.Error(1)
}

// DeleteMountTarget mock implementation
func (m *MockEFSClient) DeleteMountTarget(ctx context.Context, input *efs.DeleteMountTargetInput, opts ...func(*efs.Options)) (*efs.DeleteMountTargetOutput, error) {
	args := m.Called(ctx, input, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*efs.DeleteMountTargetOutput), args.Error(1)
}

// TagResource mock implementation
func (m *MockEFSClient) TagResource(ctx context.Context, input *efs.TagResourceInput, opts ...func(*efs.Options)) (*efs.TagResourceOutput, error) {
	args := m.Called(ctx, input, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*efs.TagResourceOutput), args.Error(1)
}

// MockEC2Client provides a mock EC2 client for testing
type MockEC2Client struct {
	mock.Mock
}

// DescribeSubnets mock implementation
func (m *MockEC2Client) DescribeSubnets(ctx context.Context, input *ec2.DescribeSubnetsInput, opts ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error) {
	args := m.Called(ctx, input, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ec2.DescribeSubnetsOutput), args.Error(1)
}

// CreateSecurityGroup mock implementation
func (m *MockEC2Client) CreateSecurityGroup(ctx context.Context, input *ec2.CreateSecurityGroupInput, opts ...func(*ec2.Options)) (*ec2.CreateSecurityGroupOutput, error) {
	args := m.Called(ctx, input, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ec2.CreateSecurityGroupOutput), args.Error(1)
}

// AuthorizeSecurityGroupIngress mock implementation
func (m *MockEC2Client) AuthorizeSecurityGroupIngress(ctx context.Context, input *ec2.AuthorizeSecurityGroupIngressInput, opts ...func(*ec2.Options)) (*ec2.AuthorizeSecurityGroupIngressOutput, error) {
	args := m.Called(ctx, input, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ec2.AuthorizeSecurityGroupIngressOutput), args.Error(1)
}

// DeleteSecurityGroup mock implementation
func (m *MockEC2Client) DeleteSecurityGroup(ctx context.Context, input *ec2.DeleteSecurityGroupInput, opts ...func(*ec2.Options)) (*ec2.DeleteSecurityGroupOutput, error) {
	args := m.Called(ctx, input, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ec2.DeleteSecurityGroupOutput), args.Error(1)
}

// DescribeSecurityGroups mock implementation
func (m *MockEC2Client) DescribeSecurityGroups(ctx context.Context, input *ec2.DescribeSecurityGroupsInput, opts ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error) {
	args := m.Called(ctx, input, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ec2.DescribeSecurityGroupsOutput), args.Error(1)
}

// CreateTags mock implementation
func (m *MockEC2Client) CreateTags(ctx context.Context, input *ec2.CreateTagsInput, opts ...func(*ec2.Options)) (*ec2.CreateTagsOutput, error) {
	args := m.Called(ctx, input, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ec2.CreateTagsOutput), args.Error(1)
}

// TestDataFactory provides factory methods for creating test data
type TestDataFactory struct{}

// NewTestDataFactory creates a new test data factory
func NewTestDataFactory() *TestDataFactory {
	return &TestDataFactory{}
}

// CreateFileSystemDescription creates a test FileSystemDescription
func (f *TestDataFactory) CreateFileSystemDescription(fsID, name string) *efstypes.FileSystemDescription {
	return &efstypes.FileSystemDescription{
		FileSystemId:   &fsID,
		Name:           &name,
		LifeCycleState: efstypes.LifeCycleStateAvailable,
		CreationTime:   &time.Time{},
		SizeInBytes: &efstypes.FileSystemSize{
			Value: 1024,
		},
		Tags: []efstypes.Tag{
			{
				Key:   stringPtr("efs-csi-driver/namespace"),
				Value: stringPtr("test-namespace"),
			},
			{
				Key:   stringPtr("efs-csi-driver/cluster"),
				Value: stringPtr("test-cluster"),
			},
		},
	}
}

// CreateMountTargetDescription creates a test MountTargetDescription
func (f *TestDataFactory) CreateMountTargetDescription(mtID, fsID, subnetID string) *efstypes.MountTargetDescription {
	return &efstypes.MountTargetDescription{
		MountTargetId:  &mtID,
		FileSystemId:   &fsID,
		SubnetId:       &subnetID,
		LifeCycleState: efstypes.LifeCycleStateAvailable,
	}
}

// CreateSubnetDescription creates a test Subnet
func (f *TestDataFactory) CreateSubnetDescription(subnetID, vpcID, az string) *types.Subnet {
	return &types.Subnet{
		SubnetId:         &subnetID,
		VpcId:            &vpcID,
		AvailabilityZone: &az,
		State:            types.SubnetStateAvailable,
	}
}

// CreateSecurityGroup creates a test SecurityGroup
func (f *TestDataFactory) CreateSecurityGroup(sgID, vpcID, name string) *types.SecurityGroup {
	return &types.SecurityGroup{
		GroupId:   &sgID,
		VpcId:     &vpcID,
		GroupName: &name,
		Tags: []types.Tag{
			{
				Key:   stringPtr("efs-csi-driver/namespace"),
				Value: stringPtr("test-namespace"),
			},
		},
	}
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}

// Validation helper functions

// ValidateFileSystemTags validates that a filesystem has the expected tags
func ValidateFileSystemTags(fs *efstypes.FileSystemDescription, expectedNamespace, expectedCluster string) error {
	tags := make(map[string]string)
	for _, tag := range fs.Tags {
		if tag.Key != nil && tag.Value != nil {
			tags[*tag.Key] = *tag.Value
		}
	}

	if namespace, exists := tags["efs-csi-driver/namespace"]; !exists || namespace != expectedNamespace {
		return fmt.Errorf("expected namespace tag %s, got %s", expectedNamespace, namespace)
	}

	if cluster, exists := tags["efs-csi-driver/cluster"]; !exists || cluster != expectedCluster {
		return fmt.Errorf("expected cluster tag %s, got %s", expectedCluster, cluster)
	}

	return nil
}

// ValidateSecurityGroupRules validates that a security group has the expected ingress rules
func ValidateSecurityGroupRules(sg *types.SecurityGroup, expectedPort int32) error {
	if sg.IpPermissions == nil || len(sg.IpPermissions) == 0 {
		return fmt.Errorf("security group has no ingress rules")
	}

	for _, rule := range sg.IpPermissions {
		if rule.FromPort != nil && *rule.FromPort == expectedPort {
			return nil // Found the expected rule
		}
	}

	return fmt.Errorf("security group does not have rule for port %d", expectedPort)
}

// Cleanup helper functions

// CleanupResourcesWithTimeout performs cleanup operations with timeout
func CleanupResourcesWithTimeout(ctx context.Context, timeout time.Duration, cleanupFuncs ...func(context.Context) error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for i, cleanupFunc := range cleanupFuncs {
		if err := cleanupFunc(timeoutCtx); err != nil {
			// Log error but continue with other cleanup functions
			fmt.Printf("Warning: Cleanup function %d failed: %v\n", i, err)
		}
	}
}

// RetryWithBackoff retries a function with exponential backoff
func RetryWithBackoff(ctx context.Context, maxRetries int, initialDelay time.Duration, maxDelay time.Duration, fn func() error) error {
	var lastErr error
	delay := initialDelay

	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
		}

		if attempt < maxRetries-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				// Double the delay for next attempt, but don't exceed maxDelay
				delay *= 2
				if delay > maxDelay {
					delay = maxDelay
				}
			}
		}
	}

	return fmt.Errorf("max retries exceeded, last error: %w", lastErr)
}