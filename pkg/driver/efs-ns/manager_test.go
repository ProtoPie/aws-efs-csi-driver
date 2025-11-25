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

package efsns

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"k8s.io/client-go/kubernetes/fake"
)

// Mock implementations for testing

type mockEFSClient struct {
	createFileSystemFunc     func(ctx context.Context, params *efs.CreateFileSystemInput, optFns ...func(*efs.Options)) (*efs.CreateFileSystemOutput, error)
	deleteFileSystemFunc     func(ctx context.Context, params *efs.DeleteFileSystemInput, optFns ...func(*efs.Options)) (*efs.DeleteFileSystemOutput, error)
	describeFileSystemsFunc  func(ctx context.Context, params *efs.DescribeFileSystemsInput, optFns ...func(*efs.Options)) (*efs.DescribeFileSystemsOutput, error)
	describeMountTargetsFunc func(ctx context.Context, params *efs.DescribeMountTargetsInput, optFns ...func(*efs.Options)) (*efs.DescribeMountTargetsOutput, error)
	createMountTargetFunc    func(ctx context.Context, params *efs.CreateMountTargetInput, optFns ...func(*efs.Options)) (*efs.CreateMountTargetOutput, error)
	deleteMountTargetFunc    func(ctx context.Context, params *efs.DeleteMountTargetInput, optFns ...func(*efs.Options)) (*efs.DeleteMountTargetOutput, error)
	createTagsFunc           func(ctx context.Context, params *efs.CreateTagsInput, optFns ...func(*efs.Options)) (*efs.CreateTagsOutput, error)
}

func (m *mockEFSClient) CreateFileSystem(ctx context.Context, params *efs.CreateFileSystemInput, optFns ...func(*efs.Options)) (*efs.CreateFileSystemOutput, error) {
	if m.createFileSystemFunc != nil {
		return m.createFileSystemFunc(ctx, params, optFns...)
	}
	return nil, errors.New("not implemented")
}

func (m *mockEFSClient) DeleteFileSystem(ctx context.Context, params *efs.DeleteFileSystemInput, optFns ...func(*efs.Options)) (*efs.DeleteFileSystemOutput, error) {
	if m.deleteFileSystemFunc != nil {
		return m.deleteFileSystemFunc(ctx, params, optFns...)
	}
	return nil, errors.New("not implemented")
}

func (m *mockEFSClient) DescribeFileSystems(ctx context.Context, params *efs.DescribeFileSystemsInput, optFns ...func(*efs.Options)) (*efs.DescribeFileSystemsOutput, error) {
	if m.describeFileSystemsFunc != nil {
		return m.describeFileSystemsFunc(ctx, params, optFns...)
	}
	return nil, errors.New("not implemented")
}

func (m *mockEFSClient) DescribeMountTargets(ctx context.Context, params *efs.DescribeMountTargetsInput, optFns ...func(*efs.Options)) (*efs.DescribeMountTargetsOutput, error) {
	if m.describeMountTargetsFunc != nil {
		return m.describeMountTargetsFunc(ctx, params, optFns...)
	}
	return nil, errors.New("not implemented")
}

func (m *mockEFSClient) CreateMountTarget(ctx context.Context, params *efs.CreateMountTargetInput, optFns ...func(*efs.Options)) (*efs.CreateMountTargetOutput, error) {
	if m.createMountTargetFunc != nil {
		return m.createMountTargetFunc(ctx, params, optFns...)
	}
	return nil, errors.New("not implemented")
}

func (m *mockEFSClient) DeleteMountTarget(ctx context.Context, params *efs.DeleteMountTargetInput, optFns ...func(*efs.Options)) (*efs.DeleteMountTargetOutput, error) {
	if m.deleteMountTargetFunc != nil {
		return m.deleteMountTargetFunc(ctx, params, optFns...)
	}
	return nil, errors.New("not implemented")
}

func (m *mockEFSClient) CreateTags(ctx context.Context, params *efs.CreateTagsInput, optFns ...func(*efs.Options)) (*efs.CreateTagsOutput, error) {
	if m.createTagsFunc != nil {
		return m.createTagsFunc(ctx, params, optFns...)
	}
	return nil, errors.New("not implemented")
}

type mockEC2Client struct {
	describeSubnetsFunc               func(ctx context.Context, params *ec2.DescribeSubnetsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error)
	describeSecurityGroupsFunc        func(ctx context.Context, params *ec2.DescribeSecurityGroupsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error)
	createSecurityGroupFunc           func(ctx context.Context, params *ec2.CreateSecurityGroupInput, optFns ...func(*ec2.Options)) (*ec2.CreateSecurityGroupOutput, error)
	deleteSecurityGroupFunc           func(ctx context.Context, params *ec2.DeleteSecurityGroupInput, optFns ...func(*ec2.Options)) (*ec2.DeleteSecurityGroupOutput, error)
	authorizeSecurityGroupIngressFunc func(ctx context.Context, params *ec2.AuthorizeSecurityGroupIngressInput, optFns ...func(*ec2.Options)) (*ec2.AuthorizeSecurityGroupIngressOutput, error)
	describeVpcsFunc                  func(ctx context.Context, params *ec2.DescribeVpcsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeVpcsOutput, error)
}

func (m *mockEC2Client) DescribeSubnets(ctx context.Context, params *ec2.DescribeSubnetsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error) {
	if m.describeSubnetsFunc != nil {
		return m.describeSubnetsFunc(ctx, params, optFns...)
	}
	return nil, errors.New("not implemented")
}

func (m *mockEC2Client) DescribeSecurityGroups(ctx context.Context, params *ec2.DescribeSecurityGroupsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error) {
	if m.describeSecurityGroupsFunc != nil {
		return m.describeSecurityGroupsFunc(ctx, params, optFns...)
	}
	return nil, errors.New("not implemented")
}

func (m *mockEC2Client) CreateSecurityGroup(ctx context.Context, params *ec2.CreateSecurityGroupInput, optFns ...func(*ec2.Options)) (*ec2.CreateSecurityGroupOutput, error) {
	if m.createSecurityGroupFunc != nil {
		return m.createSecurityGroupFunc(ctx, params, optFns...)
	}
	return nil, errors.New("not implemented")
}

func (m *mockEC2Client) DeleteSecurityGroup(ctx context.Context, params *ec2.DeleteSecurityGroupInput, optFns ...func(*ec2.Options)) (*ec2.DeleteSecurityGroupOutput, error) {
	if m.deleteSecurityGroupFunc != nil {
		return m.deleteSecurityGroupFunc(ctx, params, optFns...)
	}
	return nil, errors.New("not implemented")
}

func (m *mockEC2Client) AuthorizeSecurityGroupIngress(ctx context.Context, params *ec2.AuthorizeSecurityGroupIngressInput, optFns ...func(*ec2.Options)) (*ec2.AuthorizeSecurityGroupIngressOutput, error) {
	if m.authorizeSecurityGroupIngressFunc != nil {
		return m.authorizeSecurityGroupIngressFunc(ctx, params, optFns...)
	}
	return nil, errors.New("not implemented")
}

func (m *mockEC2Client) DescribeVpcs(ctx context.Context, params *ec2.DescribeVpcsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeVpcsOutput, error) {
	if m.describeVpcsFunc != nil {
		return m.describeVpcsFunc(ctx, params, optFns...)
	}
	return nil, errors.New("not implemented")
}

// Test helper functions

func assertEqual(t *testing.T, expected, actual interface{}) {
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}
}

func assertNotNil(t *testing.T, value interface{}) {
	if value == nil {
		t.Error("Expected non-nil value")
	}
}

func assertNil(t *testing.T, value interface{}) {
	if value != nil {
		// Check for typed nil pointers
		if reflect.ValueOf(value).Kind() == reflect.Ptr && reflect.ValueOf(value).IsNil() {
			return // This is actually nil, just typed
		}
		t.Errorf("Expected nil, but got %v (type: %T)", value, value)
	}
}

func assertError(t *testing.T, err error) {
	if err == nil {
		t.Error("Expected error, but got nil")
	}
}

func assertNoError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("Expected no error, but got: %v", err)
	}
}

func assertContains(t *testing.T, str, substr string) {
	if !strings.Contains(str, substr) {
		t.Errorf("Expected string to contain '%s', but got '%s'", substr, str)
	}
}

// Test functions

func TestNewNamespaceFileSystemManager(t *testing.T) {
	mockEFS := &mockEFSClient{}
	mockEC2 := &mockEC2Client{}
	k8sClient := fake.NewSimpleClientset()
	cache := NewFileSystemCache(nil)
	tracker := &mockTracker{}

	manager := NewNamespaceFileSystemManager(mockEFS, mockEC2, k8sClient, cache, tracker, "cluster-id", "vpc-12345")

	assertNotNil(t, manager)
	nsManager, ok := manager.(*namespaceFileSystemManager)
	if !ok {
		t.Error("Expected namespaceFileSystemManager type")
	}
	assertEqual(t, "cluster-id", nsManager.clusterID)
	assertEqual(t, "vpc-12345", nsManager.vpcID)
}

func TestCreateOrGetFileSystemForNamespace_CacheHit(t *testing.T) {
	ctx := context.Background()
	namespace := "test-namespace"
	encrypted := true
	throughput := int64(100)
	options := &FileSystemOptions{
		PerformanceMode:              "generalPurpose",
		ThroughputMode:               "provisioned",
		ProvisionedThroughputInMibps: &throughput,
		Encrypted:                    &encrypted,
	}

	// Setup cache with existing filesystem
	cache := NewFileSystemCache(nil)
	fsInfo := &FileSystemInfo{
		FileSystemID:    "fs-12345",
		Namespace:       namespace,
		ClusterID:       "test-cluster",
		CreatedAt:       time.Now(),
		SecurityGroupID: "sg-12345",
		State:           FileSystemStateAvailable,
		PVCCount:        0,
	}
	cache.Set(namespace, fsInfo)

	manager := &namespaceFileSystemManager{
		efsClient: &mockEFSClient{},
		ec2Client: &mockEC2Client{},
		cache:     cache,
		tracker:   &mockTracker{},
		clusterID: "test-cluster",
		vpcID:     "vpc-12345",
	}

	result, err := manager.CreateOrGetFileSystemForNamespace(ctx, namespace, options)

	assertNoError(t, err)
	assertNotNil(t, result)
	assertEqual(t, fsInfo.FileSystemID, result.FileSystemID)
}

func TestCreateOrGetFileSystemForNamespace_CreateNew(t *testing.T) {
	ctx := context.Background()
	namespace := "test-namespace"
	encrypted := true
	throughput := int64(100)
	options := &FileSystemOptions{
		PerformanceMode:              "generalPurpose",
		ThroughputMode:               "provisioned",
		ProvisionedThroughputInMibps: &throughput,
		Encrypted:                    &encrypted,
	}

	mockEFS := &mockEFSClient{}
	mockEC2 := &mockEC2Client{}
	cache := NewFileSystemCache(nil)
	tracker := &mockTracker{}

	// Mock EFS CreateFileSystem
	mockEFS.createFileSystemFunc = func(ctx context.Context, params *efs.CreateFileSystemInput, optFns ...func(*efs.Options)) (*efs.CreateFileSystemOutput, error) {
		return &efs.CreateFileSystemOutput{
			FileSystemId:         aws.String("fs-12345"),
			FileSystemArn:        aws.String("arn:aws:elasticfilesystem:us-west-2:123456789012:file-system/fs-12345"),
			Name:                 aws.String("efs-ns-test-namespace"),
			CreationToken:        aws.String("efs-ns-test-namespace-token"),
			PerformanceMode:      efstypes.PerformanceModeGeneralPurpose,
			ThroughputMode:       efstypes.ThroughputModeProvisioned,
			Encrypted:            aws.Bool(true),
			LifeCycleState:       efstypes.LifeCycleStateCreating,
			SizeInBytes:          &efstypes.FileSystemSize{Value: 1024},
			CreationTime:         aws.Time(time.Now()),
			NumberOfMountTargets: 0,
		}, nil
	}

	// Mock EFS DescribeFileSystems for state waiting
	mockEFS.describeFileSystemsFunc = func(ctx context.Context, params *efs.DescribeFileSystemsInput, optFns ...func(*efs.Options)) (*efs.DescribeFileSystemsOutput, error) {
		return &efs.DescribeFileSystemsOutput{
			FileSystems: []efstypes.FileSystemDescription{
				{
					FileSystemId:         aws.String("fs-12345"),
					FileSystemArn:        aws.String("arn:aws:elasticfilesystem:us-west-2:123456789012:file-system/fs-12345"),
					Name:                 aws.String("efs-ns-test-namespace"),
					CreationToken:        aws.String("efs-ns-test-namespace-token"),
					PerformanceMode:      efstypes.PerformanceModeGeneralPurpose,
					ThroughputMode:       efstypes.ThroughputModeProvisioned,
					Encrypted:            aws.Bool(true),
					LifeCycleState:       efstypes.LifeCycleStateAvailable,
					SizeInBytes:          &efstypes.FileSystemSize{Value: 1024},
					CreationTime:         aws.Time(time.Now()),
					NumberOfMountTargets: 2,
				},
			},
		}, nil
	}

	// Mock security group operations
	mockEC2.describeSecurityGroupsFunc = func(ctx context.Context, params *ec2.DescribeSecurityGroupsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error) {
		return &ec2.DescribeSecurityGroupsOutput{
			SecurityGroups: []ec2types.SecurityGroup{}, // No existing security group
		}, nil
	}

	mockEC2.describeSubnetsFunc = func(ctx context.Context, params *ec2.DescribeSubnetsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error) {
		return &ec2.DescribeSubnetsOutput{
			Subnets: []ec2types.Subnet{
				{SubnetId: aws.String("subnet-12345"), VpcId: aws.String("vpc-12345"), AvailabilityZone: aws.String("us-west-2a"), CidrBlock: aws.String("10.0.1.0/24")},
				{SubnetId: aws.String("subnet-67890"), VpcId: aws.String("vpc-12345"), AvailabilityZone: aws.String("us-west-2b"), CidrBlock: aws.String("10.0.2.0/24")},
			},
		}, nil
	}

	mockEC2.createSecurityGroupFunc = func(ctx context.Context, params *ec2.CreateSecurityGroupInput, optFns ...func(*ec2.Options)) (*ec2.CreateSecurityGroupOutput, error) {
		return &ec2.CreateSecurityGroupOutput{
			GroupId: aws.String("sg-12345"),
		}, nil
	}

	mockEC2.authorizeSecurityGroupIngressFunc = func(ctx context.Context, params *ec2.AuthorizeSecurityGroupIngressInput, optFns ...func(*ec2.Options)) (*ec2.AuthorizeSecurityGroupIngressOutput, error) {
		return &ec2.AuthorizeSecurityGroupIngressOutput{}, nil
	}

	mockEC2.describeVpcsFunc = func(ctx context.Context, params *ec2.DescribeVpcsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeVpcsOutput, error) {
		return &ec2.DescribeVpcsOutput{
			Vpcs: []ec2types.Vpc{
				{VpcId: aws.String("vpc-12345"), CidrBlock: aws.String("10.0.0.0/16")},
			},
		}, nil
	}

	// Mock mount target creation
	mockEFS.createMountTargetFunc = func(ctx context.Context, params *efs.CreateMountTargetInput, optFns ...func(*efs.Options)) (*efs.CreateMountTargetOutput, error) {
		return &efs.CreateMountTargetOutput{
			MountTargetId: aws.String("fsmt-12345"),
		}, nil
	}

	// Mock describe mount targets for waiting
	mockEFS.describeMountTargetsFunc = func(ctx context.Context, params *efs.DescribeMountTargetsInput, optFns ...func(*efs.Options)) (*efs.DescribeMountTargetsOutput, error) {
		return &efs.DescribeMountTargetsOutput{
			MountTargets: []efstypes.MountTargetDescription{
				{
					MountTargetId:  aws.String("fsmt-12345"),
					FileSystemId:   aws.String("fs-12345"),
					SubnetId:       aws.String("subnet-12345"),
					LifeCycleState: efstypes.LifeCycleStateAvailable,
				},
				{
					MountTargetId:  aws.String("fsmt-67890"),
					FileSystemId:   aws.String("fs-12345"),
					SubnetId:       aws.String("subnet-67890"),
					LifeCycleState: efstypes.LifeCycleStateAvailable,
				},
			},
		}, nil
	}
	// Mock create tags
	mockEFS.createTagsFunc = func(ctx context.Context, params *efs.CreateTagsInput, optFns ...func(*efs.Options)) (*efs.CreateTagsOutput, error) {
		return &efs.CreateTagsOutput{}, nil
	}

	manager := &namespaceFileSystemManager{
		efsClient: mockEFS,
		ec2Client: mockEC2,
		cache:     cache,
		tracker:   tracker,
		clusterID: "test-cluster",
		vpcID:     "vpc-12345",
	}

	result, err := manager.CreateOrGetFileSystemForNamespace(ctx, namespace, options)

	assertNoError(t, err)
	assertNotNil(t, result)
	assertEqual(t, "fs-12345", result.FileSystemID)
	assertEqual(t, "test-namespace", result.Namespace)
}

func TestCreateOrGetFileSystemForNamespace_InvalidNamespace(t *testing.T) {
	ctx := context.Background()
	options := &FileSystemOptions{}

	manager := &namespaceFileSystemManager{
		efsClient: &mockEFSClient{},
		ec2Client: &mockEC2Client{},
		cache:     NewFileSystemCache(nil),
		tracker:   &mockTracker{},
		clusterID: "test-cluster",
		vpcID:     "vpc-12345",
	}

	result, err := manager.CreateOrGetFileSystemForNamespace(ctx, "", options)

	assertError(t, err)
	assertNil(t, result)
	assertContains(t, err.Error(), "namespace cannot be empty")
}

func TestDeleteFileSystemForNamespace_WithActivePVCs(t *testing.T) {
	ctx := context.Background()
	namespace := "test-namespace"
	volumeID := "efs-ns::test-namespace::fs-12345::test-cluster"

	tracker := &mockTracker{
		getPVCCountFunc: func(ctx context.Context, namespace string) (int32, error) {
			return 2, nil // Active PVCs exist
		},
	}

	manager := &namespaceFileSystemManager{
		efsClient: &mockEFSClient{},
		ec2Client: &mockEC2Client{},
		cache:     NewFileSystemCache(nil),
		tracker:   tracker,
		clusterID: "test-cluster",
		vpcID:     "vpc-12345",
	}

	err := manager.DeleteFileSystemForNamespace(ctx, namespace, volumeID)

	assertError(t, err)
	assertContains(t, err.Error(), "cannot delete filesystem")
}

func TestDeleteFileSystemForNamespace_Success(t *testing.T) {
	ctx := context.Background()
	namespace := "test-namespace"
	volumeID := "efs-ns::test-namespace::fs-12345::test-cluster"

	tracker := &mockTracker{
		getPVCCountFunc: func(ctx context.Context, namespace string) (int32, error) {
			return 0, nil // No active PVCs
		},
	}

	mockEFS := &mockEFSClient{}

	// Mock filesystem describe operation
	mockEFS.describeFileSystemsFunc = func(ctx context.Context, params *efs.DescribeFileSystemsInput, optFns ...func(*efs.Options)) (*efs.DescribeFileSystemsOutput, error) {
		return &efs.DescribeFileSystemsOutput{
			FileSystems: []efstypes.FileSystemDescription{
				{
					FileSystemId:   aws.String("fs-12345"),
					Name:           aws.String("test-namespace-efs"),
					LifeCycleState: efstypes.LifeCycleStateAvailable,
					SizeInBytes:    &efstypes.FileSystemSize{Value: 0},
				},
			},
		}, nil
	}

	// Mock mount target operations
	mockEFS.describeMountTargetsFunc = func(ctx context.Context, params *efs.DescribeMountTargetsInput, optFns ...func(*efs.Options)) (*efs.DescribeMountTargetsOutput, error) {
		return &efs.DescribeMountTargetsOutput{
			MountTargets: []efstypes.MountTargetDescription{
				{MountTargetId: aws.String("fsmt-12345")},
				{MountTargetId: aws.String("fsmt-67890")},
			},
		}, nil
	}

	mockEFS.deleteMountTargetFunc = func(ctx context.Context, params *efs.DeleteMountTargetInput, optFns ...func(*efs.Options)) (*efs.DeleteMountTargetOutput, error) {
		return &efs.DeleteMountTargetOutput{}, nil
	}

	// Mock filesystem deletion
	mockEFS.deleteFileSystemFunc = func(ctx context.Context, params *efs.DeleteFileSystemInput, optFns ...func(*efs.Options)) (*efs.DeleteFileSystemOutput, error) {
		return &efs.DeleteFileSystemOutput{}, nil
	}

	manager := &namespaceFileSystemManager{
		efsClient: mockEFS,
		ec2Client: &mockEC2Client{},
		cache:     NewFileSystemCache(nil),
		tracker:   tracker,
		clusterID: "test-cluster",
		vpcID:     "vpc-12345",
	}

	err := manager.DeleteFileSystemForNamespace(ctx, namespace, volumeID)

	assertNoError(t, err)
}

func TestGetFileSystemInfo_CacheHit(t *testing.T) {
	ctx := context.Background()
	namespace := "test-namespace"

	cache := NewFileSystemCache(nil)
	fsInfo := &FileSystemInfo{
		FileSystemID: "fs-12345",
		Namespace:    namespace,
		ClusterID:    "test-cluster",
	}
	cache.Set(namespace, fsInfo)

	manager := &namespaceFileSystemManager{
		efsClient: &mockEFSClient{},
		ec2Client: &mockEC2Client{},
		cache:     cache,
		tracker:   &mockTracker{},
		clusterID: "test-cluster",
		vpcID:     "vpc-12345",
	}

	result, err := manager.GetFileSystemInfo(ctx, namespace)

	assertNoError(t, err)
	assertNotNil(t, result)
	assertEqual(t, "fs-12345", result.FileSystemID)
}

func TestGetFileSystemInfo_NotFound(t *testing.T) {
	ctx := context.Background()
	namespace := "test-namespace"

	mockEFS := &mockEFSClient{}
	mockEFS.describeFileSystemsFunc = func(ctx context.Context, params *efs.DescribeFileSystemsInput, optFns ...func(*efs.Options)) (*efs.DescribeFileSystemsOutput, error) {
		return &efs.DescribeFileSystemsOutput{
			FileSystems: []efstypes.FileSystemDescription{}, // Empty result
		}, nil
	}

	manager := &namespaceFileSystemManager{
		efsClient: mockEFS,
		ec2Client: &mockEC2Client{},
		cache:     NewFileSystemCache(nil),
		tracker:   &mockTracker{},
		clusterID: "test-cluster",
		vpcID:     "vpc-12345",
	}

	result, err := manager.GetFileSystemInfo(ctx, namespace)

	assertError(t, err)
	assertNil(t, result)
	assertContains(t, err.Error(), "filesystem not found")
}

// Mock tracker for testing
type mockTracker struct {
	getPVCCountFunc func(ctx context.Context, namespace string) (int32, error)
}

func (m *mockTracker) AddPVC(ctx context.Context, namespace, pvcName, volumeID string) error {
	return nil
}

func (m *mockTracker) RemovePVC(ctx context.Context, namespace, pvcName string) (isNamespaceEmpty bool, err error) {
	return true, nil
}

func (m *mockTracker) GetPVCCount(ctx context.Context, namespace string) (int32, error) {
	if m.getPVCCountFunc != nil {
		return m.getPVCCountFunc(ctx, namespace)
	}
	return 0, nil
}

func (m *mockTracker) ListPVCsInNamespace(ctx context.Context, namespace string) ([]string, error) {
	return []string{}, nil
}

func (m *mockTracker) SyncWithCluster(ctx context.Context) error {
	return nil
}

// Benchmark tests
func BenchmarkCreateFileSystem(b *testing.B) {
	ctx := context.Background()
	namespace := "bench-namespace"
	encrypted := true
	throughput := int64(100)
	options := &FileSystemOptions{
		PerformanceMode:              "generalPurpose",
		ThroughputMode:               "provisioned",
		ProvisionedThroughputInMibps: &throughput,
		Encrypted:                    &encrypted,
	}

	mockEFS := &mockEFSClient{}
	mockEC2 := &mockEC2Client{}

	// Setup mocks for successful creation
	mockEFS.createFileSystemFunc = func(ctx context.Context, params *efs.CreateFileSystemInput, optFns ...func(*efs.Options)) (*efs.CreateFileSystemOutput, error) {
		return &efs.CreateFileSystemOutput{
			FileSystemId:   aws.String("fs-12345"),
			LifeCycleState: efstypes.LifeCycleStateCreating,
		}, nil
	}

	mockEFS.describeFileSystemsFunc = func(ctx context.Context, params *efs.DescribeFileSystemsInput, optFns ...func(*efs.Options)) (*efs.DescribeFileSystemsOutput, error) {
		return &efs.DescribeFileSystemsOutput{
			FileSystems: []efstypes.FileSystemDescription{
				{
					FileSystemId:   aws.String("fs-12345"),
					LifeCycleState: efstypes.LifeCycleStateAvailable,
					CreationTime:   aws.Time(time.Now()),
				},
			},
		}, nil
	}

	mockEC2.describeSubnetsFunc = func(ctx context.Context, params *ec2.DescribeSubnetsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error) {
		return &ec2.DescribeSubnetsOutput{
			Subnets: []ec2types.Subnet{
				{SubnetId: aws.String("subnet-12345"), VpcId: aws.String("vpc-12345"), CidrBlock: aws.String("10.0.1.0/24")},
			},
		}, nil
	}

	manager := &namespaceFileSystemManager{
		efsClient: mockEFS,
		ec2Client: mockEC2,
		cache:     NewFileSystemCache(nil),
		tracker:   &mockTracker{},
		clusterID: "test-cluster",
		vpcID:     "vpc-12345",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := manager.CreateOrGetFileSystemForNamespace(ctx, fmt.Sprintf("%s-%d", namespace, i), options)
		if err != nil {
			b.Fatalf("CreateOrGetFileSystemForNamespace failed: %v", err)
		}
	}
}

// Mount Target Management Tests

func TestCreateMountTargets(t *testing.T) {
	tests := []struct {
		name          string
		fileSystemID  string
		namespace     string
		setupMocks    func(*mockEFSClient, *mockEC2Client)
		expectedError bool
		errorContains string
	}{
		{
			name:         "successful mount target creation",
			fileSystemID: "fs-12345",
			namespace:    "test-namespace",
			setupMocks: func(mockEFS *mockEFSClient, mockEC2 *mockEC2Client) {
				// Mock VPC subnets
				mockEC2.describeSubnetsFunc = func(ctx context.Context, params *ec2.DescribeSubnetsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error) {
					return &ec2.DescribeSubnetsOutput{
						Subnets: []ec2types.Subnet{
							{SubnetId: aws.String("subnet-1"), VpcId: aws.String("vpc-12345"), CidrBlock: aws.String("10.0.1.0/24")},
							{SubnetId: aws.String("subnet-2"), VpcId: aws.String("vpc-12345"), CidrBlock: aws.String("10.0.2.0/24")},
						},
					}, nil
				}

				// Mock security group creation
				mockEC2.createSecurityGroupFunc = func(ctx context.Context, params *ec2.CreateSecurityGroupInput, optFns ...func(*ec2.Options)) (*ec2.CreateSecurityGroupOutput, error) {
					return &ec2.CreateSecurityGroupOutput{
						GroupId: aws.String("sg-12345"),
					}, nil
				}

				mockEC2.authorizeSecurityGroupIngressFunc = func(ctx context.Context, params *ec2.AuthorizeSecurityGroupIngressInput, optFns ...func(*ec2.Options)) (*ec2.AuthorizeSecurityGroupIngressOutput, error) {
					return &ec2.AuthorizeSecurityGroupIngressOutput{}, nil
				}

				// Mock mount target creation
				mockEFS.createMountTargetFunc = func(ctx context.Context, params *efs.CreateMountTargetInput, optFns ...func(*efs.Options)) (*efs.CreateMountTargetOutput, error) {
					return &efs.CreateMountTargetOutput{
						MountTargetId: aws.String("fsmt-12345"),
					}, nil
				}

				// Mock mount targets status check for availability waiting
				mockEFS.describeMountTargetsFunc = func(ctx context.Context, params *efs.DescribeMountTargetsInput, optFns ...func(*efs.Options)) (*efs.DescribeMountTargetsOutput, error) {
					return &efs.DescribeMountTargetsOutput{
						MountTargets: []efstypes.MountTargetDescription{
							{
								MountTargetId:  aws.String("fsmt-1"),
								SubnetId:       aws.String("subnet-1"),
								LifeCycleState: efstypes.LifeCycleStateAvailable,
								IpAddress:      aws.String("10.0.1.100"),
							},
							{
								MountTargetId:  aws.String("fsmt-2"),
								SubnetId:       aws.String("subnet-2"),
								LifeCycleState: efstypes.LifeCycleStateAvailable,
								IpAddress:      aws.String("10.0.2.100"),
							},
						},
					}, nil
				}
			},
			expectedError: false,
		},
		{
			name:         "no subnets found",
			fileSystemID: "fs-12345",
			namespace:    "test-namespace",
			setupMocks: func(mockEFS *mockEFSClient, mockEC2 *mockEC2Client) {
				mockEC2.describeSubnetsFunc = func(ctx context.Context, params *ec2.DescribeSubnetsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error) {
					return &ec2.DescribeSubnetsOutput{
						Subnets: []ec2types.Subnet{},
					}, nil
				}
			},
			expectedError: true,
			errorContains: "no subnets found",
		},
		{
			name:         "security group creation failure",
			fileSystemID: "fs-12345",
			namespace:    "test-namespace",
			setupMocks: func(mockEFS *mockEFSClient, mockEC2 *mockEC2Client) {
				mockEC2.describeSubnetsFunc = func(ctx context.Context, params *ec2.DescribeSubnetsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error) {
					return &ec2.DescribeSubnetsOutput{
						Subnets: []ec2types.Subnet{
							{SubnetId: aws.String("subnet-1"), VpcId: aws.String("vpc-12345"), CidrBlock: aws.String("10.0.1.0/24")},
						},
					}, nil
				}

				mockEC2.createSecurityGroupFunc = func(ctx context.Context, params *ec2.CreateSecurityGroupInput, optFns ...func(*ec2.Options)) (*ec2.CreateSecurityGroupOutput, error) {
					return nil, errors.New("security group creation failed")
				}
			},
			expectedError: true,
			errorContains: "failed to create security group",
		},
		{
			name:         "mount target creation failure",
			fileSystemID: "fs-12345",
			namespace:    "test-namespace",
			setupMocks: func(mockEFS *mockEFSClient, mockEC2 *mockEC2Client) {
				mockEC2.describeSubnetsFunc = func(ctx context.Context, params *ec2.DescribeSubnetsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error) {
					return &ec2.DescribeSubnetsOutput{
						Subnets: []ec2types.Subnet{
							{SubnetId: aws.String("subnet-1"), VpcId: aws.String("vpc-12345"), CidrBlock: aws.String("10.0.1.0/24")},
						},
					}, nil
				}

				mockEC2.createSecurityGroupFunc = func(ctx context.Context, params *ec2.CreateSecurityGroupInput, optFns ...func(*ec2.Options)) (*ec2.CreateSecurityGroupOutput, error) {
					return &ec2.CreateSecurityGroupOutput{
						GroupId: aws.String("sg-12345"),
					}, nil
				}

				mockEC2.authorizeSecurityGroupIngressFunc = func(ctx context.Context, params *ec2.AuthorizeSecurityGroupIngressInput, optFns ...func(*ec2.Options)) (*ec2.AuthorizeSecurityGroupIngressOutput, error) {
					return &ec2.AuthorizeSecurityGroupIngressOutput{}, nil
				}

				mockEFS.createMountTargetFunc = func(ctx context.Context, params *efs.CreateMountTargetInput, optFns ...func(*efs.Options)) (*efs.CreateMountTargetOutput, error) {
					return nil, errors.New("mount target creation failed")
				}
			},
			expectedError: true,
			errorContains: "failed to create mount targets",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			mockEFS := &mockEFSClient{}
			mockEC2 := &mockEC2Client{}

			tt.setupMocks(mockEFS, mockEC2)

			manager := &namespaceFileSystemManager{
				efsClient: mockEFS,
				ec2Client: mockEC2,
				clusterID: "test-cluster",
				vpcID:     "vpc-12345",
			}

			err := manager.createMountTargets(ctx, tt.fileSystemID, tt.namespace)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain '%s', got: %v", tt.errorContains, err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

func TestDeleteMountTargets(t *testing.T) {
	tests := []struct {
		name          string
		fileSystemID  string
		setupMocks    func(*mockEFSClient)
		expectedError bool
		errorContains string
	}{
		{
			name:         "successful mount target deletion",
			fileSystemID: "fs-12345",
			setupMocks: func(mockEFS *mockEFSClient) {
				callCount := 0
				mockEFS.describeMountTargetsFunc = func(ctx context.Context, params *efs.DescribeMountTargetsInput, optFns ...func(*efs.Options)) (*efs.DescribeMountTargetsOutput, error) {
					if callCount == 0 {
						// First call: return mount targets
						callCount++
						return &efs.DescribeMountTargetsOutput{
							MountTargets: []efstypes.MountTargetDescription{
								{
									MountTargetId:  aws.String("fsmt-1"),
									SubnetId:       aws.String("subnet-1"),
									LifeCycleState: efstypes.LifeCycleStateAvailable,
								},
								{
									MountTargetId:  aws.String("fsmt-2"),
									SubnetId:       aws.String("subnet-2"),
									LifeCycleState: efstypes.LifeCycleStateAvailable,
								},
							},
						}, nil
					}
					// Subsequent calls: return empty (all deleted)
					return &efs.DescribeMountTargetsOutput{
						MountTargets: []efstypes.MountTargetDescription{},
					}, nil
				}

				mockEFS.deleteMountTargetFunc = func(ctx context.Context, params *efs.DeleteMountTargetInput, optFns ...func(*efs.Options)) (*efs.DeleteMountTargetOutput, error) {
					return &efs.DeleteMountTargetOutput{}, nil
				}
			},
			expectedError: false,
		},
		{
			name:         "mount target deletion failure",
			fileSystemID: "fs-12345",
			setupMocks: func(mockEFS *mockEFSClient) {
				mockEFS.describeMountTargetsFunc = func(ctx context.Context, params *efs.DescribeMountTargetsInput, optFns ...func(*efs.Options)) (*efs.DescribeMountTargetsOutput, error) {
					return &efs.DescribeMountTargetsOutput{
						MountTargets: []efstypes.MountTargetDescription{
							{
								MountTargetId:  aws.String("fsmt-1"),
								SubnetId:       aws.String("subnet-1"),
								LifeCycleState: efstypes.LifeCycleStateAvailable,
							},
						},
					}, nil
				}

				mockEFS.deleteMountTargetFunc = func(ctx context.Context, params *efs.DeleteMountTargetInput, optFns ...func(*efs.Options)) (*efs.DeleteMountTargetOutput, error) {
					return nil, errors.New("mount target deletion failed")
				}
			},
			expectedError: true,
			errorContains: "failed to delete mount targets",
		},
		{
			name:         "no mount targets to delete",
			fileSystemID: "fs-12345",
			setupMocks: func(mockEFS *mockEFSClient) {
				mockEFS.describeMountTargetsFunc = func(ctx context.Context, params *efs.DescribeMountTargetsInput, optFns ...func(*efs.Options)) (*efs.DescribeMountTargetsOutput, error) {
					return &efs.DescribeMountTargetsOutput{
						MountTargets: []efstypes.MountTargetDescription{},
					}, nil
				}
			},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			mockEFS := &mockEFSClient{}

			tt.setupMocks(mockEFS)

			manager := &namespaceFileSystemManager{
				efsClient: mockEFS,
			}

			err := manager.deleteMountTargets(ctx, tt.fileSystemID)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain '%s', got: %v", tt.errorContains, err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

func TestWaitForMountTargetsAvailable(t *testing.T) {
	tests := []struct {
		name          string
		fileSystemID  string
		setupMocks    func(*mockEFSClient)
		expectedError bool
		errorContains string
	}{
		{
			name:         "mount targets become available",
			fileSystemID: "fs-12345",
			setupMocks: func(mockEFS *mockEFSClient) {
				callCount := 0
				mockEFS.describeMountTargetsFunc = func(ctx context.Context, params *efs.DescribeMountTargetsInput, optFns ...func(*efs.Options)) (*efs.DescribeMountTargetsOutput, error) {
					if callCount == 0 {
						// First call: mount targets are creating
						callCount++
						return &efs.DescribeMountTargetsOutput{
							MountTargets: []efstypes.MountTargetDescription{
								{
									MountTargetId:  aws.String("fsmt-1"),
									LifeCycleState: efstypes.LifeCycleStateCreating,
								},
							},
						}, nil
					}
					// Second call: mount targets are available
					return &efs.DescribeMountTargetsOutput{
						MountTargets: []efstypes.MountTargetDescription{
							{
								MountTargetId:  aws.String("fsmt-1"),
								LifeCycleState: efstypes.LifeCycleStateAvailable,
							},
						},
					}, nil
				}
			},
			expectedError: false,
		},
		{
			name:         "mount target enters error state",
			fileSystemID: "fs-12345",
			setupMocks: func(mockEFS *mockEFSClient) {
				mockEFS.describeMountTargetsFunc = func(ctx context.Context, params *efs.DescribeMountTargetsInput, optFns ...func(*efs.Options)) (*efs.DescribeMountTargetsOutput, error) {
					return &efs.DescribeMountTargetsOutput{
						MountTargets: []efstypes.MountTargetDescription{
							{
								MountTargetId:  aws.String("fsmt-1"),
								LifeCycleState: "error",
							},
						},
					}, nil
				}
			},
			expectedError: true,
			errorContains: "entered error state",
		},
		{
			name:         "describe mount targets failure",
			fileSystemID: "fs-12345",
			setupMocks: func(mockEFS *mockEFSClient) {
				mockEFS.describeMountTargetsFunc = func(ctx context.Context, params *efs.DescribeMountTargetsInput, optFns ...func(*efs.Options)) (*efs.DescribeMountTargetsOutput, error) {
					return nil, errors.New("describe mount targets failed")
				}
			},
			expectedError: true,
			errorContains: "failed to check mount targets",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second) // Longer timeout for tests
			defer cancel()

			mockEFS := &mockEFSClient{}
			tt.setupMocks(mockEFS)

			manager := &namespaceFileSystemManager{
				efsClient: mockEFS,
			}

			err := manager.waitForMountTargetsAvailable(ctx, tt.fileSystemID)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain '%s', got: %v", tt.errorContains, err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

func TestWaitForMountTargetsDeleted(t *testing.T) {
	tests := []struct {
		name          string
		fileSystemID  string
		setupMocks    func(*mockEFSClient)
		expectedError bool
		errorContains string
	}{
		{
			name:         "mount targets are deleted",
			fileSystemID: "fs-12345",
			setupMocks: func(mockEFS *mockEFSClient) {
				callCount := 0
				mockEFS.describeMountTargetsFunc = func(ctx context.Context, params *efs.DescribeMountTargetsInput, optFns ...func(*efs.Options)) (*efs.DescribeMountTargetsOutput, error) {
					if callCount == 0 {
						// First call: mount targets still exist
						callCount++
						return &efs.DescribeMountTargetsOutput{
							MountTargets: []efstypes.MountTargetDescription{
								{
									MountTargetId:  aws.String("fsmt-1"),
									LifeCycleState: efstypes.LifeCycleStateDeleting,
								},
							},
						}, nil
					}
					// Second call: mount targets are deleted
					return &efs.DescribeMountTargetsOutput{
						MountTargets: []efstypes.MountTargetDescription{},
					}, nil
				}
			},
			expectedError: false,
		},
		{
			name:         "describe mount targets failure",
			fileSystemID: "fs-12345",
			setupMocks: func(mockEFS *mockEFSClient) {
				mockEFS.describeMountTargetsFunc = func(ctx context.Context, params *efs.DescribeMountTargetsInput, optFns ...func(*efs.Options)) (*efs.DescribeMountTargetsOutput, error) {
					return nil, errors.New("describe mount targets failed")
				}
			},
			expectedError: true,
			errorContains: "failed to check mount targets",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second) // Allow time for ticker operations
			defer cancel()

			mockEFS := &mockEFSClient{}
			tt.setupMocks(mockEFS)

			manager := &namespaceFileSystemManager{
				efsClient: mockEFS,
			}

			err := manager.waitForMountTargetsDeleted(ctx, tt.fileSystemID)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain '%s', got: %v", tt.errorContains, err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

// Note: Helper function conversion tests removed as these functions are not needed for core functionality
