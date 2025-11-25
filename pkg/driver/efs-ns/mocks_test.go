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
	"time"

	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/efs"
)

// MockFinalizerManager is a mock implementation of FinalizerManager
type MockFinalizerManager struct {
	addFinalizerFunc    func(namespace, finalizerName string) error
	removeFinalizerFunc func(namespace, finalizerName string) error
	hasFinalizerFunc    func(namespace, finalizerName string) (bool, error)
	getFinalizersFunc   func(namespace string) ([]string, error)
	startFunc           func(ctx context.Context) error
	stopFunc            func() error
}

func (m *MockFinalizerManager) AddFinalizer(namespace, finalizerName string) error {
	if m.addFinalizerFunc != nil {
		return m.addFinalizerFunc(namespace, finalizerName)
	}
	return nil
}

func (m *MockFinalizerManager) RemoveFinalizer(namespace, finalizerName string) error {
	if m.removeFinalizerFunc != nil {
		return m.removeFinalizerFunc(namespace, finalizerName)
	}
	return nil
}

func (m *MockFinalizerManager) HasFinalizer(namespace, finalizerName string) (bool, error) {
	if m.hasFinalizerFunc != nil {
		return m.hasFinalizerFunc(namespace, finalizerName)
	}
	return false, nil
}

func (m *MockFinalizerManager) GetFinalizers(namespace string) ([]string, error) {
	if m.getFinalizersFunc != nil {
		return m.getFinalizersFunc(namespace)
	}
	return []string{}, nil
}

func (m *MockFinalizerManager) Start(ctx context.Context) error {
	if m.startFunc != nil {
		return m.startFunc(ctx)
	}
	return nil
}

func (m *MockFinalizerManager) Stop() error {
	if m.stopFunc != nil {
		return m.stopFunc()
	}
	return nil
}

// MockNamespaceFileSystemManager is a mock implementation of NamespaceFileSystemManager
type MockNamespaceFileSystemManager struct {
	createFilesystemForNamespaceFunc   func(namespace, pvcName string, config map[string]string) (string, error)
	deleteFilesystemForNamespaceFunc   func(namespace, pvcName, filesystemID string) error
	getFilesystemForNamespaceFunc      func(namespace, pvcName string) (string, error)
	cleanupFilesystemsForNamespaceFunc func(namespace string, pvcNames []string) error
	startFunc                          func(ctx context.Context) error
	stopFunc                           func() error
}

func (m *MockNamespaceFileSystemManager) CreateFilesystemForNamespace(namespace, pvcName string, config map[string]string) (string, error) {
	if m.createFilesystemForNamespaceFunc != nil {
		return m.createFilesystemForNamespaceFunc(namespace, pvcName, config)
	}
	return "", nil
}

func (m *MockNamespaceFileSystemManager) DeleteFilesystemForNamespace(namespace, pvcName, filesystemID string) error {
	if m.deleteFilesystemForNamespaceFunc != nil {
		return m.deleteFilesystemForNamespaceFunc(namespace, pvcName, filesystemID)
	}
	return nil
}

func (m *MockNamespaceFileSystemManager) GetFilesystemForNamespace(namespace, pvcName string) (string, error) {
	if m.getFilesystemForNamespaceFunc != nil {
		return m.getFilesystemForNamespaceFunc(namespace, pvcName)
	}
	return "", nil
}

func (m *MockNamespaceFileSystemManager) CleanupFilesystemsForNamespace(namespace string, pvcNames []string) error {
	if m.cleanupFilesystemsForNamespaceFunc != nil {
		return m.cleanupFilesystemsForNamespaceFunc(namespace, pvcNames)
	}
	return nil
}

func (m *MockNamespaceFileSystemManager) Start(ctx context.Context) error {
	if m.startFunc != nil {
		return m.startFunc(ctx)
	}
	return nil
}

func (m *MockNamespaceFileSystemManager) Stop() error {
	if m.stopFunc != nil {
		return m.stopFunc()
	}
	return nil
}

// MockPVCTracker is a mock implementation of PVCTracker
type MockPVCTracker struct {
	trackPVCFunc            func(namespace, pvcName, filesystemID string) error
	untrackPVCFunc          func(namespace, pvcName string) error
	getFilesystemIDFunc     func(namespace, pvcName string) (string, error)
	getPVCsForNamespaceFunc func(namespace string) ([]string, error)
	getAllTrackedPVCsFunc   func() (map[string]map[string]string, error)
	syncFromConfigMapsFunc  func() error
	startFunc               func(ctx context.Context) error
	stopFunc                func() error
}

func (m *MockPVCTracker) TrackPVC(namespace, pvcName, filesystemID string) error {
	if m.trackPVCFunc != nil {
		return m.trackPVCFunc(namespace, pvcName, filesystemID)
	}
	return nil
}

func (m *MockPVCTracker) UntrackPVC(namespace, pvcName string) error {
	if m.untrackPVCFunc != nil {
		return m.untrackPVCFunc(namespace, pvcName)
	}
	return nil
}

func (m *MockPVCTracker) GetFilesystemID(namespace, pvcName string) (string, error) {
	if m.getFilesystemIDFunc != nil {
		return m.getFilesystemIDFunc(namespace, pvcName)
	}
	return "", nil
}

func (m *MockPVCTracker) GetPVCsForNamespace(namespace string) ([]string, error) {
	if m.getPVCsForNamespaceFunc != nil {
		return m.getPVCsForNamespaceFunc(namespace)
	}
	return []string{}, nil
}

func (m *MockPVCTracker) GetAllTrackedPVCs() (map[string]map[string]string, error) {
	if m.getAllTrackedPVCsFunc != nil {
		return m.getAllTrackedPVCsFunc()
	}
	return map[string]map[string]string{}, nil
}

func (m *MockPVCTracker) SyncFromConfigMaps() error {
	if m.syncFromConfigMapsFunc != nil {
		return m.syncFromConfigMapsFunc()
	}
	return nil
}

func (m *MockPVCTracker) Start(ctx context.Context) error {
	if m.startFunc != nil {
		return m.startFunc(ctx)
	}
	return nil
}

func (m *MockPVCTracker) Stop() error {
	if m.stopFunc != nil {
		return m.stopFunc()
	}
	return nil
}

// MockEFSClient is a mock implementation of EFSClient
type MockEFSClient struct {
	createFileSystemFunc    func(context.Context, *efs.CreateFileSystemInput) (*efs.CreateFileSystemOutput, error)
	deleteFileSystemFunc    func(context.Context, *efs.DeleteFileSystemInput) (*efs.DeleteFileSystemOutput, error)
	describeFileSystemsFunc func(context.Context, *efs.DescribeFileSystemsInput) (*efs.DescribeFileSystemsOutput, error)
	createMountTargetFunc   func(context.Context, *efs.CreateMountTargetInput) (*efs.CreateMountTargetOutput, error)
	deleteMountTargetFunc   func(context.Context, *efs.DeleteMountTargetInput) (*efs.DeleteMountTargetOutput, error)
	describeMountTargetsFunc func(context.Context, *efs.DescribeMountTargetsInput) (*efs.DescribeMountTargetsOutput, error)
}

func NewMockEFSClient() *MockEFSClient {
	return &MockEFSClient{}
}

func (m *MockEFSClient) CreateFileSystem(ctx context.Context, input *efs.CreateFileSystemInput) (*efs.CreateFileSystemOutput, error) {
	if m.createFileSystemFunc != nil {
		return m.createFileSystemFunc(ctx, input)
	}
	return &efs.CreateFileSystemOutput{}, nil
}

func (m *MockEFSClient) DeleteFileSystem(ctx context.Context, input *efs.DeleteFileSystemInput) (*efs.DeleteFileSystemOutput, error) {
	if m.deleteFileSystemFunc != nil {
		return m.deleteFileSystemFunc(ctx, input)
	}
	return &efs.DeleteFileSystemOutput{}, nil
}

func (m *MockEFSClient) DescribeFileSystems(ctx context.Context, input *efs.DescribeFileSystemsInput) (*efs.DescribeFileSystemsOutput, error) {
	if m.describeFileSystemsFunc != nil {
		return m.describeFileSystemsFunc(ctx, input)
	}
	return &efs.DescribeFileSystemsOutput{}, nil
}

func (m *MockEFSClient) CreateMountTarget(ctx context.Context, input *efs.CreateMountTargetInput) (*efs.CreateMountTargetOutput, error) {
	if m.createMountTargetFunc != nil {
		return m.createMountTargetFunc(ctx, input)
	}
	return &efs.CreateMountTargetOutput{}, nil
}

func (m *MockEFSClient) DeleteMountTarget(ctx context.Context, input *efs.DeleteMountTargetInput) (*efs.DeleteMountTargetOutput, error) {
	if m.deleteMountTargetFunc != nil {
		return m.deleteMountTargetFunc(ctx, input)
	}
	return &efs.DeleteMountTargetOutput{}, nil
}

func (m *MockEFSClient) DescribeMountTargets(ctx context.Context, input *efs.DescribeMountTargetsInput) (*efs.DescribeMountTargetsOutput, error) {
	if m.describeMountTargetsFunc != nil {
		return m.describeMountTargetsFunc(ctx, input)
	}
	return &efs.DescribeMountTargetsOutput{}, nil
}

// MockEC2Client is a mock implementation of EC2Client
type MockEC2Client struct {
	describeSubnetsFunc         func(context.Context, *ec2.DescribeSubnetsInput) (*ec2.DescribeSubnetsOutput, error)
	describeSecurityGroupsFunc  func(context.Context, *ec2.DescribeSecurityGroupsInput) (*ec2.DescribeSecurityGroupsOutput, error)
	createSecurityGroupFunc     func(context.Context, *ec2.CreateSecurityGroupInput) (*ec2.CreateSecurityGroupOutput, error)
	deleteSecurityGroupFunc     func(context.Context, *ec2.DeleteSecurityGroupInput) (*ec2.DeleteSecurityGroupOutput, error)
	authorizeSecurityGroupIngressFunc func(context.Context, *ec2.AuthorizeSecurityGroupIngressInput) (*ec2.AuthorizeSecurityGroupIngressOutput, error)
}

func NewMockEC2Client() *MockEC2Client {
	return &MockEC2Client{}
}

func (m *MockEC2Client) DescribeSubnets(ctx context.Context, input *ec2.DescribeSubnetsInput) (*ec2.DescribeSubnetsOutput, error) {
	if m.describeSubnetsFunc != nil {
		return m.describeSubnetsFunc(ctx, input)
	}
	return &ec2.DescribeSubnetsOutput{}, nil
}

func (m *MockEC2Client) DescribeSecurityGroups(ctx context.Context, input *ec2.DescribeSecurityGroupsInput) (*ec2.DescribeSecurityGroupsOutput, error) {
	if m.describeSecurityGroupsFunc != nil {
		return m.describeSecurityGroupsFunc(ctx, input)
	}
	return &ec2.DescribeSecurityGroupsOutput{}, nil
}

func (m *MockEC2Client) CreateSecurityGroup(ctx context.Context, input *ec2.CreateSecurityGroupInput) (*ec2.CreateSecurityGroupOutput, error) {
	if m.createSecurityGroupFunc != nil {
		return m.createSecurityGroupFunc(ctx, input)
	}
	return &ec2.CreateSecurityGroupOutput{}, nil
}

func (m *MockEC2Client) DeleteSecurityGroup(ctx context.Context, input *ec2.DeleteSecurityGroupInput) (*ec2.DeleteSecurityGroupOutput, error) {
	if m.deleteSecurityGroupFunc != nil {
		return m.deleteSecurityGroupFunc(ctx, input)
	}
	return &ec2.DeleteSecurityGroupOutput{}, nil
}

func (m *MockEC2Client) AuthorizeSecurityGroupIngress(ctx context.Context, input *ec2.AuthorizeSecurityGroupIngressInput) (*ec2.AuthorizeSecurityGroupIngressOutput, error) {
	if m.authorizeSecurityGroupIngressFunc != nil {
		return m.authorizeSecurityGroupIngressFunc(ctx, input)
	}
	return &ec2.AuthorizeSecurityGroupIngressOutput{}, nil
}

// MockRecoveryCache is a mock implementation of cache interface for recovery tests
type MockRecoveryCache struct {
	getFunc func(key string) (interface{}, bool)
	setFunc func(key string, value interface{}, ttl time.Duration)
	deleteFunc func(key string)
	clearFunc func()
}

func NewMockRecoveryCache() *MockRecoveryCache {
	return &MockRecoveryCache{}
}

func (m *MockRecoveryCache) Get(key string) (interface{}, bool) {
	if m.getFunc != nil {
		return m.getFunc(key)
	}
	return nil, false
}

func (m *MockRecoveryCache) Set(key string, value interface{}, ttl time.Duration) {
	if m.setFunc != nil {
		m.setFunc(key, value, ttl)
	}
}

func (m *MockRecoveryCache) Delete(key string) {
	if m.deleteFunc != nil {
		m.deleteFunc(key)
	}
}

func (m *MockRecoveryCache) Clear() {
	if m.clearFunc != nil {
		m.clearFunc()
	}
}

// NewMockPVCTracker creates a new MockPVCTracker instance
func NewMockPVCTracker() *MockPVCTracker {
	return &MockPVCTracker{}
}
