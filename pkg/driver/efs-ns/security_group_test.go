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
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// mockEC2ClientSG is a mock implementation of EC2Client for security group testing
type mockEC2ClientSG struct {
	createSecurityGroupFunc           func(ctx context.Context, params *ec2.CreateSecurityGroupInput, optFns ...func(*ec2.Options)) (*ec2.CreateSecurityGroupOutput, error)
	deleteSecurityGroupFunc           func(ctx context.Context, params *ec2.DeleteSecurityGroupInput, optFns ...func(*ec2.Options)) (*ec2.DeleteSecurityGroupOutput, error)
	describeSecurityGroupsFunc        func(ctx context.Context, params *ec2.DescribeSecurityGroupsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error)
	authorizeSecurityGroupIngressFunc func(ctx context.Context, params *ec2.AuthorizeSecurityGroupIngressInput, optFns ...func(*ec2.Options)) (*ec2.AuthorizeSecurityGroupIngressOutput, error)
	describeVpcsFunc                  func(ctx context.Context, params *ec2.DescribeVpcsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeVpcsOutput, error)
	describeSubnetsFunc               func(ctx context.Context, params *ec2.DescribeSubnetsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error)

	// Track calls for verification
	createSecurityGroupCalls           []ec2.CreateSecurityGroupInput
	deleteSecurityGroupCalls           []ec2.DeleteSecurityGroupInput
	describeSecurityGroupsCalls        []ec2.DescribeSecurityGroupsInput
	authorizeSecurityGroupIngressCalls []ec2.AuthorizeSecurityGroupIngressInput
	describeVpcsCalls                  []ec2.DescribeVpcsInput
}

func (m *mockEC2ClientSG) CreateSecurityGroup(ctx context.Context, params *ec2.CreateSecurityGroupInput, optFns ...func(*ec2.Options)) (*ec2.CreateSecurityGroupOutput, error) {
	if params != nil {
		m.createSecurityGroupCalls = append(m.createSecurityGroupCalls, *params)
	}
	if m.createSecurityGroupFunc != nil {
		return m.createSecurityGroupFunc(ctx, params, optFns...)
	}
	return &ec2.CreateSecurityGroupOutput{GroupId: aws.String("sg-test123")}, nil
}

func (m *mockEC2ClientSG) DeleteSecurityGroup(ctx context.Context, params *ec2.DeleteSecurityGroupInput, optFns ...func(*ec2.Options)) (*ec2.DeleteSecurityGroupOutput, error) {
	if params != nil {
		m.deleteSecurityGroupCalls = append(m.deleteSecurityGroupCalls, *params)
	}
	if m.deleteSecurityGroupFunc != nil {
		return m.deleteSecurityGroupFunc(ctx, params, optFns...)
	}
	return &ec2.DeleteSecurityGroupOutput{}, nil
}

func (m *mockEC2ClientSG) DescribeSecurityGroups(ctx context.Context, params *ec2.DescribeSecurityGroupsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error) {
	if params != nil {
		m.describeSecurityGroupsCalls = append(m.describeSecurityGroupsCalls, *params)
	}
	if m.describeSecurityGroupsFunc != nil {
		return m.describeSecurityGroupsFunc(ctx, params, optFns...)
	}
	return &ec2.DescribeSecurityGroupsOutput{SecurityGroups: []ec2types.SecurityGroup{}}, nil
}

func (m *mockEC2ClientSG) AuthorizeSecurityGroupIngress(ctx context.Context, params *ec2.AuthorizeSecurityGroupIngressInput, optFns ...func(*ec2.Options)) (*ec2.AuthorizeSecurityGroupIngressOutput, error) {
	if params != nil {
		m.authorizeSecurityGroupIngressCalls = append(m.authorizeSecurityGroupIngressCalls, *params)
	}
	if m.authorizeSecurityGroupIngressFunc != nil {
		return m.authorizeSecurityGroupIngressFunc(ctx, params, optFns...)
	}
	return &ec2.AuthorizeSecurityGroupIngressOutput{}, nil
}

func (m *mockEC2ClientSG) DescribeVpcs(ctx context.Context, params *ec2.DescribeVpcsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeVpcsOutput, error) {
	if params != nil {
		m.describeVpcsCalls = append(m.describeVpcsCalls, *params)
	}
	if m.describeVpcsFunc != nil {
		return m.describeVpcsFunc(ctx, params, optFns...)
	}
	return &ec2.DescribeVpcsOutput{
		Vpcs: []ec2types.Vpc{
			{VpcId: aws.String("vpc-test123"), CidrBlock: aws.String("10.0.0.0/16")},
		},
	}, nil
}

func (m *mockEC2ClientSG) DescribeSubnets(ctx context.Context, params *ec2.DescribeSubnetsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error) {
	if m.describeSubnetsFunc != nil {
		return m.describeSubnetsFunc(ctx, params, optFns...)
	}
	return &ec2.DescribeSubnetsOutput{}, nil
}

func TestNewSecurityGroupManager(t *testing.T) {
	ec2Client := &mockEC2ClientSG{}
	clusterID := "test-cluster"
	vpcID := "vpc-test123"

	sgm := NewSecurityGroupManager(ec2Client, clusterID, vpcID)

	if sgm == nil {
		t.Fatal("Expected SecurityGroupManager to be created, got nil")
	}

	if _, ok := sgm.(*securityGroupManager); !ok {
		t.Errorf("Expected SecurityGroupManager to be of type *securityGroupManager")
	}
}

func TestSecurityGroupManager_CreateSecurityGroup_Success(t *testing.T) {
	mock := &mockEC2ClientSG{}

	// Security group doesn't exist yet
	mock.describeSecurityGroupsFunc = func(ctx context.Context, params *ec2.DescribeSecurityGroupsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error) {
		return &ec2.DescribeSecurityGroupsOutput{SecurityGroups: []ec2types.SecurityGroup{}}, nil
	}

	// Create security group
	mock.createSecurityGroupFunc = func(ctx context.Context, params *ec2.CreateSecurityGroupInput, optFns ...func(*ec2.Options)) (*ec2.CreateSecurityGroupOutput, error) {
		expectedName := "efs-ns-test-namespace-test-cluster"
		if *params.GroupName != expectedName {
			t.Errorf("Expected group name %s, got %s", expectedName, *params.GroupName)
		}
		if *params.VpcId != "vpc-test123" {
			t.Errorf("Expected VPC ID vpc-test123, got %s", *params.VpcId)
		}
		return &ec2.CreateSecurityGroupOutput{GroupId: aws.String("sg-new123")}, nil
	}

	// Add NFS ingress rule
	mock.authorizeSecurityGroupIngressFunc = func(ctx context.Context, params *ec2.AuthorizeSecurityGroupIngressInput, optFns ...func(*ec2.Options)) (*ec2.AuthorizeSecurityGroupIngressOutput, error) {
		if *params.GroupId != "sg-new123" {
			t.Errorf("Expected group ID sg-new123, got %s", *params.GroupId)
		}
		if len(params.IpPermissions) != 1 {
			t.Errorf("Expected 1 IP permission, got %d", len(params.IpPermissions))
		}
		perm := params.IpPermissions[0]
		if *perm.IpProtocol != "tcp" {
			t.Errorf("Expected protocol tcp, got %s", *perm.IpProtocol)
		}
		if *perm.FromPort != 2049 {
			t.Errorf("Expected port 2049, got %d", *perm.FromPort)
		}
		return &ec2.AuthorizeSecurityGroupIngressOutput{}, nil
	}

	sgm := NewSecurityGroupManager(mock, "test-cluster", "vpc-test123")
	ctx := context.Background()

	sgInfo, err := sgm.CreateSecurityGroup(ctx, "test-namespace")

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if sgInfo == nil {
		t.Fatal("Expected SecurityGroupInfo to be returned, got nil")
	}

	if sgInfo.SecurityGroupID != "sg-new123" {
		t.Errorf("Expected security group ID sg-new123, got %s", sgInfo.SecurityGroupID)
	}

	if sgInfo.Namespace != "test-namespace" {
		t.Errorf("Expected namespace test-namespace, got %s", sgInfo.Namespace)
	}

	if sgInfo.ClusterID != "test-cluster" {
		t.Errorf("Expected cluster ID test-cluster, got %s", sgInfo.ClusterID)
	}

	if sgInfo.VPCID != "vpc-test123" {
		t.Errorf("Expected VPC ID vpc-test123, got %s", sgInfo.VPCID)
	}

	expectedName := "efs-ns-test-namespace-test-cluster"
	if sgInfo.Name != expectedName {
		t.Errorf("Expected name %s, got %s", expectedName, sgInfo.Name)
	}

	if !strings.Contains(sgInfo.Description, "test-namespace") {
		t.Errorf("Expected description to contain 'test-namespace', got %s", sgInfo.Description)
	}

	if !strings.Contains(sgInfo.Description, "test-cluster") {
		t.Errorf("Expected description to contain 'test-cluster', got %s", sgInfo.Description)
	}

	// Verify tags
	if sgInfo.Tags[SGTagKeyNamespace] != "test-namespace" {
		t.Errorf("Expected namespace tag to be test-namespace, got %s", sgInfo.Tags[SGTagKeyNamespace])
	}

	if sgInfo.Tags[SGTagKeyCluster] != "test-cluster" {
		t.Errorf("Expected cluster tag to be test-cluster, got %s", sgInfo.Tags[SGTagKeyCluster])
	}

	if sgInfo.Tags[SGTagKeyManagedBy] != SGTagValueManagedBy {
		t.Errorf("Expected managed-by tag to be %s, got %s", SGTagValueManagedBy, sgInfo.Tags[SGTagKeyManagedBy])
	}

	// Verify calls
	if len(mock.createSecurityGroupCalls) != 1 {
		t.Errorf("Expected 1 create security group call, got %d", len(mock.createSecurityGroupCalls))
	}

	if len(mock.authorizeSecurityGroupIngressCalls) != 1 {
		t.Errorf("Expected 1 authorize security group ingress call, got %d", len(mock.authorizeSecurityGroupIngressCalls))
	}
}

func TestSecurityGroupManager_CreateSecurityGroup_AlreadyExists(t *testing.T) {
	mock := &mockEC2ClientSG{}

	// Security group already exists
	mock.describeSecurityGroupsFunc = func(ctx context.Context, params *ec2.DescribeSecurityGroupsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error) {
		return &ec2.DescribeSecurityGroupsOutput{
			SecurityGroups: []ec2types.SecurityGroup{
				{
					GroupId:     aws.String("sg-existing123"),
					GroupName:   aws.String("efs-ns-existing-namespace-test-cluster"),
					Description: aws.String("EFS security group for namespace existing-namespace in cluster test-cluster"),
					VpcId:       aws.String("vpc-test123"),
					Tags: []ec2types.Tag{
						{Key: aws.String(SGTagKeyNamespace), Value: aws.String("existing-namespace")},
						{Key: aws.String(SGTagKeyCluster), Value: aws.String("test-cluster")},
						{Key: aws.String(SGTagKeyManagedBy), Value: aws.String(SGTagValueManagedBy)},
					},
				},
			},
		}, nil
	}

	// Should not create a new security group
	mock.createSecurityGroupFunc = func(ctx context.Context, params *ec2.CreateSecurityGroupInput, optFns ...func(*ec2.Options)) (*ec2.CreateSecurityGroupOutput, error) {
		t.Fatal("CreateSecurityGroup should not be called when security group already exists")
		return nil, nil
	}

	sgm := NewSecurityGroupManager(mock, "test-cluster", "vpc-test123")
	ctx := context.Background()

	sgInfo, err := sgm.CreateSecurityGroup(ctx, "existing-namespace")

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if sgInfo == nil {
		t.Fatal("Expected SecurityGroupInfo to be returned, got nil")
	}

	if sgInfo.SecurityGroupID != "sg-existing123" {
		t.Errorf("Expected security group ID sg-existing123, got %s", sgInfo.SecurityGroupID)
	}

	if sgInfo.Namespace != "existing-namespace" {
		t.Errorf("Expected namespace existing-namespace, got %s", sgInfo.Namespace)
	}

	if len(mock.createSecurityGroupCalls) != 0 {
		t.Errorf("Expected 0 create security group calls, got %d", len(mock.createSecurityGroupCalls))
	}
}

func TestSecurityGroupManager_CreateSecurityGroup_EmptyNamespace(t *testing.T) {
	mock := &mockEC2ClientSG{}
	sgm := NewSecurityGroupManager(mock, "test-cluster", "vpc-test123")
	ctx := context.Background()

	_, err := sgm.CreateSecurityGroup(ctx, "")

	if err == nil {
		t.Fatal("Expected error for empty namespace, got nil")
	}

	var efsnsErr *EFSNSError
	if !errors.As(err, &efsnsErr) {
		t.Fatalf("Expected EFSNSError, got %T", err)
	}

	if efsnsErr.Type != ErrInvalidParameter {
		t.Errorf("Expected error type %s, got %s", ErrInvalidParameter, efsnsErr.Type)
	}
}

func TestSecurityGroupManager_DeleteSecurityGroup_Success(t *testing.T) {
	mock := &mockEC2ClientSG{}

	mock.deleteSecurityGroupFunc = func(ctx context.Context, params *ec2.DeleteSecurityGroupInput, optFns ...func(*ec2.Options)) (*ec2.DeleteSecurityGroupOutput, error) {
		if *params.GroupId != "sg-delete123" {
			t.Errorf("Expected group ID sg-delete123, got %s", *params.GroupId)
		}
		return &ec2.DeleteSecurityGroupOutput{}, nil
	}

	sgm := NewSecurityGroupManager(mock, "test-cluster", "vpc-test123")
	ctx := context.Background()

	err := sgm.DeleteSecurityGroup(ctx, "sg-delete123")

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(mock.deleteSecurityGroupCalls) != 1 {
		t.Errorf("Expected 1 delete security group call, got %d", len(mock.deleteSecurityGroupCalls))
	}

	if *mock.deleteSecurityGroupCalls[0].GroupId != "sg-delete123" {
		t.Errorf("Expected group ID sg-delete123, got %s", *mock.deleteSecurityGroupCalls[0].GroupId)
	}
}

func TestSecurityGroupManager_DeleteSecurityGroup_EmptyID(t *testing.T) {
	mock := &mockEC2ClientSG{}
	sgm := NewSecurityGroupManager(mock, "test-cluster", "vpc-test123")
	ctx := context.Background()

	err := sgm.DeleteSecurityGroup(ctx, "")

	if err == nil {
		t.Fatal("Expected error for empty security group ID, got nil")
	}

	var efsnsErr *EFSNSError
	if !errors.As(err, &efsnsErr) {
		t.Fatalf("Expected EFSNSError, got %T", err)
	}

	if efsnsErr.Type != ErrInvalidParameter {
		t.Errorf("Expected error type %s, got %s", ErrInvalidParameter, efsnsErr.Type)
	}
}

func TestSecurityGroupManager_DeleteSecurityGroup_NotFound(t *testing.T) {
	mock := &mockEC2ClientSG{}

	mock.deleteSecurityGroupFunc = func(ctx context.Context, params *ec2.DeleteSecurityGroupInput, optFns ...func(*ec2.Options)) (*ec2.DeleteSecurityGroupOutput, error) {
		return nil, errors.New("InvalidGroupId.NotFound: The security group 'sg-notfound123' does not exist")
	}

	sgm := NewSecurityGroupManager(mock, "test-cluster", "vpc-test123")
	ctx := context.Background()

	err := sgm.DeleteSecurityGroup(ctx, "sg-notfound123")

	// Should consider deletion successful if not found
	if err != nil {
		t.Fatalf("Expected no error for not found security group, got %v", err)
	}
}

func TestSecurityGroupManager_GetSecurityGroup_Success(t *testing.T) {
	mock := &mockEC2ClientSG{}

	mock.describeSecurityGroupsFunc = func(ctx context.Context, params *ec2.DescribeSecurityGroupsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error) {
		// Verify filters
		if len(params.Filters) != 5 {
			t.Errorf("Expected 5 filters, got %d", len(params.Filters))
		}
		return &ec2.DescribeSecurityGroupsOutput{
			SecurityGroups: []ec2types.SecurityGroup{
				{
					GroupId:     aws.String("sg-found123"),
					GroupName:   aws.String("efs-ns-test-namespace-test-cluster"),
					Description: aws.String("EFS security group for namespace test-namespace in cluster test-cluster"),
					VpcId:       aws.String("vpc-test123"),
					Tags: []ec2types.Tag{
						{Key: aws.String(SGTagKeyNamespace), Value: aws.String("test-namespace")},
						{Key: aws.String(SGTagKeyCluster), Value: aws.String("test-cluster")},
						{Key: aws.String(SGTagKeyManagedBy), Value: aws.String(SGTagValueManagedBy)},
					},
				},
			},
		}, nil
	}

	sgm := NewSecurityGroupManager(mock, "test-cluster", "vpc-test123")
	ctx := context.Background()

	sgInfo, err := sgm.GetSecurityGroup(ctx, "test-namespace")

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if sgInfo == nil {
		t.Fatal("Expected SecurityGroupInfo to be returned, got nil")
	}

	if sgInfo.SecurityGroupID != "sg-found123" {
		t.Errorf("Expected security group ID sg-found123, got %s", sgInfo.SecurityGroupID)
	}

	if sgInfo.Namespace != "test-namespace" {
		t.Errorf("Expected namespace test-namespace, got %s", sgInfo.Namespace)
	}

	if sgInfo.ClusterID != "test-cluster" {
		t.Errorf("Expected cluster ID test-cluster, got %s", sgInfo.ClusterID)
	}

	if sgInfo.VPCID != "vpc-test123" {
		t.Errorf("Expected VPC ID vpc-test123, got %s", sgInfo.VPCID)
	}
}

func TestSecurityGroupManager_GetSecurityGroup_NotFound(t *testing.T) {
	mock := &mockEC2ClientSG{}

	mock.describeSecurityGroupsFunc = func(ctx context.Context, params *ec2.DescribeSecurityGroupsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error) {
		return &ec2.DescribeSecurityGroupsOutput{SecurityGroups: []ec2types.SecurityGroup{}}, nil
	}

	sgm := NewSecurityGroupManager(mock, "test-cluster", "vpc-test123")
	ctx := context.Background()

	_, err := sgm.GetSecurityGroup(ctx, "missing-namespace")

	if err == nil {
		t.Fatal("Expected error for missing security group, got nil")
	}

	var efsnsErr *EFSNSError
	if !errors.As(err, &efsnsErr) {
		t.Fatalf("Expected EFSNSError, got %T", err)
	}

	if efsnsErr.Type != ErrSecurityGroupNotFound {
		t.Errorf("Expected error type %s, got %s", ErrSecurityGroupNotFound, efsnsErr.Type)
	}
}

func TestSecurityGroupManager_AddNFSIngressRule_Success(t *testing.T) {
	mock := &mockEC2ClientSG{}

	mock.authorizeSecurityGroupIngressFunc = func(ctx context.Context, params *ec2.AuthorizeSecurityGroupIngressInput, optFns ...func(*ec2.Options)) (*ec2.AuthorizeSecurityGroupIngressOutput, error) {
		if *params.GroupId != "sg-nfs123" {
			t.Errorf("Expected group ID sg-nfs123, got %s", *params.GroupId)
		}

		if len(params.IpPermissions) != 1 {
			t.Errorf("Expected 1 IP permission, got %d", len(params.IpPermissions))
		}

		perm := params.IpPermissions[0]
		if *perm.IpProtocol != "tcp" {
			t.Errorf("Expected protocol tcp, got %s", *perm.IpProtocol)
		}
		if *perm.FromPort != 2049 {
			t.Errorf("Expected from port 2049, got %d", *perm.FromPort)
		}
		if *perm.ToPort != 2049 {
			t.Errorf("Expected to port 2049, got %d", *perm.ToPort)
		}
		if len(perm.IpRanges) != 1 {
			t.Errorf("Expected 1 IP range, got %d", len(perm.IpRanges))
		}
		if *perm.IpRanges[0].CidrIp != "10.0.0.0/16" {
			t.Errorf("Expected CIDR 10.0.0.0/16, got %s", *perm.IpRanges[0].CidrIp)
		}
		if *perm.IpRanges[0].Description != "EFS NFS access from VPC" {
			t.Errorf("Expected description 'EFS NFS access from VPC', got %s", *perm.IpRanges[0].Description)
		}

		return &ec2.AuthorizeSecurityGroupIngressOutput{}, nil
	}

	sgm := NewSecurityGroupManager(mock, "test-cluster", "vpc-test123")
	ctx := context.Background()

	err := sgm.AddNFSIngressRule(ctx, "sg-nfs123", "10.0.0.0/16")

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(mock.authorizeSecurityGroupIngressCalls) != 1 {
		t.Errorf("Expected 1 authorize security group ingress call, got %d", len(mock.authorizeSecurityGroupIngressCalls))
	}
}

func TestSecurityGroupManager_AddNFSIngressRule_AlreadyExists(t *testing.T) {
	mock := &mockEC2ClientSG{}

	mock.authorizeSecurityGroupIngressFunc = func(ctx context.Context, params *ec2.AuthorizeSecurityGroupIngressInput, optFns ...func(*ec2.Options)) (*ec2.AuthorizeSecurityGroupIngressOutput, error) {
		return nil, errors.New("InvalidPermission.Duplicate: the specified rule \"peer: 10.0.0.0/16, TCP, from port: 2049, to port: 2049, ALLOW\" already exists")
	}

	sgm := NewSecurityGroupManager(mock, "test-cluster", "vpc-test123")
	ctx := context.Background()

	err := sgm.AddNFSIngressRule(ctx, "sg-duplicate123", "10.0.0.0/16")

	// Should not error if rule already exists
	if err != nil {
		t.Fatalf("Expected no error for duplicate rule, got %v", err)
	}
}

func TestSecurityGroupManager_AddNFSIngressRule_EmptyParams(t *testing.T) {
	mock := &mockEC2ClientSG{}
	sgm := NewSecurityGroupManager(mock, "test-cluster", "vpc-test123")
	ctx := context.Background()

	// Test empty security group ID
	err := sgm.AddNFSIngressRule(ctx, "", "10.0.0.0/16")
	if err == nil {
		t.Fatal("Expected error for empty security group ID, got nil")
	}

	var efsnsErr *EFSNSError
	if !errors.As(err, &efsnsErr) {
		t.Fatalf("Expected EFSNSError, got %T", err)
	}
	if efsnsErr.Type != ErrInvalidParameter {
		t.Errorf("Expected error type %s, got %s", ErrInvalidParameter, efsnsErr.Type)
	}

	// Test empty VPC CIDR
	err = sgm.AddNFSIngressRule(ctx, "sg-empty-cidr123", "")
	if err == nil {
		t.Fatal("Expected error for empty VPC CIDR, got nil")
	}

	if !errors.As(err, &efsnsErr) {
		t.Fatalf("Expected EFSNSError, got %T", err)
	}
	if efsnsErr.Type != ErrInvalidParameter {
		t.Errorf("Expected error type %s, got %s", ErrInvalidParameter, efsnsErr.Type)
	}
}

func TestSecurityGroupManager_IsSecurityGroupInUse(t *testing.T) {
	mock := &mockEC2ClientSG{}
	sgm := NewSecurityGroupManager(mock, "test-cluster", "vpc-test123")
	ctx := context.Background()

	// Test basic implementation returns false
	inUse, err := sgm.IsSecurityGroupInUse(ctx, "sg-test123")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if inUse {
		t.Error("Expected IsSecurityGroupInUse to return false for basic implementation")
	}

	// Test empty security group ID
	_, err = sgm.IsSecurityGroupInUse(ctx, "")
	if err == nil {
		t.Fatal("Expected error for empty security group ID, got nil")
	}

	var efsnsErr *EFSNSError
	if !errors.As(err, &efsnsErr) {
		t.Fatalf("Expected EFSNSError, got %T", err)
	}
	if efsnsErr.Type != ErrInvalidParameter {
		t.Errorf("Expected error type %s, got %s", ErrInvalidParameter, efsnsErr.Type)
	}
}

func TestSecurityGroupManager_ListSecurityGroups(t *testing.T) {
	mock := &mockEC2ClientSG{}

	mock.describeSecurityGroupsFunc = func(ctx context.Context, params *ec2.DescribeSecurityGroupsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error) {
		// Verify filters for cluster-wide listing
		if len(params.Filters) != 4 {
			t.Errorf("Expected 4 filters for cluster-wide listing, got %d", len(params.Filters))
		}
		return &ec2.DescribeSecurityGroupsOutput{
			SecurityGroups: []ec2types.SecurityGroup{
				{
					GroupId:     aws.String("sg-1"),
					GroupName:   aws.String("efs-ns-namespace1-test-cluster"),
					Description: aws.String("EFS security group for namespace namespace1 in cluster test-cluster"),
					VpcId:       aws.String("vpc-test123"),
					Tags: []ec2types.Tag{
						{Key: aws.String(SGTagKeyNamespace), Value: aws.String("namespace1")},
						{Key: aws.String(SGTagKeyCluster), Value: aws.String("test-cluster")},
					},
				},
				{
					GroupId:     aws.String("sg-2"),
					GroupName:   aws.String("efs-ns-namespace2-test-cluster"),
					Description: aws.String("EFS security group for namespace namespace2 in cluster test-cluster"),
					VpcId:       aws.String("vpc-test123"),
					Tags: []ec2types.Tag{
						{Key: aws.String(SGTagKeyNamespace), Value: aws.String("namespace2")},
						{Key: aws.String(SGTagKeyCluster), Value: aws.String("test-cluster")},
					},
				},
			},
		}, nil
	}

	sgm := NewSecurityGroupManager(mock, "test-cluster", "vpc-test123")
	ctx := context.Background()

	sgInfos, err := sgm.ListSecurityGroups(ctx)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(sgInfos) != 2 {
		t.Errorf("Expected 2 security groups, got %d", len(sgInfos))
	}

	if sgInfos[0].SecurityGroupID != "sg-1" {
		t.Errorf("Expected first security group ID sg-1, got %s", sgInfos[0].SecurityGroupID)
	}

	if sgInfos[0].Namespace != "namespace1" {
		t.Errorf("Expected first namespace namespace1, got %s", sgInfos[0].Namespace)
	}

	if sgInfos[1].SecurityGroupID != "sg-2" {
		t.Errorf("Expected second security group ID sg-2, got %s", sgInfos[1].SecurityGroupID)
	}

	if sgInfos[1].Namespace != "namespace2" {
		t.Errorf("Expected second namespace namespace2, got %s", sgInfos[1].Namespace)
	}
}

func TestSecurityGroupManager_Concurrency(t *testing.T) {
	mock := &mockEC2ClientSG{}
	callCount := 0

	mock.describeSecurityGroupsFunc = func(ctx context.Context, params *ec2.DescribeSecurityGroupsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error) {
		callCount++
		time.Sleep(10 * time.Millisecond) // Simulate some latency
		return &ec2.DescribeSecurityGroupsOutput{SecurityGroups: []ec2types.SecurityGroup{}}, nil
	}

	mock.createSecurityGroupFunc = func(ctx context.Context, params *ec2.CreateSecurityGroupInput, optFns ...func(*ec2.Options)) (*ec2.CreateSecurityGroupOutput, error) {
		return &ec2.CreateSecurityGroupOutput{
			GroupId: aws.String(fmt.Sprintf("sg-%d", callCount)),
		}, nil
	}

	sgm := NewSecurityGroupManager(mock, "test-cluster", "vpc-test123")
	ctx := context.Background()

	// Run multiple concurrent operations
	const numOperations = 5
	results := make(chan error, numOperations)

	for i := 0; i < numOperations; i++ {
		go func(index int) {
			namespace := fmt.Sprintf("namespace-%d", index)
			_, err := sgm.CreateSecurityGroup(ctx, namespace)
			results <- err
		}(i)
	}

	// Collect all results
	var errors []error
	for i := 0; i < numOperations; i++ {
		if err := <-results; err != nil {
			errors = append(errors, err)
		}
	}

	// All operations should succeed
	if len(errors) > 0 {
		t.Errorf("Expected no errors from concurrent operations, got %d errors", len(errors))
		for i, err := range errors {
			t.Errorf("Error %d: %v", i, err)
		}
	}

	if callCount != numOperations {
		t.Errorf("Expected %d operations to call the mock, got %d", numOperations, callCount)
	}
}
