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
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"k8s.io/klog/v2"
)

const (
	// SecurityGroupNameFormat defines the naming convention for EFS security groups
	// Format: efs-ns-{namespace}-{cluster-id}
	SecurityGroupNameFormat = "efs-ns-%s-%s"

	// Security group description template
	SecurityGroupDescriptionFormat = "EFS security group for namespace %s in cluster %s"

	// NFS port for EFS access
	NFSPort = 2049

	// Security group tag keys
	SGTagKeyNamespace = "kubernetes.io/namespace"
	SGTagKeyCluster   = "kubernetes.io/cluster"
	SGTagKeyManagedBy = "kubernetes.io/managed-by"
	SGTagKeyCreatedBy = "kubernetes.io/created-by"
	SGTagKeyComponent = "kubernetes.io/component"
	SGTagKeyName      = "Name"

	// Security group tag values
	SGTagValueManagedBy = "efs-csi-driver"
	SGTagValueCreatedBy = "efs-ns-provisioner"
	SGTagValueComponent = "efs-ns-security-group"

	// Operation timeouts
	SecurityGroupOperationTimeout = 2 * time.Minute
)

// SecurityGroupInfo contains information about an EFS security group
type SecurityGroupInfo struct {
	// SecurityGroupID is the AWS security group identifier
	SecurityGroupID string

	// Namespace is the Kubernetes namespace this security group belongs to
	Namespace string

	// ClusterID identifies the Kubernetes cluster
	ClusterID string

	// VPCID is the VPC identifier where this security group exists
	VPCID string

	// Name is the security group name
	Name string

	// Description is the security group description
	Description string

	// Tags are the tags applied to this security group
	Tags map[string]string

	// CreatedAt is when the security group was created
	CreatedAt time.Time
}

// SecurityGroupManager interface handles EFS-related security group lifecycle
type SecurityGroupManager interface {
	// CreateSecurityGroup creates a new security group for the given namespace
	CreateSecurityGroup(ctx context.Context, namespace string) (*SecurityGroupInfo, error)

	// DeleteSecurityGroup deletes the security group for the given namespace
	DeleteSecurityGroup(ctx context.Context, securityGroupID string) error

	// GetSecurityGroup retrieves security group information for the given namespace
	GetSecurityGroup(ctx context.Context, namespace string) (*SecurityGroupInfo, error)

	// FindSecurityGroupByID finds security group by ID
	FindSecurityGroupByID(ctx context.Context, securityGroupID string) (*SecurityGroupInfo, error)

	// ListSecurityGroups lists all EFS security groups for the cluster
	ListSecurityGroups(ctx context.Context) ([]*SecurityGroupInfo, error)

	// AddNFSIngressRule adds NFS ingress rule to the security group
	AddNFSIngressRule(ctx context.Context, securityGroupID string, vpcCIDR string) error

	// IsSecurityGroupInUse checks if the security group is currently being used by mount targets
	IsSecurityGroupInUse(ctx context.Context, securityGroupID string) (bool, error)
}

// securityGroupManager implements the SecurityGroupManager interface
type securityGroupManager struct {
	ec2Client EC2Client
	clusterID string
	vpcID     string
}

// NewSecurityGroupManager creates a new SecurityGroupManager instance
func NewSecurityGroupManager(
	ec2Client EC2Client,
	clusterID string,
	vpcID string,
) SecurityGroupManager {
	return &securityGroupManager{
		ec2Client: ec2Client,
		clusterID: clusterID,
		vpcID:     vpcID,
	}
}

// CreateSecurityGroup creates a new security group for the given namespace
func (sgm *securityGroupManager) CreateSecurityGroup(
	ctx context.Context,
	namespace string,
) (*SecurityGroupInfo, error) {
	if namespace == "" {
		return nil, NewEFSNSError(ErrInvalidParameter, "CreateSecurityGroup", namespace,
			"namespace cannot be empty", nil)
	}

	klog.V(4).Infof("Creating security group for namespace: %s", namespace)

	// Check if security group already exists
	existingSG, err := sgm.GetSecurityGroup(ctx, namespace)
	if err != nil && !IsEFSNSError(err, ErrSecurityGroupNotFound) {
		return nil, NewEFSNSError(ErrSecurityGroupCreationFailed, "CreateSecurityGroup", namespace,
			"failed to check existing security group", err)
	}

	if existingSG != nil {
		klog.V(4).Infof("Security group already exists for namespace %s: %s", namespace, existingSG.SecurityGroupID)
		return existingSG, nil
	}

	// Create security group name and description
	sgName := fmt.Sprintf(SecurityGroupNameFormat, namespace, sgm.clusterID)
	sgDescription := fmt.Sprintf(SecurityGroupDescriptionFormat, namespace, sgm.clusterID)

	// Prepare tags
	tags := []ec2types.Tag{
		{Key: aws.String(SGTagKeyName), Value: aws.String(sgName)},
		{Key: aws.String(SGTagKeyNamespace), Value: aws.String(namespace)},
		{Key: aws.String(SGTagKeyCluster), Value: aws.String(sgm.clusterID)},
		{Key: aws.String(SGTagKeyManagedBy), Value: aws.String(SGTagValueManagedBy)},
		{Key: aws.String(SGTagKeyCreatedBy), Value: aws.String(SGTagValueCreatedBy)},
		{Key: aws.String(SGTagKeyComponent), Value: aws.String(SGTagValueComponent)},
	}

	// Create security group
	createInput := &ec2.CreateSecurityGroupInput{
		GroupName:   aws.String(sgName),
		Description: aws.String(sgDescription),
		VpcId:       aws.String(sgm.vpcID),
		TagSpecifications: []ec2types.TagSpecification{
			{
				ResourceType: ec2types.ResourceTypeSecurityGroup,
				Tags:         tags,
			},
		},
	}

	createCtx, cancel := context.WithTimeout(ctx, SecurityGroupOperationTimeout)
	defer cancel()

	createResp, err := sgm.ec2Client.CreateSecurityGroup(createCtx, createInput)
	if err != nil {
		return nil, NewEFSNSError(ErrSecurityGroupCreationFailed, "CreateSecurityGroup", namespace,
			fmt.Sprintf("failed to create security group: %s", err.Error()), err)
	}

	securityGroupID := *createResp.GroupId
	klog.V(2).Infof("Created security group %s for namespace %s", securityGroupID, namespace)

	// Get VPC CIDR for NFS ingress rule
	vpcCIDR, err := sgm.getVPCCIDR(ctx)
	if err != nil {
		// Attempt to clean up the security group on failure
		_ = sgm.DeleteSecurityGroup(ctx, securityGroupID)
		return nil, NewEFSNSError(ErrSecurityGroupCreationFailed, "CreateSecurityGroup", namespace,
			"failed to get VPC CIDR", err)
	}

	// Add NFS ingress rule
	err = sgm.AddNFSIngressRule(ctx, securityGroupID, vpcCIDR)
	if err != nil {
		// Attempt to clean up the security group on failure
		_ = sgm.DeleteSecurityGroup(ctx, securityGroupID)
		return nil, NewEFSNSError(ErrSecurityGroupCreationFailed, "CreateSecurityGroup", namespace,
			"failed to add NFS ingress rule", err)
	}

	// Build security group info
	tagMap := make(map[string]string)
	for _, tag := range tags {
		if tag.Key != nil && tag.Value != nil {
			tagMap[*tag.Key] = *tag.Value
		}
	}

	sgInfo := &SecurityGroupInfo{
		SecurityGroupID: securityGroupID,
		Namespace:       namespace,
		ClusterID:       sgm.clusterID,
		VPCID:           sgm.vpcID,
		Name:            sgName,
		Description:     sgDescription,
		Tags:            tagMap,
		CreatedAt:       time.Now(),
	}

	klog.V(2).Infof("Successfully created security group for namespace %s: %s", namespace, securityGroupID)
	return sgInfo, nil
}

// DeleteSecurityGroup deletes the security group for the given namespace
func (sgm *securityGroupManager) DeleteSecurityGroup(
	ctx context.Context,
	securityGroupID string,
) error {
	if securityGroupID == "" {
		return NewEFSNSError(ErrInvalidParameter, "DeleteSecurityGroup", "",
			"security group ID cannot be empty", nil)
	}

	klog.V(4).Infof("Deleting security group: %s", securityGroupID)

	// Check if security group is in use
	inUse, err := sgm.IsSecurityGroupInUse(ctx, securityGroupID)
	if err != nil {
		klog.V(4).Infof("Failed to check if security group is in use, proceeding with deletion: %v", err)
	} else if inUse {
		return NewEFSNSError(ErrSecurityGroupDeletionFailed, "DeleteSecurityGroup", "",
			fmt.Sprintf("security group %s is still in use by mount targets", securityGroupID), nil)
	}

	deleteInput := &ec2.DeleteSecurityGroupInput{
		GroupId: aws.String(securityGroupID),
	}

	deleteCtx, cancel := context.WithTimeout(ctx, SecurityGroupOperationTimeout)
	defer cancel()

	_, err = sgm.ec2Client.DeleteSecurityGroup(deleteCtx, deleteInput)
	if err != nil {
		// Check if it's already deleted
		if strings.Contains(err.Error(), "InvalidGroupId.NotFound") {
			klog.V(4).Infof("Security group %s not found, considering deletion successful", securityGroupID)
			return nil
		}
		return NewEFSNSError(ErrSecurityGroupDeletionFailed, "DeleteSecurityGroup", "",
			fmt.Sprintf("failed to delete security group %s: %s", securityGroupID, err.Error()), err)
	}

	klog.V(2).Infof("Successfully deleted security group: %s", securityGroupID)
	return nil
}

// GetSecurityGroup retrieves security group information for the given namespace
func (sgm *securityGroupManager) GetSecurityGroup(
	ctx context.Context,
	namespace string,
) (*SecurityGroupInfo, error) {
	if namespace == "" {
		return nil, NewEFSNSError(ErrInvalidParameter, "GetSecurityGroup", namespace,
			"namespace cannot be empty", nil)
	}

	// Build expected security group name
	expectedName := fmt.Sprintf(SecurityGroupNameFormat, namespace, sgm.clusterID)

	// Search for security group by tags
	filters := []ec2types.Filter{
		{
			Name:   aws.String("vpc-id"),
			Values: []string{sgm.vpcID},
		},
		{
			Name:   aws.String("tag:" + SGTagKeyNamespace),
			Values: []string{namespace},
		},
		{
			Name:   aws.String("tag:" + SGTagKeyCluster),
			Values: []string{sgm.clusterID},
		},
		{
			Name:   aws.String("tag:" + SGTagKeyManagedBy),
			Values: []string{SGTagValueManagedBy},
		},
		{
			Name:   aws.String("tag:" + SGTagKeyComponent),
			Values: []string{SGTagValueComponent},
		},
	}

	describeInput := &ec2.DescribeSecurityGroupsInput{
		Filters: filters,
	}

	describeCtx, cancel := context.WithTimeout(ctx, SecurityGroupOperationTimeout)
	defer cancel()

	resp, err := sgm.ec2Client.DescribeSecurityGroups(describeCtx, describeInput)
	if err != nil {
		return nil, NewEFSNSError(ErrAWSAPIFailed, "GetSecurityGroup", namespace,
			"failed to describe security groups", err)
	}

	// Filter results by name to ensure exact match
	var matchingSG *ec2types.SecurityGroup
	for _, sg := range resp.SecurityGroups {
		if sg.GroupName != nil && *sg.GroupName == expectedName {
			matchingSG = &sg
			break
		}
	}

	if matchingSG == nil {
		return nil, NewEFSNSError(ErrSecurityGroupNotFound, "GetSecurityGroup", namespace,
			fmt.Sprintf("security group not found for namespace %s", namespace), nil)
	}

	// Convert to SecurityGroupInfo
	sgInfo := sgm.convertToSecurityGroupInfo(matchingSG, namespace)
	return sgInfo, nil
}

// FindSecurityGroupByID finds security group by ID
func (sgm *securityGroupManager) FindSecurityGroupByID(
	ctx context.Context,
	securityGroupID string,
) (*SecurityGroupInfo, error) {
	if securityGroupID == "" {
		return nil, NewEFSNSError(ErrInvalidParameter, "FindSecurityGroupByID", "",
			"security group ID cannot be empty", nil)
	}

	describeInput := &ec2.DescribeSecurityGroupsInput{
		GroupIds: []string{securityGroupID},
	}

	describeCtx, cancel := context.WithTimeout(ctx, SecurityGroupOperationTimeout)
	defer cancel()

	resp, err := sgm.ec2Client.DescribeSecurityGroups(describeCtx, describeInput)
	if err != nil {
		if strings.Contains(err.Error(), "InvalidGroupId.NotFound") {
			return nil, NewEFSNSError(ErrSecurityGroupNotFound, "FindSecurityGroupByID", "",
				fmt.Sprintf("security group not found: %s", securityGroupID), nil)
		}
		return nil, NewEFSNSError(ErrAWSAPIFailed, "FindSecurityGroupByID", "",
			"failed to describe security group", err)
	}

	if len(resp.SecurityGroups) == 0 {
		return nil, NewEFSNSError(ErrSecurityGroupNotFound, "FindSecurityGroupByID", "",
			fmt.Sprintf("security group not found: %s", securityGroupID), nil)
	}

	sg := resp.SecurityGroups[0]

	// Extract namespace from tags
	namespace := ""
	for _, tag := range sg.Tags {
		if tag.Key != nil && *tag.Key == SGTagKeyNamespace && tag.Value != nil {
			namespace = *tag.Value
			break
		}
	}

	sgInfo := sgm.convertToSecurityGroupInfo(&sg, namespace)
	return sgInfo, nil
}

// ListSecurityGroups lists all EFS security groups for the cluster
func (sgm *securityGroupManager) ListSecurityGroups(
	ctx context.Context,
) ([]*SecurityGroupInfo, error) {
	filters := []ec2types.Filter{
		{
			Name:   aws.String("vpc-id"),
			Values: []string{sgm.vpcID},
		},
		{
			Name:   aws.String("tag:" + SGTagKeyCluster),
			Values: []string{sgm.clusterID},
		},
		{
			Name:   aws.String("tag:" + SGTagKeyManagedBy),
			Values: []string{SGTagValueManagedBy},
		},
		{
			Name:   aws.String("tag:" + SGTagKeyComponent),
			Values: []string{SGTagValueComponent},
		},
	}

	describeInput := &ec2.DescribeSecurityGroupsInput{
		Filters: filters,
	}

	describeCtx, cancel := context.WithTimeout(ctx, SecurityGroupOperationTimeout)
	defer cancel()

	resp, err := sgm.ec2Client.DescribeSecurityGroups(describeCtx, describeInput)
	if err != nil {
		return nil, NewEFSNSError(ErrAWSAPIFailed, "ListSecurityGroups", "",
			"failed to describe security groups", err)
	}

	var sgInfos []*SecurityGroupInfo
	for _, sg := range resp.SecurityGroups {
		// Extract namespace from tags
		namespace := ""
		for _, tag := range sg.Tags {
			if tag.Key != nil && *tag.Key == SGTagKeyNamespace && tag.Value != nil {
				namespace = *tag.Value
				break
			}
		}

		sgInfo := sgm.convertToSecurityGroupInfo(&sg, namespace)
		sgInfos = append(sgInfos, sgInfo)
	}

	return sgInfos, nil
}

// AddNFSIngressRule adds NFS ingress rule to the security group
func (sgm *securityGroupManager) AddNFSIngressRule(
	ctx context.Context,
	securityGroupID string,
	vpcCIDR string,
) error {
	if securityGroupID == "" {
		return NewEFSNSError(ErrInvalidParameter, "AddNFSIngressRule", "",
			"security group ID cannot be empty", nil)
	}
	if vpcCIDR == "" {
		return NewEFSNSError(ErrInvalidParameter, "AddNFSIngressRule", "",
			"VPC CIDR cannot be empty", nil)
	}

	klog.V(4).Infof("Adding NFS ingress rule to security group %s for VPC CIDR %s", securityGroupID, vpcCIDR)

	// Allow NFS traffic (port 2049) from the VPC CIDR
	authorizeInput := &ec2.AuthorizeSecurityGroupIngressInput{
		GroupId: aws.String(securityGroupID),
		IpPermissions: []ec2types.IpPermission{
			{
				IpProtocol: aws.String("tcp"),
				FromPort:   aws.Int32(NFSPort),
				ToPort:     aws.Int32(NFSPort),
				IpRanges: []ec2types.IpRange{
					{
						CidrIp:      aws.String(vpcCIDR),
						Description: aws.String("EFS NFS access from VPC"),
					},
				},
			},
		},
	}

	authorizeCtx, cancel := context.WithTimeout(ctx, SecurityGroupOperationTimeout)
	defer cancel()

	_, err := sgm.ec2Client.AuthorizeSecurityGroupIngress(authorizeCtx, authorizeInput)
	if err != nil {
		// Check if rule already exists
		if strings.Contains(err.Error(), "InvalidPermission.Duplicate") {
			klog.V(4).Infof("NFS ingress rule already exists for security group %s", securityGroupID)
			return nil
		}
		return NewEFSNSError(ErrSecurityGroupCreationFailed, "AddNFSIngressRule", "",
			fmt.Sprintf("failed to add NFS ingress rule to security group %s: %s", securityGroupID, err.Error()), err)
	}

	klog.V(4).Infof("Successfully added NFS ingress rule to security group %s", securityGroupID)
	return nil
}

// IsSecurityGroupInUse checks if the security group is currently being used by mount targets
func (sgm *securityGroupManager) IsSecurityGroupInUse(
	ctx context.Context,
	securityGroupID string,
) (bool, error) {
	if securityGroupID == "" {
		return false, NewEFSNSError(ErrInvalidParameter, "IsSecurityGroupInUse", "",
			"security group ID cannot be empty", nil)
	}

	// This would require integration with EFS mount target checking
	// For now, we'll return false as this is a basic implementation
	// In a complete implementation, this would check if any mount targets are using this security group
	klog.V(4).Infof("Checking if security group %s is in use (basic implementation)", securityGroupID)

	// TODO: Implement actual mount target checking when mount target management is available
	return false, nil
}

// getVPCCIDR retrieves the CIDR block for the VPC
func (sgm *securityGroupManager) getVPCCIDR(ctx context.Context) (string, error) {
	describeInput := &ec2.DescribeVpcsInput{
		VpcIds: []string{sgm.vpcID},
	}

	describeCtx, cancel := context.WithTimeout(ctx, SecurityGroupOperationTimeout)
	defer cancel()

	resp, err := sgm.ec2Client.DescribeVpcs(describeCtx, describeInput)
	if err != nil {
		return "", fmt.Errorf("failed to describe VPC %s: %w", sgm.vpcID, err)
	}

	if len(resp.Vpcs) == 0 {
		return "", fmt.Errorf("VPC %s not found", sgm.vpcID)
	}

	vpc := resp.Vpcs[0]
	if vpc.CidrBlock == nil {
		return "", fmt.Errorf("VPC %s has no CIDR block", sgm.vpcID)
	}

	return *vpc.CidrBlock, nil
}

// convertToSecurityGroupInfo converts EC2 SecurityGroup to SecurityGroupInfo
func (sgm *securityGroupManager) convertToSecurityGroupInfo(
	sg *ec2types.SecurityGroup,
	namespace string,
) *SecurityGroupInfo {
	tags := make(map[string]string)
	for _, tag := range sg.Tags {
		if tag.Key != nil && tag.Value != nil {
			tags[*tag.Key] = *tag.Value
		}
	}

	sgInfo := &SecurityGroupInfo{
		SecurityGroupID: aws.ToString(sg.GroupId),
		Namespace:       namespace,
		ClusterID:       sgm.clusterID,
		VPCID:           aws.ToString(sg.VpcId),
		Name:            aws.ToString(sg.GroupName),
		Description:     aws.ToString(sg.Description),
		Tags:            tags,
	}

	// Extract creation time if available (not directly available in EC2 SecurityGroup)
	// In AWS, we can't get exact creation time, so we use current time
	sgInfo.CreatedAt = time.Now()

	return sgInfo
}
