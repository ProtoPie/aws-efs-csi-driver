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

package testenv

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"k8s.io/klog/v2"
)

const (
	// Default AWS region for testing
	DefaultTestRegion = "us-west-2"

	// Default availability zones for multi-AZ testing
	DefaultTestAZs = "us-west-2a,us-west-2b,us-west-2c"

	// Test resource tags
	TestTagKey = "efs-csi-test"
	TestTagCluster = "test-cluster"
	TestTagOwner = "efs-csi-driver"

	// Timeouts for resource creation
	ResourceCreationTimeout = 10 * time.Minute
	ResourceDeletionTimeout = 10 * time.Minute

	// Retry settings
	MaxRetries = 10
	RetryDelay = 10 * time.Second
)

// AWSTestEnvironment represents a complete AWS test environment
type AWSTestEnvironment struct {
	// AWS clients
	ec2Client *ec2.Client
	efsClient *efs.Client
	iamClient *iam.Client
	stsClient *sts.Client

	// Configuration
	Region        string
	AccountID     string
	AZs           []string
	TestID        string
	ClusterName   string

	// VPC resources
	VpcID             string
	SubnetIDs         []string
	SecurityGroupIDs  []string
	RouteTableIDs     []string
	InternetGatewayID string

	// IAM resources
	TestRoleArn      string
	TestPolicyArn    string

	// EFS resources for testing
	TestFileSystemIDs []string

	// Cleanup tracking
	resourcesToClean []cleanupResource
}

type cleanupResource struct {
	Type       string
	ID         string
	Region     string
	CleanupFn  func() error
}

// NewAWSTestEnvironment creates a new AWS test environment
func NewAWSTestEnvironment(testID string) (*AWSTestEnvironment, error) {
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = DefaultTestRegion
	}

	azs := os.Getenv("AWS_AVAILABILITY_ZONES")
	if azs == "" {
		azs = DefaultTestAZs
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithRetryMode(aws.RetryModeAdaptive),
		config.WithRetryMaxAttempts(MaxRetries),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	env := &AWSTestEnvironment{
		ec2Client:   ec2.NewFromConfig(cfg),
		efsClient:   efs.NewFromConfig(cfg),
		iamClient:   iam.NewFromConfig(cfg),
		stsClient:   sts.NewFromConfig(cfg),
		Region:      region,
		AZs:         strings.Split(azs, ","),
		TestID:      testID,
		ClusterName: fmt.Sprintf("test-cluster-%s", testID),
	}

	// Get AWS account ID
	identity, err := env.stsClient.GetCallerIdentity(context.TODO(), &sts.GetCallerIdentityInput{})
	if err != nil {
		return nil, fmt.Errorf("failed to get AWS account ID: %w", err)
	}
	env.AccountID = *identity.Account

	return env, nil
}

// Setup creates all required AWS resources for testing
func (env *AWSTestEnvironment) Setup(ctx context.Context) error {
	klog.Infof("Setting up AWS test environment for test ID: %s", env.TestID)

	// Create VPC and networking resources
	if err := env.setupVPC(ctx); err != nil {
		return fmt.Errorf("failed to setup VPC: %w", err)
	}

	// Create security groups
	if err := env.setupSecurityGroups(ctx); err != nil {
		return fmt.Errorf("failed to setup security groups: %w", err)
	}

	// Create IAM roles and policies
	if err := env.setupIAMResources(ctx); err != nil {
		return fmt.Errorf("failed to setup IAM resources: %w", err)
	}

	// Validate the environment
	if err := env.Validate(ctx); err != nil {
		return fmt.Errorf("environment validation failed: %w", err)
	}

	klog.Infof("AWS test environment setup complete for test ID: %s", env.TestID)
	return nil
}

// setupVPC creates VPC and networking resources
func (env *AWSTestEnvironment) setupVPC(ctx context.Context) error {
	// Create VPC
	vpcResp, err := env.ec2Client.CreateVpc(ctx, &ec2.CreateVpcInput{
		CidrBlock: aws.String("10.0.0.0/16"),
		TagSpecifications: []ec2types.TagSpecification{
			{
				ResourceType: ec2types.ResourceTypeVpc,
				Tags: env.getTags(fmt.Sprintf("test-vpc-%s", env.TestID)),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create VPC: %w", err)
	}
	env.VpcID = *vpcResp.Vpc.VpcId
	env.addCleanupResource("vpc", env.VpcID, func() error {
		_, err := env.ec2Client.DeleteVpc(ctx, &ec2.DeleteVpcInput{
			VpcId: aws.String(env.VpcID),
		})
		return err
	})

	// Enable DNS support
	_, err = env.ec2Client.ModifyVpcAttribute(ctx, &ec2.ModifyVpcAttributeInput{
		VpcId:              aws.String(env.VpcID),
		EnableDnsHostnames: &ec2types.AttributeBooleanValue{Value: aws.Bool(true)},
	})
	if err != nil {
		return fmt.Errorf("failed to enable DNS hostnames: %w", err)
	}

	// Create Internet Gateway
	igwResp, err := env.ec2Client.CreateInternetGateway(ctx, &ec2.CreateInternetGatewayInput{
		TagSpecifications: []ec2types.TagSpecification{
			{
				ResourceType: ec2types.ResourceTypeInternetGateway,
				Tags: env.getTags(fmt.Sprintf("test-igw-%s", env.TestID)),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create internet gateway: %w", err)
	}
	env.InternetGatewayID = *igwResp.InternetGateway.InternetGatewayId
	env.addCleanupResource("igw", env.InternetGatewayID, func() error {
		// Detach and delete
		_, _ = env.ec2Client.DetachInternetGateway(ctx, &ec2.DetachInternetGatewayInput{
			InternetGatewayId: aws.String(env.InternetGatewayID),
			VpcId:            aws.String(env.VpcID),
		})
		_, err := env.ec2Client.DeleteInternetGateway(ctx, &ec2.DeleteInternetGatewayInput{
			InternetGatewayId: aws.String(env.InternetGatewayID),
		})
		return err
	})

	// Attach Internet Gateway to VPC
	_, err = env.ec2Client.AttachInternetGateway(ctx, &ec2.AttachInternetGatewayInput{
		InternetGatewayId: aws.String(env.InternetGatewayID),
		VpcId:            aws.String(env.VpcID),
	})
	if err != nil {
		return fmt.Errorf("failed to attach internet gateway: %w", err)
	}

	// Create subnets in each AZ
	for i, az := range env.AZs {
		cidr := fmt.Sprintf("10.0.%d.0/24", i+1)
		subnetResp, err := env.ec2Client.CreateSubnet(ctx, &ec2.CreateSubnetInput{
			VpcId:            aws.String(env.VpcID),
			CidrBlock:        aws.String(cidr),
			AvailabilityZone: aws.String(az),
			TagSpecifications: []ec2types.TagSpecification{
				{
					ResourceType: ec2types.ResourceTypeSubnet,
					Tags: env.getTags(fmt.Sprintf("test-subnet-%s-%s", env.TestID, az)),
				},
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create subnet in %s: %w", az, err)
		}

		subnetID := *subnetResp.Subnet.SubnetId
		env.SubnetIDs = append(env.SubnetIDs, subnetID)
		env.addCleanupResource("subnet", subnetID, func() error {
			_, err := env.ec2Client.DeleteSubnet(ctx, &ec2.DeleteSubnetInput{
				SubnetId: aws.String(subnetID),
			})
			return err
		})

		// Enable auto-assign public IP
		_, err = env.ec2Client.ModifySubnetAttribute(ctx, &ec2.ModifySubnetAttributeInput{
			SubnetId:            aws.String(subnetID),
			MapPublicIpOnLaunch: &ec2types.AttributeBooleanValue{Value: aws.Bool(true)},
		})
		if err != nil {
			return fmt.Errorf("failed to enable auto-assign public IP: %w", err)
		}
	}

	// Update route table
	routeTablesResp, err := env.ec2Client.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{
		Filters: []ec2types.Filter{
			{
				Name:   aws.String("vpc-id"),
				Values: []string{env.VpcID},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to describe route tables: %w", err)
	}

	if len(routeTablesResp.RouteTables) > 0 {
		routeTableID := *routeTablesResp.RouteTables[0].RouteTableId
		_, err = env.ec2Client.CreateRoute(ctx, &ec2.CreateRouteInput{
			RouteTableId:         aws.String(routeTableID),
			DestinationCidrBlock: aws.String("0.0.0.0/0"),
			GatewayId:           aws.String(env.InternetGatewayID),
		})
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("failed to create route: %w", err)
		}
	}

	return nil
}

// setupSecurityGroups creates security groups for testing
func (env *AWSTestEnvironment) setupSecurityGroups(ctx context.Context) error {
	// Create EFS security group
	sgResp, err := env.ec2Client.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{
		GroupName:   aws.String(fmt.Sprintf("efs-test-sg-%s", env.TestID)),
		Description: aws.String("Security group for EFS CSI driver testing"),
		VpcId:       aws.String(env.VpcID),
		TagSpecifications: []ec2types.TagSpecification{
			{
				ResourceType: ec2types.ResourceTypeSecurityGroup,
				Tags: env.getTags(fmt.Sprintf("test-sg-%s", env.TestID)),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create security group: %w", err)
	}

	sgID := *sgResp.GroupId
	env.SecurityGroupIDs = append(env.SecurityGroupIDs, sgID)
	env.addCleanupResource("sg", sgID, func() error {
		_, err := env.ec2Client.DeleteSecurityGroup(ctx, &ec2.DeleteSecurityGroupInput{
			GroupId: aws.String(sgID),
		})
		return err
	})

	// Add ingress rules for NFS
	_, err = env.ec2Client.AuthorizeSecurityGroupIngress(ctx, &ec2.AuthorizeSecurityGroupIngressInput{
		GroupId: aws.String(sgID),
		IpPermissions: []ec2types.IpPermission{
			{
				IpProtocol: aws.String("tcp"),
				FromPort:   aws.Int32(2049), // NFS
				ToPort:     aws.Int32(2049),
				IpRanges: []ec2types.IpRange{
					{
						CidrIp:      aws.String("10.0.0.0/16"),
						Description: aws.String("Allow NFS from VPC"),
					},
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to authorize security group ingress: %w", err)
	}

	return nil
}

// setupIAMResources creates IAM roles and policies for testing
func (env *AWSTestEnvironment) setupIAMResources(ctx context.Context) error {
	// Create IAM policy for EFS operations
	policyDoc := map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{
				"Effect": "Allow",
				"Action": []string{
					"elasticfilesystem:CreateAccessPoint",
					"elasticfilesystem:DeleteAccessPoint",
					"elasticfilesystem:DescribeAccessPoints",
					"elasticfilesystem:DescribeFileSystems",
					"elasticfilesystem:DescribeMountTargets",
					"elasticfilesystem:CreateFileSystem",
					"elasticfilesystem:DeleteFileSystem",
					"elasticfilesystem:CreateMountTarget",
					"elasticfilesystem:DeleteMountTarget",
					"elasticfilesystem:TagResource",
					"elasticfilesystem:UntagResource",
				},
				"Resource": "*",
			},
			{
				"Effect": "Allow",
				"Action": []string{
					"ec2:DescribeSubnets",
					"ec2:DescribeNetworkInterfaces",
					"ec2:DescribeSecurityGroups",
				},
				"Resource": "*",
			},
		},
	}

	policyJSON, err := json.Marshal(policyDoc)
	if err != nil {
		return fmt.Errorf("failed to marshal policy document: %w", err)
	}

	policyResp, err := env.iamClient.CreatePolicy(ctx, &iam.CreatePolicyInput{
		PolicyName:     aws.String(fmt.Sprintf("efs-test-policy-%s", env.TestID)),
		PolicyDocument: aws.String(string(policyJSON)),
		Description:    aws.String("Policy for EFS CSI driver testing"),
		Tags: []iamtypes.Tag{
			{
				Key:   aws.String(TestTagKey),
				Value: aws.String(env.TestID),
			},
		},
	})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("failed to create IAM policy: %w", err)
	}
	if policyResp != nil {
		env.TestPolicyArn = *policyResp.Policy.Arn
		env.addCleanupResource("policy", env.TestPolicyArn, func() error {
			_, err := env.iamClient.DeletePolicy(ctx, &iam.DeletePolicyInput{
				PolicyArn: aws.String(env.TestPolicyArn),
			})
			return err
		})
	}

	// Create IAM role
	trustPolicy := map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{
				"Effect": "Allow",
				"Principal": map[string]interface{}{
					"Service": "eks.amazonaws.com",
				},
				"Action": "sts:AssumeRole",
			},
		},
	}

	trustPolicyJSON, err := json.Marshal(trustPolicy)
	if err != nil {
		return fmt.Errorf("failed to marshal trust policy: %w", err)
	}

	roleResp, err := env.iamClient.CreateRole(ctx, &iam.CreateRoleInput{
		RoleName:                 aws.String(fmt.Sprintf("efs-test-role-%s", env.TestID)),
		AssumeRolePolicyDocument: aws.String(string(trustPolicyJSON)),
		Description:              aws.String("Role for EFS CSI driver testing"),
		Tags: []iamtypes.Tag{
			{
				Key:   aws.String(TestTagKey),
				Value: aws.String(env.TestID),
			},
		},
	})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("failed to create IAM role: %w", err)
	}
	if roleResp != nil {
		env.TestRoleArn = *roleResp.Role.Arn
		env.addCleanupResource("role", *roleResp.Role.RoleName, func() error {
			// Detach policies first
			if env.TestPolicyArn != "" {
				_, _ = env.iamClient.DetachRolePolicy(ctx, &iam.DetachRolePolicyInput{
					RoleName:  roleResp.Role.RoleName,
					PolicyArn: aws.String(env.TestPolicyArn),
				})
			}
			_, err := env.iamClient.DeleteRole(ctx, &iam.DeleteRoleInput{
				RoleName: roleResp.Role.RoleName,
			})
			return err
		})

		// Attach policy to role
		if env.TestPolicyArn != "" {
			_, err = env.iamClient.AttachRolePolicy(ctx, &iam.AttachRolePolicyInput{
				RoleName:  roleResp.Role.RoleName,
				PolicyArn: aws.String(env.TestPolicyArn),
			})
			if err != nil {
				return fmt.Errorf("failed to attach policy to role: %w", err)
			}
		}
	}

	return nil
}

// CreateTestEFS creates a test EFS file system
func (env *AWSTestEnvironment) CreateTestEFS(ctx context.Context, name string) (string, error) {
	resp, err := env.efsClient.CreateFileSystem(ctx, &efs.CreateFileSystemInput{
		CreationToken: aws.String(fmt.Sprintf("test-efs-%s-%s", env.TestID, name)),
		Tags: []efstypes.Tag{
			{
				Key:   aws.String("Name"),
				Value: aws.String(name),
			},
			{
				Key:   aws.String(TestTagKey),
				Value: aws.String(env.TestID),
			},
			{
				Key:   aws.String("kubernetes.io/cluster/" + env.ClusterName),
				Value: aws.String("owned"),
			},
			{
				Key:   aws.String("kubernetes.io/provisioning-mode"),
				Value: aws.String("efs-ns"),
			},
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to create test EFS: %w", err)
	}

	fsID := *resp.FileSystemId
	env.TestFileSystemIDs = append(env.TestFileSystemIDs, fsID)
	env.addCleanupResource("efs", fsID, func() error {
		// Delete mount targets first
		mtResp, err := env.efsClient.DescribeMountTargets(ctx, &efs.DescribeMountTargetsInput{
			FileSystemId: aws.String(fsID),
		})
		if err == nil {
			for _, mt := range mtResp.MountTargets {
				_, _ = env.efsClient.DeleteMountTarget(ctx, &efs.DeleteMountTargetInput{
					MountTargetId: mt.MountTargetId,
				})
			}
		}

		// Wait for mount targets to be deleted
		time.Sleep(30 * time.Second)

		// Delete file system
		_, err = env.efsClient.DeleteFileSystem(ctx, &efs.DeleteFileSystemInput{
			FileSystemId: aws.String(fsID),
		})
		return err
	})

	// Create mount targets
	for _, subnetID := range env.SubnetIDs {
		_, err := env.efsClient.CreateMountTarget(ctx, &efs.CreateMountTargetInput{
			FileSystemId:   aws.String(fsID),
			SubnetId:       aws.String(subnetID),
			SecurityGroups: env.SecurityGroupIDs,
		})
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			return "", fmt.Errorf("failed to create mount target: %w", err)
		}
	}

	// Wait for file system to be available
	if err := env.waitForEFSAvailable(ctx, fsID); err != nil {
		return "", fmt.Errorf("failed waiting for EFS to be available: %w", err)
	}

	return fsID, nil
}

// waitForEFSAvailable waits for an EFS file system to become available
func (env *AWSTestEnvironment) waitForEFSAvailable(ctx context.Context, fsID string) error {
	deadline := time.Now().Add(ResourceCreationTimeout)
	for time.Now().Before(deadline) {
		resp, err := env.efsClient.DescribeFileSystems(ctx, &efs.DescribeFileSystemsInput{
			FileSystemId: aws.String(fsID),
		})
		if err != nil {
			return err
		}

		if len(resp.FileSystems) > 0 && resp.FileSystems[0].LifeCycleState == efstypes.LifeCycleStateAvailable {
			return nil
		}

		time.Sleep(RetryDelay)
	}

	return fmt.Errorf("timeout waiting for EFS %s to become available", fsID)
}

// Validate validates that the test environment is properly configured
func (env *AWSTestEnvironment) Validate(ctx context.Context) error {
	// Validate VPC
	if env.VpcID == "" {
		return fmt.Errorf("VPC ID is not set")
	}

	// Validate subnets
	if len(env.SubnetIDs) == 0 {
		return fmt.Errorf("no subnets configured")
	}

	// Validate security groups
	if len(env.SecurityGroupIDs) == 0 {
		return fmt.Errorf("no security groups configured")
	}

	// Validate IAM resources
	if env.TestRoleArn == "" {
		klog.Warningf("IAM role not configured - some tests may fail")
	}

	// Test EFS API connectivity
	_, err := env.efsClient.DescribeFileSystems(ctx, &efs.DescribeFileSystemsInput{
		MaxItems: aws.Int32(1),
	})
	if err != nil {
		return fmt.Errorf("failed to connect to EFS API: %w", err)
	}

	klog.Infof("Test environment validation successful")
	return nil
}

// Cleanup removes all test resources
func (env *AWSTestEnvironment) Cleanup(ctx context.Context) error {
	klog.Infof("Cleaning up AWS test environment for test ID: %s", env.TestID)

	var cleanupErrors []string

	// Clean up resources in reverse order
	for i := len(env.resourcesToClean) - 1; i >= 0; i-- {
		resource := env.resourcesToClean[i]
		klog.V(2).Infof("Cleaning up %s: %s", resource.Type, resource.ID)

		if err := resource.CleanupFn(); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Sprintf("%s %s: %v", resource.Type, resource.ID, err))
			klog.Errorf("Failed to cleanup %s %s: %v", resource.Type, resource.ID, err)
		}
	}

	if len(cleanupErrors) > 0 {
		return fmt.Errorf("cleanup errors: %s", strings.Join(cleanupErrors, ", "))
	}

	klog.Infof("AWS test environment cleanup complete for test ID: %s", env.TestID)
	return nil
}

// GetConfig returns configuration for connecting to the test environment
func (env *AWSTestEnvironment) GetConfig() map[string]string {
	return map[string]string{
		"region":        env.Region,
		"vpcId":         env.VpcID,
		"subnets":       strings.Join(env.SubnetIDs, ","),
		"securityGroups": strings.Join(env.SecurityGroupIDs, ","),
		"roleArn":       env.TestRoleArn,
		"accountId":     env.AccountID,
		"clusterName":   env.ClusterName,
		"testId":        env.TestID,
	}
}

// addCleanupResource tracks a resource for cleanup
func (env *AWSTestEnvironment) addCleanupResource(resourceType, id string, cleanupFn func() error) {
	env.resourcesToClean = append(env.resourcesToClean, cleanupResource{
		Type:      resourceType,
		ID:        id,
		Region:    env.Region,
		CleanupFn: cleanupFn,
	})
}

// getTags returns standard tags for test resources
func (env *AWSTestEnvironment) getTags(name string) []ec2types.Tag {
	return []ec2types.Tag{
		{
			Key:   aws.String("Name"),
			Value: aws.String(name),
		},
		{
			Key:   aws.String(TestTagKey),
			Value: aws.String(env.TestID),
		},
		{
			Key:   aws.String("Owner"),
			Value: aws.String(TestTagOwner),
		},
		{
			Key:   aws.String("kubernetes.io/cluster/" + env.ClusterName),
			Value: aws.String("owned"),
		},
	}
}

// GetEFSClient returns the EFS client for direct access
func (env *AWSTestEnvironment) GetEFSClient() *efs.Client {
	return env.efsClient
}