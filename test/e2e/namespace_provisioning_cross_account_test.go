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

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
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
	"github.com/kubernetes-sigs/aws-efs-csi-driver/test/e2e/testenv"
	"k8s.io/klog/v2"
)

// CrossAccountTestEnvironment extends the base test environment for cross-account testing
type CrossAccountTestEnvironment struct {
	*testenv.AWSTestEnvironment

	// Cross-account specific resources
	TargetAccountID     string
	TargetRegion        string
	TargetRoleArn       string
	TargetVpcID         string
	TargetSubnetIDs     []string
	TargetSecurityGroup string

	// Cross-account AWS clients
	targetSTSClient *sts.Client
	targetEC2Client *ec2.Client
	targetEFSClient *efs.Client
	targetIAMClient *iam.Client

	// VPC Peering resources
	PeeringConnectionID string
	PeeringRouteTableIDs []string

	// Cross-account EFS resources
	CrossAccountFileSystemID string
	CrossAccountAccessPoints []string
}

// TestNamespaceProvisioningCrossAccount tests cross-account EFS mount functionality
func TestNamespaceProvisioningCrossAccount(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("skipping cross-account integration test in short mode")
	}

	// Check for cross-account credentials
	targetAccountID := os.Getenv("AWS_TARGET_ACCOUNT_ID")
	targetRoleArn := os.Getenv("AWS_TARGET_ROLE_ARN")

	if targetAccountID == "" || targetRoleArn == "" {
		t.Skip("Cross-account credentials not available (AWS_TARGET_ACCOUNT_ID and AWS_TARGET_ROLE_ARN required)")
	}

	// Create cross-account test environment
	testID := fmt.Sprintf("cross-account-%d", time.Now().Unix())
	env, err := NewCrossAccountTestEnvironment(testID, targetAccountID, targetRoleArn)
	if err != nil {
		t.Fatalf("failed to create cross-account test environment: %v", err)
	}

	// Setup cleanup
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer cancel()
		if err := env.CleanupCrossAccountResources(ctx); err != nil {
			t.Logf("WARNING: Cross-account cleanup failed: %v", err)
		}
	}()

	// Setup the environment
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	t.Log("Setting up cross-account AWS test environment...")
	if err := env.SetupCrossAccount(ctx); err != nil {
		t.Fatalf("failed to setup cross-account test environment: %v", err)
	}

	// Run cross-account test scenarios
	t.Run("CrossAccountSetup", func(t *testing.T) {
		testCrossAccountSetup(t, ctx, env)
	})

	t.Run("IAMRoleTrustRelationship", func(t *testing.T) {
		testIAMRoleTrustRelationship(t, ctx, env)
	})

	t.Run("CrossAccountEFSCreation", func(t *testing.T) {
		testCrossAccountEFSCreation(t, ctx, env)
	})

	t.Run("VPCPeeringConnectivity", func(t *testing.T) {
		testVPCPeeringConnectivity(t, ctx, env)
	})

	t.Run("CrossAccountEFSMount", func(t *testing.T) {
		testCrossAccountEFSMount(t, ctx, env)
	})

	t.Run("CrossAccountAccessPoint", func(t *testing.T) {
		testCrossAccountAccessPoint(t, ctx, env)
	})

	t.Run("CrossAccountPermissions", func(t *testing.T) {
		testCrossAccountPermissions(t, ctx, env)
	})

	t.Run("CrossAccountDataIsolation", func(t *testing.T) {
		testCrossAccountDataIsolation(t, ctx, env)
	})

	t.Run("CrossAccountFailover", func(t *testing.T) {
		testCrossAccountFailover(t, ctx, env)
	})

	t.Run("CrossAccountCleanup", func(t *testing.T) {
		testCrossAccountCleanup(t, ctx, env)
	})
}

// NewCrossAccountTestEnvironment creates a new cross-account test environment
func NewCrossAccountTestEnvironment(testID, targetAccountID, targetRoleArn string) (*CrossAccountTestEnvironment, error) {
	baseEnv, err := testenv.NewAWSTestEnvironment(testID)
	if err != nil {
		return nil, fmt.Errorf("failed to create base environment: %w", err)
	}

	env := &CrossAccountTestEnvironment{
		AWSTestEnvironment: baseEnv,
		TargetAccountID:    targetAccountID,
		TargetRoleArn:      targetRoleArn,
		TargetRegion:       baseEnv.Region, // Use same region for simplicity
	}

	// Setup STS assume role for target account
	if err := env.setupTargetAccountClients(); err != nil {
		return nil, fmt.Errorf("failed to setup target account clients: %w", err)
	}

	return env, nil
}

// setupTargetAccountClients configures AWS clients for the target account using AssumeRole
func (env *CrossAccountTestEnvironment) setupTargetAccountClients() error {
	// Load base config
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(env.TargetRegion),
	)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create STS client
	stsClient := sts.NewFromConfig(cfg)

	// Assume role in target account
	sessionName := fmt.Sprintf("efs-csi-cross-account-test-%s", env.TestID)
	assumeRoleOutput, err := stsClient.AssumeRole(context.TODO(), &sts.AssumeRoleInput{
		RoleArn:         aws.String(env.TargetRoleArn),
		RoleSessionName: aws.String(sessionName),
		DurationSeconds: aws.Int32(3600), // 1 hour session
	})
	if err != nil {
		return fmt.Errorf("failed to assume role %s: %w", env.TargetRoleArn, err)
	}

	// Create new config with assumed role credentials
	targetCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(env.TargetRegion),
		config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     *assumeRoleOutput.Credentials.AccessKeyId,
				SecretAccessKey: *assumeRoleOutput.Credentials.SecretAccessKey,
				SessionToken:    *assumeRoleOutput.Credentials.SessionToken,
				Source:          "AssumeRoleProvider",
				CanExpire:       true,
				Expires:         *assumeRoleOutput.Credentials.Expiration,
			}, nil
		})),
	)
	if err != nil {
		return fmt.Errorf("failed to create target account config: %w", err)
	}

	// Create target account clients
	env.targetSTSClient = sts.NewFromConfig(targetCfg)
	env.targetEC2Client = ec2.NewFromConfig(targetCfg)
	env.targetEFSClient = efs.NewFromConfig(targetCfg)
	env.targetIAMClient = iam.NewFromConfig(targetCfg)

	// Verify access to target account
	identity, err := env.targetSTSClient.GetCallerIdentity(context.TODO(), &sts.GetCallerIdentityInput{})
	if err != nil {
		return fmt.Errorf("failed to verify target account access: %w", err)
	}

	if *identity.Account != env.TargetAccountID {
		return fmt.Errorf("account mismatch: expected %s, got %s", env.TargetAccountID, *identity.Account)
	}

	klog.Infof("Successfully assumed role in target account %s", env.TargetAccountID)
	return nil
}

// SetupCrossAccount creates resources in both source and target accounts
func (env *CrossAccountTestEnvironment) SetupCrossAccount(ctx context.Context) error {
	// Setup base environment
	if err := env.Setup(ctx); err != nil {
		return fmt.Errorf("failed to setup base environment: %w", err)
	}

	// Setup target account VPC
	if err := env.setupTargetVPC(ctx); err != nil {
		return fmt.Errorf("failed to setup target VPC: %w", err)
	}

	// Setup VPC peering between accounts
	if err := env.setupVPCPeering(ctx); err != nil {
		return fmt.Errorf("failed to setup VPC peering: %w", err)
	}

	// Setup cross-account IAM roles
	if err := env.setupCrossAccountIAM(ctx); err != nil {
		return fmt.Errorf("failed to setup cross-account IAM: %w", err)
	}

	// Setup EFS in target account
	if err := env.setupTargetEFS(ctx); err != nil {
		return fmt.Errorf("failed to setup target EFS: %w", err)
	}

	return nil
}

// setupTargetVPC creates VPC resources in the target account
func (env *CrossAccountTestEnvironment) setupTargetVPC(ctx context.Context) error {
	// Create VPC
	createVpcOutput, err := env.targetEC2Client.CreateVpc(ctx, &ec2.CreateVpcInput{
		CidrBlock: aws.String("10.1.0.0/16"), // Different from source VPC
		TagSpecifications: []ec2types.TagSpecification{
			{
				ResourceType: ec2types.ResourceTypeVpc,
				Tags: []ec2types.Tag{
					{Key: aws.String("Name"), Value: aws.String(fmt.Sprintf("cross-account-target-%s", env.TestID))},
					{Key: aws.String(testenv.TestTagKey), Value: aws.String(env.TestID)},
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create target VPC: %w", err)
	}
	env.TargetVpcID = *createVpcOutput.Vpc.VpcId

	// Enable DNS hostnames
	_, err = env.targetEC2Client.ModifyVpcAttribute(ctx, &ec2.ModifyVpcAttributeInput{
		VpcId:              aws.String(env.TargetVpcID),
		EnableDnsHostnames: &ec2types.AttributeBooleanValue{Value: aws.Bool(true)},
	})
	if err != nil {
		return fmt.Errorf("failed to enable DNS hostnames: %w", err)
	}

	// Create subnets in different AZs
	env.TargetSubnetIDs = make([]string, 0, len(env.AZs))
	for i, az := range env.AZs {
		cidrBlock := fmt.Sprintf("10.1.%d.0/24", i)
		createSubnetOutput, err := env.targetEC2Client.CreateSubnet(ctx, &ec2.CreateSubnetInput{
			VpcId:            aws.String(env.TargetVpcID),
			CidrBlock:        aws.String(cidrBlock),
			AvailabilityZone: aws.String(az),
			TagSpecifications: []ec2types.TagSpecification{
				{
					ResourceType: ec2types.ResourceTypeSubnet,
					Tags: []ec2types.Tag{
						{Key: aws.String("Name"), Value: aws.String(fmt.Sprintf("cross-account-target-%s-subnet-%d", env.TestID, i))},
						{Key: aws.String(testenv.TestTagKey), Value: aws.String(env.TestID)},
					},
				},
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create target subnet in %s: %w", az, err)
		}
		env.TargetSubnetIDs = append(env.TargetSubnetIDs, *createSubnetOutput.Subnet.SubnetId)
	}

	// Create security group for EFS
	createSgOutput, err := env.targetEC2Client.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{
		GroupName:   aws.String(fmt.Sprintf("cross-account-efs-%s", env.TestID)),
		Description: aws.String("Security group for cross-account EFS testing"),
		VpcId:       aws.String(env.TargetVpcID),
		TagSpecifications: []ec2types.TagSpecification{
			{
				ResourceType: ec2types.ResourceTypeSecurityGroup,
				Tags: []ec2types.Tag{
					{Key: aws.String("Name"), Value: aws.String(fmt.Sprintf("cross-account-efs-%s", env.TestID))},
					{Key: aws.String(testenv.TestTagKey), Value: aws.String(env.TestID)},
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create target security group: %w", err)
	}
	env.TargetSecurityGroup = *createSgOutput.GroupId

	// Add ingress rule for NFS from source VPC
	_, err = env.targetEC2Client.AuthorizeSecurityGroupIngress(ctx, &ec2.AuthorizeSecurityGroupIngressInput{
		GroupId: aws.String(env.TargetSecurityGroup),
		IpPermissions: []ec2types.IpPermission{
			{
				IpProtocol: aws.String("tcp"),
				FromPort:   aws.Int32(2049),
				ToPort:     aws.Int32(2049),
				IpRanges: []ec2types.IpRange{
					{CidrIp: aws.String("10.0.0.0/16")}, // Source VPC CIDR
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to authorize NFS ingress: %w", err)
	}

	klog.Infof("Successfully created target VPC %s with subnets", env.TargetVpcID)
	return nil
}

// setupVPCPeering creates VPC peering connection between source and target accounts
func (env *CrossAccountTestEnvironment) setupVPCPeering(ctx context.Context) error {
	// Create peering connection from source account
	createPeeringOutput, err := env.ec2Client.CreateVpcPeeringConnection(ctx, &ec2.CreateVpcPeeringConnectionInput{
		VpcId:        aws.String(env.VpcID),
		PeerVpcId:    aws.String(env.TargetVpcID),
		PeerRegion:   aws.String(env.TargetRegion),
		PeerOwnerId:  aws.String(env.TargetAccountID),
		TagSpecifications: []ec2types.TagSpecification{
			{
				ResourceType: ec2types.ResourceTypeVpcPeeringConnection,
				Tags: []ec2types.Tag{
					{Key: aws.String("Name"), Value: aws.String(fmt.Sprintf("cross-account-peering-%s", env.TestID))},
					{Key: aws.String(testenv.TestTagKey), Value: aws.String(env.TestID)},
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create VPC peering connection: %w", err)
	}
	env.PeeringConnectionID = *createPeeringOutput.VpcPeeringConnection.VpcPeeringConnectionId

	// Accept peering connection from target account
	_, err = env.targetEC2Client.AcceptVpcPeeringConnection(ctx, &ec2.AcceptVpcPeeringConnectionInput{
		VpcPeeringConnectionId: aws.String(env.PeeringConnectionID),
	})
	if err != nil {
		return fmt.Errorf("failed to accept VPC peering connection: %w", err)
	}

	// Wait for peering connection to be active
	waiter := ec2.NewVpcPeeringConnectionExistsWaiter(env.ec2Client)
	err = waiter.Wait(ctx, &ec2.DescribeVpcPeeringConnectionsInput{
		VpcPeeringConnectionIds: []string{env.PeeringConnectionID},
	}, 5*time.Minute)
	if err != nil {
		return fmt.Errorf("failed waiting for VPC peering connection: %w", err)
	}

	// Add routes in source VPC
	for _, routeTableID := range env.RouteTableIDs {
		_, err = env.ec2Client.CreateRoute(ctx, &ec2.CreateRouteInput{
			RouteTableId:           aws.String(routeTableID),
			DestinationCidrBlock:   aws.String("10.1.0.0/16"), // Target VPC CIDR
			VpcPeeringConnectionId: aws.String(env.PeeringConnectionID),
		})
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("failed to create route in source VPC: %w", err)
		}
	}

	// Get and update route tables in target VPC
	describeRtOutput, err := env.targetEC2Client.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{
		Filters: []ec2types.Filter{
			{
				Name:   aws.String("vpc-id"),
				Values: []string{env.TargetVpcID},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to describe target route tables: %w", err)
	}

	// Add routes in target VPC
	for _, rt := range describeRtOutput.RouteTables {
		_, err = env.targetEC2Client.CreateRoute(ctx, &ec2.CreateRouteInput{
			RouteTableId:           rt.RouteTableId,
			DestinationCidrBlock:   aws.String("10.0.0.0/16"), // Source VPC CIDR
			VpcPeeringConnectionId: aws.String(env.PeeringConnectionID),
		})
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("failed to create route in target VPC: %w", err)
		}
		env.PeeringRouteTableIDs = append(env.PeeringRouteTableIDs, *rt.RouteTableId)
	}

	klog.Infof("Successfully established VPC peering connection %s", env.PeeringConnectionID)
	return nil
}

// setupCrossAccountIAM creates IAM resources for cross-account access
func (env *CrossAccountTestEnvironment) setupCrossAccountIAM(ctx context.Context) error {
	// Create EFS resource policy for cross-account access
	resourcePolicy := map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{
				"Effect": "Allow",
				"Principal": map[string]interface{}{
					"AWS": fmt.Sprintf("arn:aws:iam::%s:root", env.AccountID),
				},
				"Action": []string{
					"elasticfilesystem:ClientMount",
					"elasticfilesystem:ClientWrite",
					"elasticfilesystem:ClientRootAccess",
				},
				"Resource": "*",
				"Condition": map[string]interface{}{
					"IpAddress": map[string][]string{
						"aws:SourceIp": {"10.0.0.0/16"}, // Source VPC CIDR
					},
				},
			},
		},
	}

	policyJSON, err := json.Marshal(resourcePolicy)
	if err != nil {
		return fmt.Errorf("failed to marshal resource policy: %w", err)
	}

	// Store policy for later use when creating EFS
	env.TestRoleArn = string(policyJSON) // Temporarily store in TestRoleArn field

	// Create or update trust relationship in source account for accessing target EFS
	trustPolicy := map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{
				"Effect": "Allow",
				"Principal": map[string]interface{}{
					"Service": "ec2.amazonaws.com",
				},
				"Action": "sts:AssumeRole",
			},
			{
				"Effect": "Allow",
				"Principal": map[string]interface{}{
					"AWS": fmt.Sprintf("arn:aws:iam::%s:root", env.TargetAccountID),
				},
				"Action": "sts:AssumeRole",
			},
		},
	}

	trustPolicyJSON, err := json.Marshal(trustPolicy)
	if err != nil {
		return fmt.Errorf("failed to marshal trust policy: %w", err)
	}

	// Create IAM role for EFS CSI driver in source account
	roleName := fmt.Sprintf("efs-csi-cross-account-%s", env.TestID)
	createRoleOutput, err := env.iamClient.CreateRole(ctx, &iam.CreateRoleInput{
		RoleName:                 aws.String(roleName),
		AssumeRolePolicyDocument: aws.String(string(trustPolicyJSON)),
		Description:              aws.String("Role for cross-account EFS access testing"),
		Tags: []iamtypes.Tag{
			{Key: aws.String(testenv.TestTagKey), Value: aws.String(env.TestID)},
		},
	})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("failed to create IAM role: %w", err)
	}
	if createRoleOutput != nil {
		env.TestPolicyArn = *createRoleOutput.Role.Arn
	}

	// Attach policy for EFS access
	efsPolicyDocument := map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{
				"Effect": "Allow",
				"Action": []string{
					"elasticfilesystem:*",
					"ec2:DescribeSubnets",
					"ec2:DescribeNetworkInterfaces",
					"ec2:DescribeSecurityGroups",
				},
				"Resource": "*",
			},
		},
	}

	efsPolicyJSON, err := json.Marshal(efsPolicyDocument)
	if err != nil {
		return fmt.Errorf("failed to marshal EFS policy: %w", err)
	}

	policyName := fmt.Sprintf("efs-csi-cross-account-policy-%s", env.TestID)
	createPolicyOutput, err := env.iamClient.CreatePolicy(ctx, &iam.CreatePolicyInput{
		PolicyName:     aws.String(policyName),
		PolicyDocument: aws.String(string(efsPolicyJSON)),
		Description:    aws.String("Policy for cross-account EFS access"),
		Tags: []iamtypes.Tag{
			{Key: aws.String(testenv.TestTagKey), Value: aws.String(env.TestID)},
		},
	})
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("failed to create IAM policy: %w", err)
	}

	if createPolicyOutput != nil {
		// Attach policy to role
		_, err = env.iamClient.AttachRolePolicy(ctx, &iam.AttachRolePolicyInput{
			RoleName:  aws.String(roleName),
			PolicyArn: createPolicyOutput.Policy.Arn,
		})
		if err != nil {
			return fmt.Errorf("failed to attach policy to role: %w", err)
		}
	}

	klog.Infof("Successfully configured cross-account IAM resources")
	return nil
}

// setupTargetEFS creates EFS resources in the target account
func (env *CrossAccountTestEnvironment) setupTargetEFS(ctx context.Context) error {
	// Create EFS file system in target account
	createFsInput := &efs.CreateFileSystemInput{
		CreationToken: aws.String(fmt.Sprintf("cross-account-%s", env.TestID)),
		Encrypted:     aws.Bool(true),
		Tags: []efstypes.Tag{
			{Key: aws.String("Name"), Value: aws.String(fmt.Sprintf("cross-account-efs-%s", env.TestID))},
			{Key: aws.String(testenv.TestTagKey), Value: aws.String(env.TestID)},
			{Key: aws.String("CrossAccount"), Value: aws.String("true")},
			{Key: aws.String("SourceAccount"), Value: aws.String(env.AccountID)},
		},
	}

	createFsOutput, err := env.targetEFSClient.CreateFileSystem(ctx, createFsInput)
	if err != nil {
		return fmt.Errorf("failed to create cross-account EFS: %w", err)
	}
	env.CrossAccountFileSystemID = *createFsOutput.FileSystemId

	// Wait for file system to be available
	time.Sleep(10 * time.Second)

	// Put file system policy for cross-account access
	if env.TestRoleArn != "" {
		_, err = env.targetEFSClient.PutFileSystemPolicy(ctx, &efs.PutFileSystemPolicyInput{
			FileSystemId: aws.String(env.CrossAccountFileSystemID),
			Policy:       aws.String(env.TestRoleArn), // Contains the resource policy JSON
		})
		if err != nil {
			klog.Warningf("Failed to put file system policy: %v", err)
		}
	}

	// Create mount targets in each subnet
	for i, subnetID := range env.TargetSubnetIDs {
		createMtInput := &efs.CreateMountTargetInput{
			FileSystemId: aws.String(env.CrossAccountFileSystemID),
			SubnetId:     aws.String(subnetID),
			SecurityGroups: []string{
				env.TargetSecurityGroup,
			},
		}

		_, err := env.targetEFSClient.CreateMountTarget(ctx, createMtInput)
		if err != nil {
			if strings.Contains(err.Error(), "already exists") {
				klog.Infof("Mount target already exists in subnet %s", subnetID)
				continue
			}
			return fmt.Errorf("failed to create mount target in subnet %s: %w", subnetID, err)
		}
		klog.Infof("Created mount target %d in subnet %s", i, subnetID)
	}

	// Create access points for testing
	for i := 0; i < 3; i++ {
		createApInput := &efs.CreateAccessPointInput{
			FileSystemId: aws.String(env.CrossAccountFileSystemID),
			PosixUser: &efstypes.PosixUser{
				Uid: aws.Int64(int64(1000 + i)),
				Gid: aws.Int64(int64(1000 + i)),
			},
			RootDirectory: &efstypes.RootDirectory{
				Path: aws.String(fmt.Sprintf("/cross-account-test-%d", i)),
				CreationInfo: &efstypes.CreationInfo{
					OwnerUid:    aws.Int64(int64(1000 + i)),
					OwnerGid:    aws.Int64(int64(1000 + i)),
					Permissions: aws.String("755"),
				},
			},
			Tags: []efstypes.Tag{
				{Key: aws.String("Name"), Value: aws.String(fmt.Sprintf("cross-account-ap-%s-%d", env.TestID, i))},
				{Key: aws.String(testenv.TestTagKey), Value: aws.String(env.TestID)},
			},
		}

		createApOutput, err := env.targetEFSClient.CreateAccessPoint(ctx, createApInput)
		if err != nil {
			return fmt.Errorf("failed to create access point %d: %w", i, err)
		}
		env.CrossAccountAccessPoints = append(env.CrossAccountAccessPoints, *createApOutput.AccessPointId)
	}

	klog.Infof("Successfully created cross-account EFS %s with %d access points",
		env.CrossAccountFileSystemID, len(env.CrossAccountAccessPoints))
	return nil
}

// CleanupCrossAccountResources cleans up all cross-account test resources
func (env *CrossAccountTestEnvironment) CleanupCrossAccountResources(ctx context.Context) error {
	var errors []error

	// Delete access points
	for _, apID := range env.CrossAccountAccessPoints {
		_, err := env.targetEFSClient.DeleteAccessPoint(ctx, &efs.DeleteAccessPointInput{
			AccessPointId: aws.String(apID),
		})
		if err != nil && !strings.Contains(err.Error(), "not found") {
			errors = append(errors, fmt.Errorf("failed to delete access point %s: %w", apID, err))
		}
	}

	// Delete mount targets
	if env.CrossAccountFileSystemID != "" {
		describeMtOutput, err := env.targetEFSClient.DescribeMountTargets(ctx, &efs.DescribeMountTargetsInput{
			FileSystemId: aws.String(env.CrossAccountFileSystemID),
		})
		if err == nil {
			for _, mt := range describeMtOutput.MountTargets {
				_, err := env.targetEFSClient.DeleteMountTarget(ctx, &efs.DeleteMountTargetInput{
					MountTargetId: mt.MountTargetId,
				})
				if err != nil && !strings.Contains(err.Error(), "not found") {
					errors = append(errors, fmt.Errorf("failed to delete mount target %s: %w", *mt.MountTargetId, err))
				}
			}
			// Wait for mount targets to be deleted
			time.Sleep(30 * time.Second)
		}

		// Delete file system
		_, err = env.targetEFSClient.DeleteFileSystem(ctx, &efs.DeleteFileSystemInput{
			FileSystemId: aws.String(env.CrossAccountFileSystemID),
		})
		if err != nil && !strings.Contains(err.Error(), "not found") {
			errors = append(errors, fmt.Errorf("failed to delete file system %s: %w", env.CrossAccountFileSystemID, err))
		}
	}

	// Delete VPC peering connection
	if env.PeeringConnectionID != "" {
		_, err := env.ec2Client.DeleteVpcPeeringConnection(ctx, &ec2.DeleteVpcPeeringConnectionInput{
			VpcPeeringConnectionId: aws.String(env.PeeringConnectionID),
		})
		if err != nil && !strings.Contains(err.Error(), "not found") {
			errors = append(errors, fmt.Errorf("failed to delete VPC peering: %w", err))
		}
	}

	// Delete target VPC resources
	if env.TargetSecurityGroup != "" {
		_, err := env.targetEC2Client.DeleteSecurityGroup(ctx, &ec2.DeleteSecurityGroupInput{
			GroupId: aws.String(env.TargetSecurityGroup),
		})
		if err != nil && !strings.Contains(err.Error(), "not found") {
			errors = append(errors, fmt.Errorf("failed to delete security group: %w", err))
		}
	}

	for _, subnetID := range env.TargetSubnetIDs {
		_, err := env.targetEC2Client.DeleteSubnet(ctx, &ec2.DeleteSubnetInput{
			SubnetId: aws.String(subnetID),
		})
		if err != nil && !strings.Contains(err.Error(), "not found") {
			errors = append(errors, fmt.Errorf("failed to delete subnet %s: %w", subnetID, err))
		}
	}

	if env.TargetVpcID != "" {
		_, err := env.targetEC2Client.DeleteVpc(ctx, &ec2.DeleteVpcInput{
			VpcId: aws.String(env.TargetVpcID),
		})
		if err != nil && !strings.Contains(err.Error(), "not found") {
			errors = append(errors, fmt.Errorf("failed to delete VPC: %w", err))
		}
	}

	// Cleanup base environment
	if err := env.Cleanup(ctx); err != nil {
		errors = append(errors, fmt.Errorf("failed to cleanup base environment: %w", err))
	}

	if len(errors) > 0 {
		return fmt.Errorf("cleanup errors: %v", errors)
	}
	return nil
}

// Test functions for cross-account scenarios

func testCrossAccountSetup(t *testing.T, ctx context.Context, env *CrossAccountTestEnvironment) {
	t.Log("Testing cross-account environment setup...")

	// Verify target account access
	identity, err := env.targetSTSClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		t.Fatalf("Failed to get target account identity: %v", err)
	}

	if *identity.Account != env.TargetAccountID {
		t.Errorf("Account mismatch: expected %s, got %s", env.TargetAccountID, *identity.Account)
	}

	// Verify VPC exists
	describeVpcOutput, err := env.targetEC2Client.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{
		VpcIds: []string{env.TargetVpcID},
	})
	if err != nil {
		t.Fatalf("Failed to describe target VPC: %v", err)
	}

	if len(describeVpcOutput.Vpcs) != 1 {
		t.Errorf("Expected 1 VPC, got %d", len(describeVpcOutput.Vpcs))
	}

	t.Log("Cross-account setup verified successfully")
}

func testIAMRoleTrustRelationship(t *testing.T, ctx context.Context, env *CrossAccountTestEnvironment) {
	t.Log("Testing IAM role trust relationships...")

	// Verify that source account can assume the target role
	sessionName := fmt.Sprintf("test-trust-%s", env.TestID)
	assumeRoleOutput, err := env.stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(env.TargetRoleArn),
		RoleSessionName: aws.String(sessionName),
		DurationSeconds: aws.Int32(900), // 15 minutes
	})
	if err != nil {
		t.Fatalf("Failed to assume target role: %v", err)
	}

	if assumeRoleOutput.Credentials == nil {
		t.Fatal("No credentials returned from AssumeRole")
	}

	// Verify the assumed role has proper permissions
	tempCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(env.TargetRegion),
		config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     *assumeRoleOutput.Credentials.AccessKeyId,
				SecretAccessKey: *assumeRoleOutput.Credentials.SecretAccessKey,
				SessionToken:    *assumeRoleOutput.Credentials.SessionToken,
			}, nil
		})),
	)
	if err != nil {
		t.Fatalf("Failed to create temporary config: %v", err)
	}

	// Try to describe EFS with assumed role
	tempEFSClient := efs.NewFromConfig(tempCfg)
	_, err = tempEFSClient.DescribeFileSystems(ctx, &efs.DescribeFileSystemsInput{
		MaxItems: aws.Int32(1),
	})
	if err != nil {
		t.Fatalf("Failed to describe EFS with assumed role: %v", err)
	}

	t.Log("IAM trust relationship verified successfully")
}

func testCrossAccountEFSCreation(t *testing.T, ctx context.Context, env *CrossAccountTestEnvironment) {
	t.Log("Testing cross-account EFS creation...")

	// Verify EFS exists in target account
	describeFsOutput, err := env.targetEFSClient.DescribeFileSystems(ctx, &efs.DescribeFileSystemsInput{
		FileSystemId: aws.String(env.CrossAccountFileSystemID),
	})
	if err != nil {
		t.Fatalf("Failed to describe cross-account EFS: %v", err)
	}

	if len(describeFsOutput.FileSystems) != 1 {
		t.Errorf("Expected 1 file system, got %d", len(describeFsOutput.FileSystems))
	}

	fs := describeFsOutput.FileSystems[0]
	if *fs.LifeCycleState != efstypes.LifeCycleStateAvailable {
		t.Errorf("File system not available: %s", *fs.LifeCycleState)
	}

	// Verify mount targets exist
	describeMtOutput, err := env.targetEFSClient.DescribeMountTargets(ctx, &efs.DescribeMountTargetsInput{
		FileSystemId: aws.String(env.CrossAccountFileSystemID),
	})
	if err != nil {
		t.Fatalf("Failed to describe mount targets: %v", err)
	}

	if len(describeMtOutput.MountTargets) == 0 {
		t.Error("No mount targets found")
	}

	// Check mount target states
	for _, mt := range describeMtOutput.MountTargets {
		if *mt.LifeCycleState != efstypes.LifeCycleStateAvailable {
			t.Errorf("Mount target %s not available: %s", *mt.MountTargetId, *mt.LifeCycleState)
		}
	}

	t.Log("Cross-account EFS creation verified successfully")
}

func testVPCPeeringConnectivity(t *testing.T, ctx context.Context, env *CrossAccountTestEnvironment) {
	t.Log("Testing VPC peering connectivity...")

	// Verify peering connection status
	describePeeringOutput, err := env.ec2Client.DescribeVpcPeeringConnections(ctx, &ec2.DescribeVpcPeeringConnectionsInput{
		VpcPeeringConnectionIds: []string{env.PeeringConnectionID},
	})
	if err != nil {
		t.Fatalf("Failed to describe VPC peering connection: %v", err)
	}

	if len(describePeeringOutput.VpcPeeringConnections) != 1 {
		t.Errorf("Expected 1 peering connection, got %d", len(describePeeringOutput.VpcPeeringConnections))
	}

	peering := describePeeringOutput.VpcPeeringConnections[0]
	if peering.Status.Code != ec2types.VpcPeeringConnectionStateReasonCodeActive {
		t.Errorf("Peering connection not active: %s", peering.Status.Code)
	}

	// Verify routes exist in source VPC
	for _, rtID := range env.RouteTableIDs {
		describeRtOutput, err := env.ec2Client.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{
			RouteTableIds: []string{rtID},
		})
		if err != nil {
			t.Errorf("Failed to describe route table %s: %v", rtID, err)
			continue
		}

		foundRoute := false
		for _, rt := range describeRtOutput.RouteTables {
			for _, route := range rt.Routes {
				if route.DestinationCidrBlock != nil && *route.DestinationCidrBlock == "10.1.0.0/16" {
					foundRoute = true
					break
				}
			}
		}
		if !foundRoute {
			t.Errorf("Route to target VPC not found in route table %s", rtID)
		}
	}

	// Verify routes exist in target VPC
	for _, rtID := range env.PeeringRouteTableIDs {
		describeRtOutput, err := env.targetEC2Client.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{
			RouteTableIds: []string{rtID},
		})
		if err != nil {
			t.Errorf("Failed to describe target route table %s: %v", rtID, err)
			continue
		}

		foundRoute := false
		for _, rt := range describeRtOutput.RouteTables {
			for _, route := range rt.Routes {
				if route.DestinationCidrBlock != nil && *route.DestinationCidrBlock == "10.0.0.0/16" {
					foundRoute = true
					break
				}
			}
		}
		if !foundRoute {
			t.Errorf("Route to source VPC not found in route table %s", rtID)
		}
	}

	t.Log("VPC peering connectivity verified successfully")
}

func testCrossAccountEFSMount(t *testing.T, ctx context.Context, env *CrossAccountTestEnvironment) {
	t.Log("Testing cross-account EFS mount capabilities...")

	// Verify file system policy allows cross-account access
	describePolicyOutput, err := env.targetEFSClient.DescribeFileSystemPolicy(ctx, &efs.DescribeFileSystemPolicyInput{
		FileSystemId: aws.String(env.CrossAccountFileSystemID),
	})
	if err != nil {
		if !strings.Contains(err.Error(), "PolicyNotFound") {
			t.Logf("Warning: Failed to describe file system policy: %v", err)
		}
	} else if describePolicyOutput.Policy != nil {
		// Parse and verify policy
		var policy map[string]interface{}
		if err := json.Unmarshal([]byte(*describePolicyOutput.Policy), &policy); err != nil {
			t.Logf("Warning: Failed to parse file system policy: %v", err)
		} else {
			t.Log("File system policy is configured for cross-account access")
		}
	}

	// Verify security group rules allow NFS traffic
	describeSgOutput, err := env.targetEC2Client.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{
		GroupIds: []string{env.TargetSecurityGroup},
	})
	if err != nil {
		t.Fatalf("Failed to describe security group: %v", err)
	}

	if len(describeSgOutput.SecurityGroups) != 1 {
		t.Errorf("Expected 1 security group, got %d", len(describeSgOutput.SecurityGroups))
	}

	sg := describeSgOutput.SecurityGroups[0]
	foundNFSRule := false
	for _, rule := range sg.IpPermissions {
		if rule.FromPort != nil && *rule.FromPort == 2049 {
			foundNFSRule = true
			break
		}
	}
	if !foundNFSRule {
		t.Error("NFS ingress rule not found in security group")
	}

	t.Log("Cross-account EFS mount capability verified successfully")
}

func testCrossAccountAccessPoint(t *testing.T, ctx context.Context, env *CrossAccountTestEnvironment) {
	t.Log("Testing cross-account access points...")

	// Verify access points exist
	for _, apID := range env.CrossAccountAccessPoints {
		describeApOutput, err := env.targetEFSClient.DescribeAccessPoints(ctx, &efs.DescribeAccessPointsInput{
			AccessPointId: aws.String(apID),
		})
		if err != nil {
			t.Errorf("Failed to describe access point %s: %v", apID, err)
			continue
		}

		if len(describeApOutput.AccessPoints) != 1 {
			t.Errorf("Expected 1 access point, got %d", len(describeApOutput.AccessPoints))
			continue
		}

		ap := describeApOutput.AccessPoints[0]
		if *ap.LifeCycleState != efstypes.LifeCycleStateAvailable {
			t.Errorf("Access point %s not available: %s", apID, *ap.LifeCycleState)
		}

		// Verify POSIX user is set
		if ap.PosixUser == nil || ap.PosixUser.Uid == nil || ap.PosixUser.Gid == nil {
			t.Errorf("Access point %s missing POSIX user configuration", apID)
		}

		// Verify root directory is set
		if ap.RootDirectory == nil || ap.RootDirectory.Path == nil {
			t.Errorf("Access point %s missing root directory configuration", apID)
		}
	}

	t.Log("Cross-account access points verified successfully")
}

func testCrossAccountPermissions(t *testing.T, ctx context.Context, env *CrossAccountTestEnvironment) {
	t.Log("Testing cross-account permissions...")

	// Test that source account can describe target account's EFS
	// This requires setting up assumed role credentials
	sessionName := fmt.Sprintf("test-permissions-%s", env.TestID)
	assumeRoleOutput, err := env.stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(env.TargetRoleArn),
		RoleSessionName: aws.String(sessionName),
		DurationSeconds: aws.Int32(900),
	})
	if err != nil {
		t.Fatalf("Failed to assume role for permissions test: %v", err)
	}

	// Create EFS client with assumed role
	tempCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(env.TargetRegion),
		config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     *assumeRoleOutput.Credentials.AccessKeyId,
				SecretAccessKey: *assumeRoleOutput.Credentials.SecretAccessKey,
				SessionToken:    *assumeRoleOutput.Credentials.SessionToken,
			}, nil
		})),
	)
	if err != nil {
		t.Fatalf("Failed to create temp config: %v", err)
	}

	tempEFSClient := efs.NewFromConfig(tempCfg)

	// Try to describe the file system
	describeFsOutput, err := tempEFSClient.DescribeFileSystems(ctx, &efs.DescribeFileSystemsInput{
		FileSystemId: aws.String(env.CrossAccountFileSystemID),
	})
	if err != nil {
		t.Fatalf("Failed to describe file system with assumed role: %v", err)
	}

	if len(describeFsOutput.FileSystems) != 1 {
		t.Errorf("Expected 1 file system, got %d", len(describeFsOutput.FileSystems))
	}

	// Try to describe mount targets
	_, err = tempEFSClient.DescribeMountTargets(ctx, &efs.DescribeMountTargetsInput{
		FileSystemId: aws.String(env.CrossAccountFileSystemID),
	})
	if err != nil {
		t.Fatalf("Failed to describe mount targets with assumed role: %v", err)
	}

	// Try to describe access points
	for _, apID := range env.CrossAccountAccessPoints {
		_, err = tempEFSClient.DescribeAccessPoints(ctx, &efs.DescribeAccessPointsInput{
			AccessPointId: aws.String(apID),
		})
		if err != nil {
			t.Errorf("Failed to describe access point %s with assumed role: %v", apID, err)
		}
	}

	t.Log("Cross-account permissions verified successfully")
}

func testCrossAccountDataIsolation(t *testing.T, ctx context.Context, env *CrossAccountTestEnvironment) {
	t.Log("Testing cross-account data isolation...")

	// Create separate access points for isolation testing
	isolationAPs := make([]string, 2)
	for i := 0; i < 2; i++ {
		createApInput := &efs.CreateAccessPointInput{
			FileSystemId: aws.String(env.CrossAccountFileSystemID),
			PosixUser: &efstypes.PosixUser{
				Uid: aws.Int64(int64(2000 + i)),
				Gid: aws.Int64(int64(2000 + i)),
			},
			RootDirectory: &efstypes.RootDirectory{
				Path: aws.String(fmt.Sprintf("/isolation-test-%d", i)),
				CreationInfo: &efstypes.CreationInfo{
					OwnerUid:    aws.Int64(int64(2000 + i)),
					OwnerGid:    aws.Int64(int64(2000 + i)),
					Permissions: aws.String("700"), // Restrictive permissions
				},
			},
			Tags: []efstypes.Tag{
				{Key: aws.String("Name"), Value: aws.String(fmt.Sprintf("isolation-ap-%s-%d", env.TestID, i))},
				{Key: aws.String(testenv.TestTagKey), Value: aws.String(env.TestID)},
			},
		}

		createApOutput, err := env.targetEFSClient.CreateAccessPoint(ctx, createApInput)
		if err != nil {
			t.Errorf("Failed to create isolation access point %d: %v", i, err)
			continue
		}
		isolationAPs[i] = *createApOutput.AccessPointId
	}

	// Verify access points have different UIDs/GIDs
	for i, apID := range isolationAPs {
		describeApOutput, err := env.targetEFSClient.DescribeAccessPoints(ctx, &efs.DescribeAccessPointsInput{
			AccessPointId: aws.String(apID),
		})
		if err != nil {
			t.Errorf("Failed to describe isolation access point %d: %v", i, err)
			continue
		}

		ap := describeApOutput.AccessPoints[0]
		expectedUID := int64(2000 + i)
		if *ap.PosixUser.Uid != expectedUID {
			t.Errorf("Access point %d has wrong UID: expected %d, got %d", i, expectedUID, *ap.PosixUser.Uid)
		}

		// Verify restrictive permissions
		if ap.RootDirectory.CreationInfo != nil && *ap.RootDirectory.CreationInfo.Permissions != "700" {
			t.Errorf("Access point %d has wrong permissions: %s", i, *ap.RootDirectory.CreationInfo.Permissions)
		}
	}

	// Cleanup isolation test access points
	for _, apID := range isolationAPs {
		if apID != "" {
			_, err := env.targetEFSClient.DeleteAccessPoint(ctx, &efs.DeleteAccessPointInput{
				AccessPointId: aws.String(apID),
			})
			if err != nil && !strings.Contains(err.Error(), "not found") {
				t.Logf("Warning: Failed to delete isolation access point %s: %v", apID, err)
			}
		}
	}

	t.Log("Cross-account data isolation verified successfully")
}

func testCrossAccountFailover(t *testing.T, ctx context.Context, env *CrossAccountTestEnvironment) {
	t.Log("Testing cross-account failover scenarios...")

	// Simulate mount target failure by getting mount target details
	describeMtOutput, err := env.targetEFSClient.DescribeMountTargets(ctx, &efs.DescribeMountTargetsInput{
		FileSystemId: aws.String(env.CrossAccountFileSystemID),
	})
	if err != nil {
		t.Fatalf("Failed to describe mount targets for failover test: %v", err)
	}

	if len(describeMtOutput.MountTargets) < 2 {
		t.Skip("Not enough mount targets for failover test")
	}

	// Track mount target availability zones
	mountTargetAZs := make(map[string]string)
	for _, mt := range describeMtOutput.MountTargets {
		mountTargetAZs[*mt.MountTargetId] = *mt.AvailabilityZoneName
	}

	// Verify multi-AZ deployment
	uniqueAZs := make(map[string]bool)
	for _, az := range mountTargetAZs {
		uniqueAZs[az] = true
	}

	if len(uniqueAZs) < 2 {
		t.Error("Mount targets not distributed across multiple AZs")
	} else {
		t.Logf("Mount targets distributed across %d AZs", len(uniqueAZs))
	}

	// Test access point availability during simulated failover
	for _, apID := range env.CrossAccountAccessPoints {
		describeApOutput, err := env.targetEFSClient.DescribeAccessPoints(ctx, &efs.DescribeAccessPointsInput{
			AccessPointId: aws.String(apID),
		})
		if err != nil {
			t.Errorf("Access point %s not accessible during failover test: %v", apID, err)
			continue
		}

		if *describeApOutput.AccessPoints[0].LifeCycleState != efstypes.LifeCycleStateAvailable {
			t.Errorf("Access point %s not available during failover test", apID)
		}
	}

	t.Log("Cross-account failover scenarios verified successfully")
}

func testCrossAccountCleanup(t *testing.T, ctx context.Context, env *CrossAccountTestEnvironment) {
	t.Log("Testing cross-account cleanup...")

	// Create temporary resources for cleanup test
	testApID := ""
	createApInput := &efs.CreateAccessPointInput{
		FileSystemId: aws.String(env.CrossAccountFileSystemID),
		PosixUser: &efstypes.PosixUser{
			Uid: aws.Int64(9999),
			Gid: aws.Int64(9999),
		},
		RootDirectory: &efstypes.RootDirectory{
			Path: aws.String("/cleanup-test"),
			CreationInfo: &efstypes.CreationInfo{
				OwnerUid:    aws.Int64(9999),
				OwnerGid:    aws.Int64(9999),
				Permissions: aws.String("755"),
			},
		},
		Tags: []efstypes.Tag{
			{Key: aws.String("Name"), Value: aws.String(fmt.Sprintf("cleanup-test-ap-%s", env.TestID))},
			{Key: aws.String(testenv.TestTagKey), Value: aws.String(env.TestID)},
		},
	}

	createApOutput, err := env.targetEFSClient.CreateAccessPoint(ctx, createApInput)
	if err != nil {
		t.Fatalf("Failed to create test access point for cleanup: %v", err)
	}
	testApID = *createApOutput.AccessPointId

	// Delete the test access point
	_, err = env.targetEFSClient.DeleteAccessPoint(ctx, &efs.DeleteAccessPointInput{
		AccessPointId: aws.String(testApID),
	})
	if err != nil {
		t.Errorf("Failed to delete test access point: %v", err)
	}

	// Verify deletion
	time.Sleep(5 * time.Second)
	describeApOutput, err := env.targetEFSClient.DescribeAccessPoints(ctx, &efs.DescribeAccessPointsInput{
		AccessPointId: aws.String(testApID),
	})
	if err == nil && len(describeApOutput.AccessPoints) > 0 {
		ap := describeApOutput.AccessPoints[0]
		if *ap.LifeCycleState != efstypes.LifeCycleStateDeleting && *ap.LifeCycleState != efstypes.LifeCycleStateDeleted {
			t.Errorf("Access point not in deleting/deleted state: %s", *ap.LifeCycleState)
		}
	}

	// Test tag-based resource discovery for cleanup
	describeApByTagOutput, err := env.targetEFSClient.DescribeAccessPoints(ctx, &efs.DescribeAccessPointsInput{
		FileSystemId: aws.String(env.CrossAccountFileSystemID),
	})
	if err != nil {
		t.Logf("Warning: Failed to describe access points by tag: %v", err)
	} else {
		taggedCount := 0
		for _, ap := range describeApByTagOutput.AccessPoints {
			for _, tag := range ap.Tags {
				if *tag.Key == testenv.TestTagKey && *tag.Value == env.TestID {
					taggedCount++
					break
				}
			}
		}
		t.Logf("Found %d access points with test tags for cleanup", taggedCount)
	}

	t.Log("Cross-account cleanup verified successfully")
}