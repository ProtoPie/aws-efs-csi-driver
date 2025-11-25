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

//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	efsns "github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/driver/efs-ns"
)

// AWSIntegrationTestSuite contains tests that require real AWS API access
type AWSIntegrationTestSuite struct {
	suite.Suite
	ctx         context.Context
	efsClient   efsns.EFSClient
	ec2Client   efsns.EC2Client
	region      string
	vpcID       string
	subnetIDs   []string
	clusterID   string
	testPrefix  string
	createdFSs  []string // Track created filesystems for cleanup
	createdSGs  []string // Track created security groups for cleanup
}

// SetupSuite runs once before all tests in the suite
func (suite *AWSIntegrationTestSuite) SetupSuite() {
	suite.ctx = context.Background()
	suite.testPrefix = fmt.Sprintf("efs-ns-integration-test-%d", time.Now().Unix())
	suite.clusterID = "integration-test-cluster"

	// Check required environment variables
	suite.region = os.Getenv("AWS_REGION")
	if suite.region == "" {
		suite.region = "us-west-2" // Default region
	}

	suite.vpcID = os.Getenv("EFS_NS_TEST_VPC_ID")
	if suite.vpcID == "" {
		suite.T().Skip("Skipping AWS integration tests: EFS_NS_TEST_VPC_ID not set")
		return
	}

	subnetIDsStr := os.Getenv("EFS_NS_TEST_SUBNET_IDS")
	if subnetIDsStr == "" {
		suite.T().Skip("Skipping AWS integration tests: EFS_NS_TEST_SUBNET_IDS not set")
		return
	}
	suite.subnetIDs = strings.Split(subnetIDsStr, ",")

	// Initialize AWS clients
	cfg, err := config.LoadDefaultConfig(suite.ctx, config.WithRegion(suite.region))
	require.NoError(suite.T(), err, "Failed to load AWS config")

	suite.efsClient = efs.NewFromConfig(cfg)
	suite.ec2Client = ec2.NewFromConfig(cfg)

	// Initialize tracking slices
	suite.createdFSs = make([]string, 0)
	suite.createdSGs = make([]string, 0)
}

// TearDownSuite runs once after all tests in the suite complete
func (suite *AWSIntegrationTestSuite) TearDownSuite() {
	// Clean up created filesystems
	for _, fsID := range suite.createdFSs {
		suite.cleanupFileSystem(fsID)
	}

	// Clean up created security groups
	for _, sgID := range suite.createdSGs {
		suite.cleanupSecurityGroup(sgID)
	}
}

func (suite *AWSIntegrationTestSuite) cleanupFileSystem(fsID string) {
	ctx, cancel := context.WithTimeout(suite.ctx, 5*time.Minute)
	defer cancel()

	// Delete mount targets first
	mountTargetsOutput, err := suite.efsClient.DescribeMountTargets(ctx, &efs.DescribeMountTargetsInput{
		FileSystemId: aws.String(fsID),
	})
	if err == nil {
		for _, mt := range mountTargetsOutput.MountTargets {
			_, _ = suite.efsClient.DeleteMountTarget(ctx, &efs.DeleteMountTargetInput{
				MountTargetId: mt.MountTargetId,
			})
		}

		// Wait for mount targets to be deleted
		for len(mountTargetsOutput.MountTargets) > 0 {
			time.Sleep(10 * time.Second)
			mountTargetsOutput, err = suite.efsClient.DescribeMountTargets(ctx, &efs.DescribeMountTargetsInput{
				FileSystemId: aws.String(fsID),
			})
			if err != nil {
				break
			}
		}
	}

	// Delete filesystem
	_, _ = suite.efsClient.DeleteFileSystem(ctx, &efs.DeleteFileSystemInput{
		FileSystemId: aws.String(fsID),
	})
}

func (suite *AWSIntegrationTestSuite) cleanupSecurityGroup(sgID string) {
	ctx, cancel := context.WithTimeout(suite.ctx, 1*time.Minute)
	defer cancel()

	_, _ = suite.ec2Client.DeleteSecurityGroup(ctx, &ec2.DeleteSecurityGroupInput{
		GroupId: aws.String(sgID),
	})
}

func (suite *AWSIntegrationTestSuite) TestCreateFileSystem_Success() {
	namespace := fmt.Sprintf("%s-create-test", suite.testPrefix)
	
	options := &efsns.FileSystemOptions{
		PerformanceMode: "generalPurpose",
		ThroughputMode:  "bursting",
		Encrypted:       aws.Bool(true),
		Tags: map[string]string{
			"efsns-namespace": namespace,
			"efsns-cluster":   suite.clusterID,
			"test":            "integration",
		},
	}

	// Create filesystem
	input := &efs.CreateFileSystemInput{
		CreationToken:   aws.String(fmt.Sprintf("efs-ns-%s-%s", namespace, suite.clusterID)),
		PerformanceMode: "generalPurpose",
		ThroughputMode:  "bursting",
		Encrypted:       options.Encrypted,
		Tags: []efs.Tag{
			{Key: aws.String("Name"), Value: aws.String(fmt.Sprintf("efs-ns-%s-%s", namespace, suite.clusterID))},
			{Key: aws.String("efsns-namespace"), Value: aws.String(namespace)},
			{Key: aws.String("efsns-cluster"), Value: aws.String(suite.clusterID)},
			{Key: aws.String("test"), Value: aws.String("integration")},
		},
	}

	output, err := suite.efsClient.CreateFileSystem(suite.ctx, input)
	require.NoError(suite.T(), err, "Failed to create filesystem")
	require.NotNil(suite.T(), output.FileSystemId, "FileSystemId should not be nil")

	fsID := aws.ToString(output.FileSystemId)
	suite.createdFSs = append(suite.createdFSs, fsID)

	// Verify filesystem properties
	assert.True(suite.T(), strings.HasPrefix(fsID, "fs-"), "FileSystemId should start with 'fs-'")
	assert.Equal(suite.T(), "generalPurpose", string(output.PerformanceMode))
	assert.Equal(suite.T(), "bursting", string(output.ThroughputMode))
	assert.Equal(suite.T(), true, aws.ToBool(output.Encrypted))

	// Wait for filesystem to be available
	suite.waitForFileSystemState(fsID, "available", 2*time.Minute)
}

func (suite *AWSIntegrationTestSuite) TestCreateFileSystem_WithProvisionedThroughput() {
	namespace := fmt.Sprintf("%s-provisioned-test", suite.testPrefix)
	
	throughputMibps := int64(100)
	input := &efs.CreateFileSystemInput{
		CreationToken:                aws.String(fmt.Sprintf("efs-ns-%s-%s", namespace, suite.clusterID)),
		PerformanceMode:              "generalPurpose",
		ThroughputMode:               "provisioned",
		ProvisionedThroughputInMibps: aws.Float64(float64(throughputMibps)),
		Encrypted:                    aws.Bool(true),
		Tags: []efs.Tag{
			{Key: aws.String("Name"), Value: aws.String(fmt.Sprintf("efs-ns-%s-%s", namespace, suite.clusterID))},
			{Key: aws.String("efsns-namespace"), Value: aws.String(namespace)},
			{Key: aws.String("efsns-cluster"), Value: aws.String(suite.clusterID)},
		},
	}

	output, err := suite.efsClient.CreateFileSystem(suite.ctx, input)
	require.NoError(suite.T(), err, "Failed to create filesystem with provisioned throughput")
	
	fsID := aws.ToString(output.FileSystemId)
	suite.createdFSs = append(suite.createdFSs, fsID)

	// Verify provisioned throughput
	assert.Equal(suite.T(), "provisioned", string(output.ThroughputMode))
	assert.Equal(suite.T(), float64(throughputMibps), aws.ToFloat64(output.ProvisionedThroughputInMibps))

	suite.waitForFileSystemState(fsID, "available", 2*time.Minute)
}

func (suite *AWSIntegrationTestSuite) TestCreateSecurityGroup_Success() {
	namespace := fmt.Sprintf("%s-sg-test", suite.testPrefix)
	
	input := &ec2.CreateSecurityGroupInput{
		GroupName:   aws.String(fmt.Sprintf("efs-ns-%s-%s", namespace, suite.clusterID)),
		Description: aws.String(fmt.Sprintf("EFS security group for namespace %s", namespace)),
		VpcId:       aws.String(suite.vpcID),
		TagSpecifications: []ec2.TagSpecification{
			{
				ResourceType: "security-group",
				Tags: []ec2.Tag{
					{Key: aws.String("Name"), Value: aws.String(fmt.Sprintf("efs-ns-%s-%s", namespace, suite.clusterID))},
					{Key: aws.String("efsns-namespace"), Value: aws.String(namespace)},
					{Key: aws.String("efsns-cluster"), Value: aws.String(suite.clusterID)},
				},
			},
		},
	}

	output, err := suite.ec2Client.CreateSecurityGroup(suite.ctx, input)
	require.NoError(suite.T(), err, "Failed to create security group")
	require.NotNil(suite.T(), output.GroupId, "GroupId should not be nil")

	sgID := aws.ToString(output.GroupId)
	suite.createdSGs = append(suite.createdSGs, sgID)

	// Verify security group properties
	assert.True(suite.T(), strings.HasPrefix(sgID, "sg-"), "GroupId should start with 'sg-'")

	// Add EFS ingress rules
	_, err = suite.ec2Client.AuthorizeSecurityGroupIngress(suite.ctx, &ec2.AuthorizeSecurityGroupIngressInput{
		GroupId: aws.String(sgID),
		IpPermissions: []ec2.IpPermission{
			{
				IpProtocol: aws.String("tcp"),
				FromPort:   aws.Int32(2049),
				ToPort:     aws.Int32(2049),
				IpRanges:   []ec2.IpRange{{CidrIp: aws.String("10.0.0.0/8")}},
			},
		},
	})
	assert.NoError(suite.T(), err, "Failed to add ingress rules to security group")
}

func (suite *AWSIntegrationTestSuite) TestCreateMountTargets_Success() {
	namespace := fmt.Sprintf("%s-mt-test", suite.testPrefix)
	
	// First create a filesystem
	fsOutput, err := suite.efsClient.CreateFileSystem(suite.ctx, &efs.CreateFileSystemInput{
		CreationToken:   aws.String(fmt.Sprintf("efs-ns-%s-%s", namespace, suite.clusterID)),
		PerformanceMode: "generalPurpose",
		ThroughputMode:  "bursting",
		Encrypted:       aws.Bool(true),
	})
	require.NoError(suite.T(), err, "Failed to create filesystem for mount target test")
	
	fsID := aws.ToString(fsOutput.FileSystemId)
	suite.createdFSs = append(suite.createdFSs, fsID)

	suite.waitForFileSystemState(fsID, "available", 2*time.Minute)

	// Create security group
	sgOutput, err := suite.ec2Client.CreateSecurityGroup(suite.ctx, &ec2.CreateSecurityGroupInput{
		GroupName:   aws.String(fmt.Sprintf("efs-ns-%s-%s", namespace, suite.clusterID)),
		Description: aws.String("Test security group"),
		VpcId:       aws.String(suite.vpcID),
	})
	require.NoError(suite.T(), err, "Failed to create security group for mount target test")
	
	sgID := aws.ToString(sgOutput.GroupId)
	suite.createdSGs = append(suite.createdSGs, sgID)

	// Create mount targets for each subnet
	createdMountTargets := make([]string, 0)
	for _, subnetID := range suite.subnetIDs {
		mtOutput, err := suite.efsClient.CreateMountTarget(suite.ctx, &efs.CreateMountTargetInput{
			FileSystemId:   aws.String(fsID),
			SubnetId:       aws.String(subnetID),
			SecurityGroups: []string{sgID},
		})
		require.NoError(suite.T(), err, "Failed to create mount target for subnet %s", subnetID)
		
		mtID := aws.ToString(mtOutput.MountTargetId)
		createdMountTargets = append(createdMountTargets, mtID)
		
		assert.True(suite.T(), strings.HasPrefix(mtID, "fsmt-"), "MountTargetId should start with 'fsmt-'")
		assert.Equal(suite.T(), subnetID, aws.ToString(mtOutput.SubnetId))
	}

	// Verify all mount targets were created
	assert.Equal(suite.T(), len(suite.subnetIDs), len(createdMountTargets))

	// Wait for mount targets to be available
	for _, mtID := range createdMountTargets {
		suite.waitForMountTargetState(mtID, "available", 3*time.Minute)
	}
}

func (suite *AWSIntegrationTestSuite) TestDeleteFileSystem_WithMountTargets() {
	namespace := fmt.Sprintf("%s-delete-test", suite.testPrefix)
	
	// Create filesystem
	fsOutput, err := suite.efsClient.CreateFileSystem(suite.ctx, &efs.CreateFileSystemInput{
		CreationToken:   aws.String(fmt.Sprintf("efs-ns-%s-%s", namespace, suite.clusterID)),
		PerformanceMode: "generalPurpose",
		ThroughputMode:  "bursting",
	})
	require.NoError(suite.T(), err, "Failed to create filesystem for deletion test")
	
	fsID := aws.ToString(fsOutput.FileSystemId)
	suite.waitForFileSystemState(fsID, "available", 2*time.Minute)

	// Create security group
	sgOutput, err := suite.ec2Client.CreateSecurityGroup(suite.ctx, &ec2.CreateSecurityGroupInput{
		GroupName:   aws.String(fmt.Sprintf("efs-ns-%s-%s", namespace, suite.clusterID)),
		Description: aws.String("Test security group"),
		VpcId:       aws.String(suite.vpcID),
	})
	require.NoError(suite.T(), err, "Failed to create security group for deletion test")
	sgID := aws.ToString(sgOutput.GroupId)

	// Create mount target
	mtOutput, err := suite.efsClient.CreateMountTarget(suite.ctx, &efs.CreateMountTargetInput{
		FileSystemId:   aws.String(fsID),
		SubnetId:       aws.String(suite.subnetIDs[0]),
		SecurityGroups: []string{sgID},
	})
	require.NoError(suite.T(), err, "Failed to create mount target for deletion test")
	mtID := aws.ToString(mtOutput.MountTargetId)

	suite.waitForMountTargetState(mtID, "available", 3*time.Minute)

	// Delete mount target first
	_, err = suite.efsClient.DeleteMountTarget(suite.ctx, &efs.DeleteMountTargetInput{
		MountTargetId: aws.String(mtID),
	})
	assert.NoError(suite.T(), err, "Failed to delete mount target")

	// Wait for mount target deletion
	suite.waitForMountTargetDeletion(mtID, 3*time.Minute)

	// Delete filesystem
	_, err = suite.efsClient.DeleteFileSystem(suite.ctx, &efs.DeleteFileSystemInput{
		FileSystemId: aws.String(fsID),
	})
	assert.NoError(suite.T(), err, "Failed to delete filesystem")

	// Verify filesystem deletion
	suite.waitForFileSystemDeletion(fsID, 2*time.Minute)

	// Delete security group
	_, err = suite.ec2Client.DeleteSecurityGroup(suite.ctx, &ec2.DeleteSecurityGroupInput{
		GroupId: aws.String(sgID),
	})
	assert.NoError(suite.T(), err, "Failed to delete security group")
}

func (suite *AWSIntegrationTestSuite) TestListFileSystems_WithTags() {
	// Create filesystem with specific tags
	namespace := fmt.Sprintf("%s-list-test", suite.testPrefix)
	
	fsOutput, err := suite.efsClient.CreateFileSystem(suite.ctx, &efs.CreateFileSystemInput{
		CreationToken: aws.String(fmt.Sprintf("efs-ns-%s-%s", namespace, suite.clusterID)),
		Tags: []efs.Tag{
			{Key: aws.String("efsns-namespace"), Value: aws.String(namespace)},
			{Key: aws.String("efsns-cluster"), Value: aws.String(suite.clusterID)},
			{Key: aws.String("test-type"), Value: aws.String("list-test")},
		},
	})
	require.NoError(suite.T(), err, "Failed to create filesystem for list test")
	
	fsID := aws.ToString(fsOutput.FileSystemId)
	suite.createdFSs = append(suite.createdFSs, fsID)

	// List filesystems and verify our test filesystem appears with correct tags
	listOutput, err := suite.efsClient.DescribeFileSystems(suite.ctx, &efs.DescribeFileSystemsInput{})
	require.NoError(suite.T(), err, "Failed to list filesystems")

	// Find our test filesystem
	var testFS *efs.FileSystemDescription
	for _, fs := range listOutput.FileSystems {
		if aws.ToString(fs.FileSystemId) == fsID {
			testFS = &fs
			break
		}
	}

	require.NotNil(suite.T(), testFS, "Test filesystem not found in list")

	// Verify tags
	tagMap := make(map[string]string)
	for _, tag := range testFS.Tags {
		tagMap[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}

	assert.Equal(suite.T(), namespace, tagMap["efsns-namespace"])
	assert.Equal(suite.T(), suite.clusterID, tagMap["efsns-cluster"])
	assert.Equal(suite.T(), "list-test", tagMap["test-type"])
}

// Helper methods

func (suite *AWSIntegrationTestSuite) waitForFileSystemState(fsID, expectedState string, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(suite.ctx, timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			suite.T().Fatalf("Timeout waiting for filesystem %s to reach state %s", fsID, expectedState)
		default:
			output, err := suite.efsClient.DescribeFileSystems(ctx, &efs.DescribeFileSystemsInput{
				FileSystemId: aws.String(fsID),
			})
			if err == nil && len(output.FileSystems) > 0 {
				currentState := string(output.FileSystems[0].LifeCycleState)
				if currentState == expectedState {
					return
				}
			}
			time.Sleep(10 * time.Second)
		}
	}
}

func (suite *AWSIntegrationTestSuite) waitForMountTargetState(mtID, expectedState string, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(suite.ctx, timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			suite.T().Fatalf("Timeout waiting for mount target %s to reach state %s", mtID, expectedState)
		default:
			output, err := suite.efsClient.DescribeMountTargets(ctx, &efs.DescribeMountTargetsInput{
				MountTargetId: aws.String(mtID),
			})
			if err == nil && len(output.MountTargets) > 0 {
				currentState := string(output.MountTargets[0].LifeCycleState)
				if currentState == expectedState {
					return
				}
			}
			time.Sleep(5 * time.Second)
		}
	}
}

func (suite *AWSIntegrationTestSuite) waitForFileSystemDeletion(fsID string, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(suite.ctx, timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			suite.T().Fatalf("Timeout waiting for filesystem %s to be deleted", fsID)
		default:
			_, err := suite.efsClient.DescribeFileSystems(ctx, &efs.DescribeFileSystemsInput{
				FileSystemId: aws.String(fsID),
			})
			if err != nil {
				// Filesystem not found, deletion complete
				return
			}
			time.Sleep(10 * time.Second)
		}
	}
}

func (suite *AWSIntegrationTestSuite) waitForMountTargetDeletion(mtID string, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(suite.ctx, timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			suite.T().Fatalf("Timeout waiting for mount target %s to be deleted", mtID)
		default:
			_, err := suite.efsClient.DescribeMountTargets(ctx, &efs.DescribeMountTargetsInput{
				MountTargetId: aws.String(mtID),
			})
			if err != nil {
				// Mount target not found, deletion complete
				return
			}
			time.Sleep(5 * time.Second)
		}
	}
}

// TestAWSIntegrationSuite runs the AWS integration test suite
func TestAWSIntegrationSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping AWS integration tests in short mode")
	}

	suite.Run(t, new(AWSIntegrationTestSuite))
}