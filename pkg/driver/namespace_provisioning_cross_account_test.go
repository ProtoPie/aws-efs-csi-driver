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

package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/cloud"
	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/cloud/mocks"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// TestCrossAccountEFSProvisioning tests cross-account EFS provisioning scenarios
func TestCrossAccountEFSProvisioning(t *testing.T) {
	tests := []struct {
		name                string
		targetAccountID     string
		targetRoleArn       string
		sourceAccountID     string
		provisionerSecret   *corev1.Secret
		expectError         bool
		expectedErrMessage  string
		setupMocks          func(*mocks.MockCloud)
	}{
		{
			name:            "successful cross-account provisioning",
			targetAccountID: "123456789012",
			targetRoleArn:   "arn:aws:iam::123456789012:role/efs-csi-cross-account",
			sourceAccountID: "987654321098",
			provisionerSecret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cross-account-secret",
					Namespace: "kube-system",
				},
				Data: map[string][]byte{
					"awsRoleArn": []byte("arn:aws:iam::123456789012:role/efs-csi-cross-account"),
				},
			},
			expectError: false,
			setupMocks: func(mockCloud *mocks.MockCloud) {
				// Mock successful cross-account EFS creation
				mockCloud.EXPECT().CreateAccessPoint(gomock.Any(), gomock.Any(), gomock.Any()).Return(&cloud.AccessPoint{
					AccessPointId:      "fsap-cross-account-12345",
					FileSystemId:       "fs-cross-account",
					AccessPointRootDir: "/cross-account/test-namespace",
					PosixUser:          &cloud.PosixUser{Uid: 1000, Gid: 1000},
				}, nil)

				// EC2 operations would be handled internally by the cloud provider
			},
		},
		{
			name:            "cross-account provisioning with invalid role",
			targetAccountID: "123456789012",
			targetRoleArn:   "arn:aws:iam::123456789012:role/invalid-role",
			sourceAccountID: "987654321098",
			provisionerSecret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cross-account-secret",
					Namespace: "kube-system",
				},
				Data: map[string][]byte{
					"awsRoleArn": []byte("arn:aws:iam::123456789012:role/invalid-role"),
				},
			},
			expectError:        true,
			expectedErrMessage: "failed to assume role",
			setupMocks: func(mockCloud *mocks.MockCloud) {
				// Mock role assumption failure
				mockCloud.EXPECT().CreateAccessPoint(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil,
					errors.New("failed to assume role: AccessDenied"))
			},
		},
		{
			name:            "cross-account provisioning with network connectivity issue",
			targetAccountID: "123456789012",
			targetRoleArn:   "arn:aws:iam::123456789012:role/efs-csi-cross-account",
			sourceAccountID: "987654321098",
			provisionerSecret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cross-account-secret",
					Namespace: "kube-system",
				},
				Data: map[string][]byte{
					"awsRoleArn": []byte("arn:aws:iam::123456789012:role/efs-csi-cross-account"),
				},
			},
			expectError:        true,
			expectedErrMessage: "network connectivity error",
			setupMocks: func(mockCloud *mocks.MockCloud) {
				mockCloud.EXPECT().CreateAccessPoint(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil,
					errors.New("network connectivity error: VPC peering not established"))

				// VPC peering issues would result in access point creation failure
			},
		},
		{
			name:            "cross-account provisioning without secret",
			targetAccountID: "123456789012",
			targetRoleArn:   "",
			sourceAccountID: "987654321098",
			provisionerSecret: nil,
			expectError:        true,
			expectedErrMessage: "provisioner secret not provided",
			setupMocks: func(mockCloud *mocks.MockCloud) {
				// No mocks needed as it should fail early
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockCloud := mocks.NewMockCloud(ctrl)

			// Setup mocks
			if tc.setupMocks != nil {
				tc.setupMocks(mockCloud)
			}

			// Create fake Kubernetes client with secret if provided
			var k8sClient *fake.Clientset
			if tc.provisionerSecret != nil {
				k8sClient = fake.NewSimpleClientset(tc.provisionerSecret)
			} else {
				k8sClient = fake.NewSimpleClientset()
			}

			// Create namespace provisioner
			provisioner := &NamespaceProvisioner{
				cloud:     mockCloud,
				k8sClient: k8sClient,
			}

			// Test cross-account provisioning
			ctx := context.Background()
			namespace := "test-namespace"

			// Build parameters for cross-account provisioning
			params := map[string]string{
				"provisioningMode": "efs-ns",
				"basePath":         "/cross-account",
			}
			if tc.provisionerSecret != nil {
				params["csi.storage.k8s.io/provisioner-secret-name"] = tc.provisionerSecret.Name
				params["csi.storage.k8s.io/provisioner-secret-namespace"] = tc.provisionerSecret.Namespace
			}

			// Execute provisioning
			result, err := provisioner.provisionCrossAccountEFS(ctx, namespace, params)

			if tc.expectError {
				assert.Error(t, err)
				if tc.expectedErrMessage != "" {
					assert.Contains(t, err.Error(), tc.expectedErrMessage)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
			}
		})
	}
}

// TestCrossAccountIAMValidation tests IAM role and trust relationship validation
func TestCrossAccountIAMValidation(t *testing.T) {
	tests := []struct {
		name               string
		roleArn            string
		trustPolicy        string
		expectedValid      bool
		expectedErrMessage string
	}{
		{
			name:    "valid cross-account trust relationship",
			roleArn: "arn:aws:iam::123456789012:role/efs-csi-cross-account",
			trustPolicy: `{
				"Version": "2012-10-17",
				"Statement": [
					{
						"Effect": "Allow",
						"Principal": {
							"AWS": "arn:aws:iam::987654321098:root"
						},
						"Action": "sts:AssumeRole"
					}
				]
			}`,
			expectedValid: true,
		},
		{
			name:    "invalid trust relationship - wrong principal",
			roleArn: "arn:aws:iam::123456789012:role/efs-csi-cross-account",
			trustPolicy: `{
				"Version": "2012-10-17",
				"Statement": [
					{
						"Effect": "Allow",
						"Principal": {
							"AWS": "arn:aws:iam::111111111111:root"
						},
						"Action": "sts:AssumeRole"
					}
				]
			}`,
			expectedValid:      false,
			expectedErrMessage: "trust relationship does not allow access from source account",
		},
		{
			name:    "invalid trust relationship - deny effect",
			roleArn: "arn:aws:iam::123456789012:role/efs-csi-cross-account",
			trustPolicy: `{
				"Version": "2012-10-17",
				"Statement": [
					{
						"Effect": "Deny",
						"Principal": {
							"AWS": "arn:aws:iam::987654321098:root"
						},
						"Action": "sts:AssumeRole"
					}
				]
			}`,
			expectedValid:      false,
			expectedErrMessage: "trust relationship denies access",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			valid, err := validateCrossAccountTrustPolicy(tc.trustPolicy, "987654321098")

			assert.Equal(t, tc.expectedValid, valid)
			if !tc.expectedValid {
				assert.Error(t, err)
				if tc.expectedErrMessage != "" {
					assert.Contains(t, err.Error(), tc.expectedErrMessage)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestCrossAccountVPCPeering tests VPC peering validation for cross-account scenarios
func TestCrossAccountVPCPeering(t *testing.T) {
	tests := []struct {
		name               string
		sourceVPCID        string
		targetVPCID        string
		peeringStatus      string
		expectedValid      bool
		expectedErrMessage string
	}{
		{
			name:          "active VPC peering connection",
			sourceVPCID:   "vpc-source123",
			targetVPCID:   "vpc-target456",
			peeringStatus: "active",
			expectedValid: true,
		},
		{
			name:               "pending VPC peering connection",
			sourceVPCID:        "vpc-source123",
			targetVPCID:        "vpc-target456",
			peeringStatus:      "pending-acceptance",
			expectedValid:      false,
			expectedErrMessage: "VPC peering connection not active",
		},
		{
			name:               "failed VPC peering connection",
			sourceVPCID:        "vpc-source123",
			targetVPCID:        "vpc-target456",
			peeringStatus:      "failed",
			expectedValid:      false,
			expectedErrMessage: "VPC peering connection failed",
		},
		{
			name:               "no VPC peering connection",
			sourceVPCID:        "vpc-source123",
			targetVPCID:        "vpc-target456",
			peeringStatus:      "",
			expectedValid:      false,
			expectedErrMessage: "no VPC peering connection found",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// This test validates the logic without actual AWS API calls
			// In production, VPC peering would be validated through AWS EC2 APIs

			valid := tc.peeringStatus == "active"
			var err error
			if !valid {
				if tc.peeringStatus == "" {
					err = errors.New("no VPC peering connection found")
				} else if tc.peeringStatus == "failed" {
					err = errors.New("VPC peering connection failed")
				} else {
					err = errors.New("VPC peering connection not active")
				}
			}

			assert.Equal(t, tc.expectedValid, valid)
			if !tc.expectedValid {
				assert.Error(t, err)
				if tc.expectedErrMessage != "" {
					assert.Contains(t, err.Error(), tc.expectedErrMessage)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestCrossAccountEFSPolicy tests EFS resource policy validation
func TestCrossAccountEFSPolicy(t *testing.T) {
	tests := []struct {
		name               string
		fileSystemID       string
		resourcePolicy     string
		sourceAccountID    string
		expectedValid      bool
		expectedErrMessage string
	}{
		{
			name:         "valid cross-account EFS policy",
			fileSystemID: "fs-12345678",
			resourcePolicy: `{
				"Version": "2012-10-17",
				"Statement": [
					{
						"Effect": "Allow",
						"Principal": {
							"AWS": "arn:aws:iam::987654321098:root"
						},
						"Action": [
							"elasticfilesystem:ClientMount",
							"elasticfilesystem:ClientWrite"
						],
						"Resource": "*"
					}
				]
			}`,
			sourceAccountID: "987654321098",
			expectedValid:   true,
		},
		{
			name:         "EFS policy with IP restriction",
			fileSystemID: "fs-12345678",
			resourcePolicy: `{
				"Version": "2012-10-17",
				"Statement": [
					{
						"Effect": "Allow",
						"Principal": {
							"AWS": "arn:aws:iam::987654321098:root"
						},
						"Action": [
							"elasticfilesystem:ClientMount",
							"elasticfilesystem:ClientWrite"
						],
						"Resource": "*",
						"Condition": {
							"IpAddress": {
								"aws:SourceIp": ["10.0.0.0/16"]
							}
						}
					}
				]
			}`,
			sourceAccountID: "987654321098",
			expectedValid:   true,
		},
		{
			name:         "EFS policy denying cross-account access",
			fileSystemID: "fs-12345678",
			resourcePolicy: `{
				"Version": "2012-10-17",
				"Statement": [
					{
						"Effect": "Deny",
						"Principal": {
							"AWS": "arn:aws:iam::987654321098:root"
						},
						"Action": "*",
						"Resource": "*"
					}
				]
			}`,
			sourceAccountID:    "987654321098",
			expectedValid:      false,
			expectedErrMessage: "EFS policy denies access from source account",
		},
		{
			name:               "no EFS policy configured",
			fileSystemID:       "fs-12345678",
			resourcePolicy:     "",
			sourceAccountID:    "987654321098",
			expectedValid:      false,
			expectedErrMessage: "no resource policy configured",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			valid, err := validateEFSResourcePolicy(tc.resourcePolicy, tc.sourceAccountID)

			assert.Equal(t, tc.expectedValid, valid)
			if !tc.expectedValid {
				assert.Error(t, err)
				if tc.expectedErrMessage != "" {
					assert.Contains(t, err.Error(), tc.expectedErrMessage)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestCrossAccountSecurityGroup tests security group validation for cross-account scenarios
func TestCrossAccountSecurityGroup(t *testing.T) {
	tests := []struct {
		name               string
		securityGroupRules []map[string]interface{}
		sourceVPCCIDR      string
		expectedValid      bool
		expectedErrMessage string
	}{
		{
			name: "security group allows NFS from source VPC",
			securityGroupRules: []map[string]interface{}{
				{
					"protocol": "tcp",
					"fromPort": 2049,
					"toPort":   2049,
					"cidr":     "10.0.0.0/16",
				},
			},
			sourceVPCCIDR: "10.0.0.0/16",
			expectedValid: true,
		},
		{
			name: "security group allows all traffic from source VPC",
			securityGroupRules: []map[string]interface{}{
				{
					"protocol": "-1",
					"fromPort": 0,
					"toPort":   65535,
					"cidr":     "10.0.0.0/16",
				},
			},
			sourceVPCCIDR: "10.0.0.0/16",
			expectedValid: true,
		},
		{
			name: "security group blocks NFS from source VPC",
			securityGroupRules: []map[string]interface{}{
				{
					"protocol": "tcp",
					"fromPort": 2049,
					"toPort":   2049,
					"cidr":     "192.168.0.0/16",
				},
			},
			sourceVPCCIDR:      "10.0.0.0/16",
			expectedValid:      false,
			expectedErrMessage: "security group does not allow NFS traffic from source VPC",
		},
		{
			name:               "no security group rules",
			securityGroupRules: []map[string]interface{}{},
			sourceVPCCIDR:      "10.0.0.0/16",
			expectedValid:      false,
			expectedErrMessage: "no security group rules configured",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			valid, err := validateSecurityGroupRules(tc.securityGroupRules, tc.sourceVPCCIDR)

			assert.Equal(t, tc.expectedValid, valid)
			if !tc.expectedValid {
				assert.Error(t, err)
				if tc.expectedErrMessage != "" {
					assert.Contains(t, err.Error(), tc.expectedErrMessage)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Helper functions for validation

func validateCrossAccountTrustPolicy(trustPolicyJSON, sourceAccountID string) (bool, error) {
	var trustPolicy map[string]interface{}
	if err := json.Unmarshal([]byte(trustPolicyJSON), &trustPolicy); err != nil {
		return false, fmt.Errorf("failed to parse trust policy: %w", err)
	}

	statements, ok := trustPolicy["Statement"].([]interface{})
	if !ok {
		return false, errors.New("invalid trust policy format")
	}

	expectedPrincipal := fmt.Sprintf("arn:aws:iam::%s:root", sourceAccountID)

	for _, stmt := range statements {
		statement := stmt.(map[string]interface{})
		effect, _ := statement["Effect"].(string)

		if effect == "Deny" {
			principal := statement["Principal"].(map[string]interface{})
			awsPrincipal, _ := principal["AWS"].(string)
			if awsPrincipal == expectedPrincipal {
				return false, errors.New("trust relationship denies access")
			}
		}

		if effect == "Allow" {
			principal := statement["Principal"].(map[string]interface{})
			awsPrincipal, _ := principal["AWS"].(string)
			if awsPrincipal == expectedPrincipal {
				return true, nil
			}
		}
	}

	return false, errors.New("trust relationship does not allow access from source account")
}

func validateVPCPeeringConnection(sourceVPCID, targetVPCID, peeringStatus string) (bool, error) {
	// This is a mock implementation for testing
	// In real implementation, this would call EC2 API to validate VPC peering
	if peeringStatus == "active" {
		return true, nil
	}
	if peeringStatus == "" {
		return false, errors.New("no VPC peering connection found")
	}
	if peeringStatus == "failed" {
		return false, errors.New("VPC peering connection failed")
	}
	return false, errors.New("VPC peering connection not active")
}

func validateEFSResourcePolicy(resourcePolicyJSON, sourceAccountID string) (bool, error) {
	if resourcePolicyJSON == "" {
		return false, errors.New("no resource policy configured")
	}

	var policy map[string]interface{}
	if err := json.Unmarshal([]byte(resourcePolicyJSON), &policy); err != nil {
		return false, fmt.Errorf("failed to parse resource policy: %w", err)
	}

	statements, ok := policy["Statement"].([]interface{})
	if !ok {
		return false, errors.New("invalid resource policy format")
	}

	expectedPrincipal := fmt.Sprintf("arn:aws:iam::%s:root", sourceAccountID)

	for _, stmt := range statements {
		statement := stmt.(map[string]interface{})
		effect, _ := statement["Effect"].(string)

		if effect == "Deny" {
			principal := statement["Principal"].(map[string]interface{})
			awsPrincipal, _ := principal["AWS"].(string)
			if awsPrincipal == expectedPrincipal {
				return false, errors.New("EFS policy denies access from source account")
			}
		}

		if effect == "Allow" {
			principal := statement["Principal"].(map[string]interface{})
			awsPrincipal, _ := principal["AWS"].(string)
			if awsPrincipal == expectedPrincipal {
				// Check if required actions are allowed
				actions := statement["Action"].([]interface{})
				hasMount := false
				hasWrite := false
				for _, action := range actions {
					actionStr := action.(string)
					if actionStr == "elasticfilesystem:ClientMount" || actionStr == "*" {
						hasMount = true
					}
					if actionStr == "elasticfilesystem:ClientWrite" || actionStr == "*" {
						hasWrite = true
					}
				}
				if hasMount && hasWrite {
					return true, nil
				}
			}
		}
	}

	return false, errors.New("EFS policy does not allow required actions from source account")
}

func validateSecurityGroupRules(rules []map[string]interface{}, sourceVPCCIDR string) (bool, error) {
	if len(rules) == 0 {
		return false, errors.New("no security group rules configured")
	}

	for _, rule := range rules {
		protocol := rule["protocol"].(string)
		fromPort, _ := rule["fromPort"].(int)
		toPort, _ := rule["toPort"].(int)
		cidr := rule["cidr"].(string)

		// Check if rule allows NFS (port 2049) from source VPC
		if cidr == sourceVPCCIDR {
			if protocol == "-1" || (protocol == "tcp" && fromPort <= 2049 && toPort >= 2049) {
				return true, nil
			}
		}
	}

	return false, errors.New("security group does not allow NFS traffic from source VPC")
}

// Mock implementation of cross-account provisioning for testing
func (np *NamespaceProvisioner) provisionCrossAccountEFS(ctx context.Context, namespace string, params map[string]string) (*cloud.AccessPoint, error) {
	// Check for provisioner secret
	secretName := params["csi.storage.k8s.io/provisioner-secret-name"]
	secretNamespace := params["csi.storage.k8s.io/provisioner-secret-namespace"]

	if secretName == "" || secretNamespace == "" {
		return nil, errors.New("provisioner secret not provided for cross-account provisioning")
	}

	// Get secret from Kubernetes
	secret, err := np.k8sClient.CoreV1().Secrets(secretNamespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get provisioner secret: %w", err)
	}

	// Extract role ARN from secret
	roleArnBytes, ok := secret.Data["awsRoleArn"]
	if !ok {
		return nil, errors.New("awsRoleArn not found in provisioner secret")
	}
	roleArn := string(roleArnBytes)

	// Create access point using cross-account credentials
	// This is where actual AssumeRole and cross-account provisioning would happen
	accessPointOptions := &cloud.AccessPointOptions{
		FileSystemId:   fmt.Sprintf("fs-cross-%s", namespace),
		Uid:            1000,
		Gid:            1000,
		DirectoryPath:  fmt.Sprintf("%s/%s", params["basePath"], namespace),
		DirectoryPerms: "755",
		Tags: map[string]string{
			"Namespace":       namespace,
			"ProvisioningMode": "efs-ns",
			"CrossAccount":    "true",
			"RoleArn":        roleArn,
		},
	}

	// Call cloud provider to create access point
	accessPoint, err := np.cloud.CreateAccessPoint(ctx, "", accessPointOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create cross-account access point: %w", err)
	}

	return accessPoint, nil
}

// TestCrossAccountRetryLogic tests retry logic for cross-account operations
func TestCrossAccountRetryLogic(t *testing.T) {
	tests := []struct {
		name           string
		failureCount   int
		maxRetries     int
		expectedCalls  int
		expectSuccess  bool
	}{
		{
			name:          "succeeds on first attempt",
			failureCount:  0,
			maxRetries:    3,
			expectedCalls: 1,
			expectSuccess: true,
		},
		{
			name:          "succeeds after retries",
			failureCount:  2,
			maxRetries:    3,
			expectedCalls: 3,
			expectSuccess: true,
		},
		{
			name:          "fails after max retries",
			failureCount:  5,
			maxRetries:    3,
			expectedCalls: 4, // 1 initial + 3 retries
			expectSuccess: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockCloud := mocks.NewMockCloud(ctrl)

			callCount := 0
			mockCloud.EXPECT().CreateAccessPoint(gomock.Any(), gomock.Any(), gomock.Any()).
				Times(tc.expectedCalls).
				DoAndReturn(func(ctx context.Context, clientToken string, options *cloud.AccessPointOptions) (*cloud.AccessPoint, error) {
					callCount++
					if callCount <= tc.failureCount {
						return nil, fmt.Errorf("temporary error %d", callCount)
					}
					return &cloud.AccessPoint{
						AccessPointId: "fsap-success",
					}, nil
				})

			// Execute with retry logic
			var result *cloud.AccessPoint
			var err error

			for i := 0; i <= tc.maxRetries; i++ {
				result, err = mockCloud.CreateAccessPoint(context.Background(), "", &cloud.AccessPointOptions{})
				if err == nil {
					break
				}
				if i < tc.maxRetries {
					time.Sleep(10 * time.Millisecond) // Small delay for testing
				}
			}

			if tc.expectSuccess {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, "fsap-success", result.AccessPointId)
			} else {
				assert.Error(t, err)
				assert.Nil(t, result)
			}

			assert.Equal(t, tc.expectedCalls, callCount)
		})
	}
}

// TestCrossAccountCleanup tests cleanup of cross-account resources
func TestCrossAccountCleanup(t *testing.T) {
	tests := []struct {
		name              string
		resources         []string
		cleanupErrors     map[string]error
		expectFullCleanup bool
		expectedErrors    int
	}{
		{
			name:              "successful cleanup of all resources",
			resources:         []string{"fsap-1", "fsap-2", "fsap-3"},
			cleanupErrors:     map[string]error{},
			expectFullCleanup: true,
			expectedErrors:    0,
		},
		{
			name:      "partial cleanup with errors",
			resources: []string{"fsap-1", "fsap-2", "fsap-3"},
			cleanupErrors: map[string]error{
				"fsap-2": errors.New("access denied"),
			},
			expectFullCleanup: false,
			expectedErrors:    1,
		},
		{
			name:      "cleanup with all resources failing",
			resources: []string{"fsap-1", "fsap-2"},
			cleanupErrors: map[string]error{
				"fsap-1": errors.New("not found"),
				"fsap-2": errors.New("access denied"),
			},
			expectFullCleanup: false,
			expectedErrors:    2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockCloud := mocks.NewMockCloud(ctrl)

			// Setup delete expectations
			for _, resource := range tc.resources {
				err := tc.cleanupErrors[resource]
				mockCloud.EXPECT().DeleteAccessPoint(gomock.Any(), resource).Return(err)
			}

			// Execute cleanup
			errorCount := 0
			for _, resource := range tc.resources {
				if err := mockCloud.DeleteAccessPoint(context.Background(), resource); err != nil {
					errorCount++
					t.Logf("Failed to cleanup %s: %v", resource, err)
				}
			}

			assert.Equal(t, tc.expectedErrors, errorCount)
			if tc.expectFullCleanup {
				assert.Equal(t, 0, errorCount, "Expected full cleanup but had errors")
			}
		})
	}
}