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

package cloud

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	"github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/aws/smithy-go"
	"github.com/golang/mock/gomock"

	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/cloud/mocks"
)

func TestDescribeFileSystems(t *testing.T) {
	mockTime := time.Now()

	testCases := []struct {
		name           string
		creationToken  string
		maxResults     int32
		mockResponse   *efs.DescribeFileSystemsOutput
		mockError      error
		expectedFSCount int
		expectedToken  string
		expectError    bool
		expectedErrType error
	}{
		{
			name:          "Success: List all filesystems",
			creationToken: "",
			maxResults:    10,
			mockResponse: &efs.DescribeFileSystemsOutput{
				FileSystems: []types.FileSystemDescription{
					{
						FileSystemId:     aws.String("fs-12345678"),
						LifeCycleState:   types.LifeCycleStateAvailable,
						CreationTime:     &mockTime,
						PerformanceMode:  types.PerformanceModeGeneralPurpose,
						ThroughputMode:   types.ThroughputModeBursting,
						Encrypted:        aws.Bool(true),
						Tags: []types.Tag{
							{Key: aws.String("Name"), Value: aws.String("test-fs-1")},
							{Key: aws.String("Environment"), Value: aws.String("dev")},
						},
					},
					{
						FileSystemId:     aws.String("fs-87654321"),
						LifeCycleState:   types.LifeCycleStateAvailable,
						CreationTime:     &mockTime,
						PerformanceMode:  types.PerformanceModeMaxIo,
						ThroughputMode:   types.ThroughputModeProvisioned,
						Encrypted:        aws.Bool(false),
						KmsKeyId:         aws.String("arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"),
						Tags: []types.Tag{
							{Key: aws.String("Name"), Value: aws.String("test-fs-2")},
							{Key: aws.String("kubernetes.io/provisioning-mode"), Value: aws.String("efs-ns")},
						},
					},
				},
				NextMarker: aws.String("next-page-token"),
			},
			expectedFSCount: 2,
			expectedToken:  "next-page-token",
			expectError:    false,
		},
		{
			name:          "Success: Filter by creation token",
			creationToken: "efs-ns-default-12345",
			maxResults:    0,
			mockResponse: &efs.DescribeFileSystemsOutput{
				FileSystems: []types.FileSystemDescription{
					{
						FileSystemId:     aws.String("fs-abcdef12"),
						CreationToken:    aws.String("efs-ns-default-12345"),
						LifeCycleState:   types.LifeCycleStateAvailable,
						CreationTime:     &mockTime,
						PerformanceMode:  types.PerformanceModeGeneralPurpose,
						ThroughputMode:   types.ThroughputModeBursting,
						Encrypted:        aws.Bool(true),
						Tags: []types.Tag{
							{Key: aws.String("kubernetes.io/namespace"), Value: aws.String("default")},
							{Key: aws.String("kubernetes.io/provisioning-mode"), Value: aws.String("efs-ns")},
						},
					},
				},
			},
			expectedFSCount: 1,
			expectedToken:  "",
			expectError:    false,
		},
		{
			name:          "Success: Empty result set",
			creationToken: "",
			maxResults:    50,
			mockResponse: &efs.DescribeFileSystemsOutput{
				FileSystems: []types.FileSystemDescription{},
			},
			expectedFSCount: 0,
			expectedToken:  "",
			expectError:    false,
		},
		{
			name:          "Success: Max results capped at 100",
			creationToken: "",
			maxResults:    200, // Should be capped at 100
			mockResponse: &efs.DescribeFileSystemsOutput{
				FileSystems: []types.FileSystemDescription{
					{
						FileSystemId:    aws.String("fs-11111111"),
						LifeCycleState:  types.LifeCycleStateAvailable,
						PerformanceMode: types.PerformanceModeGeneralPurpose,
						ThroughputMode:  types.ThroughputModeBursting,
						Encrypted:       aws.Bool(true),
					},
				},
			},
			expectedFSCount: 1,
			expectedToken:  "",
			expectError:    false,
		},
		{
			name:          "Success: Filesystem with all optional fields",
			creationToken: "",
			maxResults:    10,
			mockResponse: &efs.DescribeFileSystemsOutput{
				FileSystems: []types.FileSystemDescription{
					{
						FileSystemId:     aws.String("fs-complex"),
						LifeCycleState:   types.LifeCycleStateAvailable,
						CreationTime:     &mockTime,
						PerformanceMode:  types.PerformanceModeMaxIo,
						ThroughputMode:   types.ThroughputModeElastic,
						Encrypted:        aws.Bool(true),
						KmsKeyId:         aws.String("arn:aws:kms:us-east-1:123456789012:key/test-key"),
						Tags: []types.Tag{
							{Key: aws.String("Tag1"), Value: aws.String("Value1")},
							{Key: aws.String("Tag2"), Value: aws.String("Value2")},
							{Key: aws.String("Tag3"), Value: aws.String("Value3")},
						},
					},
				},
			},
			expectedFSCount: 1,
			expectedToken:  "",
			expectError:    false,
		},
		{
			name:          "Failure: Access Denied",
			creationToken: "",
			maxResults:    10,
			mockError: &smithy.GenericAPIError{
				Code:    AccessDeniedException,
				Message: "User does not have permission to describe filesystems",
			},
			expectedFSCount: 0,
			expectError:     true,
			expectedErrType: ErrAccessDenied,
		},
		{
			name:          "Failure: Generic AWS error",
			creationToken: "",
			maxResults:    10,
			mockError:     errors.New("AWS service unavailable"),
			expectedFSCount: 0,
			expectError:    true,
		},
		{
			name:          "Success: Handle nil tags",
			creationToken: "",
			maxResults:    10,
			mockResponse: &efs.DescribeFileSystemsOutput{
				FileSystems: []types.FileSystemDescription{
					{
						FileSystemId:    aws.String("fs-notags"),
						LifeCycleState:  types.LifeCycleStateAvailable,
						PerformanceMode: types.PerformanceModeGeneralPurpose,
						ThroughputMode:  types.ThroughputModeBursting,
						Encrypted:       aws.Bool(true),
						Tags:            nil, // No tags
					},
				},
			},
			expectedFSCount: 1,
			expectedToken:  "",
			expectError:    false,
		},
		{
			name:          "Success: Handle partial tag data",
			creationToken: "",
			maxResults:    10,
			mockResponse: &efs.DescribeFileSystemsOutput{
				FileSystems: []types.FileSystemDescription{
					{
						FileSystemId:    aws.String("fs-partial"),
						LifeCycleState:  types.LifeCycleStateAvailable,
						PerformanceMode: types.PerformanceModeGeneralPurpose,
						ThroughputMode:  types.ThroughputModeBursting,
						Encrypted:       aws.Bool(true),
						Tags: []types.Tag{
							{Key: aws.String("ValidTag"), Value: aws.String("ValidValue")},
							{Key: nil, Value: aws.String("OrphanValue")}, // Invalid tag - no key
							{Key: aws.String("OrphanKey"), Value: nil},    // Invalid tag - no value
						},
					},
				},
			},
			expectedFSCount: 1,
			expectedToken:  "",
			expectError:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtl := gomock.NewController(t)
			defer mockCtl.Finish()

			mockEfs := mocks.NewMockEfs(mockCtl)
			c := &cloud{
				efs: mockEfs,
				rm: &retryManager{
					describeFileSystemsRetryer: newAdaptiveRetryer(),
				},
			}

			ctx := context.Background()

			// Set up mock expectations
			if tc.mockError != nil {
				mockEfs.EXPECT().DescribeFileSystems(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(nil, tc.mockError)
			} else {
				mockEfs.EXPECT().DescribeFileSystems(gomock.Eq(ctx), gomock.Any(), gomock.Any()).
					DoAndReturn(func(ctx context.Context, input *efs.DescribeFileSystemsInput, opts ...func(*efs.Options)) (*efs.DescribeFileSystemsOutput, error) {
						// Verify input parameters
						if tc.creationToken != "" && (input.CreationToken == nil || *input.CreationToken != tc.creationToken) {
							t.Errorf("Expected creation token %s, got %v", tc.creationToken, input.CreationToken)
						}

						if tc.maxResults > 0 {
							expectedMax := tc.maxResults
							if expectedMax > 100 {
								expectedMax = 100
							}
							if input.MaxItems == nil || *input.MaxItems != expectedMax {
								t.Errorf("Expected max items %d, got %v", expectedMax, input.MaxItems)
							}
						}

						return tc.mockResponse, nil
					})
			}

			// Execute the method
			fileSystems, nextToken, err := c.DescribeFileSystems(ctx, tc.creationToken, tc.maxResults)

			// Verify results
			if tc.expectError {
				if err == nil {
					t.Fatalf("Expected error but got none")
				}
				if tc.expectedErrType != nil && !errors.Is(err, tc.expectedErrType) {
					t.Fatalf("Expected error type %v, got %v", tc.expectedErrType, err)
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				if len(fileSystems) != tc.expectedFSCount {
					t.Errorf("Expected %d filesystems, got %d", tc.expectedFSCount, len(fileSystems))
				}

				if nextToken != tc.expectedToken {
					t.Errorf("Expected next token '%s', got '%s'", tc.expectedToken, nextToken)
				}

				// Verify filesystem data conversion for successful cases
				if tc.mockResponse != nil && len(tc.mockResponse.FileSystems) > 0 {
					for i, fs := range fileSystems {
						mockFS := tc.mockResponse.FileSystems[i]

						// Verify basic fields
						if mockFS.FileSystemId != nil && fs.FileSystemId != *mockFS.FileSystemId {
							t.Errorf("FileSystem[%d]: expected ID %s, got %s", i, *mockFS.FileSystemId, fs.FileSystemId)
						}

						if mockFS.LifeCycleState != "" && fs.LifeCycleState != string(mockFS.LifeCycleState) {
							t.Errorf("FileSystem[%d]: expected lifecycle state %s, got %s", i, mockFS.LifeCycleState, fs.LifeCycleState)
						}

						if mockFS.Encrypted != nil && fs.Encrypted != *mockFS.Encrypted {
							t.Errorf("FileSystem[%d]: expected encrypted %v, got %v", i, *mockFS.Encrypted, fs.Encrypted)
						}

						// Verify tags conversion
						expectedTagCount := 0
						for _, tag := range mockFS.Tags {
							if tag.Key != nil && tag.Value != nil {
								expectedTagCount++
								if tagValue, exists := fs.Tags[*tag.Key]; !exists || tagValue != *tag.Value {
									t.Errorf("FileSystem[%d]: expected tag %s=%s, got %s", i, *tag.Key, *tag.Value, tagValue)
								}
							}
						}

						if len(fs.Tags) != expectedTagCount {
							t.Errorf("FileSystem[%d]: expected %d valid tags, got %d", i, expectedTagCount, len(fs.Tags))
						}
					}
				}
			}
		})
	}
}

// TestDescribeFileSystemsPagination tests pagination handling
func TestDescribeFileSystemsPagination(t *testing.T) {
	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()

	mockEfs := mocks.NewMockEfs(mockCtl)
	c := &cloud{
		efs: mockEfs,
		rm: &retryManager{
			describeFileSystemsRetryer: newAdaptiveRetryer(),
		},
	}

	ctx := context.Background()

	// First page
	firstPageToken := "page-2-token"
	mockEfs.EXPECT().DescribeFileSystems(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(
		&efs.DescribeFileSystemsOutput{
			FileSystems: []types.FileSystemDescription{
				{FileSystemId: aws.String("fs-page1-1")},
				{FileSystemId: aws.String("fs-page1-2")},
			},
			NextMarker: &firstPageToken,
		}, nil,
	)

	fileSystems, nextToken, err := c.DescribeFileSystems(ctx, "", 2)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(fileSystems) != 2 {
		t.Errorf("Expected 2 filesystems, got %d", len(fileSystems))
	}
	if nextToken != firstPageToken {
		t.Errorf("Expected next token '%s', got '%s'", firstPageToken, nextToken)
	}
}

// TestDescribeFileSystemsConcurrency tests concurrent access to DescribeFileSystems
func TestDescribeFileSystemsConcurrency(t *testing.T) {
	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()

	mockEfs := mocks.NewMockEfs(mockCtl)
	c := &cloud{
		efs: mockEfs,
		rm: &retryManager{
			describeFileSystemsRetryer: newAdaptiveRetryer(),
		},
	}

	ctx := context.Background()

	// Set up expectations for concurrent calls
	mockEfs.EXPECT().DescribeFileSystems(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		&efs.DescribeFileSystemsOutput{
			FileSystems: []types.FileSystemDescription{
				{FileSystemId: aws.String("fs-concurrent")},
			},
		}, nil,
	).AnyTimes()

	// Run concurrent calls
	concurrency := 10
	done := make(chan bool, concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer func() { done <- true }()

			_, _, err := c.DescribeFileSystems(ctx, "", 10)
			if err != nil {
				t.Errorf("Concurrent call failed: %v", err)
			}
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < concurrency; i++ {
		<-done
	}
}