/*
Copyright 2019 The Kubernetes Authors.

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
	"fmt"

	"github.com/aws/smithy-go"
	"math/rand"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	"github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"k8s.io/klog/v2"
)

const (
	AccessDeniedException    = "AccessDeniedException"
	AccessPointAlreadyExists = "AccessPointAlreadyExists"
	PvcNameTagKey            = "pvcName"
	AccessPointPerFsLimit    = 1000
)

var (
	ErrNotFound      = errors.New("Resource was not found")
	ErrAlreadyExists = errors.New("Resource already exists")
	ErrAccessDenied  = errors.New("Access denied")
)

type FileSystem struct {
	FileSystemId         string
	LifeCycleState       string
	CreationTime         *time.Time
	PerformanceMode      string
	ThroughputMode       string
	Encrypted            bool
	KmsKeyId             string
	Tags                 map[string]string
}

type AccessPoint struct {
	AccessPointId      string
	FileSystemId       string
	AccessPointRootDir string
	// Capacity is used for testing purpose only
	// EFS does not consider capacity while provisioning new file systems or access points
	CapacityGiB int64
	PosixUser   *PosixUser
}

type PosixUser struct {
	Gid int64
	Uid int64
}

type AccessPointOptions struct {
	// Capacity is used for testing purpose only.
	// EFS does not consider capacity while provisioning new file systems or access points
	// Capacity is used to satisfy this test: https://github.com/kubernetes-csi/csi-test/blob/v3.1.1/pkg/sanity/controller.go#L559
	CapacityGiB    int64
	FileSystemId   string
	Uid            int64
	Gid            int64
	DirectoryPerms string
	DirectoryPath  string
	Tags           map[string]string
}

type MountTarget struct {
	AZName        string
	AZId          string
	MountTargetId string
	IPAddress     string
}

type FileSystemOptions struct {
	PerformanceMode               string
	ThroughputMode               string
	ProvisionedThroughputInMibps int64
	Encrypted                    bool
	KmsKeyId                     string
	LifecyclePolicy              string
	BackupPolicy                 string
	Tags                         map[string]string
}

// Efs abstracts efs client(https://docs.aws.amazon.com/sdk-for-go/api/service/efs/)
type Efs interface {
	CreateAccessPoint(context.Context, *efs.CreateAccessPointInput, ...func(*efs.Options)) (*efs.CreateAccessPointOutput, error)
	DeleteAccessPoint(context.Context, *efs.DeleteAccessPointInput, ...func(*efs.Options)) (*efs.DeleteAccessPointOutput, error)
	DescribeAccessPoints(context.Context, *efs.DescribeAccessPointsInput, ...func(*efs.Options)) (*efs.DescribeAccessPointsOutput, error)
	DescribeFileSystems(context.Context, *efs.DescribeFileSystemsInput, ...func(*efs.Options)) (*efs.DescribeFileSystemsOutput, error)
	DescribeMountTargets(context.Context, *efs.DescribeMountTargetsInput, ...func(*efs.Options)) (*efs.DescribeMountTargetsOutput, error)
	CreateFileSystem(context.Context, *efs.CreateFileSystemInput, ...func(*efs.Options)) (*efs.CreateFileSystemOutput, error)
	CreateMountTarget(context.Context, *efs.CreateMountTargetInput, ...func(*efs.Options)) (*efs.CreateMountTargetOutput, error)
}

type Cloud interface {
	GetMetadata() MetadataService
	CreateAccessPoint(ctx context.Context, clientToken string, accessPointOpts *AccessPointOptions) (accessPoint *AccessPoint, err error)
	DeleteAccessPoint(ctx context.Context, accessPointId string) (err error)
	DescribeAccessPoint(ctx context.Context, accessPointId string) (accessPoint *AccessPoint, err error)
	FindAccessPointByClientToken(ctx context.Context, clientToken, fileSystemId string) (accessPoint *AccessPoint, err error)
	ListAccessPoints(ctx context.Context, fileSystemId string) (accessPoints []*AccessPoint, err error)
	DescribeFileSystem(ctx context.Context, fileSystemId string) (fs *FileSystem, err error)
	DescribeMountTargets(ctx context.Context, fileSystemId, az string) (fs *MountTarget, err error)
	// EFS filesystem creation for namespace provisioning
	CreateFileSystem(ctx context.Context, clientToken string, options *FileSystemOptions) (fs *FileSystem, err error)
	CreateMountTarget(ctx context.Context, fileSystemId, subnetId, securityGroupId string) (mt *MountTarget, err error)
	// EFS filesystem query methods for namespace provisioning
	DescribeFileSystems(ctx context.Context, creationToken string, maxResults int32) (fileSystems []*FileSystem, nextToken string, err error)
	// Tag-based recovery methods for namespace provisioning
	FindFileSystemsByTags(ctx context.Context, tags map[string]string) (fileSystems []*FileSystem, err error)
	GetFileSystemTags(ctx context.Context, fileSystemId string) (tags map[string]string, err error)
}

type cloud struct {
	metadata MetadataService
	efs      Efs
	rm       *retryManager
}

// NewCloud returns a new instance of AWS cloud
// It panics if session is invalid
func NewCloud(adaptiveRetryMode bool) (Cloud, error) {
	return createCloud("", adaptiveRetryMode)
}

// NewCloudWithRole returns a new instance of AWS cloud after assuming an aws role
// It panics if driver does not have permissions to assume role.
func NewCloudWithRole(awsRoleArn string, adaptiveRetryMode bool) (Cloud, error) {
	return createCloud(awsRoleArn, adaptiveRetryMode)
}

func createCloud(awsRoleArn string, adaptiveRetryMode bool) (Cloud, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		klog.Warningf("Could not load config: %v", err)
	}

	svc := imds.NewFromConfig(cfg)
	api, err := DefaultKubernetesAPIClient()

	if err != nil && !isDriverBootedInECS() {
		klog.Warningf("Could not create Kubernetes Client: %v", err)
	}
	metadataProvider, err := GetNewMetadataProvider(svc, api)
	if err != nil {
		return nil, fmt.Errorf("error creating MetadataProvider: %v", err)
	}

	metadata, err := metadataProvider.getMetadata()

	if err != nil {
		return nil, fmt.Errorf("could not get metadata: %v", err)
	}

	rm := newRetryManager(adaptiveRetryMode)

	efs_client := createEfsClient(awsRoleArn, metadata)
	klog.V(5).Infof("EFS Client created using the following endpoint: %+v", cfg.BaseEndpoint)

	return &cloud{
		metadata: metadata,
		efs:      efs_client,
		rm:       rm,
	}, nil
}

func createEfsClient(awsRoleArn string, metadata MetadataService) Efs {
	cfg, _ := config.LoadDefaultConfig(context.TODO(), config.WithRegion(metadata.GetRegion()))
	if awsRoleArn != "" {
		stsClient := sts.NewFromConfig(cfg)
		roleProvider := stscreds.NewAssumeRoleProvider(stsClient, awsRoleArn)
		cfg.Credentials = aws.NewCredentialsCache(roleProvider)
	}
	return efs.NewFromConfig(cfg)
}

func (c *cloud) GetMetadata() MetadataService {
	return c.metadata
}

func (c *cloud) CreateAccessPoint(ctx context.Context, clientToken string, accessPointOpts *AccessPointOptions) (accessPoint *AccessPoint, err error) {
	efsTags := parseEfsTags(accessPointOpts.Tags)
	createAPInput := &efs.CreateAccessPointInput{
		ClientToken:  &clientToken,
		FileSystemId: &accessPointOpts.FileSystemId,
		PosixUser: &types.PosixUser{
			Gid: &accessPointOpts.Gid,
			Uid: &accessPointOpts.Uid,
		},
		RootDirectory: &types.RootDirectory{
			CreationInfo: &types.CreationInfo{
				OwnerGid:    &accessPointOpts.Gid,
				OwnerUid:    &accessPointOpts.Uid,
				Permissions: &accessPointOpts.DirectoryPerms,
			},
			Path: &accessPointOpts.DirectoryPath,
		},
		Tags: efsTags,
	}

	klog.V(5).Infof("Calling Create AP with input: %+v", *createAPInput)
	res, err := c.efs.CreateAccessPoint(ctx, createAPInput, func(o *efs.Options) {
		o.Retryer = c.rm.createAccessPointRetryer
	})
	if err != nil {
		if isAccessDenied(err) {
			return nil, ErrAccessDenied
		}
		if isAccessPointAlreadyExists(err) {
			return nil, ErrAlreadyExists
		}
		return nil, fmt.Errorf("Failed to create access point: %v", err)
	}
	klog.V(5).Infof("Create AP response : %+v", res)

	return &AccessPoint{
		AccessPointId: *res.AccessPointId,
		FileSystemId:  *res.FileSystemId,
		CapacityGiB:   accessPointOpts.CapacityGiB,
	}, nil
}

func (c *cloud) DeleteAccessPoint(ctx context.Context, accessPointId string) (err error) {
	deleteAccessPointInput := &efs.DeleteAccessPointInput{AccessPointId: &accessPointId}
	_, err = c.efs.DeleteAccessPoint(ctx, deleteAccessPointInput, func(o *efs.Options) {
		o.Retryer = c.rm.deleteAccessPointRetryer
	})
	if err != nil {
		if isAccessDenied(err) {
			return ErrAccessDenied
		}
		if isAccessPointNotFound(err) {
			return ErrNotFound
		}
		return fmt.Errorf("Failed to delete access point: %v, error: %v", accessPointId, err)
	}

	return nil
}

func (c *cloud) DescribeAccessPoint(ctx context.Context, accessPointId string) (accessPoint *AccessPoint, err error) {
	describeAPInput := &efs.DescribeAccessPointsInput{
		AccessPointId: &accessPointId,
	}
	res, err := c.efs.DescribeAccessPoints(ctx, describeAPInput, func(o *efs.Options) {
		o.Retryer = c.rm.describeAccessPointsRetryer
	})
	if err != nil {
		if isAccessDenied(err) {
			return nil, ErrAccessDenied
		}
		if isAccessPointNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("Describe Access Point failed: %v", err)
	}

	accessPoints := res.AccessPoints
	if len(accessPoints) == 0 || len(accessPoints) > 1 {
		return nil, fmt.Errorf("DescribeAccessPoint failed. Expected exactly 1 access point in DescribeAccessPoint result. However, recevied %d access points", len(accessPoints))
	}

	return &AccessPoint{
		AccessPointId:      *accessPoints[0].AccessPointId,
		FileSystemId:       *accessPoints[0].FileSystemId,
		AccessPointRootDir: *accessPoints[0].RootDirectory.Path,
	}, nil
}

func (c *cloud) FindAccessPointByClientToken(ctx context.Context, clientToken, fileSystemId string) (accessPoint *AccessPoint, err error) {
	klog.V(5).Infof("Filesystem ID to find AP : %+v", fileSystemId)
	klog.V(2).Infof("ClientToken to find AP : %s", clientToken)
	describeAPInput := &efs.DescribeAccessPointsInput{
		FileSystemId: &fileSystemId,
		MaxResults:   aws.Int32(AccessPointPerFsLimit),
	}
	res, err := c.efs.DescribeAccessPoints(ctx, describeAPInput, func(o *efs.Options) {
		o.Retryer = c.rm.describeAccessPointsRetryer
	})
	if err != nil {
		if isAccessDenied(err) {
			return nil, ErrAccessDenied
		}
		if isFileSystemNotFound(err) {
			return nil, ErrNotFound
		}
		err = fmt.Errorf("failed to list Access Points of efs = %s : %v", fileSystemId, err)
		return
	}
	for _, ap := range res.AccessPoints {
		// check if AP exists with same client token
		if *ap.ClientToken == clientToken {
			return &AccessPoint{
				AccessPointId:      *ap.AccessPointId,
				FileSystemId:       *ap.FileSystemId,
				AccessPointRootDir: *ap.RootDirectory.Path,
				PosixUser: &PosixUser{
					Gid: *ap.PosixUser.Gid,
					Uid: *ap.PosixUser.Uid,
				},
			}, nil
		}
	}
	klog.V(2).Infof("Access point does not exist")
	return nil, nil
}

func (c *cloud) ListAccessPoints(ctx context.Context, fileSystemId string) (accessPoints []*AccessPoint, err error) {
	describeAPInput := &efs.DescribeAccessPointsInput{
		FileSystemId: &fileSystemId,
		MaxResults:   aws.Int32(AccessPointPerFsLimit),
	}
	res, err := c.efs.DescribeAccessPoints(ctx, describeAPInput, func(o *efs.Options) {
		o.Retryer = c.rm.describeAccessPointsRetryer
	})
	if err != nil {
		if isAccessDenied(err) {
			return nil, ErrAccessDenied
		}
		if isFileSystemNotFound(err) {
			return nil, ErrNotFound
		}
		err = fmt.Errorf("List Access Points failed: %v", err)
		return
	}

	var posixUser *PosixUser
	for _, accessPointDescription := range res.AccessPoints {
		if accessPointDescription.PosixUser != nil {
			posixUser = &PosixUser{
				Gid: *accessPointDescription.PosixUser.Gid,
				Uid: *accessPointDescription.PosixUser.Gid,
			}
		} else {
			posixUser = nil
		}
		accessPoint := &AccessPoint{
			AccessPointId: *accessPointDescription.AccessPointId,
			FileSystemId:  *accessPointDescription.FileSystemId,
			PosixUser:     posixUser,
		}
		accessPoints = append(accessPoints, accessPoint)
	}

	return
}

func (c *cloud) CreateFileSystem(ctx context.Context, clientToken string, options *FileSystemOptions) (fs *FileSystem, err error) {
	efsTags := parseEfsTags(options.Tags)
	createFsInput := &efs.CreateFileSystemInput{
		CreationToken: &clientToken,
		Tags:          efsTags,
	}

	// Set performance mode if specified
	if options.PerformanceMode != "" {
		if options.PerformanceMode == "generalPurpose" {
			createFsInput.PerformanceMode = types.PerformanceModeGeneralPurpose
		} else if options.PerformanceMode == "maxIO" {
			createFsInput.PerformanceMode = types.PerformanceModeMaxIo
		}
	}

	// Set throughput mode and provisioned throughput if specified
	if options.ThroughputMode != "" {
		if options.ThroughputMode == "bursting" {
			createFsInput.ThroughputMode = types.ThroughputModeBursting
		} else if options.ThroughputMode == "provisioned" {
			createFsInput.ThroughputMode = types.ThroughputModeProvisioned
			if options.ProvisionedThroughputInMibps > 0 {
				provisionedThroughput := float64(options.ProvisionedThroughputInMibps)
				createFsInput.ProvisionedThroughputInMibps = &provisionedThroughput
			}
		} else if options.ThroughputMode == "elastic" {
			createFsInput.ThroughputMode = types.ThroughputModeElastic
		}
	}

	// Set encryption settings
	if options.Encrypted {
		createFsInput.Encrypted = &options.Encrypted
		if options.KmsKeyId != "" {
			createFsInput.KmsKeyId = &options.KmsKeyId
		}
	}

	klog.V(5).Infof("Calling CreateFileSystem with input: %+v", *createFsInput)
	res, err := c.efs.CreateFileSystem(ctx, createFsInput, func(o *efs.Options) {
		o.Retryer = c.rm.createAccessPointRetryer // Reuse the same retryer for consistency
	})
	if err != nil {
		if isAccessDenied(err) {
			return nil, ErrAccessDenied
		}
		if isFileSystemAlreadyExists(err) {
			return nil, ErrAlreadyExists
		}
		return nil, fmt.Errorf("Failed to create file system: %v", err)
	}
	klog.V(5).Infof("CreateFileSystem response: %+v", res)

	// Convert result to our FileSystem struct
	filesystem := &FileSystem{
		FileSystemId:    *res.FileSystemId,
		LifeCycleState:  string(res.LifeCycleState),
		PerformanceMode: string(res.PerformanceMode),
		ThroughputMode:  string(res.ThroughputMode),
		Encrypted:       res.Encrypted != nil && *res.Encrypted,
	}

	if res.CreationTime != nil {
		filesystem.CreationTime = res.CreationTime
	}
	if res.KmsKeyId != nil {
		filesystem.KmsKeyId = *res.KmsKeyId
	}

	return filesystem, nil
}

func (c *cloud) CreateMountTarget(ctx context.Context, fileSystemId, subnetId, securityGroupId string) (mt *MountTarget, err error) {
	createMtInput := &efs.CreateMountTargetInput{
		FileSystemId: &fileSystemId,
		SubnetId:     &subnetId,
	}

	if securityGroupId != "" {
		createMtInput.SecurityGroups = []string{securityGroupId}
	}

	klog.V(5).Infof("Calling CreateMountTarget with input: %+v", *createMtInput)
	res, err := c.efs.CreateMountTarget(ctx, createMtInput, func(o *efs.Options) {
		o.Retryer = c.rm.createAccessPointRetryer // Reuse the same retryer for consistency
	})
	if err != nil {
		if isAccessDenied(err) {
			return nil, ErrAccessDenied
		}
		if isMountTargetAlreadyExists(err) {
			return nil, ErrAlreadyExists
		}
		return nil, fmt.Errorf("Failed to create mount target: %v", err)
	}
	klog.V(5).Infof("CreateMountTarget response: %+v", res)

	mountTarget := &MountTarget{
		MountTargetId: *res.MountTargetId,
	}

	if res.IpAddress != nil {
		mountTarget.IPAddress = *res.IpAddress
	}
	if res.AvailabilityZoneName != nil {
		mountTarget.AZName = *res.AvailabilityZoneName
	}
	if res.AvailabilityZoneId != nil {
		mountTarget.AZId = *res.AvailabilityZoneId
	}

	return mountTarget, nil
}

func (c *cloud) DescribeFileSystem(ctx context.Context, fileSystemId string) (fs *FileSystem, err error) {
	describeFsInput := &efs.DescribeFileSystemsInput{FileSystemId: &fileSystemId}
	klog.V(5).Infof("Calling DescribeFileSystems with input: %+v", *describeFsInput)
	res, err := c.efs.DescribeFileSystems(ctx, describeFsInput, func(o *efs.Options) {
		o.Retryer = c.rm.describeFileSystemsRetryer
	})
	if err != nil {
		if isAccessDenied(err) {
			return nil, ErrAccessDenied
		}
		if isFileSystemNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("Describe File System failed: %v", err)
	}

	fileSystems := res.FileSystems
	if len(fileSystems) == 0 || len(fileSystems) > 1 {
		return nil, fmt.Errorf("DescribeFileSystem failed. Expected exactly 1 file system in DescribeFileSystem result. However, recevied %d file systems", len(fileSystems))
	}
	return &FileSystem{
		FileSystemId: *res.FileSystems[0].FileSystemId,
	}, nil
}

// DescribeFileSystems lists EFS filesystems with optional filtering by creation token and pagination support
// This method supports listing all filesystems or filtering by creation token for efficient namespace-based queries
func (c *cloud) DescribeFileSystems(ctx context.Context, creationToken string, maxResults int32) ([]*FileSystem, string, error) {
	describeFsInput := &efs.DescribeFileSystemsInput{}

	// Add optional filters
	if creationToken != "" {
		describeFsInput.CreationToken = &creationToken
		klog.V(5).Infof("Filtering filesystems by creation token: %s", creationToken)
	}

	if maxResults > 0 {
		// AWS EFS API has a maximum limit of 100
		if maxResults > 100 {
			maxResults = 100
		}
		describeFsInput.MaxItems = &maxResults
	}

	klog.V(5).Infof("Calling DescribeFileSystems with input: %+v", *describeFsInput)

	res, err := c.efs.DescribeFileSystems(ctx, describeFsInput, func(o *efs.Options) {
		o.Retryer = c.rm.describeFileSystemsRetryer
	})
	if err != nil {
		if isAccessDenied(err) {
			return nil, "", ErrAccessDenied
		}
		return nil, "", fmt.Errorf("failed to describe filesystems: %w", err)
	}

	// Convert AWS EFS types to our FileSystem type
	var fileSystems []*FileSystem
	for _, fs := range res.FileSystems {
		fileSystem := &FileSystem{}

		if fs.FileSystemId != nil {
			fileSystem.FileSystemId = *fs.FileSystemId
		}
		if fs.LifeCycleState != "" {
			fileSystem.LifeCycleState = string(fs.LifeCycleState)
		}
		if fs.CreationTime != nil {
			fileSystem.CreationTime = fs.CreationTime
		}
		if fs.PerformanceMode != "" {
			fileSystem.PerformanceMode = string(fs.PerformanceMode)
		}
		if fs.ThroughputMode != "" {
			fileSystem.ThroughputMode = string(fs.ThroughputMode)
		}
		if fs.Encrypted != nil {
			fileSystem.Encrypted = *fs.Encrypted
		}
		if fs.KmsKeyId != nil {
			fileSystem.KmsKeyId = *fs.KmsKeyId
		}

		// Convert tags to map
		tags := make(map[string]string)
		for _, tag := range fs.Tags {
			if tag.Key != nil && tag.Value != nil {
				tags[*tag.Key] = *tag.Value
			}
		}
		fileSystem.Tags = tags

		fileSystems = append(fileSystems, fileSystem)
	}

	// Extract next token for pagination
	var nextToken string
	if res.NextMarker != nil {
		nextToken = *res.NextMarker
	}

	klog.V(5).Infof("DescribeFileSystems returned %d filesystems", len(fileSystems))
	return fileSystems, nextToken, nil
}

func (c *cloud) DescribeMountTargets(ctx context.Context, fileSystemId, azName string) (fs *MountTarget, err error) {
	describeMtInput := &efs.DescribeMountTargetsInput{FileSystemId: &fileSystemId}
	klog.V(5).Infof("Calling DescribeMountTargets with input: %+v", *describeMtInput)
	res, err := c.efs.DescribeMountTargets(ctx, describeMtInput, func(o *efs.Options) {
		o.Retryer = c.rm.describeMountTargetsRetryer
	})
	if err != nil {
		if isAccessDenied(err) {
			return nil, ErrAccessDenied
		}
		if isFileSystemNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("Describe Mount Targets failed: %v", err)
	}

	mountTargets := res.MountTargets
	if len(mountTargets) == 0 {
		return nil, fmt.Errorf("Cannot find mount targets for file system %v. Please create mount targets for file system.", fileSystemId)
	}

	availableMountTargets := getAvailableMountTargets(mountTargets)

	if len(availableMountTargets) == 0 {
		return nil, fmt.Errorf("No mount target for file system %v is in available state. Please retry in 5 minutes.", fileSystemId)
	}

	var mountTarget *types.MountTargetDescription
	if azName != "" {
		mountTarget = getMountTargetForAz(availableMountTargets, azName)
	}

	// Pick random Mount target from available mount target if azName is not provided.
	// Or if there is no mount target matching azName
	if mountTarget == nil {
		klog.Infof("Picking a random mount target from available mount target")
		rand.Seed(time.Now().Unix())
		mountTarget = &availableMountTargets[rand.Intn(len(availableMountTargets))]
	}

	return &MountTarget{
		AZName:        *mountTarget.AvailabilityZoneName,
		AZId:          *mountTarget.AvailabilityZoneId,
		MountTargetId: *mountTarget.MountTargetId,
		IPAddress:     *mountTarget.IpAddress,
	}, nil
}

func isFileSystemNotFound(err error) bool {
	var FileSystemNotFoundErr *types.FileSystemNotFound
	if errors.As(err, &FileSystemNotFoundErr) {
		return true
	}
	return false
}

func isAccessPointNotFound(err error) bool {
	var AccessPointNotFoundErr *types.AccessPointNotFound
	if errors.As(err, &AccessPointNotFoundErr) {
		return true
	}
	return false
}

func isAccessDenied(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode() == AccessDeniedException {
			return true
		}
	}
	return false
}

func isAccessPointAlreadyExists(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode() == AccessPointAlreadyExists {
			return true
		}
	}
	return false
}

func isFileSystemAlreadyExists(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode() == "FileSystemAlreadyExists" {
			return true
		}
	}
	return false
}

func isMountTargetAlreadyExists(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode() == "MountTargetConflict" {
			return true
		}
	}
	return false
}

func isDriverBootedInECS() bool {
	ecsContainerMetadataUri := os.Getenv(taskMetadataV4EnvName)
	return ecsContainerMetadataUri != ""
}

func parseEfsTags(tagMap map[string]string) []types.Tag {
	efsTags := []types.Tag{}
	for k, v := range tagMap {
		key := k
		value := v
		efsTags = append(efsTags, types.Tag{
			Key:   &key,
			Value: &value,
		})
	}
	return efsTags
}

func getAvailableMountTargets(mountTargets []types.MountTargetDescription) []types.MountTargetDescription {
	availableMountTargets := []types.MountTargetDescription{}
	for _, mt := range mountTargets {
		if mt.LifeCycleState == "available" {
			availableMountTargets = append(availableMountTargets, mt)
		}
	}

	return availableMountTargets
}

func getMountTargetForAz(mountTargets []types.MountTargetDescription, azName string) *types.MountTargetDescription {
	for _, mt := range mountTargets {
		if *mt.AvailabilityZoneName == azName {
			return &mt
		}
	}
	klog.Infof("There is no mount target match %v", azName)
	return nil
}

// FindFileSystemsByTags finds EFS file systems matching the given tags
// This is used for tag-based recovery of namespace-to-EFS mappings
func (c *cloud) FindFileSystemsByTags(ctx context.Context, tags map[string]string) ([]*FileSystem, error) {
	if len(tags) == 0 {
		return nil, fmt.Errorf("at least one tag must be provided for file system search")
	}

	klog.V(4).Infof("Finding file systems by tags: %+v", tags)

	// List all file systems first
	describeFsInput := &efs.DescribeFileSystemsInput{}
	res, err := c.efs.DescribeFileSystems(ctx, describeFsInput, func(o *efs.Options) {
		o.Retryer = c.rm.describeFileSystemsRetryer
	})
	if err != nil {
		if isAccessDenied(err) {
			return nil, ErrAccessDenied
		}
		return nil, fmt.Errorf("failed to list file systems: %w", err)
	}

	var matchedFileSystems []*FileSystem
	for _, fs := range res.FileSystems {
		if fs.FileSystemId == nil {
			continue
		}

		// Check if file system has all required tags
		fsTags, err := c.GetFileSystemTags(ctx, *fs.FileSystemId)
		if err != nil {
			klog.V(2).Infof("Failed to get tags for file system %s: %v", *fs.FileSystemId, err)
			continue
		}

		if hasAllTags(fsTags, tags) {
			matchedFileSystems = append(matchedFileSystems, &FileSystem{
				FileSystemId: *fs.FileSystemId,
			})
			klog.V(4).Infof("Found matching file system: %s", *fs.FileSystemId)
		}
	}

	klog.V(4).Infof("Found %d file systems matching tags", len(matchedFileSystems))
	return matchedFileSystems, nil
}

// GetFileSystemTags retrieves tags for a specific EFS file system
func (c *cloud) GetFileSystemTags(ctx context.Context, fileSystemId string) (map[string]string, error) {
	if fileSystemId == "" {
		return nil, fmt.Errorf("fileSystemId cannot be empty")
	}

	klog.V(5).Infof("Getting tags for file system: %s", fileSystemId)

	// Use DescribeFileSystems to get the file system with tags
	describeFsInput := &efs.DescribeFileSystemsInput{
		FileSystemId: &fileSystemId,
	}
	res, err := c.efs.DescribeFileSystems(ctx, describeFsInput, func(o *efs.Options) {
		o.Retryer = c.rm.describeFileSystemsRetryer
	})
	if err != nil {
		if isAccessDenied(err) {
			return nil, ErrAccessDenied
		}
		if isFileSystemNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to describe file system %s: %w", fileSystemId, err)
	}

	if len(res.FileSystems) == 0 {
		return nil, ErrNotFound
	}
	if len(res.FileSystems) > 1 {
		return nil, fmt.Errorf("expected exactly 1 file system, got %d", len(res.FileSystems))
	}

	// Convert EFS tags to map
	tags := make(map[string]string)
	for _, tag := range res.FileSystems[0].Tags {
		if tag.Key != nil && tag.Value != nil {
			tags[*tag.Key] = *tag.Value
		}
	}

	klog.V(5).Infof("Found %d tags for file system %s", len(tags), fileSystemId)
	return tags, nil
}

// hasAllTags checks if actualTags contains all key-value pairs from requiredTags
func hasAllTags(actualTags, requiredTags map[string]string) bool {
	for requiredKey, requiredValue := range requiredTags {
		actualValue, exists := actualTags[requiredKey]
		if !exists || actualValue != requiredValue {
			return false
		}
	}
	return true
}
