package driver

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/mock/gomock"

	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/cloud"
	efsns "github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/driver/efs-ns"
	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/driver/mocks"
)

func TestCreateVolume(t *testing.T) {
	var (
		endpoint            = "endpoint"
		volumeName          = "volumeName"
		fsId                = "fs-abcd1234"
		apId                = "***REMOVED_SECRET***xyz987"
		volumeId            = "fs-abcd1234::***REMOVED_SECRET***xyz987"
		capacityRange int64 = 5368709120
		stdVolCap           = &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		}
	)
	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "Success: Using fixed UID/GID",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
						BasePath:         "test",
						Uid:              "1000",
						Gid:              "1001",
					},
				}

				ctx := context.Background()
				fileSystem := &cloud.FileSystem{
					FileSystemId: fsId,
				}
				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
				}
				mockCloud.EXPECT().DescribeFileSystem(gomock.Eq(ctx), gomock.Any()).Return(fileSystem, nil)
				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Eq(volumeName), gomock.Any()).Return(accessPoint, nil).
					Do(func(ctx context.Context, clientToken string, accessPointsOptions *cloud.AccessPointOptions) {
						if accessPointsOptions.Uid != 1000 {
							t.Fatalf("Uid mismatched. Expected: %v, actual: %v", 1000, accessPointsOptions.Uid)
						}
						if accessPointsOptions.Gid != 1001 {
							t.Fatalf("Gid mismatched. Expected: %v, actual: %v", 1001, accessPointsOptions.Gid)
						}
					})

				res, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if res.Volume.VolumeId != volumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", volumeId, res.Volume.VolumeId)
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Success: Using fixed UID/GID and GID range",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
						BasePath:         "test",
						GidMin:           "5000",
						GidMax:           "10000",
						Uid:              "1000",
						Gid:              "1001",
					},
				}

				ctx := context.Background()
				fileSystem := &cloud.FileSystem{
					FileSystemId: fsId,
				}
				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
				}
				mockCloud.EXPECT().DescribeFileSystem(gomock.Eq(ctx), gomock.Any()).Return(fileSystem, nil)
				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(accessPoint, nil).
					Do(func(ctx context.Context, clientToken string, accessPointOpts *cloud.AccessPointOptions) {
						if accessPointOpts.Uid != 1000 {
							t.Fatalf("Uid mismatched. Expected: %v, actual: %v", 1000, accessPointOpts.Uid)
						}
						if accessPointOpts.Gid != 1001 {
							t.Fatalf("Gid mismatched. Expected: %v, actual: %v", 1001, accessPointOpts.Uid)
						}
					})

				res, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if res.Volume.VolumeId != volumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", volumeId, res.Volume.VolumeId)
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Success: avoiding GID collision",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
						BasePath:         "test",
						GidMin:           "1001",
						GidMax:           "1003",
					},
				}

				ctx := context.Background()
				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
				}
				accessPoints := []*cloud.AccessPoint{
					{
						AccessPointId: apId,
						FileSystemId:  fsId,
						PosixUser: &cloud.PosixUser{
							Gid: 1001,
							Uid: 1001,
						},
					},
					{
						AccessPointId: apId,
						FileSystemId:  fsId,
						PosixUser: &cloud.PosixUser{
							Gid: 1002,
							Uid: 1002,
						},
					},
				}

				var expectedGid int64 = 1003 //1001 and 1002 are taken, next available is 1003
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(accessPoints, nil)
				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(accessPoint, nil).
					Do(func(ctx context.Context, clientToken string, accessPointOpts *cloud.AccessPointOptions) {
						if accessPointOpts.Uid != expectedGid {
							t.Fatalf("Uid mismatched. Expected: %v, actual: %v", expectedGid, accessPointOpts.Uid)
						}
						if accessPointOpts.Gid != expectedGid {
							t.Fatalf("Gid mismatched. Expected: %v, actual: %v", expectedGid, accessPointOpts.Gid)
						}
					})

				res, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if res.Volume.VolumeId != volumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", volumeId, res.Volume.VolumeId)
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Success: reuse released GID",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
						BasePath:         "test",
						GidMin:           "1001",
						GidMax:           "1005",
					},
				}

				ctx := context.Background()
				ap1 := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
					PosixUser: &cloud.PosixUser{
						Gid: 1001,
						Uid: 1001,
					},
				}
				ap2 := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
					PosixUser: &cloud.PosixUser{
						Gid: 1002,
						Uid: 1002,
					},
				}
				ap3 := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
					PosixUser: &cloud.PosixUser{
						Gid: 1003,
						Uid: 1003,
					},
				}
				ap4 := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
					PosixUser: &cloud.PosixUser{
						Gid: 1004,
						Uid: 1004,
					},
				}

				// Let allocator jump over some GIDS.
				accessPoints := []*cloud.AccessPoint{ap1, ap2, ap3}
				var expectedGid int64 = 1004 // 1001-1003 is taken.

				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(accessPoints, nil)
				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(ap2, nil).
					Do(func(ctx context.Context, clientToken string, accessPointOpts *cloud.AccessPointOptions) {
						if accessPointOpts.Uid != expectedGid {
							t.Fatalf("Uid mismatched. Expected: %v, actual: %v", expectedGid, accessPointOpts.Uid)
						}
						if accessPointOpts.Gid != expectedGid {
							t.Fatalf("Gid mismatched. Expected: %v, actual: %v", expectedGid, accessPointOpts.Gid)
						}
					})

				res, err := driver.CreateVolume(ctx, req)

				// 2. Simulate access point removal and verify their GIDs returned to allocator.
				accessPoints = []*cloud.AccessPoint{}
				expectedGid = 1001 // 1001 is now free and lowest possible, if no GID return would happen allocator would pick 1005.

				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(accessPoints, nil)
				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(ap3, nil).
					Do(func(ctx context.Context, clientToken string, accessPointOpts *cloud.AccessPointOptions) {
						if accessPointOpts.Uid != expectedGid {
							t.Fatalf("Uid mismatched. Expected: %v, actual: %v", expectedGid, accessPointOpts.Uid)
						}
						if accessPointOpts.Gid != expectedGid {
							t.Fatalf("Gid mismatched. Expected: %v, actual: %v", expectedGid, accessPointOpts.Gid)
						}
					})

				res, err = driver.CreateVolume(ctx, req)

				// 3. Simulate access point ID gap and verify their GIDs returned to allocator
				accessPoints = []*cloud.AccessPoint{ap1, ap4}
				expectedGid = 1002 // 1001 and 1004 are now taken, lowest available is 1002

				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(accessPoints, nil)
				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(ap2, nil).
					Do(func(ctx context.Context, clientToken string, accessPointOpts *cloud.AccessPointOptions) {
						if accessPointOpts.Uid != expectedGid {
							t.Fatalf("Uid mismatched. Expected: %v, actual: %v", expectedGid, accessPointOpts.Uid)
						}
						if accessPointOpts.Gid != expectedGid {
							t.Fatalf("Gid mismatched. Expected: %v, actual: %v", expectedGid, accessPointOpts.Gid)
						}
					})

				res, err = driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if res.Volume.VolumeId != volumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", volumeId, res.Volume.VolumeId)
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Success: EFS access point limit",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
						BasePath:         "test",
						GidMin:           "1000",
						GidMax:           "2000",
					},
				}

				ctx := context.Background()

				accessPoints := []*cloud.AccessPoint{}
				for i := int64(0); i < cloud.AccessPointPerFsLimit; i++ {
					gidMin, err := strconv.ParseInt(req.Parameters[GidMin], 10, 64)
					if err != nil {
						t.Fatalf("Failed to convert GidMax Parameter to int.")
					}
					userGid := gidMin + i
					ap := &cloud.AccessPoint{
						AccessPointId: apId,
						FileSystemId:  fsId,
						PosixUser: &cloud.PosixUser{
							Gid: userGid,
							Uid: userGid,
						},
					}
					accessPoints = append(accessPoints, ap)
				}

				lastAccessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
					PosixUser: &cloud.PosixUser{
						Gid: 2000,
						Uid: 2000,
					},
				}

				expectedGid := 2000
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(accessPoints, nil)
				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(lastAccessPoint, nil).
					Do(func(ctx context.Context, clientToken string, accessPointOpts *cloud.AccessPointOptions) {
						if accessPointOpts.Uid != int64(expectedGid) {
							t.Fatalf("Uid mismatched. Expected: %v, actual: %v", expectedGid, accessPointOpts.Uid)
						}
						if accessPointOpts.Gid != int64(expectedGid) {
							t.Fatalf("Gid mismatched. Expected: %v, actual: %v", expectedGid, accessPointOpts.Gid)
						}
					})

				var err error

				// Allocate last available GID
				_, err = driver.CreateVolume(ctx, req)
				if err != nil {
					t.Fatalf("CreateVolume failed unexpectedly: %v", err)
				}

				accessPoints = append(accessPoints, lastAccessPoint)
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(accessPoints, nil)

				// All 1000 GIDs are taken now, internal limit should take effect causing CreateVolume to fail.
				_, err = driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatalf("CreateVolume should have failed.")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Success: GID range exceeds EFS access point limit",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
						BasePath:         "test",
						GidMin:           "1000",
						GidMax:           "1000000",
					},
				}

				ctx := context.Background()

				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
				}

				expectedGid := 1000 // Allocator should pick lowest available GID
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(nil, nil)
				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(accessPoint, nil).
					Do(func(ctx context.Context, clientToken string, accessPointOpts *cloud.AccessPointOptions) {
						if accessPointOpts.Uid != int64(expectedGid) {
							t.Fatalf("Uid mismatched. Expected: %v, actual: %v", expectedGid, accessPointOpts.Uid)
						}
						if accessPointOpts.Gid != int64(expectedGid) {
							t.Fatalf("Gid mismatched. Expected: %v, actual: %v", expectedGid, accessPointOpts.Gid)
						}
					})

				var err error

				_, err = driver.CreateVolume(ctx, req)
				if err != nil {
					t.Fatalf("CreateVolume failed unexpectedly: %v", err)
				}

				mockCtl.Finish()
			},
		},
		{
			name: "Success: Normal flow",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
					tags:         parseTagsFromStr(""),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						DirectoryPerms:   "777",
						AzName:           "us-east-1a",
					},
				}

				ctx := context.Background()
				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
					PosixUser: &cloud.PosixUser{
						Gid: 1000,
						Uid: 1000,
					},
				}
				accessPoints := []*cloud.AccessPoint{accessPoint}
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(accessPoints, nil)
				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Eq(volumeName), gomock.Any()).Return(accessPoint, nil)

				res, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if res.Volume.VolumeId != volumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", volumeId, res.Volume.VolumeId)
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Success: Race Normal flow",
			testFunc: func(t *testing.T) {
				numGoRoutines := 100
				rand.Seed(time.Now().UnixNano())
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
					tags:         parseTagsFromStr(""),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						DirectoryPerms:   "777",
						AzName:           "us-east-1a",
					},
				}

				ctx := context.Background()
				// Generate random access points so we can return a new one every time
				accessPointArr := make([]*cloud.AccessPoint, numGoRoutines)
				for i := range accessPointArr {
					accessPointArr[i] = &cloud.AccessPoint{
						AccessPointId: fmt.Sprintf("fsap-%s", randStringBytes(12)),
						FileSystemId:  fsId,
						PosixUser: &cloud.PosixUser{
							Gid: 1000,
							Uid: 1000,
						},
					}
				}

				// Don't need to return any access points, but don't return any errors so the filesystems shows up as found
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(nil, nil).Times(numGoRoutines)

				// Return a different generated access point every time this function is called
				var createCounter int32 = 0
				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Eq(volumeName), gomock.Any()).DoAndReturn(
					func(_ context.Context, _ interface{}, _ interface{}) (*cloud.AccessPoint, error) {
						current := atomic.AddInt32(&createCounter, 1) - 1
						return accessPointArr[current], nil
					},
				).Times(numGoRoutines)

				// Lock the volume mutex to hold threads until they are all scheduled
				for _, ap := range accessPointArr {
					driver.lockManager.lockMutex(ap.AccessPointId)
				}

				var wg sync.WaitGroup
				resultChan := make(chan struct {
					index int
					resp  *csi.CreateVolumeResponse
					err   error
				}, numGoRoutines)

				for i := 0; i < numGoRoutines; i++ {
					wg.Add(1)
					go func(index int) {
						defer wg.Done()
						resp, err := driver.CreateVolume(ctx, req)
						resultChan <- struct {
							index int
							resp  *csi.CreateVolumeResponse
							err   error
						}{index, resp, err}
					}(i)
				}

				// Unlock the mutex to force a race
				for _, ap := range accessPointArr {
					driver.lockManager.unlockMutex(ap.AccessPointId)
				}

				go func() {
					wg.Wait()
					close(resultChan)
				}()

				for result := range resultChan {
					if result.err != nil {
						t.Fatalf("CreateVolume failed: %v", result.err)
					}

					if result.resp.Volume == nil {
						t.Fatal("Volume is nil")
					}

					found := false
					for _, ap := range accessPointArr {
						if result.resp.Volume.VolumeId == fmt.Sprintf("%s::%s", ap.FileSystemId, ap.AccessPointId) {
							found = true
							break
						}
					}

					if !found {
						t.Fatalf("Volume Id %v was not found in the access point array", result.resp.Volume.VolumeId)
					}
				}

				// Ensure all keys were properly deleted from the lock manager
				keys, _ := driver.lockManager.GetLockCount()
				if keys > 0 {
					t.Fatalf("%d Keys are still in the lockManager", keys)
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Success: Using Default GID ranges",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
						BasePath:         "test",
					},
				}

				ctx := context.Background()
				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
					PosixUser: &cloud.PosixUser{
						Gid: DefaultGidMin - 1, //use GID that is not in default range
					},
				}
				accessPoints := []*cloud.AccessPoint{accessPoint}
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(accessPoints, nil)
				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(accessPoint, nil)

				res, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if res.Volume.VolumeId != volumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", volumeId, res.Volume.VolumeId)
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Success: Normal flow with tags",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
					tags:         parseTagsFromStr("cluster:efs"),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						DirectoryPerms:   "777",
					},
				}

				ctx := context.Background()
				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
					PosixUser: &cloud.PosixUser{
						Gid: 1000,
						Uid: 1000,
					},
				}
				accessPoints := []*cloud.AccessPoint{accessPoint}
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(accessPoints, nil)
				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(accessPoint, nil)

				res, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if res.Volume.VolumeId != volumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", volumeId, res.Volume.VolumeId)
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Success: Normal flow with invalid tags",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
					tags:         parseTagsFromStr("cluster-efs"),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						DirectoryPerms:   "777",
					},
				}

				ctx := context.Background()
				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
					PosixUser: &cloud.PosixUser{
						Gid: 1000,
						Uid: 1000,
					},
				}
				accessPoints := []*cloud.AccessPoint{accessPoint}
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(accessPoints, nil)
				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(accessPoint, nil)

				res, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if res.Volume.VolumeId != volumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", volumeId, res.Volume.VolumeId)
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Success: reuseAccessPointName is true",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
					tags:         parseTagsFromStr(""),
				}
				pvcNameVal := "test-pvc"

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode:    "efs-ap",
						FsId:                fsId,
						GidMin:              "1000",
						GidMax:              "2000",
						DirectoryPerms:      "777",
						AzName:              "us-east-1a",
						ReuseAccessPointKey: "true",
						PvcNameKey:          pvcNameVal,
					},
				}

				ctx := context.Background()

				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
					PosixUser: &cloud.PosixUser{
						Gid: 1000,
						Uid: 1000,
					},
				}
				mockCloud.EXPECT().FindAccessPointByClientToken(gomock.Eq(ctx), gomock.Any(), gomock.Eq(fsId)).Return(accessPoint, nil)

				res, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if res.Volume.VolumeId != volumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", volumeId, res.Volume.VolumeId)
				}

				mockCtl.Finish()
			},
		},
		{
			name: "Success: reuseAccessPointName is true with existing access point not found",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
					tags:         parseTagsFromStr(""),
				}
				pvcNameVal := "test-pvc"

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode:    "efs-ap",
						FsId:                fsId,
						GidMin:              "1000",
						GidMax:              "2000",
						DirectoryPerms:      "777",
						AzName:              "us-east-1a",
						ReuseAccessPointKey: "true",
						PvcNameKey:          pvcNameVal,
					},
				}

				ctx := context.Background()

				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
					PosixUser: &cloud.PosixUser{
						Gid: 1000,
						Uid: 1000,
					},
				}
				mockCloud.EXPECT().FindAccessPointByClientToken(gomock.Eq(ctx), gomock.Any(), gomock.Eq(fsId)).Return(nil, nil)
				// When createVolume can't find existing access point name, it should create a new one
				accessPoints := []*cloud.AccessPoint{accessPoint}
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(accessPoints, nil)
				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(accessPoint, nil)

				res, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if res.Volume.VolumeId != volumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", volumeId, res.Volume.VolumeId)
				}

				mockCtl.Finish()
			},
		},
		{
			name: "Success: Race with reuseAccessPointName is true",
			testFunc: func(t *testing.T) {
				const numGoRoutines = 100
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
					tags:         parseTagsFromStr(""),
				}
				pvcNameVal := "test-pvc"

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode:    "efs-ap",
						FsId:                fsId,
						GidMin:              "1000",
						GidMax:              "2000",
						DirectoryPerms:      "777",
						AzName:              "us-east-1a",
						ReuseAccessPointKey: "true",
						PvcNameKey:          pvcNameVal,
					},
				}

				ctx := context.Background()

				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
					PosixUser: &cloud.PosixUser{
						Gid: 1000,
						Uid: 1000,
					},
				}
				mockCloud.EXPECT().FindAccessPointByClientToken(gomock.Eq(ctx), gomock.Any(), gomock.Eq(fsId)).Return(accessPoint, nil).Times(numGoRoutines)

				// Lock the volume mutex to hold threads until they are all scheduled
				driver.lockManager.lockMutex(apId)

				var wg sync.WaitGroup
				resultChan := make(chan struct {
					index int
					resp  *csi.CreateVolumeResponse
					err   error
				}, numGoRoutines)

				for i := 0; i < numGoRoutines; i++ {
					wg.Add(1)
					go func(index int) {
						defer wg.Done()
						resp, err := driver.CreateVolume(ctx, req)
						resultChan <- struct {
							index int
							resp  *csi.CreateVolumeResponse
							err   error
						}{index, resp, err}
					}(i)
				}

				// Unlock the mutex to force a race
				driver.lockManager.unlockMutex(apId)

				go func() {
					wg.Wait()
					close(resultChan)
				}()

				for result := range resultChan {
					if result.err != nil {
						t.Fatalf("CreateVolume failed: %v", result.err)
					}

					if result.resp.Volume == nil {
						t.Fatal("Volume is nil")
					}

					if result.resp.Volume.VolumeId != volumeId {
						t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", volumeId, result.resp.Volume.VolumeId)
					}
				}

				// Ensure all keys were properly deleted from the lock manager
				keys, _ := driver.lockManager.GetLockCount()
				if keys > 0 {
					t.Fatalf("%d Keys are still in the lockManager", keys)
				}

				mockCtl.Finish()
			},
		},
		{
			name: "Fail: create volume mutex timeout",
			testFunc: func(t *testing.T) {
				const timeout = 3 * time.Second
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
					tags:         parseTagsFromStr(""),
				}
				pvcNameVal := "test-pvc"

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode:    "efs-ap",
						FsId:                fsId,
						GidMin:              "1000",
						GidMax:              "2000",
						DirectoryPerms:      "777",
						AzName:              "us-east-1a",
						ReuseAccessPointKey: "true",
						PvcNameKey:          pvcNameVal,
					},
				}

				ctx := context.Background()

				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
					PosixUser: &cloud.PosixUser{
						Gid: 1000,
						Uid: 1000,
					},
				}
				mockCloud.EXPECT().FindAccessPointByClientToken(gomock.Eq(ctx), gomock.Any(), gomock.Eq(fsId)).Return(accessPoint, nil).Times(1)

				// Lock the volume mutex to hold threads to force a timeout
				driver.lockManager.lockMutex(apId)

				start := time.Now()
				resp, err := driver.CreateVolume(ctx, req)
				elapsed := time.Since(start)

				// Check that it waited for at least the timeout duration
				if elapsed < timeout {
					t.Errorf("lockMutex returned before timeout. Expected to wait %v, but waited %v", timeout, elapsed)
				}

				// Check that it didn't wait too much longer than the timeout
				// We'll allow a 100ms buffer for system scheduling variations
				if elapsed > timeout+100*time.Millisecond {
					t.Errorf("lockMutex waited too long. Expected to wait %v, but waited %v", timeout, elapsed)
				}

				if err == nil {
					t.Fatalf("CreateVolume should have failed")
				}

				if resp != nil {
					t.Fatal("Response should have been nil")
				}

				driver.lockManager.unlockMutex(apId)

				// Ensure all keys were properly deleted from the lock manager even with a timeout
				keys, _ := driver.lockManager.GetLockCount()
				if keys > 0 {
					t.Fatalf("%d Keys are still in the lockManager", keys)
				}

				mockCtl.Finish()
			},
		},
		{
			name: "Success: Normal flow with a valid directory structure set",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
					tags:         parseTagsFromStr(""),
				}

				pvName := "foo"
				pvcName := "bar"
				directoryCreated := fmt.Sprintf("/%s/%s", pvName, pvcName)

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						DirectoryPerms:   "777",
						SubPathPattern:   "${.PV.name}/${.PVC.name}",
						PvName:           pvName,
						PvcName:          pvcName,
					},
				}

				ctx := context.Background()
				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
				}
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(nil, nil)

				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(accessPoint, nil).
					Do(func(ctx context.Context, clientToken string, accessPointOpts *cloud.AccessPointOptions) {
						if !verifyPathWhenUUIDIncluded(accessPointOpts.DirectoryPath, directoryCreated) {
							t.Fatalf("Root directory mismatch. Expected: %v (with UID appended), actual: %v",
								directoryCreated,
								accessPointOpts.DirectoryPath)
						}
					})

				res, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if res.Volume.VolumeId != volumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", volumeId, res.Volume.VolumeId)
				}

				mockCtl.Finish()
			},
		},
		{
			name: "Success: Normal flow with a valid directory structure set, using a single element",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
					tags:         parseTagsFromStr(""),
				}

				pvcName := "foo"
				directoryCreated := fmt.Sprintf("/%s", pvcName)

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						DirectoryPerms:   "777",
						SubPathPattern:   "${.PVC.name}",
						PvcName:          pvcName,
					},
				}

				ctx := context.Background()
				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
				}
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(nil, nil)

				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(accessPoint, nil).
					Do(func(ctx context.Context, clientToken string, accessPointOpts *cloud.AccessPointOptions) {
						if !verifyPathWhenUUIDIncluded(accessPointOpts.DirectoryPath, directoryCreated) {
							t.Fatalf("Root directory mismatch. Expected: %v (with UID appended), actual: %v",
								directoryCreated,
								accessPointOpts.DirectoryPath)
						}
					})

				res, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if res.Volume.VolumeId != volumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", volumeId, res.Volume.VolumeId)
				}

				mockCtl.Finish()
			},
		},
		{
			name: "Success: Normal flow with a valid directory structure set, and a basePath",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
					tags:         parseTagsFromStr(""),
				}

				pvcName := "foo"
				basePath := "bash"
				directoryCreated := fmt.Sprintf("/%s/%s", basePath, pvcName)

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode:      "efs-ap",
						FsId:                  fsId,
						GidMin:                "1000",
						GidMax:                "2000",
						DirectoryPerms:        "777",
						SubPathPattern:        "${.PVC.name}",
						BasePath:              basePath,
						EnsureUniqueDirectory: "true",
						PvcName:               pvcName,
					},
				}

				ctx := context.Background()
				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
				}
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(nil, nil)

				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(accessPoint, nil).
					Do(func(ctx context.Context, clientToken string, accessPointOpts *cloud.AccessPointOptions) {
						if !verifyPathWhenUUIDIncluded(accessPointOpts.DirectoryPath, directoryCreated) {
							t.Fatalf("Root directory mismatch. Expected: %v (with UID appended), actual: %v",
								directoryCreated,
								accessPointOpts.DirectoryPath)
						}
					})

				res, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if res.Volume.VolumeId != volumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", volumeId, res.Volume.VolumeId)
				}

				mockCtl.Finish()
			},
		},
		{
			name: "Success: Normal flow with a valid directory structure set, and a basePath, and uniqueness guarantees turned off",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
					tags:         parseTagsFromStr(""),
				}

				pvcName := "foo"
				basePath := "bash"
				directoryCreated := fmt.Sprintf("/%s/%s", basePath, pvcName)

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode:      "efs-ap",
						FsId:                  fsId,
						GidMin:                "1000",
						GidMax:                "2000",
						DirectoryPerms:        "777",
						SubPathPattern:        "${.PVC.name}",
						BasePath:              basePath,
						EnsureUniqueDirectory: "false",
						PvcName:               pvcName,
					},
				}

				ctx := context.Background()
				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
				}
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(nil, nil)

				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(accessPoint, nil).
					Do(func(ctx context.Context, clientToken string, accessPointOpts *cloud.AccessPointOptions) {
						if accessPointOpts.DirectoryPath != directoryCreated {
							t.Fatalf("Root directory mismatch. Expected: %v, actual: %v",
								directoryCreated,
								accessPointOpts.DirectoryPath)
						}
					})

				res, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if res.Volume.VolumeId != volumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", volumeId, res.Volume.VolumeId)
				}

				mockCtl.Finish()
			},
		},
		{
			name: "Success: Normal flow with a valid directory structure set, but ensuring uniqueness is set incorrectly, so default of true is used." +
				"",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
					tags:         parseTagsFromStr(""),
				}

				pvcName := "foo"
				directoryCreated := fmt.Sprintf("/%s", pvcName)

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode:      "efs-ap",
						FsId:                  fsId,
						GidMin:                "1000",
						GidMax:                "2000",
						DirectoryPerms:        "777",
						SubPathPattern:        "${.PVC.name}",
						EnsureUniqueDirectory: "banana",
						PvcName:               pvcName,
					},
				}

				ctx := context.Background()
				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
				}
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(nil, nil)

				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(accessPoint, nil).
					Do(func(ctx context.Context, clientToken string, accessPointOpts *cloud.AccessPointOptions) {
						if !verifyPathWhenUUIDIncluded(accessPointOpts.DirectoryPath, directoryCreated) {
							t.Fatalf("Root directory mismatch. Expected: %v (with UID appended), actual: %v",
								directoryCreated,
								accessPointOpts.DirectoryPath)
						}
					})

				res, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if res.Volume.VolumeId != volumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", volumeId, res.Volume.VolumeId)
				}

				mockCtl.Finish()
			},
		},
		{
			name: "Success: Normal flow with an empty subPath Pattern, no basePath and uniqueness guarantees turned off",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
					tags:         parseTagsFromStr(""),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode:      "efs-ap",
						FsId:                  fsId,
						GidMin:                "1000",
						GidMax:                "2000",
						DirectoryPerms:        "777",
						SubPathPattern:        "",
						EnsureUniqueDirectory: "false",
					},
				}

				ctx := context.Background()
				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
				}
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(nil, nil)

				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(accessPoint, nil).
					Do(func(ctx context.Context, clientToken string, accessPointOpts *cloud.AccessPointOptions) {
						if accessPointOpts.DirectoryPath != "/" {
							t.Fatalf("Root directory mismatch. Expected: %v, actual: %v",
								"/",
								accessPointOpts.DirectoryPath)
						}
					})

				res, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if res.Volume.VolumeId != volumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", volumeId, res.Volume.VolumeId)
				}

				mockCtl.Finish()
			},
		},
		{
			name: "Success: Normal flow with an empty subPath Pattern, and basePath set to /",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
					tags:         parseTagsFromStr(""),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode:      "efs-ap",
						FsId:                  fsId,
						GidMin:                "1000",
						GidMax:                "2000",
						DirectoryPerms:        "777",
						SubPathPattern:        "",
						BasePath:              "/",
						EnsureUniqueDirectory: "false",
					},
				}

				ctx := context.Background()
				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
				}
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(nil, nil)

				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(accessPoint, nil).
					Do(func(ctx context.Context, clientToken string, accessPointOpts *cloud.AccessPointOptions) {
						if accessPointOpts.DirectoryPath != "/" {
							t.Fatalf("Root directory mismatch. Expected: %v, actual: %v",
								"/",
								accessPointOpts.DirectoryPath)
						}
					})

				res, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if res.Volume.VolumeId != volumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", volumeId, res.Volume.VolumeId)
				}

				mockCtl.Finish()
			},
		},
		{
			name: "Success: Normal flow with a valid directory structure set, using repeated elements (uses PVC Name in subpath pattern multiple times)",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
					tags:         parseTagsFromStr(""),
				}

				pvcName := "foo"
				directoryCreated := fmt.Sprintf("/%s/%s", pvcName, pvcName)

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						DirectoryPerms:   "777",
						SubPathPattern:   "${.PVC.name}/${.PVC.name}",
						PvcName:          pvcName,
					},
				}

				ctx := context.Background()
				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
				}
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(nil, nil)

				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(accessPoint, nil).
					Do(func(ctx context.Context, clientToken string, accessPointOpts *cloud.AccessPointOptions) {
						if !verifyPathWhenUUIDIncluded(accessPointOpts.DirectoryPath, directoryCreated) {
							t.Fatalf("Root directory mismatch. Expected: %v (with UID appended), actual: %v",
								directoryCreated,
								accessPointOpts.DirectoryPath)
						}
					})

				res, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if res.Volume.VolumeId != volumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", volumeId, res.Volume.VolumeId)
				}

				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Volume name missing",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Capacity Range missing",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Volume capability Missing",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Volume capability Not Supported",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					VolumeCapabilities: []*csi.VolumeCapability{
						{
							AccessType: &csi.VolumeCapability_Mount{
								Mount: &csi.VolumeCapability_MountVolume{},
							},
							AccessMode: &csi.VolumeCapability_AccessMode{
								Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
							},
						},
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Volume fsType not supported",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					tags:         parseTagsFromStr(""),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						{
							AccessType: &csi.VolumeCapability_Mount{
								Mount: &csi.VolumeCapability_MountVolume{
									FsType: "abc",
								},
							},
							AccessMode: &csi.VolumeCapability_AccessMode{
								Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
							},
						},
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						DirectoryPerms:   "777",
						AzName:           "us-east-1a",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: AccessType is block",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					VolumeCapabilities: []*csi.VolumeCapability{
						{
							AccessType: &csi.VolumeCapability_Block{
								Block: &csi.VolumeCapability_BlockVolume{},
							},
							AccessMode: &csi.VolumeCapability_AccessMode{
								Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
							},
						},
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Provisioning Mode Not Supported",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-fs",
						FsId:             fsId,
						DirectoryPerms:   "777",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Missing Provisioning Mode parameter",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					Parameters: map[string]string{
						FsId:           fsId,
						DirectoryPerms: "777",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Missing Parameter FsId",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						DirectoryPerms:   "777",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: FsId cannot be blank",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             "     ",
						DirectoryPerms:   "777",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Uid invalid",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
						Uid:              "invalid",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Uid cannot be negative",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
						Uid:              "-5",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Gid invalid",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
						Gid:              "invalid",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Gid cannot be negative",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
						Gid:              "-5",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Gid min cannot be 0",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
						GidMin:           "0",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: GidMin must be an integer",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
						GidMin:           "test",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: GidMax must be an integer",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
						GidMin:           "2000",
						GidMax:           "test",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: GidMax must be greater than GidMin",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
						GidMin:           "2000",
						GidMax:           "1000",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: GidMax must be provided with GidMin",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
						GidMin:           "2000",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: GidMin must be provided with GidMax",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
						GidMax:           "2000",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: File system does not exist with fixed uid/gid",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						DirectoryPerms:   "777",
						Uid:              "1000",
						Gid:              "1001",
					},
				}

				ctx := context.Background()
				mockCloud.EXPECT().DescribeFileSystem(gomock.Eq(ctx), gomock.Any()).Return(nil, cloud.ErrNotFound)
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: File system does not exist",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						DirectoryPerms:   "777",
					},
				}

				ctx := context.Background()
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(nil, cloud.ErrNotFound)
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: DescribeFileSystem Access Denied with fixed uid/gid",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						DirectoryPerms:   "777",
						Uid:              "1000",
						Gid:              "1001",
					},
				}

				ctx := context.Background()
				mockCloud.EXPECT().DescribeFileSystem(gomock.Eq(ctx), gomock.Any()).Return(nil, cloud.ErrAccessDenied)
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: DescribeFileSystem Access Denied",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						DirectoryPerms:   "777",
					},
				}

				ctx := context.Background()
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(nil, cloud.ErrAccessDenied)
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Describe File system call fails with fixed uid/gid",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						DirectoryPerms:   "777",
						Uid:              "1000",
						Gid:              "1001",
					},
				}

				ctx := context.Background()
				mockCloud.EXPECT().DescribeFileSystem(gomock.Eq(ctx), gomock.Any()).Return(nil, errors.New("DescribeFileSystem failed"))
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: List access points call fails",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						DirectoryPerms:   "777",
					},
				}

				ctx := context.Background()
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(nil, errors.New("ListAccessPoints failed"))
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Create Access Point call fails",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						DirectoryPerms:   "777",
					},
				}

				ctx := context.Background()
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return([]*cloud.AccessPoint{}, nil)
				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(nil, errors.New("CreateAccessPoint call failed"))
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: CreateAccessPoint Access Denied",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						DirectoryPerms:   "777",
					},
				}

				ctx := context.Background()
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return([]*cloud.AccessPoint{}, nil)
				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(nil, cloud.ErrAccessDenied)
				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Run out of GIDs",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "1001",
						DirectoryPerms:   "777",
					},
				}

				ctx := context.Background()
				ap1 := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
					PosixUser: &cloud.PosixUser{
						Gid: 1000,
						Uid: 1000,
					},
				}
				ap2 := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
					PosixUser: &cloud.PosixUser{
						Gid: 1001,
						Uid: 1001,
					},
				}
				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return([]*cloud.AccessPoint{ap1, ap2}, nil).AnyTimes()
				mockCloud.EXPECT().CreateAccessPoint(gomock.Eq(ctx), gomock.Any(), gomock.Any()).Return(ap2, nil).AnyTimes()

				var err error
				// All GIDs from available range are taken, CreateVolume should fail.
				_, err = driver.CreateVolume(ctx, req)

				if err == nil {
					t.Fatalf("CreateVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Cannot assume role for x-account",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					tags:         parseTagsFromStr(""),
				}

				secrets := map[string]string{}
				secrets["awsRoleArn"] = "arn:aws:iam::1234567890:role/EFSCrossAccountRole"
				secrets["crossaccount"] = "true"

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						DirectoryPerms:   "777",
						AzName:           "us-east-1a",
					},
					Secrets: secrets,
				}

				ctx := context.Background()

				_, err := driver.CreateVolume(ctx, req)

				if err == nil {
					t.Fatalf("CreateVolume did not fail")
				}

				mockCtl.Finish()
			},
		},
		{
			name: "Fail: subPathPattern is specified but uses unsupported attributes",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				subPathPattern := "${.PVC.name}/${foo}"

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
						SubPathPattern:   subPathPattern,
					},
				}

				ctx := context.Background()

				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(nil, nil)

				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("Did not throw InvalidArgument error, instead threw %v", err)
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: resulting accessPointDirectory is too over 100 characters",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				subPathPattern := "this-directory-name-is-far-too-long-for-any-practical-purposes-and-only-serves-to-prove-a-point"

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
						SubPathPattern:   subPathPattern,
					},
				}

				ctx := context.Background()

				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(nil, nil)

				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("Did not throw InvalidArgument error, instead threw %v", err)
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail:  resulting accessPointDirectory contains over 4 subdirectories",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				subPathPattern := "a/b/c/d/e/f"

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						DirectoryPerms:   "777",
						SubPathPattern:   subPathPattern,
					},
				}

				ctx := context.Background()

				mockCloud.EXPECT().ListAccessPoints(gomock.Eq(ctx), gomock.Any()).Return(nil, nil)

				_, err := driver.CreateVolume(ctx, req)
				if err == nil {
					t.Fatal("CreateVolume did not fail")
				}
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("Did not throw InvalidArgument error, instead threw %v", err)
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Success: Reuse existing access point",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						BasePath:         "/test",
						DirectoryPerms:   "777",
						PvcNameKey:       "test-pvc",
					},
				}

				existingAP := &cloud.AccessPoint{
					AccessPointId:      apId,
					FileSystemId:       fsId,
					AccessPointRootDir: "/test/directory",
					PosixUser: &cloud.PosixUser{
						Uid: 1000,
						Gid: 1500,
					},
				}

				ctx := context.Background()
				mockCloud.EXPECT().ListAccessPoints(gomock.Any(), gomock.Any()).Return(nil, nil)
				mockCloud.EXPECT().CreateAccessPoint(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, cloud.ErrAlreadyExists)
				mockCloud.EXPECT().FindAccessPointByClientToken(gomock.Any(), gomock.Any(), fsId).Return(existingAP, nil)

				res, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				expectedVolumeId := fsId + "::" + apId
				if res.Volume.VolumeId != expectedVolumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", expectedVolumeId, res.Volume.VolumeId)
				}

				mockCtl.Finish()
			},
		},
		{
			name: "Success: Reuse existing access point no leading slash in base path",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						BasePath:         "test",
						DirectoryPerms:   "777",
						PvcNameKey:       "test-pvc",
					},
				}

				existingAP := &cloud.AccessPoint{
					AccessPointId:      apId,
					FileSystemId:       fsId,
					AccessPointRootDir: "/test/directory",
					PosixUser: &cloud.PosixUser{
						Uid: 1000,
						Gid: 1500,
					},
				}

				ctx := context.Background()
				mockCloud.EXPECT().ListAccessPoints(gomock.Any(), gomock.Any()).Return(nil, nil)
				mockCloud.EXPECT().CreateAccessPoint(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, cloud.ErrAlreadyExists)
				mockCloud.EXPECT().FindAccessPointByClientToken(gomock.Any(), gomock.Any(), fsId).Return(existingAP, nil)

				res, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				expectedVolumeId := fsId + "::" + apId
				if res.Volume.VolumeId != expectedVolumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", expectedVolumeId, res.Volume.VolumeId)
				}

				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Reuse existing access point with GID outside specified range",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						BasePath:         "/test",
						DirectoryPerms:   "777",
						PvcNameKey:       "test-pvc",
					},
				}

				existingAP := &cloud.AccessPoint{
					AccessPointId:      apId,
					FileSystemId:       fsId,
					AccessPointRootDir: "/test/directory",
					PosixUser: &cloud.PosixUser{
						Uid: 1500,
						Gid: 2500, // Outside the specified range
					},
				}

				ctx := context.Background()
				mockCloud.EXPECT().ListAccessPoints(gomock.Any(), gomock.Any()).Return(nil, nil)
				mockCloud.EXPECT().CreateAccessPoint(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, cloud.ErrAlreadyExists)
				mockCloud.EXPECT().FindAccessPointByClientToken(gomock.Any(), gomock.Any(), fsId).Return(existingAP, nil)

				_, err := driver.CreateVolume(ctx, req)

				if err == nil {
					t.Fatal("CreateVolume should have failed due to invalid GID")
				}

				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Reuse existing access point with UID outside specified range",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						BasePath:         "/test",
						DirectoryPerms:   "777",
						PvcNameKey:       "test-pvc",
					},
				}

				existingAP := &cloud.AccessPoint{
					AccessPointId:      apId,
					FileSystemId:       fsId,
					AccessPointRootDir: "/test/directory",
					PosixUser: &cloud.PosixUser{
						Uid: 2500, // Outside the specified range
						Gid: 1500,
					},
				}

				ctx := context.Background()
				mockCloud.EXPECT().ListAccessPoints(gomock.Any(), gomock.Any()).Return(nil, nil)
				mockCloud.EXPECT().CreateAccessPoint(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, cloud.ErrAlreadyExists)
				mockCloud.EXPECT().FindAccessPointByClientToken(gomock.Any(), gomock.Any(), fsId).Return(existingAP, nil)

				_, err := driver.CreateVolume(ctx, req)

				if err == nil {
					t.Fatal("CreateVolume should have failed due to invalid UID")
				}

				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Reuse existing access point with different basepath from storageclass",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode: "efs-ap",
						FsId:             fsId,
						GidMin:           "1000",
						GidMax:           "2000",
						BasePath:         "/test",
						DirectoryPerms:   "777",
						PvcNameKey:       "test-pvc",
					},
				}

				existingAP := &cloud.AccessPoint{
					AccessPointId:      apId,
					FileSystemId:       fsId,
					AccessPointRootDir: "/wrong/directory",
					PosixUser: &cloud.PosixUser{
						Uid: 1500,
						Gid: 2500, // Outside the specified range
					},
				}

				ctx := context.Background()
				mockCloud.EXPECT().ListAccessPoints(gomock.Any(), gomock.Any()).Return(nil, nil)
				mockCloud.EXPECT().CreateAccessPoint(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, cloud.ErrAlreadyExists)
				mockCloud.EXPECT().FindAccessPointByClientToken(gomock.Any(), gomock.Any(), fsId).Return(existingAP, nil)

				_, err := driver.CreateVolume(ctx, req)

				if err == nil {
					t.Fatal("CreateVolume should have failed due to invalid base path")
				}

				mockCtl.Finish()
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

func TestDeleteVolume(t *testing.T) {
	var (
		apId      = "***REMOVED_SECRET***xyz987"
		apId2     = "***REMOVED_SECRET***xyz988"
		fsId      = "fs-abcd1234"
		endpoint  = "endpoint"
		volumeId  = "fs-abcd1234::***REMOVED_SECRET***xyz987"
		volumeId2 = "fs-abcd1234::***REMOVED_SECRET***xyz988"
	)

	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "Success: Normal flow",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
				}

				req := &csi.DeleteVolumeRequest{
					VolumeId: volumeId,
				}

				ctx := context.Background()
				mockCloud.EXPECT().DeleteAccessPoint(gomock.Eq(ctx), gomock.Eq(apId)).Return(nil)
				_, err := driver.DeleteVolume(ctx, req)
				if err != nil {
					t.Fatalf("Delete Volume failed: %v", err)
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Success: Normal flow with deleteAccessPointRootDir",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)
				mockMounter := mocks.NewMockMounter(mockCtl)

				driver := &Driver{
					endpoint:                 endpoint,
					cloud:                    mockCloud,
					mounter:                  mockMounter,
					gidAllocator:             NewGidAllocator(),
					lockManager:              NewLockManagerMap(),
					deleteAccessPointRootDir: true,
				}

				req := &csi.DeleteVolumeRequest{
					VolumeId: volumeId,
				}

				accessPoint := &cloud.AccessPoint{
					AccessPointId:      apId,
					FileSystemId:       fsId,
					AccessPointRootDir: "/testDir",
					CapacityGiB:        0,
				}

				dirPresent := mocks.NewMockFileInfo(
					"testFile",
					0,
					os.FileMode(0755),
					time.Now(),
					true,
					nil,
				)

				ctx := context.Background()
				mockMounter.EXPECT().MakeDir(gomock.Any()).Return(nil)
				mockMounter.EXPECT().Mount(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
				mockMounter.EXPECT().Unmount(gomock.Any()).Return(nil)
				mockMounter.EXPECT().Stat(gomock.Any()).Return(dirPresent, nil)
				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Any()).Return(true, nil)
				mockCloud.EXPECT().DescribeAccessPoint(gomock.Eq(ctx), gomock.Eq(apId)).Return(accessPoint, nil)
				mockCloud.EXPECT().DeleteAccessPoint(gomock.Eq(ctx), gomock.Eq(apId)).Return(nil)
				_, err := driver.DeleteVolume(ctx, req)
				if err != nil {
					t.Fatalf("Delete Volume failed: %v", err)
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Success: Race Delete with deleteAccessPointRootDir",
			testFunc: func(t *testing.T) {
				const numGoRoutines = 100
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)
				mockMounter := mocks.NewMockMounter(mockCtl)

				driver := &Driver{
					endpoint:                 endpoint,
					cloud:                    mockCloud,
					mounter:                  mockMounter,
					gidAllocator:             NewGidAllocator(),
					lockManager:              NewLockManagerMap(),
					deleteAccessPointRootDir: true,
				}

				req := &csi.DeleteVolumeRequest{
					VolumeId: volumeId,
				}

				accessPoint := &cloud.AccessPoint{
					AccessPointId:      apId,
					FileSystemId:       fsId,
					AccessPointRootDir: "/testDir",
					CapacityGiB:        0,
				}

				dirPresent := mocks.NewMockFileInfo(
					"testFile",
					0,
					os.FileMode(0755),
					time.Now(),
					true,
					nil,
				)

				ctx := context.Background()
				// Expect the deletion scenario to only happen once
				mockMounter.EXPECT().MakeDir(gomock.Any()).Return(nil).Times(1)
				mockMounter.EXPECT().Mount(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
				mockMounter.EXPECT().Unmount(gomock.Any()).Return(nil).Times(1)
				mockMounter.EXPECT().Stat(gomock.Any()).Return(dirPresent, nil).Times(1)
				mockCloud.EXPECT().DeleteAccessPoint(gomock.Eq(ctx), gomock.Eq(apId)).Return(nil).Times(1)

				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Any()).Return(true, nil).Times(numGoRoutines)

				// Expect the first describe call to see the access point, then subsequent calls to see it as deleted
				var describeCallCount int32 = 0
				mockCloud.EXPECT().DescribeAccessPoint(gomock.Eq(ctx), gomock.Eq(apId)).
					DoAndReturn(func(ctx, accessPointId interface{}) (*cloud.AccessPoint, error) {
						current := atomic.AddInt32(&describeCallCount, 1)
						if current == 1 {
							return accessPoint, nil
						}
						return accessPoint, cloud.ErrNotFound
					}).Times(numGoRoutines)

				// Lock the volume mutex to hold threads until they are all scheduled
				driver.lockManager.lockMutex(apId)

				var wg sync.WaitGroup
				resultChan := make(chan struct {
					index int
					resp  *csi.DeleteVolumeResponse
					err   error
				}, numGoRoutines)

				for i := 0; i < numGoRoutines; i++ {
					wg.Add(1)
					go func(index int) {
						defer wg.Done()
						resp, err := driver.DeleteVolume(ctx, req)
						resultChan <- struct {
							index int
							resp  *csi.DeleteVolumeResponse
							err   error
						}{index, resp, err}
					}(i)
				}

				// Unlock the mutex to force a race
				driver.lockManager.unlockMutex(apId)

				go func() {
					wg.Wait()
					close(resultChan)
				}()

				for result := range resultChan {
					if result.err != nil {
						t.Fatalf("Delete Volume failed on routine %d: %v", result.index, result.err)
					}
				}

				// Ensure all keys were properly deleted from the lock manager
				keys, _ := driver.lockManager.GetLockCount()
				if keys > 0 {
					t.Fatalf("%d Keys are still in the lockManager", keys)
				}

				mockCtl.Finish()
			},
		},
		{
			name: "Success: Race Delete different access points with deleteAccessPointRootDir",
			testFunc: func(t *testing.T) {
				const numGoRoutines = 100
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)
				mockMounter := mocks.NewMockMounter(mockCtl)

				driver := &Driver{
					endpoint:                 endpoint,
					cloud:                    mockCloud,
					mounter:                  mockMounter,
					gidAllocator:             NewGidAllocator(),
					lockManager:              NewLockManagerMap(),
					deleteAccessPointRootDir: true,
				}

				req := &csi.DeleteVolumeRequest{
					VolumeId: volumeId,
				}

				req2 := &csi.DeleteVolumeRequest{
					VolumeId: volumeId2,
				}

				accessPoint1 := &cloud.AccessPoint{
					AccessPointId:      apId,
					FileSystemId:       fsId,
					AccessPointRootDir: "/ap1",
					CapacityGiB:        0,
				}

				accessPoint2 := &cloud.AccessPoint{
					AccessPointId:      apId2,
					FileSystemId:       fsId,
					AccessPointRootDir: "/ap2",
					CapacityGiB:        0,
				}

				dirPresent := mocks.NewMockFileInfo(
					"testFile",
					0,
					os.FileMode(0755),
					time.Now(),
					true,
					nil,
				)

				ctx := context.Background()
				// Expect the deletion scenario to only happen once per access point
				mockMounter.EXPECT().MakeDir(gomock.Any()).Return(nil).Times(2)
				mockMounter.EXPECT().Mount(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(2)
				mockMounter.EXPECT().Unmount(gomock.Any()).Return(nil).Times(2)
				mockMounter.EXPECT().Stat(gomock.Any()).Return(dirPresent, nil).Times(2)
				mockCloud.EXPECT().DeleteAccessPoint(gomock.Eq(ctx), gomock.Eq(apId)).Return(nil).Times(1)
				mockCloud.EXPECT().DeleteAccessPoint(gomock.Eq(ctx), gomock.Eq(apId2)).Return(nil).Times(1)

				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Any()).Return(true, nil).Times(2 * numGoRoutines)

				// Expect the first describe call to see the access point, then subsequent calls to see it as deleted
				describeCallCountAp1 := 0
				mockCloud.EXPECT().DescribeAccessPoint(gomock.Eq(ctx), gomock.Eq(apId)).
					DoAndReturn(func(ctx, accessPointId interface{}) (*cloud.AccessPoint, error) {
						describeCallCountAp1++
						if describeCallCountAp1 == 1 {
							return accessPoint1, nil
						}
						return accessPoint1, cloud.ErrNotFound
					}).Times(numGoRoutines)

				describeCallCountAp2 := 0
				mockCloud.EXPECT().DescribeAccessPoint(gomock.Eq(ctx), gomock.Eq(apId2)).
					DoAndReturn(func(ctx, accessPointId interface{}) (*cloud.AccessPoint, error) {
						describeCallCountAp2++
						if describeCallCountAp2 == 1 {
							return accessPoint2, nil
						}
						return accessPoint2, cloud.ErrNotFound
					}).Times(numGoRoutines)

				// Lock the volume mutex to hold threads until they are all scheduled
				driver.lockManager.lockMutex(apId)
				driver.lockManager.lockMutex(apId2)

				var wg sync.WaitGroup
				resultChan := make(chan struct {
					index int
					resp  *csi.DeleteVolumeResponse
					err   error
				}, numGoRoutines)

				// Add apId1 threads
				for i := 0; i < numGoRoutines; i++ {
					wg.Add(1)
					go func(index int) {
						defer wg.Done()
						resp, err := driver.DeleteVolume(ctx, req)
						resultChan <- struct {
							index int
							resp  *csi.DeleteVolumeResponse
							err   error
						}{index, resp, err}
					}(i)
				}

				// Add apId2 threads
				for i := 0; i < numGoRoutines; i++ {
					wg.Add(1)
					go func(index int) {
						defer wg.Done()
						resp, err := driver.DeleteVolume(ctx, req2)
						resultChan <- struct {
							index int
							resp  *csi.DeleteVolumeResponse
							err   error
						}{index, resp, err}
					}(i)
				}

				// Unlock the mutex to force a race
				driver.lockManager.unlockMutex(apId)
				driver.lockManager.unlockMutex(apId2)

				go func() {
					wg.Wait()
					close(resultChan)
				}()

				for result := range resultChan {
					if result.err != nil {
						t.Fatalf("Delete Volume failed on routine %d: %v", result.index, result.err)
					}
				}

				// Ensure all keys were properly deleted from the lock manager
				keys, _ := driver.lockManager.GetLockCount()
				if keys > 0 {
					t.Fatalf("%d Keys are still in the lockManager", keys)
				}

				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Delete volume mutex timeout",
			testFunc: func(t *testing.T) {
				const timeout = 3 * time.Second
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)
				mockMounter := mocks.NewMockMounter(mockCtl)

				driver := &Driver{
					endpoint:                 endpoint,
					cloud:                    mockCloud,
					mounter:                  mockMounter,
					gidAllocator:             NewGidAllocator(),
					lockManager:              NewLockManagerMap(),
					deleteAccessPointRootDir: true,
				}

				req := &csi.DeleteVolumeRequest{
					VolumeId: volumeId,
				}

				ctx := context.Background()

				// Lock the volume mutex to hold thread and force it to timeout
				driver.lockManager.lockMutex(apId)

				start := time.Now()
				resp, err := driver.DeleteVolume(ctx, req)
				elapsed := time.Since(start)

				// Check that it waited for at least the timeout duration
				if elapsed < timeout {
					t.Errorf("lockMutex returned before timeout. Expected to wait %v, but waited %v", timeout, elapsed)
				}

				// Check that it didn't wait too much longer than the timeout
				// We'll allow a 100ms buffer for system scheduling variations
				if elapsed > timeout+100*time.Millisecond {
					t.Errorf("lockMutex waited too long. Expected to wait %v, but waited %v", timeout, elapsed)
				}

				if resp != nil {
					t.Errorf("Expected resp to be nil, but got %v", resp)
				}

				if err == nil {
					t.Errorf("Expected err to not be nil, but got %v", err)
				}

				driver.lockManager.unlockMutex(apId)

				// Ensure all keys were properly deleted from the lock manager even with a timeout
				keys, _ := driver.lockManager.GetLockCount()
				if keys > 0 {
					t.Fatalf("%d Keys are still in the lockManager", keys)
				}

				mockCtl.Finish()
			},
		},
		{
			name: "Success: DescribeAccessPoint Access Point Does not exist",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)
				mockMounter := mocks.NewMockMounter(mockCtl)

				driver := &Driver{
					endpoint:                 endpoint,
					cloud:                    mockCloud,
					mounter:                  mockMounter,
					gidAllocator:             NewGidAllocator(),
					lockManager:              NewLockManagerMap(),
					deleteAccessPointRootDir: true,
				}

				req := &csi.DeleteVolumeRequest{
					VolumeId: volumeId,
				}

				ctx := context.Background()
				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Any()).Return(true, nil)
				mockCloud.EXPECT().DescribeAccessPoint(gomock.Eq(ctx), gomock.Eq(apId)).Return(nil, cloud.ErrNotFound)
				_, err := driver.DeleteVolume(ctx, req)
				if err != nil {
					t.Fatalf("Delete Volume failed: %v", err)
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: DescribeAccessPoint Access Denied",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)
				mockMounter := mocks.NewMockMounter(mockCtl)

				driver := &Driver{
					endpoint:                 endpoint,
					cloud:                    mockCloud,
					mounter:                  mockMounter,
					gidAllocator:             NewGidAllocator(),
					lockManager:              NewLockManagerMap(),
					deleteAccessPointRootDir: true,
				}

				req := &csi.DeleteVolumeRequest{
					VolumeId: volumeId,
				}

				ctx := context.Background()
				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Any()).Return(true, nil)
				mockCloud.EXPECT().DescribeAccessPoint(gomock.Eq(ctx), gomock.Eq(apId)).Return(nil, cloud.ErrAccessDenied)
				_, err := driver.DeleteVolume(ctx, req)
				if err == nil {
					t.Fatalf("DeleteVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: DescribeAccessPoint failed",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)
				mockMounter := mocks.NewMockMounter(mockCtl)

				driver := &Driver{
					endpoint:                 endpoint,
					cloud:                    mockCloud,
					mounter:                  mockMounter,
					gidAllocator:             NewGidAllocator(),
					lockManager:              NewLockManagerMap(),
					deleteAccessPointRootDir: true,
				}

				req := &csi.DeleteVolumeRequest{
					VolumeId: volumeId,
				}

				ctx := context.Background()
				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Any()).Return(true, nil)
				mockCloud.EXPECT().DescribeAccessPoint(gomock.Eq(ctx), gomock.Eq(apId)).Return(nil, errors.New("Describe Access Point failed"))
				_, err := driver.DeleteVolume(ctx, req)
				if err == nil {
					t.Fatalf("DeleteVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Fail to make directory for access point mount",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)
				mockMounter := mocks.NewMockMounter(mockCtl)

				driver := &Driver{
					endpoint:                 endpoint,
					cloud:                    mockCloud,
					mounter:                  mockMounter,
					gidAllocator:             NewGidAllocator(),
					lockManager:              NewLockManagerMap(),
					deleteAccessPointRootDir: true,
				}

				req := &csi.DeleteVolumeRequest{
					VolumeId: volumeId,
				}

				accessPoint := &cloud.AccessPoint{
					AccessPointId:      apId,
					FileSystemId:       fsId,
					AccessPointRootDir: "",
					CapacityGiB:        0,
				}

				ctx := context.Background()
				mockMounter.EXPECT().MakeDir(gomock.Any()).Return(errors.New("Failed to makeDir"))
				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Any()).Return(true, nil)
				mockCloud.EXPECT().DescribeAccessPoint(gomock.Eq(ctx), gomock.Eq(apId)).Return(accessPoint, nil)
				_, err := driver.DeleteVolume(ctx, req)
				if err == nil {
					t.Fatal("DeleteVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Fail to mount file system on directory for access point root directory removal",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)
				mockMounter := mocks.NewMockMounter(mockCtl)

				driver := &Driver{
					endpoint:                 endpoint,
					cloud:                    mockCloud,
					mounter:                  mockMounter,
					gidAllocator:             NewGidAllocator(),
					lockManager:              NewLockManagerMap(),
					deleteAccessPointRootDir: true,
				}

				req := &csi.DeleteVolumeRequest{
					VolumeId: volumeId,
				}

				accessPoint := &cloud.AccessPoint{
					AccessPointId:      apId,
					FileSystemId:       fsId,
					AccessPointRootDir: "",
					CapacityGiB:        0,
				}

				ctx := context.Background()
				mockMounter.EXPECT().MakeDir(gomock.Any()).Return(nil)
				mockMounter.EXPECT().Mount(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("Failed to mount"))
				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Any()).Return(true, nil).Times(2)
				mockCloud.EXPECT().DescribeAccessPoint(gomock.Eq(ctx), gomock.Eq(apId)).Return(accessPoint, nil)
				_, err := driver.DeleteVolume(ctx, req)
				if err == nil {
					t.Fatal("DeleteVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Fail to unmount file system after access point root directory removal",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)
				mockMounter := mocks.NewMockMounter(mockCtl)

				driver := &Driver{
					endpoint:                 endpoint,
					cloud:                    mockCloud,
					mounter:                  mockMounter,
					gidAllocator:             NewGidAllocator(),
					lockManager:              NewLockManagerMap(),
					deleteAccessPointRootDir: true,
				}

				req := &csi.DeleteVolumeRequest{
					VolumeId: volumeId,
				}

				accessPoint := &cloud.AccessPoint{
					AccessPointId:      apId,
					FileSystemId:       fsId,
					AccessPointRootDir: "",
					CapacityGiB:        0,
				}

				dirPresent := mocks.NewMockFileInfo(
					"test",
					0,
					os.FileMode(0755),
					time.Now(),
					true,
					nil,
				)

				ctx := context.Background()
				mockMounter.EXPECT().MakeDir(gomock.Any()).Return(nil)
				mockMounter.EXPECT().Mount(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
				mockMounter.EXPECT().Unmount(gomock.Any()).Return(errors.New("Failed to unmount"))
				mockMounter.EXPECT().Stat(gomock.Any()).Return(dirPresent, nil).Times(1)
				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Any()).Return(true, nil).Times(2)
				mockCloud.EXPECT().DescribeAccessPoint(gomock.Eq(ctx), gomock.Eq(apId)).Return(accessPoint, nil)
				_, err := driver.DeleteVolume(ctx, req)
				if err == nil {
					t.Fatal("DeleteVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Success: Access Point already deleted",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
				}

				req := &csi.DeleteVolumeRequest{
					VolumeId: volumeId,
				}

				ctx := context.Background()
				mockCloud.EXPECT().DeleteAccessPoint(gomock.Eq(ctx), gomock.Eq(apId)).Return(cloud.ErrNotFound)
				_, err := driver.DeleteVolume(ctx, req)
				if err != nil {
					t.Fatalf("Delete Volume failed: %v", err)
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: DeleteAccessPoint access denied",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
				}

				req := &csi.DeleteVolumeRequest{
					VolumeId: volumeId,
				}

				ctx := context.Background()
				mockCloud.EXPECT().DeleteAccessPoint(gomock.Eq(ctx), gomock.Eq(apId)).Return(cloud.ErrAccessDenied)
				_, err := driver.DeleteVolume(ctx, req)
				if err == nil {
					t.Fatal("DeleteVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: DeleteVolume fails",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
				}

				req := &csi.DeleteVolumeRequest{
					VolumeId: volumeId,
				}

				ctx := context.Background()
				mockCloud.EXPECT().DeleteAccessPoint(gomock.Eq(ctx), gomock.Eq(apId)).Return(errors.New("Delete Volume failed"))
				_, err := driver.DeleteVolume(ctx, req)
				if err == nil {
					t.Fatal("DeleteVolume did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Success: Access Point is missing in volume Id",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
				}

				req := &csi.DeleteVolumeRequest{
					VolumeId: "fs-abcd1234",
				}

				ctx := context.Background()
				_, err := driver.DeleteVolume(ctx, req)
				if err != nil {
					t.Fatalf("DeleteVolume failed: %v", err)
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Cannot assume role for x-account",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint:     endpoint,
					cloud:        mockCloud,
					gidAllocator: NewGidAllocator(),
					lockManager:  NewLockManagerMap(),
					tags:         parseTagsFromStr(""),
				}

				secrets := map[string]string{}
				secrets["awsRoleArn"] = "arn:aws:iam::1234567890:role/EFSCrossAccountRole"

				req := &csi.DeleteVolumeRequest{
					VolumeId: volumeId,
					Secrets:  secrets,
				}

				ctx := context.Background()

				_, err := driver.DeleteVolume(ctx, req)

				if err == nil {
					t.Fatalf("DeleteVolume did not fail")
				}

				mockCtl.Finish()
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

func TestCreateDeleteVolumeRace(t *testing.T) {
	var (
		apId                = "***REMOVED_SECRET***xyz987"
		fsId                = "fs-abcd1234"
		endpoint            = "endpoint"
		volumeId            = "fs-abcd1234::***REMOVED_SECRET***xyz987"
		volumeName          = "volumeName"
		capacityRange int64 = 5368709120
		stdVolCap           = &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		}
	)

	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "Success: Race Create with reused access point while Deleting with deleteAccessPointRootDir",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)
				mockMounter := mocks.NewMockMounter(mockCtl)

				driver := &Driver{
					endpoint:                 endpoint,
					cloud:                    mockCloud,
					mounter:                  mockMounter,
					gidAllocator:             NewGidAllocator(),
					lockManager:              NewLockManagerMap(),
					tags:                     parseTagsFromStr(""),
					deleteAccessPointRootDir: true,
				}
				pvcNameVal := "test-pvc"

				createReq := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode:    "efs-ap",
						FsId:                fsId,
						GidMin:              "1000",
						GidMax:              "2000",
						DirectoryPerms:      "777",
						AzName:              "us-east-1a",
						ReuseAccessPointKey: "true",
						PvcNameKey:          pvcNameVal,
					},
				}

				deleteReq := &csi.DeleteVolumeRequest{
					VolumeId: volumeId,
				}

				ctx := context.Background()

				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
					PosixUser: &cloud.PosixUser{
						Gid: 1000,
						Uid: 1000,
					},
					AccessPointRootDir: "/testDir",
					CapacityGiB:        0,
				}

				dirPresent := mocks.NewMockFileInfo(
					"testFile",
					0,
					os.FileMode(0755),
					time.Now(),
					true,
					nil,
				)

				// Expected create function calls
				mockCloud.EXPECT().DescribeAccessPoint(gomock.Eq(ctx), gomock.Eq(apId)).Return(accessPoint, nil)
				mockCloud.EXPECT().FindAccessPointByClientToken(gomock.Eq(ctx), gomock.Any(), gomock.Eq(fsId)).Return(accessPoint, nil)

				// Expected delete function calls
				mockMounter.EXPECT().MakeDir(gomock.Any()).Return(nil).Times(1)
				mockMounter.EXPECT().Mount(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
				mockMounter.EXPECT().Unmount(gomock.Any()).Return(nil).Times(1)
				mockMounter.EXPECT().Stat(gomock.Any()).Return(dirPresent, nil).Times(1)
				mockCloud.EXPECT().DeleteAccessPoint(gomock.Eq(ctx), gomock.Eq(apId)).Return(nil).Times(1)

				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Any()).Return(true, nil).Times(1)

				// Lock the volume mutex to hold threads until they are all scheduled
				driver.lockManager.lockMutex(apId)

				var wg sync.WaitGroup
				deleteResultChan := make(chan struct {
					resp *csi.DeleteVolumeResponse
					err  error
				})

				// Schedule delete volume first
				wg.Add(1)
				go func() {
					defer wg.Done()
					resp, err := driver.DeleteVolume(ctx, deleteReq)
					deleteResultChan <- struct {
						resp *csi.DeleteVolumeResponse
						err  error
					}{resp, err}
				}()

				// Let the deletion thread settle on the lock to ensure it goes first
				time.Sleep(100 * time.Millisecond)

				createResultChan := make(chan struct {
					resp *csi.CreateVolumeResponse
					err  error
				})

				// Schedule the volume create second
				wg.Add(1)
				go func() {
					defer wg.Done()
					resp, err := driver.CreateVolume(ctx, createReq)
					createResultChan <- struct {
						resp *csi.CreateVolumeResponse
						err  error
					}{resp, err}
				}()

				// Let the threads settle to force the race
				time.Sleep(100 * time.Millisecond)

				driver.lockManager.unlockMutex(apId)

				go func() {
					wg.Wait()
					close(createResultChan)
					close(deleteResultChan)
				}()

				createResult := <-createResultChan
				if createResult.err != nil {
					t.Fatalf("CreateVolume failed: %v", createResult.err)
				}

				if createResult.resp.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if createResult.resp.Volume.VolumeId != volumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", volumeId, createResult.resp.Volume.VolumeId)
				}

				deleteResult := <-deleteResultChan
				if deleteResult.err != nil {
					t.Fatalf("Delete Volume failed: %v", deleteResult.err)
				}

				// Ensure all keys were properly deleted from the lock manager
				keys, _ := driver.lockManager.GetLockCount()
				if keys > 0 {
					t.Fatalf("%d Keys are still in the lockManager", keys)
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Success: Race Delete with deleteAccessPointRootDir while creating volume with reused access point",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)
				mockMounter := mocks.NewMockMounter(mockCtl)

				driver := &Driver{
					endpoint:                 endpoint,
					cloud:                    mockCloud,
					mounter:                  mockMounter,
					gidAllocator:             NewGidAllocator(),
					lockManager:              NewLockManagerMap(),
					tags:                     parseTagsFromStr(""),
					deleteAccessPointRootDir: true,
				}
				pvcNameVal := "test-pvc"

				createReq := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode:    "efs-ap",
						FsId:                fsId,
						GidMin:              "1000",
						GidMax:              "2000",
						DirectoryPerms:      "777",
						AzName:              "us-east-1a",
						ReuseAccessPointKey: "true",
						PvcNameKey:          pvcNameVal,
					},
				}

				deleteReq := &csi.DeleteVolumeRequest{
					VolumeId: volumeId,
				}

				ctx := context.Background()

				accessPoint := &cloud.AccessPoint{
					AccessPointId: apId,
					FileSystemId:  fsId,
					PosixUser: &cloud.PosixUser{
						Gid: 1000,
						Uid: 1000,
					},
					AccessPointRootDir: "/testDir",
					CapacityGiB:        0,
				}

				dirPresent := mocks.NewMockFileInfo(
					"testFile",
					0,
					os.FileMode(0755),
					time.Now(),
					true,
					nil,
				)

				// Expected create function calls
				mockCloud.EXPECT().DescribeAccessPoint(gomock.Eq(ctx), gomock.Eq(apId)).Return(accessPoint, nil)
				mockCloud.EXPECT().FindAccessPointByClientToken(gomock.Eq(ctx), gomock.Any(), gomock.Eq(fsId)).Return(accessPoint, nil)

				// Expected delete function calls
				mockMounter.EXPECT().MakeDir(gomock.Any()).Return(nil).Times(1)
				mockMounter.EXPECT().Mount(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
				mockMounter.EXPECT().Unmount(gomock.Any()).Return(nil).Times(1)
				mockMounter.EXPECT().Stat(gomock.Any()).Return(dirPresent, nil).Times(1)
				mockMounter.EXPECT().IsLikelyNotMountPoint(gomock.Any()).Return(true, nil).Times(1)
				mockCloud.EXPECT().DeleteAccessPoint(gomock.Eq(ctx), gomock.Eq(apId)).Return(nil).Times(1)

				// Lock the volume mutex to hold threads until they are all scheduled
				driver.lockManager.lockMutex(apId)

				var wg sync.WaitGroup
				createResultChan := make(chan struct {
					resp *csi.CreateVolumeResponse
					err  error
				})

				// Schedule the volume create first
				wg.Add(1)
				go func() {
					defer wg.Done()
					resp, err := driver.CreateVolume(ctx, createReq)
					createResultChan <- struct {
						resp *csi.CreateVolumeResponse
						err  error
					}{resp, err}
				}()

				// Let the creation thread settle on the lock to ensure it goes first
				time.Sleep(100 * time.Millisecond)

				// Schedule the delete volume function
				deleteResultChan := make(chan struct {
					resp *csi.DeleteVolumeResponse
					err  error
				})

				wg.Add(1)
				go func() {
					defer wg.Done()
					resp, err := driver.DeleteVolume(ctx, deleteReq)
					deleteResultChan <- struct {
						resp *csi.DeleteVolumeResponse
						err  error
					}{resp, err}
				}()

				// Let the threads settle to force the race
				time.Sleep(100 * time.Millisecond)

				driver.lockManager.unlockMutex(apId)

				go func() {
					wg.Wait()
					close(createResultChan)
					close(deleteResultChan)
				}()

				createResult := <-createResultChan
				if createResult.err != nil {
					t.Fatalf("CreateVolume failed: %v", createResult.err)
				}

				if createResult.resp.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if createResult.resp.Volume.VolumeId != volumeId {
					t.Fatalf("Volume Id mismatched. Expected: %v, Actual: %v", volumeId, createResult.resp.Volume.VolumeId)
				}

				deleteResult := <-deleteResultChan
				if deleteResult.err != nil {
					t.Fatalf("Delete Volume failed: %v", deleteResult.err)
				}

				// Ensure all keys were properly deleted from the lock manager
				keys, _ := driver.lockManager.GetLockCount()
				if keys > 0 {
					t.Fatalf("%d Keys are still in the lockManager", keys)
				}
				mockCtl.Finish()
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

func TestValidateVolumeCapabilities(t *testing.T) {
	var (
		endpoint       = "endpoint"
		volumeId       = "fs-abcd1234::***REMOVED_SECRET***xyz987"
		stdVolCapValid = &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		}
		stdVolCapInvalid = &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
			},
		}
	)
	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "Success",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint: endpoint,
					cloud:    mockCloud,
				}

				req := &csi.ValidateVolumeCapabilitiesRequest{
					VolumeId: volumeId,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCapValid,
					},
				}

				ctx := context.Background()
				res, err := driver.ValidateVolumeCapabilities(ctx, req)
				if err != nil {
					t.Fatalf("ValidateVolumeCapabilities failed: %v", err)
				}

				if res.Confirmed == nil {
					t.Fatalf("Capability is not supported")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Success: Unsupported volume capability",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint: endpoint,
					cloud:    mockCloud,
				}

				req := &csi.ValidateVolumeCapabilitiesRequest{
					VolumeId: volumeId,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCapInvalid,
					},
				}

				ctx := context.Background()
				res, err := driver.ValidateVolumeCapabilities(ctx, req)
				if err != nil {
					t.Fatalf("ValidateVolumeCapabilities failed: %v", err)
				}

				if res.Confirmed != nil {
					t.Fatal("ValidateVolumeCapabilities did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Volume Id is missing",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint: endpoint,
					cloud:    mockCloud,
				}

				req := &csi.ValidateVolumeCapabilitiesRequest{
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCapValid,
					},
				}

				ctx := context.Background()
				_, err := driver.ValidateVolumeCapabilities(ctx, req)
				if err == nil {
					t.Fatal("ValidateVolumeCapabilities did not fail")
				}
				mockCtl.Finish()
			},
		},
		{
			name: "Fail: Volume Capabilities is missing",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint: endpoint,
					cloud:    mockCloud,
				}

				req := &csi.ValidateVolumeCapabilitiesRequest{
					VolumeId: volumeId,
				}

				ctx := context.Background()
				_, err := driver.ValidateVolumeCapabilities(ctx, req)
				if err == nil {
					t.Fatal("ValidateVolumeCapabilities did not fail")
				}
				mockCtl.Finish()
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

func TestControllerGetCapabilities(t *testing.T) {
	var endpoint = "endpoint"
	mockCtl := gomock.NewController(t)
	mockCloud := mocks.NewMockCloud(mockCtl)

	driver := &Driver{
		endpoint: endpoint,
		cloud:    mockCloud,
	}

	ctx := context.Background()
	_, err := driver.ControllerGetCapabilities(ctx, &csi.ControllerGetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("ControllerGetCapabilities failed: %v", err)
	}
}

func verifyPathWhenUUIDIncluded(pathToVerify string, expectedPathWithoutUUID string) bool {
	r := regexp.MustCompile("(.*)-([0-9A-fA-F]+-[0-9A-fA-F]+-[0-9A-fA-F]+-[0-9A-fA-F]+-[0-9A-fA-F]+$)")
	matches := r.FindStringSubmatch(pathToVerify)
	doesPathMatchWithUuid := matches[1] == expectedPathWithoutUUID
	_, err := uuid.Parse(matches[2])
	return err == nil && doesPathMatchWithUuid
}

// Helper function to return a random string of a specified length. Useful when generating random apIds
func randStringBytes(n int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

// TestCreateVolumeEFSNS tests EFS-NS volume creation functionality
func TestCreateVolumeEFSNS(t *testing.T) {
	var (
		endpoint            = "endpoint"
		volumeName          = "test-volume"
		namespace           = "test-namespace"
		fsId                = "fs-abcd1234"
		clusterID           = "test-cluster"
		efsnsVolumeID       = "efs-ns::test-namespace::fs-abcd1234::test-cluster"
		capacityRange int64 = 5368709120
		stdVolCap           = &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		}
	)

	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "Success: EFS-NS volume creation with minimal parameters",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockCloud := mocks.NewMockCloud(mockCtl)
				mockNSManager := mocks.NewMockNamespaceFileSystemManager(mockCtl)
				mockPVCTracker := mocks.NewMockPVCTracker(mockCtl)
				mockFinalizerManager := mocks.NewMockFinalizerManager(mockCtl)

				driver := &Driver{
					endpoint:         endpoint,
					cloud:            mockCloud,
					namespaceManager: mockNSManager,
					pvcTracker:       mockPVCTracker,
					finalizerManager: mockFinalizerManager,
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode:              NamespaceMode,
						Namespace:                     namespace,
						ClusterID:                     clusterID,
						"csi.storage.k8s.io/pvc/name": "test-pvc",
					},
				}

				ctx := context.Background()
				fileSystemInfo := &efsns.FileSystemInfo{
					FileSystemID: fsId,
					Namespace:    namespace,
					ClusterID:    clusterID,
					State:        efsns.FileSystemStateAvailable,
				}

				mockNSManager.EXPECT().CreateOrGetFileSystemForNamespace(
					gomock.Eq(ctx),
					gomock.Eq(namespace),
					gomock.Any(),
				).Return(fileSystemInfo, nil)

				mockPVCTracker.EXPECT().AddPVC(
					gomock.Eq(ctx),
					gomock.Eq(namespace),
					gomock.Eq("test-pvc"),
					gomock.Eq(efsnsVolumeID),
				).Return(nil)

				res, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if res.Volume.VolumeId != efsnsVolumeID {
					t.Fatalf("Volume ID mismatch. Expected: %v, actual: %v", efsnsVolumeID, res.Volume.VolumeId)
				}
			},
		},
		{
			name: "Success: EFS-NS volume creation with all parameters",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockCloud := mocks.NewMockCloud(mockCtl)
				mockNSManager := mocks.NewMockNamespaceFileSystemManager(mockCtl)
				mockPVCTracker := mocks.NewMockPVCTracker(mockCtl)
				mockFinalizerManager := mocks.NewMockFinalizerManager(mockCtl)

				driver := &Driver{
					endpoint:         endpoint,
					cloud:            mockCloud,
					namespaceManager: mockNSManager,
					pvcTracker:       mockPVCTracker,
					finalizerManager: mockFinalizerManager,
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					CapacityRange: &csi.CapacityRange{
						RequiredBytes: capacityRange,
					},
					Parameters: map[string]string{
						ProvisioningMode:              NamespaceMode,
						Namespace:                     namespace,
						ClusterID:                     clusterID,
						PerformanceMode:               "generalPurpose",
						ThroughputMode:                "provisioned",
						ProvisionedThroughputInMibps:  "1000",
						Encrypted:                     "true",
						KmsKeyId:                      "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012",
						EncryptInTransit:              "true",
						"csi.storage.k8s.io/pvc/name": "test-pvc", // CSI standard parameter
					},
				}

				ctx := context.Background()
				fileSystemInfo := &efsns.FileSystemInfo{
					FileSystemID: fsId,
					Namespace:    namespace,
					ClusterID:    clusterID,
					State:        efsns.FileSystemStateAvailable,
				}

				mockNSManager.EXPECT().CreateOrGetFileSystemForNamespace(
					gomock.Eq(ctx),
					gomock.Eq(namespace),
					gomock.Any(),
				).Return(fileSystemInfo, nil).
					Do(func(ctx context.Context, namespace string, options *efsns.FileSystemOptions) {
						if options.PerformanceMode != "generalPurpose" {
							t.Fatalf("PerformanceMode mismatch. Expected: %v, actual: %v", "generalPurpose", options.PerformanceMode)
						}
						if options.ThroughputMode != "provisioned" {
							t.Fatalf("ThroughputMode mismatch. Expected: %v, actual: %v", "provisioned", options.ThroughputMode)
						}
						if options.ProvisionedThroughputInMibps == nil || *options.ProvisionedThroughputInMibps != 1000 {
							t.Fatalf("ProvisionedThroughputInMibps mismatch. Expected: %v, actual: %v", 1000, options.ProvisionedThroughputInMibps)
						}
						if options.Encrypted == nil || *options.Encrypted != true {
							t.Fatalf("Encrypted mismatch. Expected: %v, actual: %v", true, options.Encrypted)
						}
					})

				mockPVCTracker.EXPECT().AddPVC(
					gomock.Eq(ctx),
					gomock.Eq(namespace),
					gomock.Eq("test-pvc"),
					gomock.Eq(efsnsVolumeID),
				).Return(nil)

				res, err := driver.CreateVolume(ctx, req)

				if err != nil {
					t.Fatalf("CreateVolume failed: %v", err)
				}

				if res.Volume == nil {
					t.Fatal("Volume is nil")
				}

				if res.Volume.VolumeId != efsnsVolumeID {
					t.Fatalf("Volume ID mismatch. Expected: %v, actual: %v", efsnsVolumeID, res.Volume.VolumeId)
				}
			},
		},
		{
			name: "Fail: Missing namespace parameter",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint: endpoint,
					cloud:    mockCloud,
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					Parameters: map[string]string{
						ProvisioningMode: NamespaceMode,
						ClusterID:        clusterID,
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)

				if err == nil {
					t.Fatal("CreateVolume should have failed due to missing namespace")
				}

				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("Expected InvalidArgument error, got: %v", status.Code(err))
				}
			},
		},
		{
			name: "Fail: Missing cluster ID parameter",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint: endpoint,
					cloud:    mockCloud,
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					Parameters: map[string]string{
						ProvisioningMode: NamespaceMode,
						Namespace:        namespace,
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)

				if err == nil {
					t.Fatal("CreateVolume should have failed due to missing cluster ID")
				}

				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("Expected InvalidArgument error, got: %v", status.Code(err))
				}
			},
		},
		{
			name: "Fail: Invalid namespace name",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint: endpoint,
					cloud:    mockCloud,
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					Parameters: map[string]string{
						ProvisioningMode: NamespaceMode,
						Namespace:        "invalid-namespace-name-that-is-too-long-and-contains-invalid-characters!@#",
						ClusterID:        clusterID,
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)

				if err == nil {
					t.Fatal("CreateVolume should have failed due to invalid namespace name")
				}

				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("Expected InvalidArgument error, got: %v", status.Code(err))
				}
			},
		},
		{
			name: "Fail: EFS-NS components not initialized",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint: endpoint,
					cloud:    mockCloud,
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					Parameters: map[string]string{
						ProvisioningMode:              NamespaceMode,
						Namespace:                     namespace,
						ClusterID:                     clusterID,
						"csi.storage.k8s.io/pvc/name": "test-pvc",
					},
				}

				ctx := context.Background()
				_, err := driver.CreateVolume(ctx, req)

				if err == nil {
					t.Fatal("CreateVolume should have failed due to uninitialized EFS-NS components")
				}

				if status.Code(err) != codes.Internal {
					t.Fatalf("Expected Internal error, got: %v", status.Code(err))
				}
			},
		},
		{
			name: "Fail: NamespaceManager CreateOrGetFileSystemForNamespace error",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockCloud := mocks.NewMockCloud(mockCtl)
				mockNSManager := mocks.NewMockNamespaceFileSystemManager(mockCtl)
				mockPVCTracker := mocks.NewMockPVCTracker(mockCtl)
				mockFinalizerManager := mocks.NewMockFinalizerManager(mockCtl)

				driver := &Driver{
					endpoint:         endpoint,
					cloud:            mockCloud,
					namespaceManager: mockNSManager,
					pvcTracker:       mockPVCTracker,
					finalizerManager: mockFinalizerManager,
				}

				req := &csi.CreateVolumeRequest{
					Name: volumeName,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
					Parameters: map[string]string{
						ProvisioningMode:              NamespaceMode,
						Namespace:                     namespace,
						ClusterID:                     clusterID,
						"csi.storage.k8s.io/pvc/name": "test-pvc",
					},
				}

				ctx := context.Background()
				mockNSManager.EXPECT().CreateOrGetFileSystemForNamespace(
					gomock.Eq(ctx),
					gomock.Eq(namespace),
					gomock.Any(),
				).Return(nil, errors.New("filesystem creation failed"))

				_, err := driver.CreateVolume(ctx, req)

				if err == nil {
					t.Fatal("CreateVolume should have failed due to namespace manager error")
				}

				if status.Code(err) != codes.Internal {
					t.Fatalf("Expected Internal error, got: %v", status.Code(err))
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

// TestDeleteVolumeEFSNS tests EFS-NS volume deletion functionality
func TestDeleteVolumeEFSNS(t *testing.T) {
	var (
		endpoint      = "endpoint"
		namespace     = "test-namespace"
		efsnsVolumeID = "efs-ns::test-namespace::fs-abcd1234::test-cluster"
	)

	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "Success: EFS-NS volume deletion",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockCloud := mocks.NewMockCloud(mockCtl)
				mockNSManager := mocks.NewMockNamespaceFileSystemManager(mockCtl)
				mockPVCTracker := mocks.NewMockPVCTracker(mockCtl)
				mockFinalizerManager := mocks.NewMockFinalizerManager(mockCtl)

				driver := &Driver{
					endpoint:         endpoint,
					cloud:            mockCloud,
					namespaceManager: mockNSManager,
					pvcTracker:       mockPVCTracker,
					finalizerManager: mockFinalizerManager,
				}

				req := &csi.DeleteVolumeRequest{
					VolumeId: efsnsVolumeID,
				}

				ctx := context.Background()
				mockPVCTracker.EXPECT().RemovePVC(
					gomock.Eq(ctx),
					gomock.Eq(namespace),
					gomock.Eq("pvc-fs-abcd1234"),
				).Return(false, nil)

				mockNSManager.EXPECT().DeleteFileSystemForNamespace(
					gomock.Eq(ctx),
					gomock.Eq(namespace),
					gomock.Eq(efsnsVolumeID),
				).Return(nil)

				_, err := driver.DeleteVolume(ctx, req)

				if err != nil {
					t.Fatalf("DeleteVolume failed: %v", err)
				}
			},
		},
		{
			name: "Success: EFS-NS volume deletion with invalid volume ID (should not fail)",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint: endpoint,
					cloud:    mockCloud,
				}

				req := &csi.DeleteVolumeRequest{
					VolumeId: "invalid-efs-ns-volume-id",
				}

				ctx := context.Background()
				_, err := driver.DeleteVolume(ctx, req)

				if err != nil {
					t.Fatalf("DeleteVolume should not fail for invalid volume ID: %v", err)
				}
			},
		},
		{
			name: "Fail: EFS-NS components not initialized",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint: endpoint,
					cloud:    mockCloud,
				}

				req := &csi.DeleteVolumeRequest{
					VolumeId: efsnsVolumeID,
				}

				ctx := context.Background()
				_, err := driver.DeleteVolume(ctx, req)

				if err == nil {
					t.Fatal("DeleteVolume should have failed due to uninitialized EFS-NS components")
				}

				if status.Code(err) != codes.Internal {
					t.Fatalf("Expected Internal error, got: %v", status.Code(err))
				}
			},
		},
		{
			name: "Fail: PVCTracker RemovePVCMapping error",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockCloud := mocks.NewMockCloud(mockCtl)
				mockNSManager := mocks.NewMockNamespaceFileSystemManager(mockCtl)
				mockPVCTracker := mocks.NewMockPVCTracker(mockCtl)
				mockFinalizerManager := mocks.NewMockFinalizerManager(mockCtl)

				driver := &Driver{
					endpoint:         endpoint,
					cloud:            mockCloud,
					namespaceManager: mockNSManager,
					pvcTracker:       mockPVCTracker,
					finalizerManager: mockFinalizerManager,
				}

				req := &csi.DeleteVolumeRequest{
					VolumeId: efsnsVolumeID,
				}

				ctx := context.Background()
				mockPVCTracker.EXPECT().RemovePVC(
					gomock.Eq(ctx),
					gomock.Eq(namespace),
					gomock.Eq("pvc-fs-abcd1234"),
				).Return(false, errors.New("failed to remove PVC mapping"))

				_, err := driver.DeleteVolume(ctx, req)

				if err == nil {
					t.Fatal("DeleteVolume should have failed due to PVC tracker error")
				}

				if status.Code(err) != codes.Internal {
					t.Fatalf("Expected Internal error, got: %v", status.Code(err))
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

// TestValidateVolumeCapabilitiesEFSNS tests EFS-NS volume capabilities validation
func TestValidateVolumeCapabilitiesEFSNS(t *testing.T) {
	var (
		endpoint      = "endpoint"
		namespace     = "test-namespace"
		fsId          = "fs-abcd1234"
		clusterID     = "test-cluster"
		efsnsVolumeID = "efs-ns::test-namespace::fs-abcd1234::test-cluster"
		stdVolCap     = &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		}
	)

	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "Success: EFS-NS volume capabilities validation",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockCloud := mocks.NewMockCloud(mockCtl)
				mockNSManager := mocks.NewMockNamespaceFileSystemManager(mockCtl)

				driver := &Driver{
					endpoint:         endpoint,
					cloud:            mockCloud,
					namespaceManager: mockNSManager,
				}

				req := &csi.ValidateVolumeCapabilitiesRequest{
					VolumeId: efsnsVolumeID,
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
				}

				ctx := context.Background()
				fileSystemInfo := &efsns.FileSystemInfo{
					FileSystemID: fsId,
					Namespace:    namespace,
					ClusterID:    clusterID,
					State:        efsns.FileSystemStateAvailable,
				}

				mockNSManager.EXPECT().GetFileSystemInfo(
					gomock.Eq(ctx),
					gomock.Eq(namespace),
				).Return(fileSystemInfo, nil)

				res, err := driver.ValidateVolumeCapabilities(ctx, req)

				if err != nil {
					t.Fatalf("ValidateVolumeCapabilities failed: %v", err)
				}

				if res.Confirmed == nil {
					t.Fatal("Expected confirmed capabilities")
				}
			},
		},
		{
			name: "Fail: Invalid EFS-NS volume ID format",
			testFunc: func(t *testing.T) {
				mockCtl := gomock.NewController(t)
				defer mockCtl.Finish()

				mockCloud := mocks.NewMockCloud(mockCtl)

				driver := &Driver{
					endpoint: endpoint,
					cloud:    mockCloud,
				}

				req := &csi.ValidateVolumeCapabilitiesRequest{
					VolumeId: "invalid-efs-ns-volume-id",
					VolumeCapabilities: []*csi.VolumeCapability{
						stdVolCap,
					},
				}

				ctx := context.Background()
				_, err := driver.ValidateVolumeCapabilities(ctx, req)

				if err == nil {
					t.Fatal("ValidateVolumeCapabilities should have failed for invalid volume ID")
				}

				if status.Code(err) != codes.NotFound {
					t.Fatalf("Expected NotFound error, got: %v", status.Code(err))
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

// TestParseEFSNSStorageClassParameters tests parameter parsing for EFS-NS
func TestParseEFSNSStorageClassParameters(t *testing.T) {
	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "Success: Parse minimal parameters",
			testFunc: func(t *testing.T) {
				parameters := map[string]string{
					// Empty parameters - truly minimal
				}

				options, err := parseEFSNSStorageClassParameters(parameters)
				if err != nil {
					t.Fatalf("parseEFSNSStorageClassParameters failed: %v", err)
				}

				if options.PerformanceMode != "" {
					t.Fatalf("Expected empty PerformanceMode, got: %v", options.PerformanceMode)
				}
				if options.ThroughputMode != "" {
					t.Fatalf("Expected empty ThroughputMode, got: %v", options.ThroughputMode)
				}
				if options.Encrypted != nil {
					t.Fatalf("Expected nil Encrypted, got: %v", *options.Encrypted)
				}
			},
		},
		{
			name: "Success: Parse all parameters",
			testFunc: func(t *testing.T) {
				parameters := map[string]string{
					PerformanceMode:              "generalPurpose",
					ThroughputMode:               "provisioned",
					ProvisionedThroughputInMibps: "1000",
					Encrypted:                    "true",
					KmsKeyId:                     "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012",
					EncryptInTransit:             "false",
				}

				options, err := parseEFSNSStorageClassParameters(parameters)
				if err != nil {
					t.Fatalf("parseEFSNSStorageClassParameters failed: %v", err)
				}

				if options.PerformanceMode != "generalPurpose" {
					t.Fatalf("Expected PerformanceMode 'generalPurpose', got: %v", options.PerformanceMode)
				}
				if options.ThroughputMode != "provisioned" {
					t.Fatalf("Expected ThroughputMode 'provisioned', got: %v", options.ThroughputMode)
				}
				if options.ProvisionedThroughputInMibps == nil || *options.ProvisionedThroughputInMibps != 1000 {
					t.Fatalf("Expected ProvisionedThroughputInMibps 1000, got: %v", options.ProvisionedThroughputInMibps)
				}
				if options.Encrypted == nil || *options.Encrypted != true {
					t.Fatalf("Expected Encrypted true, got: %v", options.Encrypted)
				}
			},
		},
		{
			name: "Fail: Invalid provisioned throughput",
			testFunc: func(t *testing.T) {
				parameters := map[string]string{
					ProvisionedThroughputInMibps: "invalid",
				}

				_, err := parseEFSNSStorageClassParameters(parameters)
				if err == nil {
					t.Fatal("parseEFSNSStorageClassParameters should have failed for invalid throughput")
				}
			},
		},
		{
			name: "Fail: Invalid encrypted value",
			testFunc: func(t *testing.T) {
				parameters := map[string]string{
					Encrypted: "maybe",
				}

				_, err := parseEFSNSStorageClassParameters(parameters)
				if err == nil {
					t.Fatal("parseEFSNSStorageClassParameters should have failed for invalid encrypted value")
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

// TestValidateNamespaceName tests namespace name validation
func TestValidateNamespaceName(t *testing.T) {
	testCases := []struct {
		name        string
		namespace   string
		expectError bool
	}{
		{
			name:        "Valid: simple namespace",
			namespace:   "test-namespace",
			expectError: false,
		},
		{
			name:        "Valid: namespace with numbers",
			namespace:   "test-namespace-123",
			expectError: false,
		},
		{
			name:        "Valid: single character",
			namespace:   "a",
			expectError: false,
		},
		{
			name:        "Valid: maximum length",
			namespace:   "a123456789012345678901234567890123456789012345678901234567890123",
			expectError: false,
		},
		{
			name:        "Invalid: empty namespace",
			namespace:   "",
			expectError: true,
		},
		{
			name:        "Invalid: starts with number",
			namespace:   "1test-namespace",
			expectError: true,
		},
		{
			name:        "Invalid: contains invalid characters",
			namespace:   "test_namespace",
			expectError: true,
		},
		{
			name:        "Invalid: too long",
			namespace:   "a1234567890123456789012345678901234567890123456789012345678901234",
			expectError: true,
		},
		{
			name:        "Invalid: starts with hyphen",
			namespace:   "-test-namespace",
			expectError: true,
		},
		{
			name:        "Invalid: ends with hyphen",
			namespace:   "test-namespace-",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateNamespaceName(tc.namespace)
			if tc.expectError && err == nil {
				t.Fatalf("Expected error for namespace '%s', but got none", tc.namespace)
			}
			if !tc.expectError && err != nil {
				t.Fatalf("Expected no error for namespace '%s', but got: %v", tc.namespace, err)
			}
		})
	}
}
