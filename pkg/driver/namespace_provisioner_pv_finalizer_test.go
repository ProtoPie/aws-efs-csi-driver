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
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"

	"github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/cloud"
	cloudMocks "github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/cloud/mocks"
)

func fakeRestConfig() *rest.Config {
	return &rest.Config{
		Host: "https://fake-k8s-api-server:6443",
	}
}

func setupTestEnv() func() {
	// Set fake AWS_ROLE_ARN to avoid STS calls in tests
	oldRoleArn := os.Getenv("AWS_ROLE_ARN")
	os.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/test-role")

	return func() {
		if oldRoleArn != "" {
			os.Setenv("AWS_ROLE_ARN", oldRoleArn)
		} else {
			os.Unsetenv("AWS_ROLE_ARN")
		}
	}
}

func TestPVFinalizer(t *testing.T) {
	cleanup := setupTestEnv()
	defer cleanup()
	t.Run("PV with Delete reclaim policy should have finalizer added", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCloud := cloudMocks.NewMockCloud(ctrl)

		// Create test PV with Delete reclaim policy
		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-pv",
			},
			Spec: corev1.PersistentVolumeSpec{
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{
						Driver:       driverName,
						VolumeHandle: "fs-12345678::fsap-87654321",
						VolumeAttributes: map[string]string{
							"provisioningMode": NamespaceProvisioningMode,
						},
					},
				},
			},
		}

		// Create fake k8s client
		k8sClient := fake.NewSimpleClientset(pv)

		// Create provisioner
		np, err := NewNamespaceProvisioner(mockCloud, k8sClient, fakeRestConfig(), DefaultProvisionerOptions())
		assert.NoError(t, err)

		// Add finalizer
		err = np.addPVFinalizer(ctx, pv.Name)
		assert.NoError(t, err)

		// Verify finalizer was added
		updatedPV, err := k8sClient.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.Contains(t, updatedPV.Finalizers, EFSPVFinalizer)
	})

	t.Run("PV deletion should trigger access point cleanup", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCloud := cloudMocks.NewMockCloud(ctrl)
		accessPointId := "fsap-87654321"

		// Create test PV with finalizer and deletion timestamp
		deletionTime := metav1.Now()
		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "test-pv",
				Finalizers:        []string{EFSPVFinalizer},
				DeletionTimestamp: &deletionTime,
			},
			Spec: corev1.PersistentVolumeSpec{
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{
						Driver:       driverName,
						VolumeHandle: "fs-12345678::" + accessPointId,
						VolumeAttributes: map[string]string{
							"provisioningMode": NamespaceProvisioningMode,
						},
					},
				},
			},
		}

		// Create fake k8s client
		k8sClient := fake.NewSimpleClientset(pv)

		// Expect access point deletion
		mockCloud.EXPECT().DeleteAccessPoint(gomock.Any(), accessPointId).Return(nil)

		// Create provisioner
		np, err := NewNamespaceProvisioner(mockCloud, k8sClient, fakeRestConfig(), DefaultProvisionerOptions())
		assert.NoError(t, err)

		// Handle PV deletion
		np.handlePVDeletion(ctx, pv)

		// Give some time for async operations
		time.Sleep(100 * time.Millisecond)

		// Verify finalizer was removed
		updatedPV, err := k8sClient.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotContains(t, updatedPV.Finalizers, EFSPVFinalizer)
	})

	t.Run("PV with Retain policy should not have finalizer", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCloud := cloudMocks.NewMockCloud(ctrl)

		// Create test PV with Retain reclaim policy
		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-pv",
			},
			Spec: corev1.PersistentVolumeSpec{
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{
						Driver:       driverName,
						VolumeHandle: "fs-12345678::fsap-87654321",
						VolumeAttributes: map[string]string{
							"provisioningMode": NamespaceProvisioningMode,
						},
					},
				},
			},
		}

		// Create fake k8s client
		k8sClient := fake.NewSimpleClientset(pv)

		// Create provisioner
		np, err := NewNamespaceProvisioner(mockCloud, k8sClient, fakeRestConfig(), DefaultProvisionerOptions())
		assert.NoError(t, err)

		// Check if PV should have finalizer (it shouldn't for Retain policy)
		shouldHaveFinalizer := np.isEFSNamespaceProvisionedPV(pv) &&
			pv.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimDelete

		assert.False(t, shouldHaveFinalizer)
	})

	t.Run("Handle access point not found during deletion", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCloud := cloudMocks.NewMockCloud(ctrl)
		accessPointId := "fsap-87654321"

		// Create test PV with finalizer and deletion timestamp
		deletionTime := metav1.Now()
		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "test-pv",
				Finalizers:        []string{EFSPVFinalizer},
				DeletionTimestamp: &deletionTime,
			},
			Spec: corev1.PersistentVolumeSpec{
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{
						Driver:       driverName,
						VolumeHandle: "fs-12345678::" + accessPointId,
						VolumeAttributes: map[string]string{
							"provisioningMode": NamespaceProvisioningMode,
						},
					},
				},
			},
		}

		// Create fake k8s client
		k8sClient := fake.NewSimpleClientset(pv)

		// Expect access point deletion to return not found error
		mockCloud.EXPECT().DeleteAccessPoint(gomock.Any(), accessPointId).Return(cloud.ErrNotFound)

		// Create provisioner
		np, err := NewNamespaceProvisioner(mockCloud, k8sClient, fakeRestConfig(), DefaultProvisionerOptions())
		assert.NoError(t, err)

		// Handle PV deletion
		np.handlePVDeletion(ctx, pv)

		// Give some time for async operations
		time.Sleep(100 * time.Millisecond)

		// Verify finalizer was still removed (since access point is already gone)
		updatedPV, err := k8sClient.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
		assert.NoError(t, err)
		assert.NotContains(t, updatedPV.Finalizers, EFSPVFinalizer)
	})
}

func TestIsEFSNamespaceProvisionedPV(t *testing.T) {
	cleanup := setupTestEnv()
	defer cleanup()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCloud := cloudMocks.NewMockCloud(ctrl)
	k8sClient := fake.NewSimpleClientset()

	np, err := NewNamespaceProvisioner(mockCloud, k8sClient, fakeRestConfig(), DefaultProvisionerOptions())
	assert.NoError(t, err)

	tests := []struct {
		name     string
		pv       *corev1.PersistentVolume
		expected bool
	}{
		{
			name: "PV with efs-ns provisioning mode",
			pv: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							Driver:       driverName,
							VolumeHandle: "fs-12345678::fsap-87654321",
							VolumeAttributes: map[string]string{
								"provisioningMode": NamespaceProvisioningMode,
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "PV with filesystem::accesspoint format",
			pv: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							Driver:       driverName,
							VolumeHandle: "fs-12345678::fsap-87654321",
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "PV with just access point ID (backward compatibility)",
			pv: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							Driver:       driverName,
							VolumeHandle: "fsap-87654321",
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "PV with different driver",
			pv: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							Driver:       "ebs.csi.aws.com",
							VolumeHandle: "vol-12345678",
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "PV without CSI",
			pv: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: "/test",
						},
					},
				},
			},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := np.isEFSNamespaceProvisionedPV(tc.pv)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestPVWatcher(t *testing.T) {
	cleanup := setupTestEnv()
	defer cleanup()

	t.Run("PV watcher should start and stop correctly", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockCloud := cloudMocks.NewMockCloud(ctrl)

		// Create fake k8s client with watch reactor
		k8sClient := fake.NewSimpleClientset()
		watcher := watch.NewFake()
		k8sClient.PrependWatchReactor("persistentvolumes", k8stesting.DefaultWatchReactor(watcher, nil))

		// Create provisioner
		np, err := NewNamespaceProvisioner(mockCloud, k8sClient, fakeRestConfig(), DefaultProvisionerOptions())
		assert.NoError(t, err)

		// Start PV watcher
		err = np.startPVWatcher(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, np.pvWatcher)
		assert.NotNil(t, np.pvStopCh)

		// Simulate PV creation event
		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-pv",
			},
			Spec: corev1.PersistentVolumeSpec{
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{
						Driver:       driverName,
						VolumeHandle: "fs-12345678::fsap-87654321",
						VolumeAttributes: map[string]string{
							"provisioningMode": NamespaceProvisioningMode,
						},
					},
				},
			},
		}
		watcher.Add(pv)

		// Give some time for event processing
		time.Sleep(100 * time.Millisecond)

		// Stop the watcher
		close(np.pvStopCh)
		time.Sleep(100 * time.Millisecond)
	})
}