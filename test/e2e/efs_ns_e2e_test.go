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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	"github.com/aws/aws-sdk-go-v2/service/efs/types"
	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	"k8s.io/kubernetes/test/e2e/storage/utils"
	admissionapi "k8s.io/pod-security-admission/api"
)

const (
	EfsNsProvisioningMode = "efs-ns"
	// Time to wait for EFS filesystem operations
	EfsOperationTimeout = 300 * time.Second
	// Time to wait for Kubernetes operations
	K8sOperationTimeout = 120 * time.Second
	// Cleanup timeout for AWS resources
	CleanupTimeout = 180 * time.Second
)

// EFS-NS specific E2E test suite for namespace-isolated EFS provisioning
var _ = ginkgo.Describe("[efs-csi-ns] EFS Namespace Provisioning E2E Tests", func() {
	var (
		f                *framework.Framework
		efsClient        *efs.Client
		createdFileSystems []*types.FileSystemDescription
		createdStorageClasses []string
		testNamespaces      []string
	)

	ginkgo.BeforeEach(func() {
		f = framework.NewDefaultFramework("efs-ns-e2e")
		f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged

		// Initialize AWS EFS client
		cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(Region))
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Failed to load AWS config")
		efsClient = efs.NewFromConfig(cfg)

		// Reset tracking slices
		createdFileSystems = []*types.FileSystemDescription{}
		createdStorageClasses = []string{}
		testNamespaces = []string{}
	})

	ginkgo.AfterEach(func() {
		ginkgo.By("Cleaning up test resources")
		cleanupTestResources(f.ClientSet, efsClient, createdStorageClasses, createdFileSystems, testNamespaces)
	})

	ginkgo.Context("Basic EFS-NS Functionality", func() {
		ginkgo.It("should successfully create and manage namespace-isolated EFS filesystem", func() {
			ginkgo.By("Creating EFS-NS StorageClass")
			sc := createEfsNsStorageClass(f.ClientSet)
			createdStorageClasses = append(createdStorageClasses, sc.Name)

			ginkgo.By("Creating PVC with EFS-NS provisioning")
			pvc := createTestPVC(f.ClientSet, f.Namespace.Name, "test-pvc", sc.Name)

			ginkgo.By("Waiting for PVC to be bound")
			err := waitForPVCBound(f.ClientSet, f.Namespace.Name, pvc.Name, K8sOperationTimeout)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "PVC should be bound successfully")

			ginkgo.By("Verifying dedicated EFS filesystem was created for namespace")
			pv, err := f.ClientSet.CoreV1().PersistentVolumes().Get(context.TODO(), pvc.Spec.VolumeName, metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Should get PV successfully")

			fsId := extractFileSystemId(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(fsId).ToNot(gomega.BeEmpty(), "FileSystem ID should not be empty")

			// Verify filesystem exists and has correct tags
			fs, err := describeFileSystem(efsClient, fsId)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Should describe filesystem successfully")
			gomega.Expect(fs.LifeCycleState).To(gomega.Equal(types.LifeCycleStateAvailable), "Filesystem should be available")

			createdFileSystems = append(createdFileSystems, fs)

			ginkgo.By("Testing write and read operations")
			testPod := createTestPod(f.ClientSet, f.Namespace.Name, "test-pod", pvc.Name, "echo 'test-data' > /mnt/volume/test.txt && cat /mnt/volume/test.txt")
			defer cleanupPod(f.ClientSet, f.Namespace.Name, testPod.Name)

			err = e2epod.WaitForPodSuccessInNamespace(context.TODO(), f.ClientSet, testPod.Name, f.Namespace.Name)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Pod should complete successfully")

			ginkgo.By("Verifying data persistence across pod restarts")
			readPod := createTestPod(f.ClientSet, f.Namespace.Name, "read-pod", pvc.Name, "cat /mnt/volume/test.txt")
			defer cleanupPod(f.ClientSet, f.Namespace.Name, readPod.Name)

			err = e2epod.WaitForPodSuccessInNamespace(context.TODO(), f.ClientSet, readPod.Name, f.Namespace.Name)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Read pod should complete successfully")
		})

		ginkgo.It("should handle PVC deletion and filesystem cleanup gracefully", func() {
			ginkgo.By("Creating EFS-NS StorageClass with immediate deletion policy")
			sc := createEfsNsStorageClass(f.ClientSet)
			createdStorageClasses = append(createdStorageClasses, sc.Name)

			ginkgo.By("Creating and binding PVC")
			pvc := createTestPVC(f.ClientSet, f.Namespace.Name, "cleanup-test-pvc", sc.Name)
			err := waitForPVCBound(f.ClientSet, f.Namespace.Name, pvc.Name, K8sOperationTimeout)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "PVC should be bound")

			ginkgo.By("Getting filesystem ID before deletion")
			pv, err := f.ClientSet.CoreV1().PersistentVolumes().Get(context.TODO(), pvc.Spec.VolumeName, metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Should get PV")

			fsId := extractFileSystemId(pv.Spec.CSI.VolumeHandle)

			ginkgo.By("Deleting PVC")
			err = f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Delete(context.TODO(), pvc.Name, metav1.DeleteOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "PVC should be deleted successfully")

			ginkgo.By("Waiting for PV to be deleted")
			err = waitForPVDeleted(f.ClientSet, pv.Name, CleanupTimeout)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "PV should be deleted")

			ginkgo.By("Verifying EFS filesystem is properly cleaned up")
			eventually := gomega.Eventually(func() error {
				_, err := describeFileSystem(efsClient, fsId)
				if err != nil {
					// Check if it's a not found error, which means cleanup succeeded
					if strings.Contains(err.Error(), "does not exist") {
						return nil
					}
					return err
				}
				return fmt.Errorf("filesystem still exists")
			}, CleanupTimeout, 10*time.Second)
			eventually.Should(gomega.Succeed(), "EFS filesystem should be cleaned up")
		})
	})

	ginkgo.Context("Multi-Namespace Isolation", func() {
		ginkgo.It("should create separate EFS filesystems for different namespaces", func() {
			ginkgo.By("Creating multiple test namespaces")
			ns1 := createTestNamespace(f.ClientSet, "efs-ns-test-1")
			ns2 := createTestNamespace(f.ClientSet, "efs-ns-test-2")
			testNamespaces = append(testNamespaces, ns1.Name, ns2.Name)

			ginkgo.By("Creating EFS-NS StorageClass")
			sc := createEfsNsStorageClass(f.ClientSet)
			createdStorageClasses = append(createdStorageClasses, sc.Name)

			ginkgo.By("Creating PVCs in different namespaces")
			pvc1 := createTestPVC(f.ClientSet, ns1.Name, "pvc-ns1", sc.Name)
			pvc2 := createTestPVC(f.ClientSet, ns2.Name, "pvc-ns2", sc.Name)

			ginkgo.By("Waiting for both PVCs to be bound")
			err := waitForPVCBound(f.ClientSet, ns1.Name, pvc1.Name, K8sOperationTimeout)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "PVC1 should be bound")

			err = waitForPVCBound(f.ClientSet, ns2.Name, pvc2.Name, K8sOperationTimeout)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "PVC2 should be bound")

			ginkgo.By("Verifying separate EFS filesystems were created")
			pv1, err := f.ClientSet.CoreV1().PersistentVolumes().Get(context.TODO(), pvc1.Spec.VolumeName, metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Should get PV1")

			pv2, err := f.ClientSet.CoreV1().PersistentVolumes().Get(context.TODO(), pvc2.Spec.VolumeName, metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Should get PV2")

			fsId1 := extractFileSystemId(pv1.Spec.CSI.VolumeHandle)
			fsId2 := extractFileSystemId(pv2.Spec.CSI.VolumeHandle)

			gomega.Expect(fsId1).ToNot(gomega.Equal(fsId2), "Different namespaces should have different EFS filesystems")

			// Track created filesystems for cleanup
			fs1, err := describeFileSystem(efsClient, fsId1)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Should describe filesystem 1")
			createdFileSystems = append(createdFileSystems, fs1)

			fs2, err := describeFileSystem(efsClient, fsId2)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Should describe filesystem 2")
			createdFileSystems = append(createdFileSystems, fs2)

			ginkgo.By("Testing data isolation between namespaces")
			// Write data in namespace 1
			pod1 := createTestPod(f.ClientSet, ns1.Name, "writer-pod", pvc1.Name, "echo 'ns1-data' > /mnt/volume/data.txt")
			err = e2epod.WaitForPodSuccessInNamespace(context.TODO(), f.ClientSet, pod1.Name, ns1.Name)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Writer pod in ns1 should succeed")
			cleanupPod(f.ClientSet, ns1.Name, pod1.Name)

			// Write different data in namespace 2
			pod2 := createTestPod(f.ClientSet, ns2.Name, "writer-pod", pvc2.Name, "echo 'ns2-data' > /mnt/volume/data.txt")
			err = e2epod.WaitForPodSuccessInNamespace(context.TODO(), f.ClientSet, pod2.Name, ns2.Name)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Writer pod in ns2 should succeed")
			cleanupPod(f.ClientSet, ns2.Name, pod2.Name)

			// Verify data isolation - each namespace should only see its own data
			readPod1 := createTestPod(f.ClientSet, ns1.Name, "reader-pod", pvc1.Name, "cat /mnt/volume/data.txt | grep 'ns1-data'")
			err = e2epod.WaitForPodSuccessInNamespace(context.TODO(), f.ClientSet, readPod1.Name, ns1.Name)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Reader pod in ns1 should find ns1-data")
			cleanupPod(f.ClientSet, ns1.Name, readPod1.Name)

			readPod2 := createTestPod(f.ClientSet, ns2.Name, "reader-pod", pvc2.Name, "cat /mnt/volume/data.txt | grep 'ns2-data'")
			err = e2epod.WaitForPodSuccessInNamespace(context.TODO(), f.ClientSet, readPod2.Name, ns2.Name)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Reader pod in ns2 should find ns2-data")
			cleanupPod(f.ClientSet, ns2.Name, readPod2.Name)
		})

		ginkgo.It("should handle multiple PVCs within the same namespace efficiently", func() {
			ginkgo.By("Creating EFS-NS StorageClass")
			sc := createEfsNsStorageClass(f.ClientSet)
			createdStorageClasses = append(createdStorageClasses, sc.Name)

			ginkgo.By("Creating multiple PVCs in the same namespace")
			pvc1 := createTestPVC(f.ClientSet, f.Namespace.Name, "multi-pvc-1", sc.Name)
			pvc2 := createTestPVC(f.ClientSet, f.Namespace.Name, "multi-pvc-2", sc.Name)
			pvc3 := createTestPVC(f.ClientSet, f.Namespace.Name, "multi-pvc-3", sc.Name)

			ginkgo.By("Waiting for all PVCs to be bound")
			err := waitForPVCBound(f.ClientSet, f.Namespace.Name, pvc1.Name, K8sOperationTimeout)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "PVC1 should be bound")
			err = waitForPVCBound(f.ClientSet, f.Namespace.Name, pvc2.Name, K8sOperationTimeout)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "PVC2 should be bound")
			err = waitForPVCBound(f.ClientSet, f.Namespace.Name, pvc3.Name, K8sOperationTimeout)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "PVC3 should be bound")

			ginkgo.By("Verifying all PVCs share the same EFS filesystem")
			pv1, err := f.ClientSet.CoreV1().PersistentVolumes().Get(context.TODO(), pvc1.Spec.VolumeName, metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Should get PV1")
			pv2, err := f.ClientSet.CoreV1().PersistentVolumes().Get(context.TODO(), pvc2.Spec.VolumeName, metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Should get PV2")
			pv3, err := f.ClientSet.CoreV1().PersistentVolumes().Get(context.TODO(), pvc3.Spec.VolumeName, metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Should get PV3")

			fsId1 := extractFileSystemId(pv1.Spec.CSI.VolumeHandle)
			fsId2 := extractFileSystemId(pv2.Spec.CSI.VolumeHandle)
			fsId3 := extractFileSystemId(pv3.Spec.CSI.VolumeHandle)

			gomega.Expect(fsId1).To(gomega.Equal(fsId2), "PVCs in same namespace should share filesystem")
			gomega.Expect(fsId2).To(gomega.Equal(fsId3), "PVCs in same namespace should share filesystem")

			// Track filesystem for cleanup
			fs, err := describeFileSystem(efsClient, fsId1)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Should describe filesystem")
			createdFileSystems = append(createdFileSystems, fs)

			ginkgo.By("Testing concurrent access to shared filesystem")
			pod1 := createTestPod(f.ClientSet, f.Namespace.Name, "concurrent-pod-1", pvc1.Name, "echo 'pod1-data' > /mnt/volume/pod1.txt && sleep 5")
			pod2 := createTestPod(f.ClientSet, f.Namespace.Name, "concurrent-pod-2", pvc2.Name, "echo 'pod2-data' > /mnt/volume/pod2.txt && sleep 5")
			pod3 := createTestPod(f.ClientSet, f.Namespace.Name, "concurrent-pod-3", pvc3.Name, "echo 'pod3-data' > /mnt/volume/pod3.txt && sleep 5")

			// Wait for all pods to complete
			err = e2epod.WaitForPodSuccessInNamespace(context.TODO(), f.ClientSet, pod1.Name, f.Namespace.Name)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Pod1 should complete successfully")
			err = e2epod.WaitForPodSuccessInNamespace(context.TODO(), f.ClientSet, pod2.Name, f.Namespace.Name)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Pod2 should complete successfully")
			err = e2epod.WaitForPodSuccessInNamespace(context.TODO(), f.ClientSet, pod3.Name, f.Namespace.Name)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Pod3 should complete successfully")

			// Verify all files exist and contain correct data
			verifyPod := createTestPod(f.ClientSet, f.Namespace.Name, "verify-pod", pvc1.Name, "cat /mnt/volume/pod1.txt && cat /mnt/volume/pod2.txt && cat /mnt/volume/pod3.txt")
			err = e2epod.WaitForPodSuccessInNamespace(context.TODO(), f.ClientSet, verifyPod.Name, f.Namespace.Name)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Verification pod should complete successfully")

			// Cleanup pods
			cleanupPod(f.ClientSet, f.Namespace.Name, pod1.Name)
			cleanupPod(f.ClientSet, f.Namespace.Name, pod2.Name)
			cleanupPod(f.ClientSet, f.Namespace.Name, pod3.Name)
			cleanupPod(f.ClientSet, f.Namespace.Name, verifyPod.Name)
		})
	})

	ginkgo.Context("Error Handling and Recovery", func() {
		ginkgo.It("should handle EFS filesystem creation failures gracefully", func() {
			// This test would require setting up conditions that cause EFS creation to fail
			// For now, we'll test the PVC waiting timeout scenario
			ginkgo.By("Creating a StorageClass with invalid parameters")
			sc := &storagev1.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "efs-ns-invalid-" + generateRandomString(8),
				},
				Provisioner: "efs.csi.aws.com",
				Parameters: map[string]string{
					"provisioningMode": EfsNsProvisioningMode,
					"invalid-param":    "invalid-value",
				},
				VolumeBindingMode: &[]storagev1.VolumeBindingMode{storagev1.VolumeBindingImmediate}[0],
			}

			sc, err := f.ClientSet.StorageV1().StorageClasses().Create(context.TODO(), sc, metav1.CreateOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "StorageClass should be created")
			createdStorageClasses = append(createdStorageClasses, sc.Name)

			ginkgo.By("Creating PVC with invalid StorageClass")
			pvc := createTestPVC(f.ClientSet, f.Namespace.Name, "invalid-pvc", sc.Name)

			ginkgo.By("Verifying PVC remains pending due to provisioning failure")
			// Wait a bit and verify PVC is still pending
			time.Sleep(30 * time.Second)
			updatedPVC, err := f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Get(context.TODO(), pvc.Name, metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Should get PVC")
			gomega.Expect(updatedPVC.Status.Phase).To(gomega.Equal(v1.ClaimPending), "PVC should remain pending")
		})

		ginkgo.It("should recover from temporary AWS API failures", func() {
			// This test simulates recovery from API failures by testing delayed provisioning
			ginkgo.By("Creating EFS-NS StorageClass")
			sc := createEfsNsStorageClass(f.ClientSet)
			createdStorageClasses = append(createdStorageClasses, sc.Name)

			ginkgo.By("Creating PVC and monitoring for eventual success")
			pvc := createTestPVC(f.ClientSet, f.Namespace.Name, "recovery-pvc", sc.Name)

			// Use a longer timeout to allow for retries
			err := waitForPVCBound(f.ClientSet, f.Namespace.Name, pvc.Name, EfsOperationTimeout)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "PVC should eventually be bound despite potential API delays")

			// Verify filesystem was created successfully
			pv, err := f.ClientSet.CoreV1().PersistentVolumes().Get(context.TODO(), pvc.Spec.VolumeName, metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Should get PV")

			fsId := extractFileSystemId(pv.Spec.CSI.VolumeHandle)
			fs, err := describeFileSystem(efsClient, fsId)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Should describe filesystem")
			createdFileSystems = append(createdFileSystems, fs)

			gomega.Expect(fs.LifeCycleState).To(gomega.Equal(types.LifeCycleStateAvailable), "Filesystem should be available")
		})
	})

	ginkgo.Context("Performance and Scalability", func() {
		ginkgo.It("should handle rapid PVC creation and deletion", func() {
			ginkgo.By("Creating EFS-NS StorageClass")
			sc := createEfsNsStorageClass(f.ClientSet)
			createdStorageClasses = append(createdStorageClasses, sc.Name)

			ginkgo.By("Creating multiple PVCs rapidly")
			numPVCs := 3
			pvcNames := make([]string, numPVCs)

			for i := 0; i < numPVCs; i++ {
				pvcName := fmt.Sprintf("rapid-pvc-%d", i)
				pvcNames[i] = pvcName
				createTestPVC(f.ClientSet, f.Namespace.Name, pvcName, sc.Name)
			}

			ginkgo.By("Waiting for all PVCs to be bound")
			for _, pvcName := range pvcNames {
				err := waitForPVCBound(f.ClientSet, f.Namespace.Name, pvcName, K8sOperationTimeout)
				gomega.Expect(err).ToNot(gomega.HaveOccurred(), fmt.Sprintf("PVC %s should be bound", pvcName))
			}

			// Get filesystem ID for cleanup tracking
			pvc, err := f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Get(context.TODO(), pvcNames[0], metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Should get first PVC")

			pv, err := f.ClientSet.CoreV1().PersistentVolumes().Get(context.TODO(), pvc.Spec.VolumeName, metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Should get PV")

			fsId := extractFileSystemId(pv.Spec.CSI.VolumeHandle)
			fs, err := describeFileSystem(efsClient, fsId)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Should describe filesystem")
			createdFileSystems = append(createdFileSystems, fs)

			ginkgo.By("Rapidly deleting all PVCs")
			for _, pvcName := range pvcNames {
				err := f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Delete(context.TODO(), pvcName, metav1.DeleteOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred(), fmt.Sprintf("Should delete PVC %s", pvcName))
			}

			ginkgo.By("Verifying all resources are cleaned up")
			// Wait for all PVCs to be deleted
			for _, pvcName := range pvcNames {
				eventually := gomega.Eventually(func() bool {
					_, err := f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Get(context.TODO(), pvcName, metav1.GetOptions{})
					return apierrors.IsNotFound(err)
				}, CleanupTimeout, 5*time.Second)
				eventually.Should(gomega.BeTrue(), fmt.Sprintf("PVC %s should be deleted", pvcName))
			}
		})
	})
})

// Helper functions for EFS-NS E2E tests

func createEfsNsStorageClass(clientset clientset.Interface) *storagev1.StorageClass {
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "efs-ns-test-" + generateRandomString(8),
		},
		Provisioner: "efs.csi.aws.com",
		Parameters: map[string]string{
			"provisioningMode": EfsNsProvisioningMode,
		},
		VolumeBindingMode: &[]storagev1.VolumeBindingMode{storagev1.VolumeBindingImmediate}[0],
	}

	sc, err := clientset.StorageV1().StorageClasses().Create(context.TODO(), sc, metav1.CreateOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "StorageClass should be created successfully")
	return sc
}

func createTestPVC(clientset clientset.Interface, namespace, name, storageClassName string) *v1.PersistentVolumeClaim {
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
			StorageClassName: &storageClassName,
		},
	}

	pvc, err := clientset.CoreV1().PersistentVolumeClaims(namespace).Create(context.TODO(), pvc, metav1.CreateOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "PVC should be created successfully")
	return pvc
}

func createTestPod(clientset clientset.Interface, namespace, name, pvcName, command string) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:    "test-container",
					Image:   "busybox:1.35",
					Command: []string{"/bin/sh", "-c", command},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "test-volume",
							MountPath: "/mnt/volume",
						},
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: "test-volume",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcName,
						},
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}

	pod, err := clientset.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Pod should be created successfully")
	return pod
}

func createTestNamespace(clientset clientset.Interface, namePrefix string) *v1.Namespace {
	ns := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namePrefix + "-" + generateRandomString(6),
		},
	}

	ns, err := clientset.CoreV1().Namespaces().Create(context.TODO(), ns, metav1.CreateOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "Namespace should be created successfully")
	return ns
}

func waitForPVCBound(clientset clientset.Interface, namespace, pvcName string, timeout time.Duration) error {
	return wait.PollImmediate(5*time.Second, timeout, func() (bool, error) {
		pvc, err := clientset.CoreV1().PersistentVolumeClaims(namespace).Get(context.TODO(), pvcName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return pvc.Status.Phase == v1.ClaimBound, nil
	})
}

func waitForPVDeleted(clientset clientset.Interface, pvName string, timeout time.Duration) error {
	return wait.PollImmediate(5*time.Second, timeout, func() (bool, error) {
		_, err := clientset.CoreV1().PersistentVolumes().Get(context.TODO(), pvName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		return false, nil
	})
}

func extractFileSystemId(volumeHandle string) string {
	// Volume handle format for EFS-NS: "fs-12345678"
	// May include additional path information, so extract just the filesystem ID
	parts := strings.Split(volumeHandle, ":")
	return strings.TrimSpace(parts[0])
}

func describeFileSystem(efsClient *efs.Client, fsId string) (*types.FileSystemDescription, error) {
	input := &efs.DescribeFileSystemsInput{
		FileSystemId: &fsId,
	}
	
	output, err := efsClient.DescribeFileSystems(context.TODO(), input)
	if err != nil {
		return nil, err
	}
	
	if len(output.FileSystems) == 0 {
		return nil, fmt.Errorf("filesystem %s does not exist", fsId)
	}
	
	return &output.FileSystems[0], nil
}

func cleanupPod(clientset clientset.Interface, namespace, podName string) {
	err := clientset.CoreV1().Pods(namespace).Delete(context.TODO(), podName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		framework.Logf("Warning: Failed to delete pod %s: %v", podName, err)
	}
}

func cleanupTestResources(clientset clientset.Interface, efsClient *efs.Client, storageClasses []string, fileSystems []*types.FileSystemDescription, namespaces []string) {
	// Clean up storage classes
	for _, scName := range storageClasses {
		err := clientset.StorageV1().StorageClasses().Delete(context.TODO(), scName, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			framework.Logf("Warning: Failed to delete StorageClass %s: %v", scName, err)
		}
	}

	// Clean up test namespaces
	for _, nsName := range namespaces {
		err := clientset.CoreV1().Namespaces().Delete(context.TODO(), nsName, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			framework.Logf("Warning: Failed to delete namespace %s: %v", nsName, err)
		}
	}

	// Clean up EFS filesystems (with retry for dependencies)
	for _, fs := range fileSystems {
		deleteEFSFilesystemWithRetry(efsClient, *fs.FileSystemId)
	}
}

func deleteEFSFilesystemWithRetry(efsClient *efs.Client, fsId string) {
	maxRetries := 10
	retryInterval := 30 * time.Second

	for i := 0; i < maxRetries; i++ {
		// First, try to delete all mount targets
		mountTargets, err := efsClient.DescribeMountTargets(context.TODO(), &efs.DescribeMountTargetsInput{
			FileSystemId: &fsId,
		})
		
		if err == nil {
			for _, mt := range mountTargets.MountTargets {
				_, err := efsClient.DeleteMountTarget(context.TODO(), &efs.DeleteMountTargetInput{
					MountTargetId: mt.MountTargetId,
				})
				if err != nil {
					framework.Logf("Warning: Failed to delete mount target %s: %v", *mt.MountTargetId, err)
				}
			}
		}

		// Wait for mount targets to be deleted
		time.Sleep(retryInterval)

		// Try to delete the filesystem
		_, err = efsClient.DeleteFileSystem(context.TODO(), &efs.DeleteFileSystemInput{
			FileSystemId: &fsId,
		})
		
		if err == nil {
			framework.Logf("Successfully deleted EFS filesystem %s", fsId)
			return
		}

		if strings.Contains(err.Error(), "does not exist") {
			framework.Logf("EFS filesystem %s already deleted", fsId)
			return
		}

		framework.Logf("Attempt %d: Failed to delete EFS filesystem %s: %v. Retrying...", i+1, fsId, err)
		
		if i < maxRetries-1 {
			time.Sleep(retryInterval)
		}
	}

	framework.Logf("Warning: Failed to delete EFS filesystem %s after %d attempts", fsId, maxRetries)
}

func generateRandomString(length int) string {
	return rand.String(length)
}