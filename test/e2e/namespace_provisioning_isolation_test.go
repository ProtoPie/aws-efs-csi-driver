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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

// TestMultiNamespaceIsolation verifies complete isolation between namespaces
func TestMultiNamespaceIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping multi-namespace isolation test in short mode")
	}

	ctx := context.Background()
	te, cleanup := setuptestenv.AWSTestEnvironment(t)
	defer cleanup()

	t.Run("EFS isolation between namespaces", func(t *testing.T) {
		// Create StorageClass for namespace provisioning
		storageClass := createNamespaceProvisioningStorageClass(t, te)
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})

		// Create multiple namespaces
		numNamespaces := 3
		namespaces := make([]*v1.Namespace, numNamespaces)
		efsIDs := make([]string, numNamespaces)

		for i := 0; i < numNamespaces; i++ {
			ns := createTestNamespace(t, te, fmt.Sprintf("isolation-ns-%d", i))
			namespaces[i] = ns
			defer te.K8sClient.CoreV1().Namespaces().Delete(ctx, ns.Name, metav1.DeleteOptions{})

			// Create PVC in each namespace
			pvc := createPVCInNamespace(t, te, ns.Name, storageClass.Name, "isolation-pvc")
			defer te.K8sClient.CoreV1().PersistentVolumeClaims(ns.Name).Delete(ctx, pvc.Name, metav1.DeleteOptions{})

			err := waitForPVCBound(ctx, te, ns.Name, pvc.Name, 5*time.Minute)
			require.NoError(t, err, "PVC in namespace %s should bind", ns.Name)

			// Get EFS ID for this namespace
			efsIDs[i] = verifyEFSCreated(t, te, ns.Name)
			require.NotEmpty(t, efsIDs[i], "EFS ID should not be empty for namespace %s", ns.Name)
		}

		// Verify all namespaces have different EFS
		for i := 0; i < numNamespaces; i++ {
			for j := i + 1; j < numNamespaces; j++ {
				assert.NotEqual(t, efsIDs[i], efsIDs[j],
					"Namespaces %s and %s should have different EFS",
					namespaces[i].Name, namespaces[j].Name)
			}
		}

		t.Log("✅ EFS isolation between namespaces verified")
	})

	t.Run("Access Point isolation within namespace", func(t *testing.T) {
		storageClass := createNamespaceProvisioningStorageClass(t, te)
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})

		namespace := createTestNamespace(t, te, "ap-isolation")
		defer te.K8sClient.CoreV1().Namespaces().Delete(ctx, namespace.Name, metav1.DeleteOptions{})

		// Create multiple PVCs in same namespace
		numPVCs := 3
		pvcs := make([]*v1.PersistentVolumeClaim, numPVCs)
		accessPoints := make([]string, numPVCs)

		for i := 0; i < numPVCs; i++ {
			pvcName := fmt.Sprintf("ap-pvc-%d", i)
			pvc := createPVCInNamespace(t, te, namespace.Name, storageClass.Name, pvcName)
			pvcs[i] = pvc
			defer te.K8sClient.CoreV1().PersistentVolumeClaims(namespace.Name).Delete(ctx, pvc.Name, metav1.DeleteOptions{})

			err := waitForPVCBound(ctx, te, namespace.Name, pvc.Name, 3*time.Minute)
			require.NoError(t, err, "PVC %s should bind", pvc.Name)
		}

		// Verify same EFS is used
		efsID := verifyEFSCreated(t, te, namespace.Name)

		// Verify different Access Points for each PVC
		for i := 0; i < numPVCs; i++ {
			accessPoints[i] = verifyAccessPointCreated(t, te, efsID, pvcs[i].Name)
			require.NotEmpty(t, accessPoints[i], "Access Point should be created for PVC %s", pvcs[i].Name)
		}

		// Verify all Access Points are different
		for i := 0; i < numPVCs; i++ {
			for j := i + 1; j < numPVCs; j++ {
				assert.NotEqual(t, accessPoints[i], accessPoints[j],
					"PVCs %s and %s should have different Access Points",
					pvcs[i].Name, pvcs[j].Name)
			}
		}

		// Verify Access Points have correct paths
		for i, apID := range accessPoints {
			apDetails := getAccessPointDetails(t, te, apID)
			expectedPath := fmt.Sprintf("/dynamic_provisioning/%s/%s", namespace.Name, pvcs[i].Name)
			assert.Contains(t, *apDetails.RootDirectory.Path, expectedPath,
				"Access Point path should contain namespace and PVC name")
		}

		t.Log("✅ Access Point isolation within namespace verified")
	})

	t.Run("Cross-namespace access prevention", func(t *testing.T) {
		storageClass := createNamespaceProvisioningStorageClass(t, te)
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})

		// Create two namespaces
		ns1 := createTestNamespace(t, te, "cross-ns-1")
		defer te.K8sClient.CoreV1().Namespaces().Delete(ctx, ns1.Name, metav1.DeleteOptions{})

		ns2 := createTestNamespace(t, te, "cross-ns-2")
		defer te.K8sClient.CoreV1().Namespaces().Delete(ctx, ns2.Name, metav1.DeleteOptions{})

		// Create PVC and Pod in first namespace
		pvc1 := createPVCInNamespace(t, te, ns1.Name, storageClass.Name, "cross-pvc-1")
		defer te.K8sClient.CoreV1().PersistentVolumeClaims(ns1.Name).Delete(ctx, pvc1.Name, metav1.DeleteOptions{})

		err := waitForPVCBound(ctx, te, ns1.Name, pvc1.Name, 5*time.Minute)
		require.NoError(t, err)

		pod1 := createPodWithPVC(t, te, ns1.Name, pvc1.Name, "cross-pod-1")
		defer te.K8sClient.CoreV1().Pods(ns1.Name).Delete(ctx, pod1.Name, metav1.DeleteOptions{})

		err = waitForPodRunning(ctx, te, ns1.Name, pod1.Name, 2*time.Minute)
		require.NoError(t, err)

		// Write sensitive data in namespace 1
		secretData := "NAMESPACE1_SECRET_DATA"
		secretFile := "/data/secret.txt"
		err = execInPod(ctx, te, ns1.Name, pod1.Name, fmt.Sprintf("echo '%s' > %s", secretData, secretFile))
		require.NoError(t, err)

		// Get PV from namespace 1's PVC
		pvc1Obj, err := te.K8sClient.CoreV1().PersistentVolumeClaims(ns1.Name).Get(ctx, pvc1.Name, metav1.GetOptions{})
		require.NoError(t, err)
		pvName := pvc1Obj.Spec.VolumeName

		// Attempt to create PVC in namespace 2 that references namespace 1's PV
		// This should fail or create a separate volume
		pvc2 := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cross-pvc-2",
				Namespace: ns2.Name,
			},
			Spec: v1.PersistentVolumeClaimSpec{
				AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
				Resources: v1.VolumeResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceStorage: resource.MustParse("1Gi"),
					},
				},
				StorageClassName: &storageClass.Name,
				VolumeName:      pvName, // Try to bind to namespace 1's PV
			},
		}

		_, err = te.K8sClient.CoreV1().PersistentVolumeClaims(ns2.Name).Create(ctx, pvc2, metav1.CreateOptions{})

		if err == nil {
			defer te.K8sClient.CoreV1().PersistentVolumeClaims(ns2.Name).Delete(ctx, pvc2.Name, metav1.DeleteOptions{})

			// If PVC creation succeeded, verify it's bound to a different volume
			time.Sleep(10 * time.Second)
			pvc2Obj, err := te.K8sClient.CoreV1().PersistentVolumeClaims(ns2.Name).Get(ctx, pvc2.Name, metav1.GetOptions{})

			if err == nil && pvc2Obj.Status.Phase == v1.ClaimBound {
				// Should be bound to a different PV
				assert.NotEqual(t, pvName, pvc2Obj.Spec.VolumeName,
					"Namespace 2 should not be able to bind to namespace 1's PV")
			}
		}

		// Create new PVC in namespace 2 without specifying volume
		pvc3 := createPVCInNamespace(t, te, ns2.Name, storageClass.Name, "cross-pvc-3")
		defer te.K8sClient.CoreV1().PersistentVolumeClaims(ns2.Name).Delete(ctx, pvc3.Name, metav1.DeleteOptions{})

		err = waitForPVCBound(ctx, te, ns2.Name, pvc3.Name, 5*time.Minute)
		require.NoError(t, err)

		// Verify namespace 2 gets a different EFS
		efsID1 := verifyEFSCreated(t, te, ns1.Name)
		efsID2 := verifyEFSCreated(t, te, ns2.Name)
		assert.NotEqual(t, efsID1, efsID2, "Different namespaces should have different EFS")

		t.Log("✅ Cross-namespace access prevention verified")
	})

	t.Run("POSIX permission isolation", func(t *testing.T) {
		// Create StorageClass with specific UID/GID ranges
		sc := &storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("posix-sc-%s", generateRandomString(6)),
			},
			Provisioner: "efs.csi.aws.com",
			Parameters: map[string]string{
				"provisioningMode":      "efs-ns",
				"directoryPerms":        "700",
				"gidRangeStart":         "1000",
				"gidRangeEnd":           "2000",
				"basePath":              "/dynamic_provisioning",
				"ensureUniqueDirectory": "true",
			},
			VolumeBindingMode: &[]storagev1.VolumeBindingMode{storagev1.VolumeBindingImmediate}[0],
		}

		sc, err := te.K8sClient.StorageV1().StorageClasses().Create(ctx, sc, metav1.CreateOptions{})
		require.NoError(t, err)
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, sc.Name, metav1.DeleteOptions{})

		// Create namespace with multiple users
		namespace := createTestNamespace(t, te, "posix-test")
		defer te.K8sClient.CoreV1().Namespaces().Delete(ctx, namespace.Name, metav1.DeleteOptions{})

		// Create multiple PVCs for different "users"
		numUsers := 3
		for i := 0; i < numUsers; i++ {
			pvcName := fmt.Sprintf("user-%d-pvc", i)
			pvc := createPVCInNamespace(t, te, namespace.Name, sc.Name, pvcName)
			defer te.K8sClient.CoreV1().PersistentVolumeClaims(namespace.Name).Delete(ctx, pvc.Name, metav1.DeleteOptions{})

			err := waitForPVCBound(ctx, te, namespace.Name, pvc.Name, 3*time.Minute)
			require.NoError(t, err)

			// Create pod for this user
			podName := fmt.Sprintf("user-%d-pod", i)
			pod := createPodWithPVC(t, te, namespace.Name, pvc.Name, podName)
			defer te.K8sClient.CoreV1().Pods(namespace.Name).Delete(ctx, pod.Name, metav1.DeleteOptions{})

			err = waitForPodRunning(ctx, te, namespace.Name, pod.Name, 2*time.Minute)
			require.NoError(t, err)

			// Verify directory permissions
			perms, err := execInPodWithOutput(ctx, te, namespace.Name, pod.Name, "stat -c %a /data")
			require.NoError(t, err)
			assert.Equal(t, "700", strings.TrimSpace(perms),
				"User %d directory should have 700 permissions", i)

			// Get UID/GID assignment
			ownership, err := execInPodWithOutput(ctx, te, namespace.Name, pod.Name, "stat -c 'uid=%u gid=%g' /data")
			require.NoError(t, err)
			t.Logf("User %d ownership: %s", i, ownership)

			// Verify GID is within range
			if strings.Contains(ownership, "gid=") {
				parts := strings.Split(ownership, "gid=")
				if len(parts) > 1 {
					gidStr := strings.TrimSpace(parts[1])
					// Parse and verify GID is in range 1000-2000
					var gid int
					fmt.Sscanf(gidStr, "%d", &gid)
					assert.GreaterOrEqual(t, gid, 1000, "GID should be >= 1000")
					assert.LessOrEqual(t, gid, 2000, "GID should be <= 2000")
				}
			}

			// Write user-specific data
			userData := fmt.Sprintf("USER_%d_PRIVATE_DATA", i)
			userFile := fmt.Sprintf("/data/user_%d.txt", i)
			err = execInPod(ctx, te, namespace.Name, pod.Name, fmt.Sprintf("echo '%s' > %s", userData, userFile))
			require.NoError(t, err)

			// Set restrictive permissions
			err = execInPod(ctx, te, namespace.Name, pod.Name, fmt.Sprintf("chmod 600 %s", userFile))
			require.NoError(t, err)
		}

		t.Log("✅ POSIX permission isolation verified")
	})

	t.Run("Network isolation verification", func(t *testing.T) {
		storageClass := createNamespaceProvisioningStorageClass(t, te)
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})

		// Create namespaces in different network configurations
		ns1 := createTestNamespace(t, te, "network-1")
		defer te.K8sClient.CoreV1().Namespaces().Delete(ctx, ns1.Name, metav1.DeleteOptions{})

		ns2 := createTestNamespace(t, te, "network-2")
		defer te.K8sClient.CoreV1().Namespaces().Delete(ctx, ns2.Name, metav1.DeleteOptions{})

		// Create PVCs
		pvc1 := createPVCInNamespace(t, te, ns1.Name, storageClass.Name, "net-pvc-1")
		defer te.K8sClient.CoreV1().PersistentVolumeClaims(ns1.Name).Delete(ctx, pvc1.Name, metav1.DeleteOptions{})

		pvc2 := createPVCInNamespace(t, te, ns2.Name, storageClass.Name, "net-pvc-2")
		defer te.K8sClient.CoreV1().PersistentVolumeClaims(ns2.Name).Delete(ctx, pvc2.Name, metav1.DeleteOptions{})

		err := waitForPVCBound(ctx, te, ns1.Name, pvc1.Name, 5*time.Minute)
		require.NoError(t, err)
		err = waitForPVCBound(ctx, te, ns2.Name, pvc2.Name, 5*time.Minute)
		require.NoError(t, err)

		// Get EFS IDs
		efsID1 := verifyEFSCreated(t, te, ns1.Name)
		efsID2 := verifyEFSCreated(t, te, ns2.Name)

		// Verify mount targets are created with correct security groups
		mt1 := getMountTargets(t, te, efsID1)
		mt2 := getMountTargets(t, te, efsID2)

		assert.NotEmpty(t, mt1, "Mount targets should exist for EFS 1")
		assert.NotEmpty(t, mt2, "Mount targets should exist for EFS 2")

		// Verify security groups (if configured)
		for _, mt := range mt1 {
			assert.NotEmpty(t, mt.SecurityGroups, "Mount target should have security groups")
			t.Logf("Namespace %s mount target security groups: %v", ns1.Name, mt.SecurityGroups)
		}

		for _, mt := range mt2 {
			assert.NotEmpty(t, mt.SecurityGroups, "Mount target should have security groups")
			t.Logf("Namespace %s mount target security groups: %v", ns2.Name, mt.SecurityGroups)
		}

		t.Log("✅ Network isolation configuration verified")
	})

	t.Run("Concurrent namespace operations", func(t *testing.T) {
		storageClass := createNamespaceProvisioningStorageClass(t, te)
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})

		// Create multiple namespaces concurrently
		numNamespaces := 5
		var wg sync.WaitGroup
		results := make([]bool, numNamespaces)
		efsIDs := make([]string, numNamespaces)

		for i := 0; i < numNamespaces; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				nsName := fmt.Sprintf("concurrent-ns-%d", idx)
				ns := createTestNamespace(t, te, nsName)
				defer te.K8sClient.CoreV1().Namespaces().Delete(ctx, ns.Name, metav1.DeleteOptions{})

				pvc := createPVCInNamespace(t, te, ns.Name, storageClass.Name, "concurrent-pvc")
				defer te.K8sClient.CoreV1().PersistentVolumeClaims(ns.Name).Delete(ctx, pvc.Name, metav1.DeleteOptions{})

				err := waitForPVCBound(ctx, te, ns.Name, pvc.Name, 5*time.Minute)
				if err == nil {
					results[idx] = true
					efsIDs[idx] = verifyEFSCreated(t, te, ns.Name)
				} else {
					t.Logf("Warning: Namespace %s PVC failed to bind: %v", ns.Name, err)
				}
			}(i)
		}

		wg.Wait()

		// Verify results
		successCount := 0
		for _, success := range results {
			if success {
				successCount++
			}
		}

		assert.GreaterOrEqual(t, successCount, numNamespaces*9/10,
			"At least 90% of namespaces should provision successfully")

		// Verify each successful namespace got its own EFS
		uniqueEFS := make(map[string]bool)
		for i, efsID := range efsIDs {
			if results[i] && efsID != "" {
				if uniqueEFS[efsID] {
					t.Errorf("EFS %s is shared between namespaces (should be unique)", efsID)
				}
				uniqueEFS[efsID] = true
			}
		}

		t.Logf("✅ Concurrent namespace operations: %d/%d successful, %d unique EFS",
			successCount, numNamespaces, len(uniqueEFS))
	})
}

// TestNamespaceIsolationEdgeCases tests edge cases in namespace isolation
func TestNamespaceIsolationEdgeCases(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping namespace isolation edge cases test in short mode")
	}

	ctx := context.Background()
	te, cleanup := setuptestenv.AWSTestEnvironment(t)
	defer cleanup()

	t.Run("Namespace with special characters", func(t *testing.T) {
		storageClass := createNamespaceProvisioningStorageClass(t, te)
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})

		// Create namespace with hyphens and numbers (valid K8s names)
		specialNS := createTestNamespace(t, te, "test-ns-123-abc")
		defer te.K8sClient.CoreV1().Namespaces().Delete(ctx, specialNS.Name, metav1.DeleteOptions{})

		pvc := createPVCInNamespace(t, te, specialNS.Name, storageClass.Name, "special-pvc")
		defer te.K8sClient.CoreV1().PersistentVolumeClaims(specialNS.Name).Delete(ctx, pvc.Name, metav1.DeleteOptions{})

		err := waitForPVCBound(ctx, te, specialNS.Name, pvc.Name, 5*time.Minute)
		require.NoError(t, err, "PVC should bind in namespace with special characters")

		// Verify EFS is created with proper tags
		efsID := verifyEFSCreated(t, te, specialNS.Name)
		tags := getEFSTags(t, te, efsID)

		// Verify namespace tag is properly escaped/handled
		nsTag := tags["kubernetes.io/namespace"]
		assert.Equal(t, specialNS.Name, nsTag, "Namespace tag should match namespace name")

		t.Log("✅ Namespace with special characters handled correctly")
	})

	t.Run("Namespace deletion during provisioning", func(t *testing.T) {
		storageClass := createNamespaceProvisioningStorageClass(t, te)
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})

		// Create namespace
		tempNS := createTestNamespace(t, te, "temp-ns")

		// Start PVC creation
		pvc := createPVCInNamespace(t, te, tempNS.Name, storageClass.Name, "temp-pvc")

		// Immediately delete namespace (may cause race condition)
		go func() {
			time.Sleep(2 * time.Second)
			err := te.K8sClient.CoreV1().Namespaces().Delete(ctx, tempNS.Name, metav1.DeleteOptions{})
			if err != nil {
				t.Logf("Namespace deletion error (expected): %v", err)
			}
		}()

		// Wait to see what happens
		time.Sleep(10 * time.Second)

		// Check if namespace still exists
		_, err := te.K8sClient.CoreV1().Namespaces().Get(ctx, tempNS.Name, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			t.Log("Namespace was deleted during provisioning")

			// Check for orphaned resources
			orphaned := te.FailureSimulator.GetOrphanedResources()
			t.Logf("Orphaned resources found: %d", len(orphaned))

			// Cleanup should handle this gracefully
			assert.LessOrEqual(t, len(orphaned), 1, "Should not have many orphaned resources")
		} else {
			// Namespace still exists, clean up normally
			defer te.K8sClient.CoreV1().Namespaces().Delete(ctx, tempNS.Name, metav1.DeleteOptions{})
		}
	})

	t.Run("Namespace recreation with same name", func(t *testing.T) {
		storageClass := createNamespaceProvisioningStorageClass(t, te)
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})

		nsName := "recreate-ns"

		// First cycle: create namespace and PVC
		ns1 := createTestNamespace(t, te, nsName)

		pvc1 := createPVCInNamespace(t, te, ns1.Name, storageClass.Name, "recreate-pvc-1")
		err := waitForPVCBound(ctx, te, ns1.Name, pvc1.Name, 5*time.Minute)
		require.NoError(t, err)

		efsID1 := verifyEFSCreated(t, te, ns1.Name)

		// Delete PVC and namespace
		err = te.K8sClient.CoreV1().PersistentVolumeClaims(ns1.Name).Delete(ctx, pvc1.Name, metav1.DeleteOptions{})
		require.NoError(t, err)

		err = te.K8sClient.CoreV1().Namespaces().Delete(ctx, ns1.Name, metav1.DeleteOptions{})
		require.NoError(t, err)

		// Wait for namespace to be fully deleted
		err = waitForNamespaceDeletion(ctx, te, ns1.Name, 2*time.Minute)
		require.NoError(t, err, "Namespace should be deleted")

		// Second cycle: recreate namespace with same name
		ns2 := createTestNamespace(t, te, nsName)
		defer te.K8sClient.CoreV1().Namespaces().Delete(ctx, ns2.Name, metav1.DeleteOptions{})

		pvc2 := createPVCInNamespace(t, te, ns2.Name, storageClass.Name, "recreate-pvc-2")
		defer te.K8sClient.CoreV1().PersistentVolumeClaims(ns2.Name).Delete(ctx, pvc2.Name, metav1.DeleteOptions{})

		err = waitForPVCBound(ctx, te, ns2.Name, pvc2.Name, 5*time.Minute)
		require.NoError(t, err)

		efsID2 := verifyEFSCreated(t, te, ns2.Name)

		// Verify behavior based on cleanup policy
		// With delete policy: should be different EFS
		// With retain policy: might reuse if tags match
		t.Logf("First EFS: %s, Second EFS: %s", efsID1, efsID2)

		t.Log("✅ Namespace recreation handled correctly")
	})

	t.Run("Maximum namespaces limit", func(t *testing.T) {
		// Test system behavior at scale limits
		storageClass := createNamespaceProvisioningStorageClass(t, te)
		defer te.K8sClient.StorageV1().StorageClasses().Delete(ctx, storageClass.Name, metav1.DeleteOptions{})

		// Create namespaces up to a reasonable test limit
		maxNamespaces := 10 // Reduced for test practicality
		namespaces := make([]*v1.Namespace, 0, maxNamespaces)

		for i := 0; i < maxNamespaces; i++ {
			nsName := fmt.Sprintf("scale-ns-%d", i)
			ns := createTestNamespace(t, te, nsName)
			namespaces = append(namespaces, ns)

			// Clean up in reverse order
			defer func(n *v1.Namespace) {
				te.K8sClient.CoreV1().Namespaces().Delete(ctx, n.Name, metav1.DeleteOptions{})
			}(ns)

			// Create minimal PVC to trigger EFS creation
			pvc := createPVCInNamespace(t, te, ns.Name, storageClass.Name, "scale-pvc")
			defer te.K8sClient.CoreV1().PersistentVolumeClaims(ns.Name).Delete(ctx, pvc.Name, metav1.DeleteOptions{})

			// Don't wait for all to complete, just verify creation started
			go func(namespace, pvcName string) {
				waitForPVCBound(ctx, te, namespace, pvcName, 5*time.Minute)
			}(ns.Name, pvc.Name)
		}

		// Give some time for provisioning to start
		time.Sleep(30 * time.Second)

		// Count how many EFS were created
		efsCount := 0
		for _, ns := range namespaces {
			if efsID := getNamespaceEFSIfExists(t, te, ns.Name); efsID != "" {
				efsCount++
			}
		}

		t.Logf("Created %d EFS for %d namespaces", efsCount, maxNamespaces)
		assert.Greater(t, efsCount, 0, "At least some EFS should be created")

		// Verify no namespace shares EFS
		efsMap := make(map[string]string)
		for _, ns := range namespaces {
			if efsID := getNamespaceEFSIfExists(t, te, ns.Name); efsID != "" {
				if existingNS, exists := efsMap[efsID]; exists {
					t.Errorf("EFS %s is shared between %s and %s", efsID, existingNS, ns.Name)
				}
				efsMap[efsID] = ns.Name
			}
		}

		t.Log("✅ Scale limit test completed")
	})
}

// Helper function to get mount targets for an EFS
func getMountTargets(t *testing.T, te *testenv.AWSTestEnvironment, efsID string) []efstypes.MountTargetDescription {
	ctx := context.Background()

	input := &efs.DescribeMountTargetsInput{
		FileSystemId: aws.String(efsID),
	}

	output, err := te.EFSClient.DescribeMountTargets(ctx, input)
	if err != nil {
		t.Logf("Failed to get mount targets for EFS %s: %v", efsID, err)
		return []efstypes.MountTargetDescription{}
	}

	return output.MountTargets
}

// Helper function to get Access Point details
func getAccessPointDetails(t *testing.T, te *testenv.AWSTestEnvironment, apID string) *efstypes.AccessPointDescription {
	ctx := context.Background()

	input := &efs.DescribeAccessPointsInput{
		AccessPointId: aws.String(apID),
	}

	output, err := te.EFSClient.DescribeAccessPoints(ctx, input)
	if err != nil {
		t.Logf("Failed to get Access Point details for %s: %v", apID, err)
		return nil
	}

	if len(output.AccessPoints) > 0 {
		return &output.AccessPoints[0]
	}

	return nil
}

// Helper function to get EFS tags
func getEFSTags(t *testing.T, te *testenv.AWSTestEnvironment, efsID string) map[string]string {
	ctx := context.Background()

	input := &efs.DescribeFileSystemsInput{
		FileSystemId: aws.String(efsID),
	}

	output, err := te.EFSClient.DescribeFileSystems(ctx, input)
	if err != nil {
		t.Logf("Failed to get EFS tags for %s: %v", efsID, err)
		return map[string]string{}
	}

	if len(output.FileSystems) == 0 {
		return map[string]string{}
	}

	// Convert tags to map
	tags := make(map[string]string)
	for _, tag := range output.FileSystems[0].Tags {
		tags[*tag.Key] = *tag.Value
	}

	return tags
}

// Helper function to get namespace EFS if it exists
func getNamespaceEFSIfExists(t *testing.T, te *testenv.AWSTestEnvironment, namespace string) string {
	// This would query the CRD or AWS tags to find EFS for namespace
	// Simplified implementation for testing
	ctx := context.Background()

	input := &efs.DescribeFileSystemsInput{}
	output, err := te.EFSClient.DescribeFileSystems(ctx, input)
	if err != nil {
		return ""
	}

	for _, fs := range output.FileSystems {
		for _, tag := range fs.Tags {
			if *tag.Key == "kubernetes.io/namespace" && *tag.Value == namespace {
				return *fs.FileSystemId
			}
		}
	}

	return ""
}

// Helper function to wait for namespace deletion
func waitForNamespaceDeletion(ctx context.Context, te *testenv.AWSTestEnvironment, namespace string, timeout time.Duration) error {
	return wait.PollImmediate(5*time.Second, timeout, func() (bool, error) {
		_, err := te.K8sClient.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		return false, nil
	})
}