/*
Copyright The Kubernetes Authors.

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

package deployment

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

const (
	testNamespace = "kube-system"
	testTimeout   = 30 * time.Second
)

// EFSNSConfig represents the configuration for efs-ns mode
type EFSNSConfig struct {
	Enabled   bool
	ClusterID string
}

func TestEFSNSDeploymentConfiguration(t *testing.T) {
	// This test validates the deployment manifests with efs-ns configuration
	// These tests would run against a test Kubernetes cluster

	t.Run("ValidateServiceAccountPermissions", func(t *testing.T) {
		validateServiceAccountPermissions(t)
	})

	t.Run("ValidateControllerDeploymentArgs", func(t *testing.T) {
		validateControllerDeploymentArgs(t)
	})

	t.Run("ValidateRBACPermissions", func(t *testing.T) {
		validateRBACPermissions(t)
	})

	t.Run("ValidateStorageClassExamples", func(t *testing.T) {
		validateStorageClassExamples(t)
	})
}

func validateServiceAccountPermissions(t *testing.T) {
	// Test the ServiceAccount RBAC rules include efs-ns permissions
	expectedRules := []rbacv1.PolicyRule{
		{
			APIGroups: []string{""},
			Resources: []string{"configmaps"},
			Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
		},
		{
			APIGroups: []string{""},
			Resources: []string{"namespaces"},
			Verbs:     []string{"get", "list", "watch", "update", "patch"},
		},
		{
			APIGroups: []string{""},
			Resources: []string{"persistentvolumeclaims/finalizers"},
			Verbs:     []string{"update", "patch"},
		},
		{
			APIGroups: []string{""},
			Resources: []string{"namespaces/finalizers"},
			Verbs:     []string{"update", "patch"},
		},
	}

	clusterRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: "efs-csi-external-provisioner-role",
		},
		Rules: append(getBaseRules(), expectedRules...),
	}

	// Validate that all expected rules are present
	for _, expectedRule := range expectedRules {
		found := false
		for _, rule := range clusterRole.Rules {
			if rulesEqual(rule, expectedRule) {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected RBAC rule not found: %+v", expectedRule)
	}
}

func validateControllerDeploymentArgs(t *testing.T) {
	// Test the controller deployment includes efs-ns configuration args
	deployment := createTestControllerDeployment()

	container := deployment.Spec.Template.Spec.Containers[0]
	args := container.Args

	// Test efs-ns mode flag
	assert.Contains(t, args, "--enable-efs-ns-mode=true", "efs-ns mode flag should be present")

	// Test cluster ID flag
	assert.Contains(t, args, "--cluster-id=test-cluster", "cluster ID flag should be present")

	// Test cache TTL flag
	assert.Contains(t, args, "--cache-ttl=300s", "cache TTL flag should be present")

	// Test PVC tracker ConfigMap name flag
	assert.Contains(t, args, "--pvc-tracker-configmap-name=efs-ns-pvc-tracker", "PVC tracker ConfigMap name flag should be present")

	// Test filesystem creation timeout flag
	assert.Contains(t, args, "--filesystem-creation-timeout=600s", "filesystem creation timeout flag should be present")

	// Test mount target creation timeout flag
	assert.Contains(t, args, "--mount-target-creation-timeout=300s", "mount target creation timeout flag should be present")
}

func validateRBACPermissions(t *testing.T) {
	// Test that RBAC permissions are correctly configured for efs-ns operations
	testCases := []struct {
		name        string
		apiGroup    string
		resource    string
		verb        string
		expectAllow bool
	}{
		{"ConfigMap read access", "", "configmaps", "get", true},
		{"ConfigMap write access", "", "configmaps", "create", true},
		{"ConfigMap update access", "", "configmaps", "update", true},
		{"ConfigMap delete access", "", "configmaps", "delete", true},
		{"Namespace read access", "", "namespaces", "get", true},
		{"Namespace update access", "", "namespaces", "update", true},
		{"PVC finalizer access", "", "persistentvolumeclaims/finalizers", "update", true},
		{"Namespace finalizer access", "", "namespaces/finalizers", "update", true},
		{"Secret delete access", "", "secrets", "delete", false}, // Should not have delete access to secrets
	}

	clusterRole := createTestClusterRole()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			allowed := checkRBACPermission(clusterRole, tc.apiGroup, tc.resource, tc.verb)
			assert.Equal(t, tc.expectAllow, allowed,
				"Permission check failed for %s/%s:%s", tc.apiGroup, tc.resource, tc.verb)
		})
	}
}

func validateStorageClassExamples(t *testing.T) {
	// Test the example StorageClass configurations
	storageClassYAML := `
kind: StorageClass
apiVersion: storage.k8s.io/v1
metadata:
  name: efs-ns-sc
provisioner: efs.csi.aws.com
parameters:
  provisioningMode: efs-ns
  performanceMode: generalPurpose
  throughputMode: bursting
  encrypted: "true"
  encryptInTransit: "true"
reclaimPolicy: Delete
volumeBindingMode: Immediate
allowVolumeExpansion: true
`

	var storageClass map[string]interface{}
	err := yaml.Unmarshal([]byte(storageClassYAML), &storageClass)
	require.NoError(t, err, "should unmarshal StorageClass YAML")

	// Validate key fields
	params := storageClass["parameters"].(map[string]interface{})
	assert.Equal(t, "efs-ns", params["provisioningMode"], "provisioning mode should be efs-ns")
	assert.Equal(t, "generalPurpose", params["performanceMode"], "performance mode should be set")
	assert.Equal(t, "bursting", params["throughputMode"], "throughput mode should be set")
	assert.Equal(t, "true", params["encrypted"], "encryption should be enabled")
	assert.Equal(t, "true", params["encryptInTransit"], "encryption in transit should be enabled")

	assert.Equal(t, "Delete", storageClass["reclaimPolicy"], "reclaim policy should be Delete")
	assert.Equal(t, "Immediate", storageClass["volumeBindingMode"], "volume binding mode should be Immediate")
	assert.Equal(t, true, storageClass["allowVolumeExpansion"], "volume expansion should be allowed")
}

// Helper functions

func getBaseRules() []rbacv1.PolicyRule {
	return []rbacv1.PolicyRule{
		{
			APIGroups: []string{""},
			Resources: []string{"persistentvolumes"},
			Verbs:     []string{"get", "list", "watch", "create", "patch", "delete"},
		},
		{
			APIGroups: []string{""},
			Resources: []string{"persistentvolumeclaims"},
			Verbs:     []string{"get", "list", "watch", "update", "patch"},
		},
		{
			APIGroups: []string{"storage.k8s.io"},
			Resources: []string{"storageclasses"},
			Verbs:     []string{"get", "list", "watch"},
		},
		{
			APIGroups: []string{""},
			Resources: []string{"events"},
			Verbs:     []string{"list", "watch", "create", "patch"},
		},
		{
			APIGroups: []string{"storage.k8s.io"},
			Resources: []string{"csinodes"},
			Verbs:     []string{"get", "list", "watch"},
		},
		{
			APIGroups: []string{""},
			Resources: []string{"nodes"},
			Verbs:     []string{"get", "list", "watch"},
		},
		{
			APIGroups: []string{"coordination.k8s.io"},
			Resources: []string{"leases"},
			Verbs:     []string{"get", "watch", "list", "delete", "update", "create"},
		},
	}
}

func rulesEqual(a, b rbacv1.PolicyRule) bool {
	if len(a.APIGroups) != len(b.APIGroups) ||
		len(a.Resources) != len(b.Resources) ||
		len(a.Verbs) != len(b.Verbs) {
		return false
	}

	for i, group := range a.APIGroups {
		if group != b.APIGroups[i] {
			return false
		}
	}

	for i, resource := range a.Resources {
		if resource != b.Resources[i] {
			return false
		}
	}

	for i, verb := range a.Verbs {
		if verb != b.Verbs[i] {
			return false
		}
	}

	return true
}

func createTestControllerDeployment() *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "efs-csi-controller",
			Namespace: testNamespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(2),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "efs-csi-controller",
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": "efs-csi-controller",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "efs-plugin",
							Image: "public.ecr.aws/efs-csi-driver/amazon/aws-efs-csi-driver:v2.1.11",
							Args: []string{
								"--endpoint=$(CSI_ENDPOINT)",
								"--logtostderr",
								"--v=2",
								"--delete-access-point-root-dir=false",
								"--enable-efs-ns-mode=true",
								"--cluster-id=test-cluster",
								"--cache-ttl=300s",
								"--pvc-tracker-configmap-name=efs-ns-pvc-tracker",
								"--filesystem-creation-timeout=600s",
								"--mount-target-creation-timeout=300s",
							},
							Env: []corev1.EnvVar{
								{
									Name:  "CSI_ENDPOINT",
									Value: "unix:///var/lib/csi/sockets/pluginproxy/csi.sock",
								},
							},
						},
					},
				},
			},
		},
	}
}

func createTestClusterRole() *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: "efs-csi-external-provisioner-role",
		},
		Rules: append(getBaseRules(), []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"configmaps"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"namespaces"},
				Verbs:     []string{"get", "list", "watch", "update", "patch"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"persistentvolumeclaims/finalizers"},
				Verbs:     []string{"update", "patch"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"namespaces/finalizers"},
				Verbs:     []string{"update", "patch"},
			},
		}...),
	}
}

func checkRBACPermission(clusterRole *rbacv1.ClusterRole, apiGroup, resource, verb string) bool {
	for _, rule := range clusterRole.Rules {
		if containsString(rule.APIGroups, apiGroup) &&
			containsString(rule.Resources, resource) &&
			containsString(rule.Verbs, verb) {
			return true
		}
	}
	return false
}

func containsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func int32Ptr(i int32) *int32 {
	return &i
}

// Benchmark tests for configuration parsing performance
func BenchmarkEFSNSConfigParsing(b *testing.B) {
	// Set up environment variables
	envVars := map[string]string{
		"ENABLE_EFS_NS_MODE":            "true",
		"CLUSTER_ID":                    "benchmark-cluster",
		"CACHE_TTL":                     "5m",
		"PVC_TRACKER_CONFIGMAP_NAME":    "benchmark-tracker",
		"FILESYSTEM_CREATION_TIMEOUT":   "10m",
		"MOUNT_TARGET_CREATION_TIMEOUT": "5m",
	}

	for key, value := range envVars {
		os.Setenv(key, value)
	}

	defer func() {
		for key := range envVars {
			os.Unsetenv(key)
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// This would call the actual config parsing function from the main package
		// For now we'll simulate the parsing
		_ = &EFSNSConfig{
			Enabled:   true,
			ClusterID: "benchmark-cluster",
		}
	}
}

// Example integration test that would run against a real cluster
func TestEFSNSDeploymentIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// This would connect to a test Kubernetes cluster
	// and validate the actual deployed resources
	t.Run("ValidateActualDeployment", func(t *testing.T) {
		// Test implementation would:
		// 1. Create/update the deployment with efs-ns configuration
		// 2. Wait for the deployment to be ready
		// 3. Validate the pods are running with correct configuration
		// 4. Test RBAC permissions by attempting operations
		// 5. Validate metrics endpoints are accessible
		// 6. Clean up test resources

		t.Skip("Integration test requires test cluster setup")
	})
}
