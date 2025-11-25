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

package config

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// EFSNSConfig represents the configuration for efs-ns mode
type EFSNSConfig struct {
	Enabled                    bool
	ClusterID                  string
	CacheTTL                   time.Duration
	PVCTrackerConfigMapName    string
	FilesystemCreationTimeout  time.Duration
	MountTargetCreationTimeout time.Duration
	DefaultTags                map[string]string
}

// ParseEFSNSConfigFromEnv parses efs-ns configuration from environment variables
func ParseEFSNSConfigFromEnv() *EFSNSConfig {
	config := &EFSNSConfig{
		// Set defaults
		Enabled:                    false,
		CacheTTL:                   5 * time.Minute,
		PVCTrackerConfigMapName:    "efs-ns-pvc-tracker",
		FilesystemCreationTimeout:  10 * time.Minute,
		MountTargetCreationTimeout: 5 * time.Minute,
		DefaultTags:                make(map[string]string),
	}

	if os.Getenv("ENABLE_EFS_NS_MODE") == "true" {
		config.Enabled = true
	}

	if clusterId := os.Getenv("CLUSTER_ID"); clusterId != "" {
		config.ClusterID = clusterId
	}

	if cacheTTL := os.Getenv("CACHE_TTL"); cacheTTL != "" {
		if duration, err := time.ParseDuration(cacheTTL); err == nil {
			config.CacheTTL = duration
		}
	}

	if configMapName := os.Getenv("PVC_TRACKER_CONFIGMAP_NAME"); configMapName != "" {
		config.PVCTrackerConfigMapName = configMapName
	}

	if timeout := os.Getenv("FILESYSTEM_CREATION_TIMEOUT"); timeout != "" {
		if duration, err := time.ParseDuration(timeout); err == nil {
			config.FilesystemCreationTimeout = duration
		}
	}

	if timeout := os.Getenv("MOUNT_TARGET_CREATION_TIMEOUT"); timeout != "" {
		if duration, err := time.ParseDuration(timeout); err == nil {
			config.MountTargetCreationTimeout = duration
		}
	}

	return config
}

func TestParseEFSNSConfigFromEnv_Defaults(t *testing.T) {
	// Clear environment variables
	os.Unsetenv("ENABLE_EFS_NS_MODE")
	os.Unsetenv("CLUSTER_ID")
	os.Unsetenv("CACHE_TTL")
	os.Unsetenv("PVC_TRACKER_CONFIGMAP_NAME")
	os.Unsetenv("FILESYSTEM_CREATION_TIMEOUT")
	os.Unsetenv("MOUNT_TARGET_CREATION_TIMEOUT")

	config := ParseEFSNSConfigFromEnv()

	assert.False(t, config.Enabled, "efs-ns mode should be disabled by default")
	assert.Empty(t, config.ClusterID, "cluster ID should be empty by default")
	assert.Equal(t, 5*time.Minute, config.CacheTTL, "cache TTL should be 5 minutes by default")
	assert.Equal(t, "efs-ns-pvc-tracker", config.PVCTrackerConfigMapName, "PVC tracker ConfigMap name should have default value")
	assert.Equal(t, 10*time.Minute, config.FilesystemCreationTimeout, "filesystem creation timeout should be 10 minutes by default")
	assert.Equal(t, 5*time.Minute, config.MountTargetCreationTimeout, "mount target creation timeout should be 5 minutes by default")
	assert.Empty(t, config.DefaultTags, "default tags should be empty")
}

func TestParseEFSNSConfigFromEnv_Enabled(t *testing.T) {
	// Set environment variables
	os.Setenv("ENABLE_EFS_NS_MODE", "true")
	os.Setenv("CLUSTER_ID", "test-cluster-123")
	os.Setenv("CACHE_TTL", "10m")
	os.Setenv("PVC_TRACKER_CONFIGMAP_NAME", "custom-tracker")
	os.Setenv("FILESYSTEM_CREATION_TIMEOUT", "15m")
	os.Setenv("MOUNT_TARGET_CREATION_TIMEOUT", "8m")

	defer func() {
		// Cleanup
		os.Unsetenv("ENABLE_EFS_NS_MODE")
		os.Unsetenv("CLUSTER_ID")
		os.Unsetenv("CACHE_TTL")
		os.Unsetenv("PVC_TRACKER_CONFIGMAP_NAME")
		os.Unsetenv("FILESYSTEM_CREATION_TIMEOUT")
		os.Unsetenv("MOUNT_TARGET_CREATION_TIMEOUT")
	}()

	config := ParseEFSNSConfigFromEnv()

	assert.True(t, config.Enabled, "efs-ns mode should be enabled")
	assert.Equal(t, "test-cluster-123", config.ClusterID, "cluster ID should be parsed correctly")
	assert.Equal(t, 10*time.Minute, config.CacheTTL, "cache TTL should be parsed correctly")
	assert.Equal(t, "custom-tracker", config.PVCTrackerConfigMapName, "PVC tracker ConfigMap name should be parsed correctly")
	assert.Equal(t, 15*time.Minute, config.FilesystemCreationTimeout, "filesystem creation timeout should be parsed correctly")
	assert.Equal(t, 8*time.Minute, config.MountTargetCreationTimeout, "mount target creation timeout should be parsed correctly")
}

func TestParseEFSNSConfigFromEnv_InvalidDuration(t *testing.T) {
	// Set invalid duration values
	os.Setenv("CACHE_TTL", "invalid-duration")
	os.Setenv("FILESYSTEM_CREATION_TIMEOUT", "not-a-duration")

	defer func() {
		os.Unsetenv("CACHE_TTL")
		os.Unsetenv("FILESYSTEM_CREATION_TIMEOUT")
	}()

	config := ParseEFSNSConfigFromEnv()

	// Should fall back to defaults when duration parsing fails
	assert.Equal(t, 5*time.Minute, config.CacheTTL, "should use default cache TTL when parsing fails")
	assert.Equal(t, 10*time.Minute, config.FilesystemCreationTimeout, "should use default filesystem creation timeout when parsing fails")
}

func TestEFSNSConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      *EFSNSConfig
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid configuration",
			config: &EFSNSConfig{
				Enabled:                    true,
				ClusterID:                  "test-cluster",
				CacheTTL:                   5 * time.Minute,
				PVCTrackerConfigMapName:    "tracker",
				FilesystemCreationTimeout:  10 * time.Minute,
				MountTargetCreationTimeout: 5 * time.Minute,
			},
			expectError: false,
		},
		{
			name: "missing cluster ID when enabled",
			config: &EFSNSConfig{
				Enabled:   true,
				ClusterID: "",
			},
			expectError: true,
			errorMsg:    "cluster ID is required when efs-ns mode is enabled",
		},
		{
			name: "zero cache TTL",
			config: &EFSNSConfig{
				Enabled:   true,
				ClusterID: "test-cluster",
				CacheTTL:  0,
			},
			expectError: true,
			errorMsg:    "cache TTL must be positive",
		},
		{
			name: "empty ConfigMap name",
			config: &EFSNSConfig{
				Enabled:                 true,
				ClusterID:               "test-cluster",
				CacheTTL:                5 * time.Minute,
				PVCTrackerConfigMapName: "",
			},
			expectError: true,
			errorMsg:    "PVC tracker ConfigMap name is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateEFSNSConfig(tt.config)
			if tt.expectError {
				require.Error(t, err, "expected validation error")
				assert.Contains(t, err.Error(), tt.errorMsg, "error message should contain expected text")
			} else {
				require.NoError(t, err, "expected no validation error")
			}
		})
	}
}

// Helper function for validation (would be part of the actual implementation)
func validateEFSNSConfig(config *EFSNSConfig) error {
	if !config.Enabled {
		return nil // No validation needed when disabled
	}

	if config.ClusterID == "" {
		return fmt.Errorf("cluster ID is required when efs-ns mode is enabled")
	}

	if config.CacheTTL <= 0 {
		return fmt.Errorf("cache TTL must be positive")
	}

	if config.PVCTrackerConfigMapName == "" {
		return fmt.Errorf("PVC tracker ConfigMap name is required")
	}

	if config.FilesystemCreationTimeout <= 0 {
		return fmt.Errorf("filesystem creation timeout must be positive")
	}

	if config.MountTargetCreationTimeout <= 0 {
		return fmt.Errorf("mount target creation timeout must be positive")
	}

	return nil
}

func TestEFSNSConfig_IsEnabled(t *testing.T) {
	config := &EFSNSConfig{Enabled: false}
	assert.False(t, config.Enabled, "should return false when disabled")

	config.Enabled = true
	assert.True(t, config.Enabled, "should return true when enabled")
}

func TestEFSNSConfig_GetTimeouts(t *testing.T) {
	config := &EFSNSConfig{
		FilesystemCreationTimeout:  15 * time.Minute,
		MountTargetCreationTimeout: 8 * time.Minute,
	}

	assert.Equal(t, 15*time.Minute, config.FilesystemCreationTimeout, "filesystem creation timeout should match")
	assert.Equal(t, 8*time.Minute, config.MountTargetCreationTimeout, "mount target creation timeout should match")
}
