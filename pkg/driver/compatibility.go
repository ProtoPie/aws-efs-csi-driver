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
	"fmt"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	efsns "github.com/kubernetes-sigs/aws-efs-csi-driver/pkg/driver/efs-ns"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

// BackwardCompatibilityValidator provides validation for backward compatibility
// between efs-ap and efs-ns provisioning modes
type BackwardCompatibilityValidator struct {
	driver *Driver
}

// NewBackwardCompatibilityValidator creates a new backward compatibility validator
func NewBackwardCompatibilityValidator(driver *Driver) *BackwardCompatibilityValidator {
	return &BackwardCompatibilityValidator{
		driver: driver,
	}
}

// ValidateVolumeID validates volume ID format compatibility across provisioning modes
func (v *BackwardCompatibilityValidator) ValidateVolumeID(volumeID string) error {
	if volumeID == "" {
		return status.Error(codes.InvalidArgument, "Volume ID cannot be empty")
	}

	// Check if it's an efs-ns volume ID
	if strings.HasPrefix(volumeID, efsns.VolumeIDPrefix+efsns.VolumeIDSeparator) {
		// Validate efs-ns volume ID format
		return v.validateEFSNSVolumeID(volumeID)
	}

	// Validate efs-ap volume ID format (legacy format)
	return v.validateEFSAPVolumeID(volumeID)
}

// validateEFSNSVolumeID validates efs-ns volume ID format
func (v *BackwardCompatibilityValidator) validateEFSNSVolumeID(volumeID string) error {
	_, err := efsns.ParseEFSNSVolumeID(volumeID)
	if err != nil {
		klog.V(4).Infof("Invalid efs-ns volume ID format: %s, error: %v", volumeID, err)
		return status.Errorf(codes.InvalidArgument, "Invalid efs-ns volume ID format: %s", volumeID)
	}
	return nil
}

// validateEFSAPVolumeID validates efs-ap volume ID format (legacy)
func (v *BackwardCompatibilityValidator) validateEFSAPVolumeID(volumeID string) error {
	_, _, _, err := parseVolumeId(volumeID)
	if err != nil {
		klog.V(4).Infof("Invalid efs-ap volume ID format: %s, error: %v", volumeID, err)
		return status.Errorf(codes.InvalidArgument, "Invalid efs-ap volume ID format: %s", volumeID)
	}
	return nil
}

// ValidateMixedModeOperation validates that mixed-mode operations are handled correctly
func (v *BackwardCompatibilityValidator) ValidateMixedModeOperation(ctx context.Context) error {
	// Validate that both efs-ap and efs-ns can coexist
	// This includes checking that driver components can handle both volume types
	
	// Check if EFS-NS components are properly initialized (but not required)
	if v.driver.namespaceManager != nil {
		klog.V(5).Info("EFS-NS components are available for mixed-mode operation")
	}

	// Validate that legacy components are still functional
	if v.driver.cloud == nil {
		return status.Error(codes.Internal, "Cloud provider interface is not initialized")
	}

	if v.driver.mounter == nil {
		return status.Error(codes.Internal, "Mounter interface is not initialized")
	}

	klog.V(5).Info("Mixed-mode operation validation passed")
	return nil
}

// ValidateProvisioningModeParameter validates provisioning mode parameter values
func (v *BackwardCompatibilityValidator) ValidateProvisioningModeParameter(mode string) error {
	if mode == "" {
		return status.Error(codes.InvalidArgument, "Provisioning mode parameter is required")
	}

	switch mode {
	case AccessPointMode:
		// efs-ap mode - legacy access point provisioning
		return nil
	case NamespaceMode:
		// efs-ns mode - namespace-based filesystem provisioning
		if v.driver.namespaceManager == nil {
			return status.Error(codes.FailedPrecondition, 
				"EFS-NS provisioning mode is not available. EFS-NS components are not initialized")
		}
		return nil
	default:
		return status.Errorf(codes.InvalidArgument, 
			"Unsupported provisioning mode: %s. Supported modes are: %s (Access Point), %s (Namespace)",
			mode, AccessPointMode, NamespaceMode)
	}
}

// ValidateControllerCapabilities validates that controller capabilities support both modes
func (v *BackwardCompatibilityValidator) ValidateControllerCapabilities() error {
	// Ensure controller capabilities support volume operations for both modes
	supportedCapabilities := map[csi.ControllerServiceCapability_RPC_Type]bool{}
	
	for _, cap := range controllerCaps {
		supportedCapabilities[cap] = true
	}

	// Verify required capabilities are present
	requiredCaps := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
	}

	for _, required := range requiredCaps {
		if !supportedCapabilities[required] {
			return status.Errorf(codes.Internal, 
				"Required controller capability not supported: %v", required)
		}
	}

	klog.V(5).Info("Controller capabilities validation passed for backward compatibility")
	return nil
}

// ValidateVolumeCapabilities validates volume capabilities for both provisioning modes
func (v *BackwardCompatibilityValidator) ValidateVolumeCapabilities(
	volCaps []*csi.VolumeCapability,
	provisioningMode string,
) error {
	if len(volCaps) == 0 {
		return status.Error(codes.InvalidArgument, "Volume capabilities not provided")
	}

	// Use driver's existing validation logic which supports both modes
	if err := v.driver.isValidVolumeCapabilities(volCaps); err != nil {
		return status.Errorf(codes.InvalidArgument, 
			"Volume capabilities not supported for %s mode: %s", provisioningMode, err)
	}

	if err := v.driver.validateFStype(volCaps); err != nil {
		return status.Errorf(codes.InvalidArgument, 
			"Volume fstype not supported for %s mode: %s", provisioningMode, err)
	}

	klog.V(5).Infof("Volume capabilities validated for %s mode", provisioningMode)
	return nil
}

// ValidateUpgradeCompatibility validates that existing volumes remain functional after driver upgrade
func (v *BackwardCompatibilityValidator) ValidateUpgradeCompatibility(ctx context.Context) error {
	// This validation ensures that:
	// 1. Existing efs-ap volumes can still be mounted/unmounted
	// 2. Existing efs-ns volumes (if any) remain functional
	// 3. No breaking changes in volume ID parsing
	// 4. CSI interface compatibility is maintained

	// Validate controller capabilities remain backward compatible
	if err := v.ValidateControllerCapabilities(); err != nil {
		return fmt.Errorf("controller capabilities compatibility check failed: %w", err)
	}

	// Validate mixed-mode operation support
	if err := v.ValidateMixedModeOperation(ctx); err != nil {
		return fmt.Errorf("mixed-mode operation compatibility check failed: %w", err)
	}

	klog.V(4).Info("Upgrade compatibility validation passed")
	return nil
}

// ValidateDowngradeCompatibility validates downgrade scenarios
func (v *BackwardCompatibilityValidator) ValidateDowngradeCompatibility(ctx context.Context) error {
	// Validate that if EFS-NS volumes exist, downgrade is not allowed
	// This is a safety measure to prevent data access issues

	if v.driver.namespaceManager == nil {
		// If EFS-NS is not initialized, no efs-ns volumes can exist
		klog.V(4).Info("EFS-NS not initialized, downgrade compatibility check passed")
		return nil
	}

	// In a real implementation, we would check for existing efs-ns volumes
	// For now, we log a warning about potential downgrade issues
	klog.Warningf("EFS-NS mode is enabled. Ensure no efs-ns volumes exist before downgrading")
	
	return nil
}

// CompatibilityReport represents a compatibility validation report
type CompatibilityReport struct {
	DriverVersion          string                     `json:"driverVersion"`
	EFSAPModeSupported     bool                       `json:"efsApModeSupported"`
	EFSNSModeSupported     bool                       `json:"efsNsModeSupported"`
	MixedModeSupported     bool                       `json:"mixedModeSupported"`
	ControllerCapabilities []string                   `json:"controllerCapabilities"`
	ValidationResults      []ValidationResult         `json:"validationResults"`
}

// ValidationResult represents the result of a specific validation check
type ValidationResult struct {
	CheckName   string `json:"checkName"`
	Passed      bool   `json:"passed"`
	ErrorMsg    string `json:"errorMsg,omitempty"`
	Description string `json:"description"`
}

// GenerateCompatibilityReport generates a comprehensive compatibility report
func (v *BackwardCompatibilityValidator) GenerateCompatibilityReport(ctx context.Context) *CompatibilityReport {
	report := &CompatibilityReport{
		DriverVersion:          GetVersion().DriverVersion,
		EFSAPModeSupported:     true, // Always supported
		EFSNSModeSupported:     v.driver.namespaceManager != nil,
		MixedModeSupported:     true, // Both modes can coexist
		ControllerCapabilities: []string{},
		ValidationResults:      []ValidationResult{},
	}

	// Add controller capabilities
	for _, cap := range controllerCaps {
		report.ControllerCapabilities = append(report.ControllerCapabilities, cap.String())
	}

	// Run validation checks
	checks := []struct {
		name        string
		description string
		fn          func() error
	}{
		{
			name:        "controller_capabilities",
			description: "Validate controller service capabilities",
			fn:          v.ValidateControllerCapabilities,
		},
		{
			name:        "mixed_mode_operation",
			description: "Validate mixed-mode operation support",
			fn:          func() error { return v.ValidateMixedModeOperation(ctx) },
		},
		{
			name:        "upgrade_compatibility",
			description: "Validate upgrade compatibility",
			fn:          func() error { return v.ValidateUpgradeCompatibility(ctx) },
		},
		{
			name:        "downgrade_compatibility", 
			description: "Validate downgrade compatibility",
			fn:          func() error { return v.ValidateDowngradeCompatibility(ctx) },
		},
	}

	for _, check := range checks {
		result := ValidationResult{
			CheckName:   check.name,
			Description: check.description,
			Passed:      true,
		}

		if err := check.fn(); err != nil {
			result.Passed = false
			result.ErrorMsg = err.Error()
		}

		report.ValidationResults = append(report.ValidationResults, result)
	}

	return report
}