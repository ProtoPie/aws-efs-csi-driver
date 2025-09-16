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

package v1alpha1

import (
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestEFSNamespace_DeepCopy(t *testing.T) {
	throughput := float64(100.5)
	encrypted := true

	original := &EFSNamespace{
		TypeMeta: metav1.TypeMeta{
			Kind:       "EFSNamespace",
			APIVersion: "efs.csi.aws.com/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-namespace-mapping",
			Generation:      1,
			ResourceVersion: "1234",
		},
		Spec: EFSNamespaceSpec{
			Namespace:                    "test-namespace",
			FileSystemID:                 "fs-12345678",
			FileSystemArn:                "arn:aws:elasticfilesystem:us-west-2:123456789012:file-system/fs-12345678",
			Region:                       "us-west-2",
			PerformanceMode:              "generalPurpose",
			ThroughputMode:               "provisioned",
			ProvisionedThroughputInMibps: &throughput,
			Encrypted:                    &encrypted,
			KmsKeyID:                     "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012",
			LifecyclePolicy:              "AFTER_30_DAYS",
			BackupPolicy:                 "ENABLED",
			CleanupPolicy:                "retain",
			MountTargets: &MountTargetConfig{
				SubnetIDs:        []string{"subnet-12345678", "subnet-87654321"},
				SecurityGroupIDs: []string{"sg-12345678", "sg-87654321"},
				IPAddresses:      []string{"10.0.1.10", "10.0.2.10"},
			},
			Tags: map[string]string{
				"Environment": "test",
				"Team":        "platform",
			},
			AccessPoints: []AccessPointInfo{
				{
					AccessPointID: "fsap-12345678",
					PvcName:       "test-pvc",
					PvcNamespace:  "test-namespace",
					Path:          "/test-path",
					PosixUser: &PosixUser{
						UID:           1000,
						GID:           1000,
						SecondaryGIDs: []int64{1001, 1002},
					},
					RootDirectory: &RootDirectory{
						Path: "/root",
						CreationInfo: &CreationInfo{
							OwnerUID:    1000,
							OwnerGID:    1000,
							Permissions: "0755",
						},
					},
				},
			},
		},
		Status: EFSNamespaceStatus{
			State:         "Active",
			FileSystemID:  "fs-12345678",
			FileSystemArn: "arn:aws:elasticfilesystem:us-west-2:123456789012:file-system/fs-12345678",
			MountTargets: []MountTargetStatus{
				{
					MountTargetID:      "fsmt-12345678",
					AvailabilityZone:   "us-west-2a",
					SubnetID:           "subnet-12345678",
					IPAddress:          "10.0.1.10",
					NetworkInterfaceID: "eni-12345678",
					LifecycleState:     "available",
				},
			},
			AccessPointCount:   1,
			LastUpdated:        &metav1.Time{Time: time.Now()},
			Message:            "EFS filesystem is ready",
			ObservedGeneration: 1,
			Conditions: []EFSNamespaceCondition{
				{
					Type:               EFSNamespaceReady,
					Status:             metav1.ConditionTrue,
					LastTransitionTime: metav1.Time{Time: time.Now()},
					Reason:             "ProvisioningSucceeded",
					Message:            "EFS filesystem provisioned successfully",
				},
			},
		},
	}

	// Test DeepCopy
	copied := original.DeepCopy()
	if !reflect.DeepEqual(original, copied) {
		t.Errorf("DeepCopy() failed: original and copied objects are not equal")
	}

	// Verify it's a deep copy by modifying the copy
	copied.Spec.Namespace = "modified-namespace"
	if original.Spec.Namespace == copied.Spec.Namespace {
		t.Errorf("DeepCopy() returned a shallow copy: modifying copy affected original")
	}

	// Test DeepCopyInto
	var into EFSNamespace
	original.DeepCopyInto(&into)
	if !reflect.DeepEqual(original, &into) {
		t.Errorf("DeepCopyInto() failed: original and target objects are not equal")
	}

	// Test DeepCopyObject
	obj := original.DeepCopyObject()
	copiedObj, ok := obj.(*EFSNamespace)
	if !ok {
		t.Fatalf("DeepCopyObject() returned wrong type: %T", obj)
	}
	if !reflect.DeepEqual(original, copiedObj) {
		t.Errorf("DeepCopyObject() failed: original and copied objects are not equal")
	}
}

func TestEFSNamespaceList_DeepCopy(t *testing.T) {
	original := &EFSNamespaceList{
		TypeMeta: metav1.TypeMeta{
			Kind:       "EFSNamespaceList",
			APIVersion: "efs.csi.aws.com/v1alpha1",
		},
		ListMeta: metav1.ListMeta{
			ResourceVersion: "5678",
		},
		Items: []EFSNamespace{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "namespace1",
				},
				Spec: EFSNamespaceSpec{
					Namespace: "ns1",
					Region:    "us-west-2",
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "namespace2",
				},
				Spec: EFSNamespaceSpec{
					Namespace: "ns2",
					Region:    "us-east-1",
				},
			},
		},
	}

	// Test DeepCopy
	copied := original.DeepCopy()
	if !reflect.DeepEqual(original, copied) {
		t.Errorf("DeepCopy() failed: original and copied lists are not equal")
	}

	// Verify it's a deep copy
	copied.Items[0].Spec.Namespace = "modified"
	if original.Items[0].Spec.Namespace == copied.Items[0].Spec.Namespace {
		t.Errorf("DeepCopy() returned a shallow copy: modifying copy affected original")
	}

	// Test DeepCopyInto
	var into EFSNamespaceList
	original.DeepCopyInto(&into)
	if !reflect.DeepEqual(original, &into) {
		t.Errorf("DeepCopyInto() failed: original and target lists are not equal")
	}

	// Test DeepCopyObject
	obj := original.DeepCopyObject()
	copiedObj, ok := obj.(*EFSNamespaceList)
	if !ok {
		t.Fatalf("DeepCopyObject() returned wrong type: %T", obj)
	}
	if !reflect.DeepEqual(original, copiedObj) {
		t.Errorf("DeepCopyObject() failed: original and copied lists are not equal")
	}
}

func TestEFSNamespaceSpec_DeepCopy(t *testing.T) {
	throughput := float64(200.0)
	encrypted := false

	original := &EFSNamespaceSpec{
		Namespace:                    "test-ns",
		FileSystemID:                 "fs-87654321",
		Region:                       "eu-west-1",
		PerformanceMode:              "maxIO",
		ThroughputMode:               "elastic",
		ProvisionedThroughputInMibps: &throughput,
		Encrypted:                    &encrypted,
		KmsKeyID:                     "test-key",
		LifecyclePolicy:              "AFTER_7_DAYS",
		BackupPolicy:                 "DISABLED",
		CleanupPolicy:                "delete",
		MountTargets: &MountTargetConfig{
			SubnetIDs:        []string{"subnet-abc", "subnet-def"},
			SecurityGroupIDs: []string{"sg-abc"},
			IPAddresses:      []string{"192.168.1.10"},
		},
		Tags: map[string]string{
			"Key1": "Value1",
			"Key2": "Value2",
		},
		AccessPoints: []AccessPointInfo{
			{
				AccessPointID: "fsap-abc",
				PvcName:       "pvc1",
				PvcNamespace:  "ns1",
				Path:          "/path1",
			},
			{
				AccessPointID: "fsap-def",
				PvcName:       "pvc2",
				PvcNamespace:  "ns2",
				Path:          "/path2",
			},
		},
	}

	// Test DeepCopy
	copied := original.DeepCopy()
	if !reflect.DeepEqual(original, copied) {
		t.Errorf("DeepCopy() failed: specs are not equal")
	}

	// Test pointer fields
	if copied.ProvisionedThroughputInMibps == original.ProvisionedThroughputInMibps {
		t.Errorf("DeepCopy() failed: pointer fields share same memory address")
	}
	if *copied.ProvisionedThroughputInMibps != *original.ProvisionedThroughputInMibps {
		t.Errorf("DeepCopy() failed: pointer field values differ")
	}

	// Test map deep copy
	copied.Tags["Key1"] = "ModifiedValue"
	if original.Tags["Key1"] == copied.Tags["Key1"] {
		t.Errorf("DeepCopy() failed: maps share same memory")
	}

	// Test slice deep copy
	copied.AccessPoints[0].PvcName = "modified-pvc"
	if original.AccessPoints[0].PvcName == copied.AccessPoints[0].PvcName {
		t.Errorf("DeepCopy() failed: slices share same memory")
	}

	// Test DeepCopyInto
	var into EFSNamespaceSpec
	original.DeepCopyInto(&into)
	if !reflect.DeepEqual(original, &into) {
		t.Errorf("DeepCopyInto() failed: specs are not equal")
	}
}

func TestMountTargetConfig_DeepCopy(t *testing.T) {
	original := &MountTargetConfig{
		SubnetIDs:        []string{"subnet-1", "subnet-2", "subnet-3"},
		SecurityGroupIDs: []string{"sg-1", "sg-2"},
		IPAddresses:      []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"},
	}

	// Test DeepCopy
	copied := original.DeepCopy()
	if !reflect.DeepEqual(original, copied) {
		t.Errorf("DeepCopy() failed: configs are not equal")
	}

	// Verify slices are deeply copied
	copied.SubnetIDs[0] = "modified-subnet"
	if original.SubnetIDs[0] == copied.SubnetIDs[0] {
		t.Errorf("DeepCopy() failed: slices share same memory")
	}

	// Test DeepCopyInto
	var into MountTargetConfig
	original.DeepCopyInto(&into)
	if !reflect.DeepEqual(original, &into) {
		t.Errorf("DeepCopyInto() failed: configs are not equal")
	}
}

func TestAccessPointInfo_DeepCopy(t *testing.T) {
	original := &AccessPointInfo{
		AccessPointID: "fsap-test",
		PvcName:       "test-pvc",
		PvcNamespace:  "test-namespace",
		Path:          "/test",
		PosixUser: &PosixUser{
			UID:           2000,
			GID:           2000,
			SecondaryGIDs: []int64{2001, 2002, 2003},
		},
		RootDirectory: &RootDirectory{
			Path: "/root/test",
			CreationInfo: &CreationInfo{
				OwnerUID:    2000,
				OwnerGID:    2000,
				Permissions: "0777",
			},
		},
	}

	// Test DeepCopy
	copied := original.DeepCopy()
	if !reflect.DeepEqual(original, copied) {
		t.Errorf("DeepCopy() failed: access points are not equal")
	}

	// Verify nested structs are deeply copied
	if copied.PosixUser == original.PosixUser {
		t.Errorf("DeepCopy() failed: PosixUser pointers share same memory")
	}
	copied.PosixUser.UID = 3000
	if original.PosixUser.UID == copied.PosixUser.UID {
		t.Errorf("DeepCopy() failed: modifying nested struct affected original")
	}

	// Test DeepCopyInto
	var into AccessPointInfo
	original.DeepCopyInto(&into)
	if !reflect.DeepEqual(original, &into) {
		t.Errorf("DeepCopyInto() failed: access points are not equal")
	}
}

func TestEFSNamespaceStatus_DeepCopy(t *testing.T) {
	now := metav1.Time{Time: time.Now()}

	original := &EFSNamespaceStatus{
		State:         "Provisioning",
		FileSystemID:  "fs-status",
		FileSystemArn: "arn:aws:elasticfilesystem:region:account:file-system/fs-status",
		MountTargets: []MountTargetStatus{
			{
				MountTargetID:      "fsmt-1",
				AvailabilityZone:   "zone-1",
				SubnetID:           "subnet-status-1",
				IPAddress:          "10.1.1.1",
				NetworkInterfaceID: "eni-1",
				LifecycleState:     "creating",
			},
			{
				MountTargetID:      "fsmt-2",
				AvailabilityZone:   "zone-2",
				SubnetID:           "subnet-status-2",
				IPAddress:          "10.1.1.2",
				NetworkInterfaceID: "eni-2",
				LifecycleState:     "available",
			},
		},
		AccessPointCount:   5,
		LastUpdated:        &now,
		Message:            "Provisioning in progress",
		ObservedGeneration: 2,
		Conditions: []EFSNamespaceCondition{
			{
				Type:               EFSNamespaceProvisioning,
				Status:             metav1.ConditionTrue,
				LastTransitionTime: now,
				Reason:             "ProvisioningStarted",
				Message:            "EFS provisioning has started",
			},
			{
				Type:               EFSNamespaceReady,
				Status:             metav1.ConditionFalse,
				LastTransitionTime: now,
				Reason:             "NotReady",
				Message:            "EFS is not yet ready",
			},
		},
	}

	// Test DeepCopy
	copied := original.DeepCopy()
	if !reflect.DeepEqual(original, copied) {
		t.Errorf("DeepCopy() failed: statuses are not equal")
	}

	// Verify slices are deeply copied
	copied.MountTargets[0].LifecycleState = "deleted"
	if original.MountTargets[0].LifecycleState == copied.MountTargets[0].LifecycleState {
		t.Errorf("DeepCopy() failed: MountTargets slice shares same memory")
	}

	copied.Conditions[0].Message = "Modified message"
	if original.Conditions[0].Message == copied.Conditions[0].Message {
		t.Errorf("DeepCopy() failed: Conditions slice shares same memory")
	}

	// Test pointer fields
	if copied.LastUpdated == original.LastUpdated {
		t.Errorf("DeepCopy() failed: LastUpdated pointers share same memory")
	}

	// Test DeepCopyInto
	var into EFSNamespaceStatus
	original.DeepCopyInto(&into)
	if !reflect.DeepEqual(original, &into) {
		t.Errorf("DeepCopyInto() failed: statuses are not equal")
	}
}

func TestEFSNamespaceCondition_DeepCopy(t *testing.T) {
	now := metav1.Time{Time: time.Now()}

	original := &EFSNamespaceCondition{
		Type:               EFSNamespaceError,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             "TestReason",
		Message:            "This is a test error message",
	}

	// Test DeepCopy
	copied := original.DeepCopy()
	if !reflect.DeepEqual(original, copied) {
		t.Errorf("DeepCopy() failed: conditions are not equal")
	}

	// Modify copy to ensure independence
	copied.Message = "Modified message"
	if original.Message == copied.Message {
		t.Errorf("DeepCopy() failed: modifying copy affected original")
	}

	// Test DeepCopyInto
	var into EFSNamespaceCondition
	original.DeepCopyInto(&into)
	if !reflect.DeepEqual(original, &into) {
		t.Errorf("DeepCopyInto() failed: conditions are not equal")
	}
}

func TestNilFieldHandling(t *testing.T) {
	// Test with nil pointer fields
	original := &EFSNamespaceSpec{
		Namespace:                    "test",
		Region:                       "us-west-2",
		ProvisionedThroughputInMibps: nil,
		Encrypted:                    nil,
		MountTargets:                 nil,
		Tags:                         nil,
		AccessPoints:                 nil,
	}

	copied := original.DeepCopy()
	if !reflect.DeepEqual(original, copied) {
		t.Errorf("DeepCopy() failed with nil fields: specs are not equal")
	}

	// Ensure nil fields remain nil
	if copied.ProvisionedThroughputInMibps != nil {
		t.Errorf("DeepCopy() failed: nil ProvisionedThroughputInMibps became non-nil")
	}
	if copied.Encrypted != nil {
		t.Errorf("DeepCopy() failed: nil Encrypted became non-nil")
	}
	if copied.MountTargets != nil {
		t.Errorf("DeepCopy() failed: nil MountTargets became non-nil")
	}
	if copied.Tags != nil {
		t.Errorf("DeepCopy() failed: nil Tags became non-nil")
	}
	if copied.AccessPoints != nil {
		t.Errorf("DeepCopy() failed: nil AccessPoints became non-nil")
	}
}

func TestEmptySlicesAndMaps(t *testing.T) {
	original := &EFSNamespaceSpec{
		Namespace: "test",
		Region:    "us-west-2",
		MountTargets: &MountTargetConfig{
			SubnetIDs:        []string{},
			SecurityGroupIDs: []string{},
			IPAddresses:      []string{},
		},
		Tags:         map[string]string{},
		AccessPoints: []AccessPointInfo{},
	}

	copied := original.DeepCopy()
	if !reflect.DeepEqual(original, copied) {
		t.Errorf("DeepCopy() failed with empty slices/maps: specs are not equal")
	}

	// Ensure empty slices and maps are properly initialized
	if copied.MountTargets.SubnetIDs == nil {
		t.Errorf("DeepCopy() failed: empty slice became nil")
	}
	if len(copied.MountTargets.SubnetIDs) != 0 {
		t.Errorf("DeepCopy() failed: empty slice has wrong length")
	}
	if copied.Tags == nil {
		t.Errorf("DeepCopy() failed: empty map became nil")
	}
	if len(copied.Tags) != 0 {
		t.Errorf("DeepCopy() failed: empty map has wrong length")
	}
}