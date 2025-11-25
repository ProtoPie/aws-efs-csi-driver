# EFS CSI Driver Backward Compatibility and Migration Guide

## Overview

This document provides guidance for maintaining backward compatibility and migrating between different versions of the AWS EFS CSI Driver, particularly when upgrading to versions that include EFS Namespace (efs-ns) provisioning mode support.

## Compatibility Matrix

| Driver Version | efs-ap Mode | efs-ns Mode | Mixed Mode | Notes |
|---------------|-------------|-------------|------------|--------|
| v1.x.x        | ✅ Supported | ❌ Not Available | ❌ N/A | Legacy access point mode only |
| v2.0.x        | ✅ Supported | ❌ Not Available | ❌ N/A | Enhanced access point mode |
| v2.1.x+       | ✅ Supported | ✅ Available | ✅ Supported | Full backward compatibility |

## Backward Compatibility Guarantees

### Volume ID Compatibility

The EFS CSI Driver maintains backward compatibility for volume IDs across all supported provisioning modes:

**efs-ap Volume ID Format (Legacy)**:
```
fs-{filesystem-id}::{access-point-id}
fs-{filesystem-id}::{subnet-id}:{access-point-id}
```

**efs-ns Volume ID Format (New)**:
```
efs-ns::{namespace}::{filesystem-id}::{cluster-id}
```

### CSI Interface Compatibility

All CSI interface methods maintain backward compatibility:

- `CreateVolume`: Supports both efs-ap and efs-ns modes based on `provisioningMode` parameter
- `DeleteVolume`: Automatically detects volume type and routes to appropriate handler
- `ValidateVolumeCapabilities`: Validates capabilities for both volume types
- `NodePublishVolume`/`NodeUnpublishVolume`: Support mounting both volume types

### Storage Class Parameters

Existing StorageClass parameters remain fully supported:

```yaml
# efs-ap mode (existing)
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: efs-ap-sc
provisioner: efs.csi.aws.com
parameters:
  provisioningMode: efs-ap
  fileSystemId: fs-12345678
  directoryPerms: "0755"
  
# efs-ns mode (new)  
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: efs-ns-sc
provisioner: efs.csi.aws.com
parameters:
  provisioningMode: efs-ns
  performanceMode: generalPurpose
  encrypted: "true"
```

## Mixed Mode Operations

The driver supports mixed-mode operations where both efs-ap and efs-ns volumes can coexist in the same cluster:

### Supported Scenarios

1. **Existing efs-ap volumes continue to work** when efs-ns mode is enabled
2. **New efs-ns volumes can be created** alongside existing efs-ap volumes
3. **Different namespaces can use different provisioning modes** simultaneously
4. **Volume validation works correctly** for both volume types

### Example Mixed Mode Configuration

```yaml
# Namespace A uses efs-ap mode
apiVersion: v1
kind: Namespace
metadata:
  name: namespace-a
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: efs-ap-claim
  namespace: namespace-a
spec:
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 5Gi
  storageClassName: efs-ap-sc

---
# Namespace B uses efs-ns mode
apiVersion: v1
kind: Namespace
metadata:
  name: namespace-b
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: efs-ns-claim
  namespace: namespace-b
spec:
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 5Gi
  storageClassName: efs-ns-sc
```

## Migration Scenarios

### Scenario 1: Upgrading from v1.x to v2.1.x+

**Before Upgrade:**
- Only efs-ap mode available
- All existing volumes use access point provisioning

**After Upgrade:**
- ✅ All existing efs-ap volumes continue to work
- ✅ New efs-ns StorageClasses can be created
- ✅ Mixed mode operations supported
- ✅ No changes required to existing PVCs/PVs

**Migration Steps:**
1. Upgrade the CSI driver to v2.1.x+
2. Verify existing volumes are still accessible
3. Optionally create new efs-ns StorageClasses for new workloads
4. New namespaces can choose between efs-ap or efs-ns modes

### Scenario 2: Adopting efs-ns Mode Gradually

**Recommended Approach:**
1. **Phase 1**: Upgrade driver but continue using efs-ap for existing workloads
2. **Phase 2**: Create efs-ns StorageClasses for new namespaces
3. **Phase 3**: Migrate applications namespace-by-namespace to efs-ns mode (optional)

**Migration Strategy:**
```yaml
# Phase 1: Keep existing StorageClass
kind: StorageClass
metadata:
  name: efs-default  # existing
parameters:
  provisioningMode: efs-ap
  fileSystemId: fs-existing

---
# Phase 2: Add new efs-ns StorageClass
kind: StorageClass  
metadata:
  name: efs-namespace-isolated  # new
parameters:
  provisioningMode: efs-ns
  performanceMode: generalPurpose
  encrypted: "true"
```

### Scenario 3: Downgrading (Not Recommended)

**⚠️ Important Considerations:**

- **efs-ns volumes will become inaccessible** after downgrading to versions without efs-ns support
- **Data is not lost** but cannot be mounted until upgrading again
- **Always verify no efs-ns volumes exist** before downgrading

**Pre-Downgrade Checklist:**
1. List all efs-ns volumes: `kubectl get pv -o jsonpath='{.items[?(@.spec.csi.volumeHandle contains "efs-ns")].metadata.name}'`
2. Migrate data from efs-ns volumes to efs-ap volumes if needed
3. Delete all efs-ns PVCs and PVs
4. Verify no efs-ns volumes remain before downgrading

## Validation and Testing

### Compatibility Validation

The driver includes built-in compatibility validation:

```go
// Example validation in your application
validator := NewBackwardCompatibilityValidator(driver)

// Validate mixed-mode operation
err := validator.ValidateMixedModeOperation(ctx)
if err != nil {
    log.Errorf("Mixed-mode validation failed: %v", err)
}

// Generate compatibility report
report := validator.GenerateCompatibilityReport(ctx)
log.Infof("Driver compatibility: efs-ap=%v, efs-ns=%v", 
    report.EFSAPModeSupported, report.EFSNSModeSupported)
```

### Testing Mixed Mode Operations

Test both volume types work correctly:

```bash
# Create efs-ap volume
kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: test-efs-ap
spec:
  accessModes: [ReadWriteMany]
  resources:
    requests:
      storage: 1Gi
  storageClassName: efs-ap-sc
EOF

# Create efs-ns volume  
kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: test-efs-ns
spec:
  accessModes: [ReadWriteMany]
  resources:
    requests:
      storage: 1Gi  
  storageClassName: efs-ns-sc
EOF

# Verify both volumes are bound
kubectl get pvc test-efs-ap test-efs-ns
```

## Troubleshooting

### Common Issues

1. **efs-ns volumes fail to create after upgrade**
   - Check if EFS-NS components are properly initialized
   - Verify RBAC permissions for namespace and EFS management
   - Review driver logs for initialization errors

2. **Existing efs-ap volumes become inaccessible**
   - This should not happen - file a bug if it does
   - Check volume ID format is preserved
   - Verify access point still exists in AWS EFS

3. **Mixed-mode validation failures**
   - Check driver configuration and component initialization
   - Verify cloud provider credentials and permissions
   - Review compatibility report for detailed error information

### Diagnostic Commands

```bash
# Check driver version and capabilities
kubectl describe deployment efs-csi-controller -n kube-system

# List all EFS volumes and their types
kubectl get pv -o custom-columns=NAME:.metadata.name,VOLUME-HANDLE:.spec.csi.volumeHandle,STORAGE-CLASS:.spec.storageClassName

# Check driver logs
kubectl logs -f deployment/efs-csi-controller -n kube-system -c efs-plugin

# Validate volume capabilities (replace VOLUME_ID)
kubectl exec -it deployment/efs-csi-controller -n kube-system -- /bin/aws-efs-csi-driver --endpoint=unix:///tmp/csi.sock validate-volume-capabilities --volume-id=VOLUME_ID
```

### Support Matrix

| Issue Type | efs-ap Mode | efs-ns Mode | Mixed Mode |
|------------|-------------|-------------|-------------|
| Volume creation | ✅ Fully Supported | ✅ Fully Supported | ✅ Supported |
| Volume deletion | ✅ Fully Supported | ✅ Fully Supported | ✅ Supported |
| Volume mounting | ✅ Fully Supported | ✅ Fully Supported | ✅ Supported |
| Capability validation | ✅ Fully Supported | ✅ Fully Supported | ✅ Supported |
| Upgrade compatibility | ✅ Guaranteed | ✅ Forward Compatible | ✅ Supported |
| Downgrade compatibility | ✅ Guaranteed | ⚠️ Data Inaccessible | ⚠️ efs-ns volumes affected |

## Best Practices

### For New Deployments

1. **Use efs-ns mode for new namespaced applications** requiring storage isolation
2. **Use efs-ap mode for shared storage scenarios** across multiple namespaces
3. **Plan StorageClass strategy** based on isolation requirements
4. **Test both modes** in development before production deployment

### For Existing Deployments

1. **Test upgrades in non-production** environments first
2. **Maintain existing efs-ap volumes** during transition period
3. **Gradually adopt efs-ns mode** for new namespaces
4. **Monitor compatibility** using built-in validation tools
5. **Document volume type inventory** for operational awareness

### For Operations Teams

1. **Monitor both volume types** in your observability systems
2. **Update backup procedures** to handle both EFS access points and filesystems
3. **Train team on mixed-mode operations** and troubleshooting
4. **Establish migration runbooks** for planned transitions
5. **Test disaster recovery** procedures with both volume types

## Conclusion

The EFS CSI Driver v2.1.x+ provides full backward compatibility while enabling new namespace-based storage isolation capabilities. Mixed-mode operations are fully supported, allowing for gradual adoption of new features without disrupting existing workloads.

For additional support:
- Review driver logs for detailed error information
- Use built-in compatibility validation tools
- Consult the GitHub repository for known issues and updates
- Follow the compatibility matrix for supported upgrade/downgrade paths