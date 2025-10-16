# EFS Namespace Provisioning

This document describes the namespace-based EFS provisioning feature in the AWS EFS CSI Driver.

## Overview

The namespace provisioning mode (`efs-ns`) allows automatic creation and management of EFS filesystems on a per-namespace basis. When enabled, each Kubernetes namespace can have its own dedicated EFS filesystem with isolated access.

## Key Changes in v2.1.0+

### Namespaced CRD Scope

The `EFSNamespace` Custom Resource Definition (CRD) has been changed from **Cluster-scoped** to **Namespaced-scoped**. This provides better isolation and security:

- **Before**: EFSNamespace resources were cluster-wide, named after the namespace they represented
- **After**: EFSNamespace resources are created within the namespace they represent, with a fixed name `efs-mapping`

### Benefits of Namespaced Scope

1. **Better Isolation**: Each namespace owns its EFS mapping resource
2. **Improved Security**: Namespace admins can only manage their own EFS mappings
3. **Automatic Cleanup**: When a namespace is deleted, its EFSNamespace resource is automatically deleted
4. **RBAC Alignment**: Permissions can be scoped per namespace

## Installation

### 1. Enable the Feature

```yaml
# values.yaml
controller:
  efsNamespaceProvisioning:
    enabled: true
```

### 2. Deploy the Helm Chart

```bash
helm upgrade --install aws-efs-csi-driver \
  aws-efs-csi-driver/aws-efs-csi-driver \
  --namespace kube-system \
  --values values-namespace-provisioning.yaml
```

## Usage

### Creating a Storage Class

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: efs-namespace-provisioned
provisioner: efs.csi.aws.com
parameters:
  provisioningMode: efs-ns
  directoryPerms: "700"
  gidRangeStart: "1000"
  gidRangeEnd: "2000"
reclaimPolicy: Delete
volumeBindingMode: Immediate
```

### Creating a PVC

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-efs-claim
  namespace: my-app
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: efs-namespace-provisioned
  resources:
    requests:
      storage: 5Gi
```

### Viewing EFSNamespace Resources

```bash
# List all EFSNamespace resources in a namespace
kubectl get efsnamespaces -n my-app

# View details of the mapping
kubectl get efsnamespace efs-mapping -n my-app -o yaml
```

## How It Works

1. When a PVC is created with a storage class using `provisioningMode: efs-ns`:
   - The CSI driver checks if an EFS filesystem exists for that namespace
   - If not, it creates a new EFS filesystem and tags it appropriately
   - An `EFSNamespace` resource is created in the namespace to track the mapping

2. The `EFSNamespace` CRD resource:
   - Name: Always `efs-mapping` within each namespace
   - Namespace: Created in the same namespace it manages
   - Tracks: FileSystem ID, ARN, region, and status

3. When a namespace is deleted:
   - The `EFSNamespace` resource is automatically deleted (Kubernetes garbage collection)
   - The CSI driver can optionally clean up the EFS filesystem based on `cleanupPolicy`

## Migration from Cluster-Scoped to Namespaced CRDs

If upgrading from an older version with cluster-scoped CRDs:

1. **Backup existing mappings**:
   ```bash
   kubectl get efsnamespaces -o yaml > efsnamespaces-backup.yaml
   ```

2. **Delete the old CRD**:
   ```bash
   kubectl delete crd efsnamespaces.efs.csi.aws.com
   ```

3. **Apply the new CRD** (done automatically by Helm):
   ```bash
   helm upgrade aws-efs-csi-driver aws-efs-csi-driver/aws-efs-csi-driver \
     --namespace kube-system \
     --set controller.efsNamespaceProvisioning.enabled=true
   ```

4. **Mappings will be recreated** automatically when the CSI driver restarts

## Troubleshooting

### Check CRD Status

```bash
# Verify CRD is namespaced
kubectl api-resources | grep efsnamespace
# Should show: efsnamespaces  efsns  efs.csi.aws.com/v1alpha1  true  EFSNamespace

# Check if mapping exists
kubectl get efsnamespaces -n <namespace>
```

### Common Issues

1. **"the server could not find the requested resource"**
   - The CRD is not installed. Check if the feature is enabled in values.yaml

2. **PVCs stuck in Pending state**
   - Check CSI controller logs: `kubectl logs -n kube-system deployment/efs-csi-controller`
   - Verify EFS mount targets are configured in AWS

3. **EFSNamespace not created**
   - Ensure the namespace exists before creating PVCs
   - Check RBAC permissions for the CSI controller service account

## Configuration Reference

### EFSNamespace Spec Fields

| Field | Description | Default |
|-------|-------------|---------|
| `namespace` | The Kubernetes namespace | Required |
| `fileSystemId` | EFS filesystem ID (auto-created if empty) | Auto-generated |
| `region` | AWS region | Required |
| `performanceMode` | `generalPurpose` or `maxIO` | `generalPurpose` |
| `throughputMode` | `bursting`, `provisioned`, or `elastic` | `bursting` |
| `encrypted` | Enable encryption at rest | `true` |
| `cleanupPolicy` | `retain` or `delete` on namespace deletion | `retain` |

### Status Fields

| Field | Description |
|-------|-------------|
| `state` | Current state: `Provisioning`, `Active`, `Failed`, etc. |
| `fileSystemId` | The actual EFS filesystem ID in use |
| `lastUpdated` | Last update timestamp |
| `message` | Human-readable status message |

## Security Considerations

1. **RBAC**: Namespace admins can only view/edit their own `EFSNamespace` resources
2. **IAM**: Ensure proper IAM roles for EFS operations
3. **Security Groups**: Configure security groups for EFS mount targets
4. **Encryption**: Enable encryption by default for all filesystems

## Example Use Cases

1. **Multi-tenant Clusters**: Each tenant namespace gets isolated EFS storage
2. **Development Environments**: Automatic EFS provisioning for dev namespaces
3. **CI/CD Pipelines**: Dynamic storage provisioning for build namespaces
4. **Application Isolation**: Separate EFS filesystems per application namespace