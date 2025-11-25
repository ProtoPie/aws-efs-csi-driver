# EFS Namespace Provisioning (efs-ns) Example

This example demonstrates how to use the EFS CSI Driver with namespace-level provisioning mode (efs-ns). In this mode, the driver automatically creates and manages a dedicated Amazon EFS filesystem for each Kubernetes namespace, providing complete storage isolation between namespaces.

## Prerequisites

- Kubernetes cluster with EFS CSI Driver installed and configured with efs-ns mode enabled
- AWS credentials configured with appropriate EFS permissions
- VPC and subnets configured for EFS mount targets

## Features

- **Namespace Isolation**: Each namespace gets its own dedicated EFS filesystem
- **Automatic Lifecycle Management**: Filesystems are created when the first PVC is created in a namespace
- **Automatic Cleanup**: Filesystems are deleted when the last PVC is removed from a namespace
- **Configurable Performance**: Support for different performance and throughput modes
- **Encryption Support**: Both at-rest and in-transit encryption options
- **Tagging**: Automatic tagging for cost allocation and management

## Usage

1. Apply the example StorageClass:
   ```bash
   kubectl apply -f storageclass.yaml
   ```

2. Create a PVC in any namespace:
   ```bash
   kubectl apply -f pvc.yaml -n your-namespace
   ```

3. Use the PVC in your pod:
   ```bash
   kubectl apply -f pod.yaml -n your-namespace
   ```

## Configuration

The StorageClass supports the following efs-ns specific parameters:

- `provisioningMode`: Must be set to `efs-ns`
- `performanceMode`: `generalPurpose` (default) or `maxIO`
- `throughputMode`: `bursting` (default) or `provisioned`
- `provisionedThroughputInMibps`: Required when using `provisioned` throughput mode
- `encrypted`: `true` or `false` (default: true)
- `kmsKeyId`: Custom KMS key for encryption (optional)
- `encryptInTransit`: `true` (default) or `false`

## Monitoring

The driver provides metrics for monitoring efs-ns operations:

- `efs_ns_filesystems_total`: Total number of efs-ns filesystems by namespace and state
- `efs_ns_operation_duration_seconds`: Duration of efs-ns operations
- `efs_ns_cache_hit_ratio`: Cache hit ratio for filesystem lookups

## Troubleshooting

Check the controller logs for detailed information about filesystem operations:

```bash
kubectl logs -n kube-system deployment/efs-csi-controller
```

Common issues:
- Ensure IAM permissions include EFS filesystem and mount target management
- Check VPC and subnet configuration for mount target creation
- Verify cluster ID is set correctly in the Helm values