# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the AWS EFS CSI Driver - a Kubernetes Container Storage Interface (CSI) driver that manages the lifecycle of Amazon EFS file systems. The driver implements the CSI specification v1.2.0 and enables both static and dynamic provisioning of EFS volumes in Kubernetes clusters.

## Common Development Commands

### Building and Testing
- `make` - Build the binary (`bin/aws-efs-csi-driver`)
- `make test` - Run unit tests for all packages (`go test -v -race ./pkg/...`)
- `make verify` - Run all verification scripts (gofmt, govet, golint)
- `make test-e2e` - Run E2E tests (requires AWS credentials and EKS cluster)
- `make test-e2e-bin` - Build E2E test binary

### Docker Images
- `make image` - Build Docker image for linux/amd64
- `make all-image-docker` - Build images for all supported architectures (amd64, arm64)
- `make all-push` - Build and push multi-arch images to registry

### Code Quality
- `./hack/verify-all` - Runs gofmt, govet, and golint checks
- `./hack/verify-gofmt` - Check Go formatting
- `./hack/verify-govet` - Run go vet analysis
- `./hack/verify-golint` - Run golint checks

### Cleanup
- `make clean` - Remove built binaries and Docker image metadata

## Architecture and Structure

### Core Components

#### Main Entry Point
- `cmd/main.go` - Driver main entry point with command-line flag parsing and initialization
- Key flags: `--endpoint`, `--vol-metrics-opt-in`, `--delete-access-point-root-dir`, `--tags`

#### Package Structure
- `pkg/driver/` - Core CSI driver implementation
  - Implements CSI Controller, Node, and Identity services
  - Handles volume creation/deletion, mounting/unmounting, and capabilities
- `pkg/cloud/` - AWS EFS API client and cloud integration
- `pkg/util/` - Shared utilities and helper functions

#### Configuration Management
- Driver supports both legacy (`/etc/amazon/efs-legacy`) and new (`/var/amazon/efs`) config directory paths
- Automatically creates symlink to `/etc/amazon/efs` for efs-utils compatibility

### Key Features Architecture

#### Dynamic Provisioning
- Creates EFS Access Points for each PersistentVolume
- Supports POSIX permissions, directory paths, and client tokens
- Storage class parameters control access point creation

#### Static Provisioning  
- Mounts existing EFS file systems directly
- No access point creation required
- Supports file system-level and access point-level mounting

#### Encryption in Transit
- Uses efs-proxy (v2.x) or stunnel (v1.x) for TLS encryption
- Enabled by default, can be disabled with `encryptInTransit: "false"`

#### Cross-Account Mounting
- Supports mounting EFS from different AWS accounts
- Requires proper IAM permissions and network access

## Testing Framework

### Unit Tests
- Uses standard Go testing (`testing` package)
- Run with `make test` or `go test -v -race ./pkg/...`
- Mocks generated using `github.com/golang/mock`

### E2E Tests
- Uses Ginkgo v2 and Gomega testing frameworks
- Located in `test/e2e/` directory
- Tests against real Kubernetes clusters and EFS file systems
- Requires AWS credentials and existing EFS file system
- Run individual tests with ginkgo focus: `-ginkgo.focus="\[efs-csi\]"`

### E2E Test Execution
```bash
# Local cluster testing
export KUBECONFIG=$HOME/.kube/config
go test -v -timeout 0 ./test/e2e/... \
  -ginkgo.focus="\[efs-csi\]" -ginkgo.skip="\[Disruptive\]" \
  --file-system-id=$FS_ID --create-file-system=false \
  --deploy-driver=true --region=$REGION
```

## Development Workflow

### Prerequisites
- Go 1.24+ (see go.mod)
- Docker for image building
- kubectl for Kubernetes interaction
- AWS credentials for E2E testing

### Key Dependencies
- Kubernetes CSI libraries (csi-test, mount-utils)
- AWS SDK v2 for EFS/EC2 APIs
- Ginkgo/Gomega for E2E testing
- klog v2 for structured logging

### CSI Implementation
The driver implements these CSI service interfaces:
- **Controller Service**: CreateVolume, DeleteVolume, ControllerGetCapabilities, ValidateVolumeCapabilities
- **Node Service**: NodePublishVolume, NodeUnpublishVolume, NodeGetCapabilities, NodeGetInfo, NodeGetVolumeStats
- **Identity Service**: GetPluginInfo, GetPluginCapabilities, Probe

### Version and Build Information
- Version set via `VERSION` variable in Makefile (currently v2.1.11)
- Build metadata injected via ldflags: driverVersion, gitCommit, buildDate, efsClientSource
- Retrieve version info: `./aws-efs-csi-driver --version`

### Helm Chart Integration
- Helm chart located in `charts/aws-efs-csi-driver/`
- Kustomize manifests generated from Helm templates via `make generate-kustomize`
- Multiple deployment options: Helm, kubectl with public/private ECR, or EKS managed add-on