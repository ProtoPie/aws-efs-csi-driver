# EFS Namespace Integration Tests

This directory contains comprehensive integration tests for the AWS EFS CSI Driver namespace functionality (efs-ns provisioning mode).

## Overview

The EFS namespace integration test suite validates all aspects of namespace-aware EFS volume provisioning, including:

- **System Integration**: End-to-end lifecycle testing with namespace isolation
- **Performance**: Benchmark validation against specified requirements
- **Security**: Compliance testing and vulnerability validation
- **Reliability**: Chaos engineering and failover scenarios
- **Monitoring**: Resource leak detection and cleanup validation
- **Compliance**: CSI specification adherence testing

## Quick Start

### Prerequisites

- Go 1.24+
- kubectl configured with access to a Kubernetes cluster
- AWS credentials with EFS permissions
- Docker (for building test images)

### Environment Setup

1. Configure AWS credentials:
```bash
export AWS_REGION=us-west-2
aws configure
```

2. Set up Kubernetes access:
```bash
export KUBECONFIG=~/.kube/config
kubectl cluster-info
```

3. Create test configuration:
```bash
cp test-config.yaml.template test-config.yaml
# Edit test-config.yaml with your specific settings
```

### Running Tests

#### Run All Integration Tests
```bash
# Using the automation script
./scripts/run-efs-ns-integration-tests.sh

# Using make target
make test-efs-ns-integration
```

#### Run Specific Test Suites
```bash
# System integration tests only
make test-efs-ns-system

# Performance tests
make test-efs-ns-performance

# Security tests  
make test-efs-ns-security

# Reliability tests
make test-efs-ns-reliability

# Resource monitoring tests
make test-efs-ns-monitoring

# CSI compliance tests
make test-efs-ns-compliance
```

#### Run with Custom Options
```bash
# Verbose output with specific suites
./scripts/run-efs-ns-integration-tests.sh \
  --suites system,performance \
  --verbose \
  --timeout 3600

# Fail-fast mode
./scripts/run-efs-ns-integration-tests.sh \
  --fail-fast \
  --parallel 8
```

#### Dry Run Mode
```bash
# See what would be executed without running tests
./scripts/run-efs-ns-integration-tests.sh --dry-run
```

## Test Suites

### System Integration Tests (`test/integration/system/`)

Validates complete EFS namespace functionality:

- **Namespace isolation**: Ensures volumes are isolated between namespaces
- **Lifecycle management**: Create, mount, unmount, delete operations
- **Multi-tenant scenarios**: Concurrent namespace operations
- **Storage class validation**: Different provisioning configurations
- **Error handling**: Graceful failure scenarios
- **Cleanup validation**: Proper resource cleanup

**Key Test Cases:**
- `TestEfsNsBasicProvisioning`: Basic volume creation and mounting
- `TestEfsNsNamespaceIsolation`: Cross-namespace isolation validation
- `TestEfsNsMultiTenant`: Concurrent multi-namespace operations
- `TestEfsNsStorageClassVariations`: Different storage class configurations
- `TestEfsNsErrorHandling`: Error scenario validation
- `TestEfsNsCleanupValidation`: Resource cleanup verification

### Performance Tests (`test/integration/performance/`)

Benchmarks against performance requirements:

- **Creation time**: Filesystem creation <30s requirement
- **PVC lifecycle**: PVC operations <2min requirement
- **Concurrent operations**: Multi-namespace parallel provisioning
- **Resource utilization**: Memory and CPU usage monitoring
- **Throughput testing**: Data transfer performance
- **Scalability limits**: Maximum namespace/volume limits

**Performance Targets:**
- EFS filesystem creation: <30 seconds
- PVC lifecycle (create to bound): <2 minutes
- Concurrent provisioning: 10+ namespaces simultaneously
- Memory usage: <500MB per driver instance
- Error rate: <0.1% under normal conditions

### Security Tests (`test/integration/security/`)

Comprehensive security validation:

- **Encryption**: In-transit and at-rest encryption validation
- **Access control**: RBAC and permission validation
- **Network security**: Network policy enforcement
- **Vulnerability scanning**: Security compliance checks
- **Compliance frameworks**: PCI-DSS, HIPAA, SOC2 validation
- **Data leakage prevention**: Cross-namespace data isolation

**Security Standards:**
- All data encrypted in transit and at rest
- No cross-namespace data access
- Compliance with major security frameworks
- Regular vulnerability scanning and remediation

### Reliability Tests (`test/integration/reliability/`)

Chaos engineering and resilience testing:

- **Node failures**: Driver resilience during node failures
- **Network partitions**: Handling network connectivity issues
- **Storage failures**: EFS service disruption scenarios
- **High load**: Performance under stress conditions
- **Recovery testing**: Automatic recovery validation
- **Availability monitoring**: 99.9% availability target

**Reliability Scenarios:**
- Kubernetes node failures during operations
- AWS EFS service temporary unavailability
- Network connectivity interruptions
- High concurrent load testing
- Resource exhaustion scenarios

### Monitoring Tests (`test/integration/monitoring/`)

Resource monitoring and leak detection:

- **Memory leak detection**: Long-running memory usage monitoring
- **Orphaned resource detection**: AWS and Kubernetes resource leaks
- **Cleanup automation**: Automated cleanup system validation
- **Resource tracking**: Comprehensive resource lifecycle tracking
- **Metrics collection**: Performance and health metrics
- **Alert validation**: Monitoring system integration

**Monitoring Capabilities:**
- Automated detection of resource leaks
- Comprehensive cleanup of test resources
- Performance metrics collection
- Integration with monitoring systems

### Compliance Tests (`test/integration/compliance/`)

CSI specification compliance validation:

- **Identity Service**: Plugin identification and capabilities
- **Controller Service**: Volume lifecycle management
- **Node Service**: Volume mounting and node operations
- **Parameter validation**: Required vs optional parameter handling
- **Error handling**: Proper error code responses
- **Idempotency**: Operation safety and consistency

**CSI Compliance Areas:**
- Full CSI v1.2.0 specification adherence
- Proper error handling and response codes
- Idempotent operation behavior
- Parameter validation and sanitization

## Test Configuration

### Configuration Files

- `test-config.yaml.template`: Template configuration file
- `test-config.yaml`: Local configuration (not in git)
- Test suite specific configs in each test directory

### Environment Variables

Key environment variables for test execution:

```bash
# Required
export AWS_REGION="us-west-2"
export KUBECONFIG="~/.kube/config"

# Optional
export EFS_NS_TEST_EFS_ID="fs-1234567890abcdef0"  # Use existing EFS
export EFS_NS_TEST_CLUSTER="my-test-cluster"      # EKS cluster name
export EFS_NS_TEST_TIMEOUT="3600"                 # Test timeout in seconds
export EFS_NS_PARALLEL_TESTS="4"                  # Parallel test count
export EFS_NS_VERBOSE="true"                      # Verbose output
export EFS_NS_FAIL_FAST="false"                   # Stop on first failure
```

### Test Naming Conventions

Test resources use consistent naming and tagging:

- **Namespaces**: `efs-ns-test-<suite>-<timestamp>-<random>`
- **PVCs**: `test-pvc-<scenario>-<timestamp>`
- **PVs**: `test-pv-<scenario>-<timestamp>`
- **Labels**: `efs-ns-test=true`, `test-suite=<suite-name>`
- **AWS Tags**: `efs-ns-test=true`, `TestSuite=<suite-name>`

## Test Reports

### Report Generation

Tests generate comprehensive reports in the `test-reports/` directory:

- **JSON Reports**: Machine-readable test results
- **HTML Reports**: Human-readable test summaries  
- **JUnit XML**: CI/CD integration format
- **Log Files**: Detailed execution logs
- **Metrics Data**: Performance and resource metrics

### Report Structure

```
test-reports/
├── YYYYMMDD-HHMMSS/              # Timestamp-based report directory
│   ├── test-summary.json         # Overall test summary
│   ├── system-test-results.json  # System test results
│   ├── performance-test-results.json
│   ├── security-test-results.json
│   ├── reliability-test-results.json
│   ├── monitoring-test-results.json
│   ├── compliance-test-results.json
│   ├── test-config.json          # Test configuration used
│   └── logs/                     # Individual test logs
│       ├── system-test.log
│       ├── performance-test.log
│       └── ...
```

### CI/CD Integration

GitHub Actions workflow (`.github/workflows/efs-ns-integration-tests.yml`):

- **Trigger Events**: Push, PR, schedule, manual dispatch
- **Matrix Strategy**: Parallel test execution across different suites
- **Security Scanning**: Integrated vulnerability and security analysis
- **Artifact Upload**: Test reports and logs preserved
- **Status Reporting**: PR comments and status checks

## Troubleshooting

### Common Issues

#### 1. AWS Permission Errors
```bash
# Ensure proper AWS permissions
aws sts get-caller-identity
aws efs describe-file-systems --region $AWS_REGION
```

#### 2. Kubernetes Connectivity
```bash
# Test cluster connectivity
kubectl cluster-info
kubectl get nodes
kubectl get storageclass
```

#### 3. Test Resource Cleanup
```bash
# Manual cleanup if tests fail
./scripts/cleanup-efs-ns-test-resources.sh

# Force cleanup of stuck resources
./scripts/cleanup-efs-ns-test-resources.sh --force
```

#### 4. Test Failures
```bash
# Run with verbose output for debugging
./scripts/run-efs-ns-integration-tests.sh --verbose

# Run single test suite for focused debugging
make test-efs-ns-system
```

### Debug Mode

Enable debug mode for detailed troubleshooting:

```bash
export EFS_NS_DEBUG="true"
export EFS_NS_VERBOSE="true"
./scripts/run-efs-ns-integration-tests.sh --verbose
```

### Log Analysis

Test logs are organized by suite and timestamp:

```bash
# View latest test logs
ls -la test-reports/
cat test-reports/latest/system-test.log

# Analyze test failures
grep -E "(FAIL|ERROR)" test-reports/latest/*.log
```

## Development

### Adding New Tests

1. Choose appropriate test suite directory
2. Follow existing test patterns and naming conventions
3. Use testify/suite framework for consistency
4. Implement proper resource cleanup
5. Add comprehensive test documentation
6. Update this README with new test information

### Test Helper Functions

Common test utilities are available in:
- `test/utils/testhelpers.go`: General test helper functions
- `test/utils/efsnstesthelper.go`: EFS namespace specific helpers
- `test/utils/resourcetracker.go`: Resource tracking and cleanup

### Contributing

1. Follow Go testing best practices
2. Ensure all tests pass locally before submitting
3. Include appropriate test documentation
4. Add integration with automation scripts
5. Validate CI/CD pipeline integration

## Support

For issues, questions, or contributions:

1. Check existing GitHub issues
2. Review troubleshooting section
3. Run tests in debug mode
4. Create detailed issue reports with logs
5. Include test environment details

## References

- [AWS EFS CSI Driver Documentation](https://github.com/kubernetes-sigs/aws-efs-csi-driver)
- [Container Storage Interface (CSI) Specification](https://kubernetes-csi.github.io/docs/)
- [Kubernetes Storage Documentation](https://kubernetes.io/docs/concepts/storage/)
- [AWS EFS Documentation](https://docs.aws.amazon.com/efs/)
- [Go Testing Framework](https://pkg.go.dev/testing)
- [Testify Documentation](https://pkg.go.dev/github.com/stretchr/testify)