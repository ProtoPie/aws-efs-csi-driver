# EFS CSI Driver Failure Recovery Test Guide

## Overview

This document provides a comprehensive guide to the failure recovery test scenarios implemented for the EFS namespace provisioning feature. These tests ensure the system can gracefully handle and recover from various failure conditions that may occur in production environments.

## Test Architecture

### Core Components

1. **FailureSimulator** - Central component for simulating various failure scenarios
2. **FailureInjector** - Injects failures into AWS API operations
3. **RecoveryTester** - Validates recovery mechanisms and tracks metrics
4. **Test Environments** - Specialized test environments for different failure categories

### Test Categories

The failure recovery tests are organized into three main categories:

#### 1. API Timeout Simulations
Tests the system's ability to handle and recover from AWS API timeouts.

#### 2. Partial Failure Recovery
Tests recovery from scenarios where some operations succeed while others fail.

#### 3. CRD Loss Recovery
Tests the system's ability to recover from Custom Resource Definition (CRD) loss or corruption.

## Test Scenarios

### API Timeout Simulations

#### Test: EFS Creation Timeout Recovery
- **Purpose**: Validates recovery from EFS filesystem creation timeout
- **Simulation**: Configurable timeout after 3 seconds, recovery after 10 seconds
- **Expected Behavior**: System retries with exponential backoff and eventually succeeds
- **Key Validations**:
  - Retry mechanism activates
  - EFS is successfully created after recovery
  - Idempotency is maintained

#### Test: Mount Target Creation Timeout Recovery
- **Purpose**: Validates recovery from mount target creation timeout
- **Simulation**: Timeout on mount target creation with partial success allowed
- **Expected Behavior**: System retries failed mount targets while preserving successful ones
- **Key Validations**:
  - Mount targets created in all availability zones
  - PVC creation works despite initial failures
  - No duplicate mount targets created

#### Test: Access Point Creation Timeout Recovery
- **Purpose**: Validates recovery from access point creation timeout
- **Simulation**: 50% failure rate on access point creation
- **Expected Behavior**: Failed access point creations are retried successfully
- **Key Validations**:
  - All PVCs eventually get access points
  - No orphaned access points
  - Concurrent PVC creation handled correctly

#### Test: Concurrent API Timeouts
- **Purpose**: Tests handling of multiple simultaneous API timeouts
- **Simulation**: Timeouts on EFS, mount target, and access point operations
- **Expected Behavior**: System handles all failures concurrently and recovers
- **Key Validations**:
  - All operations eventually succeed
  - No resource conflicts
  - Proper retry coordination

#### Test: Cascading API Failures
- **Purpose**: Tests recovery from failures that trigger additional failures
- **Simulation**: Initial failure propagates to dependent operations
- **Expected Behavior**: System recovers in correct order
- **Key Validations**:
  - Recovery follows dependency order
  - All resources eventually created
  - No partial provisioning state

### Partial Failure Recovery

#### Test: Partial Mount Target Creation
- **Purpose**: Tests recovery when some mount targets fail
- **Simulation**: Failure in specific subnet/AZ
- **Expected Behavior**: Failed mount targets are retried while successful ones are preserved
- **Key Validations**:
  - All AZs eventually have mount targets
  - System remains operational with partial mount targets
  - PVC creation succeeds

#### Test: Incomplete EFS Provisioning
- **Purpose**: Tests recovery from interrupted provisioning workflow
- **Simulation**: Provisioning stops after EFS creation but before mount targets
- **Expected Behavior**: System detects incomplete state and completes provisioning
- **Key Validations**:
  - All provisioning stages completed
  - No orphaned resources
  - State consistency maintained

#### Test: Mixed Success and Failure
- **Purpose**: Tests handling of mixed operation results
- **Simulation**: Configurable success rates for different operations
- **Expected Behavior**: Failed operations are retried while successful ones are preserved
- **Key Validations**:
  - All operations eventually succeed
  - No duplicate resources
  - Proper state tracking

#### Test: Resource Cleanup After Failure
- **Purpose**: Tests cleanup of resources after provisioning failure
- **Simulation**: Critical failure during provisioning
- **Expected Behavior**: All partially created resources are cleaned up
- **Key Validations**:
  - No orphaned resources
  - Cleanup follows reverse creation order
  - State is reset correctly

#### Test: Rollback on Critical Failure
- **Purpose**: Tests atomic rollback on critical failures
- **Simulation**: Critical error triggers full rollback
- **Expected Behavior**: All changes are rolled back atomically
- **Key Validations**:
  - Complete rollback of all resources
  - State saved for debugging
  - System ready for retry

### CRD Loss Recovery

#### Test: CRD Deletion During Operation
- **Purpose**: Tests handling of CRD deletion during active operations
- **Simulation**: CRD deleted while PVC creation in progress
- **Expected Behavior**: Operation completes using AWS tags, CRD recreated
- **Key Validations**:
  - PVC creation succeeds
  - CRD automatically recreated
  - Mapping consistency maintained

#### Test: CRD Recreation from AWS Tags
- **Purpose**: Tests CRD recovery from AWS resource tags
- **Simulation**: All CRDs deleted, then recovery triggered
- **Expected Behavior**: CRDs recreated with correct mappings from AWS tags
- **Key Validations**:
  - All namespace mappings restored
  - EFS IDs correctly mapped
  - System fully operational after recovery

#### Test: Multiple CRD Loss and Recovery
- **Purpose**: Tests concurrent CRD loss scenarios
- **Simulation**: Random CRDs deleted during concurrent operations
- **Expected Behavior**: All operations succeed, CRDs recovered
- **Key Validations**:
  - No operation failures
  - All CRDs eventually present
  - Correct mappings maintained

#### Test: CRD Corruption Recovery
- **Purpose**: Tests recovery from corrupted CRD data
- **Simulation**: CRD data corrupted with invalid values
- **Expected Behavior**: Corruption detected and corrected from AWS state
- **Key Validations**:
  - Corruption detected
  - Correct data restored
  - Audit log created

#### Test: CRD Sync with AWS State
- **Purpose**: Tests synchronization between CRDs and AWS resources
- **Simulation**: Out-of-band changes to AWS resources
- **Expected Behavior**: CRDs updated to match AWS state
- **Key Validations**:
  - Discrepancies detected
  - CRDs updated correctly
  - Periodic sync working

## Running the Tests

### Prerequisites

1. AWS account with appropriate permissions
2. Kubernetes cluster with EFS CSI driver installed
3. Test environment configured with:
   ```bash
   export AWS_REGION=us-west-2
   export AWS_AVAILABILITY_ZONES=us-west-2a,us-west-2b,us-west-2c
   ```

### Running Individual Test Categories

```bash
# Run API timeout tests
go test -v ./test/e2e -run TestFailureRecoveryScenarios/API_Timeout_Simulations

# Run partial failure tests
go test -v ./test/e2e -run TestFailureRecoveryScenarios/Partial_Failure_Recovery

# Run CRD loss tests
go test -v ./test/e2e -run TestFailureRecoveryScenarios/CRD_Loss_Recovery
```

### Running All Failure Recovery Tests

```bash
go test -v ./test/e2e -run TestFailureRecoveryScenarios -timeout 30m
```

### Running with Verbose Logging

```bash
go test -v ./test/e2e -run TestFailureRecoveryScenarios -args -v=4
```

## Configuration Options

### Timeout Configuration

```go
timeoutConfig := &TimeoutConfig{
    Operation:      "CreateFileSystem",
    InitialTimeout: 3 * time.Second,
    RecoveryTime:   10 * time.Second,
    MaxRetries:     5,
    FailureRate:    0.8, // 80% failure rate
}
```

### Partial Failure Configuration

```go
partialConfig := &PartialFailureConfig{
    Operation:       "CreateMountTarget",
    FailurePattern:  "subnet-2",
    AffectedPercent: 0.33, // 1 out of 3 fails
    Recoverable:     true,
    RecoveryDelay:   2 * time.Second,
}
```

### Cascading Failure Configuration

```go
cascadingConfig := &CascadingFailureRule{
    TriggerOperation:   "CreateFileSystem",
    AffectedOperations: []string{"CreateMountTarget", "CreateAccessPoint"},
    PropagationDelay:   1 * time.Second,
    RecoveryOrder:      []string{"CreateFileSystem", "CreateMountTarget", "CreateAccessPoint"},
}
```

## Metrics and Monitoring

### Recovery Metrics

The tests collect the following metrics:

- **Total Failures**: Number of simulated failures
- **Successful Recovery**: Number of successful recoveries
- **Failed Recovery**: Number of failed recovery attempts
- **Average Recovery Time**: Average time to recover from failure
- **Max/Min Recovery Time**: Maximum and minimum recovery times

### Viewing Test Metrics

```go
metrics := recoveryTester.GetMetrics()
fmt.Printf("Recovery Rate: %.2f%%\n",
    float64(metrics.SuccessfulRecovery)/float64(metrics.TotalFailures)*100)
fmt.Printf("Average Recovery Time: %v\n", metrics.AverageRecoveryTime)
```

## Debugging Failed Tests

### Enable Debug Logging

```bash
export KLOG_LEVEL=4
go test -v ./test/e2e -run TestFailureRecoveryScenarios
```

### Analyze Failure History

Tests maintain a failure history that can be analyzed:

```go
history := simulator.GetFailureHistory()
for _, event := range history {
    fmt.Printf("[%s] Operation: %s, Error: %v, Recovered: %v\n",
        event.Timestamp, event.Operation, event.Error, event.Recovered)
}
```

### Check for Orphaned Resources

```go
orphans := simulator.GetOrphanedResources()
for _, resource := range orphans {
    fmt.Printf("Orphaned: %s (%s) - State: %s\n",
        resource.ResourceID, resource.ResourceType, resource.State)
}
```

## Best Practices

### 1. Test Isolation
- Each test should create its own namespace
- Use unique test IDs to prevent conflicts
- Clean up resources after test completion

### 2. Failure Simulation
- Start with simple failures before complex scenarios
- Use realistic timeout values
- Test both transient and permanent failures

### 3. Recovery Validation
- Verify not just success, but correct state
- Check for resource leaks and orphans
- Validate idempotency

### 4. Performance Considerations
- Set appropriate test timeouts
- Use parallel execution where possible
- Monitor resource consumption during tests

## Extending the Tests

### Adding New Failure Scenarios

1. Create a new test function in `namespace_provisioning_failure_recovery_test.go`
2. Configure the failure simulator with appropriate settings
3. Execute the operation and validate recovery
4. Add metrics collection and validation

Example:

```go
func testCustomFailureScenario(t *testing.T) {
    // Setup test environment
    env, err := setupCustomTestEnv(t, "custom-test")
    require.NoError(t, err)
    defer env.Cleanup(context.Background())

    // Configure failure
    customConfig := &CustomFailureConfig{
        // Your configuration
    }
    env.ApplyCustomFailure(customConfig)

    // Execute test
    result, err := performOperation()

    // Validate recovery
    assert.NoError(t, err, "Should recover from failure")
    assert.NotNil(t, result, "Should have valid result")
}
```

### Adding New Recovery Mechanisms

1. Extend the `FailureSimulator` with new recovery logic
2. Add recovery callbacks for monitoring
3. Update metrics collection
4. Add tests for the new mechanism

## Troubleshooting Common Issues

### Issue: Tests Timeout
**Solution**: Increase test timeout or reduce recovery delays
```bash
go test -timeout 60m ./test/e2e -run TestFailureRecoveryScenarios
```

### Issue: Flaky Tests
**Solution**: Add retry logic and increase stabilization delays
```go
require.Eventually(t, func() bool {
    // Your assertion
}, 30*time.Second, 1*time.Second)
```

### Issue: Resource Cleanup Failures
**Solution**: Use defer statements and force cleanup
```go
defer func() {
    if err := env.ForceCleanup(ctx); err != nil {
        t.Logf("Cleanup failed: %v", err)
    }
}()
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Failure Recovery Tests

on:
  pull_request:
    paths:
      - 'pkg/driver/**'
      - 'test/e2e/**'

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2

      - name: Setup Go
        uses: actions/setup-go@v2
        with:
          go-version: 1.21

      - name: Configure AWS
        uses: aws-actions/configure-aws-credentials@v1
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-west-2

      - name: Run Failure Recovery Tests
        run: |
          go test -v ./test/e2e \
            -run TestFailureRecoveryScenarios \
            -timeout 45m \
            -parallel 4
```

## Performance Benchmarks

Expected recovery times under normal conditions:

| Failure Type | Average Recovery Time | Max Recovery Time |
|--------------|----------------------|-------------------|
| API Timeout | 5-10 seconds | 30 seconds |
| Partial Mount Target | 10-15 seconds | 45 seconds |
| CRD Loss | 2-5 seconds | 15 seconds |
| Cascading Failure | 15-30 seconds | 60 seconds |
| Complete Rollback | 20-40 seconds | 90 seconds |

## Related Documentation

- [EFS Namespace Provisioning Requirements](../.claude/specs/efs-ns-provisioning/requirements.md)
- [EFS Namespace Provisioning Design](../.claude/specs/efs-ns-provisioning/design.md)
- [EFS CSI Driver Documentation](../../docs/README.md)
- [AWS EFS Best Practices](https://docs.aws.amazon.com/efs/latest/ug/best-practices.html)