#!/bin/bash
# Copyright 2024 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# EFS Namespace Integration Test Automation Script
# Orchestrates comprehensive testing of EFS namespace functionality

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." &> /dev/null && pwd)"
REPORTS_DIR="${PROJECT_ROOT}/test-reports/$(date +%Y%m%d-%H%M%S)"
KUBECONFIG="${KUBECONFIG:-${HOME}/.kube/config}"

# Test configuration
export EFS_NS_TEST_TIMEOUT="${EFS_NS_TEST_TIMEOUT:-3600}"  # 1 hour default
export EFS_NS_PARALLEL_TESTS="${EFS_NS_PARALLEL_TESTS:-4}"
export EFS_NS_VERBOSE="${EFS_NS_VERBOSE:-false}"
export EFS_NS_FAIL_FAST="${EFS_NS_FAIL_FAST:-false}"

# Test suite configuration
declare -A TEST_SUITES=(
    ["system"]="System Integration Tests"
    ["performance"]="Performance Benchmark Tests"
    ["security"]="Security Compliance Tests"
    ["reliability"]="Reliability & Chaos Engineering Tests"
    ["monitoring"]="Resource Monitoring & Leak Detection Tests"
    ["compliance"]="CSI Specification Compliance Tests"
)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $*${NC}" >&2
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $*${NC}" >&2
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $*${NC}" >&2
}

info() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')] INFO: $*${NC}" >&2
}

# Usage information
usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

EFS Namespace Integration Test Automation

OPTIONS:
    -s, --suites SUITES     Comma-separated list of test suites to run
                           (default: all suites)
                           Available: system,performance,security,reliability,monitoring,compliance
    -t, --timeout SECONDS  Test timeout in seconds (default: 3600)
    -p, --parallel COUNT   Number of parallel test processes (default: 4)
    -v, --verbose          Enable verbose output
    -f, --fail-fast        Stop on first test failure
    -r, --reports-dir DIR  Custom reports directory
    -k, --kubeconfig FILE  Kubeconfig file path (default: ~/.kube/config)
    --cleanup-only         Only run cleanup operations
    --dry-run             Show what would be executed without running tests
    -h, --help            Show this help message

ENVIRONMENT VARIABLES:
    AWS_REGION            AWS region for testing (required)
    EFS_NS_TEST_EFS_ID    Existing EFS filesystem ID for testing
    EFS_NS_TEST_CLUSTER   EKS cluster name for testing
    KUBECONFIG           Kubeconfig file path

EXAMPLES:
    # Run all test suites with default settings
    $0

    # Run specific test suites with verbose output
    $0 -s system,performance -v

    # Run tests with custom timeout and parallel settings
    $0 -t 7200 -p 8 --fail-fast

    # Dry run to see what would be executed
    $0 --dry-run
EOF
}

# Parse command line arguments
parse_args() {
    local suites=""
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            -s|--suites)
                suites="$2"
                shift 2
                ;;
            -t|--timeout)
                EFS_NS_TEST_TIMEOUT="$2"
                shift 2
                ;;
            -p|--parallel)
                EFS_NS_PARALLEL_TESTS="$2"
                shift 2
                ;;
            -v|--verbose)
                EFS_NS_VERBOSE="true"
                shift
                ;;
            -f|--fail-fast)
                EFS_NS_FAIL_FAST="true"
                shift
                ;;
            -r|--reports-dir)
                REPORTS_DIR="$2"
                shift 2
                ;;
            -k|--kubeconfig)
                KUBECONFIG="$2"
                shift 2
                ;;
            --cleanup-only)
                CLEANUP_ONLY="true"
                shift
                ;;
            --dry-run)
                DRY_RUN="true"
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done
    
    # Parse test suites
    if [[ -n "$suites" ]]; then
        IFS=',' read -ra SELECTED_SUITES <<< "$suites"
        # Validate suites
        for suite in "${SELECTED_SUITES[@]}"; do
            if [[ ! -v TEST_SUITES["$suite"] ]]; then
                error "Invalid test suite: $suite"
                error "Available suites: ${!TEST_SUITES[*]}"
                exit 1
            fi
        done
    else
        SELECTED_SUITES=("${!TEST_SUITES[@]}")
    fi
}

# Validate environment and prerequisites
validate_environment() {
    log "Validating environment and prerequisites..."
    
    # Check required tools
    local required_tools=("go" "kubectl" "aws")
    for tool in "${required_tools[@]}"; do
        if ! command -v "$tool" &> /dev/null; then
            error "Required tool not found: $tool"
            exit 1
        fi
    done
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        error "AWS credentials not configured or invalid"
        exit 1
    fi
    
    # Validate AWS region
    if [[ -z "${AWS_REGION:-}" ]]; then
        error "AWS_REGION environment variable is required"
        exit 1
    fi
    
    # Check Kubernetes connectivity
    if [[ ! -f "$KUBECONFIG" ]]; then
        error "Kubeconfig file not found: $KUBECONFIG"
        exit 1
    fi
    
    if ! kubectl version --client &> /dev/null; then
        error "kubectl not working properly"
        exit 1
    fi
    
    # Validate cluster connectivity
    if ! kubectl cluster-info &> /dev/null; then
        error "Cannot connect to Kubernetes cluster"
        exit 1
    fi
    
    # Check Go version
    local go_version
    go_version=$(go version | cut -d' ' -f3 | sed 's/go//')
    local required_go_version="1.24"
    if [[ "$(printf '%s\n' "$required_go_version" "$go_version" | sort -V | head -n1)" != "$required_go_version" ]]; then
        error "Go version $go_version is too old. Minimum required: $required_go_version"
        exit 1
    fi
    
    log "Environment validation completed successfully"
}

# Setup test environment
setup_test_environment() {
    log "Setting up test environment..."
    
    # Create reports directory
    mkdir -p "$REPORTS_DIR"
    
    # Export test configuration
    export EFS_NS_REPORTS_DIR="$REPORTS_DIR"
    export EFS_NS_PROJECT_ROOT="$PROJECT_ROOT"
    
    # Create test configuration file
    cat > "$REPORTS_DIR/test-config.json" <<EOF
{
    "timestamp": "$(date -Iseconds)",
    "environment": {
        "aws_region": "${AWS_REGION}",
        "kubeconfig": "${KUBECONFIG}",
        "efs_filesystem_id": "${EFS_NS_TEST_EFS_ID:-}",
        "cluster_name": "${EFS_NS_TEST_CLUSTER:-}"
    },
    "configuration": {
        "timeout": ${EFS_NS_TEST_TIMEOUT},
        "parallel_tests": ${EFS_NS_PARALLEL_TESTS},
        "verbose": ${EFS_NS_VERBOSE},
        "fail_fast": ${EFS_NS_FAIL_FAST}
    },
    "selected_suites": [$(printf '"%s",' "${SELECTED_SUITES[@]}" | sed 's/,$//')]
}
EOF
    
    log "Test environment setup completed"
}

# Run individual test suite
run_test_suite() {
    local suite="$1"
    local suite_name="${TEST_SUITES[$suite]}"
    
    log "Running $suite_name..."
    
    local test_package="./test/integration/$suite"
    local report_file="$REPORTS_DIR/${suite}-test-results.json"
    local log_file="$REPORTS_DIR/${suite}-test.log"
    
    # Prepare test command
    local test_cmd=(
        "go" "test"
        "-v"
        "-race"
        "-timeout" "${EFS_NS_TEST_TIMEOUT}s"
        "-parallel" "$EFS_NS_PARALLEL_TESTS"
        "-json"
        "$test_package"
    )
    
    if [[ "$EFS_NS_VERBOSE" == "true" ]]; then
        test_cmd+=("-test.v")
    fi
    
    if [[ "$DRY_RUN" == "true" ]]; then
        info "DRY RUN: Would execute: ${test_cmd[*]}"
        return 0
    fi
    
    # Run the test
    local start_time
    start_time=$(date +%s)
    
    if "${test_cmd[@]}" > "$report_file" 2> "$log_file"; then
        local end_time
        end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        log "$suite_name completed successfully in ${duration}s"
        return 0
    else
        local exit_code=$?
        local end_time
        end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        error "$suite_name failed after ${duration}s (exit code: $exit_code)"
        
        # Show last few lines of error log
        if [[ -f "$log_file" ]]; then
            error "Last 10 lines of error log:"
            tail -10 "$log_file" | while read -r line; do
                error "  $line"
            done
        fi
        
        return $exit_code
    fi
}

# Cleanup test resources
cleanup_test_resources() {
    log "Cleaning up test resources..."
    
    if [[ "$DRY_RUN" == "true" ]]; then
        info "DRY RUN: Would run cleanup operations"
        return 0
    fi
    
    # Run cleanup script if it exists
    local cleanup_script="$PROJECT_ROOT/scripts/cleanup-efs-ns-test-resources.sh"
    if [[ -x "$cleanup_script" ]]; then
        log "Running dedicated cleanup script..."
        "$cleanup_script" || warn "Cleanup script failed, continuing..."
    fi
    
    # Generic Kubernetes resource cleanup
    log "Cleaning up Kubernetes test resources..."
    kubectl delete namespace --selector="efs-ns-test=true" --wait=true --timeout=300s || true
    kubectl delete pvc --all-namespaces --selector="efs-ns-test=true" --wait=true --timeout=300s || true
    kubectl delete pv --selector="efs-ns-test=true" --wait=true --timeout=300s || true
    
    log "Cleanup completed"
}

# Generate comprehensive test report
generate_test_report() {
    log "Generating comprehensive test report..."
    
    local report_file="$REPORTS_DIR/comprehensive-test-report.html"
    local summary_file="$REPORTS_DIR/test-summary.json"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        info "DRY RUN: Would generate test reports"
        return 0
    fi
    
    # Create test summary
    local total_suites=${#SELECTED_SUITES[@]}
    local passed_suites=0
    local failed_suites=0
    
    # Count passed/failed suites
    for suite in "${SELECTED_SUITES[@]}"; do
        local result_file="$REPORTS_DIR/${suite}-test-results.json"
        if [[ -f "$result_file" ]] && grep -q '"Action":"pass"' "$result_file"; then
            ((passed_suites++))
        else
            ((failed_suites++))
        fi
    done
    
    # Generate JSON summary
    cat > "$summary_file" <<EOF
{
    "timestamp": "$(date -Iseconds)",
    "total_suites": $total_suites,
    "passed_suites": $passed_suites,
    "failed_suites": $failed_suites,
    "success_rate": $(echo "scale=2; $passed_suites * 100 / $total_suites" | bc -l),
    "reports_directory": "$REPORTS_DIR",
    "suite_results": [
EOF
    
    # Add individual suite results
    local first=true
    for suite in "${SELECTED_SUITES[@]}"; do
        if [[ "$first" == "true" ]]; then
            first=false
        else
            echo "," >> "$summary_file"
        fi
        
        local result_file="$REPORTS_DIR/${suite}-test-results.json"
        local status="failed"
        if [[ -f "$result_file" ]] && grep -q '"Action":"pass"' "$result_file"; then
            status="passed"
        fi
        
        cat >> "$summary_file" <<EOF
        {
            "suite": "$suite",
            "name": "${TEST_SUITES[$suite]}",
            "status": "$status",
            "report_file": "${suite}-test-results.json",
            "log_file": "${suite}-test.log"
        }
EOF
    done
    
    echo -e "\n    ]\n}" >> "$summary_file"
    
    log "Test reports generated in: $REPORTS_DIR"
    log "Summary: $passed_suites/$total_suites test suites passed ($(echo "scale=1; $passed_suites * 100 / $total_suites" | bc -l)%)"
}

# Main execution function
main() {
    local start_time
    start_time=$(date +%s)
    
    log "Starting EFS Namespace Integration Test Automation"
    log "Reports directory: $REPORTS_DIR"
    
    # Parse arguments
    parse_args "$@"
    
    # Handle cleanup-only mode
    if [[ "${CLEANUP_ONLY:-false}" == "true" ]]; then
        cleanup_test_resources
        exit 0
    fi
    
    # Validate environment
    validate_environment
    
    # Setup test environment
    setup_test_environment
    
    # Track overall success
    local overall_success=true
    local failed_suites=()
    
    # Run test suites
    for suite in "${SELECTED_SUITES[@]}"; do
        info "Starting test suite: ${TEST_SUITES[$suite]}"
        
        if run_test_suite "$suite"; then
            log "Test suite '$suite' completed successfully"
        else
            error "Test suite '$suite' failed"
            failed_suites+=("$suite")
            overall_success=false
            
            if [[ "$EFS_NS_FAIL_FAST" == "true" ]]; then
                error "Fail-fast mode enabled, stopping execution"
                break
            fi
        fi
    done
    
    # Always run cleanup
    cleanup_test_resources
    
    # Generate reports
    generate_test_report
    
    # Final summary
    local end_time
    end_time=$(date +%s)
    local total_duration=$((end_time - start_time))
    
    if [[ "$overall_success" == "true" ]]; then
        log "All test suites completed successfully in ${total_duration}s"
        log "Test reports available in: $REPORTS_DIR"
        exit 0
    else
        error "Some test suites failed:"
        for suite in "${failed_suites[@]}"; do
            error "  - ${TEST_SUITES[$suite]} ($suite)"
        done
        error "Total execution time: ${total_duration}s"
        error "Test reports available in: $REPORTS_DIR"
        exit 1
    fi
}

# Execute main function with all arguments
main "$@"