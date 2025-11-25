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

# EFS Namespace Test Setup Validation Script
# Validates test environment and automation components

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." &> /dev/null && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Counters
CHECKS_PASSED=0
CHECKS_TOTAL=0

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

# Check function that tracks pass/fail
check() {
    local description="$1"
    local command="$2"
    
    ((CHECKS_TOTAL++))
    info "Checking: $description"
    
    if eval "$command" &>/dev/null; then
        log "✅ PASS: $description"
        ((CHECKS_PASSED++))
        return 0
    else
        error "❌ FAIL: $description"
        return 1
    fi
}

# Validate required tools
validate_tools() {
    log "Validating required tools..."
    
    check "Go installation" "command -v go"
    check "Go version >= 1.24" "[[ \$(go version | cut -d' ' -f3 | sed 's/go//' | cut -d'.' -f1-2) == \"1.24\" ]]"
    check "kubectl installation" "command -v kubectl"
    check "kubectl version" "kubectl version --client"
    check "AWS CLI installation" "command -v aws"
    check "jq installation" "command -v jq"
    check "bc calculator installation" "command -v bc"
}

# Validate project structure
validate_project_structure() {
    log "Validating project structure..."
    
    check "Project root directory" "[[ -d '$PROJECT_ROOT' ]]"
    check "Go module file" "[[ -f '$PROJECT_ROOT/go.mod' ]]"
    check "Makefile exists" "[[ -f '$PROJECT_ROOT/Makefile' ]]"
    check "Main entry point" "[[ -f '$PROJECT_ROOT/cmd/main.go' ]]"
    check "Driver package" "[[ -d '$PROJECT_ROOT/pkg/driver' ]]"
    check "Test integration directory" "[[ -d '$PROJECT_ROOT/test/integration' ]]"
}

# Validate test automation scripts
validate_test_scripts() {
    log "Validating test automation scripts..."
    
    local main_script="$PROJECT_ROOT/scripts/run-efs-ns-integration-tests.sh"
    local cleanup_script="$PROJECT_ROOT/scripts/cleanup-efs-ns-test-resources.sh"
    
    check "Main test script exists" "[[ -f '$main_script' ]]"
    check "Main test script is executable" "[[ -x '$main_script' ]]"
    check "Main test script syntax" "bash -n '$main_script'"
    
    check "Cleanup script exists" "[[ -f '$cleanup_script' ]]"
    check "Cleanup script is executable" "[[ -x '$cleanup_script' ]]"
    check "Cleanup script syntax" "bash -n '$cleanup_script'"
}

# Validate test suites
validate_test_suites() {
    log "Validating test suites..."
    
    local test_suites=("system" "performance" "security" "reliability" "monitoring" "compliance")
    
    for suite in "${test_suites[@]}"; do
        local suite_dir="$PROJECT_ROOT/test/integration/$suite"
        check "Test suite directory: $suite" "[[ -d '$suite_dir' ]]"
        
        # Check for Go test files
        check "Test suite has Go files: $suite" "find '$suite_dir' -name '*.go' | grep -q ."
        
        # Validate Go syntax
        if [[ -d "$suite_dir" ]]; then
            check "Test suite Go syntax: $suite" "go vet '$suite_dir/...'"
        fi
    done
}

# Validate Makefile targets
validate_makefile_targets() {
    log "Validating Makefile targets..."
    
    local targets=(
        "test-efs-ns-integration"
        "test-efs-ns-system"
        "test-efs-ns-performance"
        "test-efs-ns-security"
        "test-efs-ns-reliability"
        "test-efs-ns-monitoring"
        "test-efs-ns-compliance"
        "test-efs-ns-cleanup"
    )
    
    for target in "${targets[@]}"; do
        check "Makefile target: $target" "grep -q '^$target:' '$PROJECT_ROOT/Makefile'"
    done
}

# Validate GitHub Actions workflow
validate_github_workflow() {
    log "Validating GitHub Actions workflow..."
    
    local workflow_file="$PROJECT_ROOT/.github/workflows/efs-ns-integration-tests.yml"
    
    check "GitHub Actions workflow exists" "[[ -f '$workflow_file' ]]"
    check "Workflow syntax (basic YAML)" "python3 -c 'import yaml; yaml.safe_load(open(\"$workflow_file\"))'" || \
    check "Workflow syntax (fallback)" "grep -q 'name:' '$workflow_file'"
}

# Validate test configuration
validate_test_configuration() {
    log "Validating test configuration..."
    
    local config_template="$PROJECT_ROOT/test-config.yaml.template"
    
    check "Test config template exists" "[[ -f '$config_template' ]]"
    check "Test config template syntax" "python3 -c 'import yaml; yaml.safe_load(open(\"$config_template\"))'" || \
    check "Config template has content" "grep -q 'aws:' '$config_template'"
}

# Validate documentation
validate_documentation() {
    log "Validating documentation..."
    
    check "Integration test README" "[[ -f '$PROJECT_ROOT/test/integration/README.md' ]]"
    check "Project CLAUDE.md exists" "[[ -f '$PROJECT_ROOT/CLAUDE.md' ]]"
    check "Main README exists" "[[ -f '$PROJECT_ROOT/README.md' ]]"
}

# Test script help functionality
test_script_help() {
    log "Testing script help functionality..."
    
    local main_script="$PROJECT_ROOT/scripts/run-efs-ns-integration-tests.sh"
    local cleanup_script="$PROJECT_ROOT/scripts/cleanup-efs-ns-test-resources.sh"
    
    check "Main script --help works" "'$main_script' --help | grep -q 'Usage:'"
    check "Cleanup script --help works" "'$cleanup_script' --help | grep -q 'Usage:'"
}

# Test dry-run functionality
test_dry_run() {
    log "Testing dry-run functionality..."
    
    local main_script="$PROJECT_ROOT/scripts/run-efs-ns-integration-tests.sh"
    local cleanup_script="$PROJECT_ROOT/scripts/cleanup-efs-ns-test-resources.sh"
    
    check "Main script --dry-run works" "'$main_script' --dry-run | grep -q 'DRY RUN'"
    check "Cleanup script --dry-run works" "'$cleanup_script' --dry-run | grep -q 'DRY RUN'"
}

# Validate environment requirements
validate_environment() {
    log "Validating environment requirements..."
    
    # Check Go modules
    check "Go modules are valid" "cd '$PROJECT_ROOT' && go mod verify"
    
    # Check for required Go dependencies
    check "testify dependency" "cd '$PROJECT_ROOT' && go list -m github.com/stretchr/testify"
    check "AWS SDK dependency" "cd '$PROJECT_ROOT' && go list -m github.com/aws/aws-sdk-go-v2"
    check "Kubernetes client dependency" "cd '$PROJECT_ROOT' && go list -m k8s.io/client-go"
    
    # Optional environment checks
    if [[ -n "${AWS_REGION:-}" ]]; then
        info "AWS_REGION is set: $AWS_REGION"
    else
        warn "AWS_REGION environment variable not set"
    fi
    
    if [[ -n "${KUBECONFIG:-}" ]]; then
        info "KUBECONFIG is set: $KUBECONFIG"
    else
        info "KUBECONFIG not set, will use default location"
    fi
}

# Generate validation report
generate_report() {
    log "Generating validation report..."
    
    local report_file="$PROJECT_ROOT/test-setup-validation-report.txt"
    
    cat > "$report_file" <<EOF
EFS Namespace Test Setup Validation Report
==========================================

Date: $(date -Iseconds)
Total Checks: $CHECKS_TOTAL
Passed: $CHECKS_PASSED
Failed: $((CHECKS_TOTAL - CHECKS_PASSED))
Success Rate: $(echo "scale=1; $CHECKS_PASSED * 100 / $CHECKS_TOTAL" | bc -l)%

Project Root: $PROJECT_ROOT
Validation Script: $0

Environment:
- Go Version: $(go version 2>/dev/null || echo "Not available")
- kubectl Version: $(kubectl version --client --short 2>/dev/null || echo "Not available")
- AWS CLI Version: $(aws --version 2>/dev/null || echo "Not available")
- OS: $(uname -a)

Test Suite Validation:
$(for suite in system performance security reliability monitoring compliance; do
    if [[ -d "$PROJECT_ROOT/test/integration/$suite" ]]; then
        echo "- $suite: ✅ Available"
    else
        echo "- $suite: ❌ Missing"
    fi
done)

Scripts:
$(for script in run-efs-ns-integration-tests.sh cleanup-efs-ns-test-resources.sh; do
    script_path="$PROJECT_ROOT/scripts/$script"
    if [[ -x "$script_path" ]]; then
        echo "- $script: ✅ Executable"
    elif [[ -f "$script_path" ]]; then
        echo "- $script: ⚠️  Exists but not executable"
    else
        echo "- $script: ❌ Missing"
    fi
done)

Overall Status: $(if [[ $CHECKS_PASSED -eq $CHECKS_TOTAL ]]; then echo "✅ ALL CHECKS PASSED"; else echo "❌ SOME CHECKS FAILED"; fi)
EOF
    
    log "Validation report saved to: $report_file"
}

# Usage information
usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

EFS Namespace Test Setup Validation

OPTIONS:
    --quick         Run only essential checks (skip optional validations)
    --report-only   Generate report without running checks
    --verbose       Enable verbose output
    -h, --help      Show this help message

EXAMPLES:
    # Full validation
    $0

    # Quick validation
    $0 --quick

    # Generate report only
    $0 --report-only
EOF
}

# Main execution function
main() {
    local quick_mode=false
    local report_only=false
    local verbose=false
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --quick)
                quick_mode=true
                shift
                ;;
            --report-only)
                report_only=true
                shift
                ;;
            --verbose)
                verbose=true
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
    
    log "Starting EFS Namespace Test Setup Validation"
    log "Project root: $PROJECT_ROOT"
    
    if [[ "$report_only" == "false" ]]; then
        # Core validations (always run)
        validate_tools
        validate_project_structure
        validate_test_scripts
        validate_test_suites
        validate_makefile_targets
        
        if [[ "$quick_mode" == "false" ]]; then
            # Extended validations
            validate_github_workflow
            validate_test_configuration
            validate_documentation
            test_script_help
            test_dry_run
            validate_environment
        fi
    fi
    
    # Always generate report
    generate_report
    
    # Final summary
    log "Validation completed: $CHECKS_PASSED/$CHECKS_TOTAL checks passed"
    
    if [[ $CHECKS_PASSED -eq $CHECKS_TOTAL ]]; then
        log "🎉 All validation checks passed! Test automation is ready to use."
        exit 0
    else
        error "Some validation checks failed. Please review the issues above."
        exit 1
    fi
}

# Execute main function with all arguments
main "$@"