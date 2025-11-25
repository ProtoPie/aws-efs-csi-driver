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

# EFS Namespace Test Resource Cleanup Script
# Comprehensive cleanup of test resources across AWS and Kubernetes

set -euo pipefail

# Configuration
AWS_REGION="${AWS_REGION:-us-west-2}"
CLEANUP_TIMEOUT="${CLEANUP_TIMEOUT:-300}"
DRY_RUN="${DRY_RUN:-false}"

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

# Cleanup Kubernetes resources
cleanup_kubernetes_resources() {
    log "Cleaning up Kubernetes test resources..."
    
    # Test namespaces
    local test_namespaces
    if test_namespaces=$(kubectl get namespaces -l "efs-ns-test=true" -o name 2>/dev/null); then
        if [[ -n "$test_namespaces" ]]; then
            log "Found test namespaces to cleanup:"
            echo "$test_namespaces" | while read -r ns; do
                log "  $ns"
            done
            
            if [[ "$DRY_RUN" == "false" ]]; then
                echo "$test_namespaces" | xargs -r kubectl delete --timeout="${CLEANUP_TIMEOUT}s" || warn "Failed to delete some test namespaces"
            else
                info "DRY RUN: Would delete test namespaces"
            fi
        fi
    fi
    
    # Test PVCs across all namespaces
    local test_pvcs
    if test_pvcs=$(kubectl get pvc --all-namespaces -l "efs-ns-test=true" -o name 2>/dev/null); then
        if [[ -n "$test_pvcs" ]]; then
            log "Found test PVCs to cleanup:"
            echo "$test_pvcs" | while read -r pvc; do
                log "  $pvc"
            done
            
            if [[ "$DRY_RUN" == "false" ]]; then
                kubectl delete pvc --all-namespaces -l "efs-ns-test=true" --timeout="${CLEANUP_TIMEOUT}s" || warn "Failed to delete some test PVCs"
            else
                info "DRY RUN: Would delete test PVCs"
            fi
        fi
    fi
    
    # Test PVs
    local test_pvs
    if test_pvs=$(kubectl get pv -l "efs-ns-test=true" -o name 2>/dev/null); then
        if [[ -n "$test_pvs" ]]; then
            log "Found test PVs to cleanup:"
            echo "$test_pvs" | while read -r pv; do
                log "  $pv"
            done
            
            if [[ "$DRY_RUN" == "false" ]]; then
                echo "$test_pvs" | xargs -r kubectl delete --timeout="${CLEANUP_TIMEOUT}s" || warn "Failed to delete some test PVs"
            else
                info "DRY RUN: Would delete test PVs"
            fi
        fi
    fi
    
    # Test StorageClasses
    local test_scs
    if test_scs=$(kubectl get storageclass -l "efs-ns-test=true" -o name 2>/dev/null); then
        if [[ -n "$test_scs" ]]; then
            log "Found test StorageClasses to cleanup:"
            echo "$test_scs" | while read -r sc; do
                log "  $sc"
            done
            
            if [[ "$DRY_RUN" == "false" ]]; then
                echo "$test_scs" | xargs -r kubectl delete || warn "Failed to delete some test StorageClasses"
            else
                info "DRY RUN: Would delete test StorageClasses"
            fi
        fi
    fi
    
    # Test ConfigMaps and Secrets
    for resource in configmap secret; do
        local test_resources
        if test_resources=$(kubectl get "$resource" --all-namespaces -l "efs-ns-test=true" -o name 2>/dev/null); then
            if [[ -n "$test_resources" ]]; then
                log "Found test ${resource}s to cleanup:"
                echo "$test_resources" | while read -r res; do
                    log "  $res"
                done
                
                if [[ "$DRY_RUN" == "false" ]]; then
                    kubectl delete "$resource" --all-namespaces -l "efs-ns-test=true" --timeout="${CLEANUP_TIMEOUT}s" || warn "Failed to delete some test ${resource}s"
                else
                    info "DRY RUN: Would delete test ${resource}s"
                fi
            fi
        fi
    done
    
    log "Kubernetes resource cleanup completed"
}

# Cleanup AWS EFS resources
cleanup_efs_resources() {
    log "Cleaning up AWS EFS test resources..."
    
    # Check if AWS CLI is available
    if ! command -v aws &> /dev/null; then
        warn "AWS CLI not found, skipping EFS resource cleanup"
        return 0
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        warn "AWS credentials not configured, skipping EFS resource cleanup"
        return 0
    fi
    
    # Cleanup test Access Points
    log "Looking for test EFS Access Points..."
    local access_points
    if access_points=$(aws efs describe-access-points --region "$AWS_REGION" --query 'AccessPoints[?contains(Tags[?Key==`efs-ns-test`].Value, `true`)].[AccessPointId,FileSystemId]' --output text 2>/dev/null); then
        if [[ -n "$access_points" ]]; then
            log "Found test Access Points to cleanup:"
            echo "$access_points" | while read -r ap_id fs_id; do
                log "  Access Point: $ap_id (FileSystem: $fs_id)"
                
                if [[ "$DRY_RUN" == "false" ]]; then
                    if aws efs delete-access-point --access-point-id "$ap_id" --region "$AWS_REGION" 2>/dev/null; then
                        log "Deleted Access Point: $ap_id"
                    else
                        warn "Failed to delete Access Point: $ap_id"
                    fi
                else
                    info "DRY RUN: Would delete Access Point: $ap_id"
                fi
            done
        fi
    fi
    
    # Cleanup test Mount Targets (only if they're specifically tagged for testing)
    log "Looking for test EFS Mount Targets..."
    local file_systems
    if file_systems=$(aws efs describe-file-systems --region "$AWS_REGION" --query 'FileSystems[?contains(Tags[?Key==`efs-ns-test`].Value, `true`)].FileSystemId' --output text 2>/dev/null); then
        if [[ -n "$file_systems" ]]; then
            echo "$file_systems" | while read -r fs_id; do
                log "Checking Mount Targets for test FileSystem: $fs_id"
                
                local mount_targets
                if mount_targets=$(aws efs describe-mount-targets --file-system-id "$fs_id" --region "$AWS_REGION" --query 'MountTargets[].MountTargetId' --output text 2>/dev/null); then
                    if [[ -n "$mount_targets" ]]; then
                        echo "$mount_targets" | while read -r mt_id; do
                            log "  Mount Target: $mt_id"
                            
                            if [[ "$DRY_RUN" == "false" ]]; then
                                if aws efs delete-mount-target --mount-target-id "$mt_id" --region "$AWS_REGION" 2>/dev/null; then
                                    log "Deleted Mount Target: $mt_id"
                                    # Wait for mount target to be deleted before proceeding
                                    local retries=0
                                    while [[ $retries -lt 30 ]]; do
                                        if ! aws efs describe-mount-targets --mount-target-id "$mt_id" --region "$AWS_REGION" &>/dev/null; then
                                            break
                                        fi
                                        sleep 10
                                        ((retries++))
                                    done
                                else
                                    warn "Failed to delete Mount Target: $mt_id"
                                fi
                            else
                                info "DRY RUN: Would delete Mount Target: $mt_id"
                            fi
                        done
                    fi
                fi
            done
        fi
    fi
    
    # Cleanup test File Systems (only those specifically tagged for testing)
    log "Looking for test EFS File Systems..."
    if [[ -n "$file_systems" ]]; then
        echo "$file_systems" | while read -r fs_id; do
            log "  Test FileSystem: $fs_id"
            
            if [[ "$DRY_RUN" == "false" ]]; then
                if aws efs delete-file-system --file-system-id "$fs_id" --region "$AWS_REGION" 2>/dev/null; then
                    log "Deleted FileSystem: $fs_id"
                else
                    warn "Failed to delete FileSystem: $fs_id (may have dependencies)"
                fi
            else
                info "DRY RUN: Would delete FileSystem: $fs_id"
            fi
        done
    fi
    
    log "AWS EFS resource cleanup completed"
}

# Cleanup test data and temporary files
cleanup_test_data() {
    log "Cleaning up test data and temporary files..."
    
    # Common test data directories
    local test_dirs=(
        "/tmp/efs-ns-test-*"
        "/tmp/test-efs-ns-*"
        "${HOME}/.efs-ns-test"
        "/var/tmp/efs-test-*"
    )
    
    for pattern in "${test_dirs[@]}"; do
        # Use find to handle glob patterns safely
        if find /tmp -maxdepth 1 -name "$(basename "$pattern")" -type d 2>/dev/null | grep -q .; then
            log "Found test directories matching: $pattern"
            if [[ "$DRY_RUN" == "false" ]]; then
                find /tmp -maxdepth 1 -name "$(basename "$pattern")" -type d -exec rm -rf {} + 2>/dev/null || warn "Failed to clean some test directories"
            else
                info "DRY RUN: Would clean test directories matching: $pattern"
            fi
        fi
    done
    
    # Cleanup test reports older than 7 days
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
    local project_root
    project_root="$(cd "${script_dir}/.." &> /dev/null && pwd)"
    local reports_dir="$project_root/test-reports"
    
    if [[ -d "$reports_dir" ]]; then
        log "Cleaning up old test reports (older than 7 days)..."
        if [[ "$DRY_RUN" == "false" ]]; then
            find "$reports_dir" -type d -mtime +7 -exec rm -rf {} + 2>/dev/null || warn "Failed to clean some old test reports"
        else
            info "DRY RUN: Would clean old test reports"
        fi
    fi
    
    log "Test data cleanup completed"
}

# Wait for resource deletion with timeout
wait_for_deletion() {
    local resource_type="$1"
    local selector="$2"
    local timeout="${3:-300}"
    
    log "Waiting for $resource_type deletion (timeout: ${timeout}s)..."
    
    local elapsed=0
    local interval=10
    
    while [[ $elapsed -lt $timeout ]]; do
        local remaining_resources
        if ! remaining_resources=$(kubectl get "$resource_type" --all-namespaces -l "$selector" -o name 2>/dev/null) || [[ -z "$remaining_resources" ]]; then
            log "$resource_type deletion completed"
            return 0
        fi
        
        log "Still waiting for $resource_type deletion... (${elapsed}s elapsed)"
        sleep $interval
        elapsed=$((elapsed + interval))
    done
    
    warn "$resource_type deletion timed out after ${timeout}s"
    return 1
}

# Force cleanup for stuck resources
force_cleanup_stuck_resources() {
    log "Attempting force cleanup of stuck resources..."
    
    # Remove finalizers from stuck PVCs
    local stuck_pvcs
    if stuck_pvcs=$(kubectl get pvc --all-namespaces -l "efs-ns-test=true" -o json 2>/dev/null | jq -r '.items[] | select(.metadata.finalizers | length > 0) | "\(.metadata.namespace)/\(.metadata.name)"' 2>/dev/null); then
        if [[ -n "$stuck_pvcs" ]]; then
            log "Removing finalizers from stuck PVCs:"
            echo "$stuck_pvcs" | while read -r pvc; do
                local namespace="${pvc%%/*}"
                local name="${pvc##*/}"
                log "  $pvc"
                
                if [[ "$DRY_RUN" == "false" ]]; then
                    kubectl patch pvc "$name" -n "$namespace" -p '{"metadata":{"finalizers":[]}}' --type=merge 2>/dev/null || warn "Failed to patch PVC: $pvc"
                else
                    info "DRY RUN: Would remove finalizers from PVC: $pvc"
                fi
            done
        fi
    fi
    
    # Remove finalizers from stuck PVs
    local stuck_pvs
    if stuck_pvs=$(kubectl get pv -l "efs-ns-test=true" -o json 2>/dev/null | jq -r '.items[] | select(.metadata.finalizers | length > 0) | .metadata.name' 2>/dev/null); then
        if [[ -n "$stuck_pvs" ]]; then
            log "Removing finalizers from stuck PVs:"
            echo "$stuck_pvs" | while read -r pv; do
                log "  $pv"
                
                if [[ "$DRY_RUN" == "false" ]]; then
                    kubectl patch pv "$pv" -p '{"metadata":{"finalizers":[]}}' --type=merge 2>/dev/null || warn "Failed to patch PV: $pv"
                else
                    info "DRY RUN: Would remove finalizers from PV: $pv"
                fi
            done
        fi
    fi
    
    log "Force cleanup completed"
}

# Usage information
usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

EFS Namespace Test Resource Cleanup

OPTIONS:
    --dry-run               Show what would be cleaned without actually cleaning
    --timeout SECONDS       Cleanup timeout in seconds (default: 300)
    --force                 Force cleanup of stuck resources
    --aws-only              Only cleanup AWS resources
    --k8s-only              Only cleanup Kubernetes resources
    -h, --help              Show this help message

ENVIRONMENT VARIABLES:
    AWS_REGION             AWS region (default: us-west-2)
    CLEANUP_TIMEOUT        Cleanup timeout in seconds (default: 300)
    DRY_RUN               Enable dry-run mode (default: false)

EXAMPLES:
    # Dry run to see what would be cleaned
    $0 --dry-run

    # Force cleanup of stuck resources
    $0 --force

    # Cleanup only AWS resources
    $0 --aws-only
EOF
}

# Main execution function
main() {
    local aws_only=false
    local k8s_only=false
    local force_cleanup=false
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --dry-run)
                DRY_RUN="true"
                shift
                ;;
            --timeout)
                CLEANUP_TIMEOUT="$2"
                shift 2
                ;;
            --force)
                force_cleanup=true
                shift
                ;;
            --aws-only)
                aws_only=true
                shift
                ;;
            --k8s-only)
                k8s_only=true
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
    
    log "Starting EFS Namespace test resource cleanup"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        warn "DRY RUN MODE: No resources will be actually deleted"
    fi
    
    # Perform cleanup operations
    if [[ "$aws_only" == "false" ]]; then
        cleanup_kubernetes_resources
        
        if [[ "$force_cleanup" == "true" ]]; then
            force_cleanup_stuck_resources
        fi
    fi
    
    if [[ "$k8s_only" == "false" ]]; then
        cleanup_efs_resources
    fi
    
    cleanup_test_data
    
    log "Cleanup completed successfully"
}

# Execute main function with all arguments
main "$@"