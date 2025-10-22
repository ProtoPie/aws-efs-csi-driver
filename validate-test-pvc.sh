#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}EFS CSI Driver - Test PVC Validation${NC}"
echo -e "${YELLOW}========================================${NC}"
echo ""

# Function to print status
print_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓${NC} $2"
    else
        echo -e "${RED}✗${NC} $2"
    fi
}

FAILED=0

# Check namespace
echo -e "${BLUE}[1] Checking namespace...${NC}"
if kubectl get namespace efs-ns-test &>/dev/null; then
    print_status 0 "Namespace 'efs-ns-test' exists"
else
    print_status 1 "Namespace 'efs-ns-test' not found"
    ((FAILED++))
fi

# Check StorageClass
echo ""
echo -e "${BLUE}[2] Checking StorageClass...${NC}"
if kubectl get storageclass efs-ns-test &>/dev/null; then
    print_status 0 "StorageClass 'efs-ns-test' exists"

    # Check provisioner
    PROVISIONER=$(kubectl get storageclass efs-ns-test -o jsonpath='{.provisioner}')
    if [[ "$PROVISIONER" == "efs.csi.aws.com" ]]; then
        print_status 0 "Provisioner is correct: $PROVISIONER"
    else
        print_status 1 "Provisioner is incorrect: $PROVISIONER"
        ((FAILED++))
    fi
else
    print_status 1 "StorageClass 'efs-ns-test' not found"
    ((FAILED++))
fi

# Check PVC 1
echo ""
echo -e "${BLUE}[3] Checking PVC 'efs-ns-test-1'...${NC}"
if kubectl get pvc efs-ns-test-1 -n efs-ns-test &>/dev/null; then
    print_status 0 "PVC 'efs-ns-test-1' exists"

    PVC1_STATUS=$(kubectl get pvc efs-ns-test-1 -n efs-ns-test -o jsonpath='{.status.phase}')
    if [[ "$PVC1_STATUS" == "Bound" ]]; then
        print_status 0 "PVC 'efs-ns-test-1' is Bound"
        PV1=$(kubectl get pvc efs-ns-test-1 -n efs-ns-test -o jsonpath='{.spec.volumeName}')
        echo -e "  ${GREEN}→${NC} Bound to PV: $PV1"
    else
        print_status 1 "PVC 'efs-ns-test-1' status: $PVC1_STATUS (expected: Bound)"
        ((FAILED++))
    fi
else
    print_status 1 "PVC 'efs-ns-test-1' not found"
    ((FAILED++))
fi

# Check PVC 2
echo ""
echo -e "${BLUE}[4] Checking PVC 'efs-ns-test-2'...${NC}"
if kubectl get pvc efs-ns-test-2 -n efs-ns-test &>/dev/null; then
    print_status 0 "PVC 'efs-ns-test-2' exists"

    PVC2_STATUS=$(kubectl get pvc efs-ns-test-2 -n efs-ns-test -o jsonpath='{.status.phase}')
    if [[ "$PVC2_STATUS" == "Bound" ]]; then
        print_status 0 "PVC 'efs-ns-test-2' is Bound"
        PV2=$(kubectl get pvc efs-ns-test-2 -n efs-ns-test -o jsonpath='{.spec.volumeName}')
        echo -e "  ${GREEN}→${NC} Bound to PV: $PV2"
    else
        print_status 1 "PVC 'efs-ns-test-2' status: $PVC2_STATUS (expected: Bound)"
        ((FAILED++))
    fi
else
    print_status 1 "PVC 'efs-ns-test-2' not found"
    ((FAILED++))
fi

# Check Pod
echo ""
echo -e "${BLUE}[5] Checking Pod 'efs-ns-test'...${NC}"
if kubectl get pod efs-ns-test -n efs-ns-test &>/dev/null; then
    print_status 0 "Pod 'efs-ns-test' exists"

    POD_STATUS=$(kubectl get pod efs-ns-test -n efs-ns-test -o jsonpath='{.status.phase}')
    if [[ "$POD_STATUS" == "Running" ]]; then
        print_status 0 "Pod 'efs-ns-test' is Running"
    else
        print_status 1 "Pod 'efs-ns-test' status: $POD_STATUS (expected: Running)"
        ((FAILED++))
    fi

    # Check Pod readiness
    POD_READY=$(kubectl get pod efs-ns-test -n efs-ns-test -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')
    if [[ "$POD_READY" == "True" ]]; then
        print_status 0 "Pod 'efs-ns-test' is Ready"
    else
        print_status 1 "Pod 'efs-ns-test' is not Ready"
        ((FAILED++))
    fi
else
    print_status 1 "Pod 'efs-ns-test' not found"
    ((FAILED++))
fi

# Test file operations
echo ""
echo -e "${BLUE}[6] Testing file operations...${NC}"
if kubectl exec -n efs-ns-test efs-ns-test -- sh -c "echo 'test-write' > /mnt/efs-1/test.txt" &>/dev/null; then
    print_status 0 "Write test to /mnt/efs-1 succeeded"

    if kubectl exec -n efs-ns-test efs-ns-test -- sh -c "cat /mnt/efs-1/test.txt" &>/dev/null; then
        CONTENT=$(kubectl exec -n efs-ns-test efs-ns-test -- sh -c "cat /mnt/efs-1/test.txt" 2>/dev/null)
        if [[ "$CONTENT" == "test-write" ]]; then
            print_status 0 "Read test from /mnt/efs-1 succeeded"
        else
            print_status 1 "Read test returned unexpected content: $CONTENT"
            ((FAILED++))
        fi
    else
        print_status 1 "Read test from /mnt/efs-1 failed"
        ((FAILED++))
    fi

    # Clean up test file
    kubectl exec -n efs-ns-test efs-ns-test -- sh -c "rm /mnt/efs-1/test.txt" &>/dev/null
else
    print_status 1 "Write test to /mnt/efs-1 failed"
    ((FAILED++))
fi

# Test second mount point
if kubectl exec -n efs-ns-test efs-ns-test -- sh -c "echo 'test-write-2' > /mnt/efs-2/test2.txt" &>/dev/null; then
    print_status 0 "Write test to /mnt/efs-2 succeeded"

    if kubectl exec -n efs-ns-test efs-ns-test -- sh -c "cat /mnt/efs-2/test2.txt" &>/dev/null; then
        CONTENT2=$(kubectl exec -n efs-ns-test efs-ns-test -- sh -c "cat /mnt/efs-2/test2.txt" 2>/dev/null)
        if [[ "$CONTENT2" == "test-write-2" ]]; then
            print_status 0 "Read test from /mnt/efs-2 succeeded"
        else
            print_status 1 "Read test returned unexpected content: $CONTENT2"
            ((FAILED++))
        fi
    else
        print_status 1 "Read test from /mnt/efs-2 failed"
        ((FAILED++))
    fi

    # Clean up test file
    kubectl exec -n efs-ns-test efs-ns-test -- sh -c "rm /mnt/efs-2/test2.txt" &>/dev/null
else
    print_status 1 "Write test to /mnt/efs-2 failed"
    ((FAILED++))
fi

# Check EFS CSI driver pods
echo ""
echo -e "${BLUE}[7] Checking EFS CSI Driver...${NC}"
DRIVER_PODS=$(kubectl get pods -n kube-system -l app=efs-csi-controller --no-headers 2>/dev/null | wc -l | tr -d ' ')
if [[ "$DRIVER_PODS" -gt 0 ]]; then
    print_status 0 "EFS CSI controller pods found: $DRIVER_PODS"
else
    print_status 1 "No EFS CSI controller pods found"
    ((FAILED++))
fi

NODE_PODS=$(kubectl get pods -n kube-system -l app=efs-csi-node --no-headers 2>/dev/null | wc -l | tr -d ' ')
if [[ "$NODE_PODS" -gt 0 ]]; then
    print_status 0 "EFS CSI node pods found: $NODE_PODS"
else
    print_status 1 "No EFS CSI node pods found"
    ((FAILED++))
fi

# Summary
echo ""
echo -e "${YELLOW}========================================${NC}"
if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All validation checks passed!${NC}"
    echo ""
    echo -e "${GREEN}Test PVC is working correctly.${NC}"
    exit 0
else
    echo -e "${RED}✗ $FAILED validation check(s) failed${NC}"
    echo ""
    echo "For more details, run:"
    echo "  kubectl describe pvc -n efs-ns-test"
    echo "  kubectl describe pod efs-ns-test -n efs-ns-test"
    echo "  kubectl logs -n efs-ns-test efs-ns-test"
    exit 1
fi
