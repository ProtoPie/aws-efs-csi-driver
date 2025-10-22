#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}EFS CSI Driver - Test PVC Deployment${NC}"
echo -e "${YELLOW}========================================${NC}"
echo ""

# Check kubectl context
CURRENT_CONTEXT=$(kubectl config current-context)
echo -e "${GREEN}✓${NC} Current kubectl context: ${CURRENT_CONTEXT}"

if [[ "${CURRENT_CONTEXT}" != *"efs-csi-test-cluster"* ]]; then
    echo -e "${RED}✗${NC} Warning: You are not connected to efs-csi-test-cluster"
    read -p "Do you want to continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo ""
echo -e "${YELLOW}[1/5] Deploying test resources...${NC}"
kubectl apply -f test-pvc.yaml

echo ""
echo -e "${YELLOW}[2/5] Waiting for namespace to be ready...${NC}"
kubectl wait --for=jsonpath='{.status.phase}'=Active namespace/efs-ns-test --timeout=30s

echo ""
echo -e "${YELLOW}[3/5] Waiting for PVCs to be bound (timeout: 120s)...${NC}"
kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/efs-ns-test-1 -n efs-ns-test --timeout=120s || true
kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/efs-ns-test-2 -n efs-ns-test --timeout=120s || true

echo ""
echo -e "${YELLOW}[4/5] Waiting for Pod to be ready (timeout: 120s)...${NC}"
kubectl wait --for=condition=Ready pod/efs-ns-test -n efs-ns-test --timeout=120s || true

echo ""
echo -e "${YELLOW}[5/5] Deployment Status Check${NC}"
echo -e "${YELLOW}========================================${NC}"

# Check PVC status
echo ""
echo -e "${GREEN}PVC Status:${NC}"
kubectl get pvc -n efs-ns-test

# Check PV status
echo ""
echo -e "${GREEN}PV Status:${NC}"
kubectl get pv | grep efs-ns-test || echo "No PVs found"

# Check Pod status
echo ""
echo -e "${GREEN}Pod Status:${NC}"
kubectl get pod -n efs-ns-test

# Describe PVCs
echo ""
echo -e "${GREEN}PVC Details:${NC}"
kubectl describe pvc -n efs-ns-test | grep -E "Name:|Status:|Volume:|StorageClass:|Events:" || true

# Check Pod events
echo ""
echo -e "${GREEN}Pod Events:${NC}"
kubectl describe pod efs-ns-test -n efs-ns-test | grep -A 10 "Events:" || true

echo ""
echo -e "${YELLOW}========================================${NC}"
echo -e "${GREEN}Deployment complete!${NC}"
echo ""
echo "To check Pod logs, run:"
echo "  kubectl logs -n efs-ns-test efs-ns-test -f"
echo ""
echo "To test file operations, run:"
echo "  kubectl exec -it -n efs-ns-test efs-ns-test -- sh"
echo "  # Inside the pod:"
echo "  # echo 'test' > /mnt/efs-1/test.txt"
echo "  # cat /mnt/efs-1/test.txt"
echo ""
echo "To clean up resources, run:"
echo "  kubectl delete -f test-pvc.yaml"
echo ""
