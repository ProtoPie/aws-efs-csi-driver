#!/bin/bash
set -euo pipefail

# Configuration
ECR_REGISTRY="310455165573.dkr.ecr.us-west-2.amazonaws.com"
IMAGE_NAME="aws-efs-csi-driver"
AWS_PROFILE="xid-dev"
AWS_REGION="us-west-2"
AWS_ACCOUNT_ID="310455165573"
NAMESPACE="kube-system"
RELEASE_NAME="aws-efs-csi-driver"

# IRSA (IAM Roles for Service Accounts) Configuration
CONTROLLER_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/EFSCSIController"
NODE_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/EFSCSINode"

# Parse arguments
PLATFORM="${1:-linux/arm64}"

# Generate unique tag based on git commit SHA
GIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo "no-git")
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
TAG="${GIT_SHA}-${TIMESTAMP}"

echo "🔧 Ensuring buildx builder is ready"
# Ensure we're using the right builder
if ! docker buildx ls | grep -q "efs-builder"; then
  echo "⚠️  Builder not found. Setting up buildx..."
  docker buildx rm efs-builder 2>/dev/null || true
  ./setup-buildx.sh
fi
docker buildx use efs-builder

echo "🔨 Building Docker image for platform: $PLATFORM"
echo "   Tag: ${TAG}"
# Use buildx for cross-platform builds
docker buildx build \
  --platform="$PLATFORM" \
  --load \
  -t "${IMAGE_NAME}:${TAG}" \
  --build-arg TARGETOS=linux \
  --build-arg TARGETARCH=arm64 \
  .

# Get the local image ID and SHA256 digest
LOCAL_IMAGE_ID=$(docker images --no-trunc --quiet "${IMAGE_NAME}:${TAG}" | head -1)
echo "📋 Local Image ID: ${LOCAL_IMAGE_ID}"

echo "🏷️  Tagging image for ECR"
docker tag "${IMAGE_NAME}:${TAG}" "${ECR_REGISTRY}/${IMAGE_NAME}:${TAG}"
docker tag "${IMAGE_NAME}:${TAG}" "${ECR_REGISTRY}/${IMAGE_NAME}:latest"

echo "🔐 Logging into ECR"
aws ecr get-login-password --region "${AWS_REGION}" --profile "${AWS_PROFILE}" | \
  docker login --username AWS --password-stdin "${ECR_REGISTRY}"

echo "📤 Pushing image to ECR"
docker push "${ECR_REGISTRY}/${IMAGE_NAME}:${TAG}"
docker push "${ECR_REGISTRY}/${IMAGE_NAME}:latest"

# Get the image digest for more reliable deployment
IMAGE_DIGEST=$(docker inspect --format='{{index .RepoDigests 0}}' "${ECR_REGISTRY}/${IMAGE_NAME}:${TAG}" | cut -d'@' -f2)
echo "📝 Image digest: ${IMAGE_DIGEST}"

# Extract just the hash part after sha256:
IMAGE_HASH=$(echo "${IMAGE_DIGEST}" | cut -d':' -f2)
echo "📝 Image hash: ${IMAGE_HASH}"

echo "🚀 Deploying to Kubernetes using Helm"
echo "   Using IRSA roles:"
echo "   - Controller: ${CONTROLLER_ROLE_ARN}"
echo "   - Node: ${NODE_ROLE_ARN}"
echo "   Image: ${ECR_REGISTRY}/${IMAGE_NAME}@${IMAGE_DIGEST}"

# Use digest for deployment to ensure exact image version
# Pass just the hash part as the tag
helm upgrade --install "${RELEASE_NAME}" ./charts/aws-efs-csi-driver \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  -f efs-values.yaml \
  --set image.repository="${ECR_REGISTRY}/${IMAGE_NAME}@sha256" \
  --set image.tag="${IMAGE_HASH}" \
  --set image.pullPolicy=IfNotPresent \
  --set controller.serviceAccount.create=true \
  --set controller.serviceAccount.annotations."eks\.amazonaws\.com/role-arn"="${CONTROLLER_ROLE_ARN}" \
  --set node.serviceAccount.create=true \
  --set node.serviceAccount.annotations."eks\.amazonaws\.com/role-arn"="${NODE_ROLE_ARN}" \
  --set controller.efsNamespaceProvisioning.enabled=true \
  --wait --timeout 5m

echo "✅ Deployment complete!"
echo ""
echo "📌 Deployed version:"
echo "   Tag: ${TAG}"
echo "   Digest: ${IMAGE_DIGEST}"
echo ""
echo "🔍 Check deployment status:"
echo "  kubectl get pods -n ${NAMESPACE} -l app.kubernetes.io/name=aws-efs-csi-driver"
echo ""
echo "📊 View Helm release:"
echo "  helm status ${RELEASE_NAME} -n ${NAMESPACE}"
echo ""
echo "🔐 Verify IRSA configuration:"
echo "  kubectl get sa -n ${NAMESPACE} efs-csi-controller-sa -o jsonpath='{.metadata.annotations.eks\.amazonaws\.com/role-arn}'"
echo "  kubectl get sa -n ${NAMESPACE} efs-csi-node-sa -o jsonpath='{.metadata.annotations.eks\.amazonaws\.com/role-arn}'"
echo ""
echo "💡 To rollback to a previous version:"
echo "  helm rollback ${RELEASE_NAME} -n ${NAMESPACE}"
