#!/bin/bash
set -euo pipefail

# Configuration
IMAGE_NAME="aws-efs-csi-driver"

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

echo "✅ Build complete!"
echo "📦 Image built: ${IMAGE_NAME}:${TAG}"
echo ""
echo "To push to ECR after logging in:"
echo "  1. aws sso login --profile xid-dev"
echo "  2. ./build-and-update.sh"
