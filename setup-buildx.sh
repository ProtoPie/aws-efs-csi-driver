#!/bin/bash
set -euo pipefail

echo "🔧 Setting up Docker buildx for cross-platform builds"

# Create a new builder instance if it doesn't exist
if ! docker buildx ls | grep -q "efs-builder"; then
  echo "📦 Creating new buildx builder: efs-builder"
  docker buildx create --name efs-builder --driver docker-container --use
else
  echo "✅ Using existing builder: efs-builder"
  docker buildx use efs-builder
fi

# Bootstrap the builder
echo "🚀 Bootstrapping builder"
docker buildx inspect --bootstrap

echo "📋 Available platforms:"
docker buildx ls

echo "✅ Buildx setup complete!"
echo ""
echo "You can now run ./build-and-update.sh to build and deploy"