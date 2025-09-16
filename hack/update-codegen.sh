#!/usr/bin/env bash

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

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(dirname "${BASH_SOURCE[0]}")/..
MODULE_NAME="github.com/kubernetes-sigs/aws-efs-csi-driver"

# Generate deepcopy functions
echo "Generating deepcopy functions..."
go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.14.0 \
    object:headerFile="${SCRIPT_ROOT}/hack/boilerplate.go.txt" \
    paths="${SCRIPT_ROOT}/pkg/apis/efs/v1alpha1"

echo "DeepCopy generation complete!"

echo "Note: For production use, you would also generate clientset, informers, and listers using:"
echo "  - k8s.io/code-generator/cmd/client-gen"
echo "  - k8s.io/code-generator/cmd/lister-gen"
echo "  - k8s.io/code-generator/cmd/informer-gen"
echo ""
echo "However, for this implementation we'll manually create a simplified client wrapper"
echo "that uses the standard Kubernetes dynamic client for CRD operations."