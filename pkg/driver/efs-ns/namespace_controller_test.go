/*
Copyright 2024 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package efsns

import (
	"testing"
)

func TestNamespaceControllerMetrics_Creation(t *testing.T) {
	metrics := NewNamespaceControllerMetrics()
	if metrics == nil {
		t.Errorf("expected metrics instance, got nil")
	}
}

func TestDefaultNamespaceControllerConfig(t *testing.T) {
	config := DefaultNamespaceControllerConfig()
	if config == nil {
		t.Errorf("expected config instance, got nil")
	}

	if config.MaxRetries != MaxRetries {
		t.Errorf("expected MaxRetries %d, got %d", MaxRetries, config.MaxRetries)
	}

	if config.CleanupTimeout != CleanupTimeout {
		t.Errorf("expected CleanupTimeout %v, got %v", CleanupTimeout, config.CleanupTimeout)
	}

	if !config.EnableMetrics {
		t.Errorf("expected EnableMetrics to be true")
	}
}
