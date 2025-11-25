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
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewHealthCheckManager(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
	}{
		{
			name:    "with custom timeout",
			timeout: 5 * time.Second,
		},
		{
			name:    "with zero timeout (should use default)",
			timeout: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewHealthCheckManager(tt.timeout)

			if manager == nil {
				t.Fatal("NewHealthCheckManager returned nil")
			}
			if manager.healthCheckers == nil {
				t.Error("healthCheckers is nil")
			}
			if manager.readinessCheckers == nil {
				t.Error("readinessCheckers is nil")
			}

			expectedTimeout := tt.timeout
			if expectedTimeout == 0 {
				expectedTimeout = DefaultHealthCheckTimeout
			}

			if manager.timeout != expectedTimeout {
				t.Errorf("Expected timeout %v, got %v", expectedTimeout, manager.timeout)
			}
		})
	}
}

func TestHealthCheckManagerAddCheckers(t *testing.T) {
	manager := NewHealthCheckManager(5 * time.Second)

	// Create mock checkers
	healthChecker := &MockHealthChecker{name: "test-health", healthy: true}
	readinessChecker := &MockReadinessChecker{name: "test-readiness", ready: true}

	// Test adding health checker
	manager.AddHealthChecker(healthChecker)
	checkers := manager.GetHealthCheckers()
	if len(checkers) != 1 {
		t.Errorf("Expected 1 health checker, got %d", len(checkers))
	}
	if checkers[0] != healthChecker {
		t.Error("Health checker not added correctly")
	}

	// Test adding readiness checker
	manager.AddReadinessChecker(readinessChecker)
	readinessCheckers := manager.GetReadinessCheckers()
	if len(readinessCheckers) != 1 {
		t.Errorf("Expected 1 readiness checker, got %d", len(readinessCheckers))
	}
	if readinessCheckers[0] != readinessChecker {
		t.Error("Readiness checker not added correctly")
	}

	// Test adding nil checkers (should not panic)
	manager.AddHealthChecker(nil)
	manager.AddReadinessChecker(nil)

	// Should not add nil checkers
	if len(manager.GetHealthCheckers()) != 1 {
		t.Error("Nil health checker was added")
	}
	if len(manager.GetReadinessCheckers()) != 1 {
		t.Error("Nil readiness checker was added")
	}
}

func TestHealthCheckManagerCheckHealth(t *testing.T) {
	tests := []struct {
		name            string
		checkers        []HealthChecker
		expectedHealthy bool
		expectedError   bool
	}{
		{
			name:            "no checkers - should be healthy",
			checkers:        []HealthChecker{},
			expectedHealthy: true,
			expectedError:   false,
		},
		{
			name: "all checkers healthy",
			checkers: []HealthChecker{
				&MockHealthChecker{name: "checker1", healthy: true},
				&MockHealthChecker{name: "checker2", healthy: true},
			},
			expectedHealthy: true,
			expectedError:   false,
		},
		{
			name: "one checker unhealthy",
			checkers: []HealthChecker{
				&MockHealthChecker{name: "checker1", healthy: true},
				&MockHealthChecker{name: "checker2", healthy: false, err: errors.New("unhealthy")},
			},
			expectedHealthy: false,
			expectedError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewHealthCheckManager(5 * time.Second)

			for _, checker := range tt.checkers {
				manager.AddHealthChecker(checker)
			}

			ctx := context.Background()
			healthy, err := manager.CheckHealth(ctx)

			if healthy != tt.expectedHealthy {
				t.Errorf("Expected healthy %v, got %v", tt.expectedHealthy, healthy)
			}

			if tt.expectedError && err == nil {
				t.Error("Expected error but got none")
			} else if !tt.expectedError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

func TestHealthCheckManagerCheckReadiness(t *testing.T) {
	tests := []struct {
		name          string
		checkers      []ReadinessChecker
		expectedReady bool
	}{
		{
			name:          "no checkers - should be ready",
			checkers:      []ReadinessChecker{},
			expectedReady: true,
		},
		{
			name: "all checkers ready",
			checkers: []ReadinessChecker{
				&MockReadinessChecker{name: "checker1", ready: true},
				&MockReadinessChecker{name: "checker2", ready: true},
			},
			expectedReady: true,
		},
		{
			name: "one checker not ready",
			checkers: []ReadinessChecker{
				&MockReadinessChecker{name: "checker1", ready: true},
				&MockReadinessChecker{name: "checker2", ready: false},
			},
			expectedReady: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewHealthCheckManager(5 * time.Second)

			for _, checker := range tt.checkers {
				manager.AddReadinessChecker(checker)
			}

			ctx := context.Background()
			ready := manager.CheckReadiness(ctx)

			if ready != tt.expectedReady {
				t.Errorf("Expected ready %v, got %v", tt.expectedReady, ready)
			}
		})
	}
}

// Mock implementations for testing

type MockHealthChecker struct {
	name    string
	healthy bool
	err     error
	delay   time.Duration
}

func (m *MockHealthChecker) Name() string {
	return m.name
}

func (m *MockHealthChecker) Check(ctx context.Context) error {
	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if !m.healthy {
		if m.err != nil {
			return m.err
		}
		return errors.New("unhealthy")
	}
	return nil
}

type MockReadinessChecker struct {
	name  string
	ready bool
}

func (m *MockReadinessChecker) Name() string {
	return m.name
}

func (m *MockReadinessChecker) IsReady(ctx context.Context) bool {
	return m.ready
}

// Simple unit tests to improve coverage for health check constructors and basic methods

func TestHealthCheckConstructors(t *testing.T) {
	// These tests verify the constructor functions are accessible and create objects

	// Test that health check manager creation sets defaults
	manager := NewHealthCheckManager(0)
	if manager.timeout != DefaultHealthCheckTimeout {
		t.Errorf("Expected default timeout %v, got %v", DefaultHealthCheckTimeout, manager.timeout)
	}

	// Test non-default timeout
	customTimeout := 30 * time.Second
	manager2 := NewHealthCheckManager(customTimeout)
	if manager2.timeout != customTimeout {
		t.Errorf("Expected custom timeout %v, got %v", customTimeout, manager2.timeout)
	}
}
