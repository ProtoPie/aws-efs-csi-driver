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
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestNewMetricsServer(t *testing.T) {
	tests := []struct {
		name      string
		config    *MetricsServerConfig
		collector MetricsCollector
	}{
		{
			name:      "with default config and nil collector",
			config:    nil,
			collector: nil,
		},
		{
			name:      "with custom config",
			config:    &MetricsServerConfig{Port: 9090, Address: "127.0.0.1", MetricsPath: "/metrics", Enabled: true},
			collector: NewPrometheusMetricsCollector(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := NewMetricsServer(tt.config, tt.collector)

			if server == nil {
				t.Fatal("NewMetricsServer returned nil")
			}
			if server.config == nil {
				t.Fatal("server.config is nil")
			}
			if server.collector == nil {
				t.Fatal("server.collector is nil")
			}
			if server.registry == nil {
				t.Fatal("server.registry is nil")
			}
			if server.mux == nil {
				t.Fatal("server.mux is nil")
			}
			if server.running {
				t.Error("Expected server.running to be false")
			}
		})
	}
}

func TestMetricsServerStartStop(t *testing.T) {
	config := &MetricsServerConfig{
		Port:        0, // Use ephemeral port for testing
		Address:     "127.0.0.1",
		MetricsPath: "/metrics",
		Enabled:     true,
	}

	collector := NewPrometheusMetricsCollector()
	server := NewMetricsServer(config, collector)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test start
	err := server.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	if !server.IsRunning() {
		t.Error("Expected server to be running")
	}

	// Wait a moment for server to start
	time.Sleep(100 * time.Millisecond)

	// Test basic endpoints
	baseURL := fmt.Sprintf("http://%s", server.GetAddress())

	// Test health endpoint
	resp, err := http.Get(baseURL + "/health")
	if err != nil {
		t.Fatalf("Failed to get health endpoint: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	// Test stop
	err = server.Stop()
	if err != nil {
		t.Fatalf("Failed to stop server: %v", err)
	}
	if server.IsRunning() {
		t.Error("Expected server to not be running after stop")
	}
}

func TestMetricsServerDisabled(t *testing.T) {
	config := &MetricsServerConfig{
		Port:        8080,
		Address:     "127.0.0.1",
		MetricsPath: "/metrics",
		Enabled:     false,
	}

	collector := NewPrometheusMetricsCollector()
	server := NewMetricsServer(config, collector)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test start with disabled config
	err := server.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start disabled server: %v", err)
	}
	if server.IsRunning() {
		t.Error("Server should not be running when disabled")
	}

	// Test stop (should be safe even when not running)
	err = server.Stop()
	if err != nil {
		t.Fatalf("Failed to stop disabled server: %v", err)
	}
}

func TestDefaultMetricsServerConfig(t *testing.T) {
	config := DefaultMetricsServerConfig()

	if config == nil {
		t.Fatal("DefaultMetricsServerConfig returned nil")
	}
	if config.Port != DefaultMetricsPort {
		t.Errorf("Expected port %d, got %d", DefaultMetricsPort, config.Port)
	}
	if config.Address != DefaultMetricsAddr {
		t.Errorf("Expected address %s, got %s", DefaultMetricsAddr, config.Address)
	}
	if config.MetricsPath != DefaultMetricsPath {
		t.Errorf("Expected metrics path %s, got %s", DefaultMetricsPath, config.MetricsPath)
	}
	if !config.Enabled {
		t.Error("Expected config to be enabled")
	}
}

func TestNewMetricsManager(t *testing.T) {
	tests := []struct {
		name   string
		config *MetricsServerConfig
	}{
		{
			name:   "with nil config",
			config: nil,
		},
		{
			name:   "with custom config",
			config: &MetricsServerConfig{Port: 9090, Address: "127.0.0.1", MetricsPath: "/metrics", Enabled: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewMetricsManager(tt.config)

			if manager == nil {
				t.Fatal("NewMetricsManager returned nil")
			}
			if manager.server == nil {
				t.Error("manager.server is nil")
			}
			if manager.collector == nil {
				t.Error("manager.collector is nil")
			}
			if manager.fileSystemCollector == nil {
				t.Error("manager.fileSystemCollector is nil")
			}
			if manager.config == nil {
				t.Error("manager.config is nil")
			}
			if manager.started {
				t.Error("Expected manager.started to be false")
			}
		})
	}
}

func TestMetricsManagerStartStop(t *testing.T) {
	config := &MetricsServerConfig{
		Port:        0,
		Address:     "127.0.0.1",
		MetricsPath: "/metrics",
		Enabled:     true,
	}

	manager := NewMetricsManager(config)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test start
	err := manager.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	if !manager.IsStarted() {
		t.Error("Expected manager to be started")
	}
	if !manager.GetServer().IsRunning() {
		t.Error("Expected server to be running")
	}

	// Test double start (should be safe)
	err = manager.Start(ctx)
	if err != nil {
		t.Fatalf("Double start failed: %v", err)
	}
	if !manager.IsStarted() {
		t.Error("Expected manager to still be started")
	}

	// Test stop
	err = manager.Stop()
	if err != nil {
		t.Fatalf("Failed to stop manager: %v", err)
	}
	if manager.IsStarted() {
		t.Error("Expected manager to not be started")
	}
	if manager.GetServer().IsRunning() {
		t.Error("Expected server to not be running")
	}
}

func TestMetricsManagerWrapCacheWithMetrics(t *testing.T) {
	manager := NewMetricsManager(nil)

	// Create a mock cache
	mockCache := &MockFileSystemCache{}

	// Wrap cache with metrics
	wrappedCache := manager.WrapCacheWithMetrics(mockCache)

	if wrappedCache == nil {
		t.Fatal("WrapCacheWithMetrics returned nil")
	}

	// Verify it's a MetricsAwareCache
	metricsCache, ok := wrappedCache.(*MetricsAwareCache)
	if !ok {
		t.Error("Expected wrapped cache to be MetricsAwareCache")
	}
	if metricsCache != nil && metricsCache.cache != mockCache {
		t.Error("Expected wrapped cache to contain original cache")
	}
}

// MockFileSystemCache for testing
type MockFileSystemCache struct {
	data map[string]*FileSystemInfo
}

func (m *MockFileSystemCache) Get(namespace string) (*FileSystemInfo, bool) {
	if m.data == nil {
		return nil, false
	}
	fsInfo, found := m.data[namespace]
	return fsInfo, found
}

func (m *MockFileSystemCache) Set(namespace string, fsInfo *FileSystemInfo) {
	if m.data == nil {
		m.data = make(map[string]*FileSystemInfo)
	}
	m.data[namespace] = fsInfo
}

func (m *MockFileSystemCache) Delete(namespace string) {
	if m.data != nil {
		delete(m.data, namespace)
	}
}

func (m *MockFileSystemCache) List() map[string]*FileSystemInfo {
	if m.data == nil {
		return make(map[string]*FileSystemInfo)
	}
	result := make(map[string]*FileSystemInfo)
	for k, v := range m.data {
		result[k] = v
	}
	return result
}

func (m *MockFileSystemCache) Refresh(ctx context.Context, namespace string) error {
	return nil
}

func (m *MockFileSystemCache) SetTTL(duration time.Duration) {}

func (m *MockFileSystemCache) Clear() {
	m.data = make(map[string]*FileSystemInfo)
}

func (m *MockFileSystemCache) GetSize() int {
	if m.data == nil {
		return 0
	}
	return len(m.data)
}
