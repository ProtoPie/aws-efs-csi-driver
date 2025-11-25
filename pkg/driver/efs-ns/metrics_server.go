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
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/klog/v2"
)

const (
	// Default metrics server configuration
	DefaultMetricsPort = 8080
	DefaultMetricsPath = "/metrics"
	DefaultMetricsAddr = "0.0.0.0"

	// Server timeouts
	DefaultReadTimeout  = 30 * time.Second
	DefaultWriteTimeout = 30 * time.Second
	DefaultIdleTimeout  = 60 * time.Second
)

// MetricsServerConfig holds configuration for the metrics server
type MetricsServerConfig struct {
	// Port to listen on
	Port int

	// Address to bind to
	Address string

	// Path for metrics endpoint
	MetricsPath string

	// HTTP server timeouts
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration

	// Whether to enable metrics collection
	Enabled bool
}

// DefaultMetricsServerConfig returns default configuration for metrics server
func DefaultMetricsServerConfig() *MetricsServerConfig {
	return &MetricsServerConfig{
		Port:         DefaultMetricsPort,
		Address:      DefaultMetricsAddr,
		MetricsPath:  DefaultMetricsPath,
		ReadTimeout:  DefaultReadTimeout,
		WriteTimeout: DefaultWriteTimeout,
		IdleTimeout:  DefaultIdleTimeout,
		Enabled:      true,
	}
}

// MetricsServer provides HTTP endpoint for Prometheus metrics
type MetricsServer struct {
	config    *MetricsServerConfig
	server    *http.Server
	collector MetricsCollector
	registry  *prometheus.Registry
	mux       *http.ServeMux
	mutex     sync.RWMutex
	running   bool
}

// NewMetricsServer creates a new metrics server
func NewMetricsServer(config *MetricsServerConfig, collector MetricsCollector) *MetricsServer {
	if config == nil {
		config = DefaultMetricsServerConfig()
	}

	if collector == nil {
		collector = NewPrometheusMetricsCollector()
	}

	registry := prometheus.NewRegistry()
	mux := http.NewServeMux()

	server := &MetricsServer{
		config:    config,
		collector: collector,
		registry:  registry,
		mux:       mux,
	}

	server.setupRoutes()
	return server
}

// setupRoutes configures HTTP routes for the metrics server
func (s *MetricsServer) setupRoutes() {
	// Metrics endpoint
	s.mux.Handle(s.config.MetricsPath, promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{
		Registry:          s.registry,
		EnableOpenMetrics: true,
		ErrorHandling:     promhttp.ContinueOnError,
		ErrorLog:          &prometheusErrorLogger{},
	}))

	// Health check endpoint
	s.mux.HandleFunc("/health", s.healthHandler)
	s.mux.HandleFunc("/healthz", s.healthHandler)

	// Ready check endpoint
	s.mux.HandleFunc("/ready", s.readyHandler)
	s.mux.HandleFunc("/readyz", s.readyHandler)

	// Root handler with basic info
	s.mux.HandleFunc("/", s.rootHandler)
}

// Start starts the metrics server
func (s *MetricsServer) Start(ctx context.Context) error {
	if !s.config.Enabled {
		klog.V(4).Info("EFS-NS metrics server disabled")
		return nil
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.running {
		return NewEFSNSError(ErrMetricsRegistration, "MetricsServer.Start", "",
			"metrics server is already running", nil)
	}

	// Register metrics with the registry
	if err := s.collector.Register(s.registry); err != nil {
		return NewEFSNSError(ErrMetricsRegistration, "MetricsServer.Start", "",
			"failed to register metrics", err)
	}

	// Create HTTP server
	addr := fmt.Sprintf("%s:%d", s.config.Address, s.config.Port)
	s.server = &http.Server{
		Addr:         addr,
		Handler:      s.mux,
		ReadTimeout:  s.config.ReadTimeout,
		WriteTimeout: s.config.WriteTimeout,
		IdleTimeout:  s.config.IdleTimeout,
	}

	// Start server in background
	go func() {
		klog.Infof("EFS-NS metrics server starting on %s%s", addr, s.config.MetricsPath)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.Errorf("EFS-NS metrics server error: %v", err)
		}
	}()

	s.running = true

	// Wait for shutdown signal
	go func() {
		<-ctx.Done()
		s.Stop()
	}()

	klog.Infof("EFS-NS metrics server started successfully on %s%s", addr, s.config.MetricsPath)
	return nil
}

// Stop stops the metrics server
func (s *MetricsServer) Stop() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if !s.running {
		return nil
	}

	var stopErr error

	// Shutdown HTTP server
	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := s.server.Shutdown(ctx); err != nil {
			stopErr = NewEFSNSError(ErrMetricsCollection, "MetricsServer.Stop", "",
				"failed to shutdown HTTP server", err)
			klog.Errorf("EFS-NS metrics server shutdown error: %v", err)
		}
	}

	// Unregister metrics
	if s.collector != nil && s.registry != nil {
		if !s.collector.Unregister(s.registry) {
			klog.Warning("Failed to unregister some EFS-NS metrics")
		}
	}

	s.running = false
	klog.Info("EFS-NS metrics server stopped")
	return stopErr
}

// GetCollector returns the metrics collector
func (s *MetricsServer) GetCollector() MetricsCollector {
	return s.collector
}

// IsRunning returns whether the server is running
func (s *MetricsServer) IsRunning() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.running
}

// GetAddress returns the server address
func (s *MetricsServer) GetAddress() string {
	if s.config == nil {
		return ""
	}
	return fmt.Sprintf("%s:%d", s.config.Address, s.config.Port)
}

// GetMetricsURL returns the full metrics endpoint URL
func (s *MetricsServer) GetMetricsURL() string {
	return fmt.Sprintf("http://%s%s", s.GetAddress(), s.config.MetricsPath)
}

// HTTP handlers

// healthHandler handles health check requests
func (s *MetricsServer) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK\n"))
}

// readyHandler handles readiness check requests
func (s *MetricsServer) readyHandler(w http.ResponseWriter, r *http.Request) {
	s.mutex.RLock()
	running := s.running
	s.mutex.RUnlock()

	w.Header().Set("Content-Type", "text/plain")
	if running {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Ready\n"))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("Not Ready\n"))
	}
}

// rootHandler handles requests to the root path
func (s *MetricsServer) rootHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)

	html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head>
    <title>EFS-NS Metrics Server</title>
</head>
<body>
    <h1>EFS-NS Metrics Server</h1>
    <p>This is the EFS-NS metrics server for AWS EFS CSI Driver.</p>
    <ul>
        <li><a href="%s">Metrics</a> - Prometheus metrics endpoint</li>
        <li><a href="/health">Health</a> - Health check endpoint</li>
        <li><a href="/ready">Ready</a> - Readiness check endpoint</li>
    </ul>
    <p>Server Status: %s</p>
</body>
</html>`, s.config.MetricsPath, s.getStatusText())

	w.Write([]byte(html))
}

// getStatusText returns the current status as text
func (s *MetricsServer) getStatusText() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.running {
		return "Running"
	}
	return "Stopped"
}

// prometheusErrorLogger implements prometheus error logging
type prometheusErrorLogger struct{}

func (l *prometheusErrorLogger) Println(v ...interface{}) {
	args := make([]interface{}, 0, len(v)+1)
	args = append(args, "Prometheus metrics error:")
	args = append(args, v...)
	klog.Error(args...)
}

// MetricsManager manages the lifecycle of metrics collection for EFS-NS
type MetricsManager struct {
	server              *MetricsServer
	collector           MetricsCollector
	fileSystemCollector *FileSystemMetricsCollector
	config              *MetricsServerConfig
	mutex               sync.RWMutex
	started             bool
}

// NewMetricsManager creates a new metrics manager
func NewMetricsManager(config *MetricsServerConfig) *MetricsManager {
	if config == nil {
		config = DefaultMetricsServerConfig()
	}

	collector := NewPrometheusMetricsCollector()
	server := NewMetricsServer(config, collector)
	fileSystemCollector := NewFileSystemMetricsCollector(collector)

	return &MetricsManager{
		server:              server,
		collector:           collector,
		fileSystemCollector: fileSystemCollector,
		config:              config,
	}
}

// Start starts the metrics manager
func (m *MetricsManager) Start(ctx context.Context) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.started {
		return nil
	}

	if err := m.server.Start(ctx); err != nil {
		return err
	}

	m.started = true
	klog.V(4).Info("EFS-NS metrics manager started")
	return nil
}

// Stop stops the metrics manager
func (m *MetricsManager) Stop() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if !m.started {
		return nil
	}

	if err := m.server.Stop(); err != nil {
		return err
	}

	m.started = false
	klog.V(4).Info("EFS-NS metrics manager stopped")
	return nil
}

// GetCollector returns the metrics collector
func (m *MetricsManager) GetCollector() MetricsCollector {
	return m.collector
}

// GetFileSystemCollector returns the filesystem metrics collector
func (m *MetricsManager) GetFileSystemCollector() *FileSystemMetricsCollector {
	return m.fileSystemCollector
}

// GetServer returns the metrics server
func (m *MetricsManager) GetServer() *MetricsServer {
	return m.server
}

// IsStarted returns whether the manager is started
func (m *MetricsManager) IsStarted() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.started
}

// WrapCacheWithMetrics wraps a FileSystemCache with metrics collection
func (m *MetricsManager) WrapCacheWithMetrics(cache FileSystemCache) FileSystemCache {
	return NewMetricsAwareCache(cache, m.collector, "filesystem_cache")
}

// Global metrics manager instance
var (
	globalMetricsManager *MetricsManager
	metricsManagerOnce   sync.Once
)

// GetGlobalMetricsManager returns the global metrics manager instance
func GetGlobalMetricsManager() *MetricsManager {
	metricsManagerOnce.Do(func() {
		globalMetricsManager = NewMetricsManager(DefaultMetricsServerConfig())
	})
	return globalMetricsManager
}

// SetGlobalMetricsManager sets a custom global metrics manager (mainly for testing)
func SetGlobalMetricsManager(manager *MetricsManager) {
	globalMetricsManager = manager
}
