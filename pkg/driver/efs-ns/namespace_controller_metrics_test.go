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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestNewNamespaceControllerMetrics(t *testing.T) {
	metrics := NewNamespaceControllerMetrics()

	if metrics == nil {
		t.Errorf("expected metrics instance, got nil")
		return
	}

	if metrics.registry == nil {
		t.Errorf("expected registry to be initialized")
	}

	if metrics.started {
		t.Errorf("expected metrics to not be started initially")
	}
}

func TestNamespaceControllerMetrics_Start(t *testing.T) {
	metrics := NewNamespaceControllerMetrics()

	err := metrics.Start()
	if err != nil {
		t.Errorf("unexpected error starting metrics: %v", err)
	}

	if !metrics.started {
		t.Errorf("expected metrics to be started")
	}

	err = metrics.Start()
	if err != nil {
		t.Errorf("starting already started metrics should not error: %v", err)
	}

	metrics.Stop()
}

func TestNamespaceControllerMetrics_Stop(t *testing.T) {
	metrics := NewNamespaceControllerMetrics()

	metrics.Stop()

	if metrics.started {
		t.Errorf("expected metrics to not be started after stop")
	}

	err := metrics.Start()
	if err != nil {
		t.Fatalf("unexpected error starting metrics: %v", err)
	}

	metrics.Stop()

	if metrics.started {
		t.Errorf("expected metrics to be stopped")
	}
}

func TestNamespaceControllerMetrics_IncNamespaceDeletionAttempts(t *testing.T) {
	metrics := NewNamespaceControllerMetrics()

	err := metrics.Start()
	if err != nil {
		t.Fatalf("unexpected error starting metrics: %v", err)
	}
	defer metrics.Stop()

	testNamespace := "test-namespace"

	metrics.IncNamespaceDeletionAttempts(testNamespace)
	metrics.IncNamespaceDeletionAttempts(testNamespace)

	value := getCounterValue(t, metrics.namespaceDeletionAttempts, testNamespace)
	if value != 2 {
		t.Errorf("expected counter value 2, got %f", value)
	}
}

func TestNamespaceControllerMetrics_IncSuccessfulNamespaceCleanups(t *testing.T) {
	metrics := NewNamespaceControllerMetrics()

	err := metrics.Start()
	if err != nil {
		t.Fatalf("unexpected error starting metrics: %v", err)
	}
	defer metrics.Stop()

	testNamespace := "test-namespace"

	metrics.IncSuccessfulNamespaceCleanups(testNamespace)

	value := getCounterValue(t, metrics.successfulNamespaceCleanups, testNamespace)
	if value != 1 {
		t.Errorf("expected counter value 1, got %f", value)
	}
}

func TestNamespaceControllerMetrics_IncNamespaceCleanupErrors(t *testing.T) {
	metrics := NewNamespaceControllerMetrics()

	err := metrics.Start()
	if err != nil {
		t.Fatalf("unexpected error starting metrics: %v", err)
	}
	defer metrics.Stop()

	testNamespace := "test-namespace"
	testErrorType := "filesystem_deletion_failed"

	metrics.IncNamespaceCleanupErrors(testNamespace, testErrorType)

	value := getCounterValueWithLabels(t, metrics.namespaceCleanupErrors, testNamespace, testErrorType)
	if value != 1 {
		t.Errorf("expected counter value 1, got %f", value)
	}
}

func TestNamespaceControllerMetrics_IncNamespaceWatcherRestarts(t *testing.T) {
	metrics := NewNamespaceControllerMetrics()

	err := metrics.Start()
	if err != nil {
		t.Fatalf("unexpected error starting metrics: %v", err)
	}
	defer metrics.Stop()

	metrics.IncNamespaceWatcherRestarts()
	metrics.IncNamespaceWatcherRestarts()

	value := getCounterValueSimple(t, metrics.namespaceWatcherRestarts)
	if value != 2 {
		t.Errorf("expected counter value 2, got %f", value)
	}
}

func TestNamespaceControllerMetrics_ObserveNamespaceCleanupDuration(t *testing.T) {
	metrics := NewNamespaceControllerMetrics()

	err := metrics.Start()
	if err != nil {
		t.Fatalf("unexpected error starting metrics: %v", err)
	}
	defer metrics.Stop()

	testNamespace := "test-namespace"
	testDuration := 5 * time.Second

	metrics.ObserveNamespaceCleanupDuration(testNamespace, testDuration)

	count := getHistogramCount(t, metrics.namespaceCleanupDuration, testNamespace)
	if count != 1 {
		t.Errorf("expected histogram count 1, got %d", count)
	}
}

func TestNamespaceControllerMetrics_SetActiveNamespaceCleanups(t *testing.T) {
	metrics := NewNamespaceControllerMetrics()

	err := metrics.Start()
	if err != nil {
		t.Fatalf("unexpected error starting metrics: %v", err)
	}
	defer metrics.Stop()

	testValue := float64(5)

	metrics.SetActiveNamespaceCleanups(testValue)

	value := getGaugeValue(t, metrics.activeNamespaceCleanups)
	if value != testValue {
		t.Errorf("expected gauge value %f, got %f", testValue, value)
	}
}

func TestNamespaceControllerMetrics_SetNamespaceWatcherStatus(t *testing.T) {
	metrics := NewNamespaceControllerMetrics()

	err := metrics.Start()
	if err != nil {
		t.Fatalf("unexpected error starting metrics: %v", err)
	}
	defer metrics.Stop()

	metrics.SetNamespaceWatcherStatus(true)
	value := getGaugeValue(t, metrics.namespaceWatcherStatus)
	if value != 1 {
		t.Errorf("expected gauge value 1 for true, got %f", value)
	}

	metrics.SetNamespaceWatcherStatus(false)
	value = getGaugeValue(t, metrics.namespaceWatcherStatus)
	if value != 0 {
		t.Errorf("expected gauge value 0 for false, got %f", value)
	}
}

func TestNamespaceControllerMetrics_GetMetricsSnapshot(t *testing.T) {
	metrics := NewNamespaceControllerMetrics()

	snapshot := metrics.GetMetricsSnapshot()
	if snapshot != nil {
		t.Errorf("expected nil snapshot when not started")
	}

	err := metrics.Start()
	if err != nil {
		t.Fatalf("unexpected error starting metrics: %v", err)
	}
	defer metrics.Stop()

	testNamespace := "test-namespace"
	metrics.IncNamespaceDeletionAttempts(testNamespace)
	metrics.SetActiveNamespaceCleanups(3)

	snapshot = metrics.GetMetricsSnapshot()
	if snapshot == nil {
		t.Errorf("expected snapshot when started")
		return
	}

	if len(snapshot) == 0 {
		t.Errorf("expected non-empty snapshot")
	}
}

func TestNamespaceControllerMetrics_Registry(t *testing.T) {
	metrics := NewNamespaceControllerMetrics()

	registry := metrics.Registry()
	if registry == nil {
		t.Errorf("expected registry, got nil")
	}

	if registry != metrics.registry {
		t.Errorf("expected same registry instance")
	}
}

func TestNamespaceControllerMetrics_NotStarted(t *testing.T) {
	metrics := NewNamespaceControllerMetrics()

	testNamespace := "test-namespace"

	metrics.IncNamespaceDeletionAttempts(testNamespace)
	metrics.IncSuccessfulNamespaceCleanups(testNamespace)
	metrics.IncNamespaceCleanupErrors(testNamespace, "error")
	metrics.IncNamespaceWatcherRestarts()
	metrics.ObserveNamespaceCleanupDuration(testNamespace, time.Second)
	metrics.SetActiveNamespaceCleanups(5)
	metrics.SetNamespaceWatcherStatus(true)

	err := metrics.Start()
	if err != nil {
		t.Fatalf("unexpected error starting metrics: %v", err)
	}
	defer metrics.Stop()

	value := getCounterValue(t, metrics.namespaceDeletionAttempts, testNamespace)
	if value != 0 {
		t.Errorf("expected counter value 0 when operations performed before start, got %f", value)
	}
}

func getCounterValue(t *testing.T, counter *prometheus.CounterVec, namespace string) float64 {
	metric := &dto.Metric{}
	err := counter.WithLabelValues(namespace).Write(metric)
	if err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	return metric.GetCounter().GetValue()
}

func getCounterValueWithLabels(t *testing.T, counter *prometheus.CounterVec, namespace, errorType string) float64 {
	metric := &dto.Metric{}
	err := counter.WithLabelValues(namespace, errorType).Write(metric)
	if err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	return metric.GetCounter().GetValue()
}

func getCounterValueSimple(t *testing.T, counter prometheus.Counter) float64 {
	metric := &dto.Metric{}
	err := counter.Write(metric)
	if err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	return metric.GetCounter().GetValue()
}

func getHistogramCount(t *testing.T, histogram *prometheus.HistogramVec, namespace string) uint64 {
	metricDTO := &dto.Metric{}
	observer := histogram.WithLabelValues(namespace)
	if h, ok := observer.(prometheus.Histogram); ok {
		err := h.Write(metricDTO)
		if err != nil {
			t.Fatalf("failed to write metric: %v", err)
		}
		return metricDTO.GetHistogram().GetSampleCount()
	}
	t.Fatalf("could not cast observer to histogram")
	return 0
}

func getGaugeValue(t *testing.T, gauge prometheus.Gauge) float64 {
	metric := &dto.Metric{}
	err := gauge.Write(metric)
	if err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	return metric.GetGauge().GetValue()
}
