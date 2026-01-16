// Package server provides PostgreSQL wire protocol compatibility for dukdb-go.
// This file implements OpenTelemetry tracing and Prometheus metrics integration.
package server

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ObservabilityConfig configures observability features.
type ObservabilityConfig struct {
	// EnableTracing enables OpenTelemetry tracing.
	EnableTracing bool

	// EnableMetrics enables Prometheus metrics.
	EnableMetrics bool

	// MetricsEndpoint is the HTTP endpoint for Prometheus metrics.
	// Default is "/metrics".
	MetricsEndpoint string

	// ServiceName is the service name for tracing.
	ServiceName string

	// ServiceVersion is the service version for tracing.
	ServiceVersion string

	// TraceExporter is the trace exporter type (e.g., "otlp", "jaeger", "stdout").
	TraceExporter string

	// TraceEndpoint is the endpoint for trace export (for OTLP).
	TraceEndpoint string

	// SampleRate is the trace sampling rate (0.0 to 1.0).
	SampleRate float64
}

// DefaultObservabilityConfig returns the default observability configuration.
func DefaultObservabilityConfig() *ObservabilityConfig {
	return &ObservabilityConfig{
		EnableTracing:   false,
		EnableMetrics:   false,
		MetricsEndpoint: "/metrics",
		ServiceName:     "dukdb-postgres",
		ServiceVersion:  "1.0.0",
		TraceExporter:   "stdout",
		SampleRate:      1.0,
	}
}

// Tracer provides tracing functionality.
// This is a minimal interface that can be implemented with OpenTelemetry.
type Tracer interface {
	// StartSpan starts a new tracing span.
	StartSpan(ctx context.Context, name string, opts ...SpanOption) (context.Context, Span)
}

// Span represents a tracing span.
type Span interface {
	// End ends the span.
	End()

	// SetStatus sets the span status.
	SetStatus(code SpanStatusCode, description string)

	// SetAttribute sets an attribute on the span.
	SetAttribute(key string, value any)

	// RecordError records an error on the span.
	RecordError(err error)
}

// SpanOption is an option for creating a span.
type SpanOption func(*SpanConfig)

// SpanConfig holds span configuration.
type SpanConfig struct {
	Attributes map[string]any
	Kind       SpanKind
}

// SpanKind represents the type of span.
type SpanKind int

const (
	SpanKindInternal SpanKind = iota
	SpanKindServer
	SpanKindClient
	SpanKindProducer
	SpanKindConsumer
)

// SpanStatusCode represents the status of a span.
type SpanStatusCode int

const (
	SpanStatusUnset SpanStatusCode = iota
	SpanStatusOK
	SpanStatusError
)

// WithSpanKind sets the span kind.
func WithSpanKind(kind SpanKind) SpanOption {
	return func(c *SpanConfig) {
		c.Kind = kind
	}
}

// WithAttributes sets span attributes.
func WithAttributes(attrs map[string]any) SpanOption {
	return func(c *SpanConfig) {
		c.Attributes = attrs
	}
}

// NoopTracer is a no-op tracer implementation.
type NoopTracer struct{}

// NewNoopTracer creates a new no-op tracer.
func NewNoopTracer() *NoopTracer {
	return &NoopTracer{}
}

// StartSpan starts a no-op span.
func (t *NoopTracer) StartSpan(ctx context.Context, name string, _ ...SpanOption) (context.Context, Span) {
	return ctx, &NoopSpan{}
}

// NoopSpan is a no-op span implementation.
type NoopSpan struct{}

func (s *NoopSpan) End()                                        {}
func (s *NoopSpan) SetStatus(_ SpanStatusCode, _ string)        {}
func (s *NoopSpan) SetAttribute(_ string, _ any)                {}
func (s *NoopSpan) RecordError(_ error)                         {}

// QueryTracer wraps query execution with tracing.
type QueryTracer struct {
	tracer Tracer
}

// NewQueryTracer creates a new query tracer.
func NewQueryTracer(tracer Tracer) *QueryTracer {
	if tracer == nil {
		tracer = NewNoopTracer()
	}
	return &QueryTracer{tracer: tracer}
}

// TraceQuery wraps a query execution with a tracing span.
func (qt *QueryTracer) TraceQuery(ctx context.Context, query string, fn func(context.Context) error) error {
	ctx, span := qt.tracer.StartSpan(ctx, "pg.query", WithSpanKind(SpanKindServer))
	defer span.End()

	// Set query attributes
	span.SetAttribute("db.system", "dukdb")
	span.SetAttribute("db.statement", truncateQuery(query, 500))
	span.SetAttribute("db.operation", getOperationType(query))

	err := fn(ctx)
	if err != nil {
		span.SetStatus(SpanStatusError, err.Error())
		span.RecordError(err)
	} else {
		span.SetStatus(SpanStatusOK, "")
	}

	return err
}

// getOperationType extracts the operation type from a query.
func getOperationType(query string) string {
	upper := strings.ToUpper(strings.TrimSpace(query))
	if len(upper) == 0 {
		return "UNKNOWN"
	}

	// Find the first word
	space := strings.IndexByte(upper, ' ')
	if space == -1 {
		return upper
	}
	return upper[:space]
}

// PrometheusMetrics exports metrics in Prometheus format.
type PrometheusMetrics struct {
	mu        sync.RWMutex
	collector *MetricsCollector
	server    *Server
}

// NewPrometheusMetrics creates a new Prometheus metrics exporter.
func NewPrometheusMetrics(collector *MetricsCollector, server *Server) *PrometheusMetrics {
	return &PrometheusMetrics{
		collector: collector,
		server:    server,
	}
}

// ServeHTTP handles Prometheus metrics scraping.
func (pm *PrometheusMetrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	var sb strings.Builder

	// Write metrics header
	sb.WriteString("# HELP dukdb_queries_total Total number of queries executed\n")
	sb.WriteString("# TYPE dukdb_queries_total counter\n")

	if pm.collector != nil {
		stats := pm.collector.GetGlobalStats()

		// Query counters
		writeMetric(&sb, "dukdb_queries_total", stats.TotalQueries, nil)

		sb.WriteString("\n# HELP dukdb_query_errors_total Total number of query errors\n")
		sb.WriteString("# TYPE dukdb_query_errors_total counter\n")
		writeMetric(&sb, "dukdb_query_errors_total", stats.TotalErrors, nil)

		sb.WriteString("\n# HELP dukdb_rows_returned_total Total number of rows returned\n")
		sb.WriteString("# TYPE dukdb_rows_returned_total counter\n")
		writeMetric(&sb, "dukdb_rows_returned_total", stats.TotalRowsReturned, nil)

		sb.WriteString("\n# HELP dukdb_rows_affected_total Total number of rows affected\n")
		sb.WriteString("# TYPE dukdb_rows_affected_total counter\n")
		writeMetric(&sb, "dukdb_rows_affected_total", stats.TotalRowsAffected, nil)

		// Uptime
		sb.WriteString("\n# HELP dukdb_uptime_seconds Server uptime in seconds\n")
		sb.WriteString("# TYPE dukdb_uptime_seconds gauge\n")
		writeMetricFloat(&sb, "dukdb_uptime_seconds", stats.Uptime.Seconds(), nil)

		// Query duration histogram
		sb.WriteString("\n# HELP dukdb_query_duration_ms_bucket Query duration histogram\n")
		sb.WriteString("# TYPE dukdb_query_duration_ms_bucket histogram\n")
		bucketLabels := []string{"1", "5", "10", "50", "100", "500", "1000", "5000", "+Inf"}
		var cumulative int64
		for i, count := range stats.Histogram {
			cumulative += count
			writeMetric(&sb, "dukdb_query_duration_ms_bucket", cumulative, map[string]string{"le": bucketLabels[i]})
		}
		writeMetric(&sb, "dukdb_query_duration_ms_count", stats.TotalQueries, nil)

		// Error counts by SQLSTATE
		errorCounts := pm.collector.GetErrorCounts()
		if len(errorCounts) > 0 {
			sb.WriteString("\n# HELP dukdb_query_errors_by_state Query errors by SQLSTATE\n")
			sb.WriteString("# TYPE dukdb_query_errors_by_state counter\n")
			for sqlState, count := range errorCounts {
				writeMetric(&sb, "dukdb_query_errors_by_state", count, map[string]string{"sqlstate": sqlState})
			}
		}
	}

	// Connection metrics
	if pm.server != nil {
		connStats := pm.server.GetConnectionStats()

		sb.WriteString("\n# HELP dukdb_connections_active Current number of active connections\n")
		sb.WriteString("# TYPE dukdb_connections_active gauge\n")
		writeMetric(&sb, "dukdb_connections_active", connStats.ActiveConnections, nil)

		sb.WriteString("\n# HELP dukdb_connections_idle Current number of idle connections\n")
		sb.WriteString("# TYPE dukdb_connections_idle gauge\n")
		writeMetric(&sb, "dukdb_connections_idle", connStats.IdleConnections, nil)

		sb.WriteString("\n# HELP dukdb_connections_total Total connections established\n")
		sb.WriteString("# TYPE dukdb_connections_total counter\n")
		writeMetric(&sb, "dukdb_connections_total", connStats.TotalConnections, nil)

		sb.WriteString("\n# HELP dukdb_connections_rejected Total connections rejected\n")
		sb.WriteString("# TYPE dukdb_connections_rejected counter\n")
		writeMetric(&sb, "dukdb_connections_rejected", connStats.RejectedConnections, nil)

		sb.WriteString("\n# HELP dukdb_connections_queued Current connections in queue\n")
		sb.WriteString("# TYPE dukdb_connections_queued gauge\n")
		writeMetric(&sb, "dukdb_connections_queued", connStats.QueuedConnections, nil)

		sb.WriteString("\n# HELP dukdb_connections_max_reached Peak concurrent connections\n")
		sb.WriteString("# TYPE dukdb_connections_max_reached gauge\n")
		writeMetric(&sb, "dukdb_connections_max_reached", connStats.MaxConnectionsReached, nil)
	}

	_, _ = w.Write([]byte(sb.String()))
}

// writeMetric writes a Prometheus metric line.
func writeMetric(sb *strings.Builder, name string, value int64, labels map[string]string) {
	sb.WriteString(name)
	if len(labels) > 0 {
		sb.WriteByte('{')
		first := true
		for k, v := range labels {
			if !first {
				sb.WriteByte(',')
			}
			sb.WriteString(k)
			sb.WriteString("=\"")
			sb.WriteString(v)
			sb.WriteByte('"')
			first = false
		}
		sb.WriteByte('}')
	}
	sb.WriteByte(' ')
	sb.WriteString(itoa64(value))
	sb.WriteByte('\n')
}

// writeMetricFloat writes a Prometheus metric line with a float value.
func writeMetricFloat(sb *strings.Builder, name string, value float64, labels map[string]string) {
	sb.WriteString(name)
	if len(labels) > 0 {
		sb.WriteByte('{')
		first := true
		for k, v := range labels {
			if !first {
				sb.WriteByte(',')
			}
			sb.WriteString(k)
			sb.WriteString("=\"")
			sb.WriteString(v)
			sb.WriteByte('"')
			first = false
		}
		sb.WriteByte('}')
	}
	sb.WriteByte(' ')
	sb.WriteString(formatFloat(value))
	sb.WriteByte('\n')
}

// formatFloat formats a float64 to string without importing fmt.
func formatFloat(f float64) string {
	// Simple formatting for positive numbers
	if f < 0 {
		return "-" + formatFloat(-f)
	}
	if f == 0 {
		return "0"
	}

	// Integer part
	intPart := int64(f)
	fracPart := f - float64(intPart)

	result := itoa64(intPart)

	// Add decimal places (up to 6)
	if fracPart > 0 {
		result += "."
		for i := 0; i < 6 && fracPart > 0.000001; i++ {
			fracPart *= 10
			digit := int64(fracPart)
			result += string(byte('0' + digit))
			fracPart -= float64(digit)
		}
	}

	return result
}

// MetricsHandler is an HTTP handler that serves the Prometheus metrics endpoint.
type MetricsHandler struct {
	prometheus *PrometheusMetrics
}

// NewMetricsHandler creates a new metrics HTTP handler.
func NewMetricsHandler(collector *MetricsCollector, server *Server) *MetricsHandler {
	return &MetricsHandler{
		prometheus: NewPrometheusMetrics(collector, server),
	}
}

// ServeHTTP handles HTTP requests for metrics.
func (mh *MetricsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	mh.prometheus.ServeHTTP(w, r)
}

// ObservabilityManager manages all observability features.
type ObservabilityManager struct {
	mu sync.RWMutex

	config    *ObservabilityConfig
	collector *MetricsCollector
	tracer    Tracer
	server    *Server

	// metricsServer is the HTTP server for Prometheus metrics.
	metricsServer *http.Server
}

// NewObservabilityManager creates a new observability manager.
func NewObservabilityManager(config *ObservabilityConfig, server *Server) *ObservabilityManager {
	if config == nil {
		config = DefaultObservabilityConfig()
	}

	om := &ObservabilityManager{
		config:    config,
		collector: NewMetricsCollector(),
		tracer:    NewNoopTracer(),
		server:    server,
	}

	return om
}

// MetricsCollector returns the metrics collector.
func (om *ObservabilityManager) MetricsCollector() *MetricsCollector {
	om.mu.RLock()
	defer om.mu.RUnlock()
	return om.collector
}

// Tracer returns the tracer.
func (om *ObservabilityManager) Tracer() Tracer {
	om.mu.RLock()
	defer om.mu.RUnlock()
	return om.tracer
}

// SetTracer sets a custom tracer (e.g., OpenTelemetry tracer).
func (om *ObservabilityManager) SetTracer(tracer Tracer) {
	om.mu.Lock()
	defer om.mu.Unlock()
	om.tracer = tracer
}

// StartMetricsServer starts the Prometheus metrics HTTP server.
func (om *ObservabilityManager) StartMetricsServer(addr string) error {
	om.mu.Lock()
	defer om.mu.Unlock()

	if om.metricsServer != nil {
		return nil // Already running
	}

	mux := http.NewServeMux()
	mux.Handle(om.config.MetricsEndpoint, NewMetricsHandler(om.collector, om.server))

	om.metricsServer = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		if err := om.metricsServer.ListenAndServe(); err != http.ErrServerClosed {
			// Log error if needed
		}
	}()

	return nil
}

// StopMetricsServer stops the Prometheus metrics HTTP server.
func (om *ObservabilityManager) StopMetricsServer(ctx context.Context) error {
	om.mu.Lock()
	defer om.mu.Unlock()

	if om.metricsServer == nil {
		return nil
	}

	err := om.metricsServer.Shutdown(ctx)
	om.metricsServer = nil
	return err
}

// Close cleans up all observability resources.
func (om *ObservabilityManager) Close(ctx context.Context) error {
	return om.StopMetricsServer(ctx)
}
