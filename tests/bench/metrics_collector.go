package bench

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/transferia/iceberg"
)

// MetricsSample holds a single point-in-time measurement.
type MetricsSample struct {
	Timestamp       time.Time
	Elapsed         time.Duration
	PGRowsWritten   int64
	PGInserts       int64
	PGUpdates       int64
	PGDeletes       int64
	IcebergRows     uint64
	LagRows         int64
	CurrentRate     int64 // rows/sec since last sample
}

// BenchmarkResult holds the final benchmark summary.
type BenchmarkResult struct {
	Profile       string
	Duration      time.Duration
	PGRowsWritten int64
	PGInserts     int64
	PGUpdates     int64
	PGDeletes     int64
	IcebergRows   uint64
	LagRows       int64
	PeakLagRows   int64
	AvgLagRows    int64
	PeakRate      int64
	AvgRate       int64
	Samples       []MetricsSample
}

func (r *BenchmarkResult) String() string {
	var sb strings.Builder
	sb.WriteString("\n=== Benchmark Results ===\n")
	sb.WriteString(fmt.Sprintf("Profile:           %s\n", r.Profile))
	sb.WriteString(fmt.Sprintf("Duration:          %s\n", r.Duration.Truncate(time.Second)))
	sb.WriteString(fmt.Sprintf("PG rows written:   %d (I:%d U:%d D:%d)\n", r.PGRowsWritten, r.PGInserts, r.PGUpdates, r.PGDeletes))
	sb.WriteString(fmt.Sprintf("Iceberg rows:      %d\n", r.IcebergRows))
	sb.WriteString(fmt.Sprintf("Replication lag:   %d rows (peak: %d, avg: %d)\n", r.LagRows, r.PeakLagRows, r.AvgLagRows))
	sb.WriteString(fmt.Sprintf("Peak write rate:   %d rows/sec\n", r.PeakRate))
	sb.WriteString(fmt.Sprintf("Avg write rate:    %d rows/sec\n", r.AvgRate))
	sb.WriteString("========================\n")
	return sb.String()
}

// MetricsCollector periodically samples replication metrics.
type MetricsCollector struct {
	target    *iceberg.Destination
	schema    string
	table     string
	generator *LoadGenerator
	interval  time.Duration

	mu      sync.Mutex
	samples []MetricsSample
}

// NewMetricsCollector creates a metrics collector.
func NewMetricsCollector(target *iceberg.Destination, schema, table string, generator *LoadGenerator, interval time.Duration) *MetricsCollector {
	return &MetricsCollector{
		target:    target,
		schema:    schema,
		table:     table,
		generator: generator,
		interval:  interval,
	}
}

// Run starts periodic metrics collection. Blocks until ctx is cancelled.
func (m *MetricsCollector) Run(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	start := time.Now()
	var lastTotal int64

	for {
		select {
		case <-ticker.C:
			sample := m.collect(start, &lastTotal)
			m.mu.Lock()
			m.samples = append(m.samples, sample)
			m.mu.Unlock()

			fmt.Printf("[%s] PG:%d  Iceberg:%d  Lag:%d  Rate:%d/s\n",
				sample.Elapsed.Truncate(time.Second),
				sample.PGRowsWritten,
				sample.IcebergRows,
				sample.LagRows,
				sample.CurrentRate,
			)
		case <-ctx.Done():
			// Final sample
			sample := m.collect(start, &lastTotal)
			m.mu.Lock()
			m.samples = append(m.samples, sample)
			m.mu.Unlock()
			return
		}
	}
}

func (m *MetricsCollector) collect(start time.Time, lastTotal *int64) MetricsSample {
	total, inserts, updates, deletes := m.generator.Stats()

	icebergRows := uint64(0)
	rows, err := iceberg.DestinationRowCount(m.target, m.schema, m.table)
	if err == nil {
		icebergRows = rows
	}

	rate := total - *lastTotal
	ratePerSec := rate * int64(time.Second) / int64(m.interval)
	*lastTotal = total

	// Lag: for insert-only, PG total == expected iceberg rows.
	// For mixed DML, lag is approximate since deletes reduce iceberg count.
	lag := total - int64(icebergRows)
	if lag < 0 {
		lag = 0
	}

	return MetricsSample{
		Timestamp:     time.Now(),
		Elapsed:       time.Since(start),
		PGRowsWritten: total,
		PGInserts:     inserts,
		PGUpdates:     updates,
		PGDeletes:     deletes,
		IcebergRows:   icebergRows,
		LagRows:       lag,
		CurrentRate:   ratePerSec,
	}
}

// Result builds the final benchmark result.
func (m *MetricsCollector) Result(profile string, duration time.Duration) *BenchmarkResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	total, inserts, updates, deletes := m.generator.Stats()

	icebergRows := uint64(0)
	rows, err := iceberg.DestinationRowCount(m.target, m.schema, m.table)
	if err == nil {
		icebergRows = rows
	}

	var peakRate, peakLag, totalLag int64
	for _, s := range m.samples {
		if s.CurrentRate > peakRate {
			peakRate = s.CurrentRate
		}
		if s.LagRows > peakLag {
			peakLag = s.LagRows
		}
		totalLag += s.LagRows
	}

	avgRate := int64(0)
	if duration.Seconds() > 0 {
		avgRate = total / int64(duration.Seconds())
	}

	avgLag := int64(0)
	if len(m.samples) > 0 {
		avgLag = totalLag / int64(len(m.samples))
	}

	lag := total - int64(icebergRows)
	if lag < 0 {
		lag = 0
	}

	return &BenchmarkResult{
		Profile:       profile,
		Duration:      duration,
		PGRowsWritten: total,
		PGInserts:     inserts,
		PGUpdates:     updates,
		PGDeletes:     deletes,
		IcebergRows:   icebergRows,
		LagRows:       lag,
		PeakLagRows:   peakLag,
		AvgLagRows:    avgLag,
		PeakRate:      peakRate,
		AvgRate:       avgRate,
		Samples:       m.samples,
	}
}
