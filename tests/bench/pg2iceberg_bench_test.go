package bench

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/transferia/iceberg"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

const (
	benchSchema = "public"
	benchTable  = "bench_orders"
)

// TestBenchmarkSmoke is a quick 20-second smoke test to verify benchmark infrastructure works.
//
//	CATALOG_ENDPOINT=... SOURCE_PG_LOCAL_PORT=5432 go test -run TestBenchmarkSmoke -timeout=5m -v ./tests/bench/
func TestBenchmarkSmoke(t *testing.T) {
	skipIfNoInfra(t)

	cfg := LoadGeneratorConfig{
		Profile:      ProfileInsertOnly,
		RateStart:    100,
		RateEnd:      500,
		RampDuration: 10 * time.Second,
		HoldDuration: 10 * time.Second,
		Workers:      4,
	}

	result := runBenchmark(t, cfg)
	t.Log(result.String())
	require.True(t, result.PGRowsWritten > 0, "expected PG rows written")
}

// TestBenchmarkInsertOnly runs the INSERT-only profile (pure append throughput, no equality deletes).
// Ramps from 1K to 10K rows/sec over 5 minutes.
//
//	go test -run TestBenchmarkInsertOnly -timeout=30m -v ./tests/bench/
func TestBenchmarkInsertOnly(t *testing.T) {
	skipIfNoInfra(t)
	result := runBenchmark(t, DefaultConfig(ProfileInsertOnly))
	t.Log(result.String())
}

// TestBenchmarkInsertHeavy runs the INSERT-heavy profile (90% INSERT, 5% UPDATE, 5% DELETE).
// Tests equality delete overhead under light CDC load.
//
//	go test -run TestBenchmarkInsertHeavy -timeout=30m -v ./tests/bench/
func TestBenchmarkInsertHeavy(t *testing.T) {
	skipIfNoInfra(t)
	result := runBenchmark(t, DefaultConfig(ProfileInsertHeavy))
	t.Log(result.String())
}

// TestBenchmarkBalanced runs the balanced OLTP profile (60% INSERT, 30% UPDATE, 10% DELETE).
// Tests sustained equality delete + RowDelta commit throughput.
//
//	go test -run TestBenchmarkBalanced -timeout=30m -v ./tests/bench/
func TestBenchmarkBalanced(t *testing.T) {
	skipIfNoInfra(t)
	result := runBenchmark(t, DefaultConfig(ProfileBalanced))
	t.Log(result.String())
}

// TestBenchmarkAll runs all profiles sequentially with results printed at the end.
//
//	go test -run TestBenchmarkAll -timeout=60m -v ./tests/bench/
func TestBenchmarkAll(t *testing.T) {
	skipIfNoInfra(t)

	profiles := []LoadProfile{ProfileInsertOnly, ProfileInsertHeavy, ProfileBalanced}
	results := make([]*BenchmarkResult, 0, len(profiles))

	for _, p := range profiles {
		t.Run(p.Name, func(t *testing.T) {
			result := runBenchmark(t, DefaultConfig(p))
			results = append(results, result)
			t.Log(result.String())
		})
	}

	t.Log("\n=== All Benchmark Results ===")
	for _, r := range results {
		t.Log(r.String())
	}
}

func runBenchmark(t *testing.T, cfg LoadGeneratorConfig) *BenchmarkResult {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Setup PG connection pool ---
	pool, err := pgxpool.Connect(ctx, pgConnectionString())
	require.NoError(t, err)
	defer pool.Close()

	// --- Setup Iceberg destination ---
	target, err := iceberg.DestinationRecipe()
	require.NoError(t, err)
	target.CommitInterval = 5 * time.Second

	// --- Setup PG source for transfer ---
	source := pgrecipe.RecipeSource()
	source.Database = pgDatabase()

	// --- Init load generator + create table ---
	gen := NewLoadGenerator(cfg, pool)
	require.NoError(t, gen.InitTable(ctx))

	// --- Start transfer (snapshot + replication) ---
	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeSnapshotAndIncrement)
	transfer.TypeSystemVersion = model.LatestVersion

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// Wait for transfer to initialize
	time.Sleep(3 * time.Second)

	// --- Start metrics collector ---
	collector := NewMetricsCollector(target, benchSchema, benchTable, gen, 10*time.Second)
	metricsCtx, metricsCancel := context.WithCancel(ctx)
	go collector.Run(metricsCtx)

	// --- Run load generator ---
	startTime := time.Now()
	t.Logf("Starting %s benchmark: ramp %d→%d rows/sec over %s, hold %s",
		cfg.Profile.Name, cfg.RateStart, cfg.RateEnd, cfg.RampDuration, cfg.HoldDuration)

	err = gen.Run(ctx)
	require.NoError(t, err)

	duration := time.Since(startTime)
	t.Logf("Load generation complete after %s, waiting for final replication flush...",
		duration.Truncate(time.Second))

	// Wait for replication to catch up
	time.Sleep(30 * time.Second)
	metricsCancel()

	return collector.Result(cfg.Profile.Name, duration)
}

func skipIfNoInfra(t *testing.T) {
	t.Helper()
	if os.Getenv("CATALOG_ENDPOINT") == "" {
		t.Skip("CATALOG_ENDPOINT not set; start infra with 'make recipe' first")
	}
}

func pgConnectionString() string {
	host := envOrDefault("SOURCE_PG_LOCAL_HOST", "localhost")
	port := envOrDefault("SOURCE_PG_LOCAL_PORT", "5432")
	user := envOrDefault("SOURCE_PG_LOCAL_USER", "postgres")
	pass := envOrDefault("SOURCE_PG_LOCAL_PASSWORD", "postgres")
	db := pgDatabase()
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, pass, db)
}

func pgDatabase() string {
	return envOrDefault("SOURCE_PG_LOCAL_DATABASE", "bench")
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
