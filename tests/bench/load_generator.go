package bench

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

// LoadProfile defines the DML mix for a benchmark run.
type LoadProfile struct {
	Name      string
	InsertPct int // percentage 0-100
	UpdatePct int
	DeletePct int
}

var (
	ProfileInsertOnly = LoadProfile{Name: "InsertOnly", InsertPct: 100, UpdatePct: 0, DeletePct: 0}
	ProfileInsertHeavy = LoadProfile{Name: "InsertHeavy", InsertPct: 90, UpdatePct: 5, DeletePct: 5}
	ProfileBalanced    = LoadProfile{Name: "Balanced", InsertPct: 60, UpdatePct: 30, DeletePct: 10}
)

// LoadGeneratorConfig configures the benchmark load generator.
type LoadGeneratorConfig struct {
	Profile      LoadProfile
	RateStart    int           // initial rows/sec
	RateEnd      int           // peak rows/sec
	RampDuration time.Duration // time to ramp from start to end
	HoldDuration time.Duration // time to hold at peak after ramp
	Workers      int           // concurrent writer goroutines
}

// DefaultConfig returns a standard benchmark configuration.
func DefaultConfig(profile LoadProfile) LoadGeneratorConfig {
	return LoadGeneratorConfig{
		Profile:      profile,
		RateStart:    1000,
		RateEnd:      10000,
		RampDuration: 150 * time.Second, // 2.5 min ramp
		HoldDuration: 150 * time.Second, // 2.5 min hold
		Workers:      8,
	}
}

// LoadGenerator generates sustained DML load on a Postgres table.
type LoadGenerator struct {
	cfg     LoadGeneratorConfig
	pool    *pgxpool.Pool
	rng     *rand.Rand

	// Tracking
	totalOps   atomic.Int64
	insertOps  atomic.Int64
	updateOps  atomic.Int64
	deleteOps  atomic.Int64

	// ID pool for UPDATE/DELETE targets
	mu         sync.Mutex
	insertedIDs []int64
	nextID     atomic.Int64
}

// NewLoadGenerator creates a load generator connected to the given PG pool.
func NewLoadGenerator(cfg LoadGeneratorConfig, pool *pgxpool.Pool) *LoadGenerator {
	return &LoadGenerator{
		cfg:  cfg,
		pool: pool,
		rng:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// InitTable creates the benchmark table.
func (g *LoadGenerator) InitTable(ctx context.Context) error {
	_, err := g.pool.Exec(ctx, `
		DROP TABLE IF EXISTS bench_orders;
		CREATE TABLE bench_orders (
			id         BIGSERIAL PRIMARY KEY,
			user_id    BIGINT NOT NULL,
			status     VARCHAR(32) NOT NULL,
			amount     NUMERIC(12,2),
			created_at TIMESTAMP DEFAULT now(),
			updated_at TIMESTAMP DEFAULT now()
		);
	`)
	return err
}

// Stats returns current operation counts.
func (g *LoadGenerator) Stats() (total, inserts, updates, deletes int64) {
	return g.totalOps.Load(), g.insertOps.Load(), g.updateOps.Load(), g.deleteOps.Load()
}

// Run starts the load generator and blocks until duration expires or ctx is cancelled.
func (g *LoadGenerator) Run(ctx context.Context) error {
	totalDuration := g.cfg.RampDuration + g.cfg.HoldDuration
	deadline := time.Now().Add(totalDuration)

	// Work channel distributes ops across workers
	workCh := make(chan struct{}, g.cfg.Workers*100)

	// Start workers
	var wg sync.WaitGroup
	errCh := make(chan error, g.cfg.Workers)
	for range g.cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case _, ok := <-workCh:
					if !ok {
						return
					}
					if err := g.doOperation(ctx); err != nil {
						select {
						case errCh <- err:
						default:
						}
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Rate controller: ramp from RateStart to RateEnd, then hold
	startTime := time.Now()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	var tokenAccum float64

	for {
		select {
		case <-ticker.C:
			if time.Now().After(deadline) {
				close(workCh)
				wg.Wait()
				return g.drainErrors(errCh)
			}

			elapsed := time.Since(startTime)
			currentRate := g.currentRate(elapsed)

			// Accumulate fractional tokens (rate is per-second, tick is per-ms)
			tokenAccum += float64(currentRate) / 1000.0
			for tokenAccum >= 1.0 {
				select {
				case workCh <- struct{}{}:
				default:
					// backpressure: workers can't keep up
				}
				tokenAccum -= 1.0
			}
		case <-ctx.Done():
			close(workCh)
			wg.Wait()
			return ctx.Err()
		}
	}
}

func (g *LoadGenerator) currentRate(elapsed time.Duration) int {
	if elapsed >= g.cfg.RampDuration {
		return g.cfg.RateEnd
	}
	progress := float64(elapsed) / float64(g.cfg.RampDuration)
	return g.cfg.RateStart + int(progress*float64(g.cfg.RateEnd-g.cfg.RateStart))
}

func (g *LoadGenerator) doOperation(ctx context.Context) error {
	roll := g.rng.Intn(100)

	switch {
	case roll < g.cfg.Profile.InsertPct:
		return g.doInsert(ctx)
	case roll < g.cfg.Profile.InsertPct+g.cfg.Profile.UpdatePct:
		return g.doUpdate(ctx)
	default:
		return g.doDelete(ctx)
	}
}

func (g *LoadGenerator) doInsert(ctx context.Context) error {
	id := g.nextID.Add(1)
	userID := g.rng.Int63n(100000)
	statuses := []string{"pending", "confirmed", "shipped", "delivered", "cancelled"}
	status := statuses[g.rng.Intn(len(statuses))]
	amount := float64(g.rng.Intn(100000)) / 100.0

	_, err := g.pool.Exec(ctx,
		"INSERT INTO bench_orders (id, user_id, status, amount) VALUES ($1, $2, $3, $4)",
		id, userID, status, amount,
	)
	if err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	g.totalOps.Add(1)
	g.insertOps.Add(1)

	// Track ID for future UPDATE/DELETE
	g.mu.Lock()
	g.insertedIDs = append(g.insertedIDs, id)
	// Cap the pool to avoid unbounded growth
	if len(g.insertedIDs) > 100000 {
		g.insertedIDs = g.insertedIDs[50000:]
	}
	g.mu.Unlock()

	return nil
}

func (g *LoadGenerator) doUpdate(ctx context.Context) error {
	id := g.pickExistingID()
	if id == 0 {
		return g.doInsert(ctx) // fallback if no IDs yet
	}

	statuses := []string{"pending", "confirmed", "shipped", "delivered", "cancelled"}
	status := statuses[g.rng.Intn(len(statuses))]
	amount := float64(g.rng.Intn(100000)) / 100.0

	_, err := g.pool.Exec(ctx,
		"UPDATE bench_orders SET status = $1, amount = $2, updated_at = now() WHERE id = $3",
		status, amount, id,
	)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}

	g.totalOps.Add(1)
	g.updateOps.Add(1)
	return nil
}

func (g *LoadGenerator) doDelete(ctx context.Context) error {
	id := g.popExistingID()
	if id == 0 {
		return g.doInsert(ctx) // fallback if no IDs yet
	}

	_, err := g.pool.Exec(ctx,
		"DELETE FROM bench_orders WHERE id = $1", id,
	)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	g.totalOps.Add(1)
	g.deleteOps.Add(1)
	return nil
}

func (g *LoadGenerator) pickExistingID() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.insertedIDs) == 0 {
		return 0
	}
	return g.insertedIDs[g.rng.Intn(len(g.insertedIDs))]
}

func (g *LoadGenerator) popExistingID() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.insertedIDs) == 0 {
		return 0
	}
	idx := g.rng.Intn(len(g.insertedIDs))
	id := g.insertedIDs[idx]
	// Swap-remove
	g.insertedIDs[idx] = g.insertedIDs[len(g.insertedIDs)-1]
	g.insertedIDs = g.insertedIDs[:len(g.insertedIDs)-1]
	return id
}

func (g *LoadGenerator) drainErrors(errCh chan error) error {
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}
