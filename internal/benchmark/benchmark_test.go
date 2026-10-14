package benchmark

import (
	"os"
	"testing"

	"TradeIT/services/engine-service/engine"
)

// TestMain disables persistence once for the whole test process.
//
// engine.Orderbook.Matcher spawns fire-and-forget goroutines that register trades and
// update order status. Flipping PersistenceEnabled back to true once a benchmark returns
// races with those still-in-flight writers: one of them can observe "true" and call into
// the (never initialised) database handle, crashing the process. Setting it once, before
// any matching happens, avoids the race - and with persistence off these benchmarks need
// no database at all.
func TestMain(m *testing.M) {
	engine.PersistenceEnabled = false
	engine.InitTrade()
	os.Exit(m.Run())
}

// BenchmarkMatcherProductionLike measures pure in-memory matching throughput on a
// production-like order flow (see DefaultConfig).
//
//	go test -run '^$' -bench BenchmarkMatcherProductionLike -benchtime 1x ./internal/benchmark/
//
// The engine logs every order it processes, so output is silenced for the measured region
// and restored before the testing package prints the results.
func BenchmarkMatcherProductionLike(b *testing.B) {
	cfg := DefaultConfig()
	cfg.Seed = 42
	const workloadSize = 100000

	gen := NewGenerator(cfg)
	work := gen.Generate(workloadSize)

	restore := Silence()
	lat := newLatencySampler(latencyStride())
	var total int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		books := map[string]*engine.Orderbook{}
		b.StartTimer()
		for j := range work.Orders {
			o := work.Orders[j]
			ob := books[o.Stock]
			if ob == nil {
				ob = engine.InitOrderBook()
				books[o.Stock] = ob
			}
			lat.begin()
			_ = ob.Matcher(o)
			lat.end()
		}
		total += len(work.Orders)
	}
	b.StopTimer()
	restore()

	lat.report(b, "order")
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(total)/secs, "orders/sec")
	}
}

// TestMatcherMultiSymbolWorkloadTerminates guards against a non-advancing match loop. The
// four-symbol, seed-42 flow used by BenchmarkMatcherProductionLike previously made
// Orderbook.Matcher spin and exhaust memory (it was observed growing past 3.8 GB and
// swapping). The same workload must simply complete here.
func TestMatcherMultiSymbolWorkloadTerminates(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 42

	work := NewGenerator(cfg).Generate(100000)

	books := map[string]*engine.Orderbook{}
	for i := range work.Orders {
		o := work.Orders[i]
		ob := books[o.Stock]
		if ob == nil {
			ob = engine.InitOrderBook()
			books[o.Stock] = ob
		}
		if err := ob.Matcher(o); err != nil {
			t.Fatalf("matcher error on order %d: %v", o.Id, err)
		}
	}
}
