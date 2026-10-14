package benchmark

import (
	"os"
	"runtime"
	"sync"
	"testing"

	"TradeIT/internal/ingest"
	"TradeIT/internal/ipc"
	"TradeIT/internal/models"
	"TradeIT/internal/symbol"
	"TradeIT/services/engine-service/engine"
	"TradeIT/services/user-order-service/producer"
)

// timedHandler wraps an ingest.Handler to record the per-order apply latency. The consumer
// calls Apply exactly once per command, so bracketing it measures the match path as the
// engine sees it (decode and the applied-WAL append happen outside Apply, in the consumer).
// It forwards the optional TradeSource/Restorer/Snapshotter capabilities so it is a drop-in
// replacement for engine.RingHandler, including trade logging.
type timedHandler struct {
	inner   ingest.Handler
	sampler *latencySampler
}

func (h *timedHandler) Apply(c ipc.Cmd) ipc.Outcome {
	h.sampler.begin()
	o := h.inner.Apply(c)
	h.sampler.end()
	return o
}

func (h *timedHandler) Trades() []ipc.Trade {
	if ts, ok := h.inner.(ingest.TradeSource); ok {
		return ts.Trades()
	}
	return nil
}

func (h *timedHandler) Restore(c ipc.Cmd, o ipc.Outcome) {
	if r, ok := h.inner.(ingest.Restorer); ok {
		r.Restore(c, o)
	}
}

func (h *timedHandler) Snapshot(symbolID uint32, seq uint64) ([]byte, error) {
	if s, ok := h.inner.(ingest.Snapshotter); ok {
		return s.Snapshot(symbolID, seq)
	}
	return nil, nil
}

func (h *timedHandler) RestoreSnapshot(symbolID uint32, seq uint64, data []byte) error {
	if s, ok := h.inner.(ingest.Snapshotter); ok {
		return s.RestoreSnapshot(symbolID, seq, data)
	}
	return nil
}

// fastDir returns a directory for the ring/WAL files. It prefers /dev/shm (tmpfs) so the
// group-commit fsync is not the measurement confound; this isolates the ring hot path.
func fastDir(b *testing.B) (string, func()) {
	b.Helper()
	if fi, err := os.Stat("/dev/shm"); err == nil && fi.IsDir() {
		if d, err := os.MkdirTemp("/dev/shm", "tradeit-bench-"); err == nil {
			return d, func() { os.RemoveAll(d) }
		}
	}
	d := b.TempDir()
	return d, func() {}
}

func benchTable(b *testing.B, symbols []string) *symbol.Table {
	b.Helper()
	tbl, err := symbol.New(symbols)
	if err != nil {
		b.Fatal(err)
	}
	return tbl
}

func itemsFor(orders []models.Order, tbl *symbol.Table, from, to int) []producer.OrderItem {
	items := make([]producer.OrderItem, 0, to-from)
	for j := from; j < to; j++ {
		sid, _ := tbl.ID(orders[j].Stock)
		items = append(items, producer.OrderItem{Order: orders[j].MetaOrder, SymbolID: sid})
	}
	return items
}

// submitAllBatched publishes orders in fixed-size batches. When lat is non-nil it times
// each SubmitOrders call, so the producer benchmark can report per-batch latency; the
// consumer benchmarks pass nil because their submission runs outside the measured region.
func submitAllBatched(p *producer.Producer, tbl *symbol.Table, orders []models.Order, batch int, lat *latencySampler) error {
	for start := 0; start < len(orders); start += batch {
		end := start + batch
		if end > len(orders) {
			end = len(orders)
		}
		if lat != nil {
			lat.begin()
		}
		_, err := p.SubmitOrders(itemsFor(orders, tbl, start, end))
		if lat != nil {
			lat.end()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// BenchmarkRingEndToEnd measures the full new ingest hot path with batched submission: the
// producer builds command batches, appends them to the producer WAL and publishes them to
// the shared-memory ring, while the consumer drains the ring and applies them to a real
// ledger. Compare with BenchmarkMatcherProductionLike (matcher alone).
//
//	go test -run '^$' -bench BenchmarkRingEndToEnd -benchtime 1x ./internal/benchmark/
func BenchmarkRingEndToEnd(b *testing.B) {
	cfg := DefaultConfig()
	cfg.Seed = 42
	const (
		workloadSize = 100000
		batch        = 512
	)

	work := NewGenerator(cfg).Generate(workloadSize)
	tbl := benchTable(b, cfg.Symbols)

	restore := Silence()
	lat := newLatencySampler(latencyStride())
	var total int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir, cleanup := fastDir(b)
		p, err := producer.Open(dir, tbl, 1<<18, 256, 5)
		if err != nil {
			b.Fatal(err)
		}
		ld := engine.InitLedger()
		th := &timedHandler{inner: engine.NewRingHandler(ld, tbl), sampler: lat}
		c, err := ingest.OpenConsumer(dir, tbl, th)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		var (
			prodErr   error
			prodErrMu sync.Mutex
		)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := submitAllBatched(p, tbl, work.Orders, batch, nil); err != nil {
				prodErrMu.Lock()
				prodErr = err
				prodErrMu.Unlock()
			}
		}()

		applied := 0
		for applied < workloadSize {
			n, err := c.DrainOnce(4096)
			if err != nil {
				b.Fatal(err)
			}
			applied += n
			if n == 0 {
				prodErrMu.Lock()
				e := prodErr
				prodErrMu.Unlock()
				if e != nil {
					b.Fatal(e)
				}
				runtime.Gosched()
			}
		}
		wg.Wait()

		b.StopTimer()
		c.Close()
		p.Close()
		cleanup()
		total += workloadSize
	}
	b.StopTimer()
	restore()

	lat.report(b, "order")
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(total)/secs, "orders/sec")
	}
}

// BenchmarkRingConsumerOnly isolates the consumer hot path (ring drain + decode + match +
// applied-WAL append) from the producer: orders are pre-published outside the timed
// region, then drained.
func BenchmarkRingConsumerOnly(b *testing.B) {
	cfg := DefaultConfig()
	cfg.Seed = 42
	const workloadSize = 100000

	work := NewGenerator(cfg).Generate(workloadSize)
	tbl := benchTable(b, cfg.Symbols)

	restore := Silence()
	lat := newLatencySampler(latencyStride())
	var total int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir, cleanup := fastDir(b)
		p, err := producer.Open(dir, tbl, 1<<18, 256, 5)
		if err != nil {
			b.Fatal(err)
		}
		if err := submitAllBatched(p, tbl, work.Orders, 512, nil); err != nil {
			b.Fatal(err)
		}
		ld := engine.InitLedger()
		th := &timedHandler{inner: engine.NewRingHandler(ld, tbl), sampler: lat}
		c, err := ingest.OpenConsumer(dir, tbl, th)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		applied := 0
		for applied < workloadSize {
			n, err := c.DrainOnce(4096)
			if err != nil {
				b.Fatal(err)
			}
			applied += n
		}
		b.StopTimer()
		c.Close()
		p.Close()
		cleanup()
		total += workloadSize
	}
	b.StopTimer()
	restore()

	lat.report(b, "order")
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(total)/secs, "orders/sec")
	}
}

// BenchmarkRingProducerOnly isolates the producer hot path with batched submission
// (encode + WAL append + ring publish + group-commit fsync). Latency is reported per
// batch; each batch is 512 orders.
func BenchmarkRingProducerOnly(b *testing.B) {
	cfg := DefaultConfig()
	cfg.Seed = 42
	const workloadSize = 100000

	work := NewGenerator(cfg).Generate(workloadSize)
	tbl := benchTable(b, cfg.Symbols)

	restore := Silence()
	// Only ~196 batches per iteration, so sample every one rather than striding.
	lat := newLatencySampler(1)
	var total int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir, cleanup := fastDir(b)
		p, err := producer.Open(dir, tbl, 1<<18, 256, 5)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := submitAllBatched(p, tbl, work.Orders, 512, lat); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		p.Close()
		cleanup()
		total += workloadSize
	}
	b.StopTimer()
	restore()

	lat.report(b, "batch")
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(total)/secs, "orders/sec")
	}
}
