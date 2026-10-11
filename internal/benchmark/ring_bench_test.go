package benchmark

import (
	"os"
	"runtime"
	"sync"
	"testing"

	"TradeIT/internal/ingest"
	"TradeIT/internal/models"
	"TradeIT/internal/symbol"
	"TradeIT/services/engine-service/engine"
	"TradeIT/services/user-order-service/producer"
)

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

func submitAllBatched(p *producer.Producer, tbl *symbol.Table, orders []models.Order, batch int) error {
	for start := 0; start < len(orders); start += batch {
		end := start + batch
		if end > len(orders) {
			end = len(orders)
		}
		if _, err := p.SubmitOrders(itemsFor(orders, tbl, start, end)); err != nil {
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
		c, err := ingest.OpenConsumer(dir, tbl, engine.NewRingHandler(ld, tbl))
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
			if err := submitAllBatched(p, tbl, work.Orders, batch); err != nil {
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
		if err := submitAllBatched(p, tbl, work.Orders, 512); err != nil {
			b.Fatal(err)
		}
		ld := engine.InitLedger()
		c, err := ingest.OpenConsumer(dir, tbl, engine.NewRingHandler(ld, tbl))
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

	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(total)/secs, "orders/sec")
	}
}

// BenchmarkRingProducerOnly isolates the producer hot path with batched submission
// (encode + WAL append + ring publish + group-commit fsync).
func BenchmarkRingProducerOnly(b *testing.B) {
	cfg := DefaultConfig()
	cfg.Seed = 42
	const workloadSize = 100000

	work := NewGenerator(cfg).Generate(workloadSize)
	tbl := benchTable(b, cfg.Symbols)

	restore := Silence()
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
		if err := submitAllBatched(p, tbl, work.Orders, 512); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		p.Close()
		cleanup()
		total += workloadSize
	}
	b.StopTimer()
	restore()

	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(total)/secs, "orders/sec")
	}
}
