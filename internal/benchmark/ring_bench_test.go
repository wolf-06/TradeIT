package benchmark

import (
	"errors"
	"os"
	"runtime"
	"sync"
	"testing"

	"TradeIT/internal/ingest"
	"TradeIT/internal/symbol"
	"TradeIT/services/engine-service/engine"
	"TradeIT/services/user-order-service/producer"
)

// fastDir returns a directory for the ring/WAL files. It prefers /dev/shm (tmpfs) so the
// group-commit fsync is not the measurement confound; this isolates the ring hot path.
// Fall back to the test temp dir when tmpfs is unavailable.
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

// BenchmarkRingEndToEnd measures the full new ingest hot path: the producer builds
// commands, appends them to the producer WAL and publishes them to the shared-memory ring,
// while the consumer drains the ring and applies them to a real ledger. Compare with
// BenchmarkMatcherProductionLike (matcher alone).
//
//	go test -run '^$' -bench BenchmarkRingEndToEnd -benchtime 1x ./internal/benchmark/
func BenchmarkRingEndToEnd(b *testing.B) {
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
		p, err := producer.Open(dir, tbl, 1<<18, 1, 5)
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
			for j := range work.Orders {
				o := work.Orders[j]
				sid, ok := tbl.ID(o.Stock)
				if !ok {
					prodErrMu.Lock()
					prodErr = errors.New("unknown symbol: " + o.Stock)
					prodErrMu.Unlock()
					return
				}
				if _, err := p.SubmitOrder(o.MetaOrder, sid); err != nil {
					prodErrMu.Lock()
					prodErr = err
					prodErrMu.Unlock()
					return
				}
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
//
//	go test -run '^$' -bench BenchmarkRingConsumerOnly -benchtime 1x ./internal/benchmark/
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
		p, err := producer.Open(dir, tbl, 1<<18, 1, 5)
		if err != nil {
			b.Fatal(err)
		}
		for j := range work.Orders {
			o := work.Orders[j]
			sid, _ := tbl.ID(o.Stock)
			if _, err := p.SubmitOrder(o.MetaOrder, sid); err != nil {
				b.Fatal(err)
			}
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

// BenchmarkRingProducerOnly isolates the producer hot path (encode + WAL append + ring
// publish + group-commit fsync).
//
//	go test -run '^$' -bench BenchmarkRingProducerOnly -benchtime 1x ./internal/benchmark/
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
		p, err := producer.Open(dir, tbl, 1<<18, 1, 5)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		for j := range work.Orders {
			o := work.Orders[j]
			sid, _ := tbl.ID(o.Stock)
			if _, err := p.SubmitOrder(o.MetaOrder, sid); err != nil {
				b.Fatal(err)
			}
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
