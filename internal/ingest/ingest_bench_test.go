package ingest

import (
	"os"
	"runtime"
	"testing"

	"TradeIT/internal/ipc"
	"TradeIT/internal/symbol"
)

// benchDir prefers tmpfs so the fsync cost matches the producer benchmark setup.
func benchDir(b *testing.B) string {
	b.Helper()
	if fi, err := os.Stat("/dev/shm"); err == nil && fi.IsDir() {
		if d, err := os.MkdirTemp("/dev/shm", "ingest-bench-"); err == nil {
			b.Cleanup(func() { os.RemoveAll(d) })
			return d
		}
	}
	return b.TempDir()
}

// benchProducer opens a 4-symbol producer and runs a drainer that keeps the rings empty,
// so Publish never blocks. It measures the producer-side primitive (WAL append + ring
// publish) without the group-commit goroutine and channel handoff.
func benchProducer(b *testing.B) (*Producer, func()) {
	b.Helper()
	tbl, err := symbol.New([]string{"AAPL", "GOOG", "TSLA", "MSFT"})
	if err != nil {
		b.Fatal(err)
	}
	p, err := OpenProducer(benchDir(b), tbl, 1<<20)
	if err != nil {
		b.Fatal(err)
	}
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, ch := range p.ch {
				ch.ring.Ack(ch.ring.Head() - ch.ring.Tail())
			}
			runtime.Gosched()
		}
	}()
	return p, func() { close(stop); p.Close() }
}

func publishOne(b *testing.B, p *Producer, cmd ipc.Cmd) {
	for {
		_, err := p.Publish(cmd)
		if err == nil {
			return
		}
		if err != ErrRingFull {
			b.Fatal(err)
		}
		runtime.Gosched()
	}
}

// BenchmarkPublishNoSync: WAL append (buffered) + ring publish, no fsync.
func BenchmarkPublishNoSync(b *testing.B) {
	p, cleanup := benchProducer(b)
	defer cleanup()
	cmd := ipc.Cmd{Op: ipc.OpNew, SymbolID: 0, Qty: 1, Remq: 1}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		publishOne(b, p, cmd)
	}
}

// BenchmarkPublishSyncPerRecord: as above plus one Sync (which flushes every symbol's
// WAL) per record - the producer's syncEveryN=1 path.
func BenchmarkPublishSyncPerRecord(b *testing.B) {
	p, cleanup := benchProducer(b)
	defer cleanup()
	cmd := ipc.Cmd{Op: ipc.OpNew, SymbolID: 0, Qty: 1, Remq: 1}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		publishOne(b, p, cmd)
		if err := p.Sync(); err != nil {
			b.Fatal(err)
		}
	}
}
