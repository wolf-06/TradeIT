package ingest

import (
	"encoding/binary"
	"os"
	"testing"

	"TradeIT/internal/ipc"
	"TradeIT/internal/snapshot"
	"TradeIT/internal/symbol"
)

// snapHandler counts applied commands and can snapshot/restore that count, so snapshot
// coverage can be verified without a real order book.
type snapHandler struct {
	applied       int
	restoreCalls  int
	snapCalls     int
	restoredState int
}

func (h *snapHandler) Apply(ipc.Cmd) ipc.Outcome { h.applied++; return ipc.Outcome{} }
func (h *snapHandler) Restore(ipc.Cmd, ipc.Outcome) { h.applied++ }

func (h *snapHandler) Snapshot(uint32, uint64) ([]byte, error) {
	h.snapCalls++
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(h.applied))
	return b, nil
}

func (h *snapHandler) RestoreSnapshot(_ uint32, _ uint64, data []byte) error {
	h.restoreCalls++
	h.applied = int(binary.LittleEndian.Uint64(data))
	h.restoredState = h.applied
	return nil
}

func TestConsumerSnapshotsAndRecovers(t *testing.T) {
	dir := t.TempDir()
	tbl, err := symbol.New([]string{"AAPL"})
	if err != nil {
		t.Fatal(err)
	}
	p, err := OpenProducer(dir, tbl, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for i := 0; i < 10; i++ {
		if _, err := p.Publish(ipc.Cmd{SymbolID: 0, Op: ipc.OpNew, Qty: 1}); err != nil {
			t.Fatal(err)
		}
	}
	p.Sync()

	opts := Options{SnapshotDir: dir, SnapshotEveryN: 4, Codec: snapshot.CodecZstd}
	h := &snapHandler{}
	c, err := OpenConsumerWithOptions(dir, tbl, h, opts)
	if err != nil {
		t.Fatal(err)
	}
	// Drain in passes so snapshots land at intermediate sequences (4 and 8).
	total := 0
	for total < 10 {
		n, err := c.DrainOnce(4)
		if err != nil {
			t.Fatal(err)
		}
		total += n
	}
	c.Close()
	if h.applied != 10 {
		t.Fatalf("applied = %d, want 10", h.applied)
	}
	if h.snapCalls < 2 {
		t.Fatalf("snapCalls = %d, want >= 2", h.snapCalls)
	}

	// Reopen: the latest snapshot (seq 8) restores state, then the WAL tail (9, 10) is
	// replayed. The handler must not re-apply 1..8.
	h2 := &snapHandler{}
	c2, err := OpenConsumerWithOptions(dir, tbl, h2, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if h2.restoreCalls != 1 {
		t.Fatalf("restoreCalls = %d, want 1", h2.restoreCalls)
	}
	if h2.restoredState != 8 {
		t.Fatalf("restored state = %d, want 8", h2.restoredState)
	}
	if h2.applied != 10 {
		t.Fatalf("after recovery applied = %d, want 10", h2.applied)
	}
	if wm := c2.Watermarks()[0]; wm != 10 {
		t.Fatalf("watermark = %d, want 10", wm)
	}
}

func TestProducerReplay(t *testing.T) {
	dir := t.TempDir()
	tbl, _ := symbol.New([]string{"AAPL"})
	p, err := OpenProducer(dir, tbl, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for i := 0; i < 5; i++ {
		if _, err := p.Publish(ipc.Cmd{SymbolID: 0, Op: ipc.OpNew, Qty: 1}); err != nil {
			t.Fatal(err)
		}
	}
	p.Sync()
	p.Close()

	// Simulate a lost ring (reboot): delete the ring file so the producer must replay
	// from its WAL into a fresh ring.
	if err := os.Remove(ringPath(dir, "AAPL")); err != nil {
		t.Fatal(err)
	}

	// Reopen and replay everything after watermark 2 into the (fresh) ring.
	p2, err := OpenProducer(dir, tbl, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if err := p2.Replay([]uint64{2}); err != nil {
		t.Fatal(err)
	}
	// The ring must now hold seq 3,4,5.
	h := &recHandler{}
	c, err := OpenConsumer(dir, tbl, h)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	n, err := c.DrainOnce(100)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("drained %d, want 3", n)
	}
	assertSeqs(t, seqsFor(h.applied, 0), 3, 4, 5)
}
