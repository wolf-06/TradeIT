package ingest

import (
	"errors"
	"testing"

	"TradeIT/internal/ipc"
	"TradeIT/internal/symbol"
)

type recHandler struct {
	applied  []ipc.Cmd
	restored []ipc.Cmd
}

func (h *recHandler) Apply(c ipc.Cmd) ipc.Outcome {
	h.applied = append(h.applied, c)
	return ipc.Outcome{Status: ipc.StatusResting}
}

func (h *recHandler) Restore(c ipc.Cmd, _ ipc.Outcome) {
	h.restored = append(h.restored, c)
}

func seqsFor(cmds []ipc.Cmd, sym uint32) []uint64 {
	var out []uint64
	for _, c := range cmds {
		if c.SymbolID == sym {
			out = append(out, c.Seq)
		}
	}
	return out
}

func assertSeqs(t *testing.T, got []uint64, want ...uint64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("seq count = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("seq[%d] = %d, want %d (%v)", i, got[i], want[i], got)
		}
	}
}

func TestEndToEndTwoSymbols(t *testing.T) {
	dir := t.TempDir()
	tbl, _ := symbol.New([]string{"AAPL", "GOOG"})
	p, err := OpenProducer(dir, tbl, 64)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := p.Publish(ipc.Cmd{SymbolID: 0, OrderID: uint64(i), Op: ipc.OpNew, Side: ipc.SideBuy, Typ: ipc.TypeLimit, Price: 100, Qty: 10, Remq: 10}); err != nil {
			t.Fatal(err)
		}
		if _, err := p.Publish(ipc.Cmd{SymbolID: 1, OrderID: uint64(100 + i), Op: ipc.OpNew, Side: ipc.SideSell, Typ: ipc.TypeLimit, Price: 200, Qty: 10, Remq: 10}); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}

	h := &recHandler{}
	c, err := OpenConsumer(dir, tbl, h)
	if err != nil {
		t.Fatal(err)
	}
	n, err := c.DrainOnce(100)
	if err != nil {
		t.Fatal(err)
	}
	if n != 10 {
		t.Fatalf("DrainOnce = %d, want 10", n)
	}
	assertSeqs(t, seqsFor(h.applied, 0), 1, 2, 3, 4, 5)
	assertSeqs(t, seqsFor(h.applied, 1), 1, 2, 3, 4, 5)
	if wm := c.Watermarks(); wm[0] != 5 || wm[1] != 5 {
		t.Fatalf("watermarks = %v, want [5 5]", wm)
	}
	c.Close()
	p.Close()
}

func TestConsumerRecoveryNoDoubleApply(t *testing.T) {
	dir := t.TempDir()
	tbl, _ := symbol.New([]string{"AAPL"})
	p, err := OpenProducer(dir, tbl, 64)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := p.Publish(ipc.Cmd{SymbolID: 0, OrderID: uint64(i), Op: ipc.OpNew, Side: ipc.SideBuy, Typ: ipc.TypeLimit, Price: 100, Qty: 1, Remq: 1}); err != nil {
			t.Fatal(err)
		}
	}
	p.Sync()

	h1 := &recHandler{}
	c1, err := OpenConsumer(dir, tbl, h1)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := c1.DrainOnce(4); n != 4 {
		t.Fatalf("first drain = %d, want 4", n)
	}
	c1.Close()

	// Restart: the applied WAL must be replayed into a fresh handler, then the rest
	// of the ring drained without re-applying anything.
	h2 := &recHandler{}
	c2, err := OpenConsumer(dir, tbl, h2)
	if err != nil {
		t.Fatal(err)
	}
	assertSeqs(t, seqsFor(h2.restored, 0), 1, 2, 3, 4)
	n, err := c2.DrainOnce(100)
	if err != nil {
		t.Fatal(err)
	}
	if n != 6 {
		t.Fatalf("second drain = %d, want 6", n)
	}
	assertSeqs(t, seqsFor(h2.applied, 0), 5, 6, 7, 8, 9, 10)
	if wm := c2.Watermarks()[0]; wm != 10 {
		t.Fatalf("watermark = %d, want 10", wm)
	}
	c2.Close()
	p.Close()
}

func TestProducerSeqResume(t *testing.T) {
	dir := t.TempDir()
	tbl, _ := symbol.New([]string{"AAPL"})
	p, _ := OpenProducer(dir, tbl, 64)
	for i := 0; i < 5; i++ {
		p.Publish(ipc.Cmd{SymbolID: 0, OrderID: uint64(i), Op: ipc.OpNew, Side: ipc.SideBuy, Typ: ipc.TypeLimit, Price: 1, Qty: 1, Remq: 1})
	}
	p.Sync()
	p.Close()

	p2, err := OpenProducer(dir, tbl, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	seq, err := p2.Publish(ipc.Cmd{SymbolID: 0, OrderID: 99, Op: ipc.OpNew, Side: ipc.SideBuy, Typ: ipc.TypeLimit, Price: 1, Qty: 1, Remq: 1})
	if err != nil {
		t.Fatal(err)
	}
	if seq != 6 {
		t.Fatalf("resumed seq = %d, want 6", seq)
	}
}

func TestRingFull(t *testing.T) {
	dir := t.TempDir()
	tbl, _ := symbol.New([]string{"AAPL"})
	p, _ := OpenProducer(dir, tbl, 4)
	defer p.Close()
	for i := 0; i < 4; i++ {
		if _, err := p.Publish(ipc.Cmd{SymbolID: 0, Op: ipc.OpNew, Qty: 1, Remq: 1}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if _, err := p.Publish(ipc.Cmd{SymbolID: 0, Op: ipc.OpNew, Qty: 1, Remq: 1}); !errors.Is(err, ErrRingFull) {
		t.Fatalf("err = %v, want ErrRingFull", err)
	}
}
