package producer

import (
	"io"
	"path/filepath"
	"sync"
	"testing"

	"TradeIT/internal/ingest"
	"TradeIT/internal/ipc"
	"TradeIT/internal/models"
	"TradeIT/internal/symbol"
	"TradeIT/internal/wal"
)

type recHandler struct{ applied []ipc.Cmd }

func (h *recHandler) Apply(c ipc.Cmd) ipc.Outcome {
	h.applied = append(h.applied, c)
	return ipc.Outcome{Status: ipc.StatusAccepted}
}

func open(t *testing.T) (*Producer, *symbol.Table, string) {
	t.Helper()
	dir := t.TempDir()
	tbl, err := symbol.New([]string{"TEST"})
	if err != nil {
		t.Fatal(err)
	}
	p, err := Open(dir, tbl, 1024, 256, 5)
	if err != nil {
		t.Fatal(err)
	}
	return p, tbl, dir
}

func TestSubmitOrderIsDurable(t *testing.T) {
	p, _, dir := open(t)
	defer p.Close()

	id, err := p.SubmitOrder(models.MetaOrder{User_id: 7, Order_Type: "limit", Side: "buy", Stock: "TEST", Price: 100, Quantity: 5}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("expected a generated order id")
	}

	// Submit returned, so the record must already be on disk in the producer WAL.
	r, err := wal.OpenReader(filepath.Join(dir, "TEST.producer.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	count := 0
	for {
		if _, err := r.Next(); err != nil {
			if err != io.EOF {
				t.Fatal(err)
			}
			break
		}
		count++
	}
	if count != 1 {
		t.Fatalf("producer WAL has %d records, want 1", count)
	}
}

func TestSubmitOrderDrainedByConsumer(t *testing.T) {
	p, tbl, dir := open(t)
	defer p.Close()

	id, err := p.SubmitOrder(models.MetaOrder{User_id: 7, Order_Type: "limit", Side: "sell", Stock: "TEST", Price: 100, Quantity: 5}, 0)
	if err != nil {
		t.Fatal(err)
	}

	h := &recHandler{}
	c, err := ingest.OpenConsumer(dir, tbl, h)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	n, err := c.DrainOnce(10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(h.applied) != 1 {
		t.Fatalf("drained %d (applied %d), want 1", n, len(h.applied))
	}
	got := h.applied[0]
	if got.OrderID != id || got.UserID != 7 || got.Side != ipc.SideSell || got.Typ != ipc.TypeLimit || got.Price != 100 || got.Qty != 5 || got.Remq != 5 {
		t.Fatalf("applied cmd = %+v", got)
	}
}

func TestOpsPreserveOrder(t *testing.T) {
	p, tbl, dir := open(t)
	defer p.Close()

	id, err := p.SubmitOrder(models.MetaOrder{User_id: 1, Order_Type: "limit", Side: "buy", Stock: "TEST", Price: 100, Quantity: 10}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Cancel(id, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ReduceQty(id, 0, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Modify(id, 0, 120, 3); err != nil {
		t.Fatal(err)
	}

	h := &recHandler{}
	c, err := ingest.OpenConsumer(dir, tbl, h)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	n, err := c.DrainOnce(10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("drained %d, want 4", n)
	}
	want := []ipc.OpType{ipc.OpNew, ipc.OpCancel, ipc.OpReduceQty, ipc.OpModify}
	for i, w := range want {
		if h.applied[i].Op != w {
			t.Fatalf("op[%d] = %v, want %v", i, h.applied[i].Op, w)
		}
		if h.applied[i].OrderID != id {
			t.Fatalf("op[%d] order id = %d, want %d", i, h.applied[i].OrderID, id)
		}
	}
	if h.applied[3].Price != 120 || h.applied[3].Qty != 3 {
		t.Fatalf("modify cmd = %+v", h.applied[3])
	}
}

// TestConcurrentSubmits exercises the commit goroutine from many goroutines: ids must be
// unique and every record must be drained exactly once.
func TestConcurrentSubmits(t *testing.T) {
	p, tbl, dir := open(t)
	defer p.Close()

	const n = 100
	ids := make([]uint64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, err := p.SubmitOrder(models.MetaOrder{User_id: 1, Order_Type: "limit", Side: "buy", Stock: "TEST", Price: 100, Quantity: 1}, 0)
			if err != nil {
				t.Errorf("submit %d: %v", i, err)
				return
			}
			ids[i] = id
		}(i)
	}
	wg.Wait()

	seen := make(map[uint64]bool, n)
	for _, id := range ids {
		if id == 0 || seen[id] {
			t.Fatalf("non-unique or zero id %d", id)
		}
		seen[id] = true
	}

	h := &recHandler{}
	c, err := ingest.OpenConsumer(dir, tbl, h)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	total := 0
	for total < n {
		m, err := c.DrainOnce(1000)
		if err != nil {
			t.Fatal(err)
		}
		total += m
	}
	if total != n {
		t.Fatalf("drained %d, want %d", total, n)
	}
}
