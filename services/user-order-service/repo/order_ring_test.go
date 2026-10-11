package repo

import (
	"testing"

	"TradeIT/internal/ingest"
	"TradeIT/internal/ipc"
	"TradeIT/internal/models"
	"TradeIT/internal/symbol"
	"TradeIT/services/user-order-service/producer"
)

type recHandler struct{ applied []ipc.Cmd }

func (h *recHandler) Apply(c ipc.Cmd) ipc.Outcome {
	h.applied = append(h.applied, c)
	return ipc.Outcome{Status: ipc.StatusAccepted}
}

// TestCreateOrderRing verifies the ring ingest wiring at the repo boundary: with a
// producer installed, CreateOrderRing publishes the order to the ring (durable in the
// producer WAL) and reports its id and sequence, and a consumer can drain it.
func TestCreateOrderRing(t *testing.T) {
	dir := t.TempDir()
	tbl, err := symbol.New([]string{"AAPL"})
	if err != nil {
		t.Fatal(err)
	}
	p, err := producer.Open(dir, tbl, 1024, 256, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	if RingEnabled() {
		t.Fatal("ring should be disabled before SetRingProducer")
	}
	SetRingProducer(p)
	defer SetRingProducer(nil)
	if !RingEnabled() {
		t.Fatal("ring should be enabled after SetRingProducer")
	}

	res, err := CreateOrderRing(models.MetaOrder{
		User_id: 7, Order_Type: "limit", Side: "buy", Stock: "AAPL", Price: 100, Quantity: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.OrderID == 0 || res.Seq == 0 {
		t.Fatalf("bad submit result: %+v", res)
	}

	h := &recHandler{}
	c, err := ingest.OpenConsumer(dir, tbl, h)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if n, err := c.DrainOnce(10); err != nil || n != 1 {
		t.Fatalf("drain = %d, err %v, want 1", n, err)
	}
	got := h.applied[0]
	if got.OrderID != res.OrderID || got.SymbolID != 0 || got.Side != ipc.SideBuy || got.Qty != 5 {
		t.Fatalf("applied cmd = %+v", got)
	}
}

// TestCreateOrderRingUnknownSymbol rejects a symbol that is not in the table.
func TestCreateOrderRingUnknownSymbol(t *testing.T) {
	dir := t.TempDir()
	tbl, _ := symbol.New([]string{"AAPL"})
	p, err := producer.Open(dir, tbl, 1024, 256, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	SetRingProducer(p)
	defer SetRingProducer(nil)

	if _, err := CreateOrderRing(models.MetaOrder{Stock: "NOPE", Order_Type: "limit", Side: "buy", Quantity: 1}); err == nil {
		t.Fatal("expected an error for an unknown symbol")
	}
}
