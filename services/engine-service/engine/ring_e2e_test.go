package engine

import (
	"testing"

	"TradeIT/internal/ingest"
	"TradeIT/internal/models"
	"TradeIT/internal/symbol"
	"TradeIT/services/user-order-service/producer"
)

// TestOrderPathEndToEnd drives real orders through the whole new path: the producer
// builds commands, appends them to the producer WAL and publishes them to the ring; the
// consumer drains the ring and applies them to a real Orderbook via RingHandler. A
// crossing sell+buy must produce exactly one trade and leave the book empty.
func TestOrderPathEndToEnd(t *testing.T) {
	dir := t.TempDir()
	tbl, err := symbol.New([]string{"AAPL"})
	if err != nil {
		t.Fatal(err)
	}

	p, err := producer.Open(dir, tbl, 1024, 64, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	sell := models.MetaOrder{User_id: 1, Order_Type: "limit", Side: "sell", Stock: "AAPL", Price: 100, Quantity: 5}
	buy := models.MetaOrder{User_id: 2, Order_Type: "limit", Side: "buy", Stock: "AAPL", Price: 100, Quantity: 5}
	if _, err := p.SubmitOrder(sell, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := p.SubmitOrder(buy, 0); err != nil {
		t.Fatal(err)
	}

	ld := InitLedger()
	c, err := ingest.OpenConsumer(dir, tbl, NewRingHandler(ld, tbl))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	total := 0
	for total < 2 {
		n, err := c.DrainOnce(100)
		if err != nil {
			t.Fatal(err)
		}
		total += n
	}
	if total != 2 {
		t.Fatalf("drained %d, want 2", total)
	}

	ob := ld.ent["AAPL"]
	if ob == nil {
		t.Fatal("no AAPL book created")
	}
	if got := ob.GetTradeCount(); got != 1 {
		t.Fatalf("trades = %d, want 1", got)
	}
	if bids, asks := ob.GetBuyOrders(), ob.GetSellOrders(); len(bids) != 0 || len(asks) != 0 {
		t.Fatalf("book not empty after full cross: bids=%v asks=%v", bids, asks)
	}
}
