package engine

import (
	"bytes"
	"testing"
)

func TestOrderbookSnapshotRoundTrip(t *testing.T) {
	ob := InitOrderBook()
	ob.InsertOrder(limitOrder(1, "buy", 100, 5))
	ob.InsertOrder(limitOrder(2, "buy", 100, 3))
	ob.InsertOrder(limitOrder(3, "buy", 99, 7))
	ob.InsertOrder(limitOrder(4, "sell", 105, 4))
	ob.InsertOrder(limitOrder(5, "sell", 106, 2))
	ob.InsertOrder(marketOrder(6, "buy", 1))
	ob.InsertOrder(marketOrder(7, "sell", 2))
	ob.SetLTP(102)

	data := ob.Snapshot()

	ob2 := InitOrderBook()
	if err := ob2.Restore("TEST", data); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, ob2.Snapshot()) {
		t.Fatal("restored book does not re-snapshot identically")
	}
	if len(ob2.GetOrderTable()) != len(ob.GetOrderTable()) {
		t.Fatalf("order table size = %d, want %d", len(ob2.GetOrderTable()), len(ob.GetOrderTable()))
	}

	// The restored book must still match: a buy of 4 hits the restored market-sell queue
	// (qty 2) first, then the best ask at 105.
	if err := ob2.Matcher(marketOrder(8, "buy", 4)); err != nil {
		t.Fatal(err)
	}
	if ob2.GetTradeCount() != 2 {
		t.Fatalf("trades after restore = %d, want 2", ob2.GetTradeCount())
	}
}

func TestOrderbookSnapshotEmpty(t *testing.T) {
	ob := InitOrderBook()
	ob.SetLTP(50)
	data := ob.Snapshot()
	ob2 := InitOrderBook()
	if err := ob2.Restore("TEST", data); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, ob2.Snapshot()) {
		t.Fatal("empty book round trip mismatch")
	}
}
