package engine

import (
	"io"
	"path/filepath"
	"testing"

	"TradeIT/internal/ingest"
	"TradeIT/internal/ipc"
	"TradeIT/internal/models"
	"TradeIT/internal/symbol"
	"TradeIT/internal/wal"
	"TradeIT/services/user-order-service/producer"
)

// TestRingPathLogsTrades drives a crossing sell+buy through the ring and checks the
// consumer WAL carries two applied records and one trade record (the durable event log the
// projection service reads).
func TestRingPathLogsTrades(t *testing.T) {
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

	r, err := wal.OpenReader(filepath.Join(dir, "AAPL.consumer.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var applied, trades int
	for {
		rec, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		switch ipc.RecordKind(rec) {
		case ipc.RecordApplied:
			applied++
		case ipc.RecordTrade:
			trades++
		}
	}
	if applied != 2 {
		t.Fatalf("applied records = %d, want 2", applied)
	}
	if trades != 1 {
		t.Fatalf("trade records = %d, want 1", trades)
	}
}
