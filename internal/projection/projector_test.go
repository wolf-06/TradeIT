package projection

import (
	"path/filepath"
	"testing"
	"time"

	"TradeIT/internal/ipc"
	"TradeIT/internal/models"
	"TradeIT/internal/symbol"
	"TradeIT/internal/wal"
)

type fakeStore struct {
	orders map[uint64]models.Order
	trades []models.TradeDetails
}

func newFakeStore() *fakeStore { return &fakeStore{orders: map[uint64]models.Order{}} }

func (s *fakeStore) UpsertOrders(orders []models.Order) error {
	for _, o := range orders {
		s.orders[o.Id] = o
	}
	return nil
}
func (s *fakeStore) InsertTrades(trades []models.TradeDetails) error {
	s.trades = append(s.trades, trades...)
	return nil
}
func (s *fakeStore) GetOrder(id uint64) (models.Order, bool, error) {
	o, ok := s.orders[id]
	return o, ok, nil
}

type fakeCache struct{ status map[uint64]string }

func newFakeCache() *fakeCache { return &fakeCache{status: map[uint64]string{}} }

func (c *fakeCache) SetOrderStatus(id uint64, status string, _ int, _ int64) error {
	c.status[id] = status
	return nil
}

func TestProjectorProjectsOrdersAndTrades(t *testing.T) {
	dir := t.TempDir()
	tbl, err := symbol.New([]string{"AAPL"})
	if err != nil {
		t.Fatal(err)
	}
	w, err := wal.CreateWriter(filepath.Join(dir, "AAPL.consumer.wal"))
	if err != nil {
		t.Fatal(err)
	}
	writeApplied := func(cmd ipc.Cmd, o ipc.Outcome) {
		var buf [ipc.AppliedRecordSize]byte
		ipc.EncodeAppliedRecord(buf[:], cmd, o)
		if err := w.Append(buf[:]); err != nil {
			t.Fatal(err)
		}
	}
	writeTrade := func(tr ipc.Trade) {
		var buf [ipc.TradeRecordSize]byte
		ipc.EncodeTradeRecord(buf[:], tr)
		if err := w.Append(buf[:]); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UnixNano()
	writeApplied(ipc.Cmd{Op: ipc.OpNew, OrderID: 1, SymbolID: 0, Qty: 5, Price: 100, Side: ipc.SideSell, Typ: ipc.TypeLimit, CreatedAt: now}, ipc.Outcome{Status: ipc.StatusResting})
	writeApplied(ipc.Cmd{Op: ipc.OpNew, OrderID: 2, SymbolID: 0, Qty: 5, Price: 100, Side: ipc.SideBuy, Typ: ipc.TypeLimit, CreatedAt: now}, ipc.Outcome{Status: ipc.StatusResting})
	writeTrade(ipc.Trade{TradeID: 999, BidOrderID: 2, AskOrderID: 1, Buyer: 2, Seller: 1, Price: 100, Quantity: 5, SymbolID: 0, ExecutedAt: now})
	w.Close()

	store := newFakeStore()
	cache := newFakeCache()
	p, err := Open(dir, tbl, store, cache, Options{BatchSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for _, ch := range p.ch {
		p.drainTail(ch)
	}
	p.flushAll()

	for _, id := range []uint64{1, 2} {
		o, ok := store.orders[id]
		if !ok {
			t.Fatalf("order %d not projected", id)
		}
		if o.Status != "filled" || o.Remq != 0 || o.AvgPrice != 100 {
			t.Fatalf("order %d = %+v, want filled/remq 0/avg 100", id, o)
		}
	}
	if len(store.trades) != 1 || store.trades[0].TradeId != 999 || store.trades[0].Quantity != 5 {
		t.Fatalf("trades = %+v, want one trade id 999 qty 5", store.trades)
	}
	if cache.status[1] != "filled" || cache.status[2] != "filled" {
		t.Fatalf("cache status = %+v, want both filled", cache.status)
	}
}

func TestProjectorCancel(t *testing.T) {
	dir := t.TempDir()
	tbl, _ := symbol.New([]string{"AAPL"})
	w, err := wal.CreateWriter(filepath.Join(dir, "AAPL.consumer.wal"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixNano()
	var ab [ipc.AppliedRecordSize]byte
	ipc.EncodeAppliedRecord(ab[:], ipc.Cmd{Op: ipc.OpNew, OrderID: 1, SymbolID: 0, Qty: 5, Price: 100, Side: ipc.SideBuy, Typ: ipc.TypeLimit, CreatedAt: now}, ipc.Outcome{Status: ipc.StatusResting})
	w.Append(ab[:])
	ipc.EncodeAppliedRecord(ab[:], ipc.Cmd{Op: ipc.OpCancel, OrderID: 1, SymbolID: 0}, ipc.Outcome{Status: ipc.StatusCancelled})
	w.Append(ab[:])
	w.Close()

	store := newFakeStore()
	p, err := Open(dir, tbl, store, nil, Options{BatchSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for _, ch := range p.ch {
		p.drainTail(ch)
	}
	p.flushAll()

	o, ok := store.orders[1]
	if !ok || o.Status != "cancelled" {
		t.Fatalf("order 1 = %+v, want cancelled", o)
	}
}
