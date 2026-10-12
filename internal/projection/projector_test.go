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
	orders      map[uint64]models.Order
	trades      []models.TradeDetails
	checkpoints map[uint32]Checkpoint
	commits     int
}

func newFakeStore() *fakeStore {
	return &fakeStore{orders: map[uint64]models.Order{}, checkpoints: map[uint32]Checkpoint{}}
}

func (s *fakeStore) GetOrder(id uint64) (models.Order, bool, error) {
	o, ok := s.orders[id]
	return o, ok, nil
}

func (s *fakeStore) LoadCheckpoints() (map[uint32]Checkpoint, error) {
	out := make(map[uint32]Checkpoint, len(s.checkpoints))
	for k, v := range s.checkpoints {
		out[k] = v
	}
	return out, nil
}

func (s *fakeStore) CommitBatch(orders []models.Order, trades []models.TradeDetails, cp Checkpoint) error {
	for _, o := range orders {
		s.orders[o.Id] = o
	}
	s.trades = append(s.trades, trades...)
	s.checkpoints[cp.SymbolID] = cp
	s.commits++
	return nil
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

// TestProjectorResumesFromCheckpoint verifies that a restart resumes at the committed
// offset (not from the start) and that the running fill value is seeded from the database,
// so an order's average price stays correct across the restart.
func TestProjectorResumesFromCheckpoint(t *testing.T) {
	dir := t.TempDir()
	tbl, _ := symbol.New([]string{"AAPL"})
	path := filepath.Join(dir, "AAPL.consumer.wal")
	store := newFakeStore()
	now := time.Now().UnixNano()

	var ab [ipc.AppliedRecordSize]byte
	var tb [ipc.TradeRecordSize]byte

	// First run: a resting sell order for 10 partially filled by a 4-lot trade.
	w, err := wal.CreateWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	ipc.EncodeAppliedRecord(ab[:], ipc.Cmd{Op: ipc.OpNew, OrderID: 1, SymbolID: 0, Qty: 10, Price: 100, Side: ipc.SideSell, Typ: ipc.TypeLimit, CreatedAt: now, Seq: 1}, ipc.Outcome{Status: ipc.StatusResting})
	w.Append(ab[:])
	ipc.EncodeTradeRecord(tb[:], ipc.Trade{TradeID: 1, BidOrderID: 2, AskOrderID: 1, Buyer: 2, Seller: 1, Price: 100, Quantity: 4, SymbolID: 0, ExecutedAt: now})
	w.Append(tb[:])
	w.Close()

	p, err := Open(dir, tbl, store, nil, Options{BatchSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, ch := range p.ch {
		p.drainTail(ch)
	}
	p.flushAll()
	p.Close()

	if o := store.orders[1]; o.Status != "partially_filled" || o.Remq != 6 || o.AvgPrice != 100 {
		t.Fatalf("after first run order 1 = %+v, want partially_filled/remq 6/avg 100", o)
	}
	if cp := store.checkpoints[0]; cp.Seq != 1 || cp.Offset == 0 {
		t.Fatalf("checkpoint = %+v, want seq 1 and a non-zero offset", cp)
	}

	// Second run: append a new order and the remaining 6-lot fill, then resume.
	w, err = wal.OpenWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	ipc.EncodeAppliedRecord(ab[:], ipc.Cmd{Op: ipc.OpNew, OrderID: 3, SymbolID: 0, Qty: 5, Price: 150, Side: ipc.SideBuy, Typ: ipc.TypeLimit, CreatedAt: now, Seq: 2}, ipc.Outcome{Status: ipc.StatusResting})
	w.Append(ab[:])
	ipc.EncodeTradeRecord(tb[:], ipc.Trade{TradeID: 2, BidOrderID: 2, AskOrderID: 1, Buyer: 2, Seller: 1, Price: 200, Quantity: 6, SymbolID: 0, ExecutedAt: now})
	w.Append(tb[:])
	w.Close()

	p2, err := Open(dir, tbl, store, nil, Options{BatchSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, ch := range p2.ch {
		p.drainTail(ch)
	}
	p2.flushAll()
	p2.Close()

	// value seeded as 100*4 = 400, plus 200*6 = 1200 -> 1600/10 = 160.
	if o := store.orders[1]; o.Status != "filled" || o.Remq != 0 || o.AvgPrice != 160 {
		t.Fatalf("after resume order 1 = %+v, want filled/remq 0/avg 160", o)
	}
	if _, ok := store.orders[3]; !ok {
		t.Fatalf("order 3 not projected after resume")
	}
	if store.checkpoints[0].Seq != 2 {
		t.Fatalf("checkpoint seq = %d, want 2", store.checkpoints[0].Seq)
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
