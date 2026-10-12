package engine

import (
	"testing"

	"TradeIT/internal/ipc"
	"TradeIT/internal/symbol"
)

func newCmd(op ipc.OpType, id uint64, side ipc.Side, typ ipc.OrderType, price int64, qty int32) ipc.Cmd {
	return ipc.Cmd{Op: op, OrderID: id, Side: side, Typ: typ, Price: price, Qty: qty, SymbolID: 0}
}

func newTestHandler(t *testing.T) (*RingHandler, *Ledger) {
	t.Helper()
	tbl, err := symbol.New([]string{"TEST"})
	if err != nil {
		t.Fatal(err)
	}
	ld := InitLedger()
	return NewRingHandler(ld, tbl), ld
}

func TestRingHandlerNewAndCancel(t *testing.T) {
	h, ld := newTestHandler(t)
	if o := h.Apply(newCmd(ipc.OpNew, 1, ipc.SideSell, ipc.TypeLimit, 100, 5)); o.Status != ipc.StatusAccepted {
		t.Fatalf("new: %+v", o)
	}
	ob := ld.ent["TEST"]
	if len(ob.GetSellOrders()) != 1 {
		t.Fatalf("want 1 sell level, got %d", len(ob.GetSellOrders()))
	}
	if o := h.Apply(ipc.Cmd{Op: ipc.OpCancel, OrderID: 1, SymbolID: 0}); o.Status != ipc.StatusAccepted {
		t.Fatalf("cancel: %+v", o)
	}
	if len(ob.GetSellOrders()) != 0 {
		t.Fatalf("want 0 sell levels after cancel, got %d", len(ob.GetSellOrders()))
	}
}

func TestRingHandlerCrossTrade(t *testing.T) {
	h, ld := newTestHandler(t)
	h.Apply(newCmd(ipc.OpNew, 1, ipc.SideSell, ipc.TypeLimit, 100, 5))
	h.Apply(newCmd(ipc.OpNew, 2, ipc.SideBuy, ipc.TypeLimit, 100, 5))
	if ob := ld.ent["TEST"]; ob.GetTradeCount() != 1 {
		t.Fatalf("want 1 trade, got %d", ob.GetTradeCount())
	}
}

func TestRingHandlerReduceQty(t *testing.T) {
	h, ld := newTestHandler(t)
	h.Apply(newCmd(ipc.OpNew, 1, ipc.SideBuy, ipc.TypeLimit, 100, 10))
	if o := h.Apply(ipc.Cmd{Op: ipc.OpReduceQty, OrderID: 1, SymbolID: 0, Qty: 4}); o.Status != ipc.StatusAccepted {
		t.Fatalf("reduce: %+v", o)
	}
	node := ld.ent["TEST"].GetOrderTable()[1]
	if node == nil || node.Order_.Remq != 4 {
		t.Fatalf("after reduce Remq = %v, want 4", node)
	}
}

func TestRingHandlerModify(t *testing.T) {
	h, ld := newTestHandler(t)
	h.Apply(newCmd(ipc.OpNew, 1, ipc.SideSell, ipc.TypeLimit, 100, 5))
	if o := h.Apply(ipc.Cmd{Op: ipc.OpModify, OrderID: 1, SymbolID: 0, Price: 120, Qty: 5}); o.Status != ipc.StatusAccepted {
		t.Fatalf("modify: %+v", o)
	}
	levels := ld.ent["TEST"].GetSellOrders()
	if _, ok := levels[120]; !ok {
		t.Fatalf("want a sell level at 120, got %v", levels)
	}
	if _, ok := levels[100]; ok {
		t.Fatalf("old price 100 should be gone, got %v", levels)
	}
}

func TestRingHandlerModifyExecutedRejected(t *testing.T) {
	h, _ := newTestHandler(t)
	h.Apply(newCmd(ipc.OpNew, 1, ipc.SideSell, ipc.TypeLimit, 100, 5))
	h.Apply(newCmd(ipc.OpNew, 2, ipc.SideBuy, ipc.TypeLimit, 100, 5)) // fills order 1
	o := h.Apply(ipc.Cmd{Op: ipc.OpModify, OrderID: 1, SymbolID: 0, Price: 110, Qty: 5})
	if o.Status != ipc.StatusRejected || o.Err != ErrOrderProcessed {
		t.Fatalf("modify executed: %+v, want rejected/ErrOrderProcessed", o)
	}
}

func TestRingHandlerUnknownSymbol(t *testing.T) {
	h, _ := newTestHandler(t)
	o := h.Apply(ipc.Cmd{Op: ipc.OpNew, SymbolID: 9, Side: ipc.SideBuy, Typ: ipc.TypeLimit, Qty: 1})
	if o.Status != ipc.StatusRejected || o.Err != ErrUnknownSymbol {
		t.Fatalf("unknown symbol: %+v", o)
	}
}

func TestRingHandlerUnknownOp(t *testing.T) {
	h, _ := newTestHandler(t)
	o := h.Apply(ipc.Cmd{Op: ipc.OpType(99), SymbolID: 0})
	if o.Status != ipc.StatusRejected || o.Err != ErrUnknownOp {
		t.Fatalf("unknown op: %+v", o)
	}
}
