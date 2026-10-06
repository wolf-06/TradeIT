package engine

import (
	"os"
	"testing"

	"TradeIT/internal/models"
)

// TestMain disables persistence for this package's tests: the matcher spawns async
// trade/status writers, and with persistence off they return immediately, so the ladder
// tests need no database.
func TestMain(m *testing.M) {
	PersistenceEnabled = false
	InitTrade()
	os.Exit(m.Run())
}

func limitOrder(id uint64, side string, price int64, qty int) models.Order {
	return models.Order{
		MetaOrder: models.MetaOrder{
			Id: id, Order_Type: "limit", Side: side, Stock: "TEST",
			Price: price, Quantity: qty, Status: "pending",
		},
		Remq: qty,
	}
}

func marketOrder(id uint64, side string, qty int) models.Order {
	return models.Order{
		MetaOrder: models.MetaOrder{
			Id: id, Order_Type: "market", Side: side, Stock: "TEST",
			Quantity: qty, Status: "pending",
		},
		Remq: qty,
	}
}

// TestLadderGrowsAcrossWidePriceRange forces the array window to grow and checks that the
// price that existed before growth is still reachable and is still the best ask.
func TestLadderGrowsAcrossWidePriceRange(t *testing.T) {
	ob := InitOrderBook()
	if err := ob.InsertOrder(limitOrder(1, "sell", 100, 5)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := ob.InsertOrder(limitOrder(2, "sell", 100000, 5)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := len(ob.GetSellOrders()); got != 2 {
		t.Fatalf("want 2 sell levels, got %d", got)
	}
	if err := ob.Matcher(marketOrder(3, "buy", 5)); err != nil {
		t.Fatalf("match: %v", err)
	}
	if ob.GetTradeCount() != 1 {
		t.Fatalf("want 1 trade, got %d", ob.GetTradeCount())
	}
	levels := ob.GetSellOrders()
	if len(levels) != 1 {
		t.Fatalf("want 1 sell level left, got %d (%v)", len(levels), levels)
	}
	if _, ok := levels[100000]; !ok {
		t.Fatalf("expected the 100000 level to remain, got %v", levels)
	}
}

// TestAskBestPriceAdvancesOnFullFill drains the best ask levels one by one.
func TestAskBestPriceAdvancesOnFullFill(t *testing.T) {
	ob := InitOrderBook()
	ob.InsertOrder(limitOrder(1, "sell", 100, 1))
	ob.InsertOrder(limitOrder(2, "sell", 101, 1))
	ob.InsertOrder(limitOrder(3, "sell", 102, 1))

	if err := ob.Matcher(limitOrder(4, "buy", 102, 2)); err != nil {
		t.Fatalf("match: %v", err)
	}
	if ob.GetTradeCount() != 2 {
		t.Fatalf("want 2 trades, got %d", ob.GetTradeCount())
	}
	levels := ob.GetSellOrders()
	if len(levels) != 1 {
		t.Fatalf("want only the 102 level left, got %v", levels)
	}
	if _, ok := levels[102]; !ok {
		t.Fatalf("expected the 102 level to remain, got %v", levels)
	}
	if err := ob.Matcher(marketOrder(5, "buy", 1)); err != nil {
		t.Fatalf("match: %v", err)
	}
	if ob.GetTradeCount() != 3 {
		t.Fatalf("want 3 trades, got %d", ob.GetTradeCount())
	}
	if len(ob.GetSellOrders()) != 0 {
		t.Fatalf("book should be empty, got %v", ob.GetSellOrders())
	}
}

// TestBidBestPriceAdvances checks the bid side picks the highest price and moves down.
func TestBidBestPriceAdvances(t *testing.T) {
	ob := InitOrderBook()
	ob.InsertOrder(limitOrder(1, "buy", 100, 1))
	ob.InsertOrder(limitOrder(2, "buy", 99, 1))

	if err := ob.Matcher(limitOrder(3, "sell", 99, 1)); err != nil {
		t.Fatalf("match: %v", err)
	}
	if ob.GetTradeCount() != 1 {
		t.Fatalf("want 1 trade, got %d", ob.GetTradeCount())
	}
	if _, ok := ob.GetBuyOrders()[100]; ok {
		t.Fatalf("the 100 bid should have been filled first")
	}
	if _, ok := ob.GetBuyOrders()[99]; !ok {
		t.Fatalf("the 99 bid should remain, got %v", ob.GetBuyOrders())
	}
}

// TestCancelBestLevelAdvances checks that cancelling the best ask exposes the next level.
func TestCancelBestLevelAdvances(t *testing.T) {
	ob := InitOrderBook()
	ob.InsertOrder(limitOrder(1, "sell", 100, 5))
	ob.InsertOrder(limitOrder(2, "sell", 101, 5))

	if err := ob.CancelOrder(1); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if _, ok := ob.GetSellOrders()[100]; ok {
		t.Fatalf("the 100 level should be gone, got %v", ob.GetSellOrders())
	}
	if err := ob.Matcher(marketOrder(3, "buy", 5)); err != nil {
		t.Fatalf("match: %v", err)
	}
	if ob.GetTradeCount() != 1 {
		t.Fatalf("want 1 trade, got %d", ob.GetTradeCount())
	}
	if _, ok := ob.GetSellOrders()[101]; ok {
		t.Fatalf("the 101 level should have been filled")
	}
}

// TestInsertOrderRestsWithoutMatching checks InsertOrder never matches.
func TestInsertOrderRestsWithoutMatching(t *testing.T) {
	ob := InitOrderBook()
	if err := ob.InsertOrder(limitOrder(1, "buy", 100, 5)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if ob.GetTradeCount() != 0 {
		t.Fatalf("InsertOrder must not trade, got %d", ob.GetTradeCount())
	}
	if ob.GetBuyCount() != 1 {
		t.Fatalf("want buyCount 1, got %d", ob.GetBuyCount())
	}
	if _, ok := ob.GetOrderTable()[1]; !ok {
		t.Fatalf("order 1 should be in the order table")
	}
}

// TestPartialFillKeepsRemainder checks a partial fill leaves the remainder resting.
func TestPartialFillKeepsRemainder(t *testing.T) {
	ob := InitOrderBook()
	ob.InsertOrder(limitOrder(1, "sell", 100, 5))
	if err := ob.Matcher(limitOrder(2, "buy", 100, 8)); err != nil {
		t.Fatalf("match: %v", err)
	}
	node, ok := ob.GetOrderTable()[2]
	if !ok {
		t.Fatalf("partially filled buy should still rest")
	}
	if node.Order_.Remq != 3 {
		t.Fatalf("want remaining 3, got %d", node.Order_.Remq)
	}
	if _, ok := ob.GetBuyOrders()[100]; !ok {
		t.Fatalf("the 100 bid level should remain")
	}
}

func TestMidpointRoundsHalfUp(t *testing.T) {
	cases := []struct{ a, b, want int64 }{
		{100, 100, 100},
		{100, 101, 101},
		{101, 102, 102},
		{100, 103, 102},
	}
	for _, c := range cases {
		if got := midpoint(c.a, c.b); got != c.want {
			t.Errorf("midpoint(%d,%d) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}
