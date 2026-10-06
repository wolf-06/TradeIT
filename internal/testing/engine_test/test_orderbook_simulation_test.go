package engine_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"TradeIT/internal/models"
	"TradeIT/services/engine-service/engine"

	"github.com/stretchr/testify/assert"
)

// setupOrderbook initializes an empty *engine.Orderbook with no orders.
func setupOrderbook() *engine.Orderbook {
	return engine.InitOrderBook()
}

// TestNormalMatch tests basic buy–sell matching and full fills.
func TestNormalMatch(t *testing.T) {
	ob := setupOrderbook()
	// Insert a sell order at price 100, quantity 10
	ob.InsertOrder(models.Order{MetaOrder: models.MetaOrder{Id: 1, Order_Type: "limit", Side: "sell", Price: 100, Quantity: 10, Status: "pending"}, Remq: 10})
	fmt.Println("Inserted 1")
	// Insert a buy order at price 110, quantity 10
	ob.Matcher(models.Order{MetaOrder: models.MetaOrder{Id: 2, Side: "buy", Order_Type: "limit", Price: 110, Quantity: 10, Status: "pending"}, Remq: 10})
	assert.Equal(t, int64(1), ob.GetTradeCount(), "one trade should have occurred")
	assert.False(t, ob.GetOrderTable()[1] != nil, "sell order must be removed after fill")
	assert.False(t, ob.GetOrderTable()[2] != nil, "buy order must be removed after fill")
}

// TestPartialFill tests partial fills and remaining quantities.
func TestPartialFill(t *testing.T) {
	ob := setupOrderbook()
	// Insert a sell order with quantity 5
	ob.InsertOrder(models.Order{MetaOrder: models.MetaOrder{Id: 3, Order_Type: "limit", Side: "sell", Price: 200, Quantity: 5, Status: "pending"}, Remq: 5})
	ob.IncSellCount()
	// Insert a buy order with quantity 10
	ob.InsertOrder(models.Order{MetaOrder: models.MetaOrder{Id: 4, Order_Type: "limit", Side: "buy", Price: 200, Quantity: 10, Status: "pending"}, Remq: 10})
	ob.IncBuyCount()
	// Matcher should partially fill the buy order
	ob.Matcher(models.Order{MetaOrder: models.MetaOrder{Id: 4, Order_Type: "limit", Side: "buy", Price: 200, Quantity: 10, Status: "pending"}, Remq: 10})
	assert.Equal(t, int64(1), ob.GetTradeCount(), "one partial trade should occur")
	node, exists := ob.GetOrderTable()[4]
	assert.True(t, exists, "buy order should remain after partial fill")
	assert.Equal(t, 5, node.Order_.Remq, "remaining quantity should be 5")
}

// TestCancelBeforeMatch tests cancellation of orders before matching.
func TestCancelBeforeMatch(t *testing.T) {
	ob := setupOrderbook()
	err := ob.InsertOrder(models.Order{MetaOrder: models.MetaOrder{Id: 5, Order_Type: "limit", Side: "buy", Price: 150, Quantity: 5, Status: "pending"}, Remq: 5})
	assert.NoError(t, err, "invalid order type")
	ob.IncBuyCount()
	err = ob.CancelOrder(5)
	assert.NoError(t, err, "cancellation before match should not error")
	_, exists := ob.GetOrderTable()[5]
	assert.False(t, exists, "orderTable should not contain cancelled order")
	_, levelExists := ob.GetBuyOrders()[150]
	assert.False(t, levelExists, "price level must be removed when empty")
}

// TestCancelAfterPartial tests cancellation after a partial fill.
func TestCancelAfterPartial(t *testing.T) {
	ob := setupOrderbook()

	ob.InsertOrder(models.Order{MetaOrder: models.MetaOrder{Id: 6, Order_Type: "limit", Side: "sell", Price: 250, Quantity: 20, Status: "pending"}, Remq: 20})
	ob.IncSellCount()
	// Trigger partial match of 10 units
	ob.Matcher(models.Order{MetaOrder: models.MetaOrder{Id: 7, Order_Type: "limit", Side: "buy", Price: 250, Quantity: 10, Status: "pending"}, Remq: 10})
	// Now cancel remaining sell order
	err := ob.CancelOrder(6)
	assert.EqualError(t, err, "ORDER_PARTIAL_CANCELLED")
	_, exists := ob.GetOrderTable()[6]
	assert.False(t, exists, "partially filled order should be removed on cancel")
}

// TestConcurrentCancellations tests thread safety with concurrent cancellations.
func TestConcurrentCancellations(t *testing.T) {
	ob := setupOrderbook()
	// Insert many orders
	for i := uint64(100); i < 110; i++ {
		ob.InsertOrder(models.Order{MetaOrder: models.MetaOrder{Id: i, Order_Type: "limit", Side: "buy", Price: 300, Quantity: 1, Status: "pending"}, Remq: 1})
		ob.IncBuyCount()
	}
	var wg sync.WaitGroup
	for i := uint64(100); i < 110; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			err := ob.CancelOrder(id)
			assert.NoError(t, err, "concurrent cancellation should not error")
		}(i)
	}
	wg.Wait()

	for i := uint64(100); i < 110; i++ {
		_, exists := ob.GetOrderTable()[i]
		assert.False(t, exists, "all orders must be removed concurrently")
	}
}

// TestCancelNonExistent tests error on cancelling non-existent order.
func TestCancelNonExistent(t *testing.T) {
	ob := setupOrderbook()
	err := ob.CancelOrder(9999)
	assert.EqualError(t, err, "ORDER_NOT_IN_MATCHER", "should return specific error for invalid ID")
}

// TestInvalidOrderType tests unsupported order type handling.
func TestInvalidOrderType(t *testing.T) {
	ob := setupOrderbook()
	// Manually inject invalid Order_ to simulate bad data
	node := &engine.Node{Order_: models.Order{MetaOrder: models.MetaOrder{Id: 10, Order_Type: "limit", Side: "hold", Price: 100, Quantity: 1, Status: "pending"}, Remq: 1}}
	ob.GetOrderTable()[10] = node

	err := ob.CancelOrder(10)
	assert.EqualError(t, err, "ORDER_UNSUPPORTED_TYPE", "should error on invalid order side")
}

// TestStressScenario simulates rapid inserts, matches, and cancels under load.
func TestStressScenario(t *testing.T) {
	eng := engine.InitOrderEngine()
	if eng != nil {
	}
	ob := setupOrderbook()
	for i := 1; i <= 45; i++ {
		side := "buy"
		if i%2 == 0 {
			side = "sell"
		}
		ob.Matcher(models.Order{MetaOrder: models.MetaOrder{Id: uint64(1000 + i), Order_Type: "limit", Side: side, Price: int64(100 + i%5), Quantity: 1, Status: "pending"}, Remq: 1})
		// if i%15 == 0 {
		// 	_ = ob.CancelOrder(uint64(1000 + i))
		// }
	}
	// Allow matcher operations to complete
	time.Sleep(100 * time.Millisecond)
	// Verify that no panics occurred and internal counts are non-negative
	ob.DisplayResult()
	assert.True(t, ob.GetBuyCount() >= 0, "buyCount must be non-negative")
	assert.True(t, ob.GetSellCount() >= 0, "sellCount must be non-negative")
}
