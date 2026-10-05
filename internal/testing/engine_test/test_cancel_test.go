package engine_test

import (
	"TradeIT/internal/models"
	"TradeIT/services/engine-service/engine"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// setupOrderbook initializes an Orderbook with sample buy and sell orders.
func SetupOrderbook() *engine.Orderbook {
	ob := engine.InitOrderBook()
	ob.InsertOrder(models.Order{
		MetaOrder: models.MetaOrder{
			Id:         1,
			Order_Type: "limit",
			Side:       "buy",
			Price:      50,
			Quantity:   10,
			Status:     "pending",
		},
		Remq: 10,
	})
	ob.InsertOrder(models.Order{
		MetaOrder: models.MetaOrder{
			Id:         2,
			Order_Type: "limit",
			Side:       "sell",
			Price:      75,
			Quantity:   5,
			Status:     "pending",
		},
		Remq: 5,
	})
	fmt.Println("lock")
	return ob
}

func TestCancelExistingBuyOrder(t *testing.T) {
	ob := SetupOrderbook()
	//ob.Unlock()
	err := ob.CancelOrder(1)
	assert.NoError(t, err, "cancelling existing buy order should not error")
	_, exists := ob.GetOrderTable()[1]
	assert.False(t, exists, "orderTable must not contain cancelled buy order")
	_, priceExists := ob.GetBuyOrders()[50]
	assert.False(t, priceExists, "buy_orders map should remove price level when empty")
}

func TestCancelExistingSellOrder(t *testing.T) {
	ob := SetupOrderbook()
	//ob.Unlock()
	err := ob.CancelOrder(2)
	assert.NoError(t, err, "cancelling existing sell order should not error")
	_, exists := ob.GetOrderTable()[2]
	assert.False(t, exists, "orderTable must not contain cancelled sell order")
	_, priceExists := ob.GetSellOrders()[75]
	assert.False(t, priceExists, "sell_orders map should remove price level when empty")
}

func TestCancelNonExistentOrder(t *testing.T) {
	ob := SetupOrderbook()
	//ob.Unlock()
	err := ob.CancelOrder(999)
	expected := errors.New("ORDER_NOT_IN_MATCHER")
	assert.EqualError(t, err, expected.Error(), "cancelling non-existent order should return specific error")
}
