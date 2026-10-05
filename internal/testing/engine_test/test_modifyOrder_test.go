package engine_test

import (
	"fmt"
	"testing"

	"TradeIT/internal/models"

	"github.com/stretchr/testify/assert"
)

func TestNormalQuantityModification(t *testing.T) {
	ob := setupOrderbook()
	//ob.Lock()
	ob.InsertOrder(models.Order{MetaOrder: models.MetaOrder{Id: 1, Order_Type: "limit", Side: "sell", Price: 100, Quantity: 5, Status: "pending"}, Remq: 5})
	fmt.Println("Inserted 1")
	// Insert a buy order at price 110, quantity 10
	//ob.InsertOrder(models.Order{MetaOrder: models.MetaOrder{Id: 2, Order_Type: "limit", Side: "buy", Price: 110, Quantity: 10, Status: "pending"}, Remq: 10})
	//ob.Unlock()
	// Matching engine should fully match both orders
	ob.Matcher(models.Order{MetaOrder: models.MetaOrder{Id: 2, Order_Type: "limit", Side: "buy", Price: 110, Quantity: 10, Status: "pending"}, Remq: 10})
	err := ob.ModifyQuantity(2, -5)
	assert.EqualError(t, err, "ORDER_INVALID_QUANTITY")
	err = ob.ModifyQuantity(2, -5)
	assert.EqualError(t, err, "ORDER_INVALID_QUANTITY")
	err = ob.ModifyQuantity(2, 1)
	assert.EqualError(t, err, "ORDER_PARTIAL_DECREASE_DENIED")
	err = ob.ModifyQuantity(2, 15)
	assert.NoError(t, err, "Updation is not working properly")
	assert.False(t, ob.GetOrderTable()[2] == nil, "order has been removed")
	assert.True(t, ob.GetOrderTable()[2].Order_.Quantity == 15, "Quantity didn't modify properly")
	assert.True(t, ob.GetOrderTable()[2].Order_.Remq == 10, "Remq didn't modify properly")
	ob.Matcher(models.Order{MetaOrder: models.MetaOrder{Id: 1, Order_Type: "limit", Side: "sell", Price: 110, Quantity: 10, Status: "pending"}, Remq: 10})
	assert.Equal(t, int64(2), ob.GetTradeCount(), "one trade should have occurred")
	assert.False(t, ob.GetOrderTable()[1] != nil, "sell order must be removed after fill")
	assert.False(t, ob.GetOrderTable()[2] != nil, "buy order must be removed after fill")
	ob.DisplayResult()
}

func TestNormalPriceModification(t *testing.T) {
	ob := setupOrderbook()
	//ob.Lock()
	ob.InsertOrder(models.Order{MetaOrder: models.MetaOrder{Id: 1, Order_Type: "limit", Side: "sell", Price: 100, Quantity: 5, Status: "pending"}, Remq: 5})
	//ob.Unlock()
	ob.Matcher(models.Order{MetaOrder: models.MetaOrder{Id: 2, Order_Type: "limit", Side: "buy", Price: 90, Quantity: 5, Status: "pending"}, Remq: 5})
	err := ob.ModifyPrice(2, -90)
	assert.EqualError(t, err, "ORDER_INVALID_PRICE")
	err = ob.ModifyPrice(2, 0)
	assert.EqualError(t, err, "ORDER_INVALID_PRICE")
	err = ob.ModifyPrice(2, 95)
	assert.NoError(t, err, "price updation logic has issues")
	err = ob.ModifyPrice(2, 96)
	assert.NoError(t, err, "price updation logic has issues")
	err = ob.ModifyPrice(1, 93)
	assert.NoError(t, err, "price updation logic has issues")
	assert.Equal(t, int64(1), ob.GetTradeCount(), "one trade should have occurred")
	assert.False(t, ob.GetOrderTable()[1] != nil, "sell order must be removed after fill")
	assert.False(t, ob.GetOrderTable()[2] != nil, "buy order must be removed after fill")
	err = ob.ModifyPrice(2, 0)
	assert.EqualError(t, err, "ORDER_ALREADY_PROCESSED")
}

func TestBothPriceAndQuantityModification(t *testing.T) {
	ob := setupOrderbook()
	//ob.Lock()
	ob.InsertOrder(models.Order{MetaOrder: models.MetaOrder{Id: 1, Order_Type: "limit", Side: "sell", Price: 100, Quantity: 15, Status: "pending"}, Remq: 10})
	//ob.Unlock()
	ob.Matcher(models.Order{MetaOrder: models.MetaOrder{Id: 2, Order_Type: "limit", Side: "buy", Price: 90, Quantity: 10, Status: "pending"}, Remq: 10})
	err := ob.ModifyPrice(2, 95)
	assert.NoError(t, err, "price updation logic has issues")
	err = ob.ModifyQuantity(1, 10)
	assert.NoError(t, err, "quantity updation logic has issues")
	err = ob.ModifyPrice(1, 93)
	assert.NoError(t, err, "price updation logic has issues")
	assert.Equal(t, int64(1), ob.GetTradeCount(), "one trade should have occurred")
	assert.False(t, ob.GetOrderTable()[1] != nil, "sell order must be removed after fill")
	assert.False(t, ob.GetOrderTable()[2] == nil, "buy order must be removed after fill")
	fmt.Println(len(ob.GetSellOrders()))
	ob.Matcher(models.Order{MetaOrder: models.MetaOrder{Id: 3, Order_Type: "limit", Side: "buy", Price: 110, Quantity: 10, Status: "pending"}, Remq: 10})
	ob.Matcher(models.Order{MetaOrder: models.MetaOrder{Id: 4, Order_Type: "limit", Side: "sell", Price: 80, Quantity: 100, Status: "pending"}, Remq: 100})
	ob.DisplayResult()

}
