package engine

import (
	"TradeIT/internal/database"
	"TradeIT/internal/models"
	// "fmt" - every debug print in this file is commented out
	"log"

	"github.com/svicknesh/tsid"
	"gorm.io/gorm"
)

var trade_db *gorm.DB
var ts *tsid.TSID

// PersistenceEnabled controls whether the matcher's asynchronous bookkeeping - trade
// registration and order status updates - is written to PostgreSQL.
//
// It defaults to true, which is the production behaviour. Offline tooling such as the
// matching-engine benchmark (cmd/engine-benchmark) sets it to false so the pure
// in-memory matching path can be measured without the database writes dominating the
// numbers (and without requiring a database connection at all).
//
// NOTE: Matcher spawns these writers in goroutines that outlive the call, so do not flip
// this back to true while any of them may still be in flight - a late writer would then
// reach a database handle that was never initialised.
var PersistenceEnabled = true

func InitTrade() {
	trade_db = database.SetDB()
	ts = tsid.NewDefault()
}

func TestDB() bool {
	return (trade_db == nil)
}

func checkTrade(orderid uint64) bool {
	temp := tradePool.acquireTrade()
	var status = true
	result := trade_db.Where("bid_order_id= ? or ask_order_id = ?", orderid, orderid).Find(&temp)
	if result.Error != nil {
		log.Println("error occured in databse query: ", result.Error)
		tradePool.releaseTrade(temp)
		// Fail closed: the lookup failed, so report the order as traded rather than
		// letting an executed order be reported as cancelled. (This used to be a
		// log.Fatalf, which killed the whole engine on a database hiccup.)
		return true
	}
	if result.RowsAffected == 0 {
		status = false
		// fmt.Println("No trades Found")
	}
	tradePool.releaseTrade(temp)
	return status
}

// registers the trade in trade database
func registerTrades(trade *models.TradeDetails) {
	if !PersistenceEnabled {
		return
	}
	trade.TradeId = ts.Generate().Number
	// fmt.Println(trade)
	// fmt.Println("creating trade for ", trade.TradeId)
	if err := trade_db.Create(&trade).Error; err != nil {
		log.Println("Error in creating the entry in trade database: ", err)
	}
}

// updates the order details in main order database
func updateOrderStatusExecuted(order models.Order) {
	if !PersistenceEnabled {
		return
	}
	order.Status = "executed"
	if err := trade_db.Save(&order).Error; err != nil {
		log.Println("Error while updating the executed order in databse: ", err)
	}
}

func updateOrderStatusUpdated(order models.Order) {
	if !PersistenceEnabled {
		return
	}
	order.Status = "filled"
	order.Quantity = order.Quantity - order.Remq
	if err := trade_db.Save(&order).Error; err != nil {
		log.Println("error while updating the partially filled order: ")
	}
}

func updateOrderStatusCancelled(order models.Order) {
	if !PersistenceEnabled {
		return
	}
	order.Status = "Cancelled"
	if err := trade_db.Save(&order).Error; err != nil {
		log.Println("error while updating the partially filled order: ")
	}
}
