package engine

import (
	"TradeIT/internal/database"
	"TradeIT/internal/models"
	"log"
	"sync"
	"time"

	"github.com/svicknesh/tsid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	tradeMu  sync.RWMutex
	trade_db *gorm.DB
	ts       *tsid.TSID
)

// tradeDB and tradeTS read the persistence handles under the lock: InitTrade may run more
// than once (e.g. a test calling InitOrderEngine again), and the background writers read
// these concurrently.
func tradeDB() *gorm.DB {
	tradeMu.RLock()
	defer tradeMu.RUnlock()
	return trade_db
}

func tradeTS() *tsid.TSID {
	tradeMu.RLock()
	defer tradeMu.RUnlock()
	return ts
}

// PersistenceEnabled controls whether the matcher's bookkeeping - trade registration and
// order status updates - is written to PostgreSQL.
//
// It defaults to true, which is the production behaviour. Offline tooling such as the
// matching-engine benchmark (cmd/engine-benchmark) sets it to false so the pure in-memory
// matching path can be measured without the database writes dominating the numbers (and
// without requiring a database connection at all). With it off the enqueue helpers return
// immediately (recycling trade records) and no writer ever touches the database.
var PersistenceEnabled = true

// The matcher does not write to PostgreSQL directly. It appends events to two buffered
// queues and background writers drain them in batches - a write-ahead-log style hand-off.
// This keeps the matching path free of per-fill goroutines and per-fill round trips, and
// turns many small INSERT/UPDATE statements into a few multi-row ones.
//
// The queues are in-memory: they decouple and batch, but they are not crash-durable. A
// crash loses whatever is still queued. Backing them with Redis or disk would make them
// durable, at the cost of a synchronous append on the match path.
const (
	persistQueueSize   = 1 << 16
	persistBatchSize   = 256
	persistFlushPeriod = 5 * time.Millisecond
)

type orderStatusUpdate struct {
	order  models.Order
	status string
}

var (
	tradeQueue chan *models.TradeDetails
	orderQueue chan orderStatusUpdate

	persistStartOnce sync.Once
)

func InitTrade() {
	tradeMu.Lock()
	trade_db = database.SetDB()
	ts = tsid.NewDefault()
	tradeMu.Unlock()
	persistStartOnce.Do(func() {
		tradeQueue = make(chan *models.TradeDetails, persistQueueSize)
		orderQueue = make(chan orderStatusUpdate, persistQueueSize)
		go tradeWriter()
		go orderWriter()
	})
}

func TestDB() bool {
	return (tradeDB() == nil)
}

func checkTrade(orderid uint64) bool {
	temp := tradePool.acquireTrade()
	var status = true
	result := tradeDB().Where("bid_order_id= ? or ask_order_id = ?", orderid, orderid).Find(&temp)
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

// enqueueTrade hands a filled trade to the batched writer. It owns the pooled record from
// this point: with persistence off it is recycled immediately, otherwise the writer
// recycles it once the batch has been written.
func enqueueTrade(trade *models.TradeDetails) {
	if !PersistenceEnabled {
		tradePool.releaseTrade(trade)
		return
	}
	tradeQueue <- trade
}

func enqueueOrderStatus(order models.Order, status string) {
	if !PersistenceEnabled {
		return
	}
	orderQueue <- orderStatusUpdate{order: order, status: status}
}

// updates the order details in main order database
func updateOrderStatusExecuted(order models.Order) {
	enqueueOrderStatus(order, "executed")
}

func updateOrderStatusUpdated(order models.Order) {
	enqueueOrderStatus(order, "filled")
}

func updateOrderStatusCancelled(order models.Order) {
	enqueueOrderStatus(order, "Cancelled")
}

// tradeWriter drains the trade queue and writes batches of trades with one statement.
func tradeWriter() {
	batch := make([]*models.TradeDetails, 0, persistBatchSize)
	ticker := time.NewTicker(persistFlushPeriod)
	defer ticker.Stop()
	for {
		select {
		case t := <-tradeQueue:
			batch = append(batch, t)
			if len(batch) >= persistBatchSize {
				flushTrades(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				flushTrades(batch)
				batch = batch[:0]
			}
		}
	}
}

func flushTrades(batch []*models.TradeDetails) {
	generator := tradeTS()
	for _, t := range batch {
		t.TradeId = generator.Generate().Number
	}
	if err := tradeDB().CreateInBatches(batch, len(batch)).Error; err != nil {
		log.Println("Error in creating trade entries: ", err)
	}
	for _, t := range batch {
		tradePool.releaseTrade(t)
	}
}

// orderWriter drains the order-status queue and upserts batches of orders.
func orderWriter() {
	batch := make([]models.Order, 0, persistBatchSize)
	ticker := time.NewTicker(persistFlushPeriod)
	defer ticker.Stop()
	for {
		select {
		case u := <-orderQueue:
			u.order.Status = u.status
			if u.status == "filled" {
				u.order.Quantity = u.order.Quantity - u.order.Remq
			}
			batch = append(batch, u.order)
			if len(batch) >= persistBatchSize {
				flushOrders(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				flushOrders(batch)
				batch = batch[:0]
			}
		}
	}
}

func flushOrders(batch []models.Order) {
	// Collapse multiple updates for the same order id within one batch, keeping the last:
	// Postgres rejects an ON CONFLICT that touches the same row twice in one statement.
	last := make(map[uint64]int, len(batch))
	for i := range batch {
		last[batch[i].Id] = i
	}
	dedup := make([]models.Order, 0, len(last))
	for i := range batch {
		if last[batch[i].Id] == i {
			dedup = append(dedup, batch[i])
		}
	}
	if err := tradeDB().Clauses(clause.OnConflict{UpdateAll: true}).CreateInBatches(&dedup, len(dedup)).Error; err != nil {
		log.Println("Error in updating order statuses: ", err)
	}
}
