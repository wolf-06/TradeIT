package engine

import (
	"TradeIT/internal/database"
	"TradeIT/internal/models"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"
)

type OrderEngine struct {
	ledger          *Ledger
	ord_redisClient *redis.Client
	mod_redisClient *redis.Client
	db              *gorm.DB
}

// cancellation/modification are shared between the order worker (reads) and the
// cancellation worker (writes), so they must be guarded: concurrent map access would
// otherwise abort the whole process.
var (
	stateMu      sync.Mutex
	cancellation = make(map[uint64]struct{})
	modification = make(map[uint64]struct{})
)

func isCancellationPending(orderId uint64) bool {
	stateMu.Lock()
	defer stateMu.Unlock()
	_, exists := cancellation[orderId]
	return exists
}

func markCancellationPending(orderId uint64) {
	stateMu.Lock()
	defer stateMu.Unlock()
	cancellation[orderId] = struct{}{}
}

func InitOrderEngine() *OrderEngine {
	InitTrade()
	TestDB()
	return &OrderEngine{
		ledger:          InitLedger(),
		ord_redisClient: GetEngineClient(0),
		mod_redisClient: GetEngineClient(1),
		db:              database.SetDB(),
	}
}

func (engine *OrderEngine) ProcessOrderQueue() {
	var order models.Order
	fmt.Printf("Starting the OrderQueue\n")
	for {
		queueData, err := engine.ord_redisClient.BLPop(context.Background(), 0, "IncomingOrder").Result()
		if err != nil {
			fmt.Println("Error in processing the order queue: ", err)
			continue
		}
		err = json.Unmarshal([]byte(queueData[1]), &order)
		if err != nil {
			fmt.Println("Error in umarshalling the order from the redis server: ", err)
		}
		if isCancellationPending(order.Id) {
			continue
		}
		// fmt.Println("processing order of id: ", order.Id)
		err = engine.ledger.ProcessOrder(order)
		if err != nil {
			fmt.Println("Error has occured in the matcher: ", err)
			//resolve the error
		}
		// order = models.Order{} // resets the data, to avoid redundancy
	}
}

func (engine *OrderEngine) ProcessCancellationQueue() {
	for {
		//recieves the orders that are too be cancelled.
		// fmt.Println("Waiting for the cancellation request.")
		queueData, err := engine.mod_redisClient.BLPop(context.Background(), 0, "cancel").Result()
		// fmt.Println("Recieved Request")
		if err != nil {
			log.Println("Error in processing the cancellation queue: ", err)
			continue
		}
		// fmt.Println(queueData)
		data := strings.Split(string(queueData[1]), ":")
		orderid, _ := strconv.ParseUint(data[0], 10, 64)
		stock := ""
		if len(data) > 1 {
			stock = data[1]
		}

		reply := engine.processCancellationRequest(stock, orderid)

		// The user-order service blocks on this reply key (see repo.CancelOrder) and
		// interprets "nil"/"order not reached the orderbook" as success, anything else
		// as an error message. Nothing used to be published here, so every cancellation
		// request waited for a reply that never arrived.
		replyKey := "cancel:" + strconv.FormatUint(orderid, 10)
		if err := engine.mod_redisClient.RPush(context.Background(), replyKey, reply).Err(); err != nil {
			log.Println("Error in replying to the cancellation request for order ", orderid, ": ", err)
		}
	}
}

// processCancellationRequest performs a cancellation request and returns the payload
// the caller expects on the reply queue.
func (engine *OrderEngine) processCancellationRequest(stock string, orderid uint64) string {
	// A second request for an order that was already handled must fail, otherwise
	// cancelling twice silently reports success.
	if isCancellationPending(orderid) {
		return "ORDER_ALREADY_CANCELLED"
	}

	result := engine.ledger.CancelOrder(stock, orderid)
	switch {
	case result == nil:
		markCancellationPending(orderid)
		log.Println("Cancel success: Order cancelled.")
		return "nil"
	case result.Error() == "ORDER_NOT_IN_MATCHER":
		if checkTrade(orderid) {
			// the order was already executed, there is nothing left to cancel
			return "ORDER_EXECUTED"
		}
		// The order has not reached the order book yet: remember the request so the
		// incoming order is dropped when it arrives.
		markCancellationPending(orderid)
		// fmt.Println("Cancelled Successfully")
		return "order not reached the orderbook"
	default:
		log.Println("Cancel failed: ", result.Error())
		return result.Error()
	}
}

func (engine *OrderEngine) ProccessQuantityModification() {
	// fmt.Println("---Starting the Quantity Modification Request Queue---")
	for {
		queueData, err := engine.mod_redisClient.BLPop(context.Background(), 0, "PriceModifyQueue").Result()
		// fmt.Println("Recieved Request")
		if err != nil {
			log.Println("Error in processing the cancellation queue: ", err)
			continue
		}
		// fmt.Println(queueData)
		data := strings.Split(string(queueData[1]), ":")
		orderid, _ := strconv.ParseUint(data[0], 10, 64)

		_ = engine.ledger.CancelOrder(data[1], orderid)
	}
}
