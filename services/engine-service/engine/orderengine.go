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

// Ledger returns the engine's order-book ledger. The ring consumer path uses it as the
// book that commands from the shared-memory ring are applied to.
func (engine *OrderEngine) Ledger() *Ledger { return engine.ledger }

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
		err = engine.ledger.ProcessOrder(order)
		if err != nil {
			fmt.Println("Error has occured in the matcher: ", err)
			//resolve the error
		}
	}
}

func (engine *OrderEngine) ProcessCancellationQueue() {
	for {
		//recieves the orders that are too be cancelled.
		queueData, err := engine.mod_redisClient.BLPop(context.Background(), 0, "cancel").Result()
		if err != nil {
			log.Println("Error in processing the cancellation queue: ", err)
			continue
		}
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
		return "order not reached the orderbook"
	default:
		log.Println("Cancel failed: ", result.Error())
		return result.Error()
	}
}

func (engine *OrderEngine) ProccessQuantityModification() {
	for {
		queueData, err := engine.mod_redisClient.BLPop(context.Background(), 0, "PriceModifyQueue").Result()
		if err != nil {
			log.Println("Error in processing the price modification queue: ", err)
			continue
		}
		// The user-order service publishes "orderId:stock:price", where price is the
		// scaled integer form (see repo.ModifyPrice / models.PriceToInt).
		data := strings.Split(string(queueData[1]), ":")
		if len(data) < 3 {
			log.Println("Malformed price modification request: ", string(queueData[1]))
			continue
		}
		orderid, err := strconv.ParseUint(data[0], 10, 64)
		if err != nil {
			log.Println("Invalid order id in price modification request: ", data[0])
			continue
		}
		stock := data[1]
		newPrice, err := strconv.ParseInt(data[2], 10, 64)
		if err != nil {
			log.Println("Invalid price in price modification request: ", data[2])
			continue
		}

		result := engine.ledger.ModifyPrice(stock, orderid, newPrice)

		// The user-order service blocks on this reply key (see repo.ModifyPrice) and
		// interprets "nil" as success, anything else as an error message.
		reply := "nil"
		if result != nil {
			reply = result.Error()
		}
		replyKey := "PriceModifyQueue:" + strconv.FormatUint(orderid, 10)
		if err := engine.mod_redisClient.RPush(context.Background(), replyKey, reply).Err(); err != nil {
			log.Println("Error in replying to the price modification request for order ", orderid, ": ", err)
		}
	}
}
