package repo

import (
	"TradeIT/internal/models"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"
)

// cancelReplyTimeout is how long we wait for the engine to answer a cancel/modify
// request. The literal values used before were wrong: `10` meant 10 nanoseconds (redis
// truncates to 1s) and `0` blocks forever.
const cancelReplyTimeout = 10 * time.Second

var orderPool = InitOrderPool()

// redis connector for placing order
var ord_producer = redis.NewClient(&redis.Options{
	Addr:     os.Getenv("redis_addr"),
	Password: os.Getenv("redis_password"),
	DB:       0,
})

// redis connectors for placing order modification request to order engine
var mod_producer = redis.NewClient(&redis.Options{
	Addr:     os.Getenv("redis_addr"),
	Password: os.Getenv("redis_password"),
	DB:       1,
})

var mod_client = redis.NewClient(&redis.Options{
	Addr:     os.Getenv("redis_addr"),
	Password: os.Getenv("redis_password"),
	DB:       1,
})

type Query struct {
	Parameter string `json:"parameter" binding:"required"`
	Value     any    `json:"value" binding:"required"`
}

// filterableColumns are the only columns that /order/sort may filter on. The parameter
// used to be concatenated straight into the SQL string, so anything else (e.g.
// "1=1 OR stock") allowed reading other users' orders.
var filterableColumns = map[string]struct{}{
	"id":         {},
	"user_id":    {},
	"order_type": {},
	"side":       {},
	"stock":      {},
	"price":      {},
	"avg_price":  {},
	"quantity":   {},
	"status":     {},
	"created_at": {},
	"remq":       {},
}

// IsFilterableColumn reports whether parameter is a column of the orders table that
// callers are allowed to filter on.
func IsFilterableColumn(parameter string) bool {
	_, ok := filterableColumns[parameter]
	return ok
}

func CreateOrder(db *gorm.DB, metaOrder models.MetaOrder) bool {
	Order := orderPool.AcquireOrder()
	Order.MetaOrder = metaOrder
	Order.Remq = Order.Quantity
	if err := db.Create(&Order).Error; err != nil {
		fmt.Println("Error in creating the order: ", err)
		return false
	}
	key := "IncomingOrder"
	details, err := json.Marshal(Order)
	if err != nil {
		fmt.Println("Error in marshalling for redis queue: ", err)
		return false
	}

	err = ord_producer.RPush(context.Background(), key, details).Err()
	if err != nil {
		fmt.Println("Error in pushing to redis queue: ", err)
		return false
	}
	orderPool.ReleaseOrder(Order)
	return true
}

func GetAllOrders(db *gorm.DB, user_id float64) []byte {
	var orders []models.Order
	result := db.Where("user_id = ?", user_id).Find(&orders)
	if result.Error != nil {
		fmt.Println("Error in getting the orders from database: ", result.Error)
	}
	order_json, err := json.MarshalIndent(orders, "", "")
	if err != nil {
		fmt.Println("Error in Json Marshalling: ", err)
	}
	return order_json
}

func GetOrders(db *gorm.DB, user_id float64, query Query) []byte {
	var orders []models.Order
	fmt.Println(query)
	cond := "user_id = ? AND " + query.Parameter + " =?"
	fmt.Println(cond)
	result := db.Where(cond, user_id, query.Value.(string)).Find(&orders)
	if result.Error != nil {
		fmt.Println("Error in fetching the data: ", result.Error)
		return nil
	}

	orderJson, err := json.MarshalIndent(orders, "", "")
	if err == nil {
		fmt.Println("Error in Marshalling json")
	}
	return orderJson
}

func CancelOrder(db *gorm.DB, order_id uint64) error {
	key := "cancel"
	order := orderPool.AcquireOrder()

	if result := db.Where("id=?", order_id).Find(&order); result.Error != nil {
		log.Println("error in fetching data from order database for cancelling order id: ", order_id, "\nError: ", result.Error)
		return errors.New("internal error")
	} else if result.RowsAffected == 0 {
		// Nothing to cancel; asking the engine would only produce a confusing reply.
		return errors.New("order not found")
	}
	value := strconv.FormatUint(order_id, 10) + ":" + order.Stock

	// Replies for earlier attempts (a previous run that timed out, or a retry) stay in
	// this list; drop them so we never read a stale answer to a newer request.
	replyKey := key + ":" + strconv.FormatUint(order_id, 10)
	if err := mod_client.Del(context.Background(), replyKey).Err(); err != nil {
		log.Println("could not clear the reply queue ", replyKey, ": ", err)
	}

	if err := mod_producer.RPush(context.Background(), key, value).Err(); err != nil {
		log.Println("Error in publishing a request to cancel order with id: ", order_id, "\nError: ", err)
		return errors.New("internal error")
	}

	status, err := mod_client.BLPop(context.Background(), cancelReplyTimeout, replyKey).Result()
	if err != nil || len(status) < 2 {
		log.Println("no cancellation reply from the order engine for order ", order_id, ": ", err)
		return errors.New("internal error")
	}
	if (status[1] == "nil") || status[1] == "order not reached the orderbook" {
		return nil
	}
	return errors.New(status[1])
}

func ModifyPrice(db *gorm.DB, order_id uint64, newPrice float64) error {
	key := "PriceModifyQueue"
	order := orderPool.AcquireOrder()

	if result := db.Where("id=?", order_id).Find(&order); result.Error != nil {
		log.Println("error in fetching data from order database for cancelling order id: ", order_id, "\nError: ", result.Error)
		return errors.New("internal error")
	} else if result.RowsAffected == 0 {
		return errors.New("order not found")
	}
	id_str := strconv.FormatUint(order_id, 10)
	price_str := fmt.Sprintf("%.2f", newPrice)
	value := id_str + ":" + order.Stock + ":" + price_str
	fmt.Println(value)

	// Same as CancelOrder: discard replies left over from earlier attempts first.
	replyKey := key + ":" + id_str
	if err := mod_client.Del(context.Background(), replyKey).Err(); err != nil {
		log.Println("could not clear the reply queue ", replyKey, ": ", err)
	}

	if err := mod_producer.RPush(context.Background(), key, value).Err(); err != nil {
		log.Println("Error in publishing a request to modify price of order with id: ", order_id, "\nError: ", err)
		return errors.New("internal error")
	}

	status, err := mod_client.BLPop(context.Background(), cancelReplyTimeout, replyKey).Result()
	if err != nil || len(status) < 2 {
		log.Println("no modification reply from the order engine for order ", order_id, ": ", err)
		return errors.New("internal error")
	}
	if (status[1] == "nil") || status[1] == "order not reached the orderbook" {
		return nil
	}
	return errors.New(status[1])
}
