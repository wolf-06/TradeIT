package repo

import (
	"TradeIT/internal/models"
	"TradeIT/services/user-order-service/producer"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"
)

// ringProducer is the shared-memory ingest producer. It is the only order input path:
// orders are published to the ring (durable in the producer WAL) and projected into the
// database by the projection service.
var ringProducer *producer.Producer

// SetRingProducer installs the shared-memory producer.
func SetRingProducer(p *producer.Producer) { ringProducer = p }

// RingEnabled reports whether the shared-memory ingest path is active.
func RingEnabled() bool { return ringProducer != nil }

// statusCache is the read-only order-status cache the projection service populates.
var statusCache = redis.NewClient(&redis.Options{
	Addr:     os.Getenv("redis_addr"),
	Password: os.Getenv("redis_password"),
})

// orderStatusView is the wire shape returned by the per-order status endpoint, matching
// what the projection service caches.
type orderStatusView struct {
	Status   string `json:"status"`
	Remq     int    `json:"remq"`
	AvgPrice int64  `json:"avgPrice"`
}

// GetOrderStatus returns one order's status. It reads the cache first and falls back to
// the database on a miss (repopulating the cache). The database is the source of truth;
// the cache is best-effort.
func GetOrderStatus(db *gorm.DB, orderID uint64) ([]byte, bool, error) {
	ctx := context.Background()
	key := "order:" + strconv.FormatUint(orderID, 10)
	if b, err := statusCache.Get(ctx, key).Bytes(); err == nil {
		return b, true, nil
	}
	var o models.Order
	if err := db.Where("id = ?", orderID).First(&o).Error; err != nil {
		return nil, false, err
	}
	b, err := json.Marshal(orderStatusView{Status: o.Status, Remq: o.Remq, AvgPrice: o.AvgPrice})
	if err != nil {
		return nil, false, err
	}
	statusCache.Set(ctx, key, b, 30*time.Minute)
	return b, true, nil
}

// CreateOrderRing submits an order through the shared-memory ring and returns its id and
// per-symbol sequence. The producer WAL is the durability point, so no synchronous DB
// write happens here; the order row is written later by the projection service.
func CreateOrderRing(o models.MetaOrder) (producer.SubmittedOrder, error) {
	sid, ok := ringProducer.SymbolID(o.Stock)
	if !ok {
		return producer.SubmittedOrder{}, fmt.Errorf("unknown symbol %q", o.Stock)
	}
	out, err := ringProducer.SubmitOrders([]producer.OrderItem{{Order: o, SymbolID: sid}})
	if err != nil {
		return producer.SubmittedOrder{}, err
	}
	return out[0], nil
}

// symbolID resolves a symbol name to its id for the ring path.
func symbolID(stock string) (uint32, error) {
	sid, ok := ringProducer.SymbolID(stock)
	if !ok {
		return 0, fmt.Errorf("unknown symbol %q", stock)
	}
	return sid, nil
}

// CancelOrderRing publishes an order cancellation through the ring.
func CancelOrderRing(orderID uint64, stock string) (producer.SubmittedOrder, error) {
	sid, err := symbolID(stock)
	if err != nil {
		return producer.SubmittedOrder{}, err
	}
	seq, err := ringProducer.Cancel(orderID, sid)
	if err != nil {
		return producer.SubmittedOrder{}, err
	}
	return producer.SubmittedOrder{OrderID: orderID, Seq: seq}, nil
}

// ReduceQtyRing publishes an in-place quantity reduction (new total quantity).
func ReduceQtyRing(orderID uint64, stock string, newQty int32) (producer.SubmittedOrder, error) {
	sid, err := symbolID(stock)
	if err != nil {
		return producer.SubmittedOrder{}, err
	}
	seq, err := ringProducer.ReduceQty(orderID, sid, newQty)
	if err != nil {
		return producer.SubmittedOrder{}, err
	}
	return producer.SubmittedOrder{OrderID: orderID, Seq: seq}, nil
}

// ModifyOrderRing publishes a combined price/quantity change (new total quantity).
func ModifyOrderRing(orderID uint64, stock string, newPrice int64, newQty int32) (producer.SubmittedOrder, error) {
	sid, err := symbolID(stock)
	if err != nil {
		return producer.SubmittedOrder{}, err
	}
	seq, err := ringProducer.Modify(orderID, sid, newPrice, newQty)
	if err != nil {
		return producer.SubmittedOrder{}, err
	}
	return producer.SubmittedOrder{OrderID: orderID, Seq: seq}, nil
}

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
