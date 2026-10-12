package services

import (
	"TradeIT/internal/database"
	"TradeIT/internal/models"
	repo "TradeIT/services/user-order-service/repo"
	"TradeIT/services/user-order-service/producer"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// placeOrderRequest is the wire form of POST /order/create. Price is a decimal on the
// wire; the service converts it to the engine's scaled integer representation so the
// matching service always receives integer prices.
type placeOrderRequest struct {
	Order_Type string  `json:"orderType" binding:"required,oneof=limit market"`
	Side       string  `json:"type" binding:"required,oneof=buy sell"`
	Stock      string  `json:"stock" binding:"required"`
	Price      float64 `json:"price" binding:"required_if=Order_Type limit,omitempty,gt=0"`
	Quantity   int     `json:"quantity" binding:"required,gt=0"`
}

// cancelRequest is the wire form of POST /order/cancel.
type cancelRequest struct {
	OrderID uint64 `json:"orderid" binding:"required"`
	Stock   string `json:"stock" binding:"required"`
}

// modifyRequest is the wire form of POST /order/modify. A price of 0 (or omitted) is an
// in-place quantity reduction (keeping time priority); a non-zero price is a combined
// price/quantity change.
type modifyRequest struct {
	OrderID  uint64  `json:"orderid" binding:"required"`
	Stock    string  `json:"stock" binding:"required"`
	Price    float64 `json:"price"`
	Quantity int     `json:"quantity" binding:"required,gt=0"`
}

type OrderService struct {
	db *gorm.DB
	op *OrderPool
}

func InitOrderService() *OrderService {
	return &OrderService{
		db: database.SetDB(),
		op: InitOrderPool(),
	}
}

// ErrInvalidRequest signals that the request was already rejected (a response has been
// written) so the caller must not write another.
var ErrInvalidRequest = errors.New("invalid request")

// parseOrder reads and validates a place-order request. On success it returns a pooled
// order the caller must release; on failure it has already written a 400 and returns
// ErrInvalidRequest.
func (os *OrderService) parseOrder(c *gin.Context) (*models.MetaOrder, error) {
	var req placeOrderRequest
	if err := c.BindJSON(&req); err != nil {
		// NOTE: this used to be log.Fatalln, which killed the whole service on any
		// malformed payload. Reject the request instead.
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return nil, ErrInvalidRequest
	}
	order := os.op.acquireOrder()
	// The order always belongs to the authenticated caller; a user_id supplied in the
	// body is ignored so nobody can place orders on another account.
	user_id, _ := c.Get("userid")
	order.User_id = int(user_id.(float64))
	order.Order_Type = req.Order_Type
	order.Side = req.Side
	order.Stock = req.Stock
	// Convert the decimal wire price into the engine's integer representation. This is
	// the single conversion point: everything downstream (DB, queue, matcher) works with
	// scaled integers only.
	order.Price = models.PriceToInt(req.Price)
	order.Quantity = req.Quantity
	order.Created_at = time.Now()
	order.Status = "pending"
	return order, nil
}

func (os *OrderService) CreateOrderService(c *gin.Context) bool {
	order, err := os.parseOrder(c)
	if err != nil {
		return false
	}
	defer os.op.releaseOrder(order)
	return repo.CreateOrder(os.db, *order)
}

// RingEnabled reports whether the shared-memory ingest path is active.
func (os *OrderService) RingEnabled() bool { return repo.RingEnabled() }

// PlaceOrderRing places an order through the shared-memory ring and returns its id and
// per-symbol sequence.
func (os *OrderService) PlaceOrderRing(c *gin.Context) (producer.SubmittedOrder, error) {
	order, err := os.parseOrder(c)
	if err != nil {
		return producer.SubmittedOrder{}, err
	}
	defer os.op.releaseOrder(order)
	return repo.CreateOrderRing(*order)
}

func (os *OrderService) GetAllOrderService(c *gin.Context) []byte {
	user_id, _ := c.Get("userid")
	json := repo.GetAllOrders(os.db, user_id.(float64))
	return json
}

func (os *OrderService) GetOrderByParameterService(c *gin.Context) []byte {
	var Query repo.Query
	user_id, _ := c.Get("userid")
	if err := c.BindJSON(&Query); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return nil
	}
	// repo.GetOrders concatenates Query.Parameter into the SQL and asserts Value to string;
	// catch the bad type here so it becomes a 400 instead of a panic/500.
	if _, ok := Query.Value.(string); !ok {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "value must be a string"})
		return nil
	}
	// Only the whitelisted column names may be used as a filter, otherwise the
	// parameter is an SQL injection vector.
	if !repo.IsFilterableColumn(Query.Parameter) {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "invalid filter parameter: " + Query.Parameter})
		return nil
	}
	json := repo.GetOrders(os.db, user_id.(float64), Query)
	return json
}

// GetOrderStatusService returns one order's status: the cache first (populated by the
// projection service), the database on a miss.
func (os *OrderService) GetOrderStatusService(c *gin.Context) []byte {
	orderID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "invalid order id"})
		return nil
	}
	b, found, err := repo.GetOrderStatus(os.db, orderID)
	if err != nil || !found {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"error": "order not found"})
		return nil
	}
	return b
}

// CancelOrderService publishes a cancellation. Cancel/modify require the shared-memory
// ingest path; the legacy Redis control plane is not wired for them.
func (os *OrderService) CancelOrderService(c *gin.Context) (producer.SubmittedOrder, error) {
	if !repo.RingEnabled() {
		return producer.SubmittedOrder{}, errors.New("cancel requires the shared-memory ingest path (ingest.enabled)")
	}
	var req cancelRequest
	if err := c.BindJSON(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return producer.SubmittedOrder{}, ErrInvalidRequest
	}
	return repo.CancelOrderRing(req.OrderID, req.Stock)
}

// ModifyOrderService publishes a modification: a non-zero price is a combined
// price/quantity change, otherwise an in-place quantity reduction.
func (os *OrderService) ModifyOrderService(c *gin.Context) (producer.SubmittedOrder, error) {
	if !repo.RingEnabled() {
		return producer.SubmittedOrder{}, errors.New("modify requires the shared-memory ingest path (ingest.enabled)")
	}
	var req modifyRequest
	if err := c.BindJSON(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return producer.SubmittedOrder{}, ErrInvalidRequest
	}
	if req.Price <= 0 {
		return repo.ReduceQtyRing(req.OrderID, req.Stock, int32(req.Quantity))
	}
	return repo.ModifyOrderRing(req.OrderID, req.Stock, models.PriceToInt(req.Price), int32(req.Quantity))
}
