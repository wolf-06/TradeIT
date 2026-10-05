package services

import (
	"TradeIT/internal/database"
	repo "TradeIT/services/user-order-service/repo"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

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

func (os *OrderService) CreateOrderService(c *gin.Context) bool {
	var order = os.op.acquireOrder()
	defer os.op.releaseOrder(order)
	if err := c.BindJSON(&order); err != nil {
		// NOTE: this used to be log.Fatalln, which killed the whole service on any
		// malformed payload. Reject the request instead.
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return false
	}
	// The order always belongs to the authenticated caller; a user_id supplied in the
	// body is ignored so nobody can place orders on another account.
	user_id, _ := c.Get("userid")
	order.User_id = int(user_id.(float64))
	order.Created_at = time.Now()
	order.Status = "pending"
	return repo.CreateOrder(os.db, *order)
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

func (os *OrderService) CancelOrderService(c *gin.Context) error {
	order_id, _ := c.Get("orderid")
	return repo.CancelOrder(os.db, (order_id.(uint64)))
}
