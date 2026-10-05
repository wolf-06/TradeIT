package services

import (
	"TradeIT/internal/database"
	repo "TradeIT/services/user-order-service/repo"
	"fmt"
	"log"

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
		log.Fatalln("Json Binding error: ", err)
		return false
	}
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
		fmt.Println("Error in binding JSON: ", err)
	}
	json := repo.GetOrders(os.db, user_id.(float64), Query)
	return json
}

func (os *OrderService) CancelOrderService(c *gin.Context) error {
	order_id, _ := c.Get("orderid")
	return repo.CancelOrder(os.db, (order_id.(uint64)))
}
