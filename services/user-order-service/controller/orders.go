package controller

import (
	"TradeIT/internal/middleware"
	"TradeIT/services/user-order-service/services"
	"net/http"

	"github.com/gin-gonic/gin"
)

type OrderController struct {
	orderService services.OrderService
}

func InitOrderController() *OrderController {
	return &OrderController{
		orderService: *services.InitOrderService(),
	}
}

func (oc *OrderController) InitOrderRoutes(router *gin.Engine) {
	protected := router.Group("/order")
	protected.Use(middleware.VerifyToken())
	{
		protected.GET("/", oc.GetAllOrders())
		protected.POST("/sort", oc.GetOrders())
		protected.POST("/create", oc.PlaceOrder())
		protected.POST("/cancel", oc.CancelOrder())
		protected.POST("/modify", oc.ModifyOrder())
		protected.GET("/:id", oc.GetOrderStatus())
	}
}

func (oc *OrderController) GetAllOrders() gin.HandlerFunc {
	return func(c *gin.Context) {
		body := oc.orderService.GetAllOrderService(c)
		if c.IsAborted() { // service already replied
			return
		}
		if len(body) == 0 {
			c.JSON(http.StatusInternalServerError, gin.H{"err": "error in json"})
			return
		}
		// c.Data instead of Writer.Write+c.JSON: the latter appended the byte count of
		// the first write to the body, producing invalid JSON like `[...]445`.
		c.Data(http.StatusOK, "application/json", body)
	}
}

func (oc *OrderController) GetOrders() gin.HandlerFunc {
	return func(c *gin.Context) {
		body := oc.orderService.GetOrderByParameterService(c)
		if c.IsAborted() { // service already replied with 400
			return
		}
		if len(body) == 0 {
			c.JSON(http.StatusInternalServerError, gin.H{"err": "error in json"})
			return
		}
		c.Data(http.StatusOK, "application/json", body)
	}
}

func (oc *OrderController) PlaceOrder() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		res, err := oc.orderService.PlaceOrderRing(ctx)
		if ctx.IsAborted() { // service already replied with 400
			return
		}
		if err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{"err": err.Error()})
			return
		}
		// The command is durable in the producer WAL; the order row is written later by
		// the projection service and read back through GET /order/:id.
		ctx.JSON(http.StatusAccepted, gin.H{
			"status":  "accepted",
			"orderid": res.OrderID,
			"seq":     res.Seq,
		})
	}
}

func (oc *OrderController) CancelOrder() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		res, err := oc.orderService.CancelOrderService(ctx)
		if ctx.IsAborted() { // service already replied with 400
			return
		}
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"err": err.Error()})
			return
		}
		// The command is durable in the producer WAL; the outcome is delivered later.
		ctx.JSON(http.StatusAccepted, gin.H{
			"status":  "accepted",
			"orderid": res.OrderID,
			"seq":     res.Seq,
		})
	}
}

// GetOrderStatus returns one order's status from the cache or database.
func (oc *OrderController) GetOrderStatus() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		body := oc.orderService.GetOrderStatusService(ctx)
		if ctx.IsAborted() { // service already replied (400/404)
			return
		}
		ctx.Data(http.StatusOK, "application/json", body)
	}
}

func (oc *OrderController) ModifyOrder() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		res, err := oc.orderService.ModifyOrderService(ctx)
		if ctx.IsAborted() { // service already replied with 400
			return
		}
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"err": err.Error()})
			return
		}
		ctx.JSON(http.StatusAccepted, gin.H{
			"status":  "accepted",
			"orderid": res.OrderID,
			"seq":     res.Seq,
		})
	}
}
