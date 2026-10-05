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
		status := oc.orderService.CreateOrderService(ctx)
		if ctx.IsAborted() { // service already replied with 400
			return
		}
		if !status {
			ctx.JSON(500, "Failed due to internal Server Error")
		} else {
			ctx.JSON(201, "Success")
		}
	}
}

func (oc *OrderController) CancelOrder() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		err := oc.orderService.CancelOrderService(ctx)
		if err != nil {
			ctx.JSON(500, "Unable to Cancel order: "+err.Error())
		} else {
			ctx.JSON(201, "Order has been successfully cancelled")
		}
	}
}
