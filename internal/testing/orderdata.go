package testing

import (
	"TradeIT/internal/database"
	"TradeIT/internal/models"
	"TradeIT/services/user-order-service/repo"
	"log"
	"math"
	"math/rand"
	"time"
)

func roundToTwoDecimal(num float64) float64 {
	return float64((math.Round(num*100) / 100))
}

func OrderTest() {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	// Initialize database connection
	db := database.SetDB()
	order := models.MetaOrder{
		Id:         1000,
		User_id:    698,
		Order_Type: "limit",
		Side:       "buy",
		Stock:      "TCS",
		Price:      roundToTwoDecimal(75), // Use our rounding function
		Quantity:   100,
		Status:     "pending",
		Created_at: time.Now(),
	}
	err := repo.CreateOrder(db, order)
	if !err {
		log.Println("error in creating order: ", err)
	}
	order = models.MetaOrder{
		Id:         1001,
		User_id:    698,
		Order_Type: "limit",
		Side:       "sell",
		Stock:      "TCS",
		Price:      roundToTwoDecimal(70), // Use our rounding function
		Quantity:   100,
		Status:     "pending",
		Created_at: time.Now(),
	}
	err = repo.CreateOrder(db, order)
	if !err {
		log.Println("error in creating order: ", err)
	}
}

// GenerateAndLodgeOrders simulates and sends orders for 4 different stocks with realistic prices.
func GenerateAndLodgeOrders() {
	db := database.SetDB()
	stocks := []string{"AAPL", "GOOG", "TSLA", "MSFT"}
	orderTypes := []string{"limit", "market"}
	sides := []string{"buy", "sell"}
	statuses := []string{"pending", "executed", "cancelled"}
	// Realistic price ranges for each stock
	priceRanges := map[string][2]float64{
		"AAPL": {170, 200},   // Apple
		"GOOG": {2700, 2900}, // Google
		"TSLA": {250, 320},   // Tesla
		"MSFT": {300, 350},   // Microsoft
	}
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < 20; i++ { // 20 orders, various scenarios
		stock := stocks[rand.Intn(len(stocks))]
		orderType := orderTypes[rand.Intn(len(orderTypes))]
		side := sides[rand.Intn(len(sides))]
		quantity := rand.Intn(50) + 1
		pr := priceRanges[stock]
		price := pr[0] + rand.Float64()*(pr[1]-pr[0])
		status := statuses[rand.Intn(len(statuses))]
		order := models.MetaOrder{
			User_id:    rand.Intn(1000) + 1,
			Order_Type: orderType,
			Side:       side,
			Stock:      stock,
			Price:      roundToTwoDecimal(price),
			Quantity:   quantity,
			Status:     status,
			Created_at: time.Now(),
		}
		repo.CreateOrder(db, order)
	}
}

func Test() {
	GenerateAndLodgeOrders()
}
