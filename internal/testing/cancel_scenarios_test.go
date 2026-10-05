package testing

import (
	"TradeIT/internal/database"
	"TradeIT/internal/models"
	"TradeIT/services/engine-service/engine"
	repo "TradeIT/services/user-order-service/repo"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

var (
	testOrderEngine *engine.OrderEngine
	engineOnce      sync.Once
)

// setupTestDBAndEngine prepares the database and the order engine once per test binary
// and starts the engine's queue workers. Without the workers, repo.CancelOrder blocks
// on a Redis reply that never arrives and the cancellation tests cannot pass.
func setupTestDBAndEngine() {
	engineOnce.Do(func() {
		// The .env file lives at the repository root; tests run with the package as cwd.
		if err := godotenv.Load("../../.env"); err != nil {
			log.Fatalln("Error in loading the env file: ", err)
		}
		database.InitDb()
		if database.SetDB() == nil {
			log.Fatalln("cannot reach the database - start the stack with: docker compose up -d db redis")
		}

		// These scenarios use fixed order ids. Leftovers from a previous run would make
		// CreateOrder fail on the primary key and would let checkTrade see stale trades,
		// so start every run from a clean slate.
		testOrderIDs := []uint64{1001, 1002, 1003, 1004, 1005}
		if err := database.SetDB().Where("id IN ?", testOrderIDs).Delete(&models.Order{}).Error; err != nil {
			log.Println("warning: could not remove leftover test orders: ", err)
		}
		if err := database.SetDB().Where("ask_order_id IN ? OR bid_order_id IN ?", testOrderIDs, testOrderIDs).Delete(&models.TradeDetails{}).Error; err != nil {
			log.Println("warning: could not remove leftover test trades: ", err)
		}

		testOrderEngine = engine.InitOrderEngine()
		go testOrderEngine.ProcessOrderQueue()
		go testOrderEngine.ProcessCancellationQueue()
	})
}

// waitForTrade blocks until the engine has registered the trade for the given order.
// Matching, trade registration and the database update all happen asynchronously, so a
// test that asserts on the outcome of a match has to wait for it.
func waitForTrade(t *testing.T, orderID uint64, stock string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var count int64
		// Match on both id columns (the engine stores the two ids in either of them)
		// and on the stock, so trades belonging to other tests cannot satisfy this wait.
		err := database.SetDB().Model(&models.TradeDetails{}).
			Where("stock = ? AND (bid_order_id = ? OR ask_order_id = ?)", stock, orderID, orderID).
			Count(&count).Error
		if err == nil && count > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("the engine never registered the trade for order %d", orderID)
}

func createTestOrder(orderId uint64, userID int, stock, side string, price float64, quantity int) models.MetaOrder {
	return models.MetaOrder{
		Id:         orderId,
		User_id:    userID,
		Order_Type: "limit",
		Side:       side,
		Stock:      stock,
		Price:      price,
		Quantity:   quantity,
		Status:     "pending",
		Created_at: time.Now(),
	}
}

func TestCancelExistingOrder(t *testing.T) {
	setupTestDBAndEngine()
	db := database.SetDB()
	order := createTestOrder(1001, 1, "AAPL", "buy", 180, 10)
	repo.CreateOrder(db, order)
	err := repo.CancelOrder(db, order.Id)
	order = createTestOrder(1002, 1, "XXXX", "buy", 0, 0)
	repo.CreateOrder(db, order)
	if err != nil {
		t.Errorf("Expected no error when cancelling existing order, got: %v", err)
	}
}

func TestCancelNonExistentOrder(t *testing.T) {
	setupTestDBAndEngine()
	db := database.SetDB()
	nonExistentOrderID := uint64(999999)
	err := repo.CancelOrder(db, nonExistentOrderID)
	if err == nil {
		t.Errorf("Expected error when cancelling non-existent order, got nil")
	}
}

func TestCancelAlreadyCancelledOrder(t *testing.T) {
	setupTestDBAndEngine()
	db := database.SetDB()
	order := createTestOrder(1003, 2, "GOOG", "sell", 2800, 5)
	repo.CreateOrder(db, order)
	repo.CancelOrder(db, order.Id)
	err := repo.CancelOrder(db, order.Id)
	if err == nil {
		t.Errorf("Expected error when cancelling already cancelled order, got nil")
	}
}

func TestCancelExecutedOrder(t *testing.T) {
	setupTestDBAndEngine()
	db := database.SetDB()
	sell := createTestOrder(1004, 3, "TSLA", "sell", 300, 2)
	repo.CreateOrder(db, sell)
	buy := createTestOrder(1005, 4, "TSLA", "buy", 300, 10)
	repo.CreateOrder(db, buy)

	// Matching is asynchronous: cancelling before the trade is registered would just look
	// like an ordinary (successful) cancellation.
	waitForTrade(t, buy.Id, buy.Stock)

	err := repo.CancelOrder(db, buy.Id)
	if err == nil {
		t.Errorf("Expected error when cancelling executed order, got nil")
	}
}
