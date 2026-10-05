package engine_test

import (
	"log"
	"os"
	"testing"

	"TradeIT/internal/database"
	"TradeIT/services/engine-service/engine"

	"github.com/joho/godotenv"
)

// TestMain initialises everything the orderbook needs for its asynchronous trade and
// status bookkeeping: the TSID generator (engine.InitTrade) and the database handle.
// The goroutines spawned by Orderbook.Matcher call registerTrades/updateOrderStatus*,
// which panic with a nil pointer dereference when those are not set up.
func TestMain(m *testing.M) {
	// The .env file lives at the repository root; tests run with the package as cwd.
	if err := godotenv.Load("../../../.env"); err != nil {
		log.Println("warning: no .env file loaded, using the existing environment: ", err)
	}
	database.InitDb()
	if database.SetDB() == nil {
		log.Fatalln("cannot reach the database - start the stack with: docker compose up -d db redis")
	}
	engine.InitTrade()
	os.Exit(m.Run())
}
