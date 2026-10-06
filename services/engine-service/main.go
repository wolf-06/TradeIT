package main

import (
	"TradeIT/internal/database"
	"TradeIT/internal/models"
	test "TradeIT/internal/testing"
	"TradeIT/services/engine-service/engine"
	"fmt"
	"log"

	"github.com/joho/godotenv"
)

func main() {
	fmt.Println("starting Engine Service on :8001")

	if err := godotenv.Load(); err != nil {
		log.Fatalln("Error in loading the env file: ", err)
	}

	database.InitDb()
	models.InitTables()

	// Initialize order engine
	orderEngine := engine.InitOrderEngine()

	// Start processing order queue
	go orderEngine.ProcessOrderQueue()

	// Start processing cancellation queue
	go orderEngine.ProcessCancellationQueue()

	// Start processing price modification queue
	go orderEngine.ProccessQuantityModification()

	// Start test (optional)
	go test.Test()

	// Keep the service running
	select {}
}
