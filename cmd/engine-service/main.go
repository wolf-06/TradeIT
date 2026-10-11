package main

import (
	"TradeIT/internal/config"
	"TradeIT/internal/control"
	"TradeIT/internal/database"
	"TradeIT/internal/models"
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

	// Choose the ingest path. The shared-memory ring is opt-in via TRADEIT_CONFIG; the
	// default remains the Redis queue so nothing changes until it is enabled.
	cfg, err := config.LoadOptional()
	if err != nil {
		log.Fatalln("Error in loading the ingest config: ", err)
	}
	if cfg != nil && cfg.Ingest.Enabled {
		region, err := control.OpenOrCreate(cfg.ControlPath(), len(cfg.Symbols))
		if err != nil {
			log.Fatalln("Error opening the control region: ", err)
		}
		defer region.Close()
		rc, err := engine.OpenRingConsumer(cfg, orderEngine.Ledger(), region)
		if err != nil {
			log.Fatalln("Error in opening the ring consumer: ", err)
		}
		go rc.Run()
		fmt.Printf("engine: shared-memory ingest enabled (dir %s)\n", cfg.Ingest.Dir)
	} else {
		// Legacy Redis path: orders plus the cancel/modify control queues.
		go orderEngine.ProcessOrderQueue()
		go orderEngine.ProcessCancellationQueue()
		go orderEngine.ProccessQuantityModification()
	}

	// Keep the service running
	select {}
}
