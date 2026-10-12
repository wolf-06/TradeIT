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

	// The shared-memory ring is the only ingest path. Load the config and require it.
	cfg, err := config.LoadOptional()
	if err != nil {
		log.Fatalln("Error in loading the ingest config: ", err)
	}
	if cfg == nil || !cfg.Ingest.Enabled {
		log.Fatalln("engine: TRADEIT_CONFIG with ingest.enabled = true is required")
	}
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

	// Keep the service running
	select {}
}
