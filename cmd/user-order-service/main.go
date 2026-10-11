package main

import (
	"TradeIT/internal/config"
	"TradeIT/internal/database"
	"TradeIT/internal/models"
	"TradeIT/services/user-order-service/controller"
	"TradeIT/services/user-order-service/producer"
	"TradeIT/services/user-order-service/repo"
	"fmt"
	"log"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

func main() {
	fmt.Println("starting User-Order Service on :8000")

	if err := godotenv.Load(); err != nil {
		log.Fatalln("Error in loading the env file: ", err)
	}

	// Fail fast instead of signing/verifying JWTs with an empty key (anyone could
	// forge a token for any user id in that case).
	if os.Getenv("jwtkey") == "" {
		log.Fatalln("jwtkey is not set: refusing to start with an empty JWT signing key")
	}

	router := gin.Default()
	database.InitDb()
	if database.SetDB() == nil {
		// Without this the service starts happily and every request then panics on a
		// nil *gorm.DB (e.g. when a db_* variable in the environment shadows .env).
		log.Fatalln("cannot connect to the database - check db_host/db_port/db_user/db_name/db_passwd and that it is running")
	}
	models.InitTables()

	userController := controller.InitUserController()
	userController.InitUserControllerRoutes(router)

	orderController := controller.InitOrderController()
	orderController.InitOrderRoutes(router)

	// Opt in to the shared-memory ingest path. When enabled, order creation is published
	// to the ring (durable in the producer WAL) instead of the synchronous DB+Redis path.
	if cfg, err := config.LoadOptional(); err != nil {
		log.Fatalln("Error in loading the ingest config: ", err)
	} else if cfg != nil && cfg.Ingest.Enabled {
		table, err := cfg.SymbolTable()
		if err != nil {
			log.Fatalln("Error building the symbol table: ", err)
		}
		p, err := producer.Open(cfg.Ingest.Dir, table, cfg.Ingest.RingCapacity, cfg.Ingest.SyncEveryN, cfg.Ingest.SyncEveryMs)
		if err != nil {
			log.Fatalln("Error opening the ring producer: ", err)
		}
		defer p.Close()
		repo.SetRingProducer(p)
		fmt.Printf("user-order-service: shared-memory ingest enabled (dir %s)\n", cfg.Ingest.Dir)
	}

	router.Run(":8000")
}
