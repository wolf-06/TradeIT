package main

import (
	"TradeIT/internal/config"
	"TradeIT/internal/control"
	"TradeIT/internal/database"
	"TradeIT/internal/metrics"
	"TradeIT/internal/middleware"
	"TradeIT/internal/models"
	"TradeIT/services/user-order-service/controller"
	"TradeIT/services/user-order-service/producer"
	"TradeIT/services/user-order-service/repo"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

func main() {
	fmt.Println("starting User-Order Service on :8000")

	if err := godotenv.Load(); err != nil {
		// Not fatal: in a container the environment is injected by the orchestrator.
		log.Println("user-order-service: no .env file loaded; using the process environment")
	}

	// Fail fast instead of signing/verifying JWTs with an empty key (anyone could
	// forge a token for any user id in that case).
	if os.Getenv("jwtkey") == "" {
		log.Fatalln("jwtkey is not set: refusing to start with an empty JWT signing key")
	}

	router := gin.Default()
	router.Use(middleware.Metrics())
	registerOpsRoutes(router)
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

	// The shared-memory ring is the only order input path. Load the config and require it.
	cfg, err := config.LoadOptional()
	if err != nil {
		log.Fatalln("Error in loading the ingest config: ", err)
	}
	if cfg == nil || !cfg.Ingest.Enabled {
		log.Fatalln("user-order-service: TRADEIT_CONFIG with ingest.enabled = true is required")
	}
	table, err := cfg.SymbolTable()
	if err != nil {
		log.Fatalln("Error building the symbol table: ", err)
	}
	p, err := producer.Open(cfg.Ingest.Dir, table, cfg.Ingest.RingCapacity, cfg.Ingest.SyncEveryN, cfg.Ingest.SyncEveryMs)
	if err != nil {
		log.Fatalln("Error opening the ring producer: ", err)
	}
	defer p.Close()

	// Re-send orders the consumer has not applied. The consumer publishes its watermark
	// to the shared control region; wait for it (1s, 2s, 3s) then fall back to replaying
	// from the oldest record.
	region, err := control.OpenOrCreate(cfg.ControlPath(), len(cfg.Symbols))
	if err != nil {
		log.Fatalln("Error opening the control region: ", err)
	}
	defer region.Close()
	if err := p.Replay(readWatermarks(region, len(cfg.Symbols))); err != nil {
		log.Fatalln("Error replaying orders: ", err)
	}

	repo.SetRingProducer(p)
	fmt.Printf("user-order-service: shared-memory ingest enabled (dir %s)\n", cfg.Ingest.Dir)

	router.Run(":8000")
}

// readWatermarks waits for the consumer to publish its per-symbol watermark, retrying at
// 1s, 2s and 3s. If none appears it returns the current values (zero), which makes the
// producer replay from the oldest record and rely on the consumer's dedup.
func readWatermarks(region *control.Region, symbols int) []uint64 {
	for _, d := range []time.Duration{time.Second, 2 * time.Second, 3 * time.Second} {
		if wm, ready := snapshotWatermarks(region, symbols); ready {
			return wm
		}
		fmt.Printf("user-order-service: waiting %s for the consumer watermark\n", d)
		time.Sleep(d)
	}
	wm, _ := snapshotWatermarks(region, symbols)
	return wm
}

func snapshotWatermarks(region *control.Region, symbols int) ([]uint64, bool) {
	wm := make([]uint64, symbols)
	ready := false
	for i := range wm {
		wm[i] = region.Watermark(uint32(i))
		if wm[i] > 0 {
			ready = true
		}
	}
	return wm, ready
}

// registerOpsRoutes exposes liveness, readiness and Prometheus metrics. They sit outside
// the JWT-protected groups so an orchestrator or scraper needs no token.
func registerOpsRoutes(router *gin.Engine) {
	router.GET("/healthz", func(c *gin.Context) { c.String(http.StatusOK, "ok") })
	router.GET("/readyz", func(c *gin.Context) {
		db := database.SetDB()
		if db == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database not initialised"})
			return
		}
		sqlDB, err := db.DB()
		if err != nil || sqlDB.Ping() != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "database unreachable"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})
	router.GET("/metrics", gin.WrapH(metrics.Handler()))
}
