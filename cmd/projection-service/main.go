package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"TradeIT/internal/config"
	"TradeIT/internal/control"
	"TradeIT/internal/database"
	"TradeIT/internal/metrics"
	"TradeIT/internal/models"
	"TradeIT/internal/projection"

	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
)

// Projection service: tails the engine's durable consumer event log and projects it into
// Postgres (source of truth) and the Redis order-status cache (broker read layer). It runs
// separately so the matching engine does no database work of its own.
func main() {
	fmt.Println("starting Projection Service")

	if err := godotenv.Load(); err != nil {
		log.Println("projection: no .env file loaded:", err)
	}

	database.InitDb()
	if database.SetDB() == nil {
		log.Fatalln("projection: cannot connect to the database")
	}
	models.InitTables()

	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalln("projection: config:", err)
	}
	table, err := cfg.SymbolTable()
	if err != nil {
		log.Fatalln("projection: symbol table:", err)
	}

	store := projection.NewGormStore(database.SetDB())
	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("redis_addr"),
		Password: os.Getenv("redis_password"),
	})
	cache := projection.NewRedisCache(rdb, cfg.Projection.CacheTTL())

	// Publish the committed watermark to a shared region (separate from the consumer's)
	// so projection lag is observable and a future WAL retention floor can use it.
	region, err := control.OpenOrCreate(cfg.ProjectionControlPath(), len(cfg.Symbols))
	if err != nil {
		log.Fatalln("projection: control region:", err)
	}
	defer region.Close()

	p, err := projection.Open(cfg.Ingest.Dir, table, store, cache, projection.Options{
		BatchSize:     cfg.Projection.Batch(),
		FlushInterval: cfg.Projection.FlushInterval(),
		PollInterval:  cfg.Projection.PollInterval(),
		Region:        region,
	})
	if err != nil {
		log.Fatalln("projection: open:", err)
	}
	defer p.Close()

	stop := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		close(stop)
	}()

	fmt.Printf("projection: tailing event log in %s\n", cfg.Ingest.Dir)

	// Metrics and health on :8002.
	metrics.Serve(":8002", func() error {
		sqlDB, err := database.SetDB().DB()
		if err != nil {
			return err
		}
		if err := sqlDB.Ping(); err != nil {
			return err
		}
		return rdb.Ping(context.Background()).Err()
	})

	p.Run(stop)
}
