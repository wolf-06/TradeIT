package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"TradeIT/internal/config"
	"TradeIT/internal/database"
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

	p, err := projection.Open(cfg.Ingest.Dir, table, store, cache, projection.Options{
		BatchSize:     cfg.Projection.Batch(),
		FlushInterval: cfg.Projection.FlushInterval(),
		PollInterval:  cfg.Projection.PollInterval(),
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
	p.Run(stop)
}
