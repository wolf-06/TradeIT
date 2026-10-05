// Command engine-benchmark measures the performance of the TradeIT matching engine.
//
// It builds a production-like order flow - the same JSON payloads the user-order service
// pushes onto the Redis "IncomingOrder" list (repo.CreateOrder) - and drives the engine
// with it. Four modes are available:
//
//	match  feed the generated orders straight into the in-memory matcher (default, no Redis)
//	load   push the generated orders onto the Redis queue the engine consumes
//	drain  consume the Redis queue exactly like engine.OrderEngine.ProcessOrderQueue does
//	all    reset the queue, load it, then drain it
//
// By default the matching path runs without PostgreSQL writes (engine.PersistenceEnabled
// is switched off) so the numbers reflect pure in-memory matching. Pass -persist to keep
// the production behaviour, which additionally requires the database.
//
// Examples:
//
//	# pure matcher throughput on a 200k order production-like flow
//	go run ./cmd/engine-benchmark -mode match -orders 200000
//
//	# build the production queue and leave it for a running engine service
//	go run ./cmd/engine-benchmark -mode load -orders 100000 -reset
//
//	# end-to-end: load the queue, then drain it like the engine does
//	go run ./cmd/engine-benchmark -mode all -orders 100000 -reset
//
//	# machine readable report
//	go run ./cmd/engine-benchmark -mode match -orders 200000 -json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"TradeIT/internal/benchmark"
	"TradeIT/internal/database"
	"TradeIT/internal/models"
	"TradeIT/services/engine-service/engine"

	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
)

type options struct {
	mode        string
	orders      int
	warmup      int
	seed        int64
	symbols     string
	tickBPS     float64
	spreadTicks int
	aggressive  float64
	marketRatio float64
	minQty      int
	maxQty      int
	users       int
	midDrift    float64
	idStart     uint64
	redisAddr   string
	redisPass   string
	redisDB     int
	key         string
	reset       bool
	timeout     time.Duration
	batch       int
	persist     bool
	settle      time.Duration
	verbose     bool
	jsonOut     bool
}

func parseFlags() *options {
	o := &options{}
	def := benchmark.DefaultConfig()

	flag.StringVar(&o.mode, "mode", "match", "benchmark mode: match | load | drain | all")
	flag.IntVar(&o.orders, "orders", 100000, "number of orders in the measured workload")
	flag.IntVar(&o.warmup, "warmup", 5000, "warm-up orders processed before the measured batch")
	flag.Int64Var(&o.seed, "seed", 0, "RNG seed (0 = time based)")
	flag.StringVar(&o.symbols, "symbols", strings.Join(def.Symbols, ","), "comma separated symbols")
	flag.Float64Var(&o.tickBPS, "tick-bps", def.TickBPS, "price level size, in basis points of the reference price")
	flag.IntVar(&o.spreadTicks, "spread-ticks", def.SpreadTicks, "book depth per side, in price levels")
	flag.Float64Var(&o.aggressive, "aggressive", def.Aggressive, "share (0..1) of limit orders priced through the opposite side")
	flag.Float64Var(&o.marketRatio, "market-ratio", def.MarketRatio, "share (0..1) of market orders")
	flag.IntVar(&o.minQty, "min-qty", def.MinQty, "minimum order quantity")
	flag.IntVar(&o.maxQty, "max-qty", def.MaxQty, "maximum order quantity")
	flag.IntVar(&o.users, "users", def.Users, "number of distinct user ids")
	flag.Float64Var(&o.midDrift, "mid-drift", def.MidDrift, "mid price random-walk step, in ticks per order")
	flag.Uint64Var(&o.idStart, "id-start", def.IDStart, "first order id")
	flag.StringVar(&o.redisAddr, "redis-addr", "", "redis address (default: $redis_addr from .env)")
	flag.StringVar(&o.redisPass, "redis-pass", "", "redis password (default: $redis_password from .env)")
	flag.IntVar(&o.redisDB, "redis-db", 0, "redis logical database holding the queue")
	flag.StringVar(&o.key, "queue-key", "IncomingOrder", "redis queue name")
	flag.BoolVar(&o.reset, "reset", false, "delete the queue before loading (load/all modes)")
	flag.DurationVar(&o.timeout, "timeout", time.Second, "BLPOP timeout used to detect an empty queue (redis clamps to a 1s minimum)")
	flag.IntVar(&o.batch, "batch", 1000, "redis pipeline batch size while loading")
	flag.BoolVar(&o.persist, "persist", false, "keep the production behaviour: write trades/status to PostgreSQL")
	flag.DurationVar(&o.settle, "settle", 2*time.Second, "with -persist, time to wait for the async DB writers before exiting")
	flag.BoolVar(&o.verbose, "verbose", false, "do not silence the engine's per-order logging")
	flag.BoolVar(&o.jsonOut, "json", false, "print the report as JSON")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "engine-benchmark measures the TradeIT matching engine.\n\nUsage: %s [flags]\n\nFlags:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	return o
}

// Result is the report of one benchmark run. It doubles as the -json payload.
type Result struct {
	Mode             string            `json:"mode"`
	Symbols          []string          `json:"symbols"`
	QueueKey         string            `json:"queue_key"`
	QueueDB          int               `json:"queue_db"`
	RedisAddr        string            `json:"redis_addr"`
	Persistence      bool              `json:"persistence_enabled"`
	Orders           int               `json:"orders"`
	Warmup           int               `json:"warmup_orders"`
	Limit            int               `json:"limit_orders"`
	Market           int               `json:"market_orders"`
	Buy              int               `json:"buy_orders"`
	Sell             int               `json:"sell_orders"`
	LoadOrders       int               `json:"load_orders,omitempty"`
	Generation       time.Duration     `json:"generation_ns"`
	QueueDepth       int64             `json:"queue_depth,omitempty"`
	LoadElapsed      time.Duration     `json:"load_elapsed_ns,omitempty"`
	LoadOrdersPerSec float64           `json:"load_orders_per_sec,omitempty"`
	Processed        int               `json:"processed_orders"`
	Elapsed          time.Duration     `json:"elapsed_ns"`
	OrdersPerSec     float64           `json:"orders_per_sec"`
	Trades           int64             `json:"trades"`
	RestingBuy       int64             `json:"resting_buy_orders"`
	RestingSell      int64             `json:"resting_sell_orders"`
	PriceLevels      int               `json:"price_levels"`
	Latency          benchmark.Latency `json:"latency"`
}

// absorb copies the matcher statistics of a run into the report.
func (r *Result) absorb(s *runStats) {
	r.Processed = s.Processed
	r.Elapsed = s.Elapsed
	r.OrdersPerSec = s.OrdersPerSec
	r.Trades = s.Trades
	r.RestingBuy = s.RestingBuy
	r.RestingSell = s.RestingSell
	r.PriceLevels = s.PriceLevels
	r.Latency = s.Latency
}

func envValue(key string) string {
	return strings.Trim(strings.TrimSpace(os.Getenv(key)), "\"")
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "engine-benchmark: "+format+"\n", args...)
	os.Exit(1)
}

func rate(n int, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(n) / d.Seconds()
}

func setupPersistence(opts *options) {
	if opts.persist {
		database.InitDb()
		if database.SetDB() == nil {
			fatalf("cannot reach the database - start the stack with: docker compose up -d db redis (or drop -persist)")
		}
	}
	// InitTrade installs the TSID generator and the db handle the matcher's async
	// bookkeeping uses. With PersistenceEnabled=false those writers return immediately,
	// so no database is required at all.
	engine.InitTrade()
	engine.PersistenceEnabled = opts.persist
}

func silence(opts *options) func() {
	if opts.verbose {
		return func() {}
	}
	return benchmark.Silence()
}

func dialRedis(opts *options) *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     opts.redisAddr,
		Password: opts.redisPass,
		DB:       opts.redisDB,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		fatalf("cannot reach redis at %q: %v (start it with: docker compose up -d redis)", opts.redisAddr, err)
	}
	return rdb
}

func main() {
	opts := parseFlags()
	// Best effort: the engine service loads .env from the working directory as well.
	_ = godotenv.Load()

	switch opts.mode {
	case "match", "load", "drain", "all":
	default:
		fatalf("unknown -mode %q (want match, load, drain or all)", opts.mode)
	}
	if opts.orders <= 0 {
		fatalf("-orders must be greater than zero")
	}
	if opts.warmup < 0 {
		opts.warmup = 0
	}
	if opts.timeout < time.Second {
		// go-redis clamps BLPOP/BRPOP timeouts to one second and logs a warning otherwise.
		opts.timeout = time.Second
	}
	if opts.redisAddr == "" {
		opts.redisAddr = envValue("redis_addr")
	}
	if opts.redisPass == "" {
		opts.redisPass = envValue("redis_password")
	}
	if opts.redisAddr == "" {
		opts.redisAddr = "localhost:6379"
	}

	res := &Result{
		Mode:        opts.mode,
		Symbols:     strings.Split(opts.symbols, ","),
		QueueKey:    opts.key,
		QueueDB:     opts.redisDB,
		RedisAddr:   opts.redisAddr,
		Persistence: opts.persist,
		Warmup:      opts.warmup,
	}

	setupPersistence(opts)

	gen := benchmark.NewGenerator(benchmark.Config{
		Symbols:     res.Symbols,
		TickBPS:     opts.tickBPS,
		SpreadTicks: opts.spreadTicks,
		Aggressive:  opts.aggressive,
		MarketRatio: opts.marketRatio,
		MinQty:      opts.minQty,
		MaxQty:      opts.maxQty,
		Users:       opts.users,
		MidDrift:    opts.midDrift,
		Seed:        opts.seed,
		IDStart:     opts.idStart,
	})

	genStart := time.Now()
	warm := gen.Generate(opts.warmup)
	work := gen.Generate(opts.orders)
	res.Generation = time.Since(genStart)
	res.Orders = work.Len()
	res.Limit, res.Market, res.Buy, res.Sell = work.Limit, work.Market, work.Buy, work.Sell

	switch opts.mode {
	case "match":
		res.absorb(runMatch(opts, warm.Orders, work.Orders))
	case "load":
		rdb := dialRedis(opts)
		runLoad(opts, rdb, append(warm.Orders, work.Orders...)).applyTo(res)
		_ = rdb.Close()
	case "drain":
		rdb := dialRedis(opts)
		res.absorb(runDrain(opts, rdb, 0))
		_ = rdb.Close()
	case "all":
		rdb := dialRedis(opts)
		loaded := append(warm.Orders, work.Orders...)
		runLoad(opts, rdb, loaded).applyTo(res)
		res.absorb(runDrain(opts, rdb, opts.warmup))
		_ = rdb.Close()
	}

	if opts.persist && opts.settle > 0 {
		// The matcher registers trades and updates order status in goroutines; give those
		// writers a moment to finish before the process exits.
		time.Sleep(opts.settle)
	}

	printReport(res, opts)
}

// runStats holds the matcher statistics collected during one measured run.
type runStats struct {
	Processed    int
	Elapsed      time.Duration
	OrdersPerSec float64
	Trades       int64
	RestingBuy   int64
	RestingSell  int64
	PriceLevels  int
	Latency      benchmark.Latency
}

// loadStats holds how long it took to build the Redis queue.
type loadStats struct {
	Count        int
	Elapsed      time.Duration
	OrdersPerSec float64
	QueueDepth   int64
}

func (l *loadStats) applyTo(r *Result) {
	r.LoadOrders = l.Count
	r.LoadElapsed = l.Elapsed
	r.LoadOrdersPerSec = l.OrdersPerSec
	r.QueueDepth = l.QueueDepth
}

// books keeps one orderbook per symbol, exactly like engine.Ledger does in production
// (entity -> orderbook). It also gives the benchmark read access to the per-book counters
// without touching engine internals.
type books map[string]*engine.Orderbook

func (b books) get(stock string) *engine.Orderbook {
	ob := b[stock]
	if ob == nil {
		ob = engine.InitOrderBook()
		b[stock] = ob
	}
	return ob
}

func (b books) fill(s *runStats) {
	for _, ob := range b {
		s.Trades += ob.GetTradeCount()
		s.RestingBuy += ob.GetBuyCount()
		s.RestingSell += ob.GetSellCount()
		s.PriceLevels += len(ob.GetBuyOrders()) + len(ob.GetSellOrders())
	}
}

// runMatch feeds the orders straight into the in-memory matcher, bypassing Redis, and
// measures the time spent inside Orderbook.Matcher per order.
func runMatch(opts *options, warm, orders []models.Order) *runStats {
	b := books{}
	restore := silence(opts)
	for i := range warm {
		_ = b.get(warm[i].Stock).Matcher(warm[i])
	}
	runtime.GC()

	latencies := make([]time.Duration, 0, len(orders))
	start := time.Now()
	for i := range orders {
		o := orders[i]
		ob := b.get(o.Stock)
		t0 := time.Now()
		if err := ob.Matcher(o); err != nil {
			log.Println("engine-benchmark: matcher error:", err)
		}
		latencies = append(latencies, time.Since(t0))
	}
	elapsed := time.Since(start)
	restore()

	s := &runStats{
		Processed: len(orders),
		Elapsed:   elapsed,
		Latency:   benchmark.SummarizeLatency(latencies),
	}
	s.OrdersPerSec = rate(s.Processed, elapsed)
	b.fill(s)
	return s
}

// runLoad pushes the generated orders onto the Redis queue the engine consumes. The
// payload is the same JSON the user-order service produces (json.Marshal(models.Order) on
// the "IncomingOrder" list), so a running engine service will drain this queue unchanged.
func runLoad(opts *options, rdb *redis.Client, orders []models.Order) *loadStats {
	ctx := context.Background()
	if opts.reset {
		if err := rdb.Del(ctx, opts.key).Err(); err != nil {
			fatalf("cannot reset queue %q: %v", opts.key, err)
		}
	}

	restore := silence(opts)
	start := time.Now()
	pipe := rdb.Pipeline()
	pending := 0
	for i := range orders {
		payload, err := json.Marshal(orders[i])
		if err != nil {
			restore()
			fatalf("cannot marshal order %d: %v", orders[i].Id, err)
		}
		pipe.RPush(ctx, opts.key, payload)
		pending++
		if pending >= opts.batch {
			if _, err := pipe.Exec(ctx); err != nil {
				restore()
				fatalf("redis load failed: %v", err)
			}
			pipe = rdb.Pipeline()
			pending = 0
		}
	}
	if pending > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			restore()
			fatalf("redis load failed: %v", err)
		}
	}
	elapsed := time.Since(start)
	restore()

	depth, err := rdb.LLen(ctx, opts.key).Result()
	if err != nil {
		fatalf("cannot read queue length: %v", err)
	}
	return &loadStats{
		Count:        len(orders),
		Elapsed:      elapsed,
		OrdersPerSec: rate(len(orders), elapsed),
		QueueDepth:   depth,
	}
}

// runDrain consumes the queue exactly like engine.OrderEngine.ProcessOrderQueue: BLPOP on
// the queue key, json.Unmarshal into models.Order, then hand it to the per-symbol
// orderbook. It measures the end-to-end processing rate (Redis round trip + decode +
// match). skip orders are processed but not timed, so a warm-up batch does not pollute the
// latency numbers.
func runDrain(opts *options, rdb *redis.Client, skip int) *runStats {
	ctx := context.Background()
	b := books{}
	restore := silence(opts)

	latencies := make([]time.Duration, 0, 4096)
	processed := 0
	var start time.Time
	timedOut := false

	for {
		popped, err := rdb.BLPop(ctx, opts.timeout, opts.key).Result()
		if err == redis.Nil {
			timedOut = true
			break
		}
		if err != nil {
			restore()
			fatalf("redis drain failed: %v", err)
		}
		if len(popped) < 2 {
			continue
		}
		processed++

		var o models.Order
		if err := json.Unmarshal([]byte(popped[1]), &o); err != nil {
			log.Println("engine-benchmark: cannot decode queue entry:", err)
			continue
		}

		timed := processed > skip
		if timed && start.IsZero() {
			start = time.Now()
		}
		t0 := time.Now()
		if err := b.get(o.Stock).Matcher(o); err != nil {
			log.Println("engine-benchmark: matcher error:", err)
		}
		if timed {
			latencies = append(latencies, time.Since(t0))
		}
	}

	elapsed := time.Duration(0)
	if !start.IsZero() {
		elapsed = time.Since(start)
		if timedOut {
			// BLPop blocked for the whole timeout before we learned the queue was empty;
			// that idle time is not engine work.
			elapsed -= opts.timeout
			if elapsed < 0 {
				elapsed = 0
			}
		}
	}
	restore()

	s := &runStats{
		Processed: max(processed-skip, 0),
		Elapsed:   elapsed,
		Latency:   benchmark.SummarizeLatency(latencies),
	}
	s.OrdersPerSec = rate(s.Processed, elapsed)
	b.fill(s)
	return s
}

// printReport renders the run either as JSON or as an aligned console block.
func printReport(r *Result, opts *options) {
	if opts.jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(r); err != nil {
			fatalf("cannot encode report: %v", err)
		}
		return
	}

	const line = "------------------------------------------------------------"
	fmt.Println("============================================================")
	fmt.Println(" TradeIT matching engine benchmark")
	fmt.Println("============================================================")
	fmt.Printf(" mode              %s\n", r.Mode)
	fmt.Printf(" symbols           %s\n", strings.Join(r.Symbols, ","))
	if r.Persistence {
		fmt.Println(" persistence       enabled (trades/status written to PostgreSQL)")
	} else {
		fmt.Println(" persistence       disabled (pure in-memory matcher)")
	}
	if r.Mode != "match" {
		fmt.Printf(" queue             %s (redis db %d @ %s)\n", r.QueueKey, r.QueueDB, r.RedisAddr)
	}
	fmt.Println(line)
	fmt.Printf(" workload          %s orders (+ %s warm-up)\n", comma(int64(r.Orders)), comma(int64(r.Warmup)))
	fmt.Printf(" limit / market    %s / %s\n", comma(int64(r.Limit)), comma(int64(r.Market)))
	fmt.Printf(" buy / sell        %s / %s\n", comma(int64(r.Buy)), comma(int64(r.Sell)))
	fmt.Printf(" generated in      %s\n", r.Generation)
	if r.LoadOrders > 0 {
		fmt.Println(line)
		fmt.Printf(" queued            %s orders in %s (%s orders/sec)\n", comma(int64(r.LoadOrders)), r.LoadElapsed, comma(int64(r.LoadOrdersPerSec)))
		fmt.Printf(" queue depth       %s\n", comma(r.QueueDepth))
	}
	fmt.Println(line)
	fmt.Printf(" processed         %s orders\n", comma(int64(r.Processed)))
	fmt.Printf(" elapsed           %s\n", r.Elapsed)
	fmt.Printf(" throughput        %s orders/sec\n", comma(int64(r.OrdersPerSec)))
	fmt.Printf(" trades matched    %s\n", comma(r.Trades))
	fmt.Printf(" resting orders    %s (buy %s / sell %s)\n", comma(r.RestingBuy+r.RestingSell), comma(r.RestingBuy), comma(r.RestingSell))
	fmt.Printf(" price levels      %s\n", comma(int64(r.PriceLevels)))
	fmt.Println(line)
	fmt.Println(" latency (per order)")
	fmt.Printf("   min             %s\n", r.Latency.Min)
	fmt.Printf("   mean            %s\n", r.Latency.Mean)
	fmt.Printf("   p50             %s\n", r.Latency.P50)
	fmt.Printf("   p90             %s\n", r.Latency.P90)
	fmt.Printf("   p99             %s\n", r.Latency.P99)
	fmt.Printf("   p99.9           %s\n", r.Latency.P999)
	fmt.Printf("   max             %s\n", r.Latency.Max)
	if r.Mode != "match" {
		fmt.Println(line)
		fmt.Println(" note: queue modes include one Redis BLPOP round trip per order")
		fmt.Println("       (the engine's consumer path); -mode match isolates the matcher.")
	}
	fmt.Println("============================================================")
}

// comma formats an integer with thousands separators, e.g. 1500000 -> "1,500,000".
func comma(n int64) string {
	s := fmt.Sprintf("%d", n)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	var b strings.Builder
	pre := len(s) % 3
	if pre > 0 {
		b.WriteString(s[:pre])
	}
	for i := pre; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}
