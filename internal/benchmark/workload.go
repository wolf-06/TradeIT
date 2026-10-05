// Package benchmark builds a production-like order flow for the TradeIT matching
// engine and the statistics reported by cmd/engine-benchmark.
//
// The workload deliberately mirrors the payloads the user-order service produces in
// production: repo.CreateOrder serialises a models.Order (with Remq set to the order
// quantity) onto the Redis "IncomingOrder" list, which the engine consumes one element
// at a time. The generator therefore emits exactly those values, across several symbols,
// with realistic discrete price levels, a configurable limit/market mix, both sides and
// varied quantities, so the resulting queue looks like production traffic.
package benchmark

import (
	"math"
	"math/rand"
	"sort"
	"time"

	"TradeIT/internal/models"
)

// defaultBasePrices are the reference prices used when a symbol is not configured. They
// are intentionally close to the fixture values used in internal/testing.
var defaultBasePrices = map[string]float64{
	"AAPL": 185.40,
	"GOOG": 2812.75,
	"TSLA": 292.10,
	"MSFT": 331.85,
}

// fallbackBasePrice is used for symbols that have neither an explicit base price nor an
// entry in defaultBasePrices.
const fallbackBasePrice = 100.0

// Config describes the synthetic order flow. The zero value is not useful; build it with
// DefaultConfig and override what you need.
type Config struct {
	// Symbols are the traded instruments; each one gets its own order book, exactly like
	// engine.Ledger does in production.
	Symbols []string
	// BasePrices is the reference (opening) mid price per symbol.
	BasePrices map[string]float64
	// TickBPS is the size of one price level, expressed in basis points of the reference
	// price (5 bps ~= 0.05%). Prices are rounded to two decimals.
	TickBPS float64
	// SpreadTicks is how many price levels deep each side of the book is generated.
	SpreadTicks int
	// Aggressive is the share (0..1) of limit orders that are priced through the opposite
	// side, so they trade immediately instead of resting.
	Aggressive float64
	// MarketRatio is the share (0..1) of orders sent as market orders.
	MarketRatio float64
	// MinQty/MaxQty bound the order quantity (uniformly distributed).
	MinQty int
	MaxQty int
	// Users is the number of distinct user ids placing orders.
	Users int
	// MidDrift is the per-order random-walk step of the mid price, in ticks.
	MidDrift float64
	// Seed makes the generated flow reproducible.
	Seed int64
	// IDStart is the first order id; ids increase by one afterwards.
	IDStart uint64
}

// DefaultConfig returns a flow that resembles the order stream the engine handles in
// production: a few liquid symbols trading around a slowly drifting mid, mostly resting
// limit orders with a minority of aggressive and market orders.
func DefaultConfig() Config {
	return Config{
		Symbols:     []string{"AAPL", "GOOG", "TSLA", "MSFT"},
		BasePrices:  defaultBasePrices,
		TickBPS:     5,
		SpreadTicks: 20,
		Aggressive:  0.20,
		MarketRatio: 0.15,
		MinQty:      1,
		MaxQty:      100,
		Users:       1000,
		MidDrift:    0.35,
		Seed:        time.Now().UnixNano(),
		IDStart:     1,
	}
}

// Workload is a generated batch of orders plus the mix counters used in the report.
type Workload struct {
	Orders []models.Order `json:"-"`
	Limit  int            `json:"limit"`
	Market int            `json:"market"`
	Buy    int            `json:"buy"`
	Sell   int            `json:"sell"`
}

// Len returns the number of generated orders.
func (w *Workload) Len() int { return len(w.Orders) }

func (w *Workload) tally(o models.Order) {
	if o.Order_Type == "market" {
		w.Market++
	} else {
		w.Limit++
	}
	if o.Side == "buy" {
		w.Buy++
	} else {
		w.Sell++
	}
}

func round2(x float64) float64 { return math.Round(x*100) / 100 }

// Generator produces a reproducible, production-like stream of orders. It keeps the
// simulated mid price of every symbol, so consecutive orders form a continuous market
// rather than independent random samples.
type Generator struct {
	cfg    Config
	rng    *rand.Rand
	base   map[string]float64
	tick   map[string]float64
	mid    map[string]float64
	nextID uint64
}

// NewGenerator normalises cfg (filling in defaults for unset fields) and returns a
// generator for it.
func NewGenerator(cfg Config) *Generator {
	def := DefaultConfig()
	if len(cfg.Symbols) == 0 {
		cfg.Symbols = def.Symbols
	}
	if cfg.TickBPS <= 0 {
		cfg.TickBPS = def.TickBPS
	}
	if cfg.SpreadTicks <= 0 {
		cfg.SpreadTicks = def.SpreadTicks
	}
	if cfg.Aggressive < 0 {
		cfg.Aggressive = 0
	}
	if cfg.Aggressive > 1 {
		cfg.Aggressive = 1
	}
	if cfg.MarketRatio < 0 {
		cfg.MarketRatio = 0
	}
	if cfg.MarketRatio > 1 {
		cfg.MarketRatio = 1
	}
	if cfg.MinQty <= 0 {
		cfg.MinQty = def.MinQty
	}
	if cfg.MaxQty < cfg.MinQty {
		cfg.MaxQty = cfg.MinQty
	}
	if cfg.Users <= 0 {
		cfg.Users = def.Users
	}
	if cfg.MidDrift <= 0 {
		cfg.MidDrift = def.MidDrift
	}
	if cfg.Seed == 0 {
		cfg.Seed = def.Seed
	}
	if cfg.IDStart == 0 {
		cfg.IDStart = def.IDStart
	}

	base := make(map[string]float64, len(cfg.Symbols))
	tick := make(map[string]float64, len(cfg.Symbols))
	mid := make(map[string]float64, len(cfg.Symbols))
	for _, s := range cfg.Symbols {
		b := cfg.BasePrices[s]
		if b <= 0 {
			b = defaultBasePrices[s]
		}
		if b <= 0 {
			b = fallbackBasePrice
		}
		b = round2(b)
		t := round2(b * cfg.TickBPS / 10000)
		if t < 0.01 {
			t = 0.01
		}
		base[s] = b
		tick[s] = t
		mid[s] = b
	}

	return &Generator{
		cfg:    cfg,
		rng:    rand.New(rand.NewSource(cfg.Seed)),
		base:   base,
		tick:   tick,
		mid:    mid,
		nextID: cfg.IDStart,
	}
}

// Next returns the next generated order.
func (g *Generator) Next() models.Order {
	sym := g.cfg.Symbols[g.rng.Intn(len(g.cfg.Symbols))]
	g.drift(sym)

	side := "buy"
	if g.rng.Intn(2) == 1 {
		side = "sell"
	}

	qty := g.cfg.MinQty
	if g.cfg.MaxQty > g.cfg.MinQty {
		qty += g.rng.Intn(g.cfg.MaxQty - g.cfg.MinQty + 1)
	}

	orderType := "limit"
	price := 0.0
	if g.rng.Float64() < g.cfg.MarketRatio {
		// Market orders carry no price in production; the engine prices them from the
		// resting book / last traded price.
		orderType = "market"
	} else {
		price = g.limitPrice(sym, side)
	}

	id := g.nextID
	g.nextID++

	return models.Order{
		MetaOrder: models.MetaOrder{
			Id:         id,
			User_id:    g.rng.Intn(g.cfg.Users) + 1,
			Order_Type: orderType,
			Side:       side,
			Stock:      sym,
			Price:      price,
			Quantity:   qty,
			Status:     "pending",
			Created_at: time.Now(),
		},
		Remq: qty, // production sets Remq = Quantity before publishing (repo.CreateOrder)
	}
}

// Generate produces n orders. The generator keeps its internal state, so successive
// calls continue the same market simulation (used to warm the matcher up before the
// measured batch).
func (g *Generator) Generate(n int) *Workload {
	w := &Workload{Orders: make([]models.Order, 0, n)}
	for i := 0; i < n; i++ {
		o := g.Next()
		w.tally(o)
		w.Orders = append(w.Orders, o)
	}
	return w
}

// drift advances the simulated mid price of sym by a small random-walk step, clamped to
// +/-10% of its reference price.
func (g *Generator) drift(sym string) {
	m := g.mid[sym] + g.tick[sym]*g.cfg.MidDrift*g.rng.NormFloat64()
	lo, hi := g.base[sym]*0.9, g.base[sym]*1.1
	if m < lo {
		m = lo
	}
	if m > hi {
		m = hi
	}
	g.mid[sym] = m
}

// limitPrice places a limit order on a discrete tick level around the mid. Most orders
// rest on their own side (buys below the mid, sells above it); the aggressive share is
// priced through the opposite side so it crosses and trades immediately.
func (g *Generator) limitPrice(sym, side string) float64 {
	ticks := g.cfg.SpreadTicks
	// Squared uniform -> biased towards the top of the book, which is where most real
	// order flow sits.
	k := 1 + int(g.rng.Float64()*g.rng.Float64()*float64(ticks))
	if k > ticks {
		k = ticks
	}
	off := float64(k) * g.tick[sym]

	aggressive := g.rng.Float64() < g.cfg.Aggressive
	price := g.mid[sym] - off // resting buy / aggressive sell
	if (side == "buy") == aggressive {
		price = g.mid[sym] + off
	}

	price = round2(price)
	if price <= 0 {
		price = round2(g.tick[sym])
	}
	return price
}

// Latency summarises the per-order matching latencies of a run.
type Latency struct {
	Count int           `json:"count"`
	Min   time.Duration `json:"min_ns"`
	Mean  time.Duration `json:"mean_ns"`
	P50   time.Duration `json:"p50_ns"`
	P90   time.Duration `json:"p90_ns"`
	P99   time.Duration `json:"p99_ns"`
	P999  time.Duration `json:"p999_ns"`
	Max   time.Duration `json:"max_ns"`
}

// SummarizeLatency copies and sorts the samples and returns the summary.
func SummarizeLatency(samples []time.Duration) Latency {
	if len(samples) == 0 {
		return Latency{}
	}
	values := make([]int64, len(samples))
	var total int64
	for i, d := range samples {
		values[i] = int64(d)
		total += values[i]
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })

	at := func(p float64) time.Duration {
		if len(values) == 1 {
			return time.Duration(values[0])
		}
		idx := int(math.Ceil(p*float64(len(values)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(values) {
			idx = len(values) - 1
		}
		return time.Duration(values[idx])
	}

	return Latency{
		Count: len(values),
		Min:   time.Duration(values[0]),
		Mean:  time.Duration(total / int64(len(values))),
		P50:   at(0.50),
		P90:   at(0.90),
		P99:   at(0.99),
		P999:  at(0.999),
		Max:   time.Duration(values[len(values)-1]),
	}
}
