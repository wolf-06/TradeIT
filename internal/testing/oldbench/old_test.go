// Package oldbench restores the original single-orderbook Matcher benchmark that used to
// live at the repository root as test.go, deleted in 618475b ("Restructured for the
// Micro-Services architecture").
//
// roundToTwoDecimal, generateOrders and main_ are kept as they were written. Only what was
// needed to make the script execute again has been added or changed:
//
//   - import paths updated to the post-restructure layout
//     (TradeIT/internal/models, TradeIT/services/engine-service/engine);
//   - engine.PersistenceEnabled is switched off in TestMain. The matcher registers trades
//     and updates order status from goroutines, and the original script never initialised a
//     database, so unmodified it now dies in trade_db.Save with a nil-pointer panic
//     (trade.go:73). With persistence off the run measures the matcher, which is what the
//     script was written for;
//   - the engine's per-order logging is drained into a bounded buffer instead of being
//     printed. Orderbook.Matcher logs several lines per order (~345 bytes/order, i.e.
//     ~345 MB for the 1M orders this script generates), which would flood stdout for
//     minutes without telling anything about matching.
//
// The package is separate from internal/testing on purpose: switching the global
// persistence flag there would break the database-backed tests in that package.
//
// Run it with:
//
//	go test -run TestOldMatcherBenchmark -v ./internal/testing/oldbench/
//
// It keeps the script's original one-shot timing (no b.N loop), so its number can be
// compared with what the script printed before it was removed.
package oldbench

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"TradeIT/internal/models"
	"TradeIT/services/engine-service/engine"
)

// TestMain switches persistence off once for this test binary, before anything is matched.
// Matcher spawns its trade/status writers in goroutines that outlive the call, so flipping
// the flag per test and restoring it would race with a late writer.
func TestMain(m *testing.M) {
	engine.PersistenceEnabled = false
	engine.InitTrade()
	os.Exit(m.Run())
}

// roundToTwoDecimal is kept verbatim from the original script, including its rounding to a
// single decimal place (math.Round(num*10)/10 rather than *100) despite the name.
func roundToTwoDecimal(num float64) float64 {
	return float64((math.Round(num*10) / 10))
}

// generateOrders is kept verbatim from the original script: one stock, 40% sells, ~40%
// market orders, prices 90-110 and quantities 1-100. The original's
// rand.New(rand.NewSource(...)) line discarded its result; the global source is randomly
// seeded since Go 1.20 anyway, so the flow differs run to run either way.
func generateOrders(n uint64) []models.Order {
	orders := make([]models.Order, 0, n)
	var i uint64
	var limitCount, marketCount int
	var sellOrders, buyOrders int
	for i = 0; i < n; i++ {
		orderSide := "buy"
		orderType := "limit"
		if rand.Float64() < 0.4 {
			orderSide = "sell"
			sellOrders++
		} else {
			buyOrders++
		}
		if rand.Float32() > 0.6 {
			orderType = "market"
			marketCount++
		} else {
			limitCount++
		}
		price := 90 + rand.Float64()*10 // Prices between 90 and 110
		quantity := rand.Intn(100) + 1  // Quantity between 1 and 100
		order := models.Order{
			MetaOrder: models.MetaOrder{
				Id:         i,
				User_id:    rand.Intn(1000),
				Order_Type: orderType,
				Side:       orderSide,
				Stock:      "TEST",
				Price:      models.PriceToInt(price),
				Quantity:   quantity,
				Status:     "pending",
				Created_at: time.Now(),
			},
			Remq: quantity,
		}
		orders = append(orders, order)
	}
	fmt.Println("Limit orders: ", limitCount, " | Market Orders ", marketCount)
	fmt.Println("Buy orders: ", buyOrders, " | Sell orders: ", sellOrders)
	return orders
}

// main_ is kept verbatim from the original script, name included: it was a helper rather
// than main because the repository root also had a main.go at the time.
func main_() {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	ob := engine.InitOrderBook()
	orders := generateOrders(1000000) //generate orders
	fmt.Println("Starting the Matching")
	start := time.Now()
	for _, order := range orders {
		ob.Matcher(order)
	}
	elapsed := time.Since(start)
	fmt.Printf("Processed %d orders in %s\n", len(orders), elapsed)
	ob.DisplayResult()
}

// capture is a bounded io.Writer that keeps the first windowMax bytes and the last
// windowMax bytes of everything written to it, plus a byte count. The script's banner is at
// the very start and its "Processed ... in ..." line plus DisplayResult are at the very
// end, so both ends are all that is worth keeping; the engine's per-order log in between is
// counted and dropped, which keeps memory constant even for millions of log lines.
type capture struct {
	windowMax int
	head      []byte
	tail      []byte
	total     int64
}

func newCapture(windowMax int) *capture {
	return &capture{windowMax: windowMax}
}

func (c *capture) Write(p []byte) (int, error) {
	c.total += int64(len(p))

	if room := c.windowMax - len(c.head); room > 0 {
		if room > len(p) {
			room = len(p)
		}
		c.head = append(c.head, p[:room]...)
	}

	c.tail = append(c.tail, p...)
	if len(c.tail) > c.windowMax {
		c.tail = c.tail[len(c.tail)-c.windowMax:]
	}
	return len(p), nil
}

// captureWindow is how many bytes are kept from each end of the captured stdout.
const captureWindow = 1024

// captureMode selects how the engine's stdout is treated during the run:
//
//	"capture" (default) - drain it through a pipe into a bounded head+tail buffer so the
//	                      script's own output can be shown, and report the volume dropped;
//	"discard"           - send it straight to the null device. Cheapest option, and the
//	                      script's own prints are lost, but it gives the log-free wall
//	                      clock for exactly the same workload.
//
// The engine formats and writes ~330 bytes for every order it matches, which on a 1M order
// run is ~330 MB, so the two modes measure noticeably different things.
func captureMode() string {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("OLD_BENCH_LOG")), "discard") {
		return "discard"
	}
	return "capture"
}

// runDiscardingLog gives the log-free number: main_() runs and its own output goes to the
// null device, so only the wall clock survives. generateOrders accounts for ~0.2s of it at
// 1M orders; the script's own matcher-only timing is in the default capture mode.
func runDiscardingLog(t *testing.T) {
	devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("cannot open %s: %v", os.DevNull, err)
	}

	original := os.Stdout
	os.Stdout = devnull
	started := time.Now()
	main_()
	wall := time.Since(started)
	os.Stdout = original
	if err := devnull.Close(); err != nil {
		t.Logf("warning: closing %s failed: %v", os.DevNull, err)
	}

	fmt.Printf("\n===== restored script (test.go), engine log discarded =====\n")
	fmt.Printf("--- wall clock for main_() (generateOrders + matching): %s => %.0f orders/sec ---\n",
		wall, float64(1_000_000)/wall.Seconds())
	fmt.Printf("--- stdout went to %s, so the script's own prints are not shown in this mode ---\n", os.DevNull)
}

// TestOldMatcherBenchmark runs the restored script and reports what it produced.
func TestOldMatcherBenchmark(t *testing.T) {
	if captureMode() == "discard" {
		runDiscardingLog(t)
		return
	}

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("cannot create the stdout pipe: %v", err)
	}
	sink := newCapture(captureWindow)
	drained := make(chan struct{})
	go func() {
		_, _ = io.Copy(sink, reader)
		close(drained)
	}()

	original := os.Stdout
	os.Stdout = writer
	started := time.Now()
	main_()
	wall := time.Since(started)
	os.Stdout = original

	// Closing the write end lets io.Copy see EOF and finish.
	if err := writer.Close(); err != nil {
		t.Logf("warning: closing the stdout pipe failed: %v", err)
	}
	<-drained
	if err := reader.Close(); err != nil {
		t.Logf("warning: closing the stdout reader failed: %v", err)
	}

	fmt.Printf("\n===== restored script (test.go) output =====\n")
	fmt.Printf("--- head of stdout (%d bytes) ---\n", len(sink.head))
	fmt.Print(string(sink.head))
	fmt.Printf("\n--- tail of stdout (%d bytes) ---\n", len(sink.tail))
	fmt.Printf("%s\n", strings.TrimRight(string(sink.tail), "\n"))
	fmt.Printf("\n--- engine log drained: %.1f MB (%.0f bytes/order) ---\n",
		float64(sink.total)/(1024*1024), float64(sink.total)/1_000_000)
	fmt.Printf("--- wall clock for main_() (generateOrders + matching): %s ---\n", wall)

	if d, ok := parseProcessed(sink.tail); ok {
		fmt.Printf("--- script's own matcher-only timing: %s => %.0f orders/sec ---\n",
			d, float64(1_000_000)/d.Seconds())
	} else {
		t.Log("could not read the script's 'Processed ... in ...' line back from the tail window")
	}
}

// parseProcessed extracts the duration from the script's own "Processed N orders in D"
// line, where D is a time.Duration string such as "9.54s" or "1m2.3s".
func parseProcessed(b []byte) (time.Duration, bool) {
	const marker = "Processed "
	i := bytes.Index(b, []byte(marker))
	if i < 0 {
		return 0, false
	}
	parts := strings.SplitN(string(b[i+len(marker):]), " orders in ", 2)
	if len(parts) != 2 {
		return 0, false
	}
	fields := strings.Fields(parts[1])
	if len(fields) == 0 {
		return 0, false
	}
	d, err := time.ParseDuration(fields[0])
	if err != nil {
		return 0, false
	}
	return d, true
}
