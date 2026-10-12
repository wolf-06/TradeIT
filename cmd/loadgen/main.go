// Command loadgen drives the user-order service with a burst of orders and then verifies
// that they become queryable. It is the tool behind the demo and the smoke test: it
// measures sustained throughput and end-to-end latency, and it checks that the projection
// service has caught up by reading orders back through GET /order/:id.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type orderReq struct {
	OrderType string  `json:"orderType"`
	Side      string  `json:"type"`
	Stock     string  `json:"stock"`
	Price     float64 `json:"price"`
	Quantity  int     `json:"quantity"`
}

type orderResp struct {
	Status  string `json:"status"`
	OrderID uint64 `json:"orderid"`
	Seq     uint64 `json:"seq"`
}

func main() {
	base := flag.String("base", "http://localhost:8000", "user-order-service base URL")
	n := flag.Int("n", 10000, "number of orders to place")
	c := flag.Int("c", 64, "concurrency")
	symbol := flag.String("symbol", "AAPL", "symbol to trade")
	email := flag.String("email", "", "account email (default a unique loadgen address)")
	password := flag.String("password", "secret123", "account password")
	verify := flag.Int("verify", 200, "how many placed orders to read back via GET /order/:id")
	verifyAll := flag.Bool("verify-all", false, "read back every accepted order (overrides -verify)")
	verifyTimeout := flag.Duration("verify-timeout", 30*time.Second, "how long to wait for read-back")
	flag.Parse()

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: *c + 8,
			MaxConnsPerHost:     *c + 8,
		},
	}

	if *email == "" {
		*email = fmt.Sprintf("loadgen-%d@example.com", time.Now().UnixNano())
	}
	token, err := auth(client, *base, *email, *password)
	if err != nil {
		fmt.Fprintln(os.Stderr, "auth:", err)
		os.Exit(1)
	}
	fmt.Printf("loadgen: authenticated as %s\n", *email)

	lats := make([]time.Duration, *n)
	ids := make([]uint64, *n)
	var next, failed int64

	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *c; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(atomic.AddInt64(&next, 1)) - 1
				if i >= *n {
					return
				}
				side := "buy"
				if i%2 == 1 {
					side = "sell"
				}
				// Buy and sell at the same price so orders cross and generate trades.
				req := orderReq{OrderType: "limit", Side: side, Stock: *symbol, Price: 100.00, Quantity: 10}
				t0 := time.Now()
				body, err := postJSON(client, *base+"/order/create", token, req)
				if err != nil {
					atomic.AddInt64(&failed, 1)
					continue
				}
				lats[i] = time.Since(t0)
				var or orderResp
				if json.Unmarshal(body, &or) == nil {
					ids[i] = or.OrderID
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	// Keep only successful latencies for the percentiles.
	ok := make([]time.Duration, 0, *n)
	for _, d := range lats {
		if d > 0 {
			ok = append(ok, d)
		}
	}
	placed := len(ok)
	rate := float64(placed) / elapsed.Seconds()
	fmt.Printf("placed: %d/%d in %s (%.0f orders/sec), failed: %d\n", placed, *n, elapsed.Round(time.Millisecond), rate, failed)
	fmt.Printf("latency: p50=%s p95=%s p99=%s\n",
		percentile(ok, 50).Round(time.Microsecond),
		percentile(ok, 95).Round(time.Microsecond),
		percentile(ok, 99).Round(time.Microsecond))

	// A recovery or smoke check must not pass when requests were rejected.
	if failed > 0 {
		fmt.Fprintf(os.Stderr, "loadgen: %d of %d requests failed\n", failed, *n)
		os.Exit(1)
	}
	if *verify <= 0 && !*verifyAll {
		return
	}
	found, want := verifyOrders(client, *base, token, ids, *verify, *verifyAll, *c, *verifyTimeout)
	if found < want {
		fmt.Fprintf(os.Stderr, "verify: only %d/%d orders were queryable within %s\n", found, want, *verifyTimeout)
		os.Exit(1)
	}
}

// auth registers (ignoring a duplicate-email conflict) and logs in, returning a token.
func auth(client *http.Client, base, email, password string) (string, error) {
	reg := map[string]string{"name": "Load Generator", "email": email, "password": password}
	_, _ = postJSON(client, base+"/register", "", reg) // a 409 just means we can log in

	body, err := postJSON(client, base+"/login", "", map[string]string{"email": email, "password": password})
	if err != nil {
		return "", err
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", err
	}
	if out.Token == "" {
		return "", fmt.Errorf("empty token in %s", string(body))
	}
	return out.Token, nil
}

// verifyOrders reads accepted orders back through GET /order/:id. By default it samples
// the first `want` accepted ids; with all=true it verifies every accepted id. Each round
// checks the ids concurrently and it repeats until they are all visible or the timeout
// elapses, returning how many were found.
func verifyOrders(client *http.Client, base, token string, ids []uint64, want int, all bool, concurrency int, timeout time.Duration) (int, int) {
	sample := make([]uint64, 0, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		sample = append(sample, id)
		if !all && len(sample) >= want {
			break
		}
	}
	total := len(sample)
	if total == 0 {
		return 0, 0
	}
	deadline := time.Now().Add(timeout)
	start := time.Now()
	found := 0
	for {
		found = countVisible(client, base, token, sample, concurrency)
		if found == total || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Printf("verify: %d/%d orders visible via GET /order/:id after %s\n",
		found, total, time.Since(start).Round(time.Millisecond))
	return found, total
}

// countVisible checks every id once, in parallel, and returns how many were reachable.
func countVisible(client *http.Client, base, token string, ids []uint64, concurrency int) int {
	if concurrency < 1 {
		concurrency = 1
	}
	var found, next int64
	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(atomic.AddInt64(&next, 1)) - 1
				if i >= len(ids) {
					return
				}
				if statusOK(client, base, token, ids[i]) {
					atomic.AddInt64(&found, 1)
				}
			}
		}()
	}
	wg.Wait()
	return int(found)
}

func statusOK(client *http.Client, base, token string, id uint64) bool {
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/order/%d", base, id), nil)
	req.Header.Set("Authorization", token)
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusOK
}

func postJSON(client *http.Client, url, token string, payload any) ([]byte, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return body, fmt.Errorf("%s: %s", resp.Status, string(body))
	}
	return body, nil
}

func percentile(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := make([]time.Duration, len(d))
	copy(s, d)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	idx := int(math.Ceil(p/100*float64(len(s)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(s) {
		idx = len(s) - 1
	}
	return s[idx]
}
