package benchmark

import (
	"os"
	"strconv"
	"testing"
	"time"
)

// defaultLatencyStride is how many operations pass between timed samples. A clock read
// costs roughly 20-30 ns through the vDSO, so timing every order would add ~10-25% to a
// ~230 ns/order match and distort the throughput figure the same benchmark reports. One
// sample in N bounds that overhead while still yielding orders/N samples for the tail.
const defaultLatencyStride = 16

// latencyStride returns the sampling stride. It is overridable with
// BENCH_LATENCY_STRIDE so a cloud run can trade throughput fidelity for tail resolution
// (set 1 to time every operation).
func latencyStride() int {
	if v := os.Getenv("BENCH_LATENCY_STRIDE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 {
			return n
		}
	}
	return defaultLatencyStride
}

// latencySampler records the latency of a sampled subset of operations so a benchmark can
// report a latency distribution (mean/p50/p90/p99/p99.9/max) next to its throughput.
//
// Sampling is deterministic: every stride-th operation is timed. The caller brackets each
// operation with begin/end; unsampled operations pay only a counter increment and a
// branch, so the clock is read only for the sampled ones.
type latencySampler struct {
	stride  int
	n       int
	samples []time.Duration
	start   time.Time
	timing  bool
}

func newLatencySampler(stride int) *latencySampler {
	if stride < 1 {
		stride = 1
	}
	return &latencySampler{stride: stride}
}

// begin marks the start of the next operation, timing it only if it is sampled.
func (s *latencySampler) begin() {
	s.timing = s.n%s.stride == 0
	if s.timing {
		s.start = time.Now()
	}
	s.n++
}

// end records the latency of the operation bracketed by the matching begin. It is a
// no-op for operations that were not sampled.
func (s *latencySampler) end() {
	if !s.timing {
		return
	}
	s.samples = append(s.samples, time.Since(s.start))
	s.timing = false
}

// report publishes the sampled distribution to b. unit names the measured operation
// (e.g. "order" or "batch") and becomes the metric suffix, so the Go benchmark line
// carries p50-ns/order, p99-ns/order, ... next to orders/sec.
func (s *latencySampler) report(b *testing.B, unit string) {
	lat := SummarizeLatency(s.samples)
	if lat.Count == 0 {
		return
	}
	ns := func(d time.Duration) float64 { return float64(d.Nanoseconds()) }
	b.ReportMetric(ns(lat.Mean), "mean-ns/"+unit)
	b.ReportMetric(ns(lat.P50), "p50-ns/"+unit)
	b.ReportMetric(ns(lat.P90), "p90-ns/"+unit)
	b.ReportMetric(ns(lat.P99), "p99-ns/"+unit)
	b.ReportMetric(ns(lat.P999), "p999-ns/"+unit)
	b.ReportMetric(ns(lat.Max), "max-ns/"+unit)
}
