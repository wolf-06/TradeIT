// Package metrics holds the process-wide Prometheus metrics for the TradeIT services and
// serves them alongside lightweight health endpoints. Metrics are registered on the
// default registry, which already exposes the Go runtime and process collectors.
//
// The counters are intentionally labelled by symbol name so a dashboard can show
// per-symbol throughput and lag. The most useful derived signal is projection lag, which
// Grafana computes as tradeit_consumer_watermark - tradeit_projection_seq.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Producer / ingest side.
	OrdersSubmitted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tradeit_orders_submitted_total",
		Help: "Commands published to the ring, by symbol.",
	}, []string{"symbol"})

	RingFull = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tradeit_ring_full_total",
		Help: "Publish attempts rejected because the ring had no free slot, by symbol.",
	}, []string{"symbol"})

	WalSyncSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "tradeit_wal_sync_seconds",
		Help:    "Time to fsync one symbol's producer WAL.",
		Buckets: []float64{.00005, .0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1},
	}, []string{"symbol"})

	// Engine / consumer side.
	CommandsApplied = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tradeit_commands_applied_total",
		Help: "Commands applied to the book, by symbol.",
	}, []string{"symbol"})

	ConsumerWatermark = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "tradeit_consumer_watermark",
		Help: "Last sequence the engine has applied, by symbol.",
	}, []string{"symbol"})

	RingPending = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "tradeit_ring_pending",
		Help: "Records published but not yet applied (head - watermark), by symbol.",
	}, []string{"symbol"})

	// Projection side.
	RecordsProjected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tradeit_records_projected_total",
		Help: "Event-log records projected into the database, by symbol.",
	}, []string{"symbol"})

	TradesProjected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tradeit_trades_projected_total",
		Help: "Trades projected into the database, by symbol.",
	}, []string{"symbol"})

	ProjectionSeq = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "tradeit_projection_seq",
		Help: "Last sequence the projection service has committed, by symbol.",
	}, []string{"symbol"})

	ProjectionFlushSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "tradeit_projection_flush_seconds",
		Help:    "Time to commit one projection batch.",
		Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5},
	})

	ProjectionErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tradeit_projection_errors_total",
		Help: "Failed projection commits.",
	})

	CacheErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tradeit_cache_errors_total",
		Help: "Failed order-status cache writes.",
	})

	// HTTP side.
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tradeit_http_requests_total",
		Help: "HTTP requests by route, method and status.",
	}, []string{"route", "method", "status"})

	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "tradeit_http_request_seconds",
		Help:    "HTTP request latency by route.",
		Buckets: []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"route"})
)

// Handler serves the default Prometheus registry.
func Handler() http.Handler { return promhttp.Handler() }

// ObserveDuration is a small helper for timing a section in seconds.
func ObserveDuration(h prometheus.Observer, start time.Time) {
	h.Observe(time.Since(start).Seconds())
}

// Serve starts an HTTP server exposing /metrics, /healthz and /readyz. It returns
// immediately; the server runs for the life of the process. ready may be nil, in which
// case /readyz always reports ready.
func Serve(addr string, ready func() error) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if ready != nil {
			if err := ready(); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	go func() { _ = http.ListenAndServe(addr, mux) }()
}
