package engine

import (
	"log"
	"time"

	"TradeIT/internal/config"
	"TradeIT/internal/control"
	"TradeIT/internal/ingest"
	"TradeIT/internal/snapshot"
)

// RingConsumer drains the shared-memory rings and applies commands to a ledger. It is the
// engine's input path. It also publishes its per-symbol watermark to the shared control
// region so the producer knows where to resume replaying after a restart.
type RingConsumer struct {
	consumer *ingest.Consumer
	region   *control.Region
	batch    int
}

// OpenRingConsumer opens the rings named by cfg and binds them to ledger. region may be
// nil, in which case watermarks are not published.
func OpenRingConsumer(cfg *config.Config, ledger *Ledger, region *control.Region) (*RingConsumer, error) {
	table, err := cfg.SymbolTable()
	if err != nil {
		return nil, err
	}
	opts := ingest.Options{
		SnapshotDir:     cfg.Ingest.Dir,
		SnapshotEveryN:  cfg.Ingest.SnapshotEveryN,
		SnapshotEveryMs: cfg.Ingest.SnapshotEveryMs,
		Codec:           snapshot.ParseCodec(cfg.Ingest.SnapshotCodec),
	}
	c, err := ingest.OpenConsumerWithOptions(cfg.Ingest.Dir, table, NewRingHandler(ledger, table), opts)
	if err != nil {
		return nil, err
	}
	batch := cfg.Ingest.DrainBatch
	if batch <= 0 {
		batch = 1024
	}
	ringMode = true
	return &RingConsumer{consumer: c, region: region, batch: batch}, nil
}

// Run drains the rings until the process exits. When a pass finds nothing it yields
// briefly rather than spinning on a hot core.
func (rc *RingConsumer) Run() {
	for {
		n, err := rc.consumer.DrainOnce(rc.batch)
		if err != nil {
			log.Println("ring consumer: drain error:", err)
			continue
		}
		rc.publishWatermarks()
		if n == 0 {
			time.Sleep(100 * time.Microsecond)
		}
	}
}

// publishWatermarks mirrors the consumer's progress into the shared control region.
func (rc *RingConsumer) publishWatermarks() {
	if rc.region == nil {
		return
	}
	for id, seq := range rc.consumer.Watermarks() {
		if seq > rc.region.Watermark(uint32(id)) {
			rc.region.SetWatermark(uint32(id), seq)
		}
	}
}

// Close releases the rings and WALs.
func (rc *RingConsumer) Close() error { return rc.consumer.Close() }
