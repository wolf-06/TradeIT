package engine

import (
	"log"
	"time"

	"TradeIT/internal/config"
	"TradeIT/internal/ingest"
)

// RingConsumer drains the shared-memory rings and applies commands to a ledger. It is the
// ring-mode replacement for OrderEngine.ProcessOrderQueue.
type RingConsumer struct {
	consumer *ingest.Consumer
	batch    int
}

// OpenRingConsumer opens the rings named by cfg and binds them to ledger.
func OpenRingConsumer(cfg *config.Config, ledger *Ledger) (*RingConsumer, error) {
	table, err := cfg.SymbolTable()
	if err != nil {
		return nil, err
	}
	c, err := ingest.OpenConsumer(cfg.Ingest.Dir, table, NewRingHandler(ledger, table))
	if err != nil {
		return nil, err
	}
	batch := cfg.Ingest.DrainBatch
	if batch <= 0 {
		batch = 1024
	}
	return &RingConsumer{consumer: c, batch: batch}, nil
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
		if n == 0 {
			time.Sleep(100 * time.Microsecond)
		}
	}
}

// Close releases the rings and WALs.
func (rc *RingConsumer) Close() error { return rc.consumer.Close() }
