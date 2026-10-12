// Package projection tails the per-symbol consumer event log (the durable applied log the
// engine writes) and projects it into the database and the broker status cache.
//
// It is deliberately isolated from the engine: the engine only matches and logs, and this
// service turns that log into queryable state. The DB is the source of truth; the cache is
// a best-effort read layer that never blocks projection.
package projection

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"TradeIT/internal/ipc"
	"TradeIT/internal/metrics"
	"TradeIT/internal/models"
	"TradeIT/internal/symbol"
	"TradeIT/internal/wal"
)

// Checkpoint is how far one symbol's event log has been projected. Seq is the applied
// command sequence; Offset is the byte offset just past the last record it covers.
type Checkpoint struct {
	SymbolID uint32
	Seq      uint64
	Offset   int64
}

// Store persists projected entities and their checkpoints. Implemented by the Postgres
// layer, which commits the checkpoint in the same transaction as the batch so projection
// is exactly-once: a crash can never leave the checkpoint ahead of the data.
type Store interface {
	// GetOrder returns an order row, used to seed state for orders created before the
	// projector's current position.
	GetOrder(id uint64) (models.Order, bool, error)
	// LoadCheckpoints returns the last committed position per symbol, keyed by symbol id.
	LoadCheckpoints() (map[uint32]Checkpoint, error)
	// CommitBatch atomically writes the orders, trades and checkpoint for one symbol.
	CommitBatch(orders []models.Order, trades []models.TradeDetails, cp Checkpoint) error
}

// Cache holds order status for broker reads. Implemented by Redis.
type Cache interface {
	SetOrderStatus(orderID uint64, status string, remaining int, avgPrice int64) error
}

// WatermarkSink receives the projector's per-symbol committed sequence so it can be
// published to shared memory (for lag metrics and a future WAL truncation floor).
// *control.Region satisfies it. A nil sink disables publishing.
type WatermarkSink interface {
	SetWatermark(id uint32, seq uint64)
}

// Options configures the projector.
type Options struct {
	PollInterval  time.Duration
	BatchSize     int
	FlushInterval time.Duration
	// Region, when set, receives the projector's committed sequence per symbol.
	Region WatermarkSink
}

func (o Options) withDefaults() Options {
	if o.PollInterval <= 0 {
		o.PollInterval = 2 * time.Millisecond
	}
	if o.BatchSize <= 0 {
		o.BatchSize = 512
	}
	if o.FlushInterval <= 0 {
		o.FlushInterval = 5 * time.Millisecond
	}
	return o
}

// Projector tails one consumer WAL per symbol.
type Projector struct {
	table *symbol.Table
	store Store
	cache Cache
	opts  Options
	ch    []*tailer
}

type tailer struct {
	id     uint32
	name   string
	path   string
	reader *wal.Reader
	offset int64  // byte offset just past the last record read
	seq    uint64 // last applied command sequence (checkpoint identity)
	orders map[uint64]*orderState
	dirty  []uint64
	trades []models.TradeDetails
}

type orderState struct {
	order  models.Order
	filled int
	value  int64 // sum(price * quantity) over fills
}

// Open opens the projector over the per-symbol consumer WALs in dir. When the store holds
// a checkpoint for a symbol and the log has not been truncated below it, reading resumes
// at that offset; otherwise the log is replayed from the beginning. Either way the writes
// are idempotent, so a crash mid-run is safe.
func Open(dir string, table *symbol.Table, store Store, cache Cache, opts Options) (*Projector, error) {
	p := &Projector{table: table, store: store, cache: cache, opts: opts.withDefaults()}

	var cps map[uint32]Checkpoint
	if store != nil {
		var err error
		cps, err = store.LoadCheckpoints()
		if err != nil {
			return nil, err
		}
	}

	for id, name := range table.Names() {
		path := filepath.Join(dir, name+".consumer.wal")
		cp := cps[uint32(id)]
		start := cp.Offset
		if start > 0 {
			// Trust the offset only while the log still extends to it: a truncated or
			// rotated log invalidates byte offsets, so fall back to a full replay.
			if fi, err := os.Stat(path); err != nil || fi.Size() < start {
				start = 0
			}
		}
		var seq uint64
		if start > 0 {
			seq = cp.Seq
		}
		r, err := wal.OpenReaderAt(path, start)
		if err != nil {
			// A missing log (symbol never traded) is not fatal.
			continue
		}
		p.ch = append(p.ch, &tailer{
			id:     uint32(id),
			name:   name,
			path:   path,
			reader: r,
			offset: start,
			seq:    seq,
			orders: make(map[uint64]*orderState),
		})
	}
	return p, nil
}

// Close releases the readers.
func (p *Projector) Close() {
	for _, ch := range p.ch {
		if ch.reader != nil {
			ch.reader.Close()
		}
	}
}

// Run tails the logs until stop is closed.
func (p *Projector) Run(stop <-chan struct{}) {
	lastFlush := time.Now()
	for {
		select {
		case <-stop:
			p.flushAll()
			return
		default:
		}
		read := 0
		for _, ch := range p.ch {
			read += p.drainTail(ch)
		}
		if read == 0 {
			if time.Since(lastFlush) >= p.opts.FlushInterval {
				p.flushAll()
				lastFlush = time.Now()
			}
			time.Sleep(p.opts.PollInterval)
			continue
		}
		if time.Since(lastFlush) >= p.opts.FlushInterval {
			p.flushAll()
			lastFlush = time.Now()
		}
	}
}

// drainTail reads and processes up to BatchSize records from one tailer, returning how many
// it processed.
func (p *Projector) drainTail(ch *tailer) int {
	n := 0
	for n < p.opts.BatchSize {
		rec, err := ch.reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println("projection: read error on", ch.name, ":", err)
			return n
		}
		ch.offset = ch.reader.Offset()
		switch ipc.RecordKind(rec) {
		case ipc.RecordApplied:
			p.applyCommand(ch, rec[1:])
		case ipc.RecordTrade:
			p.applyTrade(ch, ipc.DecodeTrade(rec[1:]))
		}
		metrics.RecordsProjected.WithLabelValues(ch.name).Inc()
		n++
		if len(ch.dirty) >= p.opts.BatchSize || len(ch.trades) >= p.opts.BatchSize {
			p.flush(ch)
		}
	}
	return n
}

func (p *Projector) applyCommand(ch *tailer, rec []byte) {
	cmd, o := ipc.DecodeApplied(rec)
	// Advance the checkpoint identity even when the command has no visible effect (e.g.
	// a cancel for an order this projector never saw), so it is not reprocessed.
	ch.seq = cmd.Seq
	name, _ := p.table.Name(cmd.SymbolID)
	switch cmd.Op {
	case ipc.OpNew:
		order := models.Order{
			MetaOrder: models.MetaOrder{
				Id:         cmd.OrderID,
				User_id:    int(cmd.UserID),
				Order_Type: cmd.Typ.String(),
				Side:       cmd.Side.String(),
				Stock:      name,
				Price:      cmd.Price,
				Quantity:   int(cmd.Qty),
				Status:     statusFromOutcome(o.Status),
				Created_at: time.Unix(0, cmd.CreatedAt),
			},
			Remq: int(cmd.Qty) - int(o.FilledQty),
		}
		ch.orders[order.Id] = &orderState{
			order:  order,
			filled: int(o.FilledQty),
			value:  o.AvgPrice * int64(o.FilledQty),
		}
		ch.markDirty(order.Id)
	case ipc.OpCancel:
		st := p.stateOf(ch, cmd.OrderID)
		if st == nil {
			return
		}
		if o.Status == ipc.StatusPartiallyCancelled {
			st.order.Status = "partially_cancelled"
		} else {
			st.order.Status = "cancelled"
		}
		st.order.Remq = 0
		ch.markDirty(cmd.OrderID)
	case ipc.OpReduceQty, ipc.OpModify:
		st := p.stateOf(ch, cmd.OrderID)
		if st == nil {
			return
		}
		if cmd.Price > 0 {
			st.order.Price = cmd.Price
		}
		st.order.Quantity = int(cmd.Qty)
		st.order.Remq = st.order.Quantity - st.filled
		st.order.Status = fillStatus(st.filled, st.order.Quantity)
		ch.markDirty(cmd.OrderID)
	}
}

func (p *Projector) applyTrade(ch *tailer, t ipc.Trade) {
	for _, id := range []uint64{t.BidOrderID, t.AskOrderID} {
		st := p.stateOf(ch, id)
		if st == nil {
			continue
		}
		st.filled += int(t.Quantity)
		st.value += t.Price * int64(t.Quantity)
		st.order.Remq = st.order.Quantity - st.filled
		if st.filled > 0 {
			st.order.AvgPrice = st.value / int64(st.filled)
		}
		st.order.Status = fillStatus(st.filled, st.order.Quantity)
		ch.markDirty(id)
	}
	ch.trades = append(ch.trades, models.TradeDetails{
		TradeId:     t.TradeID,
		Stock:       ch.name,
		Buyer:       int(t.Buyer),
		Seller:      int(t.Seller),
		BidOrderID:  t.BidOrderID,
		AskOrderID:  t.AskOrderID,
		Quantity:    int(t.Quantity),
		Price:       t.Price,
		Executed_at: time.Unix(0, t.ExecutedAt),
	})
	metrics.TradesProjected.WithLabelValues(ch.name).Inc()
}

// stateOf returns the live state for an order, loading it from the store when it was
// created before the projector started (or before a checkpoint).
func (p *Projector) stateOf(ch *tailer, id uint64) *orderState {
	if st, ok := ch.orders[id]; ok {
		return st
	}
	if p.store == nil {
		return nil
	}
	order, found, err := p.store.GetOrder(id)
	if err != nil || !found {
		return nil
	}
	filled := order.Quantity - order.Remq
	st := &orderState{
		order:  order,
		filled: filled,
		// Seed the running fill value so a later trade computes a correct average: the
		// database row already reflects every fill up to the checkpoint.
		value: order.AvgPrice * int64(filled),
	}
	ch.orders[id] = st
	return st
}

func (ch *tailer) markDirty(id uint64) {
	for _, d := range ch.dirty {
		if d == id {
			return
		}
	}
	ch.dirty = append(ch.dirty, id)
}

func (p *Projector) flushAll() {
	for _, ch := range p.ch {
		p.flush(ch)
	}
}

func (p *Projector) flush(ch *tailer) {
	if len(ch.dirty) == 0 && len(ch.trades) == 0 {
		return
	}
	if p.store == nil {
		ch.dirty = ch.dirty[:0]
		ch.trades = ch.trades[:0]
		return
	}
	orders := make([]models.Order, 0, len(ch.dirty))
	for _, id := range ch.dirty {
		if st, ok := ch.orders[id]; ok {
			orders = append(orders, st.order)
		}
	}
	cp := Checkpoint{SymbolID: ch.id, Seq: ch.seq, Offset: ch.offset}
	// The checkpoint is committed in the same transaction as the data, so it can never
	// run ahead of it: after a crash the projector resumes exactly at this position.
	start := time.Now()
	if err := p.store.CommitBatch(orders, ch.trades, cp); err != nil {
		metrics.ProjectionErrors.Inc()
		log.Println("projection: commit failed on", ch.name, ":", err)
		return // keep dirty; retry next flush
	}
	metrics.ObserveDuration(metrics.ProjectionFlushSeconds, start)
	if p.cache != nil {
		for _, o := range orders {
			if err := p.cache.SetOrderStatus(o.Id, o.Status, o.Remq, o.AvgPrice); err != nil {
				metrics.CacheErrors.Inc()
				log.Println("projection: cache write failed for", o.Id, ":", err)
			}
		}
	}
	if p.opts.Region != nil {
		p.opts.Region.SetWatermark(ch.id, cp.Seq)
	}
	metrics.ProjectionSeq.WithLabelValues(ch.name).Set(float64(cp.Seq))
	ch.dirty = ch.dirty[:0]
	ch.trades = ch.trades[:0]
}

func fillStatus(filled, quantity int) string {
	switch {
	case filled <= 0:
		return "open"
	case filled >= quantity:
		return "filled"
	default:
		return "partially_filled"
	}
}

func statusFromOutcome(s ipc.Status) string {
	switch s {
	case ipc.StatusResting:
		return "open"
	case ipc.StatusFilled:
		return "filled"
	case ipc.StatusPartiallyFilled:
		return "partially_filled"
	case ipc.StatusCancelled:
		return "cancelled"
	case ipc.StatusPartiallyCancelled:
		return "partially_cancelled"
	case ipc.StatusRejected:
		return "rejected"
	default:
		return "accepted"
	}
}
