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
	"path/filepath"
	"time"

	"TradeIT/internal/ipc"
	"TradeIT/internal/models"
	"TradeIT/internal/symbol"
	"TradeIT/internal/wal"
)

// Store persists projected entities. Implemented by the Postgres layer.
type Store interface {
	UpsertOrders(orders []models.Order) error
	InsertTrades(trades []models.TradeDetails) error
	// GetOrder returns an order row, used to seed state for orders created before the
	// projector's current position.
	GetOrder(id uint64) (models.Order, bool, error)
}

// Cache holds order status for broker reads. Implemented by Redis.
type Cache interface {
	SetOrderStatus(orderID uint64, status string, remaining int, avgPrice int64) error
}

// Options configures the projector.
type Options struct {
	PollInterval  time.Duration
	BatchSize     int
	FlushInterval time.Duration
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
	offset int64
	orders map[uint64]*orderState
	dirty  []uint64
	trades []models.TradeDetails
}

type orderState struct {
	order  models.Order
	filled int
	value  int64 // sum(price * quantity) over fills
}

// Open opens the projector over the per-symbol consumer WALs in dir. It reads from the
// beginning of each log; a crash mid-run simply replays, and the idempotent writes make
// that safe.
func Open(dir string, table *symbol.Table, store Store, cache Cache, opts Options) (*Projector, error) {
	p := &Projector{table: table, store: store, cache: cache, opts: opts.withDefaults()}
	for id, name := range table.Names() {
		path := filepath.Join(dir, name+".consumer.wal")
		r, err := wal.OpenReaderAt(path, 0)
		if err != nil {
			// A missing log (symbol never traded) is not fatal.
			continue
		}
		p.ch = append(p.ch, &tailer{
			id:     uint32(id),
			name:   name,
			path:   path,
			reader: r,
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
		n++
		if len(ch.dirty) >= p.opts.BatchSize || len(ch.trades) >= p.opts.BatchSize {
			p.flush(ch)
		}
	}
	return n
}

func (p *Projector) applyCommand(ch *tailer, rec []byte) {
	cmd, o := ipc.DecodeApplied(rec)
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
	st := &orderState{order: order, filled: order.Quantity - order.Remq}
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
	if len(ch.dirty) > 0 && p.store != nil {
		orders := make([]models.Order, 0, len(ch.dirty))
		for _, id := range ch.dirty {
			if st, ok := ch.orders[id]; ok {
				orders = append(orders, st.order)
			}
		}
		if err := p.store.UpsertOrders(orders); err != nil {
			log.Println("projection: upsert orders failed on", ch.name, ":", err)
			return // keep dirty; retry next flush
		}
		if p.cache != nil {
			for _, o := range orders {
				if err := p.cache.SetOrderStatus(o.Id, o.Status, o.Remq, o.AvgPrice); err != nil {
					log.Println("projection: cache write failed for", o.Id, ":", err)
				}
			}
		}
	}
	if len(ch.trades) > 0 && p.store != nil {
		if err := p.store.InsertTrades(ch.trades); err != nil {
			log.Println("projection: insert trades failed on", ch.name, ":", err)
			return
		}
	}
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
