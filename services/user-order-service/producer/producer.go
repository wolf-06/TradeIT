// Package producer is the order service's write side of the ingest path. It builds
// command records from orders, appends them to the per-symbol producer WAL and publishes
// them to the ring.
//
// Durability is group-committed: a single goroutine owns every ring/WAL operation (the
// ring is single-producer by design). A submission may carry a batch of commands, so one
// channel round-trip and one flush serve many orders; the flush fsyncs only the symbols
// that actually received records. A submission returns only after the flush that covers
// it, so a returned Submit is durable - which is what lets the HTTP layer answer 202
// honestly.
package producer

import (
	"errors"
	"sync"
	"time"

	"github.com/svicknesh/tsid"

	"TradeIT/internal/ingest"
	"TradeIT/internal/ipc"
	"TradeIT/internal/models"
	"TradeIT/internal/symbol"
)

// ErrClosed is returned when submitting after Close.
var ErrClosed = errors.New("producer: closed")

type resp struct {
	seqs []uint64
	err  error
}

type req struct {
	cmds   []ipc.Cmd
	replay []uint64 // non-nil => replay request
	resp   chan resp
}

// OrderItem is one order to submit in a batch.
type OrderItem struct {
	Order    models.MetaOrder
	SymbolID uint32
}

// SubmittedOrder is the result of submitting one order: its id and the per-symbol ring
// sequence it was assigned.
type SubmittedOrder struct {
	OrderID uint64
	Seq     uint64
}

// Producer serialises command submission through a single goroutine that owns the WAL and
// the ring, and group-commits the WAL.
type Producer struct {
	ring  *ingest.Producer
	table *symbol.Table
	ids   *tsid.TSID
	idsMu sync.Mutex

	cmds chan req
	stop chan struct{}
	done chan struct{}

	syncEveryN int
	linger     time.Duration
	closeOnce  sync.Once
}

// Open opens (creating if needed) the per-symbol ring and producer WAL under dir and
// starts the commit goroutine. syncEveryN bounds the group-commit batch size; syncEveryMs
// is the linger after which a partial batch is flushed anyway.
func Open(dir string, table *symbol.Table, capacity, syncEveryN, syncEveryMs int) (*Producer, error) {
	rp, err := ingest.OpenProducer(dir, table, capacity)
	if err != nil {
		return nil, err
	}
	p := &Producer{
		ring:       rp,
		table:      table,
		ids:        tsid.NewDefault(),
		cmds:       make(chan req, 4096),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
		syncEveryN: syncEveryN,
		linger:     time.Duration(syncEveryMs) * time.Millisecond,
	}
	if p.linger <= 0 {
		p.linger = time.Millisecond
	}
	go p.loop()
	return p, nil
}

func (p *Producer) loop() {
	defer close(p.done)
	var (
		pending []req
		count   int
		lingerC <-chan time.Time
		lingerT *time.Timer
	)
	flush := func() {
		if len(pending) == 0 {
			return
		}
		results := make([]resp, len(pending))
		dirty := make(map[uint32]struct{})
		for ri := range pending {
			cmds := pending[ri].cmds
			// Reserve space up front so a batch is all-or-nothing.
			need := make(map[uint32]int, 4)
			for ci := range cmds {
				need[cmds[ci].SymbolID]++
			}
			short := false
			for id, n := range need {
				if p.ring.FreeFor(id) < uint64(n) {
					short = true
					break
				}
			}
			if short {
				results[ri] = resp{err: ingest.ErrRingFull}
				continue
			}
			seqs := make([]uint64, len(cmds))
			for ci := range cmds {
				seq, err := p.ring.Publish(cmds[ci])
				if err != nil {
					results[ri] = resp{err: err}
					break
				}
				seqs[ci] = seq
				dirty[cmds[ci].SymbolID] = struct{}{}
			}
			if results[ri].err == nil {
				results[ri].seqs = seqs
			}
		}
		var syncErr error
		for id := range dirty {
			if err := p.ring.SyncSymbol(id); err != nil {
				syncErr = err
				break
			}
		}
		for ri := range pending {
			res := results[ri]
			if res.err == nil {
				res.err = syncErr
			}
			pending[ri].resp <- res
		}
		pending = pending[:0]
		count = 0
		if lingerT != nil {
			lingerT.Stop()
			lingerT = nil
			lingerC = nil
		}
	}
	for {
		select {
		case r := <-p.cmds:
			if r.replay != nil {
				// Replay runs on this goroutine so the ring keeps its single writer.
				flush()
				r.resp <- resp{err: p.ring.Replay(r.replay)}
				continue
			}
			pending = append(pending, r)
			count += len(r.cmds)
			if p.syncEveryN > 0 && count >= p.syncEveryN {
				flush()
			} else if lingerC == nil {
				lingerT = time.NewTimer(p.linger)
				lingerC = lingerT.C
			}
		case <-lingerC:
			flush()
		case <-p.stop:
			flush()
			return
		}
	}
}

func (p *Producer) submitBatch(cmds []ipc.Cmd) ([]uint64, error) {
	if len(cmds) == 0 {
		return nil, nil
	}
	respCh := make(chan resp, 1)
	select {
	case p.cmds <- req{cmds: cmds, resp: respCh}:
	case <-p.stop:
		return nil, ErrClosed
	}
	r := <-respCh
	return r.seqs, r.err
}

func (p *Producer) nextID() uint64 {
	p.idsMu.Lock()
	defer p.idsMu.Unlock()
	return p.ids.Generate().Number
}

// SymbolID resolves a symbol name to its id, or reports it is unknown.
func (p *Producer) SymbolID(name string) (uint32, bool) { return p.table.ID(name) }

// Replay re-publishes orders from the producer WAL that the consumer has not applied
// (sequence greater than from[id]) into the ring. It runs on the commit goroutine so the
// ring keeps its single-writer invariant.
func (p *Producer) Replay(from []uint64) error {
	respCh := make(chan resp, 1)
	select {
	case p.cmds <- req{replay: from, resp: respCh}:
	case <-p.stop:
		return ErrClosed
	}
	return (<-respCh).err
}

// SubmitOrders publishes a batch of new orders. One channel round-trip and one flush
// serve the whole batch. It returns each order's id (assigning a TSID when an order has
// none) and ring sequence.
func (p *Producer) SubmitOrders(items []OrderItem) ([]SubmittedOrder, error) {
	cmds := make([]ipc.Cmd, len(items))
	out := make([]SubmittedOrder, len(items))
	for i := range items {
		o := items[i].Order
		orderID := o.Id
		if orderID == 0 {
			orderID = p.nextID()
		}
		out[i].OrderID = orderID
		cmds[i] = ipc.Cmd{
			RequestID: orderID,
			OrderID:   orderID,
			UserID:    int64(o.User_id),
			SymbolID:  items[i].SymbolID,
			Price:     o.Price,
			Qty:       int32(o.Quantity),
			Remq:      int32(o.Quantity),
			Op:        ipc.OpNew,
			Side:      sideOf(o.Side),
			Typ:       typOf(o.Order_Type),
		}
	}
	seqs, err := p.submitBatch(cmds)
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].Seq = seqs[i]
	}
	return out, nil
}

// SubmitOrder publishes a single new order and returns its order id.
func (p *Producer) SubmitOrder(o models.MetaOrder, symbolID uint32) (uint64, error) {
	out, err := p.SubmitOrders([]OrderItem{{Order: o, SymbolID: symbolID}})
	if err != nil {
		return 0, err
	}
	return out[0].OrderID, nil
}

// Cancel publishes an OpCancel for a resting order.
func (p *Producer) Cancel(orderID uint64, symbolID uint32) (uint64, error) {
	seqs, err := p.submitBatch([]ipc.Cmd{{
		RequestID: p.nextID(), OrderID: orderID, SymbolID: symbolID, Op: ipc.OpCancel,
	}})
	if err != nil {
		return 0, err
	}
	return seqs[0], nil
}

// ReduceQty publishes an in-place quantity reduction (new total quantity).
func (p *Producer) ReduceQty(orderID uint64, symbolID uint32, newQty int32) (uint64, error) {
	seqs, err := p.submitBatch([]ipc.Cmd{{
		RequestID: p.nextID(), OrderID: orderID, SymbolID: symbolID, Qty: newQty, Op: ipc.OpReduceQty,
	}})
	if err != nil {
		return 0, err
	}
	return seqs[0], nil
}

// Modify publishes a combined price/quantity change (new total quantity).
func (p *Producer) Modify(orderID uint64, symbolID uint32, newPrice int64, newQty int32) (uint64, error) {
	seqs, err := p.submitBatch([]ipc.Cmd{{
		RequestID: p.nextID(), OrderID: orderID, SymbolID: symbolID, Price: newPrice, Qty: newQty, Op: ipc.OpModify,
	}})
	if err != nil {
		return 0, err
	}
	return seqs[0], nil
}

// Close stops the commit goroutine (flushing pending records) and closes the ring.
func (p *Producer) Close() error {
	p.closeOnce.Do(func() { close(p.stop) })
	<-p.done
	return p.ring.Close()
}

func sideOf(s string) ipc.Side {
	if s == "sell" {
		return ipc.SideSell
	}
	return ipc.SideBuy
}

func typOf(t string) ipc.OrderType {
	if t == "market" {
		return ipc.TypeMarket
	}
	return ipc.TypeLimit
}
