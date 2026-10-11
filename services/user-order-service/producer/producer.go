// Package producer is the order service's write side of the ingest path. It builds
// command records from orders, appends them to the per-symbol producer WAL and publishes
// them to the ring.
//
// Durability is group-committed: a single goroutine owns every ring/WAL operation (the
// ring is single-producer by design), batches fsyncs, and only answers a Submit after the
// flush that covers its record has completed. So a Submit that returns without error is
// durable, which is what lets the HTTP layer answer 202 honestly.
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

type submitResp struct {
	seq uint64
	err error
}

type waitReq struct {
	cmd  ipc.Cmd
	seq  uint64
	resp chan submitResp
}

// Producer serialises command submission through a single goroutine that owns the WAL
// and the ring, and group-commits the WAL.
type Producer struct {
	ring  *ingest.Producer
	table *symbol.Table
	ids   *tsid.TSID

	cmds chan waitReq
	stop chan struct{}
	done chan struct{}

	syncEveryN  int
	syncEveryMs time.Duration
	closeOnce   sync.Once
}

// Open opens (creating if needed) the per-symbol ring and producer WAL under dir and
// starts the commit goroutine. syncEveryN and syncEveryMs bound the group commit.
func Open(dir string, table *symbol.Table, capacity, syncEveryN, syncEveryMs int) (*Producer, error) {
	rp, err := ingest.OpenProducer(dir, table, capacity)
	if err != nil {
		return nil, err
	}
	p := &Producer{
		ring:        rp,
		table:       table,
		ids:         tsid.NewDefault(),
		cmds:        make(chan waitReq, 4096),
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
		syncEveryN:  syncEveryN,
		syncEveryMs: time.Duration(syncEveryMs) * time.Millisecond,
	}
	if p.syncEveryMs <= 0 {
		p.syncEveryMs = 5 * time.Millisecond
	}
	go p.loop()
	return p, nil
}

func (p *Producer) loop() {
	defer close(p.done)
	var waiting []waitReq
	ticker := time.NewTicker(p.syncEveryMs)
	defer ticker.Stop()

	flush := func() {
		if len(waiting) == 0 {
			return
		}
		err := p.ring.Sync()
		for _, w := range waiting {
			w.resp <- submitResp{seq: w.seq, err: err}
		}
		waiting = waiting[:0]
	}

	for {
		select {
		case req := <-p.cmds:
			seq, err := p.ring.Publish(req.cmd)
			if err != nil {
				req.resp <- submitResp{err: err}
				continue
			}
			req.seq = seq
			waiting = append(waiting, req)
			if p.syncEveryN > 0 && len(waiting) >= p.syncEveryN {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-p.stop:
			flush()
			return
		}
	}
}

// submit hands one command to the commit goroutine and waits until it is durable.
func (p *Producer) submit(c ipc.Cmd) (uint64, error) {
	resp := make(chan submitResp, 1)
	select {
	case p.cmds <- waitReq{cmd: c, resp: resp}:
	case <-p.stop:
		return 0, ErrClosed
	}
	r := <-resp
	return r.seq, r.err
}

// SymbolID resolves a symbol name to its id, or reports it is unknown.
func (p *Producer) SymbolID(name string) (uint32, bool) { return p.table.ID(name) }

// SubmitOrder assigns an order id (TSID) if the order has none, then publishes an OpNew
// command. It returns the order id.
func (p *Producer) SubmitOrder(o models.MetaOrder, symbolID uint32) (uint64, error) {
	orderID := o.Id
	if orderID == 0 {
		orderID = p.ids.Generate().Number
	}
	side := ipc.SideBuy
	if o.Side == "sell" {
		side = ipc.SideSell
	}
	typ := ipc.TypeLimit
	if o.Order_Type == "market" {
		typ = ipc.TypeMarket
	}
	if _, err := p.submit(ipc.Cmd{
		RequestID: orderID,
		OrderID:   orderID,
		UserID:    int64(o.User_id),
		SymbolID:  symbolID,
		Price:     o.Price,
		Qty:       int32(o.Quantity),
		Remq:      int32(o.Quantity),
		Op:        ipc.OpNew,
		Side:      side,
		Typ:       typ,
	}); err != nil {
		return 0, err
	}
	return orderID, nil
}

// Cancel publishes an OpCancel for a resting order.
func (p *Producer) Cancel(orderID uint64, symbolID uint32) (uint64, error) {
	return p.submit(ipc.Cmd{
		RequestID: p.ids.Generate().Number,
		OrderID:   orderID,
		SymbolID:  symbolID,
		Op:        ipc.OpCancel,
	})
}

// ReduceQty publishes an in-place quantity reduction (new total quantity).
func (p *Producer) ReduceQty(orderID uint64, symbolID uint32, newQty int32) (uint64, error) {
	return p.submit(ipc.Cmd{
		RequestID: p.ids.Generate().Number,
		OrderID:   orderID,
		SymbolID:  symbolID,
		Qty:       newQty,
		Op:        ipc.OpReduceQty,
	})
}

// Modify publishes a combined price/quantity change (new total quantity).
func (p *Producer) Modify(orderID uint64, symbolID uint32, newPrice int64, newQty int32) (uint64, error) {
	return p.submit(ipc.Cmd{
		RequestID: p.ids.Generate().Number,
		OrderID:   orderID,
		SymbolID:  symbolID,
		Price:     newPrice,
		Qty:       newQty,
		Op:        ipc.OpModify,
	})
}

// Close stops the commit goroutine (flushing pending records) and closes the ring.
func (p *Producer) Close() error {
	p.closeOnce.Do(func() { close(p.stop) })
	<-p.done
	return p.ring.Close()
}
