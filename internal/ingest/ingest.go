// Package ingest wires the shared-memory ring and the write-ahead logs into the
// producer and consumer sides of the order path.
//
// One ring and one pair of WALs (producer command log, consumer applied log) are used
// per symbol, so per-symbol ordering is preserved by construction and each file is
// single-writer. The producer assigns a per-symbol sequence number, appends the command
// to its WAL, then publishes it to the ring. The consumer drains the ring, skips
// anything at or below its per-symbol watermark (idempotent replay), applies the
// command, appends the applied record to its WAL, and only then acknowledges the ring.
package ingest

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"TradeIT/internal/ipc"
	"TradeIT/internal/shmring"
	"TradeIT/internal/symbol"
	"TradeIT/internal/wal"
)

// ErrRingFull is returned when a symbol's ring has no free slot. The caller is
// expected to apply backpressure and retry rather than drop the command.
var ErrRingFull = errors.New("ingest: ring full")

// Handler applies commands to a symbol's book.
type Handler interface {
	Apply(c ipc.Cmd) ipc.Outcome
}

// Restorer is an optional Handler capability used during recovery to re-apply a
// previously recorded command/outcome without producing a new outcome.
type Restorer interface {
	Restore(c ipc.Cmd, o ipc.Outcome)
}

type channel struct {
	id   uint32
	name string
	ring *shmring.Ring
	pwal *wal.Writer // producer command log (producer side)
	awal *wal.Writer // consumer applied log (consumer side)
	seq  uint64      // producer: next sequence to assign
	wm   uint64      // consumer: last applied sequence
}

func ringPath(dir, name string) string { return filepath.Join(dir, name+".ring") }
func pwalPath(dir, name string) string { return filepath.Join(dir, name+".producer.wal") }
func awalPath(dir, name string) string { return filepath.Join(dir, name+".consumer.wal") }

func openOrCreateRing(path string, capacity, slotSize int) (*shmring.Ring, error) {
	r, err := shmring.Open(path)
	if err == nil {
		if r.Capacity() != uint64(capacity) || r.SlotSize() != uint64(slotSize) {
			r.Close()
			return nil, fmt.Errorf("ingest: ring %s layout mismatch", path)
		}
		return r, nil
	}
	return shmring.Create(path, capacity, slotSize)
}

// lastSeq repairs path and returns the next sequence number to assign. Sequences are
// 1-based so that watermark 0 unambiguously means "nothing applied yet".
func lastSeq(path string) (uint64, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return 1, nil
	}
	if _, err := wal.Repair(path); err != nil {
		return 0, err
	}
	r, err := wal.OpenReader(path)
	if err != nil {
		return 0, err
	}
	defer r.Close()
	var last uint64
	have := false
	for {
		p, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		last = ipc.DecodeCmd(p).Seq
		have = true
	}
	if !have {
		return 1, nil
	}
	return last + 1, nil
}

// Producer owns the rings and producer WALs for every symbol.
type Producer struct {
	table *symbol.Table
	ch    []*channel
}

// OpenProducer creates (or reopens) the ring and producer WAL for each symbol and
// resumes each symbol's sequence from its WAL.
func OpenProducer(dir string, table *symbol.Table, capacity int) (*Producer, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("ingest: mkdir %s: %w", dir, err)
	}
	p := &Producer{table: table}
	for id, name := range table.Names() {
		ring, err := openOrCreateRing(ringPath(dir, name), capacity, ipc.CmdSize)
		if err != nil {
			p.Close()
			return nil, err
		}
		pp := pwalPath(dir, name)
		seq, err := lastSeq(pp)
		if err != nil {
			ring.Close()
			p.Close()
			return nil, err
		}
		w, err := wal.OpenWriter(pp)
		if err != nil {
			ring.Close()
			p.Close()
			return nil, err
		}
		p.ch = append(p.ch, &channel{id: uint32(id), name: name, ring: ring, pwal: w, seq: seq})
	}
	return p, nil
}

// Publish assigns the next sequence for c's symbol, appends it to the producer WAL and
// publishes it to the ring. The WAL append is durable only after Sync.
func (p *Producer) Publish(c ipc.Cmd) (uint64, error) {
	if int(c.SymbolID) >= len(p.ch) {
		return 0, fmt.Errorf("ingest: unknown symbol id %d", c.SymbolID)
	}
	ch := p.ch[c.SymbolID]
	if ch.ring.Free() == 0 {
		return 0, ErrRingFull
	}
	c.Seq = ch.seq
	var buf [ipc.CmdSize]byte
	c.Encode(buf[:])
	if err := ch.pwal.Append(buf[:]); err != nil {
		return 0, err
	}
	if !ch.ring.Publish(buf[:]) {
		return 0, ErrRingFull
	}
	ch.seq++
	return c.Seq, nil
}

// Sync flushes and fsyncs every producer WAL, making all prior Publish calls durable.
func (p *Producer) Sync() error {
	for _, ch := range p.ch {
		if err := ch.pwal.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// SyncSymbol flushes and fsyncs one symbol's producer WAL. Syncing only the symbols that
// actually received records avoids fsyncing every symbol on every flush.
func (p *Producer) SyncSymbol(id uint32) error {
	if int(id) >= len(p.ch) {
		return fmt.Errorf("ingest: unknown symbol id %d", id)
	}
	return p.ch[id].pwal.Sync()
}

// FreeFor returns the number of free slots in one symbol's ring.
func (p *Producer) FreeFor(id uint32) uint64 {
	if int(id) >= len(p.ch) {
		return 0
	}
	return p.ch[id].ring.Free()
}

// Close closes all rings and WALs.
func (p *Producer) Close() error {
	var first error
	for _, ch := range p.ch {
		if ch.ring != nil {
			if err := ch.ring.Close(); err != nil && first == nil {
				first = err
			}
		}
		if ch.pwal != nil {
			if err := ch.pwal.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	p.ch = nil
	return first
}

// Consumer drains the rings, applies commands, records outcomes and tracks watermarks.
type Consumer struct {
	table   *symbol.Table
	ch      []*channel
	handler Handler
}

// OpenConsumer opens the rings, repairs and replays each applied WAL into the handler
// (via Restorer when available), and resumes each symbol's watermark. The ring capacity
// is defined by the files the producer created.
func OpenConsumer(dir string, table *symbol.Table, h Handler) (*Consumer, error) {
	c := &Consumer{table: table, handler: h}
	for id, name := range table.Names() {
		ring, err := shmring.Open(ringPath(dir, name))
		if err != nil {
			c.Close()
			return nil, err
		}
		ap := awalPath(dir, name)
		wm, err := replayApplied(ap, h)
		if err != nil {
			ring.Close()
			c.Close()
			return nil, err
		}
		aw, err := wal.OpenWriter(ap)
		if err != nil {
			ring.Close()
			c.Close()
			return nil, err
		}
		c.ch = append(c.ch, &channel{id: uint32(id), name: name, ring: ring, awal: aw, wm: wm})
	}
	return c, nil
}

func replayApplied(path string, h Handler) (uint64, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return 0, nil
	}
	if _, err := wal.Repair(path); err != nil {
		return 0, err
	}
	r, err := wal.OpenReader(path)
	if err != nil {
		return 0, err
	}
	defer r.Close()
	var wm uint64
	rs, canRestore := h.(Restorer)
	for {
		p, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		cmd, o := ipc.DecodeApplied(p)
		if canRestore {
			rs.Restore(cmd, o)
		} else {
			h.Apply(cmd)
		}
		wm = cmd.Seq
	}
	return wm, nil
}

// DrainOnce processes up to max records per symbol. For each record it skips anything
// at or below the symbol watermark (idempotent replay), applies the rest, appends the
// applied record, and only after the WAL is synced acknowledges the ring.
func (c *Consumer) DrainOnce(max int) (int, error) {
	total := 0
	for i := range c.ch {
		ch := c.ch[i]
		base := ch.ring.Tail()
		avail := ch.ring.Head() - base
		n := 0
		for n < max && uint64(n) < avail {
			cmd := ipc.DecodeCmd(ch.ring.ReadSlot(base + uint64(n)))
			if cmd.Seq <= ch.wm { // already applied in a previous life
				n++
				continue
			}
			o := c.handler.Apply(cmd)
			var ab [ipc.AppliedSize]byte
			ipc.EncodeApplied(ab[:], cmd, o)
			if err := ch.awal.Append(ab[:]); err != nil {
				return total, err
			}
			ch.wm = cmd.Seq
			n++
		}
		if n == 0 {
			continue
		}
		if err := ch.awal.Sync(); err != nil {
			return total, err
		}
		ch.ring.Ack(uint64(n))
		total += n
	}
	return total, nil
}

// Watermarks returns the last applied sequence per symbol (indexed by symbol id).
func (c *Consumer) Watermarks() []uint64 {
	wm := make([]uint64, len(c.ch))
	for i, ch := range c.ch {
		wm[i] = ch.wm
	}
	return wm
}

// Close closes all rings and applied WALs.
func (c *Consumer) Close() error {
	var first error
	for _, ch := range c.ch {
		if ch.ring != nil {
			if err := ch.ring.Close(); err != nil && first == nil {
				first = err
			}
		}
		if ch.awal != nil {
			if err := ch.awal.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	c.ch = nil
	return first
}
