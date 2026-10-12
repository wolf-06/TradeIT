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
	"log"
	"os"
	"path/filepath"
	"time"

	"TradeIT/internal/ipc"
	"TradeIT/internal/shmring"
	"TradeIT/internal/snapshot"
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

// Snapshotter is an optional Handler capability: it serialises and restores one symbol's
// state. seq is the per-symbol sequence the snapshot represents.
type Snapshotter interface {
	Snapshot(symbolID uint32, seq uint64) ([]byte, error)
	RestoreSnapshot(symbolID uint32, seq uint64, data []byte) error
}

// Options configures the consumer's snapshot behaviour. The zero value disables
// snapshots.
type Options struct {
	// SnapshotDir is where snapshot files are written. Empty disables snapshots.
	SnapshotDir string
	// SnapshotEveryN triggers a snapshot after N applied commands for a symbol.
	SnapshotEveryN int
	// SnapshotEveryMs triggers a snapshot for a symbol after this long.
	SnapshotEveryMs int
	// Codec selects the snapshot body compression.
	Codec snapshot.Codec
}

type channel struct {
	id       uint32
	name     string
	ring     *shmring.Ring
	pwal     *wal.Writer // producer command log (producer side)
	pwalPath string
	awal     *wal.Writer // consumer applied log (consumer side)
	seq      uint64      // producer: next sequence to assign
	wm       uint64      // consumer: last applied sequence
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
		p.ch = append(p.ch, &channel{id: uint32(id), name: name, ring: ring, pwal: w, pwalPath: pp, seq: seq})
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
	// Encode straight into the ring slot so no per-record stack buffer has to escape
	// into the WAL append.
	slot := ch.ring.WriteSlot(ch.ring.Head())
	c.Encode(slot)
	if err := ch.pwal.Append(slot); err != nil {
		return 0, err
	}
	ch.ring.Commit(1)
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

// Replay re-publishes records from each symbol's producer WAL whose sequence is greater
// than from[id] (or the ring's current head, whichever is larger) into the ring. It is
// used at startup to re-send orders the consumer has not applied - for example after the
// ring was lost on a reboot. It blocks briefly if a ring is full while the consumer
// drains.
func (p *Producer) Replay(from []uint64) error {
	for i, ch := range p.ch {
		var floor uint64
		if i < len(from) {
			floor = from[i]
		}
		if err := replayChannel(ch, floor); err != nil {
			return err
		}
	}
	return nil
}

func replayChannel(ch *channel, floor uint64) error {
	if _, err := os.Stat(ch.pwalPath); os.IsNotExist(err) {
		return nil
	}
	if _, err := wal.Repair(ch.pwalPath); err != nil {
		return err
	}
	start := ch.ring.Head()
	if floor > start {
		start = floor
	}
	r, err := wal.OpenReader(ch.pwalPath)
	if err != nil {
		return err
	}
	defer r.Close()
	for {
		rec, err := r.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if ipc.DecodeCmd(rec).Seq <= start {
			continue
		}
		for !ch.ring.Publish(rec) {
			time.Sleep(100 * time.Microsecond)
		}
	}
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
	opts    Options
	snapper Snapshotter // nil when the handler does not support snapshots

	appliedBuf   []byte
	lastSnapSeq  []uint64
	lastSnapTime []time.Time
}

// OpenConsumer opens the rings with snapshots disabled. See OpenConsumerWithOptions.
func OpenConsumer(dir string, table *symbol.Table, h Handler) (*Consumer, error) {
	return OpenConsumerWithOptions(dir, table, h, Options{})
}

// OpenConsumerWithOptions opens the rings, restores the latest snapshot per symbol (when
// the handler supports snapshots), repairs and replays the applied WAL tail, and resumes
// each symbol's watermark. The ring capacity is defined by the files the producer created.
func OpenConsumerWithOptions(dir string, table *symbol.Table, h Handler, opts Options) (*Consumer, error) {
	snapper, _ := h.(Snapshotter)
	if snapper == nil {
		opts.SnapshotDir = ""
	}
	c := &Consumer{
		table:        table,
		handler:      h,
		opts:         opts,
		snapper:      snapper,
		appliedBuf:   make([]byte, ipc.AppliedSize),
		lastSnapSeq:  make([]uint64, len(table.Names())),
		lastSnapTime: make([]time.Time, len(table.Names())),
	}
	now := time.Now()
	for id, name := range table.Names() {
		ring, err := shmring.Open(ringPath(dir, name))
		if err != nil {
			c.Close()
			return nil, err
		}
		// Restore the latest snapshot, then replay only the WAL tail after it.
		var startSeq uint64
		if snapper != nil && opts.SnapshotDir != "" {
			path, hdr, ok, err := snapshot.Latest(opts.SnapshotDir, name)
			if err != nil {
				ring.Close()
				c.Close()
				return nil, err
			}
			if ok {
				_, payload, err := snapshot.Read(path)
				if err != nil {
					ring.Close()
					c.Close()
					return nil, err
				}
				if err := snapper.RestoreSnapshot(uint32(id), hdr.Seq, payload); err != nil {
					ring.Close()
					c.Close()
					return nil, err
				}
				startSeq = hdr.Seq
			}
		}
		ap := awalPath(dir, name)
		wm, err := replayApplied(ap, h, startSeq)
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
		c.lastSnapSeq[id] = startSeq
		c.lastSnapTime[id] = now
	}
	return c, nil
}

// replayApplied replays the applied WAL into the handler. Records at or below skipUntil
// are assumed to be already present in a restored snapshot and are skipped.
func replayApplied(path string, h Handler, skipUntil uint64) (uint64, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return skipUntil, nil
	}
	if _, err := wal.Repair(path); err != nil {
		return 0, err
	}
	r, err := wal.OpenReader(path)
	if err != nil {
		return 0, err
	}
	defer r.Close()
	wm := skipUntil
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
		if cmd.Seq <= skipUntil {
			continue // already contained in the restored snapshot
		}
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
// applied record, and only after the WAL is synced acknowledges the ring. It then
// snapshots any symbol whose count or time threshold is met.
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
			ipc.EncodeApplied(c.appliedBuf, cmd, o)
			if err := ch.awal.Append(c.appliedBuf); err != nil {
				return total, err
			}
			ch.wm = cmd.Seq
			n++
		}
		if n > 0 {
			if err := ch.awal.Sync(); err != nil {
				return total, err
			}
			ch.ring.Ack(uint64(n))
			total += n
		}
		c.maybeSnapshot(i, ch)
	}
	return total, nil
}

// maybeSnapshot writes a snapshot for one symbol when its count or time threshold is met.
func (c *Consumer) maybeSnapshot(i int, ch *channel) {
	if c.snapper == nil || c.opts.SnapshotDir == "" || ch.wm == c.lastSnapSeq[i] {
		return
	}
	now := time.Now()
	countDue := c.opts.SnapshotEveryN > 0 && ch.wm-c.lastSnapSeq[i] >= uint64(c.opts.SnapshotEveryN)
	timeDue := c.opts.SnapshotEveryMs > 0 && now.Sub(c.lastSnapTime[i]) >= time.Duration(c.opts.SnapshotEveryMs)*time.Millisecond
	if !countDue && !timeDue {
		return
	}
	data, err := c.snapper.Snapshot(ch.id, ch.wm)
	if err != nil {
		log.Println("ingest: snapshot error for", ch.name, ":", err)
		return
	}
	hdr := snapshot.Header{Codec: c.opts.Codec, SymbolID: ch.id, Seq: ch.wm}
	if _, err := snapshot.Write(c.opts.SnapshotDir, ch.name, hdr, data); err != nil {
		log.Println("ingest: snapshot write error for", ch.name, ":", err)
		return
	}
	c.lastSnapSeq[i] = ch.wm
	c.lastSnapTime[i] = now
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
