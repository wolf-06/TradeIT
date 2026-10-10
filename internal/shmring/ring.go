// Package shmring implements a single-producer/single-consumer (SPSC) ring buffer
// backed by a memory-mapped file, so two processes on the same host can exchange
// fixed-size records through shared memory without a broker or a syscall per record.
//
// Design notes:
//
//   - head and tail are free-running 64-bit counters owned by the producer and the
//     consumer respectively. count = head - tail; empty when equal, full when
//     count == capacity. Physical slot index = seq & (capacity-1), so capacity must
//     be a power of two.
//   - The producer writes slot data first and only then advances head. The consumer
//     only reads slots below head. Because Go's sync/atomic operations are
//     sequentially consistent, the slot writes happen-before the slot reads: no lock
//     and no torn record.
//   - head and tail each live on their own 64-byte cache line to avoid false sharing.
//   - The mapped layout is versioned and frozen. A process refuses to attach to a
//     file whose magic, version, capacity or slot size do not match, instead of
//     reading a layout it does not understand.
package shmring

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	magic   uint32 = 0x54524e47 // "TRNG"
	version uint32 = 1
)

// Header layout (all fields little-endian). head and tail are cache-line aligned.
//
//	offset 0   : magic     uint32
//	offset 4   : version   uint32
//	offset 8   : capacity  uint64 (power of two, number of slots)
//	offset 16  : slotSize  uint64 (bytes per slot)
//	offset 24  : epoch     uint64 (run generation; informational)
//	offset 64  : head      uint64 (producer-owned)
//	offset 128 : tail      uint64 (consumer-owned)
//	offset 192 : slots
const (
	offMagic    = 0
	offVersion  = 4
	offCapacity = 8
	offSlotSize = 16
	offEpoch    = 24
	offHead     = 64
	offTail     = 128
	headerSize  = 192
)

var (
	ErrBadMagic    = errors.New("shmring: bad magic")
	ErrBadVersion  = errors.New("shmring: unsupported layout version")
	ErrBadCapacity = errors.New("shmring: capacity must be a power of two > 0")
	ErrBadSlotSize = errors.New("shmring: slot size must be > 0 and a multiple of 8")
	ErrBadSize     = errors.New("shmring: file size does not match layout")
)

// Size returns the file size needed for a ring with the given capacity and slot size.
func Size(capacity, slotSize int) int { return headerSize + capacity*slotSize }

// Ring is an SPSC ring buffer mapped from a file. By design it is not safe for use by
// more than one producer or more than one consumer at a time.
type Ring struct {
	data     []byte
	capacity uint64
	mask     uint64
	slotSize uint64
	head     *uint64
	tail     *uint64
	slots    []byte
}

// Create creates (or truncates) path and initialises a ring with the given capacity
// (power of two) and slot size (multiple of 8). The file is mapped shared.
func Create(path string, capacity, slotSize int) (*Ring, error) {
	if capacity <= 0 || capacity&(capacity-1) != 0 {
		return nil, ErrBadCapacity
	}
	if slotSize <= 0 || slotSize%8 != 0 {
		return nil, ErrBadSlotSize
	}
	size := Size(capacity, slotSize)
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR|unix.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("shmring: open %s: %w", path, err)
	}
	defer unix.Close(fd)
	if err := unix.Ftruncate(fd, int64(size)); err != nil {
		return nil, fmt.Errorf("shmring: truncate %s: %w", path, err)
	}
	data, err := unix.Mmap(fd, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("shmring: mmap %s: %w", path, err)
	}
	binary.LittleEndian.PutUint32(data[offMagic:], magic)
	binary.LittleEndian.PutUint32(data[offVersion:], version)
	binary.LittleEndian.PutUint64(data[offCapacity:], uint64(capacity))
	binary.LittleEndian.PutUint64(data[offSlotSize:], uint64(slotSize))
	binary.LittleEndian.PutUint64(data[offEpoch:], 0)
	r := wrap(data, uint64(capacity), uint64(slotSize))
	atomic.StoreUint64(r.head, 0)
	atomic.StoreUint64(r.tail, 0)
	return r, nil
}

// Open attaches to an existing ring file and validates its layout.
func Open(path string) (*Ring, error) {
	fd, err := unix.Open(path, unix.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("shmring: open %s: %w", path, err)
	}
	defer unix.Close(fd)

	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return nil, fmt.Errorf("shmring: stat %s: %w", path, err)
	}
	size := int(st.Size)
	if size < headerSize {
		return nil, ErrBadSize
	}
	data, err := unix.Mmap(fd, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("shmring: mmap %s: %w", path, err)
	}
	if binary.LittleEndian.Uint32(data[offMagic:]) != magic {
		unix.Munmap(data)
		return nil, ErrBadMagic
	}
	if binary.LittleEndian.Uint32(data[offVersion:]) != version {
		unix.Munmap(data)
		return nil, ErrBadVersion
	}
	capacity := binary.LittleEndian.Uint64(data[offCapacity:])
	slotSize := binary.LittleEndian.Uint64(data[offSlotSize:])
	if capacity == 0 || capacity&(capacity-1) != 0 {
		unix.Munmap(data)
		return nil, ErrBadCapacity
	}
	if slotSize == 0 || slotSize%8 != 0 {
		unix.Munmap(data)
		return nil, ErrBadSlotSize
	}
	if uint64(size) != uint64(headerSize)+capacity*slotSize {
		unix.Munmap(data)
		return nil, ErrBadSize
	}
	return wrap(data, capacity, slotSize), nil
}

func wrap(data []byte, capacity, slotSize uint64) *Ring {
	return &Ring{
		data:     data,
		capacity: capacity,
		mask:     capacity - 1,
		slotSize: slotSize,
		head:     (*uint64)(unsafe.Pointer(&data[offHead])),
		tail:     (*uint64)(unsafe.Pointer(&data[offTail])),
		slots:    data[headerSize:],
	}
}

// Close unmaps the ring. It does not remove the file.
func (r *Ring) Close() error {
	if r.data == nil {
		return nil
	}
	err := unix.Munmap(r.data)
	r.data = nil
	return err
}

// Flush synchronously flushes the mapping to disk. Only meaningful for a file-backed
// (durable) ring; a tmpfs ring has nothing to flush.
func (r *Ring) Flush() error { return unix.Msync(r.data, unix.MS_SYNC) }

// Capacity returns the number of slots.
func (r *Ring) Capacity() uint64 { return r.capacity }

// SlotSize returns the size in bytes of one slot.
func (r *Ring) SlotSize() uint64 { return r.slotSize }

// Head returns the producer counter (number of records ever published).
func (r *Ring) Head() uint64 { return atomic.LoadUint64(r.head) }

// Tail returns the consumer counter (number of records ever consumed).
func (r *Ring) Tail() uint64 { return atomic.LoadUint64(r.tail) }

// Free returns the number of slots available to the producer.
func (r *Ring) Free() uint64 { return r.capacity - (r.Head() - r.Tail()) }

// Available returns the number of records available to the consumer.
func (r *Ring) Available() uint64 { return r.Head() - r.Tail() }

func (r *Ring) slot(seq uint64) []byte {
	off := (seq & r.mask) * r.slotSize
	return r.slots[off : off+r.slotSize]
}

// WriteSlot returns the writable slot for sequence seq. The caller fills it and then
// calls Commit(1). seq must be >= Head().
func (r *Ring) WriteSlot(seq uint64) []byte { return r.slot(seq) }

// ReadSlot returns the readable slot for sequence seq. seq must be >= Tail().
func (r *Ring) ReadSlot(seq uint64) []byte { return r.slot(seq) }

// Commit advances head by n after n slots were written with WriteSlot.
func (r *Ring) Commit(n uint64) { atomic.StoreUint64(r.head, r.Head()+n) }

// Ack advances tail by n after n slots were read with ReadSlot.
func (r *Ring) Ack(n uint64) { atomic.StoreUint64(r.tail, r.Tail()+n) }

// Publish copies rec into the next free slot and advances head. It returns false if
// the ring is full. len(rec) must equal SlotSize().
func (r *Ring) Publish(rec []byte) bool {
	if uint64(len(rec)) != r.slotSize {
		panic("shmring: record length must equal slot size")
	}
	h := r.Head()
	if h-r.Tail() >= r.capacity {
		return false
	}
	copy(r.slot(h), rec)
	atomic.StoreUint64(r.head, h+1)
	return true
}

// PublishBatch copies as many of recs as fit and advances head once. It returns the
// number of records published. Each record must equal SlotSize().
func (r *Ring) PublishBatch(recs [][]byte) int {
	h := r.Head()
	free := r.capacity - (h - r.Tail())
	n := len(recs)
	if uint64(n) > free {
		n = int(free)
	}
	for i := 0; i < n; i++ {
		if uint64(len(recs[i])) != r.slotSize {
			panic("shmring: record length must equal slot size")
		}
		copy(r.slot(h+uint64(i)), recs[i])
	}
	atomic.StoreUint64(r.head, h+uint64(n))
	return n
}

// Drain copies up to max records into buf contiguously (buf[i*slotSize:...]) and
// advances tail once. It returns the number of records copied. If buf is too small it
// copies only what fits.
func (r *Ring) Drain(buf []byte, max int) int {
	t := r.Tail()
	n := uint64(max)
	if avail := r.Head() - t; n > avail {
		n = avail
	}
	if uint64(len(buf)) < n*r.slotSize {
		n = uint64(len(buf)) / r.slotSize
	}
	for i := uint64(0); i < n; i++ {
		copy(buf[i*r.slotSize:(i+1)*r.slotSize], r.slot(t+i))
	}
	atomic.StoreUint64(r.tail, t+n)
	return int(n)
}
