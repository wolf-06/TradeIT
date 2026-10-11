// Package control implements a small shared-memory region through which the consumer
// publishes its per-symbol watermark to the producer.
//
// It is a live channel, not durable storage: shared memory survives a process crash but
// not a machine reboot. The consumer's WAL is the durable backing - the consumer
// republishes the watermark it derives from its WAL after every restart.
package control

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	magic      uint32 = 0x54494354 // "TICT"
	version    uint32 = 1
	headerSize        = 64
)

var (
	ErrBadMagic   = errors.New("control: bad magic")
	ErrBadVersion = errors.New("control: unsupported version")
	ErrBadSize    = errors.New("control: file size does not match layout")
)

// Size returns the file size for a region holding one watermark per symbol.
func Size(symbols int) int { return headerSize + symbols*8 }

// Region is a shared-memory array of per-symbol watermarks.
type Region struct {
	data []byte
	n    uint32
}

// Create creates (or truncates) path and initialises a region for symbols symbols.
func Create(path string, symbols int) (*Region, error) {
	if symbols <= 0 {
		return nil, fmt.Errorf("control: symbols must be > 0")
	}
	size := Size(symbols)
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR|unix.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("control: open %s: %w", path, err)
	}
	defer unix.Close(fd)
	if err := unix.Ftruncate(fd, int64(size)); err != nil {
		return nil, fmt.Errorf("control: truncate %s: %w", path, err)
	}
	data, err := unix.Mmap(fd, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("control: mmap %s: %w", path, err)
	}
	binary.LittleEndian.PutUint32(data[0:], magic)
	binary.LittleEndian.PutUint32(data[4:], version)
	binary.LittleEndian.PutUint32(data[8:], uint32(symbols))
	r := &Region{data: data, n: uint32(symbols)}
	for i := 0; i < symbols; i++ {
		r.SetWatermark(uint32(i), 0)
	}
	return r, nil
}

// OpenOrCreate attaches to path, creating it if absent.
func OpenOrCreate(path string, symbols int) (*Region, error) {
	r, err := Open(path)
	if err == nil {
		return r, nil
	}
	return Create(path, symbols)
}

// Open attaches to an existing region and validates its layout.
func Open(path string) (*Region, error) {
	fd, err := unix.Open(path, unix.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("control: open %s: %w", path, err)
	}
	defer unix.Close(fd)
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return nil, fmt.Errorf("control: stat %s: %w", path, err)
	}
	size := int(st.Size)
	if size < headerSize {
		return nil, ErrBadSize
	}
	data, err := unix.Mmap(fd, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("control: mmap %s: %w", path, err)
	}
	if binary.LittleEndian.Uint32(data[0:]) != magic {
		unix.Munmap(data)
		return nil, ErrBadMagic
	}
	if binary.LittleEndian.Uint32(data[4:]) != version {
		unix.Munmap(data)
		return nil, ErrBadVersion
	}
	n := binary.LittleEndian.Uint32(data[8:])
	if uint64(size) != uint64(headerSize)+uint64(n)*8 {
		unix.Munmap(data)
		return nil, ErrBadSize
	}
	return &Region{data: data, n: n}, nil
}

// Close unmaps the region.
func (r *Region) Close() error { return unix.Munmap(r.data) }

// Count returns the number of symbol slots.
func (r *Region) Count() int { return int(r.n) }

func (r *Region) slot(id uint32) *uint64 {
	off := headerSize + int(id)*8
	return (*uint64)(unsafe.Pointer(&r.data[off]))
}

// Watermark returns the watermark for symbol id, or 0 if out of range.
func (r *Region) Watermark(id uint32) uint64 {
	if id >= r.n {
		return 0
	}
	return atomic.LoadUint64(r.slot(id))
}

// SetWatermark publishes the watermark for symbol id.
func (r *Region) SetWatermark(id uint32, seq uint64) {
	if id >= r.n {
		return
	}
	atomic.StoreUint64(r.slot(id), seq)
}
