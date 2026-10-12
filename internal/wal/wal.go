// Package wal implements a durable append-only log of length- and CRC-framed records.
//
// Each record is framed as [length uint32][crc32 uint32][payload], little-endian. The
// CRC lets recovery detect a torn tail (a crash mid-append) and stop at the last valid
// record instead of replaying garbage. Writers buffer appends; durability is explicit
// via Sync, so the caller can implement group commit (fsync every N records or every
// X microseconds).
//
// The log is deliberately generic: the payload bytes are produced by the ipc codecs.
// One log file is used per symbol.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

const (
	headerLen = 8 // length(4) + crc(4)
	// maxRecord guards recovery against a corrupt length field turning into a huge
	// allocation. A record is never larger than this.
	maxRecord = 1 << 20
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ErrTornTail reports a truncated or corrupt record at the end of the log. It is not a
// fatal condition: recovery treats everything before it as valid.
var ErrTornTail = errors.New("wal: torn or corrupt record tail")

// Writer appends framed records to a log file. It is not safe for concurrent use.
type Writer struct {
	f   *os.File
	bw  *bufio.Writer
	hdr [headerLen]byte
}

// CreateWriter creates (or truncates) path for writing.
func CreateWriter(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("wal: create %s: %w", path, err)
	}
	return &Writer{f: f, bw: bufio.NewWriterSize(f, 1<<20)}, nil
}

// OpenWriter opens path for appending, creating it if absent.
func OpenWriter(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}
	return &Writer{f: f, bw: bufio.NewWriterSize(f, 1<<20)}, nil
}

// Append buffers one record. It is durable only after Sync returns.
func (w *Writer) Append(payload []byte) error {
	if len(payload) > maxRecord {
		return fmt.Errorf("wal: record too large (%d bytes)", len(payload))
	}
	binary.LittleEndian.PutUint32(w.hdr[0:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(w.hdr[4:], crc32.Checksum(payload, crcTable))
	if _, err := w.bw.Write(w.hdr[:]); err != nil {
		return err
	}
	if _, err := w.bw.Write(payload); err != nil {
		return err
	}
	return nil
}

// Sync flushes buffered records and fsyncs the file, making all prior appends durable.
func (w *Writer) Sync() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

// Close flushes and closes the file.
func (w *Writer) Close() error {
	flushErr := w.bw.Flush()
	closeErr := w.f.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

// Reader reads framed records sequentially. It is not safe for concurrent use.
type Reader struct {
	f  *os.File
	br *bufio.Reader
	// off is the byte offset just past the last successfully read record.
	off int64
}

// OpenReader opens path for reading.
func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}
	return &Reader{f: f, br: bufio.NewReaderSize(f, 1<<20)}, nil
}

// Next returns the next payload, io.EOF at a clean end of file, or ErrTornTail if the
// trailing record is truncated or corrupt.
func (r *Reader) Next() ([]byte, error) {
	var hdr [headerLen]byte
	n, err := io.ReadFull(r.br, hdr[:])
	if err == io.EOF && n == 0 {
		return nil, io.EOF
	}
	if err != nil {
		return nil, ErrTornTail
	}
	length := binary.LittleEndian.Uint32(hdr[0:])
	want := binary.LittleEndian.Uint32(hdr[4:])
	if length > maxRecord {
		return nil, ErrTornTail
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r.br, payload); err != nil {
		return nil, ErrTornTail
	}
	if crc32.Checksum(payload, crcTable) != want {
		return nil, ErrTornTail
	}
	r.off += int64(headerLen) + int64(length)
	return payload, nil
}

// Offset returns the byte offset just past the last successfully read record.
func (r *Reader) Offset() int64 { return r.off }

// Close closes the underlying file.
func (r *Reader) Close() error { return r.f.Close() }

// Recover scans path and returns the offset just past the last valid record. A torn or
// corrupt tail is not an error: the returned offset is simply less than the file size,
// and the caller may truncate to it.
func Recover(path string) (validEnd int64, err error) {
	r, err := OpenReader(path)
	if err != nil {
		return 0, err
	}
	defer r.Close()
	for {
		if _, err := r.Next(); err != nil {
			if err == io.EOF || err == ErrTornTail {
				return r.Offset(), nil
			}
			return r.Offset(), err
		}
	}
}

// Repair truncates path to the last valid record and returns that offset. It is the
// startup step before appending to a log that may have a torn tail.
func Repair(path string) (int64, error) {
	end, err := Recover(path)
	if err != nil {
		return 0, err
	}
	if err := os.Truncate(path, end); err != nil {
		return 0, fmt.Errorf("wal: truncate %s: %w", path, err)
	}
	return end, nil
}
