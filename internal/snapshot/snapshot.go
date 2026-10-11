// Package snapshot stores order-book snapshots as compressed, versioned binary files.
//
// A snapshot file is a 64-byte header followed by the (optionally compressed) book body.
// Writes are crash-safe: data goes to a temporary file, is fsynced, then renamed over the
// final name, and the directory is fsynced - so a crash mid-write cannot destroy the last
// good snapshot. Files are named "<name>.snap.<seq>" so the latest is the highest seq.
package snapshot

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// Codec identifies the body compression.
type Codec uint8

const (
	CodecNone Codec = iota
	CodecGzip
	CodecZstd
)

func (c Codec) String() string {
	switch c {
	case CodecNone:
		return "none"
	case CodecGzip:
		return "gzip"
	case CodecZstd:
		return "zstd"
	default:
		return "unknown"
	}
}

// ParseCodec maps a config string to a Codec (defaults to zstd).
func ParseCodec(s string) Codec {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "none":
		return CodecNone
	case "gzip":
		return CodecGzip
	case "zstd", "":
		return CodecZstd
	default:
		return CodecZstd
	}
}

const (
	magic      = uint32(0x5449534e) // "TISN"
	version    uint32 = 1
	headerSize        = 64
	suffix            = ".snap."
)

var (
	ErrBadMagic   = errors.New("snapshot: bad magic")
	ErrBadVersion = errors.New("snapshot: unsupported version")
	ErrBadSize    = errors.New("snapshot: file size does not match header")
)

// Header describes a snapshot file.
type Header struct {
	Codec    Codec
	SymbolID uint32
	Seq      uint64
	Epoch    uint64
	RawLen   uint64
	CompLen  uint64
}

// Write stores payload as "<dir>/<name>.snap.<seq>" and returns the path.
func Write(dir, name string, h Header, payload []byte) (string, error) {
	comp, err := compress(h.Codec, payload)
	if err != nil {
		return "", err
	}
	h.RawLen = uint64(len(payload))
	h.CompLen = uint64(len(comp))

	hdr := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(hdr[0:], magic)
	binary.LittleEndian.PutUint32(hdr[4:], version)
	hdr[8] = byte(h.Codec)
	binary.LittleEndian.PutUint32(hdr[12:], h.SymbolID)
	binary.LittleEndian.PutUint64(hdr[16:], h.Seq)
	binary.LittleEndian.PutUint64(hdr[24:], h.Epoch)
	binary.LittleEndian.PutUint64(hdr[32:], h.RawLen)
	binary.LittleEndian.PutUint64(hdr[40:], h.CompLen)

	final := filepath.Join(dir, fmt.Sprintf("%s%s%d", name, suffix, h.Seq))
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return "", fmt.Errorf("snapshot: create %s: %w", tmp, err)
	}
	if _, err := f.Write(hdr); err != nil {
		f.Close()
		return "", err
	}
	if _, err := f.Write(comp); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, final); err != nil {
		return "", fmt.Errorf("snapshot: rename %s: %w", tmp, err)
	}
	syncDir(dir)
	return final, nil
}

// ReadHeader reads only the header of a snapshot file.
func ReadHeader(path string) (Header, error) {
	f, err := os.Open(path)
	if err != nil {
		return Header{}, err
	}
	defer f.Close()
	var hdr [headerSize]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return Header{}, ErrBadSize
	}
	return decodeHeader(hdr[:])
}

// Read reads and decompresses a snapshot file.
func Read(path string) (Header, []byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Header{}, nil, err
	}
	if len(b) < headerSize {
		return Header{}, nil, ErrBadSize
	}
	h, err := decodeHeader(b[:headerSize])
	if err != nil {
		return Header{}, nil, err
	}
	if uint64(len(b)-headerSize) != h.CompLen {
		return Header{}, nil, ErrBadSize
	}
	payload, err := decompress(h.Codec, b[headerSize:])
	if err != nil {
		return Header{}, nil, err
	}
	if uint64(len(payload)) != h.RawLen {
		return Header{}, nil, ErrBadSize
	}
	return h, payload, nil
}

// Latest returns the path and header of the highest-seq valid snapshot for name, or ok
// false when none exists.
func Latest(dir, name string) (path string, h Header, ok bool, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", Header{}, false, err
	}
	prefix := name + suffix
	var bestPath string
	var bestSeq uint64
	found := false
	for _, e := range entries {
		n := e.Name()
		if !strings.HasPrefix(n, prefix) || strings.HasSuffix(n, ".tmp") {
			continue
		}
		seq, perr := strconv.ParseUint(strings.TrimPrefix(n, prefix), 10, 64)
		if perr != nil {
			continue
		}
		if !found || seq > bestSeq {
			bestSeq, bestPath, found = seq, filepath.Join(dir, n), true
		}
	}
	if !found {
		return "", Header{}, false, nil
	}
	h, err = ReadHeader(bestPath)
	if err != nil {
		return "", Header{}, false, err
	}
	return bestPath, h, true, nil
}

func decodeHeader(b []byte) (Header, error) {
	if binary.LittleEndian.Uint32(b[0:]) != magic {
		return Header{}, ErrBadMagic
	}
	if binary.LittleEndian.Uint32(b[4:]) != version {
		return Header{}, ErrBadVersion
	}
	return Header{
		Codec:    Codec(b[8]),
		SymbolID: binary.LittleEndian.Uint32(b[12:]),
		Seq:      binary.LittleEndian.Uint64(b[16:]),
		Epoch:    binary.LittleEndian.Uint64(b[24:]),
		RawLen:   binary.LittleEndian.Uint64(b[32:]),
		CompLen:  binary.LittleEndian.Uint64(b[40:]),
	}, nil
}

func compress(c Codec, data []byte) ([]byte, error) {
	switch c {
	case CodecNone:
		return data, nil
	case CodecGzip:
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		if _, err := w.Write(data); err != nil {
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	case CodecZstd:
		enc, err := zstd.NewWriter(nil)
		if err != nil {
			return nil, err
		}
		defer enc.Close()
		return enc.EncodeAll(data, nil), nil
	default:
		return nil, fmt.Errorf("snapshot: unknown codec %d", c)
	}
}

func decompress(c Codec, data []byte) ([]byte, error) {
	switch c {
	case CodecNone:
		return data, nil
	case CodecGzip:
		r, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	case CodecZstd:
		dec, err := zstd.NewReader(nil)
		if err != nil {
			return nil, err
		}
		defer dec.Close()
		return dec.DecodeAll(data, nil)
	default:
		return nil, fmt.Errorf("snapshot: unknown codec %d", c)
	}
}

func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}
