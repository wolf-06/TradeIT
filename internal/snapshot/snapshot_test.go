package snapshot

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestCodecRoundTrip(t *testing.T) {
	payload := bytes.Repeat([]byte("tradeit-snapshot-"), 1000)
	for _, codec := range []Codec{CodecNone, CodecGzip, CodecZstd} {
		dir := t.TempDir()
		h := Header{Codec: codec, SymbolID: 2, Seq: 42, Epoch: 1}
		path, err := Write(dir, "AAPL", h, payload)
		if err != nil {
			t.Fatalf("%s: Write: %v", codec, err)
		}
		got, data, err := Read(path)
		if err != nil {
			t.Fatalf("%s: Read: %v", codec, err)
		}
		if !bytes.Equal(data, payload) {
			t.Fatalf("%s: payload mismatch", codec)
		}
		if got.Codec != codec || got.SymbolID != 2 || got.Seq != 42 || got.RawLen != uint64(len(payload)) {
			t.Fatalf("%s: header mismatch: %+v", codec, got)
		}
	}
}

func TestLatestPicksHighestSeq(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("x")
	for _, seq := range []uint64{10, 30, 20} {
		if _, err := Write(dir, "AAPL", Header{Codec: CodecZstd, Seq: seq}, payload); err != nil {
			t.Fatal(err)
		}
	}
	// A temp file must be ignored.
	if err := os.WriteFile(filepath.Join(dir, "AAPL.snap.99.tmp"), []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	path, h, ok, err := Latest(dir, "AAPL")
	if err != nil || !ok {
		t.Fatalf("Latest: ok=%v err=%v", ok, err)
	}
	if h.Seq != 30 {
		t.Fatalf("Latest seq = %d, want 30 (%s)", h.Seq, path)
	}
}

func TestLatestMissing(t *testing.T) {
	_, _, ok, err := Latest(t.TempDir(), "NOPE")
	if err != nil || ok {
		t.Fatalf("Latest on empty dir: ok=%v err=%v", ok, err)
	}
}

func TestReadRejectsCorruptHeader(t *testing.T) {
	dir := t.TempDir()
	path, err := Write(dir, "AAPL", Header{Codec: CodecGzip, Seq: 1}, []byte("data"))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(path)
	b[0] ^= 0xFF // corrupt magic
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Read(path); err != ErrBadMagic {
		t.Fatalf("Read err = %v, want ErrBadMagic", err)
	}
}
