package wal

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func payloads(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = bytes.Repeat([]byte{byte(i)}, 8+i)
	}
	return out
}

func TestAppendReadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, err := CreateWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	ps := payloads(5)
	for _, p := range ps {
		if err := w.Append(p); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for i, want := range ps {
		got, err := r.Next()
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("record %d mismatch", i)
		}
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("end err = %v, want io.EOF", err)
	}
}

func TestRecoverClean(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, _ := CreateWriter(path)
	for _, p := range payloads(3) {
		w.Append(p)
	}
	w.Sync()
	w.Close()

	fi, _ := os.Stat(path)
	end, err := Recover(path)
	if err != nil {
		t.Fatal(err)
	}
	if end != fi.Size() {
		t.Fatalf("Recover end = %d, want file size %d", end, fi.Size())
	}
}

func TestRecoverTornPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, _ := CreateWriter(path)
	ps := payloads(2)
	w.Append(ps[0])
	w.Append(ps[1])
	w.Sync()
	w.Close()

	// First record occupies headerLen+len(ps[0]) bytes. Chop the tail of record 2.
	firstEnd := int64(headerLen + len(ps[0]))
	fi, _ := os.Stat(path)
	if err := os.Truncate(path, fi.Size()-3); err != nil {
		t.Fatal(err)
	}
	end, err := Recover(path)
	if err != nil {
		t.Fatal(err)
	}
	if end != firstEnd {
		t.Fatalf("Recover end = %d, want %d (after record 1)", end, firstEnd)
	}
}

func TestRecoverPartialHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, _ := CreateWriter(path)
	ps := payloads(2)
	w.Append(ps[0])
	w.Append(ps[1])
	w.Sync()
	w.Close()

	firstEnd := int64(headerLen + len(ps[0]))
	if err := os.Truncate(path, firstEnd+3); err != nil { // 3 bytes of record 2's header
		t.Fatal(err)
	}
	end, err := Recover(path)
	if err != nil {
		t.Fatal(err)
	}
	if end != firstEnd {
		t.Fatalf("Recover end = %d, want %d", end, firstEnd)
	}
}

func TestCorruptCRC(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, _ := CreateWriter(path)
	w.Append(payloads(1)[0])
	w.Sync()
	w.Close()

	// Flip a byte inside record 1's payload (after the 8-byte header).
	b, _ := os.ReadFile(path)
	b[headerLen+1] ^= 0xFF
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	end, err := Recover(path)
	if err != nil {
		t.Fatal(err)
	}
	if end != 0 {
		t.Fatalf("Recover end = %d, want 0 (first record corrupt)", end)
	}
}

func TestRepairTruncatesAndAllowsAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, _ := CreateWriter(path)
	ps := payloads(2)
	w.Append(ps[0])
	w.Append(ps[1])
	w.Sync()
	w.Close()

	fi, _ := os.Stat(path)
	os.Truncate(path, fi.Size()-2) // tear record 2

	end, err := Repair(path)
	if err != nil {
		t.Fatal(err)
	}
	if end != int64(headerLen+len(ps[0])) {
		t.Fatalf("Repair end = %d", end)
	}

	// After repair, appending must work and the log must read back cleanly.
	w2, err := OpenWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	w2.Append([]byte("after-repair"))
	w2.Sync()
	w2.Close()

	r, _ := OpenReader(path)
	defer r.Close()
	if got, _ := r.Next(); !bytes.Equal(got, ps[0]) {
		t.Fatal("record 1 not intact after repair")
	}
	if got, _ := r.Next(); string(got) != "after-repair" {
		t.Fatalf("record 2 = %q, want after-repair", got)
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("end err = %v, want io.EOF", err)
	}
}

func TestDurableAfterSync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, _ := CreateWriter(path)
	w.Append([]byte("durable"))
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	// Do not Close (simulate a crash after fsync); data must still be readable.
	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if got, err := r.Next(); err != nil || string(got) != "durable" {
		t.Fatalf("got %q err %v", got, err)
	}
	w.f.Close()
}

func TestTooLargeRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, _ := CreateWriter(path)
	defer w.Close()
	if err := w.Append(make([]byte, maxRecord+1)); err == nil {
		t.Fatal("expected error for oversized record")
	}
}
