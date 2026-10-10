package shmring

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

func tmpRing(t *testing.T, capacity, slotSize int) *Ring {
	t.Helper()
	r, err := Create(filepath.Join(t.TempDir(), "ring"), capacity, slotSize)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func rec(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}

func TestPublishDrainFIFO(t *testing.T) {
	r := tmpRing(t, 4, 8)
	for i := uint64(1); i <= 3; i++ {
		if !r.Publish(rec(i)) {
			t.Fatalf("publish %d: unexpectedly full", i)
		}
	}
	if got := r.Available(); got != 3 {
		t.Fatalf("Available = %d, want 3", got)
	}
	buf := make([]byte, 8*4)
	n := r.Drain(buf, 4)
	if n != 3 {
		t.Fatalf("Drain = %d, want 3", n)
	}
	for i := 0; i < 3; i++ {
		if got := binary.LittleEndian.Uint64(buf[i*8:]); got != uint64(i+1) {
			t.Fatalf("record %d = %d, want %d", i, got, i+1)
		}
	}
	if r.Available() != 0 || r.Free() != 4 {
		t.Fatalf("after drain: avail=%d free=%d, want 0/4", r.Available(), r.Free())
	}
}

func TestFullAndEmpty(t *testing.T) {
	r := tmpRing(t, 4, 8)
	for i := uint64(0); i < 4; i++ {
		if !r.Publish(rec(i)) {
			t.Fatalf("publish %d failed before full", i)
		}
	}
	if r.Publish(rec(99)) {
		t.Fatal("publish succeeded on a full ring")
	}
	buf := make([]byte, 8*4)
	if n := r.Drain(buf, 4); n != 4 {
		t.Fatalf("Drain = %d, want 4", n)
	}
	if n := r.Drain(buf, 4); n != 0 {
		t.Fatalf("Drain on empty = %d, want 0", n)
	}
}

// TestWraparoundSequence publishes and drains many more records than the capacity,
// forcing the physical indices to wrap, and checks the logical order is preserved.
func TestWraparoundSequence(t *testing.T) {
	r := tmpRing(t, 4, 8)
	const total = 1000
	buf := make([]byte, 8)
	got := uint64(0)
	for i := uint64(0); i < total; i++ {
		if !r.Publish(rec(i)) {
			t.Fatalf("publish %d failed", i)
		}
		if n := r.Drain(buf, 1); n != 1 {
			t.Fatalf("drain %d returned %d", i, n)
		}
		if v := binary.LittleEndian.Uint64(buf); v != got {
			t.Fatalf("got %d, want %d", v, got)
		}
		got++
	}
	if r.Head() != total || r.Tail() != total {
		t.Fatalf("head=%d tail=%d, want %d/%d", r.Head(), r.Tail(), total, total)
	}
}

func TestPublishBatchPartial(t *testing.T) {
	r := tmpRing(t, 4, 8)
	batch := [][]byte{rec(1), rec(2), rec(3), rec(4), rec(5), rec(6)}
	n := r.PublishBatch(batch)
	if n != 4 {
		t.Fatalf("PublishBatch = %d, want 4 (capacity)", n)
	}
	if got := r.Head(); got != 4 {
		t.Fatalf("Head = %d, want 4", got)
	}
	// Drain two, freeing two slots, then the rest of the batch fits partially.
	buf := make([]byte, 8*4)
	if n := r.Drain(buf, 2); n != 2 {
		t.Fatalf("Drain = %d, want 2", n)
	}
	if n := r.PublishBatch(batch); n != 2 {
		t.Fatalf("second PublishBatch = %d, want 2", n)
	}
}

func TestOpenValidates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ring")
	r, err := Create(path, 4, 8)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	r.Close()

	// Valid attach.
	r2, err := Open(path)
	if err != nil {
		t.Fatalf("Open valid: %v", err)
	}
	if r2.Capacity() != 4 || r2.SlotSize() != 8 {
		t.Fatalf("Open got cap=%d slot=%d", r2.Capacity(), r2.SlotSize())
	}
	r2.Close()

	// Bad magic.
	badMagic := filepath.Join(dir, "badmagic")
	if _, err := Create(badMagic, 4, 8); err != nil {
		t.Fatal(err)
	}
	if err := overwrite(badMagic, offMagic, []byte{0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(badMagic); err != ErrBadMagic {
		t.Fatalf("Open bad magic err = %v, want ErrBadMagic", err)
	}

	// Bad version.
	badVer := filepath.Join(dir, "badver")
	if _, err := Create(badVer, 4, 8); err != nil {
		t.Fatal(err)
	}
	if err := overwrite(badVer, offVersion, []byte{99, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(badVer); err != ErrBadVersion {
		t.Fatalf("Open bad version err = %v, want ErrBadVersion", err)
	}

	// Bad size.
	badSize := filepath.Join(dir, "badsize")
	if _, err := Create(badSize, 4, 8); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(badSize, int64(Size(4, 8)+8)); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(badSize); err != ErrBadSize {
		t.Fatalf("Open bad size err = %v, want ErrBadSize", err)
	}
}

func TestCreateRejectsBadParams(t *testing.T) {
	dir := t.TempDir()
	if _, err := Create(filepath.Join(dir, "a"), 3, 8); err != ErrBadCapacity {
		t.Fatalf("capacity 3 err = %v, want ErrBadCapacity", err)
	}
	if _, err := Create(filepath.Join(dir, "b"), 4, 12); err != ErrBadSlotSize {
		t.Fatalf("slot 12 err = %v, want ErrBadSlotSize", err)
	}
}

// TestDryRunTrace prints a step-by-step trace of head/tail/slot contents so the data
// transformation can be followed by eye (run with -v).
func TestDryRunTrace(t *testing.T) {
	r := tmpRing(t, 4, 8)
	logState := func(step string) {
		t.Logf("%-20s head=%d tail=%d free=%d avail=%d", step, r.Head(), r.Tail(), r.Free(), r.Available())
	}
	logState("initial")

	for _, v := range []uint64{100, 101, 102} {
		ok := r.Publish(rec(v))
		t.Logf("publish %-4d ok=%v", v, ok)
	}
	logState("after 3 publishes")

	buf := make([]byte, 8*4)
	n := r.Drain(buf, 4)
	for i := 0; i < n; i++ {
		t.Logf("drained[%d]=%d", i, binary.LittleEndian.Uint64(buf[i*8:]))
	}
	logState("after drain")

	// Wrap: publish 4 (fills indices 3,0,1,2 after the tail advanced to 3).
	for _, v := range []uint64{200, 201, 202, 203} {
		if !r.Publish(rec(v)) {
			t.Fatalf("publish %d failed", v)
		}
	}
	logState("after wrap fill")
	n = r.Drain(buf, 4)
	for i := 0; i < n; i++ {
		t.Logf("wrapped[%d]=%d", i, binary.LittleEndian.Uint64(buf[i*8:]))
	}
	logState("after wrap drain")
}

// TestConcurrentSPSC runs one producer and one consumer concurrently and asserts every
// record arrives exactly once and in order. Run with -race.
func TestConcurrentSPSC(t *testing.T) {
	const total = 200000
	r := tmpRing(t, 1024, 8)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 8)
		for seq := uint64(0); seq < total; seq++ {
			binary.LittleEndian.PutUint64(buf, seq)
			for !r.Publish(buf) {
				runtime.Gosched()
			}
		}
	}()

	got := uint64(0)
	buf := make([]byte, 8*64)
	for got < total {
		n := r.Drain(buf, 64)
		if n == 0 {
			runtime.Gosched()
			continue
		}
		for i := 0; i < n; i++ {
			if v := binary.LittleEndian.Uint64(buf[i*8:]); v != got {
				t.Fatalf("out of order: got %d, want %d", v, got)
			}
			got++
		}
	}
	wg.Wait()
}

func overwrite(path string, off int64, b []byte) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteAt(b, off)
	return err
}
