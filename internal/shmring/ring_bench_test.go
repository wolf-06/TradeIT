package shmring

import (
	"encoding/binary"
	"path/filepath"
	"runtime"
	"testing"
)

func benchRing(b *testing.B, capacity, slotSize int) *Ring {
	b.Helper()
	r, err := Create(filepath.Join(b.TempDir(), "ring"), capacity, slotSize)
	if err != nil {
		b.Fatalf("Create: %v", err)
	}
	b.Cleanup(func() { r.Close() })
	return r
}

// BenchmarkPublishDrain measures the in-process cost of one publish + one drain of a
// single 64-byte record (the ring's own overhead, no encode/decode).
func BenchmarkPublishDrain(b *testing.B) {
	r := benchRing(b, 1<<16, 64)
	in := make([]byte, 64)
	out := make([]byte, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for !r.Publish(in) {
			runtime.Gosched()
		}
		for r.Drain(out, 1) == 0 {
			runtime.Gosched()
		}
	}
}

// BenchmarkPublishDrainBatch measures batched publish + drain of 64-byte records.
func BenchmarkPublishDrainBatch(b *testing.B) {
	const batch = 256
	r := benchRing(b, 1<<16, 64)
	in := make([][]byte, batch)
	for i := range in {
		in[i] = make([]byte, 64)
		binary.LittleEndian.PutUint64(in[i], uint64(i))
	}
	out := make([]byte, 64*batch)
	b.ResetTimer()
	for i := 0; i < b.N; i += batch {
		r.PublishBatch(in)
		r.Drain(out, batch)
	}
}
