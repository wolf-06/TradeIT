package wal

import (
	"os"
	"path/filepath"
	"testing"
)

// benchPath returns a WAL path on tmpfs when available, so the fsync cost reflects the
// same setup the producer benchmark used (ring+WAL in /dev/shm).
func benchPath(b *testing.B) string {
	b.Helper()
	if fi, err := os.Stat("/dev/shm"); err == nil && fi.IsDir() {
		if d, err := os.MkdirTemp("/dev/shm", "wal-bench-"); err == nil {
			b.Cleanup(func() { os.RemoveAll(d) })
			return filepath.Join(d, "wal")
		}
	}
	return filepath.Join(b.TempDir(), "wal")
}

// BenchmarkAppendOnly isolates the cost of framing and buffering a record (no fsync).
func BenchmarkAppendOnly(b *testing.B) {
	w, err := CreateWriter(benchPath(b))
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	rec := make([]byte, 72)
	b.SetBytes(int64(len(rec)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Append(rec); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAppendSyncPerRecord is one append followed by one fsync (the producer's
// syncEveryN=1 path).
func BenchmarkAppendSyncPerRecord(b *testing.B) {
	w, err := CreateWriter(benchPath(b))
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	rec := make([]byte, 72)
	b.SetBytes(int64(len(rec)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Append(rec); err != nil {
			b.Fatal(err)
		}
		if err := w.Sync(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAppendSyncBatch256 amortises one fsync over 256 appends (the group-commit
// path with a full batch).
func BenchmarkAppendSyncBatch256(b *testing.B) {
	const batch = 256
	w, err := CreateWriter(benchPath(b))
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	rec := make([]byte, 72)
	b.SetBytes(int64(len(rec)))
	b.ResetTimer()
	for i := 0; i < b.N; i += batch {
		for j := 0; j < batch; j++ {
			if err := w.Append(rec); err != nil {
				b.Fatal(err)
			}
		}
		if err := w.Sync(); err != nil {
			b.Fatal(err)
		}
	}
}
