package control

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWatermarkRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control")
	r, err := Create(path, 4)
	if err != nil {
		t.Fatal(err)
	}
	if r.Count() != 4 {
		t.Fatalf("Count = %d, want 4", r.Count())
	}
	r.SetWatermark(2, 12345)
	if got := r.Watermark(2); got != 12345 {
		t.Fatalf("Watermark = %d, want 12345", got)
	}
	r.Close()

	r2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if got := r2.Watermark(2); got != 12345 {
		t.Fatalf("after reopen Watermark = %d, want 12345", got)
	}
	if got := r2.Watermark(0); got != 0 {
		t.Fatalf("Watermark(0) = %d, want 0", got)
	}
	if got := r2.Watermark(99); got != 0 {
		t.Fatalf("out-of-range Watermark = %d, want 0", got)
	}
}

func TestOpenOrCreate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control")
	r, err := OpenOrCreate(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	r.SetWatermark(1, 7)
	r.Close()

	r2, err := OpenOrCreate(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if got := r2.Watermark(1); got != 7 {
		t.Fatalf("Watermark = %d, want 7", got)
	}
}

func TestOpenRejectsBadMagic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control")
	r, _ := Create(path, 2)
	r.Close()
	if err := os.WriteFile(path, []byte("not-a-control-region"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); err == nil {
		t.Fatal("expected Open to reject a corrupt region")
	}
}
