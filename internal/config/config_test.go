package config

import (
	"os"
	"path/filepath"
	"testing"
)

func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tradeit.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadOverridesDefaults(t *testing.T) {
	path := write(t, `
symbols = ["AAPL", "GOOG"]

[ingest]
dir = "/tmp/ipc"
ring_capacity = 4096
drain_batch = 128
sync_every_n = 32
sync_every_ms = 2
`)
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Symbols) != 2 || c.Symbols[0] != "AAPL" || c.Symbols[1] != "GOOG" {
		t.Fatalf("symbols = %v", c.Symbols)
	}
	if c.Ingest.Dir != "/tmp/ipc" || c.Ingest.RingCapacity != 4096 || c.Ingest.DrainBatch != 128 {
		t.Fatalf("ingest = %+v", c.Ingest)
	}
	tbl, err := c.SymbolTable()
	if err != nil {
		t.Fatal(err)
	}
	if id, ok := tbl.ID("GOOG"); !ok || id != 1 {
		t.Fatalf("GOOG id = %d,%v want 1", id, ok)
	}
}

func TestLoadKeepsDefaultsForOmittedFields(t *testing.T) {
	path := write(t, `symbols = ["AAPL"]`)
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Ingest.RingCapacity != Default().Ingest.RingCapacity {
		t.Fatalf("ring capacity = %d, want default", c.Ingest.RingCapacity)
	}
}

func TestValidateRejectsBadRingCapacity(t *testing.T) {
	path := write(t, "[ingest]\nring_capacity = 1000\n")
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for non-power-of-two ring capacity")
	}
}

func TestValidateRejectsDuplicateSymbols(t *testing.T) {
	path := write(t, `symbols = ["AAPL", "AAPL"]`)
	if _, err := Load(path); err == nil {
		t.Fatal("expected error for duplicate symbols")
	}
}

func TestDefaultIsValid(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
}
