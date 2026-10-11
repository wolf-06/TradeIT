// Package config loads the shared TradeIT configuration from a TOML file. The file
// location is provided via the TRADEIT_CONFIG environment variable (secrets and
// deployment-specific paths live in .env); the file itself holds structured settings
// such as the ordered symbol list and the ingest knobs.
//
// The symbol list is the single source of truth for symbol ids: both services load the
// same file, so name/id encoding and decoding cannot drift.
package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/pelletier/go-toml/v2"

	"TradeIT/internal/symbol"
)

// Config is the root configuration document.
type Config struct {
	Symbols []string `toml:"symbols"`
	Ingest  Ingest   `toml:"ingest"`
}

// Ingest holds the shared-memory ingest settings.
type Ingest struct {
	// Enabled selects the shared-memory ring path over the legacy Redis path. Off by
	// default so nothing changes until it is explicitly turned on.
	Enabled bool `toml:"enabled"`
	// Dir is the directory holding the per-symbol ring and WAL files. Both services
	// must point at the same directory (a shared volume in Docker).
	Dir string `toml:"dir"`
	// RingCapacity is the number of slots per symbol ring (power of two).
	RingCapacity int `toml:"ring_capacity"`
	// DrainBatch is the maximum records the consumer processes per symbol per pass.
	DrainBatch int `toml:"drain_batch"`
	// SyncEveryN and SyncEveryMs bound the producer group commit: the WAL is fsynced
	// after N appends or Ms milliseconds, whichever comes first.
	SyncEveryN  int `toml:"sync_every_n"`
	SyncEveryMs int `toml:"sync_every_ms"`
	// SnapshotEveryN triggers a book snapshot after N applied commands for a symbol.
	SnapshotEveryN int `toml:"snapshot_every_n"`
	// SnapshotEveryMs triggers a book snapshot for a symbol after this long.
	SnapshotEveryMs int `toml:"snapshot_every_ms"`
	// SnapshotCodec is the snapshot body compression: "none", "gzip" or "zstd".
	SnapshotCodec string `toml:"snapshot_codec"`
}

// Default returns the built-in configuration (used as the base for Load and in tests).
func Default() *Config {
	return &Config{
		Symbols: []string{"AAPL", "GOOG", "TSLA", "MSFT"},
		Ingest: Ingest{
			Dir:             "./ipc",
			RingCapacity:    1 << 18,
			DrainBatch:      1024,
			SyncEveryN:      256,
			SyncEveryMs:     5,
			SnapshotEveryN:  100000,
			SnapshotEveryMs: 30000,
			SnapshotCodec:   "zstd",
		},
	}
}

// Load reads and validates a TOML configuration file. Values not present fall back to
// Default.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	c := Default()
	if err := toml.Unmarshal(data, c); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// LoadFromEnv loads the config file named by TRADEIT_CONFIG.
func LoadFromEnv() (*Config, error) {
	path := os.Getenv("TRADEIT_CONFIG")
	if path == "" {
		return nil, fmt.Errorf("config: TRADEIT_CONFIG is not set")
	}
	return Load(path)
}

// LoadOptional loads the config file named by TRADEIT_CONFIG, or returns (nil, nil) when
// the variable is unset. It lets a service fall back to its legacy behaviour when ingest
// is not configured.
func LoadOptional() (*Config, error) {
	path := os.Getenv("TRADEIT_CONFIG")
	if path == "" {
		return nil, nil
	}
	return Load(path)
}

// Validate checks the configuration and returns the first problem found.
func (c *Config) Validate() error {
	if _, err := symbol.New(c.Symbols); err != nil {
		return fmt.Errorf("config: symbols: %w", err)
	}
	if c.Ingest.Dir == "" {
		return fmt.Errorf("config: ingest.dir must be set")
	}
	if cap := c.Ingest.RingCapacity; cap <= 0 || cap&(cap-1) != 0 {
		return fmt.Errorf("config: ingest.ring_capacity must be a power of two, got %d", cap)
	}
	if c.Ingest.DrainBatch <= 0 {
		return fmt.Errorf("config: ingest.drain_batch must be > 0, got %d", c.Ingest.DrainBatch)
	}
	if c.Ingest.SyncEveryN < 0 || c.Ingest.SyncEveryMs < 0 {
		return fmt.Errorf("config: sync_every_n and sync_every_ms must be >= 0")
	}
	return nil
}

// SymbolTable builds the shared symbol table from the configured order.
func (c *Config) SymbolTable() (*symbol.Table, error) { return symbol.New(c.Symbols) }

// ControlPath returns the shared watermark region path (inside the ingest directory).
func (c *Config) ControlPath() string { return filepath.Join(c.Ingest.Dir, "control") }
