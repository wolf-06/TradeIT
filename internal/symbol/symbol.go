// Package symbol maps between symbol names (used on the wire and in the database) and
// the compact integer ids used inside the engine and the shared-memory ring. The table
// is fixed at startup from configuration so every service encodes and decodes ids
// identically; ids are simply the index of the name in the configured order.
package symbol

import (
	"errors"
	"fmt"
)

const (
	// MaxNameLen is the maximum symbol name length in bytes.
	MaxNameLen = 16
	// MaxSymbols bounds the table so ids stay small and the ring fan-out is sane.
	MaxSymbols = 1024
)

var (
	ErrEmptyName   = errors.New("symbol: name must not be empty")
	ErrNameTooLong = errors.New("symbol: name exceeds MaxNameLen")
	ErrDuplicate   = errors.New("symbol: duplicate name")
	ErrTooMany     = errors.New("symbol: too many symbols")
)

// Table is an immutable, ordered mapping between symbol names and ids.
type Table struct {
	names []string
	index map[string]uint32
}

// New builds a table from names in order: names[i] has id i.
func New(names []string) (*Table, error) {
	if len(names) > MaxSymbols {
		return nil, ErrTooMany
	}
	t := &Table{
		names: make([]string, 0, len(names)),
		index: make(map[string]uint32, len(names)),
	}
	for i, n := range names {
		if n == "" {
			return nil, ErrEmptyName
		}
		if len(n) > MaxNameLen {
			return nil, fmt.Errorf("%w: %q", ErrNameTooLong, n)
		}
		if _, dup := t.index[n]; dup {
			return nil, fmt.Errorf("%w: %q", ErrDuplicate, n)
		}
		t.index[n] = uint32(i)
		t.names = append(t.names, n)
	}
	return t, nil
}

// Len returns the number of symbols.
func (t *Table) Len() int { return len(t.names) }

// ID returns the id for name and whether it exists.
func (t *Table) ID(name string) (uint32, bool) {
	id, ok := t.index[name]
	return id, ok
}

// Name returns the name for id and whether it exists.
func (t *Table) Name(id uint32) (string, bool) {
	if int(id) >= len(t.names) {
		return "", false
	}
	return t.names[id], true
}

// Names returns a copy of the ordered names.
func (t *Table) Names() []string { return append([]string(nil), t.names...) }
