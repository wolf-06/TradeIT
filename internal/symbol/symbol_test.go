package symbol

import (
	"errors"
	"testing"
)

func TestLookup(t *testing.T) {
	tbl, err := New([]string{"AAPL", "GOOG", "TSLA", "MSFT"})
	if err != nil {
		t.Fatal(err)
	}
	if tbl.Len() != 4 {
		t.Fatalf("Len = %d, want 4", tbl.Len())
	}
	for i, name := range []string{"AAPL", "GOOG", "TSLA", "MSFT"} {
		id, ok := tbl.ID(name)
		if !ok || id != uint32(i) {
			t.Fatalf("ID(%s) = %d,%v want %d", name, id, ok, i)
		}
		got, ok := tbl.Name(uint32(i))
		if !ok || got != name {
			t.Fatalf("Name(%d) = %q,%v want %q", i, got, ok, name)
		}
	}
	if _, ok := tbl.ID("NONE"); ok {
		t.Fatal("ID of unknown symbol should be false")
	}
	if _, ok := tbl.Name(4); ok {
		t.Fatal("Name of out-of-range id should be false")
	}
}

func TestValidation(t *testing.T) {
	if _, err := New([]string{"A", "A"}); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("dup err = %v", err)
	}
	if _, err := New([]string{""}); !errors.Is(err, ErrEmptyName) {
		t.Fatalf("empty err = %v", err)
	}
	long := make([]byte, MaxNameLen+1)
	for i := range long {
		long[i] = 'A'
	}
	if _, err := New([]string{string(long)}); !errors.Is(err, ErrNameTooLong) {
		t.Fatalf("long err = %v, want ErrNameTooLong", err)
	}
}

func TestNamesIsCopy(t *testing.T) {
	tbl, _ := New([]string{"AAPL", "GOOG"})
	names := tbl.Names()
	names[0] = "MUT"
	if got, _ := tbl.Name(0); got != "AAPL" {
		t.Fatalf("Names() leaked internal slice: Name(0) = %q", got)
	}
}
