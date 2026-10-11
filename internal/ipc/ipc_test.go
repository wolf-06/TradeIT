package ipc

import (
	"bytes"
	"math"
	"testing"
)

func TestCmdRoundTrip(t *testing.T) {
	in := Cmd{
		RequestID: math.MaxUint64,
		OrderID:   0x0123456789abcdef,
		Seq:       1 << 40,
		Price:     18540,
		UserID:    -9,
		Version:   7,
		SymbolID:  3,
		Qty:       -5, // exercise signed round-trip
		Remq:      42,
		Op:        OpModify,
		Side:      SideSell,
		Typ:       TypeMarket,
	}
	var buf [CmdSize]byte
	in.Encode(buf[:])
	got := DecodeCmd(buf[:])
	if got != in {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, in)
	}
}

func TestOutcomeRoundTrip(t *testing.T) {
	in := Outcome{AvgPrice: -12345, FilledQty: -7, Err: 0xBEEF, Status: StatusPartiallyCancelled}
	var buf [OutcomeSize]byte
	in.Encode(buf[:])
	if got := DecodeOutcome(buf[:]); got != in {
		t.Fatalf("round trip mismatch: got %+v want %+v", got, in)
	}
}

func TestAppliedRoundTrip(t *testing.T) {
	c := Cmd{RequestID: 1, OrderID: 2, Seq: 3, Price: 100, Version: 1, SymbolID: 2, Qty: 10, Remq: 4, Op: OpNew, Side: SideBuy, Typ: TypeLimit}
	o := Outcome{AvgPrice: 100, FilledQty: 6, Err: 0, Status: StatusPartiallyFilled}
	buf := make([]byte, AppliedSize)
	EncodeApplied(buf, c, o)
	gotC, gotO := DecodeApplied(buf)
	if gotC != c || gotO != o {
		t.Fatalf("applied round trip mismatch:\n got %+v %+v\nwant %+v %+v", gotC, gotO, c, o)
	}
}

// TestEncodeIsCanonical verifies reserved bytes are zeroed and repeated encodes are
// byte-identical, so WAL CRCs are stable for equal records.
func TestEncodeIsCanonical(t *testing.T) {
	c := Cmd{RequestID: 1, OrderID: 2, Seq: 3, Price: 4, Version: 5, SymbolID: 6, Qty: 7, Remq: 8, Op: OpCancel, Side: SideBuy, Typ: TypeLimit}
	a := make([]byte, CmdSize)
	b := make([]byte, CmdSize)
	c.Encode(a)
	c.Encode(b)
	if !bytes.Equal(a, b) {
		t.Fatal("repeated encodes differ")
	}
	for i := offTyp + 1; i < CmdSize; i++ {
		if a[i] != 0 {
			t.Fatalf("reserved byte %d = %d, want 0", i, a[i])
		}
	}
}

// TestDecodeIgnoresReserved confirms reserved bytes do not affect the decoded value.
func TestDecodeIgnoresReserved(t *testing.T) {
	c := Cmd{RequestID: 9, OrderID: 8, Seq: 7, Price: 6, Version: 5, SymbolID: 4, Qty: 3, Remq: 2, Op: OpReduceQty, Side: SideSell, Typ: TypeMarket}
	buf := make([]byte, CmdSize)
	c.Encode(buf)
	for i := offTyp + 1; i < CmdSize; i++ {
		buf[i] = 0xFF
	}
	if got := DecodeCmd(buf); got != c {
		t.Fatalf("reserved bytes affected decode: got %+v want %+v", got, c)
	}
}

func TestEnumStrings(t *testing.T) {
	if OpNew.String() != "new" || OpCancel.String() != "cancel" || OpReduceQty.String() != "reduce_qty" || OpModify.String() != "modify" {
		t.Fatal("op strings")
	}
	if SideBuy.String() != "buy" || SideSell.String() != "sell" {
		t.Fatal("side strings")
	}
	if TypeLimit.String() != "limit" || TypeMarket.String() != "market" {
		t.Fatal("type strings")
	}
}
