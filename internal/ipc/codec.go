package ipc

import "encoding/binary"

// Fixed encoded sizes. CmdSize is one cache line so a ring slot holds exactly one
// command and producer/consumer writes never share a line at record granularity.
const (
	CmdSize     = 64
	OutcomeSize = 16
	AppliedSize = CmdSize + OutcomeSize
)

// Cmd field offsets within the 64-byte encoding.
const (
	offRequestID = 0
	offOrderID   = 8
	offSeq       = 16
	offPrice     = 24
	offUserID    = 32
	offVersion   = 40
	offSymbolID  = 44
	offQty       = 48
	offRemq      = 52
	offOp        = 56
	offSide      = 57
	offTyp       = 58
)

// Outcome field offsets within the 16-byte encoding.
const (
	offAvgPrice  = 0
	offFilledQty = 8
	offErr       = 12
	offStatus    = 14
)

// Encode writes c into dst, which must be at least CmdSize bytes. Reserved bytes are
// zeroed so the encoding is canonical (important for WAL CRCs).
func (c Cmd) Encode(dst []byte) {
	_ = dst[CmdSize-1]
	binary.LittleEndian.PutUint64(dst[offRequestID:], c.RequestID)
	binary.LittleEndian.PutUint64(dst[offOrderID:], c.OrderID)
	binary.LittleEndian.PutUint64(dst[offSeq:], c.Seq)
	binary.LittleEndian.PutUint64(dst[offPrice:], uint64(c.Price))
	binary.LittleEndian.PutUint64(dst[offUserID:], uint64(c.UserID))
	binary.LittleEndian.PutUint32(dst[offVersion:], c.Version)
	binary.LittleEndian.PutUint32(dst[offSymbolID:], c.SymbolID)
	binary.LittleEndian.PutUint32(dst[offQty:], uint32(c.Qty))
	binary.LittleEndian.PutUint32(dst[offRemq:], uint32(c.Remq))
	dst[offOp] = byte(c.Op)
	dst[offSide] = byte(c.Side)
	dst[offTyp] = byte(c.Typ)
	for i := offTyp + 1; i < CmdSize; i++ {
		dst[i] = 0
	}
}

// DecodeCmd reads a Cmd from src, which must be at least CmdSize bytes.
func DecodeCmd(src []byte) Cmd {
	_ = src[CmdSize-1]
	return Cmd{
		RequestID: binary.LittleEndian.Uint64(src[offRequestID:]),
		OrderID:   binary.LittleEndian.Uint64(src[offOrderID:]),
		Seq:       binary.LittleEndian.Uint64(src[offSeq:]),
		Price:     int64(binary.LittleEndian.Uint64(src[offPrice:])),
		UserID:    int64(binary.LittleEndian.Uint64(src[offUserID:])),
		Version:   binary.LittleEndian.Uint32(src[offVersion:]),
		SymbolID:  binary.LittleEndian.Uint32(src[offSymbolID:]),
		Qty:       int32(binary.LittleEndian.Uint32(src[offQty:])),
		Remq:      int32(binary.LittleEndian.Uint32(src[offRemq:])),
		Op:        OpType(src[offOp]),
		Side:      Side(src[offSide]),
		Typ:       OrderType(src[offTyp]),
	}
}

// Encode writes o into dst, which must be at least OutcomeSize bytes.
func (o Outcome) Encode(dst []byte) {
	_ = dst[OutcomeSize-1]
	binary.LittleEndian.PutUint64(dst[offAvgPrice:], uint64(o.AvgPrice))
	binary.LittleEndian.PutUint32(dst[offFilledQty:], uint32(o.FilledQty))
	binary.LittleEndian.PutUint16(dst[offErr:], o.Err)
	dst[offStatus] = byte(o.Status)
	dst[OutcomeSize-1] = 0
}

// DecodeOutcome reads an Outcome from src, which must be at least OutcomeSize bytes.
func DecodeOutcome(src []byte) Outcome {
	_ = src[OutcomeSize-1]
	return Outcome{
		AvgPrice:  int64(binary.LittleEndian.Uint64(src[offAvgPrice:])),
		FilledQty: int32(binary.LittleEndian.Uint32(src[offFilledQty:])),
		Err:       binary.LittleEndian.Uint16(src[offErr:]),
		Status:    Status(src[offStatus]),
	}
}

// EncodeApplied writes a Cmd followed by its Outcome. dst must be at least AppliedSize.
func EncodeApplied(dst []byte, c Cmd, o Outcome) {
	_ = dst[AppliedSize-1]
	c.Encode(dst[:CmdSize])
	o.Encode(dst[CmdSize:AppliedSize])
}

// DecodeApplied reads a Cmd and its Outcome. src must be at least AppliedSize bytes.
func DecodeApplied(src []byte) (Cmd, Outcome) {
	_ = src[AppliedSize-1]
	return DecodeCmd(src[:CmdSize]), DecodeOutcome(src[CmdSize:AppliedSize])
}
