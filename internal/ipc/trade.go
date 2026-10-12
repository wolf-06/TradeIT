package ipc

import "encoding/binary"

// Record kinds in the consumer event log (the durable applied log the projector reads).
const (
	RecordApplied uint8 = 0 // an applied command + its outcome (order status)
	RecordTrade   uint8 = 1 // a matched trade
)

// TradeSize is the fixed encoded size of a Trade.
const TradeSize = 64

// Record sizes: a one-byte kind followed by the payload.
const (
	AppliedRecordSize = 1 + AppliedSize
	TradeRecordSize   = 1 + TradeSize
)

// Trade is a matched trade. It is written to the consumer event log and projected to the
// database; the symbol is carried as an id (the projector resolves the name).
type Trade struct {
	TradeID    uint64
	BidOrderID uint64
	AskOrderID uint64
	Buyer      int64
	Seller     int64
	Price      int64
	ExecutedAt int64 // unix nanoseconds
	SymbolID   uint32
	Quantity   int32
}

const (
	offTradeID    = 0
	offBidOrderID = 8
	offAskOrderID = 16
	offBuyer      = 24
	offSeller     = 32
	offTradePrice = 40
	offExecutedAt = 48
	offTradeSym   = 56
	offTradeQty   = 60
)

// Encode writes t into dst, which must be at least TradeSize bytes.
func (t Trade) Encode(dst []byte) {
	_ = dst[TradeSize-1]
	binary.LittleEndian.PutUint64(dst[offTradeID:], t.TradeID)
	binary.LittleEndian.PutUint64(dst[offBidOrderID:], t.BidOrderID)
	binary.LittleEndian.PutUint64(dst[offAskOrderID:], t.AskOrderID)
	binary.LittleEndian.PutUint64(dst[offBuyer:], uint64(t.Buyer))
	binary.LittleEndian.PutUint64(dst[offSeller:], uint64(t.Seller))
	binary.LittleEndian.PutUint64(dst[offTradePrice:], uint64(t.Price))
	binary.LittleEndian.PutUint64(dst[offExecutedAt:], uint64(t.ExecutedAt))
	binary.LittleEndian.PutUint32(dst[offTradeSym:], t.SymbolID)
	binary.LittleEndian.PutUint32(dst[offTradeQty:], uint32(t.Quantity))
}

// DecodeTrade reads a Trade from src, which must be at least TradeSize bytes.
func DecodeTrade(src []byte) Trade {
	_ = src[TradeSize-1]
	return Trade{
		TradeID:    binary.LittleEndian.Uint64(src[offTradeID:]),
		BidOrderID: binary.LittleEndian.Uint64(src[offBidOrderID:]),
		AskOrderID: binary.LittleEndian.Uint64(src[offAskOrderID:]),
		Buyer:      int64(binary.LittleEndian.Uint64(src[offBuyer:])),
		Seller:     int64(binary.LittleEndian.Uint64(src[offSeller:])),
		Price:      int64(binary.LittleEndian.Uint64(src[offTradePrice:])),
		ExecutedAt: int64(binary.LittleEndian.Uint64(src[offExecutedAt:])),
		SymbolID:   binary.LittleEndian.Uint32(src[offTradeSym:]),
		Quantity:   int32(binary.LittleEndian.Uint32(src[offTradeQty:])),
	}
}

// EncodeAppliedRecord writes a kind byte followed by a Cmd and its Outcome. dst must be at
// least AppliedRecordSize bytes.
func EncodeAppliedRecord(dst []byte, c Cmd, o Outcome) {
	_ = dst[AppliedRecordSize-1]
	dst[0] = RecordApplied
	EncodeApplied(dst[1:], c, o)
}

// EncodeTradeRecord writes a kind byte followed by a Trade. dst must be at least
// TradeRecordSize bytes.
func EncodeTradeRecord(dst []byte, t Trade) {
	_ = dst[TradeRecordSize-1]
	dst[0] = RecordTrade
	t.Encode(dst[1:])
}

// RecordKind returns the kind byte of a record payload.
func RecordKind(src []byte) uint8 { return src[0] }
