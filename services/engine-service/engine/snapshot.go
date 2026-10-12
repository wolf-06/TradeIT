package engine

import (
	"encoding/binary"
	"errors"
	"io"
	"math/bits"

	"TradeIT/internal/models"
)

// snapshotBodyVersion versions the book body layout independently of the snapshot file
// header.
const snapshotBodyVersion uint32 = 1

// Snapshot serialises the whole book (counters, last traded price, market queues and all
// occupied price levels) into a self-describing byte slice. The ladder array/bitmap and
// the order table are not stored: they are rebuilt from the levels on Restore.
func (ob *Orderbook) Snapshot() []byte {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	b := make([]byte, 0, 1<<12)
	b = appendU32(b, snapshotBodyVersion)
	b = appendU64(b, uint64(ob.lastTradedPrice))
	b = appendU64(b, uint64(ob.tradeCount))
	b = appendU64(b, uint64(ob.buyCount))
	b = appendU64(b, uint64(ob.sellCount))
	b = appendMarket(b, &ob.marketBuyOrders)
	b = appendMarket(b, &ob.marketSellOrders)
	b = appendLadder(b, ob.bids)
	b = appendLadder(b, ob.asks)
	return b
}

// Restore replaces the book with the state encoded by Snapshot. name is the symbol name,
// which the restored orders carry (the body does not store it).
func (ob *Orderbook) Restore(name string, data []byte) error {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	r := &reader{b: data}
	if v := r.u32(); v != snapshotBodyVersion {
		return errors.New("engine: unsupported snapshot body version")
	}
	lastTradedPrice := int64(r.u64())
	tradeCount := int64(r.u64())
	buyCount := int64(r.u64())
	sellCount := int64(r.u64())

	ob.bids = &ladder{isBid: true, best: -1}
	ob.asks = &ladder{isBid: false, best: -1}
	ob.orderTable = make(map[uint64]*Node)
	ob.marketBuyOrders = priceLevel{}
	ob.marketSellOrders = priceLevel{}

	restoreMarket(ob, &ob.marketBuyOrders, r, name, sideBuy, typeMarket)
	restoreMarket(ob, &ob.marketSellOrders, r, name, sideSell, typeMarket)
	restoreLadder(ob, ob.bids, r, name, sideBuy)
	restoreLadder(ob, ob.asks, r, name, sideSell)

	ob.lastTradedPrice = lastTradedPrice
	ob.tradeCount = tradeCount
	ob.buyCount = buyCount
	ob.sellCount = sellCount
	return r.err
}

func appendMarket(b []byte, lvl *priceLevel) []byte {
	b = appendU32(b, uint32(lvl.size))
	for n := lvl.head; n != nil; n = n.next {
		b = appendOrder(b, n)
	}
	return b
}

func appendLadder(b []byte, l *ladder) []byte {
	occupied := occupiedIndices(l)
	b = appendU32(b, uint32(len(occupied)))
	for _, idx := range occupied {
		lvl := l.levels[idx]
		b = appendU64(b, uint64(l.origin+int64(idx)))
		b = appendU32(b, uint32(lvl.size))
		for n := lvl.head; n != nil; n = n.next {
			b = appendOrder(b, n)
		}
	}
	return b
}

func occupiedIndices(l *ladder) []int {
	var out []int
	for wi, w := range l.words {
		for w != 0 {
			bit := bits.TrailingZeros64(w)
			idx := wi*64 + bit
			w &^= 1 << uint(bit)
			if lvl := l.levels[idx]; lvl != nil && lvl.size > 0 {
				out = append(out, idx)
			}
		}
	}
	return out
}

func appendOrder(b []byte, n *Node) []byte {
	b = appendU64(b, n.Order_.Id)
	b = appendU64(b, uint64(n.Order_.User_id))
	b = appendU32(b, uint32(n.Order_.Quantity))
	b = appendU32(b, uint32(n.Order_.Remq))
	b = appendU64(b, uint64(n.Order_.AvgPrice))
	return b
}

func restoreMarket(ob *Orderbook, lvl *priceLevel, r *reader, name string, side, typ uint8) {
	n := r.u32()
	for i := uint32(0); i < n; i++ {
		o := r.order(name, 0, side, typ)
		ob.orderTable[o.Id] = lvl.pushBack(o, side, typ)
	}
}

func restoreLadder(ob *Orderbook, l *ladder, r *reader, name string, side uint8) {
	n := r.u32()
	for i := uint32(0); i < n; i++ {
		price := int64(r.u64())
		count := r.u32()
		lvl := l.addLevel(price)
		for j := uint32(0); j < count; j++ {
			o := r.order(name, price, side, typeLimit)
			ob.orderTable[o.Id] = lvl.pushBack(o, side, typeLimit)
		}
	}
}

func appendU32(b []byte, v uint32) []byte {
	var t [4]byte
	binary.LittleEndian.PutUint32(t[:], v)
	return append(b, t[:]...)
}

func appendU64(b []byte, v uint64) []byte {
	var t [8]byte
	binary.LittleEndian.PutUint64(t[:], v)
	return append(b, t[:]...)
}

// reader is a bounds-checked little-endian cursor; the first failure is sticky.
type reader struct {
	b   []byte
	off int
	err error
}

func (r *reader) u32() uint32 {
	if r.err != nil || r.off+4 > len(r.b) {
		r.err = io.ErrUnexpectedEOF
		return 0
	}
	v := binary.LittleEndian.Uint32(r.b[r.off:])
	r.off += 4
	return v
}

func (r *reader) u64() uint64 {
	if r.err != nil || r.off+8 > len(r.b) {
		r.err = io.ErrUnexpectedEOF
		return 0
	}
	v := binary.LittleEndian.Uint64(r.b[r.off:])
	r.off += 8
	return v
}

func (r *reader) order(name string, price int64, side, typ uint8) models.Order {
	id := r.u64()
	userID := int64(r.u64())
	qty := int32(r.u32())
	remq := int32(r.u32())
	avg := int64(r.u64())
	return models.Order{
		MetaOrder: models.MetaOrder{
			Id:         id,
			User_id:    int(userID),
			Order_Type: typeString(typ),
			Side:       sideString(side),
			Stock:      name,
			Price:      price,
			Quantity:   int(qty),
			AvgPrice:   avg,
		},
		Remq: int(remq),
	}
}

func sideString(s uint8) string {
	if s == sideSell {
		return "sell"
	}
	return "buy"
}

func typeString(t uint8) string {
	if t == typeMarket {
		return "market"
	}
	return "limit"
}
