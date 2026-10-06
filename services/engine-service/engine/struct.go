package engine

import (
	"math/bits"

	"TradeIT/internal/models"
)

// Node is a resting order inside a price level's FIFO queue. It is referenced from the
// order book's orderTable by order id, so cancel and modify can reach it in O(1) without
// searching any queue.
//
// The embedded models.Order is stored by value and mutated in place: matching decrements
// Remq through this node, and cancel/modify read the live Quantity/Remq/Price from it.
type Node struct {
	Order_ models.Order
	prev   *Node
	next   *Node
}

// priceLevel is a FIFO queue of resting orders at one integer price. It is also used for
// the unpriced market-order queues.
type priceLevel struct {
	head *Node
	tail *Node
	size int
}

func (l *priceLevel) pushBack(o models.Order) *Node {
	n := &Node{Order_: o}
	if l.tail == nil {
		l.head = n
		l.tail = n
	} else {
		n.prev = l.tail
		l.tail.next = n
		l.tail = n
	}
	l.size++
	return n
}

// unlink removes n from the queue. The caller is responsible for checking size and
// clearing the owning price level when it becomes empty.
func (l *priceLevel) unlink(n *Node) {
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		l.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		l.tail = n.prev
	}
	n.prev = nil
	n.next = nil
	l.size--
}

// ladderInitialWindow is the number of price slots the ladder allocates around the first
// price it sees. The window grows on demand (amortized) when prices fall outside it.
const ladderInitialWindow = 4096

// ladder is an array-based price index. levels[i] is the FIFO queue resting at price
// origin+i, and words is an occupancy bitmap over the same indices. Bids treat the
// highest occupied index as best; asks treat the lowest. The bitmap lets the next best
// price be found with a short word scan instead of an O(log n) heap walk.
type ladder struct {
	levels []*priceLevel
	words  []uint64
	origin int64
	best   int
	isBid  bool
}

func (l *ladder) ensureInit(price int64) {
	l.origin = price - ladderInitialWindow/2
	l.levels = make([]*priceLevel, ladderInitialWindow)
	l.words = make([]uint64, ladderInitialWindow/64)
	l.best = -1
}

// ensure returns the index for price, growing the window if the price is outside it.
func (l *ladder) ensure(price int64) int {
	if l.levels == nil {
		l.ensureInit(price)
	}
	idx := int(price - l.origin)
	if idx < 0 || idx >= len(l.levels) {
		l.grow(price)
		idx = int(price - l.origin)
	}
	return idx
}

// grow expands the window so that price is addressable. Growth is always towards lower
// origin or higher indices, never a shift of existing prices, so stored prices stay valid;
// only the bitmap and the cached best index are translated.
func (l *ladder) grow(price int64) {
	oldOrigin := l.origin
	oldLen := len(l.levels)
	idx := int(price - oldOrigin)
	newOrigin := oldOrigin
	newLen := oldLen
	if idx < 0 {
		shift := -idx
		newOrigin = oldOrigin - int64(shift)
		newLen = oldLen + shift
	} else {
		newLen = idx + 1
	}
	// Slack so repeated growth is amortized.
	newLen += newLen/2 + 64

	newLevels := make([]*priceLevel, newLen)
	newWords := make([]uint64, (newLen+63)/64)
	offset := int(oldOrigin - newOrigin)
	copy(newLevels[offset:], l.levels)
	shiftBitsInto(newWords, offset, l.words)
	l.levels = newLevels
	l.words = newWords
	l.origin = newOrigin
	if l.best >= 0 {
		l.best += offset
	}
}

func (l *ladder) setBit(i int)   { l.words[i>>6] |= 1 << uint(i&63) }
func (l *ladder) clearBit(i int) { l.words[i>>6] &^= 1 << uint(i&63) }

// addLevel returns the queue at price, creating it (and marking it occupied) if needed.
func (l *ladder) addLevel(price int64) *priceLevel {
	idx := l.ensure(price)
	lvl := l.levels[idx]
	if lvl == nil {
		lvl = &priceLevel{}
		l.levels[idx] = lvl
		l.setBit(idx)
		if l.best < 0 || (l.isBid && idx > l.best) || (!l.isBid && idx < l.best) {
			l.best = idx
		}
	}
	return lvl
}

// getLevel returns the queue at price, or nil if there is no level there.
func (l *ladder) getLevel(price int64) *priceLevel {
	if l.levels == nil {
		return nil
	}
	idx := int(price - l.origin)
	if idx < 0 || idx >= len(l.levels) {
		return nil
	}
	return l.levels[idx]
}

// removeIfEmpty clears the occupancy bit at price when its queue has drained, advancing
// the cached best index if that level was the best.
func (l *ladder) removeIfEmpty(price int64) {
	if l.levels == nil {
		return
	}
	idx := int(price - l.origin)
	if idx < 0 || idx >= len(l.levels) {
		return
	}
	lvl := l.levels[idx]
	if lvl == nil || lvl.size == 0 {
		l.levels[idx] = nil
		l.clearBit(idx)
		if idx == l.best {
			l.best = l.findNext(idx)
		}
	}
}

// findNext returns the next occupied index after from (lower for bids, higher for asks),
// or -1 if the ladder is empty. from is assumed to have just been cleared.
func (l *ladder) findNext(from int) int {
	if l.isBid {
		return l.highestAtOrBelow(from)
	}
	return l.lowestAtOrAbove(from)
}

func (l *ladder) highestAtOrBelow(i int) int {
	if i < 0 {
		return -1
	}
	if i >= len(l.levels) {
		i = len(l.levels) - 1
	}
	w := i >> 6
	word := l.words[w] & (^uint64(0) >> (63 - uint(i&63)))
	for {
		if word != 0 {
			return w<<6 + bits.Len64(word) - 1
		}
		if w == 0 {
			return -1
		}
		w--
		word = l.words[w]
	}
}

func (l *ladder) lowestAtOrAbove(i int) int {
	if i < 0 {
		i = 0
	}
	if i >= len(l.levels) {
		return -1
	}
	w := i >> 6
	word := l.words[w] & (^uint64(0) << uint(i&63))
	for {
		if word != 0 {
			return w<<6 + bits.TrailingZeros64(word)
		}
		w++
		if w >= len(l.words) {
			return -1
		}
		word = l.words[w]
	}
}

// levelCount returns the number of occupied price levels.
func (l *ladder) levelCount() int {
	n := 0
	for _, w := range l.words {
		n += bits.OnesCount64(w)
	}
	return n
}

// snapshot returns a copy of the occupied levels as price -> resting order count. Used by
// the benchmark and tests for reporting; it is not on the matching hot path.
func (l *ladder) snapshot() map[int64]int {
	m := make(map[int64]int, l.levelCount())
	for i, lvl := range l.levels {
		if lvl != nil && lvl.size > 0 {
			m[l.origin+int64(i)] = lvl.size
		}
	}
	return m
}

// shiftBitsInto copies src into dst with every set bit moved up by bitOffset.
func shiftBitsInto(dst []uint64, bitOffset int, src []uint64) {
	if bitOffset == 0 {
		copy(dst, src)
		return
	}
	wordShift := bitOffset / 64
	bitShift := uint(bitOffset % 64)
	for i, w := range src {
		if w == 0 {
			continue
		}
		j := i + wordShift
		if j >= len(dst) {
			break
		}
		dst[j] |= w << bitShift
		if bitShift != 0 && j+1 < len(dst) {
			dst[j+1] |= w >> (64 - bitShift)
		}
	}
}
