package engine

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"TradeIT/internal/models"
)

// Orderbook is an array-based (ladder) limit order book with integer prices.
//
// Prices are integers in the scaled unit defined by models.PriceScale (for example
// 185.40 -> 18540). The user-order service performs that conversion, so the matcher only
// ever sees integers. Each side is a ladder: an array of price levels indexed by
// (price - origin) plus an occupancy bitmap, so price lookup is an array index and best
// price is a short bitmap scan. Resting orders at a level form a FIFO doubly linked list.
type Orderbook struct {
	bids *ladder
	asks *ladder

	// orderTable maps an order id to its resting node so cancel/modify are O(1).
	orderTable map[uint64]*Node

	marketBuyOrders  priceLevel
	marketSellOrders priceLevel

	tradeCount      int64
	buyCount        int64
	sellCount       int64
	lastTradedPrice int64

	mu sync.RWMutex
}

func InitOrderBook() *Orderbook {
	return &Orderbook{
		bids:       &ladder{isBid: true, best: -1},
		asks:       &ladder{isBid: false, best: -1},
		orderTable: make(map[uint64]*Node),
	}
}

// Lock/Unlock expose the book mutex for tests.
func (ob *Orderbook) Lock()   { ob.mu.Lock() }
func (ob *Orderbook) Unlock() { ob.mu.Unlock() }

// SetLTP sets the last traded price used to price market-vs-market fills.
func (ob *Orderbook) SetLTP(price int64) { ob.lastTradedPrice = price }

func (ob *Orderbook) GetTradeCount() int64            { return ob.tradeCount }
func (ob *Orderbook) GetBuyCount() int64              { return ob.buyCount }
func (ob *Orderbook) GetSellCount() int64             { return ob.sellCount }
func (ob *Orderbook) IncBuyCount()                    { ob.buyCount++ }
func (ob *Orderbook) IncSellCount()                   { ob.sellCount++ }
func (ob *Orderbook) GetOrderTable() map[uint64]*Node { return ob.orderTable }

// GetBuyOrders and GetSellOrders return the occupied price levels (price -> resting
// order count). Callers use them for reporting; they are not on the matching path.
func (ob *Orderbook) GetBuyOrders() map[int64]int  { return ob.bids.snapshot() }
func (ob *Orderbook) GetSellOrders() map[int64]int { return ob.asks.snapshot() }

// InsertOrder rests an order without attempting to match it.
func (ob *Orderbook) InsertOrder(orderData models.Order) error {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	return ob.insertOrder(orderData)
}

func (ob *Orderbook) insertOrder(orderData models.Order) error {
	switch orderData.Order_Type {
	case "market":
		switch orderData.Side {
		case "buy":
			ob.orderTable[orderData.Id] = ob.marketBuyOrders.pushBack(orderData)
			ob.buyCount++
		case "sell":
			ob.orderTable[orderData.Id] = ob.marketSellOrders.pushBack(orderData)
			ob.sellCount++
		default:
			return errors.New("ORDER_INVALID_SIDE")
		}
	case "limit":
		switch orderData.Side {
		case "buy":
			lvl := ob.bids.addLevel(orderData.Price)
			ob.orderTable[orderData.Id] = lvl.pushBack(orderData)
			ob.buyCount++
		case "sell":
			lvl := ob.asks.addLevel(orderData.Price)
			ob.orderTable[orderData.Id] = lvl.pushBack(orderData)
			ob.sellCount++
		default:
			return errors.New("ORDER_INVALID_SIDE")
		}
	default:
		return errors.New("ORDER_INVALID_TYPE")
	}
	return nil
}

// Matcher matches an incoming order against the resting book, then rests any remainder.
func (ob *Orderbook) Matcher(order models.Order) error {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	return ob.matchLocked(order)
}

// matchLocked runs the matching algorithm. The caller must hold ob.mu.
func (ob *Orderbook) matchLocked(order models.Order) error {
	isBuy := order.Side == "buy"
	if !isBuy && order.Side != "sell" {
		return errors.New("ORDER_INVALID_SIDE")
	}
	orderType := order.Order_Type
	if orderType != "limit" && orderType != "market" {
		return errors.New("ORDER_INVALID_TYPE")
	}

	var counterMarket *priceLevel
	var counterLimit *ladder
	var counterCount *int64
	if isBuy {
		counterMarket = &ob.marketSellOrders
		counterLimit = ob.asks
		counterCount = &ob.sellCount
	} else {
		counterMarket = &ob.marketBuyOrders
		counterLimit = ob.bids
		counterCount = &ob.buyCount
	}

	for order.Remq > 0 {
		var node *Node

		if counterMarket.size > 0 {
			node = counterMarket.head
		} else if counterLimit.best >= 0 {
			bestPrice := counterLimit.origin + int64(counterLimit.best)
			if orderType == "limit" {
				if isBuy && bestPrice > order.Price {
					break
				}
				if !isBuy && bestPrice < order.Price {
					break
				}
			}
			lvl := counterLimit.levels[counterLimit.best]
			if lvl == nil || lvl.size == 0 {
				counterLimit.removeIfEmpty(bestPrice)
				continue
			}
			node = lvl.head
		} else {
			break
		}

		matchQuantity := min(order.Remq, node.Order_.Remq)
		order.Remq -= matchQuantity
		node.Order_.Remq -= matchQuantity

		var tradedPrice int64
		if orderType == "market" {
			if node.Order_.Order_Type == "limit" {
				tradedPrice = node.Order_.Price
			} else {
				tradedPrice = ob.lastTradedPrice
			}
		} else {
			if node.Order_.Order_Type == "market" {
				tradedPrice = order.Price
			} else {
				tradedPrice = midpoint(order.Price, node.Order_.Price)
			}
		}

		order.AvgPrice += tradedPrice * int64(matchQuantity)
		node.Order_.AvgPrice += tradedPrice * int64(matchQuantity)
		ob.lastTradedPrice = tradedPrice
		ob.tradeCount++

		trade := tradePool.acquireTrade()
		trade.Stock, trade.Price, trade.Quantity = order.Stock, tradedPrice, matchQuantity
		trade.Executed_at = time.Now()
		if isBuy {
			trade.Buyer, trade.AskOrderID = order.User_id, order.Id
			trade.Seller, trade.BidOrderID = node.Order_.User_id, node.Order_.Id
		} else {
			trade.Seller, trade.BidOrderID = order.User_id, order.Id
			trade.Buyer, trade.AskOrderID = node.Order_.User_id, node.Order_.Id
		}
		go func(trade *models.TradeDetails) {
			registerTrades(trade)
			tradePool.releaseTrade(trade)
		}(trade)

		if node.Order_.Remq == 0 {
			if node.Order_.Order_Type == "market" {
				counterMarket.unlink(node)
			} else if lvl := counterLimit.getLevel(node.Order_.Price); lvl != nil {
				lvl.unlink(node)
				counterLimit.removeIfEmpty(node.Order_.Price)
			}
			delete(ob.orderTable, node.Order_.Id)
			node.Order_.AvgPrice = node.Order_.AvgPrice / int64(node.Order_.Quantity)
			*counterCount--
			go updateOrderStatusExecuted(node.Order_)
		}

		if order.Remq == 0 {
			order.AvgPrice = order.AvgPrice / int64(order.Quantity)
			delete(ob.orderTable, order.Id)
			go updateOrderStatusExecuted(order)
		}
	}

	if order.Remq > 0 {
		if err := ob.insertOrder(order); err != nil {
			return err
		}
	}
	return nil
}

// removeNode unlinks a node from whichever queue currently holds it.
func (ob *Orderbook) removeNode(n *Node) {
	if n.Order_.Order_Type == "market" {
		if n.Order_.Side == "buy" {
			ob.marketBuyOrders.unlink(n)
		} else {
			ob.marketSellOrders.unlink(n)
		}
		return
	}
	if n.Order_.Side == "buy" {
		if lvl := ob.bids.getLevel(n.Order_.Price); lvl != nil {
			lvl.unlink(n)
			ob.bids.removeIfEmpty(n.Order_.Price)
		}
		return
	}
	if lvl := ob.asks.getLevel(n.Order_.Price); lvl != nil {
		lvl.unlink(n)
		ob.asks.removeIfEmpty(n.Order_.Price)
	}
}

// CancelOrder cancels a resting order. A partially filled order is removed and reported
// as ORDER_PARTIAL_CANCELLED with its filled quantity kept on the order.
func (ob *Orderbook) CancelOrder(orderId uint64) error {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	node, exists := ob.orderTable[orderId]
	if !exists {
		return errors.New("ORDER_NOT_IN_MATCHER")
	}
	difference := node.Order_.Quantity - node.Order_.Remq

	switch node.Order_.Side {
	case "buy":
		ob.removeNode(node)
		if difference != 0 {
			node.Order_.Remq = 0
			node.Order_.Quantity = difference
			node.Order_.AvgPrice = node.Order_.AvgPrice / int64(node.Order_.Quantity)
			ob.buyCount--
			delete(ob.orderTable, orderId)
			updateOrderStatusUpdated(node.Order_)
			return errors.New("ORDER_PARTIAL_CANCELLED")
		}
		ob.buyCount--
	case "sell":
		ob.removeNode(node)
		if difference != 0 {
			node.Order_.Remq = 0
			node.Order_.Quantity = difference
			node.Order_.AvgPrice = node.Order_.AvgPrice / int64(node.Order_.Quantity)
			ob.sellCount--
			delete(ob.orderTable, orderId)
			updateOrderStatusUpdated(node.Order_)
			return errors.New("ORDER_PARTIAL_CANCELLED")
		}
		ob.sellCount--
	default:
		return errors.New("ORDER_UNSUPPORTED_TYPE")
	}
	delete(ob.orderTable, orderId)
	updateOrderStatusCancelled(node.Order_)
	return nil
}

// ModifyQuantity changes the total quantity of a resting order. The increase becomes the
// new remaining quantity on top of what has already been filled.
func (ob *Orderbook) ModifyQuantity(orderId uint64, newQuantity int) error {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	order_node, exists := ob.orderTable[orderId]
	if !exists {
		return errors.New("ORDER_NOT_FOUND_OR_PROCESSED")
	}
	if newQuantity <= 0 {
		return errors.New("ORDER_INVALID_QUANTITY")
	}
	difference := newQuantity - (order_node.Order_.Quantity - order_node.Order_.Remq)

	if difference < 0 { // new quantity is below the already filled amount
		return errors.New("ORDER_PARTIAL_DECREASE_DENIED")
	} else if difference == 0 {
		if order_node.Order_.Side == "buy" {
			// already fully accounted for on the buy side; nothing to unlink
		} else if order_node.Order_.Side == "sell" {
			ob.removeNode(order_node)
			ob.sellCount--
		}
		order_node.Order_.Quantity = newQuantity
	} else {
		order_node.Order_.Quantity = newQuantity
		order_node.Order_.Remq = difference
	}
	return nil
}

// ModifyPrice re-prices a resting order and re-matches it at the new price.
func (ob *Orderbook) ModifyPrice(orderId uint64, newPrice int64) error {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	order_node, exists := ob.orderTable[orderId]
	if !exists {
		return errors.New("ORDER_ALREADY_PROCESSED")
	}
	if newPrice <= 0 {
		return errors.New("ORDER_INVALID_PRICE")
	}
	if newPrice == order_node.Order_.Price {
		return nil
	}
	ob.removeNode(order_node)
	if order_node.Order_.Side == "buy" {
		ob.buyCount--
	} else {
		ob.sellCount--
	}
	delete(ob.orderTable, orderId)
	order_node.Order_.Price = newPrice
	return ob.matchLocked(order_node.Order_)
}

func (ob *Orderbook) DisplayResult() {
	fmt.Printf("Trades: %d |\nBuyOrders: %d|\nSellOrders: %d|\nTotal: %d", ob.tradeCount, ob.buyCount, ob.sellCount, (ob.tradeCount + ob.sellCount + ob.buyCount))
	fmt.Printf("\nPrice levels: \n bids: %d\nasks: %d", ob.bids.levelCount(), ob.asks.levelCount())
	fmt.Printf("\nMarket order \n buy: %d\nsell: %d", ob.marketBuyOrders.size, ob.marketSellOrders.size)
}

// midpoint returns the rounded midpoint of two integer prices (half away from zero).
func midpoint(a, b int64) int64 {
	sum := a + b
	if sum >= 0 {
		return (sum + 1) / 2
	}
	return (sum - 1) / 2
}
