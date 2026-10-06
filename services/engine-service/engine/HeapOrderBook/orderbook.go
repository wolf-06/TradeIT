//go:build ignore

// Backup of the original heap/map-based order book, kept for reference.
// The ladder-based implementation replaced it. Excluded from normal builds via the
// build tag above; build it explicitly with `go build -tags ignore` if needed.
package engine

import (
	"TradeIT/internal/models"
	"container/heap"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

type Orderbook struct {
	orderTable       map[uint64]*Node
	marketBuyOrders  DoublyLinkedList
	marketSellOrders DoublyLinkedList
	buy_orders       map[float64]*DoublyLinkedList
	sell_orders      map[float64]*DoublyLinkedList
	bids_prices      MaxHeap
	asks_prices      MinHeap
	tradeCount       int64
	buyCount         int64
	sellCount        int64
	lastTradedPrice  float64
	mu               sync.RWMutex
}

// Exported getters and incrementers for testing
func (ob *Orderbook) GetTradeCount() int64                         { return ob.tradeCount }
func (ob *Orderbook) GetOrderTable() map[uint64]*Node              { return ob.orderTable }
func (ob *Orderbook) GetBuyOrders() map[float64]*DoublyLinkedList  { return ob.buy_orders }
func (ob *Orderbook) GetBuyCount() int64                           { return ob.buyCount }
func (ob *Orderbook) GetSellCount() int64                          { return ob.sellCount }
func (ob *Orderbook) IncBuyCount()                                 { ob.buyCount++ }
func (ob *Orderbook) IncSellCount()                                { ob.sellCount++ }
func (ob *Orderbook) GetSellOrders() map[float64]*DoublyLinkedList { return ob.sell_orders }

var tradePool = InitTradePool()

func InitOrderBook() *Orderbook {
	ob := Orderbook{
		buy_orders:       make(map[float64]*DoublyLinkedList),
		sell_orders:      make(map[float64]*DoublyLinkedList),
		orderTable:       make(map[uint64]*Node),
		marketBuyOrders:  DoublyLinkedList{},
		marketSellOrders: DoublyLinkedList{},
	}
	heap.Init(&ob.bids_prices)
	heap.Init(&ob.asks_prices)
	return &ob
}

func (ob *Orderbook) Lock()                { ob.mu.Lock() }   //used while testing
func (ob *Orderbook) Unlock()              { ob.mu.Unlock() } //used while testing
func (ob *Orderbook) SetLTP(price float64) { ob.lastTradedPrice = price }

func (ob *Orderbook) InsertOrder(orderData models.Order) error {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	return ob.internalInsertOrder(orderData)
}

func (ob *Orderbook) internalInsertOrder(orderData models.Order) error {

	if orderData.Order_Type == "market" {
		if orderData.Side == "buy" {
			ob.orderTable[orderData.Id] = ob.marketBuyOrders.PushBack(orderData)
			ob.buyCount++
		} else if orderData.Side == "sell" {
			ob.orderTable[orderData.Id] = ob.marketSellOrders.PushBack(orderData)
			ob.sellCount++
		} else {
			return errors.New("ORDER_INVALID_SIDE")
		}
	} else if orderData.Order_Type == "limit" {
		if orderData.Side == "buy" {
			if _, exists := ob.buy_orders[orderData.Price]; !exists {
				ob.buy_orders[orderData.Price] = &DoublyLinkedList{}
				heap.Push(&ob.bids_prices, orderData.Price) // inserts the price in Heap
			}
			ob.orderTable[orderData.Id] = ob.buy_orders[orderData.Price].PushBack(orderData) // insert order details in buy order list and also stores the nodes reference in the orderTable
			ob.buyCount++
		} else if orderData.Side == "sell" {
			if _, exists := ob.sell_orders[orderData.Price]; !exists {
				ob.sell_orders[orderData.Price] = &DoublyLinkedList{}
				heap.Push(&ob.asks_prices, orderData.Price) // inserts the price in Heap
			}
			ob.orderTable[orderData.Id] = ob.sell_orders[orderData.Price].PushBack(orderData) //insert order details in buy order list
			ob.sellCount++
		} else {
			return errors.New("ORDER_INVALID_SIDE")
		}
	} else {
		return errors.New("ORDER_INVALID_TYPE")
	}
	return nil
}

// limit orders matcher
func (ob *Orderbook) Matcher_limit(order models.Order) {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	if order.Side == "buy" {
		for ob.asks_prices.Len() > 0 && order.Remq > 0 {
			bestAsk := ob.asks_prices.Peek()
			if order.Order_Type == "market" {
			}
			if order.Price < bestAsk { //buyers price is
				break
			}

			askList, exists := ob.sell_orders[bestAsk]
			if !exists {
				heap.Pop(&ob.asks_prices)
			} else {
				for node := askList.Head; node != nil && order.Remq > 0; node = node.Next {
					matchQuantity := min(node.Order_.Remq, order.Remq)
					order.Remq -= matchQuantity
					node.Order_.Remq -= matchQuantity
					executionPrice := math.Round(((order.Price+node.Order_.Price)/2.0)*100) / 100
					node.Order_.AvgPrice = float64(matchQuantity) * executionPrice
					order.AvgPrice = float64(matchQuantity) * executionPrice
					ob.tradeCount++
					if node.Order_.Remq == 0 {
						// remove the node's reference from the orderTable
						delete(ob.orderTable, node.Order_.Id)
						// remove the node from the linked list
						if node.Prev != nil {
							node.Prev.Next = node.Next
						} else {
							askList.Head = node.Next
						}

						if node.Next != nil {
							node.Next.Prev = node.Prev
						} else {
							askList.Tail = node.Prev
						}
						node.Order_.AvgPrice = node.Order_.AvgPrice / float64(node.Order_.Quantity)
						askList.Size--
						ob.sellCount--
						//send the message to the user that order has been filled
					}
					if order.Remq == 0 {
						order.AvgPrice = order.AvgPrice / float64(order.Quantity) //finalises the avg price
						delete(ob.orderTable, order.Id)                           //removes the order from the orderTable
						//send the message to the user that the order has been filled
					}

				}
				if askList.Size == 0 {
					delete(ob.sell_orders, bestAsk)
					heap.Pop(&ob.asks_prices)
				}
			}
		}
		if order.Remq > 0 { //adds the order to the orderbook if not filled
			ob.internalInsertOrder(order)
		}
	} else {
		// Proccessing the sell order

		for ob.bids_prices.Len() > 0 && order.Remq > 0 { //checking for the bids available
			bestBid := ob.bids_prices.Peek()
			if order.Price > bestBid { //breaks if bids are less than the asks
				break
			}
			bidList, exists := ob.buy_orders[bestBid]
			if !exists {
				heap.Pop(&ob.bids_prices)
			} else {
				for node := bidList.Head; node != nil && order.Remq > 0; node = node.Next {
					//trade occurs
					matchQuantity := min(order.Remq, node.Order_.Remq)
					order.Remq -= matchQuantity
					node.Order_.Remq -= matchQuantity
					executionPrice := math.Round(((order.Price+node.Order_.Price)/2.0)*100) / 100
					node.Order_.AvgPrice += float64(matchQuantity) * executionPrice
					order.AvgPrice += float64(matchQuantity) * executionPrice
					ob.tradeCount++
					//register the trade here
					//
					if node.Order_.Remq == 0 { //buyer order is fullfilled and node has become stale
						if node.Order_.Remq == 0 {
							// remove the node's reference from the orderTable
							delete(ob.orderTable, node.Order_.Id)
							// remove the node from the linked list
							if node.Prev != nil {
								node.Prev.Next = node.Next
							} else {
								bidList.Head = node.Next
							}

							if node.Next != nil {
								node.Next.Prev = node.Prev
							} else {
								bidList.Tail = node.Prev
							}
							node.Order_.AvgPrice = node.Order_.AvgPrice / float64(node.Order_.Quantity)
							bidList.Size--
							ob.buyCount--
							//send the message to the user that the order has been filled
						}
					}

					if order.Remq == 0 {
						order.AvgPrice = order.AvgPrice / (float64(order.Quantity))
						delete(ob.orderTable, order.Id) //removes the order from the orderTable
						//send the message to the user that the order has been filled
					}
				}
				if bidList.Size == 0 {
					delete(ob.buy_orders, bestBid)
					heap.Pop(&ob.bids_prices)
				}
			}
		}
		if order.Remq > 0 {
			ob.internalInsertOrder(order)
		}

	}

	if ob.asks_prices.Len() == 0 && ob.bids_prices.Len() == 0 {
		//close the matcher and orderbook
		// fmt.Println("Time to close the orderbook and matcher")
	}

}

// Market and Limit order matchers
func (ob *Orderbook) Matcher(order models.Order) error {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	var mainFlag bool
	var counterMarketOrders *DoublyLinkedList
	var counterLimitOrders map[float64]*DoublyLinkedList
	var counterOrderCount *int64
	var counterMinHeap *MinHeap
	var counterMaxHeap *MaxHeap
	var bestCounterPrice float64
	var orderList *DoublyLinkedList
	var node *Node
	orderType := order.Order_Type
	mainFlag = true

	if order.Side == "buy" {
		counterMarketOrders = &ob.marketSellOrders
		counterLimitOrders = ob.sell_orders
		counterMinHeap = &ob.asks_prices
		counterOrderCount = &ob.sellCount
	} else {
		counterMarketOrders = &ob.marketBuyOrders
		counterLimitOrders = ob.buy_orders
		counterMaxHeap = &ob.bids_prices
		counterOrderCount = &ob.buyCount
	}

	for mainFlag {
		// fmt.Println(order.Id, "reached the matcher is inside the main loop")
		var flag bool
		var orderListType string
		flag = false
		// fmt.Println(order)
		if counterMarketOrders.Size != 0 {
			orderList = counterMarketOrders
			orderListType = "market"
			flag = true
			// fmt.Println(flag)
		} else if len(counterLimitOrders) != 0 {
			// fmt.Println("inside the limit section")
			orderListType = "limit"
			if order.Side == "buy" {

				bestCounterPrice = counterMinHeap.Peek()
				if orderType == "limit" && bestCounterPrice > order.Price {
					// fmt.Println("Breaking")
					break
				}
				orderList, flag = counterLimitOrders[bestCounterPrice]
				if !flag {
					heap.Pop(counterMinHeap)
				}

			} else if order.Side == "sell" {

				bestCounterPrice = counterMaxHeap.Peek()
				if orderType == "limit" && bestCounterPrice < order.Price {
					// fmt.Println("Breaking")
					break
				}
				orderList, flag = counterLimitOrders[bestCounterPrice]
				if !flag {
					heap.Pop(counterMaxHeap)
				}

			}
		} else if counterMarketOrders.Size == 0 && len(counterLimitOrders) == 0 {
			break
		}
		if !flag {
			continue
		} else {
			for node = orderList.Head; node != nil && order.Remq > 0; node = node.Next {

				// fmt.Println("Matching Order in matcher")
				matchQuantity := min(order.Remq, node.Order_.Remq)
				order.Remq -= matchQuantity
				node.Order_.Remq -= matchQuantity
				var tradedPrice float64

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
						tradedPrice = math.Round(((order.Price+node.Order_.Price)/2.0)*100) / 100
					}
				}

				order.AvgPrice += tradedPrice * float64(matchQuantity)
				node.Order_.AvgPrice += tradedPrice * float64(matchQuantity)
				ob.lastTradedPrice = tradedPrice
				ob.tradeCount++

				//registering trade
				trade := tradePool.acquireTrade()
				trade.Stock, trade.Price, trade.Quantity = order.Stock, tradedPrice, matchQuantity
				trade.Executed_at = time.Now()
				if order.Side == "buy" {
					trade.Buyer, trade.AskOrderID = order.User_id, order.Id
					trade.Seller, trade.BidOrderID = node.Order_.User_id, node.Order_.Id
				} else {
					trade.Seller, trade.BidOrderID = order.User_id, order.Id
					trade.Buyer, trade.AskOrderID = node.Order_.User_id, node.Order_.Id
				}
				// fmt.Println("Sent to trade register")
				// fmt.Println("Trade:\n", trade)
				//registers the trade and releases the trade to objectPool back
				go func(trade *models.TradeDetails) {
					registerTrades(trade)
					tradePool.releaseTrade(trade)
				}(trade)

				//clearing the node and order if completed
				if node.Order_.Remq == 0 {
					// remove the node's reference from the orderTable
					delete(ob.orderTable, node.Order_.Id)
					// remove the node from the linked list
					if node.Prev != nil {
						node.Prev.Next = node.Next
					} else {
						orderList.Head = node.Next
					}

					if node.Next != nil {
						node.Next.Prev = node.Prev
					} else {
						orderList.Tail = node.Prev
					}
					node.Order_.AvgPrice = node.Order_.AvgPrice / float64(node.Order_.Quantity)
					orderList.Size--
					*counterOrderCount--
					//send the message to the user that the oder has been fulfilled
					go updateOrderStatusExecuted(node.Order_)
				}
				if order.Remq == 0 {
					order.AvgPrice = order.AvgPrice / (float64(order.Quantity))
					delete(ob.orderTable, order.Id)
					go updateOrderStatusExecuted(order)
					mainFlag = false
				}
			}
			if orderList.Size == 0 && orderListType == "limit" {
				delete(counterLimitOrders, bestCounterPrice)
				if order.Side == "buy" {
					heap.Pop(&ob.asks_prices)
				} else {
					heap.Pop(&ob.bids_prices)
				}
			}
		}
	}
	if order.Remq > 0 {
		// fmt.Println("inserting order")
		err := ob.internalInsertOrder(order)
		if err != nil {
			// fmt.Println(err)
			return err
		}
	}
	return nil
}

func (ob *Orderbook) DisplayResult() {
	fmt.Printf("Trades: %d |\nBuyOrders: %d|\nSellOrders: %d|\nTotal: %d", ob.tradeCount, ob.buyCount, ob.sellCount, (ob.tradeCount + ob.sellCount + ob.buyCount))
	fmt.Println("\nHeap lengths: ")
	fmt.Printf("bids: %d\nasks: %d", ob.bids_prices.Len(), ob.asks_prices.Len())
	fmt.Printf("\nMarket order \n buy: %d\nsell: %d", ob.marketBuyOrders.Size, ob.marketSellOrders.Size)
}

func (ob *Orderbook) CancelOrder(orderId uint64) error {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	node, exists := ob.orderTable[orderId]
	if !exists {
		// fmt.Println("Order not in Orderbook")
		return errors.New("ORDER_NOT_IN_MATCHER")
	}
	difference := node.Order_.Quantity - node.Order_.Remq
	switch node.Order_.Side {
	case "buy":
		sig := ob.buy_orders[node.Order_.Price].RemoveNode(node)
		if sig != nil {
			delete(ob.buy_orders, node.Order_.Price) // deletes the entry of linkedlist in the map
		}
		if difference != 0 {
			node.Order_.Remq = 0
			node.Order_.Quantity = difference
			node.Order_.AvgPrice = node.Order_.AvgPrice / float64(node.Order_.Quantity)
			//send the user the message of the partial filled quantity order is success
			ob.buyCount--
			delete(ob.orderTable, orderId)
			updateOrderStatusUpdated(node.Order_)
			return errors.New("ORDER_PARTIAL_CANCELLED")
		}
		ob.buyCount--
	case "sell":
		sig := ob.sell_orders[node.Order_.Price].RemoveNode(node)
		if sig != nil {
			delete(ob.sell_orders, node.Order_.Price)
		}
		if difference != 0 {
			node.Order_.Remq = 0
			node.Order_.Quantity = difference
			node.Order_.AvgPrice = node.Order_.AvgPrice / float64(node.Order_.Quantity)
			//send the user the message of the partial filled quantity order is success
			ob.sellCount--
			delete(ob.orderTable, orderId)
			updateOrderStatusUpdated(node.Order_)
			return errors.New("ORDER_PARTIAL_CANCELLED")
		}
		ob.sellCount--
	default:
		return errors.New("ORDER_UNSUPPORTED_TYPE")
	}
	delete(ob.orderTable, orderId) //deletes the node and order entry from the ordertable
	updateOrderStatusCancelled(node.Order_)
	return nil
}

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

	if difference < 0 { //Partially filled more than than newQuantity
		return errors.New("ORDER_PARTIAL_DECREASE_DENIED")
	} else if difference == 0 { //order has just completed and rest of the order is not to be filled
		if order_node.Order_.Side == "buy" {

		} else if order_node.Order_.Side == "sell" {
			sig := ob.sell_orders[order_node.Order_.Price].RemoveNode(order_node)
			if sig != nil {
				delete(ob.sell_orders, order_node.Order_.Price)
			}
			ob.sellCount--
		}
		order_node.Order_.Quantity = newQuantity
	} else {
		order_node.Order_.Quantity = newQuantity
		order_node.Order_.Remq = difference
	}
	return nil
}

func (ob *Orderbook) ModifyPrice(orderId uint64, newPrice float64) error {
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
	if order_node.Order_.Side == "buy" {
		sig := ob.buy_orders[order_node.Order_.Price].RemoveNode(order_node)
		if sig != nil {
			delete(ob.buy_orders, order_node.Order_.Price) // deletes the entry of linkedlist in the map
		}
		ob.buyCount--

	} else if order_node.Order_.Side == "sell" {
		sig := ob.sell_orders[order_node.Order_.Price].RemoveNode(order_node)
		if sig != nil {
			delete(ob.sell_orders, order_node.Order_.Price)
		}
		ob.sellCount--
	} else {
		return errors.New("ORDER_INVALID_SIDE")
	}
	delete(ob.orderTable, orderId)
	order_node.Order_.Price = newPrice
	ob.Unlock()
	ob.Matcher(order_node.Order_)
	ob.Lock()
	return nil
}
