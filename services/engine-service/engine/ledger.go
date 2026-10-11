package engine

import (
	"TradeIT/internal/models"
	"errors"
	"log"
)

type Ledger struct {
	ent      map[string]*Orderbook
	ob_count int64
}

func InitLedger() *Ledger {
	return &Ledger{
		ent: make(map[string]*Orderbook),
	}
}

func (ld *Ledger) ProcessOrder(order models.Order) error {
	ob, exists := ld.ent[order.Stock]
	if !exists {
		ob = InitOrderBook()
		ld.ent[order.Stock] = ob
		ld.ob_count++
	}
	err := ob.Matcher(order)
	if err != nil {
		log.Println("error occured in Matcher ", err)
	}
	return err
}

func (ld *Ledger) CancelOrder(stock string, order_id uint64) error {
	ob, exists := ld.ent[stock]
	if !exists {
		return errors.New("INVALID STOCK")
	}
	return ob.CancelOrder(order_id)
}

func (ld *Ledger) ModifyPrice(stock string, order_id uint64, newPrice int64) error {
	ob, exists := ld.ent[stock]
	if !exists {
		return errors.New("INVALID STOCK")
	}
	return ob.ModifyPrice(order_id, newPrice)
}

// ModifyQuantity changes the total quantity of a resting order in place (keeping its
// time priority).
func (ld *Ledger) ModifyQuantity(stock string, order_id uint64, newQuantity int) error {
	ob, exists := ld.ent[stock]
	if !exists {
		return errors.New("INVALID STOCK")
	}
	return ob.ModifyQuantity(order_id, newQuantity)
}

// Modify re-prices and re-quantifies a resting order and re-matches it.
func (ld *Ledger) Modify(stock string, order_id uint64, newPrice int64, newQuantity int) error {
	ob, exists := ld.ent[stock]
	if !exists {
		return errors.New("INVALID STOCK")
	}
	return ob.Modify(order_id, newPrice, newQuantity)
}
