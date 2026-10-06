package engine

import (
	"TradeIT/internal/models"
	"sync"
)

type TradePool struct {
	pool sync.Pool
}

// tradePool recycles models.TradeDetails between fills. It is package-level because both
// the matcher (which acquires) and the async trade registrar (which releases) use it.
var tradePool = InitTradePool()

func InitTradePool() *TradePool {
	var tp TradePool
	tp.pool = sync.Pool{
		New: func() interface{} {
			return new(models.TradeDetails)
		},
	}
	return &tp
}

func (tp *TradePool) acquireTrade() *models.TradeDetails {
	return tp.pool.Get().(*models.TradeDetails)
}

func (tp *TradePool) releaseTrade(td *models.TradeDetails) {
	*td = models.TradeDetails{}
	tp.pool.Put(td)
}
