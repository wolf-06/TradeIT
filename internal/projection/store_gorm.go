package projection

import (
	"TradeIT/internal/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GormStore is the Postgres-backed Store. Writes are idempotent: orders upsert by id and
// trades insert with ON CONFLICT DO NOTHING, so replay after a crash is safe.
type GormStore struct {
	db *gorm.DB
}

// NewGormStore wraps a gorm handle.
func NewGormStore(db *gorm.DB) *GormStore { return &GormStore{db: db} }

// UpsertOrders writes orders in one statement, updating existing rows.
func (s *GormStore) UpsertOrders(orders []models.Order) error {
	if len(orders) == 0 {
		return nil
	}
	return s.db.Clauses(clause.OnConflict{UpdateAll: true}).CreateInBatches(orders, len(orders)).Error
}

// InsertTrades writes trades in one statement, ignoring duplicates by trade id.
func (s *GormStore) InsertTrades(trades []models.TradeDetails) error {
	if len(trades) == 0 {
		return nil
	}
	return s.db.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(trades, len(trades)).Error
}

// GetOrder returns an order row, or found=false when it does not exist.
func (s *GormStore) GetOrder(id uint64) (models.Order, bool, error) {
	var o models.Order
	res := s.db.Where("id = ?", id).First(&o)
	if res.Error == gorm.ErrRecordNotFound {
		return models.Order{}, false, nil
	}
	if res.Error != nil {
		return models.Order{}, false, res.Error
	}
	return o, true, nil
}
