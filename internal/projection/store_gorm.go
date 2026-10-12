package projection

import (
	"time"

	"TradeIT/internal/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GormStore is the Postgres-backed Store. A batch's orders, trades and checkpoint are
// written in one transaction, so projection is exactly-once: a crash can never leave the
// checkpoint ahead of the data. Writes are idempotent anyway (orders upsert by id, trades
// insert with ON CONFLICT DO NOTHING), so an accidental replay is also safe.
type GormStore struct {
	db *gorm.DB
}

// NewGormStore wraps a gorm handle.
func NewGormStore(db *gorm.DB) *GormStore { return &GormStore{db: db} }

// LoadCheckpoints returns the last committed position per symbol.
func (s *GormStore) LoadCheckpoints() (map[uint32]Checkpoint, error) {
	var rows []models.ProjectionCheckpoint
	if err := s.db.Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make(map[uint32]Checkpoint, len(rows))
	for _, r := range rows {
		out[r.SymbolID] = Checkpoint{SymbolID: r.SymbolID, Seq: r.Seq, Offset: r.Offset}
	}
	return out, nil
}

// CommitBatch writes the orders, trades and checkpoint for one symbol atomically.
func (s *GormStore) CommitBatch(orders []models.Order, trades []models.TradeDetails, cp Checkpoint) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		if len(orders) > 0 {
			if err := tx.Clauses(clause.OnConflict{UpdateAll: true}).CreateInBatches(orders, len(orders)).Error; err != nil {
				return err
			}
		}
		if len(trades) > 0 {
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(trades, len(trades)).Error; err != nil {
				return err
			}
		}
		row := models.ProjectionCheckpoint{
			SymbolID:  cp.SymbolID,
			Seq:       cp.Seq,
			Offset:    cp.Offset,
			UpdatedAt: time.Now(),
		}
		return tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&row).Error
	})
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
