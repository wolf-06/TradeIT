package engine_test

import (
	"testing"

	"TradeIT/internal/database"
	"TradeIT/internal/models"
	"TradeIT/internal/projection"
)

// TestProjectionCheckpointIdentity guards a subtle GORM trap. ProjectionCheckpoint's
// primary key is an integer symbol id and symbol id 0 is a valid key. Without
// autoIncrement:false, GORM treats the zero value as "unset", lets the sequence assign it,
// and inserts a brand-new row on every flush instead of updating symbol 0 - so the
// checkpoint never advances and LoadCheckpoints cannot find symbol 0 at all.
func TestProjectionCheckpointIdentity(t *testing.T) {
	db := database.SetDB()
	if db == nil {
		t.Fatal("no database handle")
	}
	models.InitTables()
	if err := db.Exec("DELETE FROM projection_checkpoints").Error; err != nil {
		t.Fatalf("clean checkpoints: %v", err)
	}

	store := projection.NewGormStore(db)
	if err := store.CommitBatch(nil, nil, projection.Checkpoint{SymbolID: 0, Seq: 10, Offset: 100}); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	if err := store.CommitBatch(nil, nil, projection.Checkpoint{SymbolID: 0, Seq: 20, Offset: 200}); err != nil {
		t.Fatalf("second commit: %v", err)
	}

	cps, err := store.LoadCheckpoints()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	cp, ok := cps[0]
	if !ok {
		t.Fatalf("no checkpoint for symbol 0: %+v", cps)
	}
	if cp.Seq != 20 || cp.Offset != 200 {
		t.Fatalf("checkpoint = %+v, want seq 20 offset 200", cp)
	}
	if len(cps) != 1 {
		t.Fatalf("got %d checkpoints, want 1 (symbol 0 must update in place): %+v", len(cps), cps)
	}
}
