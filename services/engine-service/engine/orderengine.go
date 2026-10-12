package engine

// OrderEngine owns the matching ledger for the engine service. The engine consumes the
// shared-memory ring (see RingConsumer); this type is the handle the service wires up.
type OrderEngine struct {
	ledger *Ledger
}

// InitOrderEngine prepares the trade persistence handles and returns an engine bound to a
// fresh ledger.
func InitOrderEngine() *OrderEngine {
	InitTrade()
	return &OrderEngine{ledger: InitLedger()}
}

// Ledger returns the engine's order-book ledger. The ring consumer path uses it as the
// book that commands from the shared-memory ring are applied to.
func (engine *OrderEngine) Ledger() *Ledger { return engine.ledger }
