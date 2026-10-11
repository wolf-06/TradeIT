package engine

import (
	"time"

	"TradeIT/internal/ipc"
	"TradeIT/internal/models"
	"TradeIT/internal/symbol"
)

// Error codes carried in ipc.Outcome.Err. They are stable wire values, not error
// strings.
const (
	ErrNone             uint16 = 0
	ErrUnknownSymbol    uint16 = 100
	ErrUnknownOp        uint16 = 101
	ErrOrderNotInBook   uint16 = 102
	ErrOrderProcessed   uint16 = 103
	ErrPartialDecrease  uint16 = 104
	ErrInvalidPrice     uint16 = 105
	ErrInvalidQuantity  uint16 = 106
	ErrInvalidSide      uint16 = 107
	ErrInvalidType      uint16 = 108
	ErrInvalidStock     uint16 = 109
	ErrOther            uint16 = 255
)

// RingHandler applies commands from the shared-memory ring to the ledger. It implements
// ingest.Handler and ingest.Restorer. Commands are already ordered per symbol by the
// ring, so no additional ordering is applied here.
type RingHandler struct {
	ledger *Ledger
	table  *symbol.Table
}

// NewRingHandler builds a handler that routes commands to ledger using the shared
// symbol table to resolve symbol ids.
func NewRingHandler(ledger *Ledger, table *symbol.Table) *RingHandler {
	return &RingHandler{ledger: ledger, table: table}
}

// Apply applies one command and reports its outcome.
func (h *RingHandler) Apply(c ipc.Cmd) ipc.Outcome {
	return h.apply(c)
}

// Restore re-applies a recorded command during recovery. The book is a deterministic
// function of the ordered command stream, so the same apply path reproduces it.
func (h *RingHandler) Restore(c ipc.Cmd, _ ipc.Outcome) {
	h.apply(c)
}

func (h *RingHandler) apply(c ipc.Cmd) ipc.Outcome {
	name, ok := h.table.Name(c.SymbolID)
	if !ok {
		return ipc.Outcome{Status: ipc.StatusRejected, Err: ErrUnknownSymbol}
	}
	switch c.Op {
	case ipc.OpNew:
		return outcomeFromError(h.ledger.ProcessOrder(h.toOrder(c, name)))
	case ipc.OpCancel:
		return outcomeFromError(h.ledger.CancelOrder(name, c.OrderID))
	case ipc.OpReduceQty:
		return outcomeFromError(h.ledger.ModifyQuantity(name, c.OrderID, int(c.Qty)))
	case ipc.OpModify:
		return outcomeFromError(h.ledger.Modify(name, c.OrderID, c.Price, int(c.Qty)))
	default:
		return ipc.Outcome{Status: ipc.StatusRejected, Err: ErrUnknownOp}
	}
}

// toOrder converts a command into the models.Order the matcher consumes. Prices are
// already scaled integers; the symbol name comes from the shared table.
func (h *RingHandler) toOrder(c ipc.Cmd, name string) models.Order {
	return models.Order{
		MetaOrder: models.MetaOrder{
			Id:         c.OrderID,
			User_id:    int(c.UserID),
			Order_Type: c.Typ.String(),
			Side:       c.Side.String(),
			Stock:      name,
			Price:      c.Price,
			Quantity:   int(c.Qty),
			Status:     "pending",
			Created_at: time.Now(),
		},
		Remq: int(c.Remq),
	}
}

func outcomeFromError(err error) ipc.Outcome {
	if err == nil {
		return ipc.Outcome{Status: ipc.StatusAccepted}
	}
	// A partial cancel is reported as an error by CancelOrder but is a successful
	// state transition, not a rejection.
	if err.Error() == "ORDER_PARTIAL_CANCELLED" {
		return ipc.Outcome{Status: ipc.StatusPartiallyCancelled}
	}
	return ipc.Outcome{Status: ipc.StatusRejected, Err: errCode(err)}
}

func errCode(err error) uint16 {
	switch err.Error() {
	case "ORDER_NOT_IN_MATCHER":
		return ErrOrderNotInBook
	case "ORDER_ALREADY_PROCESSED":
		return ErrOrderProcessed
	case "ORDER_PARTIAL_DECREASE_DENIED":
		return ErrPartialDecrease
	case "ORDER_INVALID_PRICE":
		return ErrInvalidPrice
	case "ORDER_INVALID_QUANTITY":
		return ErrInvalidQuantity
	case "ORDER_INVALID_SIDE":
		return ErrInvalidSide
	case "ORDER_INVALID_TYPE":
		return ErrInvalidType
	case "INVALID STOCK":
		return ErrInvalidStock
	default:
		return ErrOther
	}
}
