// Package ipc defines the fixed-size records exchanged between the order service
// (producer) and the matching engine (consumer) over the shared-memory ring, and the
// outcome records the engine appends to its WAL.
//
// Two record shapes exist on purpose:
//
//   - Cmd is the command (input) written by the producer.
//   - Outcome is the result of applying a Cmd, appended by the consumer.
//
// Both are encoded field-by-field in little-endian so the on-wire layout does not
// depend on Go struct padding.
package ipc

// OpType is the kind of operation carried by a command.
type OpType uint8

const (
	OpNew       OpType = iota // place a new order
	OpCancel                  // remove a resting order
	OpReduceQty               // in-place quantity reduction (keeps time priority)
	OpModify                  // price/quantity change: engine removes and re-matches
)

func (o OpType) String() string {
	switch o {
	case OpNew:
		return "new"
	case OpCancel:
		return "cancel"
	case OpReduceQty:
		return "reduce_qty"
	case OpModify:
		return "modify"
	default:
		return "unknown"
	}
}

// Side mirrors models.Order.Side as an enum for the hot path.
type Side uint8

const (
	SideBuy Side = iota
	SideSell
)

func (s Side) String() string {
	if s == SideSell {
		return "sell"
	}
	return "buy"
}

// OrderType mirrors models.Order.Order_Type as an enum for the hot path.
type OrderType uint8

const (
	TypeLimit OrderType = iota
	TypeMarket
)

func (t OrderType) String() string {
	if t == TypeMarket {
		return "market"
	}
	return "limit"
}

// Status is the outcome of applying a command.
type Status uint8

const (
	StatusAccepted Status = iota
	StatusResting
	StatusFilled
	StatusPartiallyFilled
	StatusCancelled
	StatusPartiallyCancelled
	StatusExecuted
	StatusRejected
)

func (s Status) String() string {
	switch s {
	case StatusAccepted:
		return "accepted"
	case StatusResting:
		return "resting"
	case StatusFilled:
		return "filled"
	case StatusPartiallyFilled:
		return "partially_filled"
	case StatusCancelled:
		return "cancelled"
	case StatusPartiallyCancelled:
		return "partially_cancelled"
	case StatusExecuted:
		return "executed"
	case StatusRejected:
		return "rejected"
	default:
		return "unknown"
	}
}

// Cmd is the fixed-size command record. RequestID correlates an API request with its
// result; (SymbolID, Seq) is the internal replay identity; OrderID+Version identify the
// order across its lifecycle.
type Cmd struct {
	RequestID uint64
	OrderID   uint64
	Seq       uint64
	Price     int64
	Version   uint32
	SymbolID  uint32
	Qty       int32
	Remq      int32
	Op        OpType
	Side      Side
	Typ       OrderType
}

// Outcome is the result of applying a Cmd.
type Outcome struct {
	AvgPrice  int64
	FilledQty int32
	Err       uint16
	Status    Status
}
