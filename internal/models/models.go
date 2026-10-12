package models

import (
	"TradeIT/internal/database"
	"log"
	"math"
	"time"
)

// PriceScale is the number of integer units in one currency unit. All prices in the
// engine are integer values scaled by this factor: a price of 185.40 is represented as
// 18540. The user-order service converts the client's decimal price into this integer
// form at its boundary, so the matching engine only ever sees scaled integers.
const PriceScale int64 = 100

// PriceToInt converts a decimal price into the scaled integer representation, rounding
// to the nearest integer unit. This is the conversion the user-order service applies
// before an order is persisted or published to the engine.
func PriceToInt(price float64) int64 {
	return int64(math.Round(price * float64(PriceScale)))
}

// PriceToFloat converts a scaled integer price back to a decimal value, for display or
// for clients that still expect a decimal.
func PriceToFloat(price int64) float64 {
	return float64(price) / float64(PriceScale)
}

// NOTE: gin's binder only inspects the "binding" struct tag - it reconfigures
// go-playground/validator with SetTagName("binding") (see gin/binding/default_validator.go).
// The previous "validate:" tags were therefore never evaluated, and their syntax
// (space separated rules, missing commas) is invalid for validator v10: rules must be
// comma separated with no spaces, e.g. `binding:"required,oneof=buy sell"`.

type User struct {
	Id    uint64  `json:"userid" gorm:"PrimaryKey"`
	Name  string  `json:"name" binding:"required"`
	Email string  `json:"email" binding:"required,email"`
	Funds float32 `json:"funds"`
}

type Credential struct {
	Id uint64 `json:"userid" gorm:"PrimaryKey"`
	// uniqueIndex: one account per email address. AutoMigrate creates it (and silently
	// skips it if pre-existing duplicate rows make that impossible).
	Email  string `json:"email" gorm:"uniqueIndex" binding:"required,email"`
	Passwd string `json:"passwd" binding:"required,min=6"`
	Token  string
}

type MetaOrder struct {
	Id         uint64 `json:"orderid" gorm:"PrimaryKey"`
	User_id    int    `json:"user_id"`
	Order_Type string `json:"orderType" binding:"required,oneof=limit market"`
	Side       string `json:"type" binding:"required,oneof=buy sell"`
	Stock      string `json:"stock" binding:"required"`
	// Price is required (>0) for limit orders; market orders legitimately carry no price,
	// so it is only checked when present. It is stored in the scaled integer form
	// (see PriceScale), never as a float.
	Price      int64     `json:"price" gorm:"type:bigint" binding:"required_if=Order_Type limit,omitempty,gt=0"`
	AvgPrice   int64     `json:"avgPrice" gorm:"type:bigint"`
	Quantity   int       `json:"quantity" binding:"required,gt=0"`
	Status     string    `json:"status"`
	Created_at time.Time `json:"created_at"`
}

type Order struct {
	MetaOrder
	Remq int `json:"rem_quantity"`
}

type TradeDetails struct {
	TradeId     uint64    `json:"tradeid" gorm:"PrimaryKey"`
	Stock       string    `json:"stock"`
	Buyer       int       `json:"buyer"`
	Seller      int       `json:"seller"`
	BidOrderID  uint64    `json:"bidorder_id"`
	AskOrderID  uint64    `json:"askorder_id"`
	Quantity    int       `json:"quantity"`
	Price       int64     `json:"price" gorm:"type:bigint"`
	Executed_at time.Time `json:"execution_time"`
}

// ProjectionCheckpoint records how far the projection service has consumed a symbol's
// consumer event log. It is written in the same transaction as the orders and trades it
// covers, so on restart the projector resumes exactly where it left off. Seq is the
// stable identity (the applied command's sequence); Offset is a seek optimisation that is
// only trusted while the log has not been truncated.
type ProjectionCheckpoint struct {
	// autoIncrement:false is essential: symbol id 0 is a valid key, but GORM would treat
	// the zero value as "unset" and let the sequence assign it, silently inserting a new
	// row on every flush instead of updating symbol 0's checkpoint.
	SymbolID  uint32    `json:"symbol_id" gorm:"PrimaryKey;autoIncrement:false"`
	Seq       uint64    `json:"seq"`
	Offset    int64     `json:"offset" gorm:"column:log_offset"`
	UpdatedAt time.Time `json:"updated_at"`
}

func InitTables() {
	// AutoMigrate errors used to be discarded, so a change that could not be applied
	// (for example the unique index on credentials.email while duplicate rows still
	// exist) failed completely silently.
	migrate := func(value any) {
		if err := database.DB.AutoMigrate(value); err != nil {
			log.Println("warning: auto migration failed: ", err)
		}
	}
	migrate(User{})
	migrate(Credential{})
	migrate(Order{})
	migrate(TradeDetails{})
	migrate(ProjectionCheckpoint{})
}
