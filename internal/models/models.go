package models

import (
	"TradeIT/internal/database"
	"log"
	"time"
)

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
	// so it is only checked when present.
	Price      float64   `json:"price" gorm:"type:decimal" binding:"required_if=Order_Type limit,omitempty,gt=0"`
	AvgPrice   float64   `json:"avgPrice" gorm:"type:decimal"`
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
	Price       float64   `json:"price" gorm:"type:decimal"`
	Executed_at time.Time `json:"execution_time"`
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
}
