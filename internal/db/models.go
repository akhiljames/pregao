package db

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// Order status constants.
const (
	StatusSubmitted = "SUBMITTED"
	StatusPartial   = "PARTIAL"
	StatusFilled    = "FILLED"
	StatusCanceled  = "CANCELED"
	StatusRejected  = "REJECTED"
)

// Order side constants.
const (
	SideBuy  = "BUY"
	SideSell = "SELL"
)

// BrokerCredential represents encrypted broker API credentials stored in PostgreSQL at the Tenant (Fund Manager) level.
// Plaintext credentials NEVER touch PostgreSQL or disk.
type BrokerCredential struct {
	ID                  uuid.UUID `json:"id" db:"id"`
	TenantID            string    `json:"tenant_id" db:"tenant_id"`
	Provider            string    `json:"provider" db:"provider"`
	APIKeyCiphertext    string    `json:"api_key_ciphertext" db:"api_key_ciphertext"`
	APISecretCiphertext string    `json:"api_secret_ciphertext" db:"api_secret_ciphertext"`
	CreatedAt           time.Time `json:"created_at" db:"created_at"`
}

// BrokerOrder tracks the complete execution lifecycle of an order tied to an Ativos trade intent.
// All financial values strictly use decimal.Decimal.
type BrokerOrder struct {
	ID               uuid.UUID       `json:"id" db:"id"`
	TradeIntentID    uuid.UUID       `json:"trade_intent_id" db:"trade_intent_id"`
	TenantID         string          `json:"tenant_id" db:"tenant_id"`
	UserID           string          `json:"user_id" db:"user_id"`
	Provider         string          `json:"provider" db:"provider"`
	ProviderOrderID  string          `json:"provider_order_id" db:"provider_order_id"`
	Symbol           string          `json:"symbol" db:"symbol"`
	Side             string          `json:"side" db:"side"`
	TargetQuantity   decimal.Decimal `json:"target_quantity" db:"target_quantity"`
	Status           string          `json:"status" db:"status"`
	FilledQuantity   decimal.Decimal `json:"filled_quantity" db:"filled_quantity"`
	AverageFillPrice decimal.Decimal `json:"average_fill_price" db:"average_fill_price"`
	IdempotencyKey   string          `json:"idempotency_key" db:"idempotency_key"`
	PortfolioID      string          `json:"portfolio_id" db:"portfolio_id"`
	LivroHoldID      string          `json:"livro_hold_id" db:"livro_hold_id"`
	CreatedAt        time.Time       `json:"created_at" db:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at" db:"updated_at"`
}
