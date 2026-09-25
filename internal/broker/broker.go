package broker

import (
	"context"
	"errors"

	"github.com/akhiljames/pregao/internal/vault"
	"github.com/shopspring/decimal"
)

var (
	ErrRateLimited     = errors.New("broker rate limited request (HTTP 429)")
	ErrOrderRejected   = errors.New("broker rejected order")
	ErrSymbolNotFound  = errors.New("symbol not found or inactive")
	ErrInvalidQuantity = errors.New("invalid order quantity")
)

// BookTicker holds the best bid and ask along with computed mid-price.
// STRICT: float32/float64 banned. All pricing uses decimal.Decimal.
type BookTicker struct {
	Symbol   string          `json:"symbol"`
	BidPrice decimal.Decimal `json:"bid_price"`
	BidQty   decimal.Decimal `json:"bid_qty"`
	AskPrice decimal.Decimal `json:"ask_price"`
	AskQty   decimal.Decimal `json:"ask_qty"`
	MidPrice decimal.Decimal `json:"mid_price"`
}

// MarketOrderRequest defines the parameters for a broker market order.
type MarketOrderRequest struct {
	Symbol        string          `json:"symbol"`
	Side          string          `json:"side"` // "BUY" or "SELL"
	Quantity      decimal.Decimal `json:"quantity"`
	ClientOrderID string          `json:"client_order_id"`
}

// OrderResult represents the result returned by the broker upon placing an order.
type OrderResult struct {
	ProviderOrderID         string          `json:"provider_order_id"`
	Status                  string          `json:"status"` // "SUBMITTED", "PARTIAL", "FILLED", "REJECTED"
	ExecutedQuantity        decimal.Decimal `json:"executed_quantity"`
	CumulativeQuoteQuantity decimal.Decimal `json:"cumulative_quote_quantity"`
	AveragePrice            decimal.Decimal `json:"average_price"`
}

// Broker defines the external broker operations (Binance, etc.).
type Broker interface {
	GetBookTickers(ctx context.Context, symbols []string) (map[string]BookTicker, error)
	ValidateSymbols(ctx context.Context, symbols []string) (map[string]bool, error)
	PlaceMarketOrder(ctx context.Context, creds *vault.PlaintextCredentials, req MarketOrderRequest) (*OrderResult, error)
	GetAccountBalances(ctx context.Context, creds *vault.PlaintextCredentials) (map[string]decimal.Decimal, error)
}
