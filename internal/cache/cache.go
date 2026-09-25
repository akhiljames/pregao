package cache

import (
	"context"
	"errors"

	"github.com/shopspring/decimal"
)

var (
	ErrCacheMiss = errors.New("quote not found in cache")
)

// CachedQuote represents the cached quote structure stored in Redis.
// Strictly uses decimal.Decimal for price precision.
type CachedQuote struct {
	Symbol       string          `json:"symbol"`
	Price        decimal.Decimal `json:"price"`
	Timestamp    int64           `json:"timestamp"`
	IsStaleCache bool            `json:"is_stale_cache"`
}

// QuotesCache defines the market data caching operations.
type QuotesCache interface {
	GetQuotes(ctx context.Context, provider string, symbols []string) (hits map[string]CachedQuote, misses []string, err error)
	SetQuotes(ctx context.Context, provider string, quotes map[string]decimal.Decimal, timestamp int64) error
	GetStaleQuote(ctx context.Context, provider string, symbol string) (*CachedQuote, error)
	GetStaleQuotes(ctx context.Context, provider string, symbols []string) (map[string]CachedQuote, error)
}
