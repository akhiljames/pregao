package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

type redisQuotesCache struct {
	client   redis.UniversalClient
	freshTTL time.Duration
	staleTTL time.Duration
}

// NewRedisQuotesCache returns an implementation of QuotesCache backed by Redis.
func NewRedisQuotesCache(client redis.UniversalClient, freshTTL, staleTTL time.Duration) QuotesCache {
	if freshTTL <= 0 {
		freshTTL = 5 * time.Second
	}
	if staleTTL <= 0 {
		staleTTL = 24 * time.Hour
	}
	return &redisQuotesCache{
		client:   client,
		freshTTL: freshTTL,
		staleTTL: staleTTL,
	}
}

func freshKey(provider, symbol string) string {
	return fmt.Sprintf("pregao:quotes:%s:%s", strings.ToUpper(provider), strings.ToUpper(symbol))
}

func staleKey(provider, symbol string) string {
	return fmt.Sprintf("pregao:quotes:stale:%s:%s", strings.ToUpper(provider), strings.ToUpper(symbol))
}

func (c *redisQuotesCache) GetQuotes(ctx context.Context, provider string, symbols []string) (map[string]CachedQuote, []string, error) {
	hits := make(map[string]CachedQuote, len(symbols))
	var misses []string

	if len(symbols) == 0 {
		return hits, misses, nil
	}

	keys := make([]string, len(symbols))
	for i, s := range symbols {
		keys[i] = freshKey(provider, s)
	}

	results, err := c.client.MGet(ctx, keys...).Result()
	if err != nil {
		// On Redis failure, treat all as misses so fallback/direct fetch can occur
		return hits, symbols, fmt.Errorf("redis MGet failed: %w", err)
	}

	for i, res := range results {
		sym := symbols[i]
		if res == nil {
			misses = append(misses, sym)
			continue
		}

		strVal, ok := res.(string)
		if !ok {
			misses = append(misses, sym)
			continue
		}

		var q CachedQuote
		if err := json.Unmarshal([]byte(strVal), &q); err != nil {
			misses = append(misses, sym)
			continue
		}

		q.IsStaleCache = false
		hits[sym] = q
	}

	return hits, misses, nil
}

func (c *redisQuotesCache) SetQuotes(ctx context.Context, provider string, quotes map[string]decimal.Decimal, timestamp int64) error {
	if len(quotes) == 0 {
		return nil
	}

	pipe := c.client.Pipeline()
	for symbol, price := range quotes {
		quote := CachedQuote{
			Symbol:       symbol,
			Price:        price,
			Timestamp:    timestamp,
			IsStaleCache: false,
		}

		bytes, err := json.Marshal(quote)
		if err != nil {
			return fmt.Errorf("failed to marshal quote for %s: %w", symbol, err)
		}

		fKey := freshKey(provider, symbol)
		sKey := staleKey(provider, symbol)

		pipe.Set(ctx, fKey, bytes, c.freshTTL)
		pipe.Set(ctx, sKey, bytes, c.staleTTL)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute pipeline set: %w", err)
	}
	return nil
}

func (c *redisQuotesCache) GetStaleQuote(ctx context.Context, provider string, symbol string) (*CachedQuote, error) {
	key := staleKey(provider, symbol)
	val, err := c.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, ErrCacheMiss
		}
		return nil, fmt.Errorf("failed to get stale quote: %w", err)
	}

	var q CachedQuote
	if err := json.Unmarshal([]byte(val), &q); err != nil {
		return nil, fmt.Errorf("failed to unmarshal stale quote: %w", err)
	}

	q.IsStaleCache = true
	return &q, nil
}

func (c *redisQuotesCache) GetStaleQuotes(ctx context.Context, provider string, symbols []string) (map[string]CachedQuote, error) {
	result := make(map[string]CachedQuote, len(symbols))
	if len(symbols) == 0 {
		return result, nil
	}

	keys := make([]string, len(symbols))
	for i, s := range symbols {
		keys[i] = staleKey(provider, s)
	}

	vals, err := c.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to mget stale quotes: %w", err)
	}

	for i, res := range vals {
		sym := symbols[i]
		if res == nil {
			continue
		}
		strVal, ok := res.(string)
		if !ok {
			continue
		}
		var q CachedQuote
		if err := json.Unmarshal([]byte(strVal), &q); err != nil {
			continue
		}
		q.IsStaleCache = true
		result[sym] = q
	}

	return result, nil
}
