package cache

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCachedQuote_JSONSerialization(t *testing.T) {
	price := decimal.RequireFromString("62345.67890123")
	quote := CachedQuote{
		Symbol:       "BTCUSDT",
		Price:        price,
		Timestamp:    1600000000,
		IsStaleCache: false,
	}

	data, err := json.Marshal(quote)
	require.NoError(t, err)

	var decoded CachedQuote
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, "BTCUSDT", decoded.Symbol)
	assert.True(t, price.Equal(decoded.Price), "Deserialized price %s must equal original %s", decoded.Price, price)
	assert.Equal(t, int64(1600000000), decoded.Timestamp)
	assert.False(t, decoded.IsStaleCache)
}

func TestRedisKeyFormatting(t *testing.T) {
	assert.Equal(t, "pregao:quotes:BINANCE:BTCUSDT", freshKey("binance", "btcusdt"))
	assert.Equal(t, "pregao:quotes:stale:BINANCE:BTCUSDT", staleKey("binance", "btcusdt"))
	assert.Equal(t, "pregao:quotes:BINANCE:ETHUSDT", freshKey("BINANCE", "ETHUSDT"))
	assert.Equal(t, "pregao:quotes:stale:BINANCE:ETHUSDT", staleKey("BINANCE", "ETHUSDT"))
}

func TestNewRedisQuotesCache_Defaults(t *testing.T) {
	cache := NewRedisQuotesCache(nil, 0, 0)
	rc, ok := cache.(*redisQuotesCache)
	require.True(t, ok)
	assert.Equal(t, 5*time.Second, rc.freshTTL)
	assert.Equal(t, 24*time.Hour, rc.staleTTL)
}
