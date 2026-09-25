package db

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestBrokerOrder_ModelDecimalPrecision(t *testing.T) {
	intentID := uuid.New()
	targetQty := decimal.RequireFromString("123456789.12345678")
	filledQty := decimal.RequireFromString("98765432.87654321")
	avgPrice := decimal.RequireFromString("65432.1234")

	order := BrokerOrder{
		ID:               uuid.New(),
		TradeIntentID:    intentID,
		TenantID:         "tenant-alpha",
		UserID:           "user-omega",
		Provider:         "BINANCE",
		ProviderOrderID:  "provider-order-1",
		Symbol:           "BTCUSDT",
		Side:             SideBuy,
		TargetQuantity:   targetQty,
		Status:           StatusPartial,
		FilledQuantity:   filledQty,
		AverageFillPrice: avgPrice,
		IdempotencyKey:   "idem-12345",
		PortfolioID:      "port-1",
		LivroHoldID:      "hold-1",
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}

	assert.Equal(t, intentID, order.TradeIntentID)
	assert.True(t, targetQty.Equal(order.TargetQuantity))
	assert.True(t, filledQty.Equal(order.FilledQuantity))
	assert.True(t, avgPrice.Equal(order.AverageFillPrice))
}

func TestBrokerCredential_Model(t *testing.T) {
	credID := uuid.New()
	cred := BrokerCredential{
		ID:                  credID,
		TenantID:            "tenant-1",
		UserID:              "user-1",
		Provider:            "BINANCE",
		APIKeyCiphertext:    "vault:v1:some_encrypted_key",
		APISecretCiphertext: "vault:v1:some_encrypted_secret",
		CreatedAt:           time.Now().UTC(),
	}

	assert.Equal(t, credID, cred.ID)
	assert.Equal(t, "vault:v1:some_encrypted_key", cred.APIKeyCiphertext)
	assert.Equal(t, "vault:v1:some_encrypted_secret", cred.APISecretCiphertext)
}
