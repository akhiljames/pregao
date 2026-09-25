package worker

import (
	"context"
	"testing"
	"time"

	ativosv1 "github.com/akhiljames/proto/gen/go/ativos/v1"
	livrov1 "github.com/akhiljames/proto/gen/go/livro/v1"
	"github.com/akhiljames/pregao/internal/client/ativos"
	"github.com/akhiljames/pregao/internal/client/livro"
	"github.com/akhiljames/pregao/internal/db"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockOrdersRepo implements db.OrdersRepository in-memory for testing.
type mockOrdersRepo struct {
	orders map[string]*db.BrokerOrder // keyed by provider + ":" + provider_order_id
}

func newMockOrdersRepo() *mockOrdersRepo {
	return &mockOrdersRepo{orders: make(map[string]*db.BrokerOrder)}
}

func (m *mockOrdersRepo) GetOrderByIntentID(ctx context.Context, tradeIntentID uuid.UUID) (*db.BrokerOrder, error) {
	for _, o := range m.orders {
		if o.TradeIntentID == tradeIntentID {
			return o, nil
		}
	}
	return nil, db.ErrOrderNotFound
}

func (m *mockOrdersRepo) GetOrderByIdempotencyKey(ctx context.Context, tenantID, idempotencyKey string) (*db.BrokerOrder, error) {
	for _, o := range m.orders {
		if o.TenantID == tenantID && o.IdempotencyKey == idempotencyKey {
			return o, nil
		}
	}
	return nil, db.ErrOrderNotFound
}

func (m *mockOrdersRepo) GetOrderByProviderOrderID(ctx context.Context, provider, providerOrderID string) (*db.BrokerOrder, error) {
	key := provider + ":" + providerOrderID
	if o, ok := m.orders[key]; ok {
		return o, nil
	}
	return nil, db.ErrOrderNotFound
}

func (m *mockOrdersRepo) CreateOrder(ctx context.Context, order *db.BrokerOrder) error {
	key := order.Provider + ":" + order.ProviderOrderID
	m.orders[key] = order
	return nil
}

func (m *mockOrdersRepo) UpdateOrderFill(ctx context.Context, provider, providerOrderID string, status string, filledQuantity decimal.Decimal, avgFillPrice decimal.Decimal) error {
	key := provider + ":" + providerOrderID
	o, ok := m.orders[key]
	if !ok {
		return db.ErrOrderNotFound
	}
	o.Status = status
	o.FilledQuantity = filledQuantity
	o.AverageFillPrice = avgFillPrice
	o.UpdatedAt = time.Now().UTC()
	return nil
}

// MockLivroClient implements livro.Client in-memory for testing.
type mockLivroClient struct {
	capturedHolds []livro.CaptureHoldParams
	credits       []livro.CreditParams
}

func (m *mockLivroClient) CaptureHold(ctx context.Context, params livro.CaptureHoldParams) (*livrov1.CaptureHoldResponse, error) {
	m.capturedHolds = append(m.capturedHolds, params)
	return &livrov1.CaptureHoldResponse{}, nil
}

func (m *mockLivroClient) Credit(ctx context.Context, params livro.CreditParams) (*livrov1.TransactionResponse, error) {
	m.credits = append(m.credits, params)
	return &livrov1.TransactionResponse{}, nil
}

func (m *mockLivroClient) GetBalance(ctx context.Context, accountID string) (decimal.Decimal, error) {
	return decimal.RequireFromString("10000.0000"), nil
}

func (m *mockLivroClient) Close() error { return nil }

// MockAtivosClient implements ativos.Client in-memory for testing.
type mockAtivosClient struct {
	syncCalls []ativos.SyncExecutionParams
}

func (m *mockAtivosClient) SyncBrokerExecution(ctx context.Context, params ativos.SyncExecutionParams) (*ativosv1.SyncBrokerExecutionResponse, error) {
	m.syncCalls = append(m.syncCalls, params)
	return &ativosv1.SyncBrokerExecutionResponse{Status: "SYNCED"}, nil
}

func (m *mockAtivosClient) Close() error { return nil }

func TestFillProcessor_ProcessBuyFill(t *testing.T) {
	ordersRepo := newMockOrdersRepo()
	livroClient := &mockLivroClient{}
	ativosClient := &mockAtivosClient{}

	intentID := uuid.New()
	initialOrder := &db.BrokerOrder{
		ID:               uuid.New(),
		TradeIntentID:    intentID,
		TenantID:         "tenant-1",
		UserID:           "user-1",
		Provider:         "BINANCE",
		ProviderOrderID:  "order-100",
		Symbol:           "BTCUSDT",
		Side:             db.SideBuy,
		TargetQuantity:   decimal.RequireFromString("1.0"),
		Status:           db.StatusSubmitted,
		FilledQuantity:   decimal.Zero,
		AverageFillPrice: decimal.Zero,
		LivroHoldID:      "hold-999",
		PortfolioID:      "portfolio-555",
	}
	require.NoError(t, ordersRepo.CreateOrder(context.Background(), initialOrder))

	processor := NewFillProcessor(ordersRepo, nil, ativosClient)

	// Process first partial fill: 0.4 BTC @ $60,000.00
	event1 := FillEvent{
		Provider:         "BINANCE",
		ProviderOrderID:  "order-100",
		Symbol:           "BTCUSDT",
		Side:             "BUY",
		Status:           "PARTIAL",
		ExecutedQuantity: decimal.RequireFromString("0.4"),
		FillPrice:        decimal.RequireFromString("60000.00"),
	}

	err := processor.ProcessFill(context.Background(), event1)
	require.NoError(t, err)

	// Check Ativos: SyncBrokerExecution called
	require.Len(t, ativosClient.syncCalls, 1)
	sync1 := ativosClient.syncCalls[0]
	assert.Equal(t, "portfolio-555", sync1.PortfolioID)
	assert.Equal(t, intentID.String(), sync1.TradeIntentID)
	assert.Equal(t, ativosv1.SyncBrokerExecutionRequest_BUY, sync1.Action)
	assert.True(t, decimal.RequireFromString("0.4").Equal(sync1.ExecutedShares))
	assert.True(t, decimal.RequireFromString("60000.00").Equal(sync1.ExecutedPrice))

	// Check DB update: status PARTIAL, filled 0.4, avg price 60000.00
	updated1, err := ordersRepo.GetOrderByProviderOrderID(context.Background(), "BINANCE", "order-100")
	require.NoError(t, err)
	assert.Equal(t, db.StatusPartial, updated1.Status)
	assert.True(t, decimal.RequireFromString("0.4").Equal(updated1.FilledQuantity))
	assert.True(t, decimal.RequireFromString("60000.00").Equal(updated1.AverageFillPrice))

	// Process second final fill: 0.6 BTC @ $65,000.00 (reaching 1.0 total)
	event2 := FillEvent{
		Provider:         "BINANCE",
		ProviderOrderID:  "order-100",
		Symbol:           "BTCUSDT",
		Side:             "BUY",
		Status:           "FILLED",
		ExecutedQuantity: decimal.RequireFromString("0.6"),
		FillPrice:        decimal.RequireFromString("65000.00"),
	}

	err = processor.ProcessFill(context.Background(), event2)
	require.NoError(t, err)

	// Check Ativos second sync:
	require.Len(t, ativosClient.syncCalls, 2)
	sync2 := ativosClient.syncCalls[1]
	assert.True(t, decimal.RequireFromString("0.6").Equal(sync2.ExecutedShares))

	// Check DB final state: status FILLED, filled 1.0, weighted avg: (24000 + 39000) / 1.0 = 63,000
	updated2, err := ordersRepo.GetOrderByProviderOrderID(context.Background(), "BINANCE", "order-100")
	require.NoError(t, err)
	assert.Equal(t, db.StatusFilled, updated2.Status)
	assert.True(t, decimal.RequireFromString("1.0").Equal(updated2.FilledQuantity))
	assert.True(t, decimal.RequireFromString("63000.00").Equal(updated2.AverageFillPrice))

	// Test standalone Livro mode when Ativos is nil:
	standaloneProcessor := NewFillProcessor(ordersRepo, livroClient, nil)
	err = standaloneProcessor.ProcessFill(context.Background(), event1)
	require.NoError(t, err)
	require.Len(t, livroClient.capturedHolds, 1)
	cap1 := livroClient.capturedHolds[0]
	assert.Equal(t, "hold-999", cap1.HoldID)
	assert.True(t, decimal.RequireFromString("24000.0000").Equal(cap1.Amount))
	assert.True(t, cap1.ReleaseRemainder)
}

func TestFillProcessor_ProcessSellFill(t *testing.T) {
	ordersRepo := newMockOrdersRepo()
	livroClient := &mockLivroClient{}
	ativosClient := &mockAtivosClient{}

	intentID := uuid.New()
	initialOrder := &db.BrokerOrder{
		ID:               uuid.New(),
		TradeIntentID:    intentID,
		TenantID:         "tenant-1",
		UserID:           "user-wallet-123",
		Provider:         "BINANCE",
		ProviderOrderID:  "order-200",
		Symbol:           "ETHUSDT",
		Side:             db.SideSell,
		TargetQuantity:   decimal.RequireFromString("2.0"),
		Status:           db.StatusSubmitted,
		FilledQuantity:   decimal.Zero,
		AverageFillPrice: decimal.Zero,
		PortfolioID:      "portfolio-777",
	}
	require.NoError(t, ordersRepo.CreateOrder(context.Background(), initialOrder))

	processor := NewFillProcessor(ordersRepo, nil, ativosClient)

	// Fill 2.0 ETH @ $3,500.00
	event := FillEvent{
		Provider:         "BINANCE",
		ProviderOrderID:  "order-200",
		Symbol:           "ETHUSDT",
		Side:             "SELL",
		Status:           "FILLED",
		ExecutedQuantity: decimal.RequireFromString("2.0"),
		FillPrice:        decimal.RequireFromString("3500.00"),
	}

	err := processor.ProcessFill(context.Background(), event)
	require.NoError(t, err)

	// Check Ativos: SyncBrokerExecution called with Action SELL
	require.Len(t, ativosClient.syncCalls, 1)
	sync := ativosClient.syncCalls[0]
	assert.Equal(t, ativosv1.SyncBrokerExecutionRequest_SELL, sync.Action)
	assert.True(t, decimal.RequireFromString("2.0").Equal(sync.ExecutedShares))
	assert.True(t, decimal.RequireFromString("3500.00").Equal(sync.ExecutedPrice))

	// Test standalone Livro mode when Ativos is nil:
	standaloneProcessor := NewFillProcessor(ordersRepo, livroClient, nil)
	err = standaloneProcessor.ProcessFill(context.Background(), event)
	require.NoError(t, err)
	require.Len(t, livroClient.credits, 1)
	cred := livroClient.credits[0]
	assert.Equal(t, "user-wallet-123", cred.AccountID)
	assert.Equal(t, "tenant-1", cred.TenantID)
	assert.True(t, decimal.RequireFromString("7000.0000").Equal(cred.Amount))
}
