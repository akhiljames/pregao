package service

import (
	"context"
	"errors"
	"testing"

	livrov1 "github.com/akhiljames/proto/gen/go/livro/v1"
	pregaov1 "github.com/akhiljames/proto/gen/go/pregao/v1"
	"github.com/akhiljames/pregao/internal/broker"
	"github.com/akhiljames/pregao/internal/cache"
	"github.com/akhiljames/pregao/internal/client/livro"
	"github.com/akhiljames/pregao/internal/db"
	"github.com/akhiljames/pregao/internal/vault"
	"github.com/akhiljames/pregao/internal/worker"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// --- Mocks ---

type mockQuotesCache struct {
	freshQuotes map[string]cache.CachedQuote
	staleQuotes map[string]cache.CachedQuote
	savedQuotes map[string]decimal.Decimal
}

func newMockQuotesCache() *mockQuotesCache {
	return &mockQuotesCache{
		freshQuotes: make(map[string]cache.CachedQuote),
		staleQuotes: make(map[string]cache.CachedQuote),
		savedQuotes: make(map[string]decimal.Decimal),
	}
}

func (m *mockQuotesCache) GetQuotes(ctx context.Context, provider string, symbols []string) (map[string]cache.CachedQuote, []string, error) {
	hits := make(map[string]cache.CachedQuote)
	var misses []string
	for _, s := range symbols {
		if q, ok := m.freshQuotes[s]; ok {
			hits[s] = q
		} else {
			misses = append(misses, s)
		}
	}
	return hits, misses, nil
}

func (m *mockQuotesCache) SetQuotes(ctx context.Context, provider string, quotes map[string]decimal.Decimal, timestamp int64) error {
	for k, v := range quotes {
		m.savedQuotes[k] = v
		m.freshQuotes[k] = cache.CachedQuote{
			Symbol:       k,
			Price:        v,
			Timestamp:    timestamp,
			IsStaleCache: false,
		}
		m.staleQuotes[k] = cache.CachedQuote{
			Symbol:       k,
			Price:        v,
			Timestamp:    timestamp,
			IsStaleCache: true,
		}
	}
	return nil
}

func (m *mockQuotesCache) GetStaleQuote(ctx context.Context, provider string, symbol string) (*cache.CachedQuote, error) {
	if q, ok := m.staleQuotes[symbol]; ok {
		return &q, nil
	}
	return nil, cache.ErrCacheMiss
}

func (m *mockQuotesCache) GetStaleQuotes(ctx context.Context, provider string, symbols []string) (map[string]cache.CachedQuote, error) {
	res := make(map[string]cache.CachedQuote)
	for _, s := range symbols {
		if q, ok := m.staleQuotes[s]; ok {
			res[s] = q
		}
	}
	return res, nil
}

type mockBroker struct {
	bookTickers     map[string]broker.BookTicker
	rateLimitError  bool
	marketOrderResp *broker.OrderResult
	marketOrderErr  error
	balancesResp    map[string]decimal.Decimal
}

func (m *mockBroker) GetBookTickers(ctx context.Context, symbols []string) (map[string]broker.BookTicker, error) {
	if m.rateLimitError {
		return nil, broker.ErrRateLimited
	}
	res := make(map[string]broker.BookTicker)
	for _, s := range symbols {
		if t, ok := m.bookTickers[s]; ok {
			res[s] = t
		}
	}
	return res, nil
}

func (m *mockBroker) ValidateSymbols(ctx context.Context, symbols []string) (map[string]bool, error) {
	res := make(map[string]bool)
	for _, s := range symbols {
		res[s] = (s == "BTCUSDT" || s == "ETHUSDT")
	}
	return res, nil
}

func (m *mockBroker) PlaceMarketOrder(ctx context.Context, creds *vault.PlaintextCredentials, req broker.MarketOrderRequest) (*broker.OrderResult, error) {
	defer creds.Zero()
	if m.marketOrderErr != nil {
		return nil, m.marketOrderErr
	}
	return m.marketOrderResp, nil
}

func (m *mockBroker) GetAccountBalances(ctx context.Context, creds *vault.PlaintextCredentials) (map[string]decimal.Decimal, error) {
	defer creds.Zero()
	return m.balancesResp, nil
}

type mockVaultClient struct {
	decryptedKey    string
	decryptedSecret string
}

func (m *mockVaultClient) Decrypt(ctx context.Context, ciphertext string) ([]byte, error) {
	return []byte("plain-key"), nil
}

func (m *mockVaultClient) DecryptCredentials(ctx context.Context, apiKeyCiphertext, apiSecretCiphertext string) (*vault.PlaintextCredentials, error) {
	return &vault.PlaintextCredentials{
		APIKey:    []byte(m.decryptedKey),
		APISecret: []byte(m.decryptedSecret),
	}, nil
}

type mockCredentialsRepo struct {
	creds map[string]*db.BrokerCredential
}

func newMockCredentialsRepo() *mockCredentialsRepo {
	return &mockCredentialsRepo{creds: make(map[string]*db.BrokerCredential)}
}

func (m *mockCredentialsRepo) GetCredentials(ctx context.Context, tenantID, provider string) (*db.BrokerCredential, error) {
	key := tenantID + ":" + provider
	if c, ok := m.creds[key]; ok {
		return c, nil
	}
	return nil, db.ErrCredentialsNotFound
}

func (m *mockCredentialsRepo) SaveCredentials(ctx context.Context, cred *db.BrokerCredential) error {
	key := cred.TenantID + ":" + cred.Provider
	m.creds[key] = cred
	return nil
}

type mockOrdersRepo struct {
	orders map[string]*db.BrokerOrder
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
	for _, o := range m.orders {
		if o.Provider == provider && o.ProviderOrderID == providerOrderID {
			return o, nil
		}
	}
	return nil, db.ErrOrderNotFound
}

func (m *mockOrdersRepo) CreateOrder(ctx context.Context, order *db.BrokerOrder) error {
	m.orders[order.TradeIntentID.String()] = order
	return nil
}

func (m *mockOrdersRepo) UpdateOrderFill(ctx context.Context, provider, providerOrderID string, status string, filledQuantity decimal.Decimal, avgFillPrice decimal.Decimal) error {
	for _, o := range m.orders {
		if o.Provider == provider && o.ProviderOrderID == providerOrderID {
			o.Status = status
			o.FilledQuantity = filledQuantity
			o.AverageFillPrice = avgFillPrice
			return nil
		}
	}
	return db.ErrOrderNotFound
}

type mockLivroClient struct {
	balance decimal.Decimal
}

func (m *mockLivroClient) CaptureHold(ctx context.Context, params livro.CaptureHoldParams) (*livrov1.CaptureHoldResponse, error) {
	return &livrov1.CaptureHoldResponse{}, nil
}

func (m *mockLivroClient) Credit(ctx context.Context, params livro.CreditParams) (*livrov1.TransactionResponse, error) {
	return &livrov1.TransactionResponse{}, nil
}

func (m *mockLivroClient) GetBalance(ctx context.Context, accountID string) (decimal.Decimal, error) {
	return m.balance, nil
}

func (m *mockLivroClient) GetOrCreateBrokerAccount(ctx context.Context, tenantID string) (string, error) {
	return "00000000-0000-0000-0000-000000000001", nil
}

func (m *mockLivroClient) Close() error { return nil }

// --- Tests ---

func TestPregaoServer_GetQuotes_CacheHit(t *testing.T) {
	cacheMock := newMockQuotesCache()
	cacheMock.freshQuotes["BTCUSDT"] = cache.CachedQuote{
		Symbol:       "BTCUSDT",
		Price:        decimal.RequireFromString("65000.00"),
		Timestamp:    1600000000,
		IsStaleCache: false,
	}

	brokerMock := &mockBroker{}
	srv := NewPregaoServer(ServerParams{
		Cache:        cacheMock,
		BrokerClient: brokerMock,
	})

	resp, err := srv.GetQuotes(context.Background(), &pregaov1.GetQuotesRequest{
		Symbols: []string{"BTCUSDT"},
	})
	require.NoError(t, err)
	require.Contains(t, resp.Quotes, "BTCUSDT")

	quote := resp.Quotes["BTCUSDT"]
	assert.Equal(t, "BTCUSDT", quote.Symbol)
	assert.Equal(t, "65000", quote.Price)
	assert.False(t, quote.IsStaleCache)
}

func TestPregaoServer_GetQuotes_CacheMiss_FetchesFromBroker(t *testing.T) {
	cacheMock := newMockQuotesCache()
	brokerMock := &mockBroker{
		bookTickers: map[string]broker.BookTicker{
			"ETHUSDT": {
				Symbol:   "ETHUSDT",
				MidPrice: decimal.RequireFromString("3500.25"),
			},
		},
	}

	srv := NewPregaoServer(ServerParams{
		Cache:        cacheMock,
		BrokerClient: brokerMock,
	})

	resp, err := srv.GetQuotes(context.Background(), &pregaov1.GetQuotesRequest{
		Symbols: []string{"ETHUSDT"},
	})
	require.NoError(t, err)
	require.Contains(t, resp.Quotes, "ETHUSDT")

	quote := resp.Quotes["ETHUSDT"]
	assert.Equal(t, "ETHUSDT", quote.Symbol)
	assert.Equal(t, "3500.25", quote.Price)
	assert.False(t, quote.IsStaleCache)

	// Verify quote was saved to Redis
	assert.True(t, decimal.RequireFromString("3500.25").Equal(cacheMock.savedQuotes["ETHUSDT"]))
}

func TestPregaoServer_GetQuotes_BrokerRateLimit_ServesStaleCache(t *testing.T) {
	cacheMock := newMockQuotesCache()
	// Stale quote present from earlier in the day
	cacheMock.staleQuotes["SOLUSDT"] = cache.CachedQuote{
		Symbol:       "SOLUSDT",
		Price:        decimal.RequireFromString("150.00"),
		Timestamp:    1600000000,
		IsStaleCache: true,
	}

	// Broker fails with 429
	brokerMock := &mockBroker{
		rateLimitError: true,
	}

	srv := NewPregaoServer(ServerParams{
		Cache:        cacheMock,
		BrokerClient: brokerMock,
	})

	resp, err := srv.GetQuotes(context.Background(), &pregaov1.GetQuotesRequest{
		Symbols: []string{"SOLUSDT"},
	})
	require.NoError(t, err)
	require.Contains(t, resp.Quotes, "SOLUSDT")

	quote := resp.Quotes["SOLUSDT"]
	assert.Equal(t, "SOLUSDT", quote.Symbol)
	assert.Equal(t, "150", quote.Price)
	assert.True(t, quote.IsStaleCache, "Should have served stale cache with flag=true")
}

func TestPregaoServer_ValidateTickers(t *testing.T) {
	srv := NewPregaoServer(ServerParams{
		BrokerClient: &mockBroker{},
	})

	resp, err := srv.ValidateTickers(context.Background(), &pregaov1.ValidateTickersRequest{
		Symbols: []string{"BTCUSDT", "INVALID"},
	})
	require.NoError(t, err)
	assert.True(t, resp.ValidSymbols["BTCUSDT"])
	assert.False(t, resp.ValidSymbols["INVALID"])
}

func TestPregaoServer_ExecuteTrade_Idempotency(t *testing.T) {
	ordersRepo := newMockOrdersRepo()
	intentID := uuid.New()

	existingOrder := &db.BrokerOrder{
		ID:              uuid.New(),
		TradeIntentID:   intentID,
		TenantID:        "tenant-1",
		UserID:          "user-1",
		Provider:        "BINANCE",
		ProviderOrderID: "existing-order-456",
		Symbol:          "BTCUSDT",
		Side:            db.SideBuy,
		TargetQuantity:  decimal.RequireFromString("1.0"),
		Status:          db.StatusSubmitted,
	}
	require.NoError(t, ordersRepo.CreateOrder(context.Background(), existingOrder))

	brokerMock := &mockBroker{
		marketOrderErr: errors.New("should never be called"),
	}

	srv := NewPregaoServer(ServerParams{
		OrdersRepo:   ordersRepo,
		BrokerClient: brokerMock,
	})

	resp, err := srv.ExecuteTrade(context.Background(), &pregaov1.ExecuteTradeRequest{
		TradeIntentId: intentID.String(),
		TenantId:      "tenant-1",
		UserId:        "user-1",
		Provider:      "BINANCE",
		Symbol:        "BTCUSDT",
		Side:          pregaov1.ExecuteTradeRequest_BUY,
		Quantity:      "1.0",
	})
	require.NoError(t, err)
	assert.Equal(t, "existing-order-456", resp.ProviderOrderId)
	assert.Equal(t, db.StatusSubmitted, resp.Status)
}

func TestPregaoServer_ExecuteTrade_Success(t *testing.T) {
	ordersRepo := newMockOrdersRepo()
	credsRepo := newMockCredentialsRepo()
	vaultMock := &mockVaultClient{
		decryptedKey:    "real-binance-key",
		decryptedSecret: "real-binance-secret",
	}

	// Seed ciphertext credentials at tenant level
	require.NoError(t, credsRepo.SaveCredentials(context.Background(), &db.BrokerCredential{
		TenantID:            "tenant-1",
		Provider:            "BINANCE",
		APIKeyCiphertext:    "vault:v1:encrypted_key",
		APISecretCiphertext: "vault:v1:encrypted_secret",
	}))

	brokerMock := &mockBroker{
		marketOrderResp: &broker.OrderResult{
			ProviderOrderID:  "binance-order-789",
			Status:           db.StatusSubmitted,
			ExecutedQuantity: decimal.Zero,
			AveragePrice:     decimal.Zero,
		},
	}

	srv := NewPregaoServer(ServerParams{
		OrdersRepo:      ordersRepo,
		CredentialsRepo: credsRepo,
		VaultClient:     vaultMock,
		BrokerClient:    brokerMock,
	})

	intentID := uuid.New()
	resp, err := srv.ExecuteTrade(context.Background(), &pregaov1.ExecuteTradeRequest{
		TradeIntentId:  intentID.String(),
		TenantId:       "tenant-1",
		UserId:         "user-1",
		Provider:       "BINANCE",
		Symbol:         "BTCUSDT",
		Side:           pregaov1.ExecuteTradeRequest_BUY,
		Quantity:       "0.5",
		IdempotencyKey: "idem-key-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "binance-order-789", resp.ProviderOrderId)
	assert.Equal(t, db.StatusSubmitted, resp.Status)

	// Verify order was saved to database
	savedOrder, err := ordersRepo.GetOrderByIntentID(context.Background(), intentID)
	require.NoError(t, err)
	assert.Equal(t, "binance-order-789", savedOrder.ProviderOrderID)
	assert.True(t, decimal.RequireFromString("0.5").Equal(savedOrder.TargetQuantity))
}

func TestPregaoServer_ExecuteTrade_MissingCredentials(t *testing.T) {
	ordersRepo := newMockOrdersRepo()
	credsRepo := newMockCredentialsRepo()

	srv := NewPregaoServer(ServerParams{
		OrdersRepo:      ordersRepo,
		CredentialsRepo: credsRepo,
	})

	intentID := uuid.New()
	_, err := srv.ExecuteTrade(context.Background(), &pregaov1.ExecuteTradeRequest{
		TradeIntentId: intentID.String(),
		TenantId:      "tenant-unknown",
		UserId:        "user-unknown",
		Provider:      "BINANCE",
		Symbol:        "BTCUSDT",
		Quantity:      "1.0",
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

func TestPregaoServer_SyncBrokerBalances(t *testing.T) {
	credsRepo := newMockCredentialsRepo()
	vaultMock := &mockVaultClient{
		decryptedKey:    "k",
		decryptedSecret: "s",
	}

	require.NoError(t, credsRepo.SaveCredentials(context.Background(), &db.BrokerCredential{
		TenantID:            "tenant-1",
		Provider:            "BINANCE",
		APIKeyCiphertext:    "vault:v1:k",
		APISecretCiphertext: "vault:v1:s",
	}))

	// Broker has 15000.50 USDT
	brokerMock := &mockBroker{
		balancesResp: map[string]decimal.Decimal{
			"USDT": decimal.RequireFromString("15000.50"),
		},
	}

	// Livro ledger has 14000.00 USD
	livroMock := &mockLivroClient{
		balance: decimal.RequireFromString("14000.00"),
	}

	srv := NewPregaoServer(ServerParams{
		CredentialsRepo: credsRepo,
		VaultClient:     vaultMock,
		BrokerClient:    brokerMock,
		LivroClient:     livroMock,
	})

	resp, err := srv.SyncBrokerBalances(context.Background(), &pregaov1.SyncBrokerBalancesRequest{
		TenantId: "tenant-1",
		UserId:   "user-1",
		Provider: "BINANCE",
	})
	require.NoError(t, err)

	// Net adjustment = 15000.50 - 14000.00 = +1000.50
	assert.False(t, resp.InSync)
	assert.Equal(t, "1000.5000", resp.NetFiatAdjustment)
}

type mockFillProcessor struct {
	ordersRepo db.OrdersRepository
	lastEvent  worker.FillEvent
}

func (m *mockFillProcessor) ProcessFill(ctx context.Context, event worker.FillEvent) error {
	m.lastEvent = event
	order, err := m.ordersRepo.GetOrderByProviderOrderID(ctx, event.Provider, event.ProviderOrderID)
	if err != nil {
		return err
	}
	newFilledQty := order.FilledQuantity.Add(event.ExecutedQuantity)
	return m.ordersRepo.UpdateOrderFill(ctx, event.Provider, event.ProviderOrderID, event.Status, newFilledQty, event.FillPrice)
}

func TestPregaoServer_ExecuteTrade_FillProcessor_NoDoubleCount(t *testing.T) {
	ordersRepo := newMockOrdersRepo()
	credsRepo := newMockCredentialsRepo()
	vaultMock := &mockVaultClient{
		decryptedKey:    "real-binance-key",
		decryptedSecret: "real-binance-secret",
	}

	require.NoError(t, credsRepo.SaveCredentials(context.Background(), &db.BrokerCredential{
		TenantID:            "tenant-1",
		Provider:            "BINANCE",
		APIKeyCiphertext:    "vault:v1:encrypted_key",
		APISecretCiphertext: "vault:v1:encrypted_secret",
	}))

	fillQty := decimal.RequireFromString("0.00011000")
	fillPrice := decimal.RequireFromString("83924.7300")

	brokerMock := &mockBroker{
		marketOrderResp: &broker.OrderResult{
			ProviderOrderID:  "binance-order-12345",
			Status:           db.StatusFilled,
			ExecutedQuantity: fillQty,
			AveragePrice:     fillPrice,
		},
	}

	fillProc := &mockFillProcessor{ordersRepo: ordersRepo}

	srv := NewPregaoServer(ServerParams{
		OrdersRepo:      ordersRepo,
		CredentialsRepo: credsRepo,
		VaultClient:     vaultMock,
		BrokerClient:    brokerMock,
		FillProcessor:   fillProc,
	})

	intentID := uuid.New()
	resp, err := srv.ExecuteTrade(context.Background(), &pregaov1.ExecuteTradeRequest{
		TradeIntentId:  intentID.String(),
		TenantId:       "tenant-1",
		UserId:         "user-1",
		Provider:       "BINANCE",
		Symbol:         "BTCUSDT",
		Side:           pregaov1.ExecuteTradeRequest_BUY,
		Quantity:       "0.00011671",
		IdempotencyKey: "idem-single-fill",
	})
	require.NoError(t, err)
	assert.Equal(t, "binance-order-12345", resp.ProviderOrderId)

	// Verify order was saved and filled in database without double counting
	savedOrder, err := ordersRepo.GetOrderByIntentID(context.Background(), intentID)
	require.NoError(t, err)
	assert.Equal(t, "binance-order-12345", savedOrder.ProviderOrderID)
	assert.True(t, fillQty.Equal(savedOrder.FilledQuantity), "expected %s, got %s", fillQty, savedOrder.FilledQuantity)
	assert.True(t, fillPrice.Equal(savedOrder.AverageFillPrice), "expected %s, got %s", fillPrice, savedOrder.AverageFillPrice)
}

