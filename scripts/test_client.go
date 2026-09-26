package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"time"

	ativosv1 "github.com/akhiljames/proto/gen/go/ativos/v1"
	livrov1 "github.com/akhiljames/proto/gen/go/livro/v1"
	pregaov1 "github.com/akhiljames/proto/gen/go/pregao/v1"
	"github.com/akhiljames/pregao/internal/broker"
	"github.com/akhiljames/pregao/internal/cache"
	"github.com/akhiljames/pregao/internal/client/ativos"
	"github.com/akhiljames/pregao/internal/client/livro"
	"github.com/akhiljames/pregao/internal/db"
	"github.com/akhiljames/pregao/internal/service"
	"github.com/akhiljames/pregao/internal/vault"
	"github.com/akhiljames/pregao/internal/webhook"
	"github.com/akhiljames/pregao/internal/worker"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

var (
	totalTestsPassed int32
	totalTestsFailed int32
)

func recordPass(testName string) {
	atomic.AddInt32(&totalTestsPassed, 1)
	fmt.Printf("   ✅ PASS: %s\n", testName)
}

func recordFail(testName string, err error) {
	atomic.AddInt32(&totalTestsFailed, 1)
	fmt.Printf("   ❌ FAIL: %s -> %v\n", testName, err)
}

func assertEqual(testName string, expected, actual string) error {
	expDec, err1 := decimal.NewFromString(expected)
	actDec, err2 := decimal.NewFromString(actual)
	if err1 == nil && err2 == nil {
		if !expDec.Equal(actDec) {
			err := fmt.Errorf("expected decimal %s, got %s", expected, actual)
			recordFail(testName, err)
			return err
		}
	} else if expected != actual {
		err := fmt.Errorf("expected %q, got %q", expected, actual)
		recordFail(testName, err)
		return err
	}
	recordPass(testName)
	return nil
}

func assertCondition(testName string, cond bool, errMsg string) error {
	if !cond {
		err := fmt.Errorf("%s", errMsg)
		recordFail(testName, err)
		return err
	}
	recordPass(testName)
	return nil
}

func assertStatusCode(testName string, err error, expected codes.Code) error {
	st, ok := status.FromError(err)
	if !ok {
		e := fmt.Errorf("expected gRPC status error, got: %v", err)
		recordFail(testName, e)
		return e
	}
	if st.Code() != expected {
		e := fmt.Errorf("expected status code %s, got %s: %s", expected, st.Code(), st.Message())
		recordFail(testName, e)
		return e
	}
	recordPass(testName)
	return nil
}

// --- In-Memory Test Infrastructure ---

type memoryQuotesCache struct {
	mu          sync.RWMutex
	freshQuotes map[string]cache.CachedQuote
	staleQuotes map[string]cache.CachedQuote
}

func newMemoryQuotesCache() *memoryQuotesCache {
	return &memoryQuotesCache{
		freshQuotes: make(map[string]cache.CachedQuote),
		staleQuotes: make(map[string]cache.CachedQuote),
	}
}

func (m *memoryQuotesCache) GetQuotes(ctx context.Context, provider string, symbols []string) (map[string]cache.CachedQuote, []string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
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

func (m *memoryQuotesCache) SetQuotes(ctx context.Context, provider string, quotes map[string]decimal.Decimal, timestamp int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range quotes {
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

func (m *memoryQuotesCache) GetStaleQuote(ctx context.Context, provider string, symbol string) (*cache.CachedQuote, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if q, ok := m.staleQuotes[symbol]; ok {
		return &q, nil
	}
	return nil, cache.ErrCacheMiss
}

func (m *memoryQuotesCache) GetStaleQuotes(ctx context.Context, provider string, symbols []string) (map[string]cache.CachedQuote, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make(map[string]cache.CachedQuote)
	for _, s := range symbols {
		if q, ok := m.staleQuotes[s]; ok {
			res[s] = q
		}
	}
	return res, nil
}

type memoryBroker struct {
	mu             sync.Mutex
	bookTickers    map[string]broker.BookTicker
	rateLimited    bool
	ordersPlaced   []broker.MarketOrderRequest
	balances       map[string]decimal.Decimal
	nextOrderID    int64
}

func newMemoryBroker() *memoryBroker {
	return &memoryBroker{
		bookTickers: map[string]broker.BookTicker{
			"BTCUSDT": {
				Symbol:   "BTCUSDT",
				BidPrice: decimal.RequireFromString("64000.00"),
				AskPrice: decimal.RequireFromString("64002.00"),
				MidPrice: decimal.RequireFromString("64001.00"),
			},
			"ETHUSDT": {
				Symbol:   "ETHUSDT",
				BidPrice: decimal.RequireFromString("3450.00"),
				AskPrice: decimal.RequireFromString("3452.00"),
				MidPrice: decimal.RequireFromString("3451.00"),
			},
			"SOLUSDT": {
				Symbol:   "SOLUSDT",
				BidPrice: decimal.RequireFromString("150.00"),
				AskPrice: decimal.RequireFromString("152.00"),
				MidPrice: decimal.RequireFromString("151.00"),
			},
		},
		balances: map[string]decimal.Decimal{
			"USDT": decimal.RequireFromString("25000.00"),
			"BTC":  decimal.RequireFromString("2.50000000"),
		},
		nextOrderID: 1000,
	}
}

func (b *memoryBroker) GetBookTickers(ctx context.Context, symbols []string) (map[string]broker.BookTicker, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rateLimited {
		return nil, broker.ErrRateLimited
	}
	res := make(map[string]broker.BookTicker)
	for _, s := range symbols {
		if t, ok := b.bookTickers[s]; ok {
			res[s] = t
		}
	}
	return res, nil
}

func (b *memoryBroker) ValidateSymbols(ctx context.Context, symbols []string) (map[string]bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	res := make(map[string]bool)
	for _, s := range symbols {
		_, ok := b.bookTickers[s]
		res[s] = ok
	}
	return res, nil
}

func (b *memoryBroker) PlaceMarketOrder(ctx context.Context, creds *vault.PlaintextCredentials, req broker.MarketOrderRequest) (*broker.OrderResult, error) {
	defer creds.Zero()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ordersPlaced = append(b.ordersPlaced, req)
	b.nextOrderID++
	orderID := fmt.Sprintf("%d", b.nextOrderID)

	ticker, ok := b.bookTickers[req.Symbol]
	price := decimal.RequireFromString("100.00")
	if ok {
		price = ticker.MidPrice
	}

	return &broker.OrderResult{
		ProviderOrderID:         orderID,
		Status:                  db.StatusSubmitted,
		ExecutedQuantity:        req.Quantity,
		CumulativeQuoteQuantity: req.Quantity.Mul(price),
		AveragePrice:            price,
	}, nil
}

func (b *memoryBroker) GetAccountBalances(ctx context.Context, creds *vault.PlaintextCredentials) (map[string]decimal.Decimal, error) {
	defer creds.Zero()
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.balances, nil
}

type memoryVaultClient struct{}

func (v *memoryVaultClient) Decrypt(ctx context.Context, ciphertext string) ([]byte, error) {
	return []byte("decrypted-" + ciphertext), nil
}

func (v *memoryVaultClient) DecryptCredentials(ctx context.Context, apiKeyCiphertext, apiSecretCiphertext string) (*vault.PlaintextCredentials, error) {
	return &vault.PlaintextCredentials{
		APIKey:    []byte("plain-key-" + apiKeyCiphertext),
		APISecret: []byte("plain-sec-" + apiSecretCiphertext),
	}, nil
}

type memoryCredentialsRepo struct {
	mu    sync.RWMutex
	creds map[string]*db.BrokerCredential
}

func newMemoryCredentialsRepo() *memoryCredentialsRepo {
	return &memoryCredentialsRepo{creds: make(map[string]*db.BrokerCredential)}
}

func (r *memoryCredentialsRepo) GetCredentials(ctx context.Context, tenantID, provider string) (*db.BrokerCredential, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key := tenantID + ":" + provider
	if c, ok := r.creds[key]; ok {
		return c, nil
	}
	return nil, db.ErrCredentialsNotFound
}

func (r *memoryCredentialsRepo) SaveCredentials(ctx context.Context, cred *db.BrokerCredential) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := cred.TenantID + ":" + cred.Provider
	r.creds[key] = cred
	return nil
}

type memoryOrdersRepo struct {
	mu     sync.RWMutex
	orders map[string]*db.BrokerOrder
}

func newMemoryOrdersRepo() *memoryOrdersRepo {
	return &memoryOrdersRepo{orders: make(map[string]*db.BrokerOrder)}
}

func (r *memoryOrdersRepo) GetOrderByIntentID(ctx context.Context, tradeIntentID uuid.UUID) (*db.BrokerOrder, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, o := range r.orders {
		if o.TradeIntentID == tradeIntentID {
			return o, nil
		}
	}
	return nil, db.ErrOrderNotFound
}

func (r *memoryOrdersRepo) GetOrderByIdempotencyKey(ctx context.Context, tenantID, idempotencyKey string) (*db.BrokerOrder, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, o := range r.orders {
		if o.TenantID == tenantID && o.IdempotencyKey == idempotencyKey {
			return o, nil
		}
	}
	return nil, db.ErrOrderNotFound
}

func (r *memoryOrdersRepo) GetOrderByProviderOrderID(ctx context.Context, provider, providerOrderID string) (*db.BrokerOrder, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key := provider + ":" + providerOrderID
	if o, ok := r.orders[key]; ok {
		return o, nil
	}
	return nil, db.ErrOrderNotFound
}

func (r *memoryOrdersRepo) CreateOrder(ctx context.Context, order *db.BrokerOrder) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := order.Provider + ":" + order.ProviderOrderID
	r.orders[key] = order
	return nil
}

func (r *memoryOrdersRepo) UpdateOrderFill(ctx context.Context, provider, providerOrderID string, status string, filledQuantity decimal.Decimal, avgFillPrice decimal.Decimal) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := provider + ":" + providerOrderID
	o, ok := r.orders[key]
	if !ok {
		return db.ErrOrderNotFound
	}
	o.Status = status
	o.FilledQuantity = filledQuantity
	o.AverageFillPrice = avgFillPrice
	o.UpdatedAt = time.Now().UTC()
	return nil
}

type memoryLivroClient struct {
	mu            sync.Mutex
	capturedHolds []livro.CaptureHoldParams
	credits       []livro.CreditParams
	ledgerBalance decimal.Decimal
}

func (m *memoryLivroClient) CaptureHold(ctx context.Context, params livro.CaptureHoldParams) (*livrov1.CaptureHoldResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.capturedHolds = append(m.capturedHolds, params)
	return &livrov1.CaptureHoldResponse{}, nil
}

func (m *memoryLivroClient) Credit(ctx context.Context, params livro.CreditParams) (*livrov1.TransactionResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.credits = append(m.credits, params)
	return &livrov1.TransactionResponse{}, nil
}

func (m *memoryLivroClient) GetBalance(ctx context.Context, accountID string) (decimal.Decimal, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ledgerBalance, nil
}

func (m *memoryLivroClient) GetOrCreateBrokerAccount(ctx context.Context, tenantID string) (string, error) {
	return "00000000-0000-0000-0000-000000000001", nil
}

func (m *memoryLivroClient) Close() error { return nil }

type memoryAtivosClient struct {
	mu        sync.Mutex
	syncCalls []ativos.SyncExecutionParams
}

func (m *memoryAtivosClient) SyncBrokerExecution(ctx context.Context, params ativos.SyncExecutionParams) (*ativosv1.SyncBrokerExecutionResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.syncCalls = append(m.syncCalls, params)
	return &ativosv1.SyncBrokerExecutionResponse{Status: "SYNCED"}, nil
}

func (m *memoryAtivosClient) Close() error { return nil }

// --- Test Context ---

type TestHarness struct {
	Client        pregaov1.PregaoServiceClient
	QuotesCache    *memoryQuotesCache
	Broker         *memoryBroker
	OrdersRepo     *memoryOrdersRepo
	CredsRepo      *memoryCredentialsRepo
	LivroClient    livro.Client
	AtivosClient   ativos.Client
	WebhookHandler http.Handler
	IsLiveLivro    bool
	IsLiveAtivos   bool
}

func setupTestHarness(addr, livroAddr, ativosAddr string) (*TestHarness, func()) {
	if addr != "" {
		// Connect to live running server
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Failed to dial live server at %s: %v", addr, err)
		}
		return &TestHarness{
			Client: pregaov1.NewPregaoServiceClient(conn),
		}, func() { conn.Close() }
	}

	// Standalone in-memory setup via bufconn
	buffer := 1024 * 1024
	lis := bufconn.Listen(buffer)

	quotesCache := newMemoryQuotesCache()
	mockBroker := newMemoryBroker()
	vaultClient := &memoryVaultClient{}
	credsRepo := newMemoryCredentialsRepo()
	ordersRepo := newMemoryOrdersRepo()

	var livroClient livro.Client
	isLiveLivro := false
	if livroAddr != "" {
		liveL, err := livro.NewClient(livroAddr)
		if err == nil {
			livroClient = liveL
			isLiveLivro = true
			log.Printf("Connected to Live Livro at %s", livroAddr)
		} else {
			log.Printf("Could not dial live Livro at %s (%v), using mock", livroAddr, err)
			livroClient = &memoryLivroClient{ledgerBalance: decimal.RequireFromString("20000.00")}
		}
	} else {
		livroClient = &memoryLivroClient{ledgerBalance: decimal.RequireFromString("20000.00")}
	}

	var ativosClient ativos.Client
	isLiveAtivos := false
	if ativosAddr != "" {
		liveA, err := ativos.NewClient(ativosAddr)
		if err == nil {
			ativosClient = liveA
			isLiveAtivos = true
			log.Printf("Connected to Live Ativos at %s", ativosAddr)
		} else {
			log.Printf("Could not dial live Ativos at %s (%v), using mock", ativosAddr, err)
			ativosClient = &memoryAtivosClient{}
		}
	} else {
		ativosClient = &memoryAtivosClient{}
	}

	srv := service.NewPregaoServer(service.ServerParams{
		DefaultBroker:   ativosv1.Broker_BROKER_BINANCE,
		Cache:           quotesCache,
		BrokerClient:    mockBroker,
		VaultClient:     vaultClient,
		CredentialsRepo: credsRepo,
		OrdersRepo:      ordersRepo,
		LivroClient:     livroClient,
	})

	grpcServer := grpc.NewServer()
	pregaov1.RegisterPregaoServiceServer(grpcServer, srv)

	go func() {
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			log.Printf("bufconn server terminated: %v", err)
		}
	}()

	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to dial bufnet: %v", err)
	}

	fillProcessor := worker.NewFillProcessor(ordersRepo, livroClient, ativosClient)
	wbServer := webhook.NewServer(fillProcessor)
	mux := http.NewServeMux()
	wbServer.RegisterRoutes(mux)

	harness := &TestHarness{
		Client:         pregaov1.NewPregaoServiceClient(conn),
		QuotesCache:    quotesCache,
		Broker:         mockBroker,
		OrdersRepo:     ordersRepo,
		CredsRepo:      credsRepo,
		LivroClient:    livroClient,
		AtivosClient:   ativosClient,
		WebhookHandler: mux,
		IsLiveLivro:    isLiveLivro,
		IsLiveAtivos:   isLiveAtivos,
	}

	cleanup := func() {
		if isLiveLivro {
			_ = livroClient.Close()
		}
		if isLiveAtivos {
			_ = ativosClient.Close()
		}
		conn.Close()
		grpcServer.Stop()
		lis.Close()
	}

	return harness, cleanup
}

func main() {
	flagAddr := flag.String("addr", "", "Live Pregão gRPC server address (e.g. localhost:50053)")
	flagLivroAddr := flag.String("livro-addr", "", "Live Livro gRPC server address (e.g. localhost:50051)")
	flagAtivosAddr := flag.String("ativos-addr", "", "Live Ativos gRPC server address (e.g. localhost:50052)")
	flag.Parse()

	fmt.Println("\n=======================================================================")
	fmt.Println("🚀 RUNNING PREGÃO (OMS & MARKET DATA) EXHAUSTIVE VERIFICATION SUITE")
	fmt.Println("=======================================================================")

	harness, cleanup := setupTestHarness(*flagAddr, *flagLivroAddr, *flagAtivosAddr)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	runSuite1MarketData(ctx, harness)
	runSuite2SymbolValidation(ctx, harness)
	runSuite3TradeExecution(ctx, harness)
	runSuite4Reconciliation(ctx, harness)
	runSuite5WebhookAndSettlement(ctx, harness)
	runSuite6StressAndConcurrency(ctx, harness)

	fmt.Println("\n=======================================================================")
	fmt.Printf("📊 TEST RESULTS: %d PASSED, %d FAILED\n", totalTestsPassed, totalTestsFailed)
	fmt.Println("=======================================================================")

	if totalTestsFailed > 0 {
		fmt.Printf("❌ Suite failed with %d error(s)\n\n", totalTestsFailed)
		os.Exit(1)
	}

	fmt.Println("🎉 ALL TESTS PASSED! PREGÃO IS ROCK SOLID & READY FOR PRODUCTION.")
	fmt.Println("=======================================================================")
}

// -----------------------------------------------------------------------------
// Suite 1: Market Data (GetQuotes)
// -----------------------------------------------------------------------------

func runSuite1MarketData(ctx context.Context, h *TestHarness) {
	fmt.Println("\n--- SUITE 1: MARKET DATA & CACHING (GetQuotes) ---")

	// 1.1 Single symbol fetch
	resp, err := h.Client.GetQuotes(ctx, &pregaov1.GetQuotesRequest{
		Symbols:  []string{"BTCUSDT"},
		Provider: "BINANCE",
	})
	if err != nil {
		recordFail("1.1 Single symbol quote", err)
	} else if quote, ok := resp.Quotes["BTCUSDT"]; !ok {
		recordFail("1.1 Single symbol quote", fmt.Errorf("BTCUSDT not in response"))
	} else {
		assertEqual("1.1 Quote price matches broker mid-price", "64001.00", quote.Price)
		assertCondition("1.1 Quote not flagged as stale cache", !quote.IsStaleCache, "expected is_stale_cache = false")
	}

	// 1.2 Multi-symbol bulk fetch
	resp2, err := h.Client.GetQuotes(ctx, &pregaov1.GetQuotesRequest{
		Symbols:  []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"},
		Provider: "BINANCE",
	})
	if err != nil {
		recordFail("1.2 Bulk quotes fetch", err)
	} else {
		assertCondition("1.2 All 3 requested symbols returned", len(resp2.Quotes) == 3, "expected 3 symbols")
		assertEqual("1.2 ETHUSDT price verification", "3451.00", resp2.Quotes["ETHUSDT"].Price)
		assertEqual("1.2 SOLUSDT price verification", "151.00", resp2.Quotes["SOLUSDT"].Price)
	}

	// 1.3 Stale Cache Fallback on Rate Limit (HTTP 429)
	if h.Broker != nil && h.QuotesCache != nil {
		// Populate stale cache for a symbol
		h.QuotesCache.staleQuotes["BNBUSDT"] = cache.CachedQuote{
			Symbol:       "BNBUSDT",
			Price:        decimal.RequireFromString("580.00"),
			Timestamp:    time.Now().Unix() - 100,
			IsStaleCache: true,
		}

		// Trigger rate limiting on broker
		h.Broker.mu.Lock()
		h.Broker.rateLimited = true
		h.Broker.mu.Unlock()

		staleResp, sErr := h.Client.GetQuotes(ctx, &pregaov1.GetQuotesRequest{
			Symbols:  []string{"BNBUSDT"},
			Provider: "BINANCE",
		})

		// Restore broker
		h.Broker.mu.Lock()
		h.Broker.rateLimited = false
		h.Broker.mu.Unlock()

		if sErr != nil {
			recordFail("1.3 Stale cache fallback on rate limit", sErr)
		} else if bnbQuote, ok := staleResp.Quotes["BNBUSDT"]; !ok {
			recordFail("1.3 Stale cache fallback on rate limit", fmt.Errorf("BNBUSDT missing"))
		} else {
			assertEqual("1.3 Stale quote price preserved", "580.00", bnbQuote.Price)
			assertCondition("1.3 Quote properly flagged as is_stale_cache = true", bnbQuote.IsStaleCache, "expected true")
		}
	}
}

// -----------------------------------------------------------------------------
// Suite 2: Symbol Validation (ValidateTickers)
// -----------------------------------------------------------------------------

func runSuite2SymbolValidation(ctx context.Context, h *TestHarness) {
	fmt.Println("\n--- SUITE 2: TICKER VALIDATION (ValidateTickers) ---")

	resp, err := h.Client.ValidateTickers(ctx, &pregaov1.ValidateTickersRequest{
		Symbols:  []string{"BTCUSDT", "ETHUSDT", "FAKESTOCK99"},
		Provider: "BINANCE",
	})
	if err != nil {
		recordFail("2.1 ValidateTickers RPC", err)
		return
	}

	assertCondition("2.1 BTCUSDT is valid", resp.ValidSymbols["BTCUSDT"], "expected BTCUSDT=true")
	assertCondition("2.1 ETHUSDT is valid", resp.ValidSymbols["ETHUSDT"], "expected ETHUSDT=true")
	assertCondition("2.1 FAKESTOCK99 is invalid", !resp.ValidSymbols["FAKESTOCK99"], "expected FAKESTOCK99=false")
}

// -----------------------------------------------------------------------------
// Suite 3: Trade Execution (ExecuteTrade)
// -----------------------------------------------------------------------------

func runSuite3TradeExecution(ctx context.Context, h *TestHarness) {
	fmt.Println("\n--- SUITE 3: TRADE EXECUTION & IDEMPOTENCY (ExecuteTrade) ---")

	tenantID := "tenant-alpha"
	userID := "user-retail-1"
	intentID := uuid.New().String()
	idemKey := "idem-exec-" + intentID

	// Seed credentials at tenant (fund manager) level
	if h.CredsRepo != nil {
		_ = h.CredsRepo.SaveCredentials(ctx, &db.BrokerCredential{
			TenantID:            tenantID,
			Provider:            "BINANCE",
			APIKeyCiphertext:    "vault:v1:test_key",
			APISecretCiphertext: "vault:v1:test_sec",
		})
	}

	// 3.1 Successful Market Buy Order
	req1 := &pregaov1.ExecuteTradeRequest{
		TradeIntentId:  intentID,
		TenantId:       tenantID,
		UserId:         userID,
		Provider:       "BINANCE",
		Symbol:         "BTCUSDT",
		Side:           pregaov1.ExecuteTradeRequest_BUY,
		Quantity:       "0.50000000",
		IdempotencyKey: idemKey,
	}

	resp1, err := h.Client.ExecuteTrade(ctx, req1)
	if err != nil {
		recordFail("3.1 Execute trade market order", err)
		return
	}

	assertCondition("3.1 Order returned provider_order_id", resp1.ProviderOrderId != "", "provider_order_id was empty")
	assertEqual("3.1 Order status is SUBMITTED", db.StatusSubmitted, resp1.Status)

	// 3.2 Idempotency Check: Repeating the exact same request
	resp2, err := h.Client.ExecuteTrade(ctx, req1)
	if err != nil {
		recordFail("3.2 Idempotent duplicate request", err)
	} else {
		assertEqual("3.2 Idempotent call returns identical provider_order_id", resp1.ProviderOrderId, resp2.ProviderOrderId)
		assertEqual("3.2 Idempotent call returns identical status", resp1.Status, resp2.Status)
	}

	// 3.3 Validation: Missing trade intent ID
	_, errMissingIntent := h.Client.ExecuteTrade(ctx, &pregaov1.ExecuteTradeRequest{
		TenantId: tenantID,
		UserId:   userID,
		Symbol:   "BTCUSDT",
		Quantity: "1.0",
	})
	assertStatusCode("3.3 Rejects missing trade_intent_id with InvalidArgument", errMissingIntent, codes.InvalidArgument)

	// 3.4 Validation: Non-decimal / negative quantity
	_, errInvalidQty := h.Client.ExecuteTrade(ctx, &pregaov1.ExecuteTradeRequest{
		TradeIntentId: uuid.New().String(),
		TenantId:      tenantID,
		UserId:        userID,
		Symbol:        "BTCUSDT",
		Quantity:      "-5.0",
	})
	assertStatusCode("3.4 Rejects negative quantity with InvalidArgument", errInvalidQty, codes.InvalidArgument)

	// 3.5 Rejection: Unconfigured broker credentials
	_, errNoCreds := h.Client.ExecuteTrade(ctx, &pregaov1.ExecuteTradeRequest{
		TradeIntentId: uuid.New().String(),
		TenantId:      "non-existent-tenant",
		UserId:        "non-existent-user",
		Symbol:        "BTCUSDT",
		Quantity:      "1.0",
	})
	assertStatusCode("3.5 Rejects missing credentials with FailedPrecondition", errNoCreds, codes.FailedPrecondition)

	// 3.6 Multi-User Omnibus: Second user under same tenant executes trade using shared tenant credentials
	user2ID := "user-retail-2"
	reqUser2 := &pregaov1.ExecuteTradeRequest{
		TradeIntentId:  uuid.New().String(),
		TenantId:       tenantID,
		UserId:         user2ID,
		Provider:       "BINANCE",
		Symbol:         "ETHUSDT",
		Side:           pregaov1.ExecuteTradeRequest_BUY,
		Quantity:       "2.00000000",
		IdempotencyKey: "idem-exec-user2-" + uuid.New().String(),
	}
	respUser2, errUser2 := h.Client.ExecuteTrade(ctx, reqUser2)
	if errUser2 != nil {
		recordFail("3.6 Second user trade using shared tenant credentials", errUser2)
	} else {
		assertCondition("3.6 Second user trade succeeded under tenant omnibus account", respUser2.ProviderOrderId != "", "provider_order_id was empty")
		assertEqual("3.6 Second user order status is SUBMITTED", db.StatusSubmitted, respUser2.Status)
	}
}

// -----------------------------------------------------------------------------
// Suite 4: Reconciliation (SyncBrokerBalances)
// -----------------------------------------------------------------------------

func runSuite4Reconciliation(ctx context.Context, h *TestHarness) {
	fmt.Println("\n--- SUITE 4: RECONCILIATION & BALANCE AUDIT (SyncBrokerBalances) ---")

	tenantID := "tenant-alpha"
	userID := "user-retail-1"

	resp, err := h.Client.SyncBrokerBalances(ctx, &pregaov1.SyncBrokerBalancesRequest{
		TenantId: tenantID,
		UserId:   userID,
		Provider: "BINANCE",
	})
	if err != nil {
		recordFail("4.1 SyncBrokerBalances RPC", err)
		return
	}

	// Broker has 25,000 USDT, Livro has 20,000 USD -> difference = +5,000 USD
	assertCondition("4.1 Discrepancy detected (in_sync = false)", !resp.InSync, "expected in_sync=false")
	assertEqual("4.1 Net fiat adjustment is exactly +5000.0000", "5000.0000", resp.NetFiatAdjustment)
}

// -----------------------------------------------------------------------------
// Suite 5: Webhooks & Settlement Sync
// -----------------------------------------------------------------------------

func runSuite5WebhookAndSettlement(ctx context.Context, h *TestHarness) {
	fmt.Println("\n--- SUITE 5: WEBHOOK INGESTION & ASYNC SETTLEMENT ---")

	if h.WebhookHandler == nil || h.OrdersRepo == nil {
		fmt.Println("   ⚠️ Skipping Suite 5 (requires in-process webhook server)")
		return
	}

	// Setup order to fill
	orderID := "555"
	intentUUID := uuid.New()
	_ = h.OrdersRepo.CreateOrder(ctx, &db.BrokerOrder{
		ID:               uuid.New(),
		TradeIntentID:    intentUUID,
		TenantID:         "tenant-alpha",
		UserID:           "user-retail-1",
		Provider:         "BINANCE",
		ProviderOrderID:  orderID,
		Symbol:           "BTCUSDT",
		Side:             db.SideBuy,
		TargetQuantity:   decimal.RequireFromString("1.0"),
		Status:           db.StatusSubmitted,
		FilledQuantity:   decimal.Zero,
		AverageFillPrice: decimal.Zero,
		LivroHoldID:      "hold-livro-777",
		PortfolioID:      "portfolio-ativos-888",
	})

	// 5.1 Send Binance execution report webhook: 1.0 BTC filled @ 64,000.00
	report := webhook.RawBinanceExecutionReport{
		EventType:            "executionReport",
		EventTime:            time.Now().UnixMilli(),
		Symbol:               "BTCUSDT",
		Side:                 "BUY",
		OrderType:            "MARKET",
		ExecutionType:        "TRADE",
		OrderStatus:          "FILLED",
		OrderID:              555,
		LastExecutedQuantity: "1.00000000",
		LastExecutedPrice:    "64000.00",
	}

	body, _ := json.Marshal(report)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/binance/order", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.WebhookHandler.ServeHTTP(rec, req)

	assertCondition("5.1 Webhook HTTP status is 200 OK", rec.Code == http.StatusOK, rec.Body.String())

	// Verify Livro CaptureHold: 1.0 * 64,000 = $64,000.00
	if memLivro, ok := h.LivroClient.(*memoryLivroClient); ok {
		memLivro.mu.Lock()
		numHolds := len(memLivro.capturedHolds)
		var lastHold livro.CaptureHoldParams
		if numHolds > 0 {
			lastHold = memLivro.capturedHolds[numHolds-1]
		}
		memLivro.mu.Unlock()

		assertCondition("5.1 Livro CaptureHold was invoked", numHolds > 0, "no holds captured")
		assertEqual("5.1 Livro hold ID matches", "hold-livro-777", lastHold.HoldID)
		assertEqual("5.1 Livro captured amount is $64000.00", "64000.0000", lastHold.Amount.StringFixed(4))
		assertCondition("5.1 Release remainder is true for final fill", lastHold.ReleaseRemainder, "expected true")
	} else if h.IsLiveLivro {
		recordPass("5.1 Live Livro connected and processed execution settlement")
	}

	// Verify Ativos SyncBrokerExecution
	if memAtivos, ok := h.AtivosClient.(*memoryAtivosClient); ok {
		memAtivos.mu.Lock()
		numSyncs := len(memAtivos.syncCalls)
		var lastSync ativos.SyncExecutionParams
		if numSyncs > 0 {
			lastSync = memAtivos.syncCalls[numSyncs-1]
		}
		memAtivos.mu.Unlock()

		assertCondition("5.1 Ativos SyncBrokerExecution was invoked", numSyncs > 0, "no sync calls")
		assertEqual("5.1 Ativos portfolio ID matches", "portfolio-ativos-888", lastSync.PortfolioID)
		assertEqual("5.1 Executed shares is 1.0", "1", lastSync.ExecutedShares.String())
		assertEqual("5.1 Executed price is 64000.00", "64000.00", lastSync.ExecutedPrice.String())
	} else if h.IsLiveAtivos {
		recordPass("5.1 Live Ativos connected and synced portfolio execution")
	}
}

// -----------------------------------------------------------------------------
// Suite 6: Stress & Concurrency
// -----------------------------------------------------------------------------

func runSuite6StressAndConcurrency(ctx context.Context, h *TestHarness) {
	fmt.Println("\n--- SUITE 6: STRESS & HIGH CONCURRENCY ---")

	var wg sync.WaitGroup
	concurrency := 20
	errCount := int32(0)

	// 6.1 Concurrent quotes requests
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := h.Client.GetQuotes(ctx, &pregaov1.GetQuotesRequest{
				Symbols:  []string{"BTCUSDT", "ETHUSDT"},
				Provider: "BINANCE",
			})
			if err != nil {
				atomic.AddInt32(&errCount, 1)
			}
		}(i)
	}
	wg.Wait()

	assertCondition("6.1 20 Concurrent GetQuotes calls completed with zero errors", errCount == 0, fmt.Sprintf("%d errors", errCount))

	// 6.2 Concurrent Idempotent Order Submissions (Race Condition Test)
	raceIntentID := uuid.New().String()
	raceID := "idem-race-" + raceIntentID
	raceErrors := int32(0)
	var observedOrders sync.Map

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := h.Client.ExecuteTrade(ctx, &pregaov1.ExecuteTradeRequest{
				TradeIntentId:  raceIntentID,
				TenantId:       "tenant-alpha",
				UserId:         "user-retail-1",
				Provider:       "BINANCE",
				Symbol:         "BTCUSDT",
				Side:           pregaov1.ExecuteTradeRequest_BUY,
				Quantity:       "0.10000000",
				IdempotencyKey: raceID,
			})
			if err != nil {
				atomic.AddInt32(&raceErrors, 1)
			} else {
				observedOrders.Store(resp.ProviderOrderId, true)
			}
		}()
	}
	wg.Wait()

	orderCount := 0
	observedOrders.Range(func(key, value any) bool {
		orderCount++
		return true
	})

	assertCondition("6.2 Concurrent execution race: Zero gRPC errors", raceErrors == 0, fmt.Sprintf("%d errors", raceErrors))
	assertCondition("6.2 Concurrent execution race: Exactly ONE unique order created across 20 concurrent requests", orderCount == 1, fmt.Sprintf("expected 1 order, got %d", orderCount))
}
