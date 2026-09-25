package service

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	pregaov1 "github.com/akhiljames/proto/gen/go/pregao/v1"
	"github.com/akhiljames/pregao/internal/broker"
	"github.com/akhiljames/pregao/internal/cache"
	"github.com/akhiljames/pregao/internal/client/livro"
	"github.com/akhiljames/pregao/internal/db"
	"github.com/akhiljames/pregao/internal/vault"
	"github.com/akhiljames/pregao/internal/worker"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PregaoServer implements pregaov1.PregaoServiceServer.
type PregaoServer struct {
	pregaov1.UnimplementedPregaoServiceServer

	defaultProvider string
	cache           cache.QuotesCache
	brokerClient    broker.Broker
	vaultClient     vault.TransitClient
	credentialsRepo db.CredentialsRepository
	ordersRepo      db.OrdersRepository
	livroClient     livro.Client
	fillProcessor   worker.FillProcessor
	execLocks       sync.Map
}

func (s *PregaoServer) getExecLock(key string) *sync.Mutex {
	val, _ := s.execLocks.LoadOrStore(key, &sync.Mutex{})
	return val.(*sync.Mutex)
}

// ServerParams encapsulates dependencies required to instantiate PregaoServer.
type ServerParams struct {
	DefaultProvider string
	Cache           cache.QuotesCache
	BrokerClient    broker.Broker
	VaultClient     vault.TransitClient
	CredentialsRepo db.CredentialsRepository
	OrdersRepo      db.OrdersRepository
	LivroClient     livro.Client
	FillProcessor   worker.FillProcessor
}

// NewPregaoServer returns an initialized PregaoServer.
func NewPregaoServer(params ServerParams) *PregaoServer {
	if params.DefaultProvider == "" {
		params.DefaultProvider = "BINANCE"
	}
	return &PregaoServer{
		defaultProvider: params.DefaultProvider,
		cache:           params.Cache,
		brokerClient:    params.BrokerClient,
		vaultClient:     params.VaultClient,
		credentialsRepo: params.CredentialsRepo,
		ordersRepo:      params.OrdersRepo,
		livroClient:     params.LivroClient,
		fillProcessor:   params.FillProcessor,
	}
}

// -----------------------------------------------------------------------------
// Workflow A: Market Data (GetQuotes & ValidateTickers)
// -----------------------------------------------------------------------------

func (s *PregaoServer) GetQuotes(ctx context.Context, req *pregaov1.GetQuotesRequest) (*pregaov1.GetQuotesResponse, error) {
	if len(req.Symbols) == 0 {
		return &pregaov1.GetQuotesResponse{Quotes: make(map[string]*pregaov1.Quote)}, nil
	}

	provider := strings.TrimSpace(req.Provider)
	if provider == "" {
		provider = s.defaultProvider
	}
	provider = strings.ToUpper(provider)

	// Clean and deduplicate symbols
	symbolSet := make(map[string]struct{}, len(req.Symbols))
	cleanSymbols := make([]string, 0, len(req.Symbols))
	for _, sym := range req.Symbols {
		sClean := strings.ToUpper(strings.TrimSpace(sym))
		if sClean == "" {
			continue
		}
		if _, exists := symbolSet[sClean]; !exists {
			symbolSet[sClean] = struct{}{}
			cleanSymbols = append(cleanSymbols, sClean)
		}
	}

	resultQuotes := make(map[string]*pregaov1.Quote, len(cleanSymbols))

	// Step 1: Bulk MGET from Redis cache (5s TTL)
	hits, misses, err := s.cache.GetQuotes(ctx, provider, cleanSymbols)
	if err != nil {
		log.Printf("[CACHE_WARN] Redis MGET failed: %v", err)
	}

	for sym, hit := range hits {
		resultQuotes[sym] = &pregaov1.Quote{
			Symbol:       hit.Symbol,
			Price:        hit.Price.String(),
			Timestamp:    hit.Timestamp,
			IsStaleCache: false,
		}
	}

	// Step 2: For cache misses, issue REST call to Binance bookTicker
	if len(misses) > 0 {
		tickers, brokerErr := s.brokerClient.GetBookTickers(ctx, misses)
		if brokerErr == nil {
			nowUnix := time.Now().Unix()
			quotesToCache := make(map[string]decimal.Decimal, len(tickers))

			for sym, ticker := range tickers {
				resultQuotes[sym] = &pregaov1.Quote{
					Symbol:       ticker.Symbol,
					Price:        ticker.MidPrice.String(),
					Timestamp:    nowUnix,
					IsStaleCache: false,
				}
				quotesToCache[sym] = ticker.MidPrice
			}

			// Save to Redis (5s fresh + 24h stale)
			if err := s.cache.SetQuotes(ctx, provider, quotesToCache, nowUnix); err != nil {
				log.Printf("[CACHE_WARN] Failed to write quotes to Redis: %v", err)
			}
		} else {
			log.Printf("[BROKER_WARN] Failed to fetch tickers from broker: %v. Initiating stale cache fallback.", brokerErr)
			// Step 3: Fallback Strategy - Retrieve 24h stale keys on broker error / rate-limit
			staleQuotes, staleErr := s.cache.GetStaleQuotes(ctx, provider, misses)
			if staleErr == nil {
				for sym, sQuote := range staleQuotes {
					resultQuotes[sym] = &pregaov1.Quote{
						Symbol:       sQuote.Symbol,
						Price:        sQuote.Price.String(),
						Timestamp:    sQuote.Timestamp,
						IsStaleCache: true,
					}
				}
			}
		}
	}

	return &pregaov1.GetQuotesResponse{Quotes: resultQuotes}, nil
}

func (s *PregaoServer) ValidateTickers(ctx context.Context, req *pregaov1.ValidateTickersRequest) (*pregaov1.ValidateTickersResponse, error) {
	if len(req.Symbols) == 0 {
		return &pregaov1.ValidateTickersResponse{ValidSymbols: make(map[string]bool)}, nil
	}

	validMap, err := s.brokerClient.ValidateSymbols(ctx, req.Symbols)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "failed to validate tickers with broker: %v", err)
	}

	return &pregaov1.ValidateTickersResponse{ValidSymbols: validMap}, nil
}

// -----------------------------------------------------------------------------
// Workflow B: Trade Execution (ExecuteTrade)
// -----------------------------------------------------------------------------

func (s *PregaoServer) ExecuteTrade(ctx context.Context, req *pregaov1.ExecuteTradeRequest) (*pregaov1.ExecuteTradeResponse, error) {
	// Validate basic inputs
	if strings.TrimSpace(req.TradeIntentId) == "" {
		return nil, status.Error(codes.InvalidArgument, "trade_intent_id is required")
	}
	if strings.TrimSpace(req.TenantId) == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	if strings.TrimSpace(req.UserId) == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	if strings.TrimSpace(req.Symbol) == "" {
		return nil, status.Error(codes.InvalidArgument, "symbol is required")
	}
	if strings.TrimSpace(req.Quantity) == "" {
		return nil, status.Error(codes.InvalidArgument, "quantity is required")
	}

	tradeIntentUUID, err := uuid.Parse(req.TradeIntentId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid trade_intent_id UUID format: %v", err)
	}

	qty, err := decimal.NewFromString(req.Quantity)
	if err != nil || qty.LessThanOrEqual(decimal.Zero) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid quantity %q: must be a positive decimal", req.Quantity)
	}

	provider := strings.TrimSpace(req.Provider)
	if provider == "" {
		provider = s.defaultProvider
	}
	provider = strings.ToUpper(provider)

	// Serialize concurrent execution requests for the same trade intent or idempotency key
	lockKey := req.TenantId + ":" + req.TradeIntentId
	if req.IdempotencyKey != "" {
		lockKey = req.TenantId + ":" + req.IdempotencyKey
	}
	mu := s.getExecLock(lockKey)
	mu.Lock()
	defer mu.Unlock()

	// Step 1: Idempotency Check in PostgreSQL
	existingOrder, err := s.ordersRepo.GetOrderByIntentID(ctx, tradeIntentUUID)
	if err == nil && existingOrder != nil {
		log.Printf("[IDEMPOTENCY] Returning existing order %s for intent %s", existingOrder.ProviderOrderID, req.TradeIntentId)
		return &pregaov1.ExecuteTradeResponse{
			ProviderOrderId: existingOrder.ProviderOrderID,
			Status:          existingOrder.Status,
		}, nil
	}

	if req.IdempotencyKey != "" {
		existingByIdem, err := s.ordersRepo.GetOrderByIdempotencyKey(ctx, req.TenantId, req.IdempotencyKey)
		if err == nil && existingByIdem != nil {
			log.Printf("[IDEMPOTENCY] Returning existing order %s for idempotency key %s", existingByIdem.ProviderOrderID, req.IdempotencyKey)
			return &pregaov1.ExecuteTradeResponse{
				ProviderOrderId: existingByIdem.ProviderOrderID,
				Status:          existingByIdem.Status,
			}, nil
		}
	}

	// Step 2: Fetch ciphertext credentials from PostgreSQL
	cred, err := s.credentialsRepo.GetCredentials(ctx, req.TenantId, req.UserId, provider)
	if err != nil {
		if errors.Is(err, db.ErrCredentialsNotFound) {
			return nil, status.Errorf(codes.FailedPrecondition, "broker credentials not found for tenant %s, user %s, provider %s", req.TenantId, req.UserId, provider)
		}
		return nil, status.Errorf(codes.Internal, "database error loading broker credentials: %v", err)
	}

	// Step 3: Just-in-Time Decryption via OpenBao Transit API
	plaintextCreds, err := s.vaultClient.DecryptCredentials(ctx, cred.APIKeyCiphertext, cred.APISecretCiphertext)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cryptographic transit decryption failed: %v", err)
	}
	// Memory safety guarantee: Wipe plaintext byte buffers immediately upon function return
	defer plaintextCreds.Zero()

	// Step 4: Dispatch Market Order to Broker with HMAC-SHA256 signature
	sideStr := db.SideBuy
	if req.Side == pregaov1.ExecuteTradeRequest_SELL {
		sideStr = db.SideSell
	}

	orderResult, err := s.brokerClient.PlaceMarketOrder(ctx, plaintextCreds, broker.MarketOrderRequest{
		Symbol:        strings.ToUpper(req.Symbol),
		Side:          sideStr,
		Quantity:      qty,
		ClientOrderID: req.TradeIntentId,
	})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "broker rejected order execution: %v", err)
	}

	// Step 5: Persist execution state into PostgreSQL broker_orders
	orderStatus := db.StatusSubmitted
	if orderResult.Status != "" {
		orderStatus = orderResult.Status
	}

	newOrder := &db.BrokerOrder{
		ID:               uuid.New(),
		TradeIntentID:    tradeIntentUUID,
		TenantID:         req.TenantId,
		UserID:           req.UserId,
		Provider:         provider,
		ProviderOrderID:  orderResult.ProviderOrderID,
		Symbol:           strings.ToUpper(req.Symbol),
		Side:             sideStr,
		TargetQuantity:   qty,
		Status:           orderStatus,
		FilledQuantity:   orderResult.ExecutedQuantity,
		AverageFillPrice: orderResult.AveragePrice,
		IdempotencyKey:   req.IdempotencyKey,
		PortfolioID:      req.PortfolioId,
		LivroHoldID:      req.LivroHoldId,
	}

	if err := s.ordersRepo.CreateOrder(ctx, newOrder); err != nil {
		log.Printf("[DB_ERROR] Order executed on broker (%s) but failed to record in DB: %v", orderResult.ProviderOrderID, err)
		// We still return the provider_order_id so the caller can reconcile
	}

	// Step 6: If order is immediately filled by broker, trigger fillProcessor for Livro hold capture and Ativos VWAP sync
	if s.fillProcessor != nil && orderResult.ExecutedQuantity.IsPositive() {
		fillEvent := worker.FillEvent{
			Provider:         provider,
			ProviderOrderID:  orderResult.ProviderOrderID,
			Symbol:           strings.ToUpper(req.Symbol),
			Side:             sideStr,
			Status:           orderStatus,
			ExecutedQuantity: orderResult.ExecutedQuantity,
			FillPrice:        orderResult.AveragePrice,
		}
		if err := s.fillProcessor.ProcessFill(ctx, fillEvent); err != nil {
			log.Printf("[FILL_SYNC_WARN] Order %s filled on broker but fill settlement failed: %v", orderResult.ProviderOrderID, err)
		} else {
			log.Printf("[FILL_SYNC_SUCCESS] Order %s synchronously settled (Livro hold captured, Ativos VWAP updated)", orderResult.ProviderOrderID)
		}
	}

	return &pregaov1.ExecuteTradeResponse{
		ProviderOrderId: orderResult.ProviderOrderID,
		Status:          orderStatus,
	}, nil
}

// -----------------------------------------------------------------------------
// Reconciliation: SyncBrokerBalances
// -----------------------------------------------------------------------------

func (s *PregaoServer) SyncBrokerBalances(ctx context.Context, req *pregaov1.SyncBrokerBalancesRequest) (*pregaov1.SyncBrokerBalancesResponse, error) {
	if strings.TrimSpace(req.TenantId) == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	if strings.TrimSpace(req.UserId) == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}

	provider := strings.TrimSpace(req.Provider)
	if provider == "" {
		provider = s.defaultProvider
	}
	provider = strings.ToUpper(provider)

	// Fetch encrypted credentials
	cred, err := s.credentialsRepo.GetCredentials(ctx, req.TenantId, req.UserId, provider)
	if err != nil {
		if errors.Is(err, db.ErrCredentialsNotFound) {
			return nil, status.Errorf(codes.FailedPrecondition, "broker credentials not configured")
		}
		return nil, status.Errorf(codes.Internal, "database error: %v", err)
	}

	// Decrypt just-in-time
	plaintextCreds, err := s.vaultClient.DecryptCredentials(ctx, cred.APIKeyCiphertext, cred.APISecretCiphertext)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to decrypt broker credentials: %v", err)
	}
	defer plaintextCreds.Zero()

	// Query broker account balances
	balances, err := s.brokerClient.GetAccountBalances(ctx, plaintextCreds)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "failed to fetch broker balances: %v", err)
	}

	// Resolve fiat quote currency balance (e.g., USDT, USD, BUSD)
	brokerFiatBal := decimal.Zero
	for _, asset := range []string{"USDT", "USD", "BUSD", "USDC"} {
		if bal, ok := balances[asset]; ok && bal.GreaterThan(decimal.Zero) {
			brokerFiatBal = bal
			break
		}
	}

	// Query Livro ledger balance
	ledgerBal := decimal.Zero
	if s.livroClient != nil {
		lBal, err := s.livroClient.GetBalance(ctx, req.UserId)
		if err == nil {
			ledgerBal = lBal
		} else {
			log.Printf("[LIVRO_WARN] Failed to query ledger balance for user %s: %v", req.UserId, err)
		}
	}

	// Exact decimal adjustment: brokerFiatBal - ledgerBal
	netAdjustment := brokerFiatBal.Sub(ledgerBal)
	inSync := netAdjustment.IsZero()

	return &pregaov1.SyncBrokerBalancesResponse{
		InSync:            inSync,
		NetFiatAdjustment: netAdjustment.StringFixed(4),
	}, nil
}
