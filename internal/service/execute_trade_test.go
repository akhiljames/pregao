package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/akhiljames/pregao/internal/broker"
	"github.com/akhiljames/pregao/internal/db"
	pregaov1 "github.com/akhiljames/proto/gen/go/pregao/v1"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newTradeTestServer wires a PregaoServer with tenant-1 BINANCE credentials seeded.
func newTradeTestServer(t *testing.T, brokerMock *mockBroker) (*PregaoServer, *mockOrdersRepo) {
	t.Helper()
	ordersRepo := newMockOrdersRepo()
	credsRepo := newMockCredentialsRepo()
	require.NoError(t, credsRepo.SaveCredentials(context.Background(), &db.BrokerCredential{
		TenantID:            "tenant-1",
		Provider:            "BINANCE",
		APIKeyCiphertext:    "vault:v1:encrypted_key",
		APISecretCiphertext: "vault:v1:encrypted_secret",
	}))

	srv := NewPregaoServer(ServerParams{
		OrdersRepo:      ordersRepo,
		CredentialsRepo: credsRepo,
		VaultClient:     &mockVaultClient{decryptedKey: "k", decryptedSecret: "s"},
		BrokerClient:    brokerMock,
	})
	return srv, ordersRepo
}

func tradeRequest(intentID uuid.UUID, idempotencyKey string) *pregaov1.ExecuteTradeRequest {
	return &pregaov1.ExecuteTradeRequest{
		TradeIntentId:  intentID.String(),
		TenantId:       "tenant-1",
		UserId:         "user-1",
		Provider:       "BINANCE",
		Symbol:         "BTCUSDT",
		Side:           pregaov1.ExecuteTradeRequest_BUY,
		Quantity:       "0.5",
		IdempotencyKey: idempotencyKey,
	}
}

func TestPregaoServer_ExecuteTrade_BrokerFailure_ReleasesReservation(t *testing.T) {
	brokerMock := &mockBroker{marketOrderErr: errors.New("binance unavailable")}
	srv, ordersRepo := newTradeTestServer(t, brokerMock)
	intentID := uuid.New()

	_, err := srv.ExecuteTrade(context.Background(), tradeRequest(intentID, "idem-retry"))
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))

	_, err = ordersRepo.GetOrderByIntentID(context.Background(), intentID)
	assert.ErrorIs(t, err, db.ErrOrderNotFound, "failed dispatch must not leave a reservation behind")

	// The same intent can be retried once the broker recovers.
	brokerMock.marketOrderErr = nil
	brokerMock.marketOrderResp = &broker.OrderResult{ProviderOrderID: "binance-order-1", Status: db.StatusSubmitted}

	resp, err := srv.ExecuteTrade(context.Background(), tradeRequest(intentID, "idem-retry"))
	require.NoError(t, err)
	assert.Equal(t, "binance-order-1", resp.ProviderOrderId)
	assert.EqualValues(t, 2, brokerMock.orderCalls.Load())
}

func TestPregaoServer_ExecuteTrade_MissingCredentials_ReleasesReservation(t *testing.T) {
	brokerMock := &mockBroker{}
	srv, ordersRepo := newTradeTestServer(t, brokerMock)
	intentID := uuid.New()

	req := tradeRequest(intentID, "")
	req.TenantId = "tenant-unknown"
	_, err := srv.ExecuteTrade(context.Background(), req)
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = ordersRepo.GetOrderByIntentID(context.Background(), intentID)
	assert.ErrorIs(t, err, db.ErrOrderNotFound)
	assert.EqualValues(t, 0, brokerMock.orderCalls.Load())
}

// reservePending simulates another request that holds the reservation for intentID.
func reservePending(t *testing.T, ordersRepo *mockOrdersRepo, intentID uuid.UUID) *db.BrokerOrder {
	t.Helper()
	held := &db.BrokerOrder{
		ID:            uuid.New(),
		TradeIntentID: intentID,
		TenantID:      "tenant-1",
		Provider:      "BINANCE",
	}
	reserved, err := ordersRepo.ReserveOrder(context.Background(), held)
	require.NoError(t, err)
	require.True(t, reserved)
	return held
}

func TestPregaoServer_ExecuteTrade_InFlightDuplicate_WaitsForRecordedOrder(t *testing.T) {
	brokerMock := &mockBroker{
		marketOrderResp: &broker.OrderResult{ProviderOrderID: "should-not-be-placed", Status: db.StatusSubmitted},
	}
	srv, ordersRepo := newTradeTestServer(t, brokerMock)
	srv.reservationPoll = time.Millisecond
	intentID := uuid.New()
	held := reservePending(t, ordersRepo, intentID)

	type result struct {
		resp *pregaov1.ExecuteTradeResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := srv.ExecuteTrade(context.Background(), tradeRequest(intentID, "idem-other"))
		done <- result{resp, err}
	}()

	select {
	case <-done:
		t.Fatal("duplicate returned while the first request was still in flight")
	case <-time.After(20 * time.Millisecond):
	}

	// The first request hears back from the broker and records its order.
	require.NoError(t, ordersRepo.ConfirmOrder(context.Background(), held.ID, "binance-order-1", db.StatusSubmitted, decimal.Zero, decimal.Zero))

	res := <-done
	require.NoError(t, res.err)
	assert.Equal(t, "binance-order-1", res.resp.ProviderOrderId)
	assert.Equal(t, db.StatusSubmitted, res.resp.Status)
	assert.EqualValues(t, 0, brokerMock.orderCalls.Load())
}

func TestPregaoServer_ExecuteTrade_InFlightDuplicate_TakesOverReleasedReservation(t *testing.T) {
	brokerMock := &mockBroker{
		marketOrderResp: &broker.OrderResult{ProviderOrderID: "binance-order-2", Status: db.StatusSubmitted},
	}
	srv, ordersRepo := newTradeTestServer(t, brokerMock)
	srv.reservationPoll = time.Millisecond
	intentID := uuid.New()
	held := reservePending(t, ordersRepo, intentID)

	done := make(chan error, 1)
	go func() {
		_, err := srv.ExecuteTrade(context.Background(), tradeRequest(intentID, "idem-other"))
		done <- err
	}()

	// The first request fails before reaching the broker and releases its reservation.
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, ordersRepo.ReleaseOrder(context.Background(), held.ID))

	require.NoError(t, <-done)
	assert.EqualValues(t, 1, brokerMock.orderCalls.Load())
	saved, err := ordersRepo.GetOrderByIntentID(context.Background(), intentID)
	require.NoError(t, err)
	assert.Equal(t, "binance-order-2", saved.ProviderOrderID)
}

func TestPregaoServer_ExecuteTrade_StuckReservation_IsAborted(t *testing.T) {
	brokerMock := &mockBroker{
		marketOrderResp: &broker.OrderResult{ProviderOrderID: "should-not-be-placed", Status: db.StatusSubmitted},
	}
	srv, ordersRepo := newTradeTestServer(t, brokerMock)
	srv.reservationWait = 20 * time.Millisecond
	srv.reservationPoll = time.Millisecond
	intentID := uuid.New()
	reservePending(t, ordersRepo, intentID)

	_, err := srv.ExecuteTrade(context.Background(), tradeRequest(intentID, "idem-other"))
	require.Error(t, err)
	assert.Equal(t, codes.Aborted, status.Code(err))
	assert.EqualValues(t, 0, brokerMock.orderCalls.Load())

	// The stuck reservation is left untouched.
	existing, err := ordersRepo.GetOrderByIntentID(context.Background(), intentID)
	require.NoError(t, err)
	assert.Equal(t, db.StatusPending, existing.Status)
}

func TestPregaoServer_ExecuteTrade_SameIdempotencyKey_ReturnsExistingOrder(t *testing.T) {
	brokerMock := &mockBroker{
		marketOrderResp: &broker.OrderResult{ProviderOrderID: "binance-order-1", Status: db.StatusSubmitted},
	}
	srv, _ := newTradeTestServer(t, brokerMock)

	first, err := srv.ExecuteTrade(context.Background(), tradeRequest(uuid.New(), "idem-shared"))
	require.NoError(t, err)

	// A different intent reusing the tenant's idempotency key replays the first order.
	second, err := srv.ExecuteTrade(context.Background(), tradeRequest(uuid.New(), "idem-shared"))
	require.NoError(t, err)
	assert.Equal(t, first.ProviderOrderId, second.ProviderOrderId)
	assert.EqualValues(t, 1, brokerMock.orderCalls.Load())
}

func TestPregaoServer_ExecuteTrade_ConcurrentSameIntent_PlacesOneOrder(t *testing.T) {
	brokerMock := &mockBroker{
		// Slow broker: the other callers arrive while the winner's reservation is still PENDING.
		orderDelay: 30 * time.Millisecond,
		marketOrderResp: &broker.OrderResult{
			ProviderOrderID:  "binance-order-1",
			Status:           db.StatusFilled,
			ExecutedQuantity: decimal.RequireFromString("0.5"),
			AveragePrice:     decimal.RequireFromString("65000"),
		},
	}
	srv, ordersRepo := newTradeTestServer(t, brokerMock)
	srv.reservationPoll = time.Millisecond
	intentID := uuid.New()

	const callers = 32
	errs := make([]error, callers)
	resps := make([]*pregaov1.ExecuteTradeResponse, callers)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			// Distinct idempotency keys: only the trade intent ties these requests together.
			resps[i], errs[i] = srv.ExecuteTrade(context.Background(), tradeRequest(intentID, fmt.Sprintf("idem-%d", i)))
		}(i)
	}
	close(start)
	wg.Wait()

	assert.EqualValues(t, 1, brokerMock.orderCalls.Load(), "exactly one broker order per trade intent")

	for i := range errs {
		require.NoError(t, errs[i], "caller %d", i)
		assert.Equal(t, "binance-order-1", resps[i].ProviderOrderId)
	}

	saved, err := ordersRepo.GetOrderByIntentID(context.Background(), intentID)
	require.NoError(t, err)
	assert.Equal(t, "binance-order-1", saved.ProviderOrderID)
	assert.Equal(t, db.StatusFilled, saved.Status)
}
