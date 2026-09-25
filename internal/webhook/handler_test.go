package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/akhiljames/pregao/internal/worker"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockFillProcessor struct {
	events []worker.FillEvent
}

func (m *mockFillProcessor) ProcessFill(ctx context.Context, event worker.FillEvent) error {
	m.events = append(m.events, event)
	return nil
}

func TestWebhook_HealthCheck(t *testing.T) {
	processor := &mockFillProcessor{}
	server := NewServer(processor)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"status":"OK"}`, rec.Body.String())
}

func TestWebhook_BinanceExecutionReport(t *testing.T) {
	processor := &mockFillProcessor{}
	server := NewServer(processor)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	report := RawBinanceExecutionReport{
		EventType:            "executionReport",
		EventTime:            1600000000,
		Symbol:               "BTCUSDT",
		ClientOrderID:        "order-abc",
		Side:                 "BUY",
		OrderType:            "MARKET",
		ExecutionType:        "TRADE",
		OrderStatus:          "FILLED",
		OrderID:              12345678,
		LastExecutedQuantity: "0.25000000",
		CumulativeFilledQty:  "0.25000000",
		LastExecutedPrice:    "64000.00",
	}

	payload, err := json.Marshal(report)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/binance/order", bytes.NewReader(payload))
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"status":"PROCESSED"}`, rec.Body.String())

	require.Len(t, processor.events, 1)
	ev := processor.events[0]
	assert.Equal(t, "BINANCE", ev.Provider)
	assert.Equal(t, "12345678", ev.ProviderOrderID)
	assert.Equal(t, "BTCUSDT", ev.Symbol)
	assert.Equal(t, "BUY", ev.Side)
	assert.Equal(t, "FILLED", ev.Status)
	assert.True(t, decimal.RequireFromString("0.25").Equal(ev.ExecutedQuantity))
	assert.True(t, decimal.RequireFromString("64000.00").Equal(ev.FillPrice))
}

func TestWebhook_GenericFillEvent(t *testing.T) {
	processor := &mockFillProcessor{}
	server := NewServer(processor)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	event := worker.FillEvent{
		Provider:         "BINANCE",
		ProviderOrderID:  "998877",
		Symbol:           "ETHUSDT",
		Side:             "SELL",
		Status:           "FILLED",
		ExecutedQuantity: decimal.RequireFromString("1.5"),
		FillPrice:        decimal.RequireFromString("3200.50"),
	}

	payload, err := json.Marshal(event)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/binance/order", bytes.NewReader(payload))
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"status":"PROCESSED"}`, rec.Body.String())

	require.Len(t, processor.events, 1)
	assert.Equal(t, "998877", processor.events[0].ProviderOrderID)
}
