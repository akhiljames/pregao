package binance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/akhiljames/pregao/internal/broker"
	"github.com/akhiljames/pregao/internal/db"
	"github.com/akhiljames/pregao/internal/vault"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestSignHMAC(t *testing.T) {
	// Standard test vector
	payload := []byte("symbol=BTCUSDT&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559")
	secret := []byte("NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j")

	expectedSig := "9495fce965f74f4818f29ede2b9667a87f0a4972565671cc458abf4ff179b9ae"
	actualSig := SignHMAC(payload, secret)

	assert.Equal(t, expectedSig, actualSig)
}

func TestBinanceClient_GetBookTickers_MidPrice(t *testing.T) {
	mockTransport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		assert.Equal(t, http.MethodGet, req.Method)
		assert.Contains(t, req.URL.Path, "/api/v3/ticker/bookTicker")

		rawTicker := RawBookTicker{
			Symbol:   "BTCUSDT",
			BidPrice: "60000.50000000",
			BidQty:   "1.50000000",
			AskPrice: "60001.50000000",
			AskQty:   "2.00000000",
		}
		data, _ := json.Marshal(rawTicker)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(data)),
			Header:     make(http.Header),
		}, nil
	})

	httpClient := &http.Client{Transport: mockTransport}
	client := NewClientWithHTTPClient("https://api.binance.com", 5000, httpClient)

	tickers, err := client.GetBookTickers(context.Background(), []string{"BTCUSDT"})
	require.NoError(t, err)
	require.Contains(t, tickers, "BTCUSDT")

	ticker := tickers["BTCUSDT"]
	// (60000.50 + 60001.50) / 2 = 60001.00
	expectedMid := decimal.RequireFromString("60001.00000000")
	assert.True(t, expectedMid.Equal(ticker.MidPrice), "MidPrice %s should equal %s", ticker.MidPrice, expectedMid)
}

func TestBinanceClient_GetBookTickers_RateLimit429(t *testing.T) {
	mockTransport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Body:       io.NopCloser(bytes.NewReader([]byte(`{"code": -1003, "msg": "Too many requests"}`))),
			Header:     make(http.Header),
		}, nil
	})

	httpClient := &http.Client{Transport: mockTransport}
	client := NewClientWithHTTPClient("https://api.binance.com", 5000, httpClient)

	_, err := client.GetBookTickers(context.Background(), []string{"BTCUSDT"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, broker.ErrRateLimited), "Expected ErrRateLimited, got: %v", err)
}

func TestBinanceClient_PlaceMarketOrder_ZeroesCredentials(t *testing.T) {
	apiKeyBytes := []byte("binance_api_key_sample")
	apiSecretBytes := []byte("binance_api_secret_sample")

	creds := &vault.PlaintextCredentials{
		APIKey:    apiKeyBytes,
		APISecret: apiSecretBytes,
	}

	mockTransport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		assert.Equal(t, http.MethodPost, req.Method)
		assert.Contains(t, req.URL.Path, "/api/v3/order")
		assert.Equal(t, "binance_api_key_sample", req.Header.Get("X-MBX-APIKEY"))
		assert.Contains(t, req.URL.RawQuery, "signature=")
		assert.Contains(t, req.URL.RawQuery, "type=MARKET")
		assert.Contains(t, req.URL.RawQuery, "side=BUY")

		resp := OrderResponse{
			Symbol:              "BTCUSDT",
			OrderID:             987654321,
			ClientOrderID:       "intent-uuid-1",
			TransactTime:        1600000000,
			Price:               "0.00000000",
			OrigQty:             "0.50000000",
			ExecutedQty:         "0.50000000",
			CummulativeQuoteQty: "30000.00000000",
			Status:              "FILLED",
		}
		data, _ := json.Marshal(resp)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(data)),
			Header:     make(http.Header),
		}, nil
	})

	httpClient := &http.Client{Transport: mockTransport}
	client := NewClientWithHTTPClient("https://api.binance.com", 5000, httpClient)

	result, err := client.PlaceMarketOrder(context.Background(), creds, broker.MarketOrderRequest{
		Symbol:        "BTCUSDT",
		Side:          "BUY",
		Quantity:      decimal.RequireFromString("0.5"),
		ClientOrderID: "intent-uuid-1",
	})

	require.NoError(t, err)
	assert.Equal(t, "987654321", result.ProviderOrderID)
	assert.Equal(t, db.StatusFilled, result.Status)
	assert.True(t, decimal.RequireFromString("0.5").Equal(result.ExecutedQuantity))
	assert.True(t, decimal.RequireFromString("60000").Equal(result.AveragePrice))

	// Verify that PlaceMarketOrder zeroed out credentials immediately
	assert.Nil(t, creds.APIKey)
	assert.Nil(t, creds.APISecret)
	for _, b := range apiKeyBytes {
		assert.Equal(t, byte(0), b, "API key byte was not zeroed out")
	}
	for _, b := range apiSecretBytes {
		assert.Equal(t, byte(0), b, "API secret byte was not zeroed out")
	}
}

func TestBinanceClient_GetAccountBalances(t *testing.T) {
	creds := &vault.PlaintextCredentials{
		APIKey:    []byte("key"),
		APISecret: []byte("secret"),
	}

	mockTransport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		resp := AccountResponse{
			Balances: []AccountBalance{
				{Asset: "USDT", Free: "5000.25000000", Locked: "0.00000000"},
				{Asset: "BTC", Free: "1.23450000", Locked: "0.10000000"},
			},
		}
		data, _ := json.Marshal(resp)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(data)),
			Header:     make(http.Header),
		}, nil
	})

	httpClient := &http.Client{Transport: mockTransport}
	client := NewClientWithHTTPClient("https://api.binance.com", 5000, httpClient)

	balances, err := client.GetAccountBalances(context.Background(), creds)
	require.NoError(t, err)

	expectedUSDT := decimal.RequireFromString("5000.25000000")
	expectedBTC := decimal.RequireFromString("1.23450000")

	assert.True(t, expectedUSDT.Equal(balances["USDT"]))
	assert.True(t, expectedBTC.Equal(balances["BTC"]))
}

func TestBinanceClient_ValidateSymbols(t *testing.T) {
	mockTransport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		resp := ExchangeInfoResponse{
			Symbols: []ExchangeInfoSymbol{
				{Symbol: "BTCUSDT", Status: "TRADING", BaseAsset: "BTC", QuoteAsset: "USDT"},
				{Symbol: "ETHUSDT", Status: "TRADING", BaseAsset: "ETH", QuoteAsset: "USDT"},
				{Symbol: "DELISTED", Status: "BREAK", BaseAsset: "DEL", QuoteAsset: "USDT"},
			},
		}
		data, _ := json.Marshal(resp)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(data)),
			Header:     make(http.Header),
		}, nil
	})

	httpClient := &http.Client{Transport: mockTransport}
	client := NewClientWithHTTPClient("https://api.binance.com", 5000, httpClient)

	res, err := client.ValidateSymbols(context.Background(), []string{"BTCUSDT", "DELISTED", "NONEXISTENT"})
	require.NoError(t, err)

	assert.True(t, res["BTCUSDT"])
	assert.False(t, res["DELISTED"])
	assert.False(t, res["NONEXISTENT"])
}
