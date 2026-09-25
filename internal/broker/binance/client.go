package binance

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/akhiljames/pregao/internal/broker"
	"github.com/akhiljames/pregao/internal/db"
	"github.com/akhiljames/pregao/internal/vault"
	"github.com/shopspring/decimal"
)

type binanceClient struct {
	baseURL    string
	recvWindow int64
	httpClient *http.Client
}

// NewClient returns a new broker.Broker client for Binance.
func NewClient(baseURL string, recvWindow int64, timeout time.Duration) broker.Broker {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return NewClientWithHTTPClient(baseURL, recvWindow, &http.Client{Timeout: timeout})
}

// NewClientWithHTTPClient returns a broker.Broker client for Binance using a custom http.Client.
func NewClientWithHTTPClient(baseURL string, recvWindow int64, httpClient *http.Client) broker.Broker {
	if baseURL == "" {
		baseURL = "https://api.binance.com"
	}
	if recvWindow <= 0 {
		recvWindow = 5000
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &binanceClient{
		baseURL:    strings.TrimRight(baseURL, "/"),
		recvWindow: recvWindow,
		httpClient: httpClient,
	}
}

// SignHMAC computes the HMAC-SHA256 signature in hex format.
func SignHMAC(payload, secret []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

func (c *binanceClient) GetBookTickers(ctx context.Context, symbols []string) (map[string]broker.BookTicker, error) {
	result := make(map[string]broker.BookTicker, len(symbols))
	if len(symbols) == 0 {
		return result, nil
	}

	var reqURL string
	if len(symbols) == 1 {
		reqURL = fmt.Sprintf("%s/api/v3/ticker/bookTicker?symbol=%s", c.baseURL, url.QueryEscape(symbols[0]))
	} else {
		symbolsParam, err := json.Marshal(symbols)
		if err != nil {
			return nil, fmt.Errorf("failed to encode symbols: %w", err)
		}
		reqURL = fmt.Sprintf("%s/api/v3/ticker/bookTicker?symbols=%s", c.baseURL, url.QueryEscape(string(symbolsParam)))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("binance bookTicker request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == 418 {
		return nil, broker.ErrRateLimited
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("binance bookTicker returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	if len(symbols) == 1 {
		var raw RawBookTicker
		if err := json.Unmarshal(bodyBytes, &raw); err != nil {
			return nil, fmt.Errorf("failed to unmarshal single bookTicker: %w", err)
		}
		ticker, err := parseBookTicker(raw)
		if err != nil {
			return nil, err
		}
		result[ticker.Symbol] = ticker
		return result, nil
	}

	var rawList []RawBookTicker
	if err := json.Unmarshal(bodyBytes, &rawList); err != nil {
		return nil, fmt.Errorf("failed to unmarshal bookTicker list: %w", err)
	}

	for _, raw := range rawList {
		ticker, err := parseBookTicker(raw)
		if err != nil {
			continue
		}
		result[ticker.Symbol] = ticker
	}

	return result, nil
}

func parseBookTicker(raw RawBookTicker) (broker.BookTicker, error) {
	bidPrice, err := decimal.NewFromString(raw.BidPrice)
	if err != nil {
		return broker.BookTicker{}, fmt.Errorf("invalid bid price %q for %s: %w", raw.BidPrice, raw.Symbol, err)
	}
	bidQty, err := decimal.NewFromString(raw.BidQty)
	if err != nil {
		return broker.BookTicker{}, fmt.Errorf("invalid bid qty %q for %s: %w", raw.BidQty, raw.Symbol, err)
	}
	askPrice, err := decimal.NewFromString(raw.AskPrice)
	if err != nil {
		return broker.BookTicker{}, fmt.Errorf("invalid ask price %q for %s: %w", raw.AskPrice, raw.Symbol, err)
	}
	askQty, err := decimal.NewFromString(raw.AskQty)
	if err != nil {
		return broker.BookTicker{}, fmt.Errorf("invalid ask qty %q for %s: %w", raw.AskQty, raw.Symbol, err)
	}

	// Mid-price: (bid + ask) / 2 using shopspring/decimal
	midPrice := bidPrice.Add(askPrice).Div(decimal.NewFromInt(2))

	return broker.BookTicker{
		Symbol:   raw.Symbol,
		BidPrice: bidPrice,
		BidQty:   bidQty,
		AskPrice: askPrice,
		AskQty:   askQty,
		MidPrice: midPrice,
	}, nil
}

func (c *binanceClient) ValidateSymbols(ctx context.Context, symbols []string) (map[string]bool, error) {
	result := make(map[string]bool, len(symbols))
	for _, s := range symbols {
		result[s] = false
	}
	if len(symbols) == 0 {
		return result, nil
	}

	reqURL := fmt.Sprintf("%s/api/v3/exchangeInfo", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create exchangeInfo request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("binance exchangeInfo request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == 418 {
		return nil, broker.ErrRateLimited
	}

	var info ExchangeInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("failed to decode exchangeInfo: %w", err)
	}

	tradingSymbols := make(map[string]struct{}, len(info.Symbols))
	for _, s := range info.Symbols {
		if strings.EqualFold(s.Status, "TRADING") {
			tradingSymbols[strings.ToUpper(s.Symbol)] = struct{}{}
		}
	}

	for _, s := range symbols {
		_, ok := tradingSymbols[strings.ToUpper(s)]
		result[s] = ok
	}

	return result, nil
}

func (c *binanceClient) PlaceMarketOrder(ctx context.Context, creds *vault.PlaintextCredentials, orderReq broker.MarketOrderRequest) (*broker.OrderResult, error) {
	if creds == nil || len(creds.APIKey) == 0 || len(creds.APISecret) == 0 {
		return nil, errors.New("missing broker credentials for order placement")
	}
	// Zero plaintext credentials immediately upon function return
	defer creds.Zero()

	if orderReq.Quantity.LessThanOrEqual(decimal.Zero) {
		return nil, broker.ErrInvalidQuantity
	}

	stepSize := GetDefaultStepSize(orderReq.Symbol)
	quantizedQty, qtyStr := TruncateToStepSize(orderReq.Quantity, stepSize)
	if quantizedQty.LessThanOrEqual(decimal.Zero) {
		return nil, fmt.Errorf("%w: quantity %s is smaller than minimum step size %s for %s", broker.ErrInvalidQuantity, orderReq.Quantity, stepSize, orderReq.Symbol)
	}

	params := url.Values{}
	params.Set("symbol", strings.ToUpper(orderReq.Symbol))
	params.Set("side", strings.ToUpper(orderReq.Side))
	params.Set("type", "MARKET")
	params.Set("quantity", qtyStr)
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	params.Set("recvWindow", strconv.FormatInt(c.recvWindow, 10))
	if orderReq.ClientOrderID != "" {
		params.Set("newClientOrderId", orderReq.ClientOrderID)
	}

	queryString := params.Encode()
	sig := SignHMAC([]byte(queryString), creds.APISecret)
	fullURL := fmt.Sprintf("%s/api/v3/order?%s&signature=%s", c.baseURL, queryString, sig)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create order request: %w", err)
	}

	req.Header.Set("X-MBX-APIKEY", string(creds.APIKey))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("binance order request failed: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read order response: %w", err)
	}

	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == 418 {
		return nil, broker.ErrRateLimited
	}

	var ordResp OrderResponse
	if err := json.Unmarshal(bodyBytes, &ordResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal order response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: status %d (code: %d, msg: %s)", broker.ErrOrderRejected, resp.StatusCode, ordResp.Code, ordResp.Msg)
	}

	status := mapOrderStatus(ordResp.Status)
	executedQty, _ := decimal.NewFromString(ordResp.ExecutedQty)
	cumQuoteQty, _ := decimal.NewFromString(ordResp.CummulativeQuoteQty)

	var avgPrice decimal.Decimal
	if executedQty.GreaterThan(decimal.Zero) {
		avgPrice = cumQuoteQty.Div(executedQty)
	}

	return &broker.OrderResult{
		ProviderOrderID:         strconv.FormatInt(ordResp.OrderID, 10),
		Status:                  status,
		ExecutedQuantity:        executedQty,
		CumulativeQuoteQuantity: cumQuoteQty,
		AveragePrice:            avgPrice,
	}, nil
}

func (c *binanceClient) GetAccountBalances(ctx context.Context, creds *vault.PlaintextCredentials) (map[string]decimal.Decimal, error) {
	if creds == nil || len(creds.APIKey) == 0 || len(creds.APISecret) == 0 {
		return nil, errors.New("missing broker credentials for balance check")
	}
	// Zero credentials immediately
	defer creds.Zero()

	params := url.Values{}
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	params.Set("recvWindow", strconv.FormatInt(c.recvWindow, 10))

	queryString := params.Encode()
	sig := SignHMAC([]byte(queryString), creds.APISecret)
	fullURL := fmt.Sprintf("%s/api/v3/account?%s&signature=%s", c.baseURL, queryString, sig)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create account request: %w", err)
	}
	req.Header.Set("X-MBX-APIKEY", string(creds.APIKey))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("binance account request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == 418 {
		return nil, broker.ErrRateLimited
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read account response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("binance account returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var accResp AccountResponse
	if err := json.Unmarshal(bodyBytes, &accResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal account response: %w", err)
	}

	result := make(map[string]decimal.Decimal, len(accResp.Balances))
	for _, b := range accResp.Balances {
		free, err := decimal.NewFromString(b.Free)
		if err != nil {
			continue
		}
		result[b.Asset] = free
	}

	return result, nil
}

func mapOrderStatus(binanceStatus string) string {
	switch strings.ToUpper(binanceStatus) {
	case "NEW":
		return db.StatusSubmitted
	case "PARTIALLY_FILLED":
		return db.StatusPartial
	case "FILLED":
		return db.StatusFilled
	case "CANCELED", "EXPIRED":
		return db.StatusCanceled
	case "REJECTED":
		return db.StatusRejected
	default:
		return db.StatusSubmitted
	}
}

// TruncateToStepSize quantizes a decimal quantity down to the nearest multiple of stepSize.
// E.g., qty = 0.00012345, stepSize = 0.00001 -> returns 0.00012 and formatted string "0.00012".
func TruncateToStepSize(qty decimal.Decimal, stepSize decimal.Decimal) (decimal.Decimal, string) {
	if stepSize.LessThanOrEqual(decimal.Zero) {
		return qty, qty.String()
	}
	steps := qty.Div(stepSize).Floor()
	quantized := steps.Mul(stepSize)

	// Determine decimal places from stepSize string (e.g. "0.00001000" -> 5 decimals)
	stepStr := stepSize.String()
	decimals := 0
	if idx := strings.Index(stepStr, "."); idx != -1 {
		trimmed := strings.TrimRight(stepStr[idx+1:], "0")
		decimals = len(trimmed)
		if decimals == 0 {
			decimals = len(stepStr[idx+1:])
		}
	}
	return quantized, quantized.StringFixed(int32(decimals))
}

// GetDefaultStepSize returns standard Binance spot step sizes for common trading pairs.
func GetDefaultStepSize(symbol string) decimal.Decimal {
	switch strings.ToUpper(strings.TrimSpace(symbol)) {
	case "BTCUSDT":
		return decimal.RequireFromString("0.00001000") // 5 decimals
	case "ETHUSDT":
		return decimal.RequireFromString("0.00010000") // 4 decimals
	case "SOLUSDT":
		return decimal.RequireFromString("0.01000000") // 2 decimals
	case "BNBUSDT":
		return decimal.RequireFromString("0.00100000") // 3 decimals
	case "DOGEUSDT":
		return decimal.RequireFromString("1.00000000") // 0 decimals
	case "ADAUSDT", "XRPUSDT":
		return decimal.RequireFromString("0.10000000") // 1 decimal
	default:
		// Safe fallback for unknown crypto pairs: 4 decimal places
		return decimal.RequireFromString("0.00010000")
	}
}

