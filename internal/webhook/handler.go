package webhook

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/akhiljames/pregao/internal/worker"
	"github.com/shopspring/decimal"
)

// RawBinanceExecutionReport models the standard executionReport event from Binance.
type RawBinanceExecutionReport struct {
	EventType               string `json:"e"` // "executionReport"
	EventTime               int64  `json:"E"`
	Symbol                  string `json:"s"`
	ClientOrderID           string `json:"c"`
	Side                    string `json:"S"` // "BUY", "SELL"
	OrderType               string `json:"o"` // "MARKET"
	OriginalQuantity        string `json:"q"`
	Price                   string `json:"p"`
	ExecutionType           string `json:"x"` // "TRADE", "NEW", etc.
	OrderStatus             string `json:"X"` // "FILLED", "PARTIALLY_FILLED"
	OrderID                 int64  `json:"i"`
	LastExecutedQuantity    string `json:"l"`
	CumulativeFilledQty     string `json:"z"`
	LastExecutedPrice       string `json:"L"`
	CumulativeQuoteQuantity string `json:"Z"`
}

// Server provides HTTP webhook endpoints for broker execution reports.
type Server struct {
	processor worker.FillProcessor
}

// NewServer returns a new webhook HTTP server handler.
func NewServer(processor worker.FillProcessor) *Server {
	return &Server{processor: processor}
}

// RegisterRoutes registers webhook routes on the given ServeMux.
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", s.handleHealthCheck)
	mux.HandleFunc("/webhooks/binance/order", s.handleBinanceOrderWebhook)
}

func (s *Server) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"OK"}`))
}

func (s *Server) handleBinanceOrderWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Try standard FillEvent payload first, then RawBinanceExecutionReport
	var genericEvent worker.FillEvent
	decoder := json.NewDecoder(r.Body)

	var rawMap map[string]interface{}
	if err := decoder.Decode(&rawMap); err != nil {
		http.Error(w, fmt.Sprintf("invalid json payload: %v", err), http.StatusBadRequest)
		return
	}

	// Re-marshal to process
	payloadBytes, err := json.Marshal(rawMap)
	if err != nil {
		http.Error(w, "failed to parse payload", http.StatusBadRequest)
		return
	}

	// If payload has "provider_order_id", it's a generic FillEvent
	if _, ok := rawMap["provider_order_id"]; ok {
		if err := json.Unmarshal(payloadBytes, &genericEvent); err != nil {
			http.Error(w, fmt.Sprintf("invalid fill event payload: %v", err), http.StatusBadRequest)
			return
		}
	} else if _, ok := rawMap["e"]; ok {
		// Binance native executionReport
		var binanceReport RawBinanceExecutionReport
		if err := json.Unmarshal(payloadBytes, &binanceReport); err != nil {
			http.Error(w, fmt.Sprintf("invalid binance execution report: %v", err), http.StatusBadRequest)
			return
		}

		// Only process trade executions (fills)
		if binanceReport.ExecutionType != "TRADE" && binanceReport.OrderStatus != "FILLED" && binanceReport.OrderStatus != "PARTIALLY_FILLED" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ignored"}`))
			return
		}

		lastQty, err := decimal.NewFromString(binanceReport.LastExecutedQuantity)
		if err != nil || lastQty.IsZero() {
			lastQty, err = decimal.NewFromString(binanceReport.CumulativeFilledQty)
			if err != nil {
				http.Error(w, "invalid execution quantity", http.StatusBadRequest)
				return
			}
		}

		lastPrice, err := decimal.NewFromString(binanceReport.LastExecutedPrice)
		if err != nil || lastPrice.IsZero() {
			cumQuote, err1 := decimal.NewFromString(binanceReport.CumulativeQuoteQuantity)
			if err1 == nil && lastQty.GreaterThan(decimal.Zero) {
				lastPrice = cumQuote.Div(lastQty)
			}
		}

		genericEvent = worker.FillEvent{
			Provider:         "BINANCE",
			ProviderOrderID:  strconv.FormatInt(binanceReport.OrderID, 10),
			Symbol:           binanceReport.Symbol,
			Side:             binanceReport.Side,
			Status:           binanceReport.OrderStatus,
			ExecutedQuantity: lastQty,
			FillPrice:        lastPrice,
		}
	} else {
		http.Error(w, "unrecognized webhook payload format", http.StatusBadRequest)
		return
	}

	if err := s.processor.ProcessFill(r.Context(), genericEvent); err != nil {
		http.Error(w, fmt.Sprintf("fill processing failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"PROCESSED"}`))
}
