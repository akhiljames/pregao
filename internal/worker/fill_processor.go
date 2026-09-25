package worker

import (
	"context"
	"fmt"
	"strings"

	ativosv1 "github.com/akhiljames/proto/gen/go/ativos/v1"
	"github.com/akhiljames/pregao/internal/client/ativos"
	"github.com/akhiljames/pregao/internal/client/livro"
	"github.com/akhiljames/pregao/internal/db"
	"github.com/shopspring/decimal"
)

// FillEvent represents an execution fill update received from a broker.
// STRICT: float32/float64 banned. All numeric data uses decimal.Decimal.
type FillEvent struct {
	Provider         string          `json:"provider"`
	ProviderOrderID  string          `json:"provider_order_id"`
	Symbol           string          `json:"symbol"`
	Side             string          `json:"side"` // "BUY" or "SELL"
	Status           string          `json:"status"` // "PARTIAL", "FILLED", etc.
	ExecutedQuantity decimal.Decimal `json:"executed_quantity"`
	FillPrice        decimal.Decimal `json:"fill_price"`
}

// FillProcessor orchestrates two-phase settlement and portfolio sync when orders are filled.
type FillProcessor interface {
	ProcessFill(ctx context.Context, event FillEvent) error
}

type fillProcessor struct {
	ordersRepo   db.OrdersRepository
	livroClient  livro.Client
	ativosClient ativos.Client
}

// NewFillProcessor creates a new FillProcessor.
func NewFillProcessor(ordersRepo db.OrdersRepository, livroClient livro.Client, ativosClient ativos.Client) FillProcessor {
	return &fillProcessor{
		ordersRepo:   ordersRepo,
		livroClient:  livroClient,
		ativosClient: ativosClient,
	}
}

func (p *fillProcessor) ProcessFill(ctx context.Context, event FillEvent) error {
	if event.ExecutedQuantity.LessThanOrEqual(decimal.Zero) {
		return fmt.Errorf("invalid executed quantity: %s", event.ExecutedQuantity)
	}
	if event.FillPrice.LessThanOrEqual(decimal.Zero) {
		return fmt.Errorf("invalid fill price: %s", event.FillPrice)
	}

	order, err := p.ordersRepo.GetOrderByProviderOrderID(ctx, event.Provider, event.ProviderOrderID)
	if err != nil {
		return fmt.Errorf("failed to lookup order by provider_order_id %s: %w", event.ProviderOrderID, err)
	}

	// 1. Exact fiat value calculation using shopspring/decimal: executed_qty * fill_price
	exactFiatValue := event.ExecutedQuantity.Mul(event.FillPrice)

	// Calculate cumulative filled quantity and updated weighted average fill price
	newFilledQty := order.FilledQuantity.Add(event.ExecutedQuantity)
	var newAvgPrice decimal.Decimal
	if order.FilledQuantity.IsZero() {
		newAvgPrice = event.FillPrice
	} else {
		prevTotalFiat := order.FilledQuantity.Mul(order.AverageFillPrice)
		newTotalFiat := prevTotalFiat.Add(exactFiatValue)
		newAvgPrice = newTotalFiat.Div(newFilledQty)
	}

	// Determine new order status
	newStatus := db.StatusPartial
	if strings.EqualFold(event.Status, db.StatusFilled) || newFilledQty.GreaterThanOrEqual(order.TargetQuantity) {
		newStatus = db.StatusFilled
	}

	// 2. Ledger Sync with Livro (port 50051)
	if p.livroClient != nil {
		if strings.EqualFold(order.Side, db.SideBuy) {
			holdID := order.LivroHoldID
			if holdID == "" {
				holdID = order.TradeIntentID.String()
			}
			capParams := livro.CaptureHoldParams{
				HoldID:               holdID,
				Amount:               exactFiatValue,
				DestinationAccountID: "system_clearing",
				Description:          fmt.Sprintf("Execution fill for order %s", order.ProviderOrderID),
				IdempotencyKey:       fmt.Sprintf("cap-%s-%s-%s", order.TradeIntentID, order.ProviderOrderID, newFilledQty.String()),
				ReleaseRemainder:     newStatus == db.StatusFilled,
			}
			if _, err := p.livroClient.CaptureHold(ctx, capParams); err != nil {
				return fmt.Errorf("livro CaptureHold failed: %w", err)
			}
		} else if strings.EqualFold(order.Side, db.SideSell) {
			creditParams := livro.CreditParams{
				TenantID:       order.TenantID,
				AccountID:      order.UserID,
				Amount:         exactFiatValue,
				Currency:       "USD",
				Description:    fmt.Sprintf("Sale execution proceeds for order %s", order.ProviderOrderID),
				Reference:      order.TradeIntentID.String(),
				IdempotencyKey: fmt.Sprintf("credit-%s-%s-%s", order.TradeIntentID, order.ProviderOrderID, newFilledQty.String()),
			}
			if _, err := p.livroClient.Credit(ctx, creditParams); err != nil {
				return fmt.Errorf("livro Credit failed: %w", err)
			}
		}
	}

	// 3. Portfolio Sync with Ativos (port 50052)
	if p.ativosClient != nil {
		action := ativosv1.SyncBrokerExecutionRequest_BUY
		if strings.EqualFold(order.Side, db.SideSell) {
			action = ativosv1.SyncBrokerExecutionRequest_SELL
		}
		syncParams := ativos.SyncExecutionParams{
			TenantID:       order.TenantID,
			PortfolioID:    order.PortfolioID,
			TradeIntentID:  order.TradeIntentID.String(),
			Ticker:         order.Symbol,
			Action:         action,
			ExecutedShares: event.ExecutedQuantity,
			ExecutedPrice:  event.FillPrice,
			IdempotencyKey: fmt.Sprintf("sync-ativos-%s-%s-%s", order.TradeIntentID, order.ProviderOrderID, newFilledQty.String()),
		}
		if _, err := p.ativosClient.SyncBrokerExecution(ctx, syncParams); err != nil {
			return fmt.Errorf("ativos SyncBrokerExecution failed: %w", err)
		}
	}

	// 4. Update order in PostgreSQL broker_orders
	if err := p.ordersRepo.UpdateOrderFill(ctx, order.Provider, order.ProviderOrderID, newStatus, newFilledQty, newAvgPrice); err != nil {
		return fmt.Errorf("failed to update broker_orders record: %w", err)
	}

	return nil
}
