package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

var (
	ErrOrderNotFound = errors.New("broker order not found")
)

// OrdersRepository defines operations for broker_orders table.
type OrdersRepository interface {
	GetOrderByIntentID(ctx context.Context, tradeIntentID uuid.UUID) (*BrokerOrder, error)
	GetOrderByIdempotencyKey(ctx context.Context, tenantID, idempotencyKey string) (*BrokerOrder, error)
	GetOrderByProviderOrderID(ctx context.Context, provider, providerOrderID string) (*BrokerOrder, error)
	// ReserveOrder inserts the order as a PENDING row with no provider order ID. It returns false,
	// writing nothing, when a row already holds the same trade intent or (tenant, idempotency key).
	ReserveOrder(ctx context.Context, order *BrokerOrder) (bool, error)
	// ConfirmOrder records the broker's response on a PENDING reservation.
	ConfirmOrder(ctx context.Context, id uuid.UUID, providerOrderID string, status string, filledQuantity decimal.Decimal, avgFillPrice decimal.Decimal) error
	// ReleaseOrder deletes a PENDING reservation whose order never reached the broker.
	ReleaseOrder(ctx context.Context, id uuid.UUID) error
	UpdateOrderFill(ctx context.Context, provider, providerOrderID string, status string, filledQuantity decimal.Decimal, avgFillPrice decimal.Decimal) error
}

type pgOrdersRepo struct {
	pool *pgxpool.Pool
}

// NewOrdersRepository returns an implementation of OrdersRepository backed by PostgreSQL.
func NewOrdersRepository(pool *pgxpool.Pool) OrdersRepository {
	return &pgOrdersRepo{pool: pool}
}

func (r *pgOrdersRepo) GetOrderByIntentID(ctx context.Context, tradeIntentID uuid.UUID) (*BrokerOrder, error) {
	query := `
		SELECT id, trade_intent_id, tenant_id, user_id, provider, provider_order_id, symbol, side,
		       target_quantity, status, filled_quantity, average_fill_price, idempotency_key,
		       portfolio_id, livro_hold_id, created_at, updated_at
		FROM broker_orders
		WHERE trade_intent_id = $1
	`
	return r.scanOrder(r.pool.QueryRow(ctx, query, tradeIntentID))
}

func (r *pgOrdersRepo) GetOrderByIdempotencyKey(ctx context.Context, tenantID, idempotencyKey string) (*BrokerOrder, error) {
	query := `
		SELECT id, trade_intent_id, tenant_id, user_id, provider, provider_order_id, symbol, side,
		       target_quantity, status, filled_quantity, average_fill_price, idempotency_key,
		       portfolio_id, livro_hold_id, created_at, updated_at
		FROM broker_orders
		WHERE tenant_id = $1 AND idempotency_key = $2
	`
	return r.scanOrder(r.pool.QueryRow(ctx, query, tenantID, idempotencyKey))
}

func (r *pgOrdersRepo) GetOrderByProviderOrderID(ctx context.Context, provider, providerOrderID string) (*BrokerOrder, error) {
	query := `
		SELECT id, trade_intent_id, tenant_id, user_id, provider, provider_order_id, symbol, side,
		       target_quantity, status, filled_quantity, average_fill_price, idempotency_key,
		       portfolio_id, livro_hold_id, created_at, updated_at
		FROM broker_orders
		WHERE provider = $1 AND provider_order_id = $2
	`
	return r.scanOrder(r.pool.QueryRow(ctx, query, provider, providerOrderID))
}

func (r *pgOrdersRepo) ReserveOrder(ctx context.Context, order *BrokerOrder) (bool, error) {
	if order.ID == uuid.Nil {
		order.ID = uuid.New()
	}
	now := time.Now().UTC()
	if order.CreatedAt.IsZero() {
		order.CreatedAt = now
	}
	if order.UpdatedAt.IsZero() {
		order.UpdatedAt = now
	}
	order.Status = StatusPending

	// provider_order_id stays NULL until ConfirmOrder. ON CONFLICT covers both the UNIQUE
	// trade_intent_id and the partial unique index on (tenant_id, idempotency_key).
	query := `
		INSERT INTO broker_orders (
			id, trade_intent_id, tenant_id, user_id, provider, symbol, side,
			target_quantity, status, filled_quantity, average_fill_price, idempotency_key,
			portfolio_id, livro_hold_id, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		ON CONFLICT DO NOTHING
	`
	cmdTag, err := r.pool.Exec(
		ctx,
		query,
		order.ID,
		order.TradeIntentID,
		order.TenantID,
		order.UserID,
		order.Provider,
		order.Symbol,
		order.Side,
		order.TargetQuantity,
		order.Status,
		order.FilledQuantity,
		order.AverageFillPrice,
		order.IdempotencyKey,
		order.PortfolioID,
		order.LivroHoldID,
		order.CreatedAt,
		order.UpdatedAt,
	)
	if err != nil {
		return false, fmt.Errorf("failed to reserve broker order: %w", err)
	}
	return cmdTag.RowsAffected() == 1, nil
}

func (r *pgOrdersRepo) ConfirmOrder(ctx context.Context, id uuid.UUID, providerOrderID string, status string, filledQuantity decimal.Decimal, avgFillPrice decimal.Decimal) error {
	query := `
		UPDATE broker_orders
		SET provider_order_id = $1, status = $2, filled_quantity = $3, average_fill_price = $4, updated_at = NOW()
		WHERE id = $5 AND status = $6
	`
	cmdTag, err := r.pool.Exec(ctx, query, providerOrderID, status, filledQuantity, avgFillPrice, id, StatusPending)
	if err != nil {
		return fmt.Errorf("failed to confirm broker order: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		return ErrOrderNotFound
	}
	return nil
}

func (r *pgOrdersRepo) ReleaseOrder(ctx context.Context, id uuid.UUID) error {
	query := `DELETE FROM broker_orders WHERE id = $1 AND status = $2`
	if _, err := r.pool.Exec(ctx, query, id, StatusPending); err != nil {
		return fmt.Errorf("failed to release broker order reservation: %w", err)
	}
	return nil
}

func (r *pgOrdersRepo) UpdateOrderFill(ctx context.Context, provider, providerOrderID string, status string, filledQuantity decimal.Decimal, avgFillPrice decimal.Decimal) error {
	query := `
		UPDATE broker_orders
		SET status = $1, filled_quantity = $2, average_fill_price = $3, updated_at = NOW()
		WHERE provider = $4 AND provider_order_id = $5
	`
	cmdTag, err := r.pool.Exec(ctx, query, status, filledQuantity, avgFillPrice, provider, providerOrderID)
	if err != nil {
		return fmt.Errorf("failed to update broker order fill: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		return ErrOrderNotFound
	}
	return nil
}

func (r *pgOrdersRepo) scanOrder(row pgx.Row) (*BrokerOrder, error) {
	var o BrokerOrder
	var providerOrderID, idempotencyKey, portfolioID, livroHoldID *string

	err := row.Scan(
		&o.ID,
		&o.TradeIntentID,
		&o.TenantID,
		&o.UserID,
		&o.Provider,
		&providerOrderID,
		&o.Symbol,
		&o.Side,
		&o.TargetQuantity,
		&o.Status,
		&o.FilledQuantity,
		&o.AverageFillPrice,
		&idempotencyKey,
		&portfolioID,
		&livroHoldID,
		&o.CreatedAt,
		&o.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrOrderNotFound
		}
		return nil, fmt.Errorf("failed to scan broker order: %w", err)
	}

	if providerOrderID != nil {
		o.ProviderOrderID = *providerOrderID
	}
	if idempotencyKey != nil {
		o.IdempotencyKey = *idempotencyKey
	}
	if portfolioID != nil {
		o.PortfolioID = *portfolioID
	}
	if livroHoldID != nil {
		o.LivroHoldID = *livroHoldID
	}

	return &o, nil
}
