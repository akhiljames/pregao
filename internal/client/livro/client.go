package livro

import (
	"context"
	"fmt"

	livrov1 "github.com/akhiljames/proto/gen/go/livro/v1"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// CaptureHoldParams defines the inputs to capture a 2PC hold in Livro.
type CaptureHoldParams struct {
	HoldID               string
	Amount               decimal.Decimal
	DestinationAccountID string
	Description          string
	IdempotencyKey       string
	ReleaseRemainder     bool
}

// CreditParams defines the inputs to credit funds in Livro.
type CreditParams struct {
	TenantID       string
	AccountID      string
	Amount         decimal.Decimal
	Currency       string
	Description    string
	Reference      string
	IdempotencyKey string
}

// Client defines the interface for interacting with Livro (Financial Ledger).
type Client interface {
	CaptureHold(ctx context.Context, params CaptureHoldParams) (*livrov1.CaptureHoldResponse, error)
	Credit(ctx context.Context, params CreditParams) (*livrov1.TransactionResponse, error)
	GetBalance(ctx context.Context, accountID string) (decimal.Decimal, error)
	Close() error
}

type grpcLivroClient struct {
	conn   *grpc.ClientConn
	client livrov1.LedgerServiceClient
}

// NewClient dials Livro LedgerService over gRPC.
func NewClient(addr string) (Client, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial livro at %s: %w", addr, err)
	}
	return &grpcLivroClient{
		conn:   conn,
		client: livrov1.NewLedgerServiceClient(conn),
	}, nil
}

// NewClientWithConn creates a Livro client with an existing gRPC connection.
func NewClientWithConn(conn *grpc.ClientConn) Client {
	return &grpcLivroClient{
		conn:   conn,
		client: livrov1.NewLedgerServiceClient(conn),
	}
}

func (c *grpcLivroClient) CaptureHold(ctx context.Context, params CaptureHoldParams) (*livrov1.CaptureHoldResponse, error) {
	req := &livrov1.CaptureHoldRequest{
		HoldId:               params.HoldID,
		Amount:               params.Amount.StringFixed(4),
		DestinationAccountId: params.DestinationAccountID,
		Description:          params.Description,
		IdempotencyKey:       params.IdempotencyKey,
		ReleaseRemainder:     params.ReleaseRemainder,
	}

	resp, err := c.client.CaptureHold(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("livro CaptureHold failed: %w", err)
	}
	return resp, nil
}

func (c *grpcLivroClient) Credit(ctx context.Context, params CreditParams) (*livrov1.TransactionResponse, error) {
	req := &livrov1.CreditRequest{
		IdempotencyKey: params.IdempotencyKey,
		Scope: &livrov1.Scope{
			Type: "tenant",
			Id:   params.TenantID,
		},
		TargetAccount: &livrov1.CreditRequest_AccountId{
			AccountId: params.AccountID,
		},
		Amount:      params.Amount.StringFixed(4),
		Currency:    params.Currency,
		Description: params.Description,
		Reference:   params.Reference,
	}

	resp, err := c.client.Credit(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("livro Credit failed: %w", err)
	}
	return resp, nil
}

func (c *grpcLivroClient) GetBalance(ctx context.Context, accountID string) (decimal.Decimal, error) {
	req := &livrov1.GetBalanceRequest{
		Identifier: &livrov1.GetBalanceRequest_AccountId{
			AccountId: accountID,
		},
	}

	resp, err := c.client.GetBalance(ctx, req)
	if err != nil {
		return decimal.Zero, fmt.Errorf("livro GetBalance failed: %w", err)
	}

	bal, err := decimal.NewFromString(resp.Balance)
	if err != nil {
		return decimal.Zero, fmt.Errorf("failed to parse livro balance %q: %w", resp.Balance, err)
	}
	return bal, nil
}

func (c *grpcLivroClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
