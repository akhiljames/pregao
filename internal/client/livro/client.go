package livro

import (
	"context"
	"fmt"
	"sync"

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
	GetOrCreateBrokerAccount(ctx context.Context, tenantID string) (string, error)
	Close() error
}

type grpcLivroClient struct {
	conn           *grpc.ClientConn
	client         livrov1.LedgerServiceClient
	brokerAccounts sync.Map
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

func (c *grpcLivroClient) GetOrCreateBrokerAccount(ctx context.Context, tenantID string) (string, error) {
	if val, ok := c.brokerAccounts.Load(tenantID); ok {
		return val.(string), nil
	}

	resp, err := c.client.InitializeAccount(ctx, &livrov1.InitializeAccountRequest{
		Scope: &livrov1.Scope{
			Type: "tenant",
			Id:   tenantID,
		},
		EntityType:     "broker",
		EntityId:       "clearing",
		AccountName:    "broker_usd",
		Currency:       "USD",
		AccountType:    livrov1.AccountType_ACCOUNT_TYPE_CUSTOMER_WALLET,
		AllowOverdraft: true,
	})
	if err == nil && resp != nil && resp.Account != nil && resp.Account.Id != "" {
		c.brokerAccounts.Store(tenantID, resp.Account.Id)
		return resp.Account.Id, nil
	}

	lookupResp, err := c.client.GetAccount(ctx, &livrov1.GetAccountRequest{
		Identifier: &livrov1.GetAccountRequest_Lookup{
			Lookup: &livrov1.AccountLookup{
				Scope: &livrov1.Scope{
					Type: "tenant",
					Id:   tenantID,
				},
				EntityType:  "broker",
				EntityId:    "clearing",
				AccountName: "broker_usd",
				Currency:    "USD",
			},
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to resolve broker clearing account in livro: %w", err)
	}

	if lookupResp == nil || lookupResp.Account == nil || lookupResp.Account.Id == "" {
		return "", fmt.Errorf("empty broker clearing account returned from livro")
	}

	c.brokerAccounts.Store(tenantID, lookupResp.Account.Id)
	return lookupResp.Account.Id, nil
}

func (c *grpcLivroClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
