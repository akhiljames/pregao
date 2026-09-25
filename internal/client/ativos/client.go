package ativos

import (
	"context"
	"fmt"

	ativosv1 "github.com/akhiljames/proto/gen/go/ativos/v1"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// SyncExecutionParams holds inputs to sync trade execution into Ativos portfolio engine.
type SyncExecutionParams struct {
	TenantID       string
	PortfolioID    string
	TradeIntentID  string
	Ticker         string
	Action         ativosv1.SyncBrokerExecutionRequest_Action
	ExecutedShares decimal.Decimal
	ExecutedPrice  decimal.Decimal
	IdempotencyKey string
}

// Client defines the interface for interacting with Ativos (PMS Engine).
type Client interface {
	SyncBrokerExecution(ctx context.Context, params SyncExecutionParams) (*ativosv1.SyncBrokerExecutionResponse, error)
	Close() error
}

type grpcAtivosClient struct {
	conn   *grpc.ClientConn
	client ativosv1.PortfolioServiceClient
}

// NewClient dials Ativos PortfolioService over gRPC.
func NewClient(addr string) (Client, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial ativos at %s: %w", addr, err)
	}
	return &grpcAtivosClient{
		conn:   conn,
		client: ativosv1.NewPortfolioServiceClient(conn),
	}, nil
}

// NewClientWithConn creates an Ativos client with an existing gRPC connection.
func NewClientWithConn(conn *grpc.ClientConn) Client {
	return &grpcAtivosClient{
		conn:   conn,
		client: ativosv1.NewPortfolioServiceClient(conn),
	}
}

func (c *grpcAtivosClient) SyncBrokerExecution(ctx context.Context, params SyncExecutionParams) (*ativosv1.SyncBrokerExecutionResponse, error) {
	req := &ativosv1.SyncBrokerExecutionRequest{
		TenantId:       params.TenantID,
		PortfolioId:    params.PortfolioID,
		TradeIntentId:  params.TradeIntentID,
		Ticker:         params.Ticker,
		Action:         params.Action,
		ExecutedShares: params.ExecutedShares.String(),
		ExecutedPrice:  params.ExecutedPrice.String(),
		IdempotencyKey: params.IdempotencyKey,
	}

	resp, err := c.client.SyncBrokerExecution(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("ativos SyncBrokerExecution failed: %w", err)
	}
	return resp, nil
}

func (c *grpcAtivosClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
