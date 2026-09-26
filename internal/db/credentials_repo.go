package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrCredentialsNotFound = errors.New("broker credentials not found")
)

// CredentialsRepository defines operations for broker_credentials table.
type CredentialsRepository interface {
	GetCredentials(ctx context.Context, tenantID, provider string) (*BrokerCredential, error)
	SaveCredentials(ctx context.Context, cred *BrokerCredential) error
}

type pgCredentialsRepo struct {
	pool *pgxpool.Pool
}

// NewCredentialsRepository returns an implementation of CredentialsRepository backed by PostgreSQL.
func NewCredentialsRepository(pool *pgxpool.Pool) CredentialsRepository {
	return &pgCredentialsRepo{pool: pool}
}

func (r *pgCredentialsRepo) GetCredentials(ctx context.Context, tenantID, provider string) (*BrokerCredential, error) {
	query := `
		SELECT id, tenant_id, provider, api_key_ciphertext, api_secret_ciphertext, created_at
		FROM broker_credentials
		WHERE tenant_id = $1 AND provider = $2
	`
	var cred BrokerCredential
	err := r.pool.QueryRow(ctx, query, tenantID, provider).Scan(
		&cred.ID,
		&cred.TenantID,
		&cred.Provider,
		&cred.APIKeyCiphertext,
		&cred.APISecretCiphertext,
		&cred.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrCredentialsNotFound
		}
		return nil, fmt.Errorf("failed to query broker credentials: %w", err)
	}
	return &cred, nil
}

func (r *pgCredentialsRepo) SaveCredentials(ctx context.Context, cred *BrokerCredential) error {
	if cred.ID == uuid.Nil {
		cred.ID = uuid.New()
	}
	if cred.CreatedAt.IsZero() {
		cred.CreatedAt = time.Now().UTC()
	}

	query := `
		INSERT INTO broker_credentials (
			id, tenant_id, provider, api_key_ciphertext, api_secret_ciphertext, created_at
		) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (tenant_id, provider) DO UPDATE SET
			api_key_ciphertext = EXCLUDED.api_key_ciphertext,
			api_secret_ciphertext = EXCLUDED.api_secret_ciphertext
	`
	_, err := r.pool.Exec(
		ctx,
		query,
		cred.ID,
		cred.TenantID,
		cred.Provider,
		cred.APIKeyCiphertext,
		cred.APISecretCiphertext,
		cred.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to save broker credentials: %w", err)
	}
	return nil
}
