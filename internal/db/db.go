package db

import (
	"context"
	_ "embed"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/001_initial_schema.sql
var initialSchemaSQL string

// NewPool initializes a new PostgreSQL connection pool using pgxpool.
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database URL: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create pgxpool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return pool, nil
}

// RunMigrations executes initial schema migrations.
func RunMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	log.Println("[DB] Applying database schema migrations...")
	if _, err := pool.Exec(ctx, initialSchemaSQL); err != nil {
		return fmt.Errorf("failed to execute schema migration: %w", err)
	}
	log.Println("[DB] Schema migrations applied successfully.")
	return nil
}
