-- Pregão Master Database Schema: PostgreSQL 14+

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Table: broker_credentials
-- Stores OpenBao ciphertexts mapping users to their broker API keys.
-- Plaintext credentials NEVER touch PostgreSQL.
CREATE TABLE IF NOT EXISTS broker_credentials (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    provider VARCHAR(32) NOT NULL,
    api_key_ciphertext TEXT NOT NULL,
    api_secret_ciphertext TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_broker_credentials UNIQUE (tenant_id, user_id, provider)
);

CREATE INDEX IF NOT EXISTS idx_broker_credentials_lookup 
    ON broker_credentials (tenant_id, user_id, provider);

-- Table: broker_orders
-- Tracks execution lifecycle tied to Ativos intents.
CREATE TABLE IF NOT EXISTS broker_orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trade_intent_id UUID UNIQUE NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    provider VARCHAR(32) NOT NULL,
    provider_order_id VARCHAR(64),
    symbol VARCHAR(32) NOT NULL,
    side VARCHAR(16) NOT NULL,
    target_quantity NUMERIC(28, 8) NOT NULL,
    status VARCHAR(32) NOT NULL,
    filled_quantity NUMERIC(28, 8) NOT NULL DEFAULT 0,
    average_fill_price NUMERIC(28, 4) NOT NULL DEFAULT 0,
    idempotency_key VARCHAR(128),
    portfolio_id VARCHAR(64),
    livro_hold_id VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_broker_orders_provider_order_id 
    ON broker_orders (provider, provider_order_id);

CREATE INDEX IF NOT EXISTS idx_broker_orders_idempotency_key 
    ON broker_orders (tenant_id, idempotency_key);

CREATE INDEX IF NOT EXISTS idx_broker_orders_intent_id 
    ON broker_orders (trade_intent_id);
