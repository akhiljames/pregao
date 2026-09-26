-- Migration 002: Transition broker_credentials to Tenant-Level (Omnibus)
-- Removes user_id requirement from broker_credentials table.

ALTER TABLE broker_credentials DROP CONSTRAINT IF EXISTS uq_broker_credentials;
DROP INDEX IF EXISTS idx_broker_credentials_lookup;

-- Drop user_id column from broker_credentials table
ALTER TABLE broker_credentials DROP COLUMN IF EXISTS user_id;

-- Enforce one credential set per tenant per provider
ALTER TABLE broker_credentials 
    ADD CONSTRAINT uq_broker_credentials UNIQUE (tenant_id, provider);

CREATE INDEX IF NOT EXISTS idx_broker_credentials_lookup 
    ON broker_credentials (tenant_id, provider);
