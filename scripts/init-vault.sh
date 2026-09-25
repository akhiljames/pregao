#!/bin/sh
set -e

echo "[INIT-VAULT] Waiting for OpenBao / Vault to be reachable..."
until nc -z openbao 8200; do
  sleep 1
done

echo "[INIT-VAULT] Enabling transit secrets engine..."
export VAULT_ADDR="http://openbao:8200"
export VAULT_TOKEN="root"

# Try enabling transit secrets engine (ignore error if already enabled)
vault secrets enable transit 2>/dev/null || true

# Create transit key for broker keys
vault write -f transit/keys/broker-keys 2>/dev/null || true

echo "[INIT-VAULT] Transit secrets engine and 'broker-keys' initialized successfully."
