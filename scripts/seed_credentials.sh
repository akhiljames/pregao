#!/bin/bash
set -e

# ==============================================================================
# Pregão: Secure Broker Credential Seeder (OpenBao Transit + PostgreSQL)
# ==============================================================================

OPENBAO_ADDR="${OPENBAO_ADDR:-http://localhost:8200}"
OPENBAO_TOKEN="${OPENBAO_TOKEN:-root}"
OPENBAO_KEY="${OPENBAO_TRANSIT_KEY:-broker-keys}"
PG_CONTAINER="${PG_CONTAINER:-pregao-postgres}"
PG_USER="${PG_USER:-postgres}"
PG_DB="${PG_DB:-pregao}"

TENANT_ID="${1:-${TENANT_ID}}"
USER_ID="${2:-${USER_ID}}"
BINANCE_KEY="${3:-${BINANCE_API_KEY}}"
BINANCE_SECRET="${4:-${BINANCE_API_SECRET}}"

if [ -z "$TENANT_ID" ] || [ -z "$USER_ID" ]; then
    echo "Usage: $0 <tenant_id> <user_id> [binance_api_key] [binance_api_secret]"
    echo "Or set environment variables: TENANT_ID, USER_ID, BINANCE_API_KEY, BINANCE_API_SECRET"
    exit 1
fi

if [ -z "$BINANCE_KEY" ]; then
    read -sp "Enter Binance API Key: " BINANCE_KEY
    echo
fi

if [ -z "$BINANCE_SECRET" ]; then
    read -sp "Enter Binance API Secret: " BINANCE_SECRET
    echo
fi

if [ -z "$BINANCE_KEY" ] || [ -z "$BINANCE_SECRET" ]; then
    echo "❌ Error: API Key and API Secret cannot be empty."
    exit 1
fi

echo "🔐 Encrypting credentials via OpenBao Transit at $OPENBAO_ADDR..."

# 1. Base64 encode for OpenBao Transit API
B64_KEY=$(echo -n "$BINANCE_KEY" | base64)
B64_SECRET=$(echo -n "$BINANCE_SECRET" | base64)

# 2. Encrypt API Key
ENC_KEY_RESP=$(curl -s -f -X POST "$OPENBAO_ADDR/v1/transit/encrypt/$OPENBAO_KEY" \
    -H "X-Vault-Token: $OPENBAO_TOKEN" \
    -d "{\"plaintext\": \"$B64_KEY\"}")

CIPHERTEXT_KEY=$(echo "$ENC_KEY_RESP" | grep -o '"ciphertext":"[^"]*' | cut -d'"' -f4)

if [ -z "$CIPHERTEXT_KEY" ]; then
    echo "❌ Failed to encrypt API Key via OpenBao: $ENC_KEY_RESP"
    exit 1
fi

# 3. Encrypt API Secret
ENC_SECRET_RESP=$(curl -s -f -X POST "$OPENBAO_ADDR/v1/transit/encrypt/$OPENBAO_KEY" \
    -H "X-Vault-Token: $OPENBAO_TOKEN" \
    -d "{\"plaintext\": \"$B64_SECRET\"}")

CIPHERTEXT_SECRET=$(echo "$ENC_SECRET_RESP" | grep -o '"ciphertext":"[^"]*' | cut -d'"' -f4)

if [ -z "$CIPHERTEXT_SECRET" ]; then
    echo "❌ Failed to encrypt API Secret via OpenBao: $ENC_SECRET_RESP"
    exit 1
fi

echo "✅ Successfully encrypted credentials into transit ciphertexts."

# 4. Insert / Update into pregao PostgreSQL database
echo "💾 Seeding credentials into $PG_CONTAINER ($PG_DB)..."

SQL_STMT="INSERT INTO broker_credentials (id, tenant_id, user_id, provider, api_key_ciphertext, api_secret_ciphertext, created_at)
VALUES (gen_random_uuid(), '$TENANT_ID', '$USER_ID', 'BINANCE', '$CIPHERTEXT_KEY', '$CIPHERTEXT_SECRET', NOW())
ON CONFLICT (tenant_id, user_id, provider)
DO UPDATE SET api_key_ciphertext = EXCLUDED.api_key_ciphertext,
              api_secret_ciphertext = EXCLUDED.api_secret_ciphertext,
              created_at = NOW();"

docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c "$SQL_STMT" >/dev/null

echo "🎉 Credentials successfully seeded for:"
echo "   Tenant ID : $TENANT_ID"
echo "   User ID   : $USER_ID"
echo "   Provider  : BINANCE"
echo "   Key Cipher: ${CIPHERTEXT_KEY:0:25}..."
