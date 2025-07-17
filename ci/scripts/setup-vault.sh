#!/usr/bin/env bash

set -euo pipefail

echo "Setting up HashiCorp Vault test data..."

# Set vault environment variables
export VAULT_ADDR="http://vault-server:8200"
export VAULT_TOKEN="root-token"

# Wait for Vault to be ready
echo "Waiting for Vault to be ready..."
timeout=60
while [ $timeout -gt 0 ]; do
    if curl -s -f "$VAULT_ADDR/v1/sys/health" > /dev/null 2>&1; then
        echo "Vault is ready!"
        break
    fi
    echo "Vault not ready yet, waiting... (${timeout}s remaining)"
    sleep 2
    timeout=$((timeout - 2))
done

if [ $timeout -le 0 ]; then
    echo "ERROR: Vault failed to become ready within 60 seconds"
    exit 1
fi

# Check if kv-v2 secrets engine is already enabled
if ! curl -s -H "X-Vault-Token: $VAULT_TOKEN" "$VAULT_ADDR/v1/sys/mounts" | grep -q '"secret/"'; then
    echo "Enabling KV v2 secrets engine..."
    curl -s -H "X-Vault-Token: $VAULT_TOKEN" \
         -H "Content-Type: application/json" \
         -X POST \
         -d '{"type": "kv", "options": {"version": "2"}}' \
         "$VAULT_ADDR/v1/sys/mounts/secret"
else
    echo "KV v2 secrets engine already enabled"
fi

# Create test secrets
echo "Creating test secrets..."

# Secret for database credentials
curl -s -H "X-Vault-Token: $VAULT_TOKEN" \
     -H "Content-Type: application/json" \
     -X POST \
     -d '{
       "data": {
         "username": "testuser",
         "password": "testpass123",
         "host": "localhost",
         "port": "5432"
       }
     }' \
     "$VAULT_ADDR/v1/secret/data/myapp/db"

# Secret for API keys
curl -s -H "X-Vault-Token: $VAULT_TOKEN" \
     -H "Content-Type: application/json" \
     -X POST \
     -d '{
       "data": {
         "key": "test-api-key-12345",
         "secret": "test-api-secret-67890"
       }
     }' \
     "$VAULT_ADDR/v1/secret/data/myapp/api_key"

# Secret for webhook
curl -s -H "X-Vault-Token: $VAULT_TOKEN" \
     -H "Content-Type: application/json" \
     -X POST \
     -d '{
       "data": {
         "signing_key": "webhook-secret-key"
       }
     }' \
     "$VAULT_ADDR/v1/secret/data/myapp/webhook"

# Enable approle auth method if not already enabled
if ! curl -s -H "X-Vault-Token: $VAULT_TOKEN" "$VAULT_ADDR/v1/sys/auth" | grep -q '"approle/"'; then
    echo "Enabling AppRole authentication..."
    curl -s -H "X-Vault-Token: $VAULT_TOKEN" \
         -H "Content-Type: application/json" \
         -X POST \
         -d '{"type": "approle"}' \
         "$VAULT_ADDR/v1/sys/auth/approle"
else
    echo "AppRole authentication already enabled"
fi

# Create a policy for test access
curl -s -H "X-Vault-Token: $VAULT_TOKEN" \
     -H "Content-Type: application/json" \
     -X PUT \
     -d '{
       "policy": "path \"secret/data/*\" {\n  capabilities = [\"read\"]\n}"
     }' \
     "$VAULT_ADDR/v1/sys/policies/acl/test-policy"

# Create an approle
curl -s -H "X-Vault-Token: $VAULT_TOKEN" \
     -H "Content-Type: application/json" \
     -X POST \
     -d '{
       "token_policies": ["test-policy"],
       "token_ttl": "1h",
       "token_max_ttl": "4h"
     }' \
     "$VAULT_ADDR/v1/auth/approle/role/test-role"

# Get role ID and secret ID for testing
ROLE_ID=$(curl -s -H "X-Vault-Token: $VAULT_TOKEN" \
               "$VAULT_ADDR/v1/auth/approle/role/test-role/role-id" | \
               grep -o '"role_id":"[^"]*"' | cut -d'"' -f4)

SECRET_ID=$(curl -s -H "X-Vault-Token: $VAULT_TOKEN" \
                 -X POST \
                 "$VAULT_ADDR/v1/auth/approle/role/test-role/secret-id" | \
                 grep -o '"secret_id":"[^"]*"' | cut -d'"' -f4)

echo "Setup complete!"
echo "Root token: root-token"
echo "Test AppRole - Role ID: $ROLE_ID"
echo "Test AppRole - Secret ID: $SECRET_ID"

# Store these values in environment variables for tests to use
export VAULT_TEST_ROLE_ID="$ROLE_ID"
export VAULT_TEST_SECRET_ID="$SECRET_ID"

echo "Vault setup completed successfully"