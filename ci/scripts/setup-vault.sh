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
    if vault status > /dev/null 2>&1; then
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
if ! vault secrets list -format=json | jq -e '.["secret/"]' > /dev/null 2>&1; then
    echo "Enabling KV v2 secrets engine..."
    vault secrets enable -version=2 -path=secret kv
else
    echo "KV v2 secrets engine already enabled"
fi

# Create test secrets
echo "Creating test secrets..."

# Secret for database credentials
vault kv put secret/myapp/db \
  username="testuser" \
  password="testpass123" \
  host="localhost" \
  port="5432"

# Secret for API keys
vault kv put secret/myapp/api_key \
  key="test-api-key-12345" \
  secret="test-api-secret-67890"

# Secret for webhook
vault kv put secret/myapp/webhook \
  signing_key="webhook-secret-key"

# Enable approle auth method if not already enabled
if ! vault auth list -format=json | jq -e '.["approle/"]' > /dev/null 2>&1; then
    echo "Enabling AppRole authentication..."
    vault auth enable approle
else
    echo "AppRole authentication already enabled"
fi

# Create a policy for test access
vault policy write test-policy - <<EOF
path "secret/data/*" {
  capabilities = ["read"]
}
EOF

# Create an approle
vault write auth/approle/role/test-role \
  token_policies="test-policy" \
  token_ttl=1h \
  token_max_ttl=4h

# Get role ID and secret ID for testing
ROLE_ID=$(vault read -field=role_id auth/approle/role/test-role/role-id)
SECRET_ID=$(vault write -field=secret_id -f auth/approle/role/test-role/secret-id)

echo "Setup complete!"
echo "Root token: root-token"
echo "Test AppRole - Role ID: $ROLE_ID"
echo "Test AppRole - Secret ID: $SECRET_ID"

# Store these values in environment variables for tests to use
export VAULT_TEST_ROLE_ID="$ROLE_ID"
export VAULT_TEST_SECRET_ID="$SECRET_ID"

echo "Vault setup completed successfully"