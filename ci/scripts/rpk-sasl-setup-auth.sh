#!/bin/bash

set -euo pipefail

echo "Setting up Redpanda SASL authentication..."

rpk cluster health

# Create superuser admin
echo "Creating admin superuser..."
rpk security user create admin \
  --password=admin123 \
  --mechanism=SCRAM-SHA-256

# Create regular user
echo "Creating regular user..."
rpk -X sasl.mechanism=SCRAM-SHA-256 -X user=admin -X pass=admin123 security user create user1 \
  --password=password123 \
  --mechanism=SCRAM-SHA-256

# Create ACLs for the regular user
echo "Setting up ACLs for user1..."
rpk -X sasl.mechanism=SCRAM-SHA-256 -X user=admin -X pass=admin123 security acl create \
  --allow-principal=User:user1 \
  --operation=all \
  --topic="*"

rpk -X sasl.mechanism=SCRAM-SHA-256 -X user=admin -X pass=admin123 security acl create \
  --allow-principal=User:user1 \
  --operation=all \
  --group="*"

# List users
echo "Created users:"
rpk -X sasl.mechanism=SCRAM-SHA-256 -X user=admin -X pass=admin123 security user list

echo "Created ACLs:"
rpk -X sasl.mechanism=SCRAM-SHA-256 -X user=admin -X pass=admin123 security acl list

rpk -X sasl.mechanism=SCRAM-SHA-256 -X user=admin -X pass=admin123 cluster config set enable_sasl true

echo ""
echo "✅ Setup complete!"
echo ""
echo "Connection details:"
echo "  Bootstrap servers: localhost:9092"
echo "  Admin user: admin / admin123"
echo "  Regular user: user1 / password123"
echo "  SASL mechanism: SCRAM-SHA-256"
echo ""
echo "Example client configuration:"
echo "  sasl.mechanism=SCRAM-SHA-256"
echo "  sasl.username=user1"
echo "  sasl.password=password123"
echo "  security.protocol=SASL_PLAINTEXT"
