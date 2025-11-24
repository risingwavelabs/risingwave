#!/usr/bin/env bash
set -euo pipefail

# Certificate directory inside OpenLDAP container
CERT_DIR="${CERT_DIR:-/container/service/slapd/assets/certs}"

echo "--- Preparing certificate directory: $CERT_DIR ---"
mkdir -p "$CERT_DIR"

echo "--- Cleaning old certificates..."
rm -f "$CERT_DIR"/*.crt "$CERT_DIR"/*.key "$CERT_DIR"/*.srl "$CERT_DIR"/*.csr "$CERT_DIR"/*.cnf "$CERT_DIR"/.pem

# --- Generate DH Parameters ---
DH_PARAM_FILE="$CERT_DIR/dhparam.pem"
if [ ! -f "$DH_PARAM_FILE" ]; then
    echo "--- Generating DH parameters (this may take a while) ---"
    openssl dhparam -out "$DH_PARAM_FILE" 2048
    echo "✅ DH parameters generated at $DH_PARAM_FILE"
else
    echo "✅ DH parameters already exist, skipping generation"
fi

echo "--- Generating CA..."
openssl genrsa -out "$CERT_DIR/ca.key" 4096
openssl req -x509 -new -nodes \
    -key "$CERT_DIR/ca.key" \
    -sha256 \
    -days 365 \
    -out "$CERT_DIR/ca.crt" \
    -subj "/C=US/ST=State/L=City/O=Example Corp/CN=Example Root CA"

echo "--- Generating server certificate..."
openssl genrsa -out "$CERT_DIR/server.key" 4096

cat > "$CERT_DIR/server.cnf" <<'EOF'
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_server
prompt = no

[req_distinguished_name]
C = US
ST = State
L = City
O = Example Corp
CN = ldap-server

[v3_server]
subjectAltName = @alt_names
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
basicConstraints = CA:FALSE

[alt_names]
DNS.1 = ldap-server
DNS.2 = localhost
DNS.3 = ldap.example.com
IP.1  = 127.0.0.1
IP.2  = ::1
EOF

openssl req -new -key "$CERT_DIR/server.key" \
    -out "$CERT_DIR/server.csr" \
    -config "$CERT_DIR/server.cnf"

openssl x509 -req -in "$CERT_DIR/server.csr" \
    -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" \
    -CAcreateserial -out "$CERT_DIR/server.crt" \
    -days 365 -sha256 \
    -extfile "$CERT_DIR/server.cnf" -extensions v3_server

echo "--- Generating client certificate..."
openssl genrsa -out "$CERT_DIR/client.key" 4096

cat > "$CERT_DIR/client.cnf" <<'EOF'
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_client
prompt = no

[req_distinguished_name]
C = US
ST = State
L = City
O = Example Corp
CN = RisingWave Client
emailAddress = risingwave@example.com

[v3_client]
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
basicConstraints = CA:FALSE
EOF

openssl req -new -key "$CERT_DIR/client.key" \
    -out "$CERT_DIR/client.csr" \
    -config "$CERT_DIR/client.cnf"

openssl x509 -req -in "$CERT_DIR/client.csr" \
    -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" \
    -CAcreateserial -out "$CERT_DIR/client.crt" \
    -days 365 -sha256 \
    -extfile "$CERT_DIR/client.cnf" -extensions v3_client

# Cleanup
rm -f "$CERT_DIR"/*.csr "$CERT_DIR"/*.cnf "$CERT_DIR"/*.srl

chmod 644 "$CERT_DIR"/*.crt
chmod 600 "$CERT_DIR"/*.key

# Verify certificates
echo "--- Verifying certificates ---"
for f in "$CERT_DIR"/*.crt; do
    if [ ! -f "$f" ]; then
        echo "ERROR: $f not found!"
        exit 1
    fi
    openssl x509 -in "$f" -noout -subject -issuer -dates || {
        echo "ERROR: $f is invalid"
        exit 1
    }
done

echo "✅ All certificates generated and verified successfully"
