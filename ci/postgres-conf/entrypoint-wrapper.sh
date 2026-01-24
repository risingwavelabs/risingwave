#!/bin/sh
set -euo pipefail

# Install openssl if not present (Postgres alpine image)
if ! command -v openssl >/dev/null 2>&1; then
    echo "Installing openssl..."
    apk add --no-cache openssl
fi

PG_CERT_DIR="/etc/postgresql"
mkdir -p "$PG_CERT_DIR"
SERVER_KEY="$PG_CERT_DIR/server.key"
SERVER_CRT="$PG_CERT_DIR/server.crt"

# Generate self-signed cert if key doesn't exist
if [ ! -f "$SERVER_KEY" ]; then
    echo "Generating self-signed SSL certificate..."
    # Generate a key and self-signed certificate valid for 365 days
    openssl req -new -newkey rsa:2048 -days 365 -nodes -x509 \
        -subj "/C=US/ST=CA/L=SanFrancisco/O=RisingWave/CN=localhost" \
        -keyout "$SERVER_KEY" -out "$SERVER_CRT"
fi

# Set correct permissions and ownership
chmod 600 "$SERVER_KEY"
chown postgres:postgres "$SERVER_KEY"
# Ensure the cert is also owned by postgres (though readable by all is default 644)
chown postgres:postgres "$SERVER_CRT"

# Execute the original entrypoint
exec /usr/local/bin/docker-entrypoint.sh "$@"
