#!/usr/bin/env bash
set -euo pipefail

cert_dir="${MYSQL_TLS_CERT_DIR:-/mysql-tls}"
mkdir -p "$cert_dir"
# Compose may restart this completed service. Keep the certificates already loaded by MySQL.
if [ -f "$cert_dir/.ready" ]; then
    openssl verify -CAfile "$cert_dir/ca.pem" "$cert_dir/server.pem"
    exit 0
fi
for name in ca untrusted-ca; do
    openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 2 \
        -subj "/CN=RisingWave MySQL TLS test $name" \
        -keyout "$cert_dir/$name.key" -out "$cert_dir/$name.pem"
done
openssl req -newkey rsa:2048 -nodes -sha256 -subj '/CN=mysql-tls' \
    -keyout "$cert_dir/server.key" -out "$cert_dir/server.csr"
cat > "$cert_dir/server.ext" <<'EXT'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:mysql-tls,DNS:localhost
EXT
openssl x509 -req -sha256 -days 2 -in "$cert_dir/server.csr" \
    -CA "$cert_dir/ca.pem" -CAkey "$cert_dir/ca.key" -CAcreateserial \
    -extfile "$cert_dir/server.ext" -out "$cert_dir/server.pem"
# This volume contains only disposable test identities. mysqld runs as a non-root user.
chmod 644 "$cert_dir/ca.pem" "$cert_dir/untrusted-ca.pem" "$cert_dir/server.pem" "$cert_dir/server.key"
rm -f "$cert_dir/ca.key" "$cert_dir/untrusted-ca.key" "$cert_dir/ca.srl" \
    "$cert_dir/server.csr" "$cert_dir/server.ext"
touch "$cert_dir/.ready"
