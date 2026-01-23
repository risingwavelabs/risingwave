#!/usr/bin/env bash

set -euo pipefail

mkdir ./secrets
cd ./secrets

echo '--- 1. Generate CA ---'
openssl genrsa -out ca.key 2048
openssl req -new -x509 -key ca.key -out ca_root.cer -days 365 \
  -subj '/CN=MyCustomCA/OU=Test/O=Test/L=Test/C=SG'

echo '--- 2. Generate Server Key & CSR ---'
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr \
  -subj '/CN=mock-server/OU=Test/O=Test/L=Test/C=SG'

echo '--- 3. Sign Server Cert (PEM format is fine for Python) ---'
printf '[SAN]\nsubjectAltName=DNS:mock-server,DNS:localhost' > san.cnf
openssl x509 -req -CA ca_root.cer -CAkey ca.key -in server.csr \
  -out server.crt -days 365 -CAcreateserial \
  -extfile san.cnf -extensions SAN

echo '--- Permissions ---'
chmod 644 ca_root.cer server.crt server.key
