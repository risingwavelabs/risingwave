# LDAP Authentication E2E Tests

This directory contains end-to-end tests for LDAP authentication in RisingWave.

**Note: These tests are designed to run in CI only. The LDAP server is managed by `ci/docker-compose.yml`.**

## Overview

These tests verify that RisingWave can authenticate users against an LDAP server using two authentication modes:

1. **Simple Bind**: Uses `ldapprefix` and `ldapsuffix` to construct the DN directly
2. **Search and Bind**: Uses `ldapbasedn`, `ldapsearchattribute`, `ldapbinddn`, and `ldapbindpasswd` to search for the user first, then bind

Each authentication mode is tested with and without client certificates (mutual TLS), and with both legacy parameter format and URL format.

## Directory Structure

```
ci/
├── docker-compose.yml               # Contains ldap-server service
└── ldap-test/
    ├── setup-ldap-certs.sh          # Script to generate TLS certificates
    ├── ldif/
    │   └── 01-users.ldif            # LDAP test users definition
    ├── certs/                       # Generated TLS certificates (gitignored, auto-generated)
    └── README.md                    # This file

src/config/
├── ci-ldap-simple-bind.toml         # RisingWave config for simple bind
├── ci-ldap-search-bind.toml         # RisingWave config for search+bind
├── ci-ldap-simple-bind-url.toml     # RisingWave config for simple bind (URL format)
└── ci-ldap-search-bind-url.toml     # RisingWave config for search+bind (URL format)

e2e_test/ldap/
└── ldap_auth.slt                    # LDAP authentication tests (reused for all scenarios)

ci/scripts/
└── e2e-ldap-test.sh                 # Test script
```

## Test Users

One test user is created in LDAP:

- `testuser1` with password `testpass1`

The user is under the organizational unit: `ou=people,dc=example,dc=com`

## How It Works

### Certificate Generation

Certificates are generated **inside the ldap-server container** during startup:

1. The `ldap-server` entrypoint runs `setup-ldap-certs.sh` with `CERT_DIR=/container/service/slapd/assets/certs`
2. Certificates (CA, server, client) are generated directly at the location where OpenLDAP expects them
3. The same directory is bind-mounted to the host at `./ldap-test/certs/` so test scripts can access them
4. OpenLDAP then starts using the generated certificates

### Test Execution

The test script `ci/scripts/e2e-ldap-test.sh` performs minimal setup:

1. Installs `ldap-utils` for user management
2. Waits for STARTTLS to be available (verifies TLS is ready)
3. Verifies test user exists in LDAP
4. Sets the test user's password
5. Exports environment variables for RisingWave:
   - `LDAPTLS_CACERT`: Path to CA certificate
   - `LDAPTLS_REQCERT`: Set to "demand" for strict verification
   - `LDAPTLS_CERT` / `LDAPTLS_KEY`: Client certificate (for mutual TLS tests)
6. Runs 6 test scenarios

### Test Scenarios

1. Simple Bind without client certificate
2. Simple Bind with client certificate
3. Search and Bind without client certificate
4. Search and Bind with client certificate
5. Simple Bind URL format without client certificate
6. Search and Bind URL format without client certificate

Each scenario:
- Starts RisingWave with the appropriate config file
- Runs the same SQLLogicTest suite (`e2e_test/ldap/ldap_auth.slt`)
- Tests failure scenarios (wrong password, non-existent user, LDAP injection)
- Stops RisingWave

## RisingWave LDAP TLS Configuration

RisingWave reads LDAP TLS configuration from environment variables:

- **`LDAPTLS_CACERT`**: Path to CA certificate file (required for self-signed certificates)
- **`LDAPTLS_CERT`**: Path to client certificate file (optional, for mutual TLS)
- **`LDAPTLS_KEY`**: Path to client private key file (optional, for mutual TLS)
- **`LDAPTLS_REQCERT`**: Certificate verification policy (`never`, `allow`, `try`, `demand`)

These environment variables must be set **before** starting RisingWave:

```bash
export LDAPTLS_CACERT="$(pwd)/ci/ldap-test/certs/ca.crt"
export LDAPTLS_REQCERT="demand"
risedev ci-start ci-ldap-simple-bind
```

If `LDAPTLS_CACERT` is not set, RisingWave will use the system's native certificate store, which won't include our self-signed CA certificate, causing authentication to fail with `invalid peer certificate` errors.

## Local Development

### Prerequisites

Add hostname to `/etc/hosts`:

```bash
sudo bash -c 'echo "127.0.0.1 ldap-server" >> /etc/hosts'
```

This allows using the `ldap-server` hostname (matching the certificate CN) both locally and in CI.

### Starting the LDAP Server

```bash
# From the repository root
docker-compose -f ci/docker-compose.yml up -d ldap-server
```

This will:
1. Generate TLS certificates inside the container
2. Start OpenLDAP with STARTTLS support
3. Load test users from `ldif/01-users.ldif`

### Setting Test User Password

```bash
ldappasswd -x -H ldap://ldap-server:389 \
    -D "cn=admin,dc=example,dc=com" -w "admin123" \
    -s "testpass1" "uid=testuser1,ou=people,dc=example,dc=com"
```

### Testing LDAP Connection

```bash
export LDAPTLS_CACERT="$(pwd)/ci/ldap-test/certs/ca.crt"
export LDAPTLS_REQCERT="demand"

# Test with ldapwhoami (may need Homebrew openldap on macOS)
ldapwhoami -x -H ldap://ldap-server:389 -ZZ \
    -D "cn=admin,dc=example,dc=com" -w "admin123"
```

**macOS Note**: Install OpenLDAP via Homebrew for proper TLS support:
```bash
brew install openldap
# Use: /opt/homebrew/opt/openldap/bin/ldapwhoami
```

The macOS system LDAP client may not work properly with STARTTLS.

### Cleanup

```bash
docker-compose -f ci/docker-compose.yml down
rm -rf ci/ldap-test/certs/
```

## Implementation Details

The `setup-ldap-certs.sh` script generates:

- **CA Certificate** (`ca.crt`, `ca.key`): Self-signed root CA
- **Server Certificate** (`server.crt`, `server.key`):
  - CN: `ldap-server`
  - SAN: `DNS:ldap-server`, `DNS:localhost`, `DNS:ldap.example.com`, `IP:127.0.0.1`, `IP:::1`
  - Key Usage: digitalSignature, keyEncipherment
  - Extended Key Usage: serverAuth
- **Client Certificate** (`client.crt`, `client.key`):
  - CN: `RisingWave Client`
  - Key Usage: digitalSignature, keyEncipherment
  - Extended Key Usage: clientAuth
- **DH Parameters** (`dhparam.pem`): 2048-bit, reused if already exists

All certificates use RSA 4096-bit keys and SHA256 signatures.
