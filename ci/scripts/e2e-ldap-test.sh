#!/usr/bin/env bash

# E2E test for LDAP authentication
# Tests both simple bind and search+bind authentication modes

set -euo pipefail

source ci/scripts/common.sh

# Cleanup function
cleanup_ldap() {
    echo "--- Cleaning up LDAP environment"

    # Stop RisingWave if running
    risedev ci-kill || true

    # Unset environment variables
    unset LDAPTLS_CACERT || true
    unset LDAPTLS_REQCERT || true
    unset LDAPTLS_CERT || true
    unset LDAPTLS_KEY || true
}

# Register cleanup function to run on exit
trap cleanup_ldap EXIT

while getopts 'p:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            ;;
    esac
done
shift $((OPTIND -1))

# Install LDAP utilities
echo "--- Install LDAP utilities"
apt-get update
apt-get install -y ldap-utils

# Additional check: Ensure STARTTLS is ready before proceeding
echo "--- Verifying STARTTLS availability"
MAX_RETRIES=10
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if timeout 5 openssl s_client -connect ldap-server:389 -starttls ldap </dev/null 2>&1 | grep -q "Verify return code"; then
        echo "✓ STARTTLS is ready"
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
        echo "STARTTLS not ready yet, retry $RETRY_COUNT/$MAX_RETRIES in 2 seconds..."
        sleep 2
    else
        echo "⚠ WARNING: STARTTLS check failed after $MAX_RETRIES attempts, but continuing anyway..."
        echo "The test may fail if STARTTLS is not ready"
    fi
done

# Verify LDAP users are created
echo "--- Verifying LDAP users"
ldapsearch -x -H ldap://ldap-server:389 -b "ou=people,dc=example,dc=com" -D "cn=admin,dc=example,dc=com" -w "admin123" "(uid=testuser1)" || {
    echo "Failed to find testuser1 in LDAP"
    exit 1
}

# Add test user password to LDAP (LDIF format uses SSHA which is auto-generated, we need to set plain passwords)
echo "--- Setting up LDAP user password"
ldappasswd -x -H ldap://ldap-server:389 -D "cn=admin,dc=example,dc=com" -w "admin123" -s "testpass1" "uid=testuser1,ou=people,dc=example,dc=com"

echo "--- LDAP user configured successfully"

# Download and prepare RisingWave
download_and_prepare_rw "$profile" source

# Export CA certificate for RisingWave to trust LDAP TLS
echo "--- Configuring LDAP TLS certificates"
CERT_DIR="$(pwd)/ci/ldap-test/certs"
echo "Certificate directory: $CERT_DIR"

echo "--- Testing LDAP TLS connection"
export LDAPTLS_CACERT="$CERT_DIR/ca.crt"
export LDAPTLS_REQCERT="demand"

risedev ci-start ci-ldap-simple-bind

risedev psql-env
source .risingwave/config/psql-env

echo "--- Running LDAP authentication tests"
sqllogictest -p 4566 -d dev './e2e_test/ldap/ldap_auth.slt'

echo "--- Stopping RisingWave cluster"
risedev ci-kill

# Test 2: Simple Bind with client certificate
echo "--- Test 2: Simple Bind (with client cert)"
export LDAPTLS_CERT="$CERT_DIR/client.crt"
export LDAPTLS_KEY="$CERT_DIR/client.key"
risedev ci-start ci-ldap-simple-bind

echo "--- Running LDAP authentication tests"
sqllogictest -p 4566 -d dev './e2e_test/ldap/ldap_auth.slt'

echo "--- Stopping RisingWave cluster"
risedev ci-kill

# Unset client certificate for next test
unset LDAPTLS_CERT
unset LDAPTLS_KEY

# Test 3: Search and Bind without client certificate
echo "--- Test 3: Search and Bind (without client cert)"
risedev ci-start ci-ldap-search-bind

echo "--- Running LDAP authentication tests"
sqllogictest -p 4566 -d dev './e2e_test/ldap/ldap_auth.slt'

echo "--- Stopping RisingWave cluster"
risedev ci-kill

# Test 4: Search and Bind with client certificate
echo "--- Test 4: Search and Bind (with client cert)"
export LDAPTLS_CERT="$CERT_DIR/client.crt"
export LDAPTLS_KEY="$CERT_DIR/client.key"
risedev ci-start ci-ldap-search-bind

echo "--- Running LDAP authentication tests"
sqllogictest -p 4566 -d dev './e2e_test/ldap/ldap_auth.slt'

echo "--- Stopping RisingWave cluster"
risedev ci-kill

# Unset client certificate environment variables
unset LDAPTLS_CERT
unset LDAPTLS_KEY

# Test 5: Simple Bind with URL format (without client cert)
echo "--- Test 5: Simple Bind URL format (without client cert)"
risedev ci-start ci-ldap-simple-bind-url

echo "--- Running LDAP authentication tests"
sqllogictest -p 4566 -d dev './e2e_test/ldap/ldap_auth.slt'

echo "--- Stopping RisingWave cluster"
risedev ci-kill

# Test 6: Search and Bind with URL format (without client cert)
echo "--- Test 6: Search and Bind URL format (without client cert)"
risedev ci-start ci-ldap-search-bind-url

echo "--- Running LDAP authentication tests"
sqllogictest -p 4566 -d dev './e2e_test/ldap/ldap_auth.slt'

echo "--- Stopping RisingWave cluster"
risedev ci-kill

# Test 7: Search and Bind with search filter (without client cert)
echo "--- Test 7: Search and Bind with search filter"
risedev ci-start ci-ldap-search-bind-filter

echo "--- Running LDAP authentication tests"
sqllogictest -p 4566 -d dev './e2e_test/ldap/ldap_auth.slt'

echo "--- Stopping RisingWave cluster"
risedev ci-kill

echo "--- LDAP E2E tests completed successfully"
