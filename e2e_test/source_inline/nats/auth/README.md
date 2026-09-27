# NATS authentication tests

`../authentication.slt.serial` checks source reads, sink writes, and rejection of
a valid but unauthorized NKey seed. Run it twice: once against a server with
static NKey authentication, then against an operator/JWT server. The second run
also rejects a seed that does not match the user JWT.

The servers use the same test user key so the JWT run cannot pass by accidentally
switching to bare NKey authentication. The JWT server requires a user JWT.
JetStream resources are reset before each run and deleted after successful tests.
The shared `../operation.py` helper provides the `auth_setup`, `auth_check_sink`,
and `auth_cleanup` commands used by the SLT.
After an interrupted run, drop the test's RisingWave tables/sinks or use a fresh
database before rerunning.

All seeds and JWTs in this directory are **public test credentials**. JWTs use a
fixed issuance date of 2026-09-27 UTC and have no expiration. The generated
`credentials.env`, `nkey.conf`, and `jwt.conf` files must be checked in together
so CI can start the authentication servers before running the tests.
To regenerate these files with the Python dependencies from
`e2e_test/source_inline/requirements.txt` installed:

```sh
python3 e2e_test/source_inline/nats/auth/generate_auth.py
```

## Run locally

From the repository root, start RisingWave and the two authentication servers:

```sh
./risedev d local-nats-test
docker compose -p rw-nats-auth-test -f e2e_test/source_inline/nats/auth/docker-compose.yml up -d
source e2e_test/source_inline/nats/auth/credentials.env
export UV_PYTHON=3.11

export NATS_AUTH_MODE=nkey NATS_AUTH_URL=nats://127.0.0.1:4223 NATS_AUTH_JWT_OPTION=''
./risedev slt './e2e_test/source_inline/nats/authentication.slt.serial'

export NATS_AUTH_MODE=jwt NATS_AUTH_URL=nats://127.0.0.1:4224
export NATS_AUTH_JWT_OPTION=", jwt = '${NATS_AUTH_JWT}'"
./risedev slt './e2e_test/source_inline/nats/authentication.slt.serial'

docker compose -p rw-nats-auth-test -f e2e_test/source_inline/nats/auth/docker-compose.yml down
./risedev k
```

`NATS_AUTH_URL` is used by both the SLT and the Python helper; adjust it when using
remote servers or different port mappings.

## CI coverage

`ci/scripts/e2e-source-nats-test.sh` runs both modes explicitly. Its environment
starts `nats-nkey` and `nats-jwt` from `ci/docker-compose.yml`, using the same
configuration files as the local Compose file. The PR lane is selected by
`ci/run-e2e-nats-source-tests`. 
