# MySQL CDC TLS

MySQL CDC uses the same `ssl.mode` and `ssl.root.cert` options for Java validation,
Debezium snapshots and streaming, Rust schema discovery and backfill, and upstream
binlog monitoring. Configure them on the shared CDC source; its tables inherit these options.

| `ssl.mode` | Encryption | Verify CA | Verify hostname |
| --- | --- | --- | --- |
| `disabled` (default) | No | No | No |
| `required` | Required | No | No |
| `verify-ca` | Required | Yes | No |
| `verify-full` | Required | Yes | Yes |

`preferred` is rejected because the Rust backfill/monitoring driver cannot provide
TLS-first, plaintext-fallback behavior. Use `disabled` or `required` explicitly.
An invalid mode is an error, including during monitoring; it never falls back to
plaintext.

`ssl.root.cert` is a path to a PEM file containing exactly one trusted CA
certificate. Certificate bundles are rejected because SQLx native TLS loads only
one certificate. Make the file available at that path on every node that connects to
MySQL. The supplied CA is added to each driver's platform/JVM default trust roots.
Without it, verification uses only those default roots.
It is accepted only with `verify-ca` or `verify-full`. An empty path is rejected.
In particular, supplying a CA with `required` is rejected because Debezium's binlog
client would verify it while the other drivers would not.

For compatibility, `debezium.database.sslmode` aliases `ssl.mode`, and
`debezium.database.sslrootcert` aliases `ssl.root.cert`. Specify either the
canonical name or its alias; specifying both is an error, even if their values
match. The older values `disable`, `require`, `verifyca`, and `verifyfull` are also
accepted. Values are case sensitive. Use the canonical hyphenated verification
names, not Java's `verify_ca` or `verify_identity`.

Raw `database.ssl.*` and `debezium.database.ssl.*` options (including
`database.ssl.mode` and `debezium.database.ssl.mode`) are rejected rather than
being applied to only Java connections. Java truststore/client-keystore options
are not a substitute for `ssl.root.cert`; client-certificate authentication is
not supported by this contract.

The Java connector translates verification modes to Connector/J's `VERIFY_CA` /
`VERIFY_IDENTITY` and Debezium's `verify_ca` / `verify_identity`. It converts PEM
CAs to a temporary JKS truststore shared by validation and Debezium, without
changing JVM-wide SSL settings. Truststores are cached by CA contents until JVM
exit; replacing a PEM file takes effect the next time the source configuration
is constructed. Existing connections must be restarted to pick up rotated CAs.

## Regression tests

`ci/mysql-tls/generate-certs.sh` creates disposable test certificates. The
`mysql-tls` Compose service requires TLS. Its `mysql-tls-wrong-host` alias points
to the same server but is absent from the certificate's SANs. The CDC CI lane
mounts the CA volume into RisingWave and runs
`e2e_test/source_inline/cdc/mysql/mysql_tls.slt.serial`. That test covers schema
inference, backfill, streaming, recovery, aliases, rejection cases, and successful
binlog monitoring for every TLS mode.

The CDC CI script also runs the ignored Rust `test_mysql_tls_connections` test.
It tests both SQLx and the shared mysql_async pool against the real TLS server,
including untrusted CAs and hostname mismatches. For a local fixture, set
`MYSQL_TLS_HOST`, `MYSQL_TLS_WRONG_HOST`, `MYSQL_TLS_PORT`, and
`MYSQL_TLS_CERT_DIR` and run:

```sh
cargo test -p risingwave_connector --lib test_mysql_tls_connections -- --ignored
mvn -f java/pom.xml -pl connector-node/risingwave-connector-service -am test \
  -Dno-build-rust -Dtest=MySqlSslConfigTest,MySqlTlsIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false
```

The SLT uses the same four TLS environment variables, plus the regular RiseDev
MySQL variables for its plaintext fixture. With RisingWave and both MySQL
fixtures running, execute it with:

```sh
./risedev slt e2e_test/source_inline/cdc/mysql/mysql_tls.slt.serial
```

The optional Java integration test uses the same environment variables to test
both validation and Debezium JDBC connections. On a local published Docker port,
use `localhost` as the matching host and `127.0.0.1` as the mismatching host.
