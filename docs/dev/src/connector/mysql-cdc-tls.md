# MySQL CDC TLS

Configure `ssl.mode` on the shared CDC source; its tables inherit the mode for
Java validation, Debezium snapshots/streaming, Rust schema discovery/backfill,
and binlog monitoring.

| `ssl.mode` | Encryption | Verify CA | Verify hostname |
| --- | --- | --- | --- |
| `disabled` (default) | No | No | No |
| `preferred` | When the server supports TLS | No | No |
| `required` | Required | No | No |
| `verify-ca` | Required | Yes | No |
| `verify-full` | Required | Yes | Yes |

`preferred` connects with TLS when the server advertises TLS support, otherwise
in plaintext. It does not verify the CA or hostname. The Rust pool retries
without TLS only when the server lacks the MySQL `CLIENT_SSL` capability, never
on TLS handshake, authentication, or network errors. It checks on every
acquisition instead of permanently selecting plaintext after one fallback.
Previously the Rust pool always used plaintext for this mode; backfill, primary
key discovery, and monitoring can now also connect to TLS-required servers.

Verification uses each driver's default platform/JVM trust roots. The Java
connector translates verification modes to Connector/J's `VERIFY_CA` /
`VERIFY_IDENTITY` and Debezium's `verify_ca` / `verify_identity`.

## Compatibility

MySQL CDC continues to **ignore `ssl.root.cert` and
`debezium.database.sslrootcert` in every mode**, as it did before this change.
The shared Rust configuration previously parsed these fields but neither MySQL
Rust driver used them, and the Java handling of `ssl.root.cert` was PostgreSQL
specific. Empty paths, missing files, malformed files, and certificate bundles
are not loaded or validated. Supplying a CA this way does not make a private CA
trusted and does not enable verification. PostgreSQL behavior is unchanged.

`debezium.database.sslmode` aliases `ssl.mode`; the canonical `ssl.mode` takes
precedence when both are supplied. Mode values are case insensitive, preserving
Connector/J spellings. Accepted aliases include `disable`, `prefer`, `require`,
`verifyca`, `verifyfull`, `verify_ca`, and `verify_identity`. Unknown modes still
fail validation.

Raw options retain their existing behavior rather than being newly rejected:

- `database.ssl.*` SQL options are not wired directly to the MySQL drivers.
- `debezium.database.ssl.*` options still pass through to Debezium when they do
  not override a generated property. Existing Java truststore/keystore settings
  remain Java-only and are not copied to Rust or validation connections.
- `debezium.database.ssl.mode` cannot override the generated mode, including its
  `disabled` default, because the existing merge uses `putIfAbsent`.

This change does not introduce a MySQL PEM CA-file option or generate Java
truststores. A future custom-CA feature needs a separate compatibility design.

## Regression tests

The `mysql-tls` Compose service requires TLS using MySQL's generated certificates;
`mysql-no-tls` disables TLS. The CDC CI lane runs
`e2e_test/source_inline/cdc/mysql/mysql_tls.slt.serial`, covering inferred schemas,
backfill, streaming, monitoring, and recovery for `required` and both `preferred`
connection paths. It also checks that verification modes reject the untrusted
server certificate. Rust and Java unit tests cover mode aliases, option precedence,
ignored root-cert options, and driver configuration.

With RisingWave and the two MySQL fixtures running, set `MYSQL_TLS_HOST`,
`MYSQL_TLS_PORT`, `MYSQL_NO_TLS_HOST`, and `MYSQL_NO_TLS_PORT`, then run:

```sh
./risedev slt e2e_test/source_inline/cdc/mysql/mysql_tls.slt.serial
```
