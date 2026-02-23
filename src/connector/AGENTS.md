# AGENTS.md - Connector Crate

## 1. Scope

Policies for the connector crate (`src/connector/`), which implements source connectors (data ingestion) and sink connectors (data egress) for RisingWave.

## 2. Purpose

The connector crate provides the unified connector framework for RisingWave, enabling data ingestion from external systems (Kafka, Pulsar, Kinesis, CDC databases, filesystems, etc.) and data egress to downstream systems (databases, data lakes, message queues, search engines, etc.). It handles protocol parsing, serialization/deserialization, and connector lifecycle management.

## 3. Structure

```
src/connector/
├── src/
│   ├── lib.rs                    # Crate root, shared deserialization helpers
│   ├── error.rs                  # ConnectorError type definitions
│   ├── enforce_secret.rs         # Secret enforcement for sensitive options
│   ├── macros.rs                 # Feature-gated module macros
│   ├── with_options.rs           # Options trait implementations
│   ├── allow_alter_on_fly_fields.rs  # Auto-generated: alterable field tracking
│   ├── source/                   # Source connectors (data ingestion)
│   │   ├── base.rs               # Source trait definitions
│   │   ├── cdc/                  # CDC sources (Postgres, MySQL, MongoDB, SQL Server, Citus)
│   │   ├── kafka/                # Apache Kafka source
│   │   ├── pulsar/               # Apache Pulsar source
│   │   ├── kinesis/              # AWS Kinesis source
│   │   ├── nats/                 # NATS source
│   │   ├── mqtt/                 # MQTT source
│   │   ├── google_pubsub/        # Google Cloud Pub/Sub source
│   │   ├── filesystem/           # File-based sources (S3, GCS, Azure Blob, POSIX)
│   │   ├── iceberg/              # Apache Iceberg source
│   │   ├── datagen/              # Test data generator source
│   │   ├── nexmark/              # Nexmark benchmark source
│   │   └── adbc_snowflake/       # Snowflake via ADBC (feature-gated)
│   ├── sink/                     # Sink connectors (data egress)
│   │   ├── catalog/              # Sink catalog types
│   │   ├── formatter/            # Output formatters (JSON, Debezium, Upsert, Append-only)
│   │   ├── encoder/              # Data encoders (JSON, Avro, Protobuf, BSON, Template)
│   │   ├── file_sink/            # File-based sinks (S3, GCS, WebHDFS, POSIX)
│   │   ├── iceberg/              # Apache Iceberg sink
│   │   ├── elasticsearch_opensearch/  # Elasticsearch & OpenSearch sinks
│   │   ├── snowflake_redshift/   # Snowflake & Redshift sinks
│   │   ├── kafka.rs              # Kafka sink
│   │   ├── pulsar.rs             # Pulsar sink
│   │   ├── redis.rs              # Redis sink
│   │   ├── mongodb.rs            # MongoDB sink
│   │   ├── postgres.rs           # PostgreSQL sink
│   │   ├── clickhouse.rs         # ClickHouse sink (feature-gated)
│   │   ├── big_query.rs          # BigQuery sink (feature-gated)
│   │   ├── deltalake.rs          # Delta Lake sink (feature-gated)
│   │   ├── doris.rs              # Doris sink (feature-gated)
│   │   ├── starrocks.rs          # StarRocks sink (feature-gated)
│   │   └── ...
│   ├── parser/                   # Message parsers
│   │   ├── json_parser.rs        # JSON format parser
│   │   ├── avro/                 # Apache Avro parser
│   │   ├── protobuf/             # Protocol Buffers parser
│   │   ├── csv_parser.rs         # CSV parser
│   │   ├── bytes_parser.rs       # Raw bytes parser
│   │   ├── parquet_parser.rs     # Apache Parquet parser
│   │   ├── debezium/             # Debezium CDC parsers
│   │   ├── canal/                # Alibaba Canal parsers
│   │   ├── maxwell/              # Maxwell CDC parsers
│   │   ├── upsert_parser.rs      # Upsert envelope parser
│   │   └── plain_parser.rs       # Plain format parser
│   ├── schema/                   # Schema resolution & registry
│   │   ├── schema_registry/      # Confluent Schema Registry client
│   │   ├── avro.rs               # Avro schema handling
│   │   └── protobuf.rs           # Protobuf schema handling
│   └── connector_common/         # Shared connector utilities
│       ├── common.rs             # Common connection properties
│       ├── connection.rs         # Connection management
│       ├── postgres.rs           # PostgreSQL common code
│       └── iceberg/              # Iceberg common code
├── codec/                        # Separate codec crate (encoding/decoding)
│   ├── src/
│   │   ├── decoder/              # Decoders (Avro, JSON, Protobuf)
│   │   └── common/               # Common codec utilities
│   └── tests/integration_tests/  # Integration tests
├── with_options/                 # Proc-macro crate for options
│   └── src/lib.rs                # WithOptions derive macro
├── benches/                      # Benchmarks
│   ├── debezium_json_parser.rs
│   ├── json_parser_case_insensitive.rs
│   ├── json_vs_plain_parser.rs
│   ├── protobuf_bench.rs
│   └── nexmark_integration.rs
├── with_options_source.yaml      # Auto-generated: source connector options
├── with_options_sink.yaml        # Auto-generated: sink connector options
└── with_options_connection.yaml  # Auto-generated: connection options
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `Cargo.toml` | Crate manifest with feature flags for optional connectors |
| `src/lib.rs` | Crate root with shared deserialization utilities |
| `src/error.rs` | Unified `ConnectorError` type with `def_anyhow_newtype!` macro |
| `src/enforce_secret.rs` | Enforces secret usage for sensitive connector options |
| `src/source/base.rs` | Core `SplitEnumerator` and `SplitReader` traits |
| `src/sink/mod.rs` | Core `Sink` and `SinkWriter` traits, feature-gated modules |
| `src/parser/mod.rs` | Core `ByteStreamSourceParser` and `ParseResult` types |
| `src/macros.rs` | `feature_gated_source_mod!` and `feature_gated_sink_mod!` macros |
| `src/with_options.rs` | `WithOptions` trait and `Get` trait implementations |
| `with_options_source.yaml` | Auto-generated source connector options metadata |
| `with_options_sink.yaml` | Auto-generated sink connector options metadata |
| `src/allow_alter_on_fly_fields.rs` | Auto-generated alterable fields tracking |

## 5. Edit Rules (Must)

- Use `feature_gated_sink_mod!` macro for optional sink connectors (see existing patterns)
- Use `feature_gated_source_mod!` macro for optional source connectors
- Add `#[with_option(allow_alter_on_fly)]` attribute for runtime-alterable options
- Implement `EnforceSecret` trait for sensitive connector options (passwords, tokens)
- Run `./risedev generate-with-options` after adding/modifying connector options
- Run `cargo fmt` before committing
- Pass `./risedev c` (clippy) before submitting changes
- Write all code comments in English
- Add unit tests for new public functions in connector implementations
- Follow existing connector patterns: implement `SplitEnumerator`, `SplitReader` for sources; implement `Sink` and `SinkWriter` for sinks

## 6. Forbidden Changes (Must Not)

- Modify `with_options_*.yaml` files manually (auto-generated via `./risedev generate-with-options`)
- Modify `src/allow_alter_on_fly_fields.rs` manually (auto-generated)
- Commit hardcoded credentials, API keys, or secrets in connector code
- Remove existing feature gates from optional connectors without explicit approval
- Break backward compatibility for existing connector options without migration path
- Use blocking I/O operations in async connector code
- Panic in connector code - always return proper errors via `ConnectorError`

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_connector` |
| Codec tests | `cargo test -p risingwave_connector_codec` |
| Integration tests | `cargo test -p risingwave_connector --test integration_tests` |
| Benchmarks | `cargo bench -p risingwave_connector` |
| Clippy check | `./risedev c` |
| Generate options | `./risedev generate-with-options` |

## 8. Dependencies & Contracts

- External SDKs: `rdkafka`, `pulsar`, `aws-sdk-*`, `redis`, `mongodb`, `clickhouse`, `elasticsearch`, `opensearch`, `deltalake`, `iceberg`
- Protocol parsing: `apache-avro`, `prost`, `simd-json`, `csv`, `parquet`
- Serialization: `serde`, `serde_json`
- Async runtime: `tokio` (via `madsim-tokio`)
- Internal: `risingwave_common`, `risingwave_pb`, `risingwave_rpc_client`, `risingwave_jni_core`
- Feature flags control optional heavy dependencies (bigquery, clickhouse, deltalake, doris, starrocks, google_pubsub, adbc_snowflake)
- Codec crate (`risingwave_connector_codec`) is a separate workspace member for encoding/decoding logic

## 9. Overrides

None. Follows root AGENTS.md policies.

## 10. Update Triggers

Regenerate this file when:
- New connector type added (source or sink)
- Feature flags for connectors changed
- Options generation process modified
- New auto-generated files added
- Testing framework for connectors updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/AGENTS.md
