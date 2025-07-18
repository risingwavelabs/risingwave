# RisingWave Connector Feature Flags

This document describes the optional feature flags available for RisingWave connectors, which allow selective compilation to reduce build times and binary size.

## Sink Connectors with Feature Flags

The following sink connectors have been split into optional features to reduce compilation time when not needed:

| Connector | Feature Flag | Dependencies | Description |
|-----------|-------------|--------------|-------------|
| DeltaLake | `sink-deltalake` | `deltalake` | Apache Delta Lake format sink |
| Iceberg | `sink-iceberg` | `iceberg`, `iceberg-catalog-glue`, `iceberg-catalog-rest` | Apache Iceberg format sink |
| ClickHouse | `sink-clickhouse` | `clickhouse` | ClickHouse database sink |
| MongoDB | `sink-mongodb` | `mongodb` | MongoDB database sink |
| BigQuery | `sink-bigquery` | `gcp-bigquery-client`, `google-cloud-bigquery`, `google-cloud-gax`, `google-cloud-googleapis` | Google BigQuery sink |
| DynamoDB | `sink-dynamodb` | `aws-sdk-dynamodb` | AWS DynamoDB sink |
| ElasticSearch | `sink-elasticsearch` | `elasticsearch` | ElasticSearch sink |
| OpenSearch | `sink-opensearch` | `opensearch` | OpenSearch sink |

## Aggregate Features

- `all-sinks`: Enables all sink connector features
- `all-sources`: Enables all source connector features (currently empty)
- `all-connectors`: Enables both `all-sinks` and `all-sources`

## Usage Examples

### Building with specific connectors only

```bash
# Build with only DeltaLake and Iceberg sinks
cargo build --no-default-features --features "rw-static-link,sink-deltalake,sink-iceberg"

# Build with only ClickHouse and MongoDB sinks
cargo build --no-default-features --features "rw-static-link,sink-clickhouse,sink-mongodb"
```

### Building without any optional connectors

```bash
# Minimal build (fastest compilation)
cargo build --no-default-features --features "rw-static-link"
```

### Default behavior

By default, all connector features are enabled via the `all-connectors` feature in the main binary:

```bash
# This includes all connectors (default behavior)
cargo build
```

## Development Configuration

For faster development builds, you can use `risedev configure` to disable default features:

1. Run `./risedev configure`
2. Select "No Default Features" to add `--no-default-features` to build commands
3. This will build only core functionality by default, significantly reducing compile time

## Architecture

Each optional connector follows this pattern:

1. **Dependencies**: Made optional in `Cargo.toml` with `optional = true`
2. **Implementation**: Moved to `imp.rs` within a connector subdirectory
3. **Conditional compilation**: `mod.rs` uses `cfg_if!` to conditionally include implementation
4. **Dummy implementation**: When feature is disabled, a dummy sink that returns "feature not enabled" errors is provided
5. **Error handling**: Feature-gated error conversions for connector-specific error types

This approach ensures:
- ✅ Fast compilation when heavy dependencies are not needed
- ✅ No runtime overhead - decisions made at compile time
- ✅ Clear error messages when attempting to use disabled connectors
- ✅ IDE compatibility - no manual configuration required
- ✅ Backward compatibility - default builds include everything

## Contributing

When adding new connectors with heavy dependencies, consider following this pattern:

1. Add the dependency as optional in `Cargo.toml`
2. Create a feature flag for the connector
3. Move implementation to `imp.rs` in a subdirectory
4. Create conditional `mod.rs` with dummy implementation
5. Add feature-gated error conversions if needed
6. Update the `all-sinks` or `all-sources` feature list