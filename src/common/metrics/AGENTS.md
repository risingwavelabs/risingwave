# AGENTS.md - Common Metrics Crate

## 1. Scope

Policies for the `risingwave_common_metrics` crate providing Prometheus metrics wrappers and extensions.

## 2. Purpose

The common metrics crate provides:
- **Metric Wrappers**: Type-safe wrappers around Prometheus metrics
- **Guarded Metrics**: Automatic metric cleanup on scope exit
- **Relabeled Metrics**: Dynamic label transformation
- **Error Metrics**: Standardized error counting and classification
- **System Metrics**: OS-level resource monitoring (Linux, macOS)

This enables consistent observability across all RisingWave components.

## 3. Structure

```
src/common/metrics/
├── Cargo.toml                    # Crate manifest
└── src/
    ├── lib.rs                    # Module exports and core types
    ├── metrics.rs                # Metric wrapper implementations
    ├── gauge_ext.rs              # Gauge metric extensions
    ├── guarded_metrics.rs        # RAII metric guards
    ├── relabeled_metric.rs       # Label transformation utilities
    ├── error_metrics.rs          # Error classification metrics
    └── monitor/                  # System resource monitoring
        └── process_linux.rs      # Linux-specific process metrics
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Public exports, `LabelGuardedMetrics` trait |
| `src/metrics.rs` | `MetricsInfo`, `TrAdder` implementations |
| `src/gauge_ext.rs` | Gauge metric helper methods |
| `src/guarded_metrics.rs` | `LabelGuardedIntGauge`, `LabelGuardedHistogram` |
| `src/relabeled_metric.rs` | `RelabeledCounterVec`, `RelabeledHistogramVec` |
| `src/error_metrics.rs` | `ErrorType`, `ErrorMetrics` |
| `src/monitor/` | Platform-specific system metrics |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Use `LabelGuarded` wrappers for metrics with dynamic labels
- Implement `Drop` for automatic metric cleanup
- Add error type classifications to `ErrorMetrics` for new error categories
- Test metric label cardinality (prevent high-cardinality explosions)
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Add high-cardinality labels (unique IDs, timestamps) to metrics
- Remove metric guards without ensuring proper cleanup
- Use `prometheus` crate directly without wrappers in application code
- Break compatibility with existing metric names in dashboards
- Remove platform-specific metric implementations without fallback
- Use `unwrap()` in metric recording paths (must handle errors gracefully)

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p risingwave_common_metrics` |
| Guard tests | `cargo test -p risingwave_common_metrics guarded` |
| Relabel tests | `cargo test -p risingwave_common_metrics relabel` |
| System metrics | `cargo test -p risingwave_common_metrics monitor` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **prometheus**: Core metrics collection (version 0.14)
- **parking_lot**: Fast synchronization for metric updates
- **hytra**: Hybrid thread-local aggregator for counters
- **hyper**: HTTP server for metrics exposition
- **libc/procfs**: System metrics on Linux
- **darwin-libproc/mach2**: System metrics on macOS

Contracts:
- Metric names follow Prometheus naming conventions (snake_case)
- Label values are validated to prevent cardinality explosion
- Guarded metrics decrement on scope exit (even on panic)
- System metrics updated lazily on collection

## 9. Overrides

None. Follows parent `src/common/AGENTS.md` rules.

## 10. Update Triggers

Regenerate this file when:
- New metric wrapper type added
- Prometheus crate version upgraded
- Error classification scheme changes
- Platform-specific metric support added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/common/AGENTS.md
