# AGENTS.md - Meta Dashboard Examples

## 1. Scope

Policies for `/home/k11/risingwave/src/meta/dashboard/examples` - example programs demonstrating the RisingWave meta dashboard web interface.

## 2. Purpose

The dashboard examples provide:
- Standalone dashboard server demonstration
- Development and testing entry point for the dashboard UI
- Example of embedding the dashboard in an Axum application
- Local development server for UI testing

These examples serve as documentation and development aids for the dashboard crate.

## 3. Structure

```
src/meta/dashboard/examples/
└── serve.rs                  # Example: standalone dashboard server
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `serve.rs` | Example standalone Axum server serving the dashboard |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Use `#[tokio::main]` for async runtime initialization
- Initialize tracing for example logging
- Bind to configurable addresses (default: 0.0.0.0:10188)
- Use `risingwave_meta_dashboard::router()` for dashboard routes
- Document example purpose and usage in code comments
- Keep examples simple and focused on demonstrating one feature
- Pass `./risedev c` (clippy) before submitting changes
- Ensure examples compile: `cargo check --examples -p risingwave_meta_dashboard`

## 6. Forbidden Changes (Must Not)

- Add complex business logic to examples
- Hardcode production credentials or endpoints
- Remove error handling from examples
- Use blocking I/O in async contexts
- Skip example compilation checks in CI
- Add dependencies not needed for demonstration
- Use unsafe code in examples

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Check examples | `cargo check --examples -p risingwave_meta_dashboard` |
| Run example | `cargo run --example serve -p risingwave_meta_dashboard` |
| Build examples | `cargo build --examples -p risingwave_meta_dashboard` |

## 8. Dependencies & Contracts

- `risingwave_meta_dashboard`: Dashboard router and static assets
- `tokio`: Async runtime for the example server
- `axum`: Web framework (via risingwave_meta_dashboard)
- `tracing-subscriber`: Logging initialization
- Example configuration: Bind address `0.0.0.0:10188` (configurable)

## 9. Overrides

None. Inherits all rules from parent `/home/k11/risingwave/src/meta/dashboard/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New examples added
- Example patterns or conventions change
- Dashboard API changes affecting examples
- Example framework dependencies updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/meta/dashboard/AGENTS.md
