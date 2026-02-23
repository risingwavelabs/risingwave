# AGENTS.md - Connector Examples

## 1. Scope

Policies for the `src/connector/examples/` directory containing example code demonstrating connector usage patterns.

## 2. Purpose

The examples directory provides runnable sample code showing how to implement and use RisingWave connectors. These examples serve as reference implementations for connector developers and integration testers.

## 3. Structure

```
src/connector/examples/
└── (empty - reserved for future examples)
```

## 4. Key Files

| File | Purpose |
|------|---------|
| (To be added) | Source connector examples |
| (To be added) | Sink connector examples |
| (To be added) | Parser implementation examples |
| (To be added) | Encoder/decoder examples |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Pass `./risedev c` (clippy) before submitting changes
- Include comprehensive comments explaining each example
- Ensure examples compile and run successfully
- Use `tokio::main` for async examples
- Include example data files where applicable

## 6. Forbidden Changes (Must Not)

- Commit examples with hardcoded credentials
- Add broken or non-compiling examples
- Include proprietary or licensed data without permission

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Compile examples | `cargo build --examples -p risingwave_connector` |
| Run specific example | `cargo run --example <name> -p risingwave_connector` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- Examples depend on `risingwave_connector` crate
- Async runtime: `tokio`
- External examples may require running services (Kafka, etc.)

## 9. Overrides

None. Inherits rules from parent `src/connector/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New example category added
- Example patterns change significantly
- New connector type introduced

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/connector/AGENTS.md
