# AGENTS.md - Meta Model Migration Crate

## 1. Scope

Policies for the `risingwave_meta_model_migration` crate providing database schema migrations for RisingWave metadata.

## 2. Purpose

The migration crate provides:
- **Schema Migrations**: sea-orm-migration based database evolution
- **Version Tracking**: Applied migration history
- **Rollback Support**: Downgrade migrations for recovery
- **Multi-Backend**: Support for SQLite, PostgreSQL, MySQL

This manages the SQL schema for RisingWave's metadata store.

## 3. Structure

```
src/meta/model/migration/
├── Cargo.toml                    # Crate manifest
├── README.md                     # Migration development guide
└── src/
    ├── lib.rs                    # Migration module exports
    ├── main.rs                   # CLI entry point
    ├── utils.rs                  # Migration helpers
    └── m*.rs                     # Individual migration files (60+)
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Migration collection, `Migrator` struct |
| `src/main.rs` | CLI for running migrations |
| `src/mYYYYMM*.rs` | Individual migration files (sea-orm format) |
| `README.md` | Migration authoring guidelines |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Create new migration files using sea-orm-cli
- Write both `up()` and `down()` methods
- Test migrations on all supported databases
- Add migration tests for data transformations
- Name migrations with timestamp prefix (mYYYYMMDD_HHMMSS_*)
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Modify already-applied migration files
- Delete migration files after deployment
- Write migrations without down() methods (except irreversible)
- Add migrations that lose data without explicit warning
- Skip migration versions
- Use raw SQL without type safety where possible

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Migration tests | `cargo test -p risingwave_meta_model_migration` |
| Apply migrations | `cargo run -p risingwave_meta_model_migration` |
| Fresh database | `cargo run -p risingwave_meta_model_migration -- fresh` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **sea-orm-migration**: Migration framework
- **sea-orm**: ORM for migration queries
- **tokio**: Async runtime
- **uuid**: Migration tracking identifiers

Contracts:
- Migrations are applied in timestamp order
- Each migration is applied exactly once
- Down migrations reverse up migrations precisely
- Migrations are transactional where database supports

## 9. Overrides

None. Follows parent `src/meta/model/AGENTS.md` rules.

## 10. Update Triggers

Regenerate this file when:
- Migration framework version upgraded
- New database backend support added
- Migration authoring patterns changed

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/meta/model/AGENTS.md
