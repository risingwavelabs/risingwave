# AGENTS.md - Meta Model Crate

## 1. Scope

Policies for the `src/meta/model/` directory containing sea-orm based data models for the RisingWave meta service persistence layer.

## 2. Purpose

The meta model crate defines database entities and relations for the SQL metadata store using sea-orm. It provides type-safe database models for catalog objects (tables, sources, sinks), cluster state, Hummock metadata, and user management.

## 3. Structure

```
src/meta/model/
├── migration/                    # Database schema migrations
│   ├── src/
│   ├── Cargo.toml
│   ├── clippy.toml
│   └── README.md
└── src/
    ├── lib.rs                    # Module exports and re-exports
    ├── prelude.rs                # Common sea-orm imports
    ├── object.rs                 # Base object entity
    ├── table.rs                  # Table metadata
    ├── source.rs                 # Source connector metadata
    ├── sink.rs                   # Sink connector metadata
    ├── fragment.rs               # Stream fragment metadata
    ├── index.rs                  # Index definitions
    ├── view.rs                   # Materialized view metadata
    ├── database.rs               # Database metadata
    ├── schema.rs                 # Schema metadata
    ├── user.rs                   # User and authentication
    ├── user_privilege.rs         # RBAC privilege grants
    ├── cluster.rs                # Cluster membership
    ├── worker.rs                 # Worker node metadata
    ├── hummock_*.rs              # Hummock storage metadata
    ├── compaction_*.rs           # Compaction metadata
    └── ...                       # Additional entities
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Entity module declarations and exports |
| `src/prelude.rs` | Common sea-orm type imports |
| `src/object.rs` | Base object entity (parent of catalog objects) |
| `src/table.rs` | Table and materialized view definitions |
| `src/fragment.rs` | Stream job fragment topology |
| `src/hummock_version.rs` | Hummock version tracking |
| `migration/src/` | Schema migration definitions |
| `migration/README.md` | Migration development guide |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing
- Pass `./risedev c` (clippy) before submitting changes
- Use `sea-orm` entity macros for model definitions
- Add migrations in `migration/src/` for schema changes
- Derive `DeriveEntityModel` for new entities
- Implement `Related` traits for entity relationships
- Update `prelude.rs` for new entity exports

## 6. Forbidden Changes (Must Not)

- Modify existing entity fields without migration
- Delete migrations after they have been applied
- Change primary key definitions without schema migration
- Remove entity relations without updating dependent code
- Modify auto-generated migration files manually
- Break backward compatibility in entity serialization

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Model tests | `cargo test -p risingwave_meta_model` |
| Migration tests | `cargo test -p risingwave_meta_model_migration` |
| Meta integration | `cargo test -p risingwave_meta controller` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- sea-orm: ORM framework for entity definitions
- sea-query: SQL query builder
- async-trait: Async trait implementations
- Internal: `risingwave_pb` for protobuf conversions
- Database: SQLite, PostgreSQL, or MySQL backend
- Migrations: Applied via `sea-orm-migration`

## 9. Overrides

None. Inherits rules from parent `src/meta/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New entity type added
- Migration framework changed
- sea-orm version upgraded
- Entity relationship patterns modified

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/meta/AGENTS.md
