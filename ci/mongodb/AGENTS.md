# AGENTS.md - MongoDB Test Configuration

## 1. Scope

Policies for the ci/mongodb directory, covering MongoDB configurations for CDC (Change Data Capture) source connector testing.

## 2. Purpose

The mongodb module provides MongoDB replica set configuration for testing RisingWave's MongoDB CDC source. It initializes the replica set required for change streams to function properly, enabling real-time data ingestion from MongoDB collections.

## 3. Structure

```
ci/mongodb/
└── config-replica.js      # Replica set initialization script
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `config-replica.js` | Initializes MongoDB replica set (rs0) with single member |

## 5. Edit Rules (Must)

- Document replica set name and member configuration
- Ensure replica set initialization is idempotent
- Use appropriate priority settings for single-node setups
- Include rs.status() for verification output
- Test with official MongoDB Docker images
- Document MongoDB version compatibility
- Handle initialization timing issues gracefully
- Verify change stream availability after initialization

## 6. Forbidden Changes (Must Not)

- Remove replica set initialization (required for CDC)
- Change replica set name without updating tests
- Add multi-node configuration without CI infrastructure
- Remove rs.status() verification
- Break idempotency of initialization script
- Use deprecated MongoDB initialization methods
- Configure without proper error handling

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Replica init | `mongo mongodb:27017 config-replica.js` |
| RS status | `rs.status()` in mongo shell |
| Change stream | `db.collection.watch()` in mongo shell |
| Integration | docker-compose with RisingWave MongoDB source |

## 8. Dependencies & Contracts

- MongoDB 4.4+ with replica set support
- Single-node replica set configuration (rs0)
- MongoDB service hostname accessibility
- docker-compose service orchestration
- RisingWave MongoDB CDC connector

## 9. Overrides

Inherits from `./ci/AGENTS.md`:
- Override: Edit Rules - MongoDB replica set requirements

## 10. Update Triggers

Regenerate this file when:
- MongoDB CDC connector requirements change
- Replica set configuration needs evolve
- Multi-node testing scenarios are added
- MongoDB version compatibility changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./ci/AGENTS.md
