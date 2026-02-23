# AGENTS.md - Redis Test Configuration

## 1. Scope

Policies for the ci/redis-conf directory, covering Redis cluster configurations for CDC and stream processing integration testing.

## 2. Purpose

The redis-conf module provides Redis Cluster configuration files for testing RisingWave's Redis CDC source connector. It sets up a multi-node Redis cluster environment that simulates production Redis deployments with clustering enabled.

## 3. Structure

```
ci/redis-conf/
├── redis-7000.conf      # Redis node 1 configuration (port 7000)
├── redis-7001.conf      # Redis node 2 configuration (port 7001)
└── redis-7002.conf      # Redis node 3 configuration (port 7002)
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `redis-7000.conf` | Primary cluster node configuration |
| `redis-7001.conf` | Secondary cluster node configuration |
| `redis-7002.conf` | Tertiary cluster node configuration |

## 5. Edit Rules (Must)

- Use consistent cluster naming convention (nodes-{port}.conf)
- Set appropriate node timeout values (15000ms default)
- Document port allocation strategy
- Enable cluster mode explicitly in each config
- Ensure unique cluster-config-file per node
- Test with Redis 6.0+ cluster images
- Document cluster topology (3-node minimum)
- Verify key slot distribution across nodes

## 6. Forbidden Changes (Must Not)

- Disable cluster-mode once enabled
- Use identical cluster-config-file names
- Remove cluster-node-timeout without replacement
- Change ports without updating docker-compose
- Configure fewer than 3 nodes for HA testing
- Use default config without cluster settings
- Break cluster bus communication (ports + 10000)

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Node startup | `redis-server redis-{port}.conf` |
| Cluster meet | `redis-cli --cluster create` |
| Cluster info | `redis-cli cluster info` |
| Integration | docker-compose with RisingWave Redis source |

## 8. Dependencies & Contracts

- Redis 6.0+ with cluster support
- 3-node minimum cluster configuration
- Port range 7000-7002 for nodes
- Cluster bus ports 17000-17002
- docker-compose service orchestration
- RisingWave Redis CDC connector

## 9. Overrides

Inherits from `./ci/AGENTS.md`:
- Override: Edit Rules - Redis cluster-specific requirements

## 10. Update Triggers

Regenerate this file when:
- Redis CDC connector features expand
- Cluster configuration requirements change
- Node count or topology changes
- Redis version compatibility updates

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./ci/AGENTS.md
