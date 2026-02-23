# AGENTS.md - RabbitMQ Test Configuration

## 1. Scope

Policies for the ci/rabbitmq-conf directory, covering RabbitMQ and MQTT broker configurations for message queue integration testing.

## 2. Purpose

The rabbitmq-conf module provides RabbitMQ configuration for testing RisingWave's MQTT source connector. It enables MQTT v5 protocol support and configures the broker for message ingestion testing with anonymous authentication for CI environments.

## 3. Structure

```
ci/rabbitmq-conf/
├── advanced.config      # Erlang-style advanced configuration (feature flags)
├── enabled_plugins      # List of enabled RabbitMQ plugins
└── rabbitmq.conf        # Main RabbitMQ configuration file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `rabbitmq.conf` | Main broker configuration (MQTT listener, default user, TCP settings) |
| `enabled_plugins` | Plugin list: rabbitmq_management and rabbitmq_mqtt |
| `advanced.config` | Advanced Erlang config for feature flags (mqtt_v5) |

## 5. Edit Rules (Must)

- Document all MQTT configuration parameters
- Maintain anonymous access only for CI/testing environments
- Use standard MQTT port 1883 for compatibility
- Enable management plugin on port 15672 for monitoring
- Set reasonable session expiry (86400 seconds default)
- Document feature flags and version requirements
- Test configuration with RabbitMQ 3.13+ images
- Keep logging at appropriate level (info for CI)

## 6. Forbidden Changes (Must Not)

- Enable anonymous access in production configurations
- Remove mqtt_v5 feature flag without justification
- Change default credentials without updating tests
- Disable management plugin (needed for debugging)
- Use non-standard ports without documentation
- Remove MQTT listener configuration
- Break compatibility with RisingWave MQTT source

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Config validation | `rabbitmqctl status` in container |
| MQTT connection | mosquitto_pub/sub or equivalent |
| Plugin check | `rabbitmq-plugins list` |
| Integration | docker-compose up with RisingWave MQTT source |

## 8. Dependencies & Contracts

- RabbitMQ 3.13+ with management image
- MQTT v5 protocol support
- Port 1883 (MQTT) and 15672 (management) availability
- docker-compose service orchestration
- RisingWave MQTT source connector

## 9. Overrides

Inherits from `/home/k11/risingwave/ci/AGENTS.md`:
- Override: Edit Rules - MQTT-specific configuration requirements

## 10. Update Triggers

Regenerate this file when:
- MQTT protocol requirements change
- RabbitMQ version updates require config changes
- New plugins are needed for testing
- RisingWave MQTT connector features expand

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/ci/AGENTS.md
