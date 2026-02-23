# AGENTS.md - Analysis Engine

## 1. Scope

Policies for the auto-error-detection/analysis-engine directory, covering the core error analysis engine and detection algorithms.

## 2. Purpose

The analysis-engine module provides the core detection and analysis capabilities for the auto-error-detection system. It implements pattern matching algorithms, log analysis logic, and metric correlation to identify anomalous behavior and potential system issues in RisingWave deployments.

## 3. Structure

```
analysis-engine/
├── src/                       # Core engine source code
│   ├── patterns/             # Detection pattern definitions
│   ├── analyzers/            # Log and metric analyzers
│   ├── correlators/          # Event correlation logic
│   └── models/               # Data models and structures
├── tests/                    # Unit and integration tests
├── config/                   # Engine configuration files
│   ├── patterns.yaml         # Pattern definitions
│   └── thresholds.json       # Detection thresholds
├── examples/                 # Example detections and test cases
└── AGENTS.md                 # This file
```

## 4. Key Files

| File/Directory | Purpose |
|----------------|---------|
| `src/patterns/` | Error pattern definitions and matching logic |
| `src/analyzers/` | Log parsing and metric analysis components |
| `src/correlators/` | Multi-source event correlation engine |
| `config/patterns.yaml` | Configurable detection patterns |
| `config/thresholds.json` | Alert thresholds and sensitivity settings |
| `tests/` | Comprehensive test suite for detection accuracy |

## 5. Edit Rules (Must)

- Document all detection patterns with clear descriptions and examples
- Add unit tests for every new detection algorithm
- Include severity classification (critical, warning, info) for all patterns
- Document false positive rates and mitigation strategies
- Use consistent naming conventions for pattern identifiers
- Implement configurable thresholds for all detection rules
- Add performance benchmarks for detection logic
- Update pattern documentation when modifying detection behavior

## 6. Forbidden Changes (Must Not)

- Add detection patterns without corresponding unit tests
- Modify core matching algorithms without regression testing
- Hardcode thresholds that cannot be configured
- Remove detection patterns without deprecation period
- Introduce detection logic with known high false positive rates
- Add external dependencies without security and license review
- Commit test data containing sensitive information
- Disable detection rules without documented justification

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p analysis-engine` |
| Pattern validation | `cargo test patterns::` |
| Integration tests | `cargo test --features integration` |
| Benchmarks | `cargo bench` |
| Example validation | Run examples against sample logs |

## 8. Dependencies & Contracts

- Rust standard library and regex engine
- Log parsing libraries (e.g., nom, serde_json)
- Metrics collection interfaces (Prometheus client)
- Configuration management (serde, toml/yaml)
- Pattern matching engine (regex, custom matchers)
- RisingWave internal telemetry APIs
- Time-series analysis libraries

## 9. Overrides

Inherits from `/home/k11/risingwave/auto-error-detection/AGENTS.md`:
- Override: Test Entry - Analysis engine has specialized pattern tests
- Override: Dependencies - Pattern matching engine specific

## 10. Update Triggers

Regenerate this file when:
- New analysis algorithm categories are introduced
- Detection pattern format changes
- Core matching engine architecture changes
- New correlation strategies are implemented
- Performance requirements for detection change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/auto-error-detection/AGENTS.md
