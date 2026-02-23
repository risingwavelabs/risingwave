# AGENTS.md - Auto Error Detection

## 1. Scope

Policies for the auto-error-detection directory, covering error analysis engine and automated detection tooling.

## 2. Purpose

The auto-error-detection module provides automated error detection and analysis capabilities for RisingWave. It analyzes logs, metrics, and system behavior to identify potential issues before they impact production workloads.

## 3. Structure

```
auto-error-detection/
├── analysis-engine/       # Core error analysis engine
│   └── (analysis logic and detection algorithms)
└── AGENTS.md             # This file
```

## 4. Key Files

| File/Directory | Purpose |
|----------------|---------|
| `analysis-engine/` | Core detection algorithms and analysis logic |
| Pattern definitions | Error patterns and signatures to detect |
| Detection rules | Configurable rules for issue identification |

## 5. Edit Rules (Must)

- Document all detection patterns with clear descriptions
- Add unit tests for new detection rules
- Follow existing pattern definition format
- Include severity levels for each detection rule
- Document false positive mitigation strategies
- Update detection rule documentation when adding patterns
- Ensure detection logic is efficient and does not impact system performance

## 6. Forbidden Changes (Must Not)

- Add detection rules without corresponding tests
- Modify existing detection patterns without regression testing
- Hardcode thresholds without configuration options
- Remove detection rules without deprecation notice
- Introduce detection logic with high false positive rates
- Add external dependencies without security review

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p auto-error-detection` |
| Pattern tests | Run pattern validation against sample logs |
| Integration tests | Test with simulated error scenarios |

## 8. Dependencies & Contracts

- Pattern matching engine
- Log parsing libraries
- Metrics collection interfaces
- Alerting system integration
- RisingWave internal APIs for state inspection

## 9. Overrides

Inherits from `./AGENTS.md`:
- Override: Test Entry - Pattern tests are specific to this module

## 10. Update Triggers

Regenerate this file when:
- New detection engine architecture is introduced
- Detection pattern format changes
- Integration points with other systems change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./AGENTS.md
