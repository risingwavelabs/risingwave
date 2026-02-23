# AGENTS.md - Python Client

## 1. Scope

Policies for the python-client module, providing Python client for connector testing.

## 2. Purpose

The python-client module provides a Python-based client for testing connector functionality. It includes integration tests that validate sink and source connectors using gRPC calls.

## 3. Structure

```
python-client/
├── proto/                      # Protobuf definitions
├── data/                       # Test data files
├── integration_tests.py        # Main integration tests
├── pyspark-util.py             # PySpark utilities
├── build-venv.sh               # Virtual environment setup
├── gen-stub.sh                 # gRPC stub generation
├── format-python.sh            # Python formatting
└── .gitignore
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `integration_tests.py` | Main integration test suite |
| `pyspark-util.py` | PySpark helper utilities |
| `gen-stub.sh` | Generate Python gRPC stubs |

## 5. Edit Rules (Must)

- Use Python 3.8+ features
- Follow PEP 8 style guidelines
- Use type hints where appropriate
- Handle gRPC errors properly
- Clean up test resources
- Use virtual environments
- Document test cases

## 6. Forbidden Changes (Must Not)

- Hardcode connection credentials
- Leave test data in databases
- Skip error handling
- Use Python 2 syntax

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Run tests | `python integration_tests.py` |
| Generate stubs | `bash gen-stub.sh` |
| Format code | `bash format-python.sh` |

## 8. Dependencies & Contracts

- Python 3.8+
- gRPC Python bindings
- psycopg2 for PostgreSQL
- protobuf

## 9. Overrides

Inherits from `./java/connector-node/AGENTS.md`:
- Override: Python-specific testing patterns

## 10. Update Triggers

Regenerate this file when:
- Test framework changes
- New integration tests added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./java/connector-node/AGENTS.md
