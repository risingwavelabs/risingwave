# AGENTS.md - Dashboard Mock Data

## 1. Scope

Policies for the dashboard/mock directory, containing mock data and fixtures for development.

## 2. Purpose

The mock module provides test data and mock API responses for dashboard development and testing without requiring a live RisingWave cluster.

## 3. Structure

```
mock/
├── fetch.sh        # Script to fetch mock data
└── .gitignore      # Ignore fetched data files
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `fetch.sh` | Fetches real data for mocking |
| `.gitignore` | Excludes generated mock files |

## 5. Edit Rules (Must)

- Document how to regenerate mock data
- Use realistic data structures
- Keep mock data up to date with API changes
- Sanitize any sensitive information
- Version control mock generation scripts

## 6. Forbidden Changes (Must Not)

- Commit sensitive data
- Use outdated mock structures
- Hardcode credentials in fetch scripts

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Fetch data | `bash fetch.sh` |
| Mock server | `node mock-server.js` |

## 8. Dependencies & Contracts

- bash for fetch scripts
- curl or wget for HTTP requests

## 9. Overrides

Inherits from `./dashboard/AGENTS.md`:
- Override: Mock data patterns

## 10. Update Triggers

Regenerate this file when:
- Mock data structure changes
- New mock endpoints are added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./dashboard/AGENTS.md
