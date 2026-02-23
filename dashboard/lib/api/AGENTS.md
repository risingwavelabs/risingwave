# AGENTS.md - Dashboard API Client

## 1. Scope

Policies for the dashboard/lib/api directory, containing API client modules for the RisingWave Dashboard.

## 2. Purpose

The API client module provides type-safe HTTP clients for communicating with the RisingWave meta node API. It handles endpoint configuration, request/response serialization, and error handling for dashboard data fetching.

## 3. Structure

```
lib/api/
├── api.ts              # Core API client class
├── cluster.ts          # Cluster management API
├── fetch.ts            # Fetch utilities
├── streaming.ts        # Streaming job API
└── streamingStats.ts   # Streaming statistics API
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `api.ts` | Core API client with endpoint management |
| `cluster.ts` | Cluster node and actor APIs |
| `streaming.ts` | Fragment and job APIs |
| `streamingStats.ts` | Performance metrics APIs |
| `fetch.ts` | HTTP fetch wrappers |

## 5. Edit Rules (Must)

- Use native fetch API (no axios)
- Implement proper error handling with typed errors
- Support configurable API endpoints
- Cache responses where appropriate
- Use TypeScript strict typing
- Handle network failures gracefully
- Parse JSON responses with validation
- Support mock endpoints for development

## 6. Forbidden Changes (Must Not)

- Add external HTTP client libraries
- Hardcode API URLs in components
- Skip error handling for network requests
- Use `any` type for API responses
- Mix async patterns (callbacks vs promises)

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Type check | `tsc --noEmit` |
| API test | Test against running meta node |
| Mock test | Use mock server |

## 8. Dependencies & Contracts

- Native fetch API
- RisingWave meta node API (port 5691)
- TypeScript 5.x

## 9. Overrides

Inherits from `./dashboard/lib/AGENTS.md`:
- Override: API client patterns

## 10. Update Triggers

Regenerate this file when:
- API endpoint structure changes
- New API clients are added
- Backend API contracts change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./dashboard/lib/AGENTS.md
