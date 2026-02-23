# AGENTS.md - Dashboard Library

## 1. Scope

Policies for the dashboard/lib directory, containing utility functions, API clients, and shared library code for the RisingWave Dashboard.

## 2. Purpose

The lib directory provides shared utilities, API clients, data transformation functions, and algorithmic helpers used across the dashboard application. It abstracts data fetching, processing, and common operations to keep components clean and focused on presentation.

## 3. Structure

```
lib/
├── algo.ts                   # Algorithmic utilities and helpers
├── api/                      # API client modules
│   ├── api.ts               # Core API client and endpoint management
│   ├── cluster.ts           # Cluster-related API calls
│   ├── fetch.ts             # Fetch utilities and wrappers
│   ├── streaming.ts         # Streaming job API endpoints
│   └── streamingStats.ts    # Streaming statistics API
├── extractInfo.ts           # Data extraction utilities
├── layout.ts                # Layout calculation algorithms
├── util.js                  # General JavaScript utilities
└── utils/                   # Additional utility functions
    └── timeUtils.ts         # Time formatting and manipulation
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `api/api.ts` | Core API client with endpoint configuration |
| `api/streaming.ts` | Streaming job data fetching |
| `api/cluster.ts` | Cluster status and node information |
| `layout.ts` | Graph layout algorithms (D3 integration) |
| `extractInfo.ts` | Column and schema data extraction |
| `algo.ts` | Generic algorithmic utilities |

## 5. Edit Rules (Must)

- Write all utilities in TypeScript with explicit return types
- Document function parameters and return values with JSDoc
- Use async/await for asynchronous operations
- Implement proper error handling with typed errors
- Cache API responses where appropriate
- Use pure functions without side effects when possible
- Export functions as named exports (avoid default exports)
- Add unit tests for complex algorithms
- Handle null/undefined values gracefully
- Follow consistent naming conventions (camelCase)

## 6. Forbidden Changes (Must Not)

- Modify API endpoints without updating type definitions
- Add console.log statements (use proper logging)
- Create circular dependencies between modules
- Mutate input parameters in functions
- Use `any` type in public API functions
- Hardcode API URLs (use configuration)
- Mix async callbacks with promises inconsistently

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Type check | `cd dashboard && tsc --noEmit` |
| Unit tests | Add jest tests alongside source files |
| API integration | Test against running RisingWave cluster |
| Lint | `cd dashboard && npm run lint` |

## 8. Dependencies & Contracts

- TypeScript 5.x for type safety
- Native fetch API for HTTP requests
- RisingWave meta node API (port 5691)
- Proto-generated TypeScript types
- D3.js types for graph data structures
- No external HTTP client libraries (use native fetch)

## 9. Overrides

Inherits from `./dashboard/AGENTS.md`:
- Override: Library code patterns and API conventions
- Override: TypeScript strictness requirements

## 10. Update Triggers

Regenerate this file when:
- API client architecture changes
- New data transformation patterns are introduced
- Backend API contracts change
- Graph layout algorithms are modified

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./dashboard/AGENTS.md
