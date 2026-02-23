# AGENTS.md - Dashboard Utilities

## 1. Scope

Policies for the dashboard/lib/utils directory, containing utility functions for the RisingWave Dashboard.

## 2. Purpose

The utils module provides shared utility functions for common operations like time parsing, formatting, and data transformation used across the dashboard.

## 3. Structure

```
lib/utils/
└── timeUtils.ts    # Time and date utilities
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `timeUtils.ts` | Duration parsing, timestamp conversion |

## 5. Edit Rules (Must)

- Write pure functions without side effects
- Use TypeScript strict typing
- Handle timezone properly
- Validate input parameters
- Return meaningful error messages
- Document function parameters
- Add unit tests for complex logic

## 6. Forbidden Changes (Must Not)

- Mutate input parameters
- Use `any` type without justification
- Skip input validation
- Hardcode timezone assumptions

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Type check | `tsc --noEmit` |
| Unit tests | Add jest tests |

## 8. Dependencies & Contracts

- TypeScript 5.x
- JavaScript Date API
- Intl API for localization

## 9. Overrides

Inherits from `./dashboard/lib/AGENTS.md`:
- Override: Utility function patterns

## 10. Update Triggers

Regenerate this file when:
- New utility categories are added
- Time handling patterns change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./dashboard/lib/AGENTS.md
