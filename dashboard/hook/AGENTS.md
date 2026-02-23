# AGENTS.md - Dashboard Hooks

## 1. Scope

Policies for the dashboard/hook directory, containing custom React hooks.

## 2. Purpose

The hooks module provides reusable React hooks for state management, side effects, and UI interactions across the dashboard components.

## 3. Structure

```
hook/
└── useErrorToast.ts    # Hook for displaying error toasts
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `useErrorToast.ts` | Display error notifications with Chakra UI |

## 5. Edit Rules (Must)

- Follow React hooks naming convention (use*)
- Use TypeScript for type safety
- Handle cleanup in useEffect properly
- Memoize callbacks with useCallback
- Memoize values with useMemo when needed
- Document hook parameters and return values
- Keep hooks focused on single responsibilities

## 6. Forbidden Changes (Must Not)

- Call hooks conditionally
- Use hooks outside React components
- Skip dependency arrays in useEffect
- Create memory leaks with uncleaned subscriptions

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Type check | `tsc --noEmit` |
| Component test | Use React Testing Library |

## 8. Dependencies & Contracts

- React 18+ hooks API
- Chakra UI hooks
- TypeScript 5.x

## 9. Overrides

Inherits from `./dashboard/AGENTS.md`:
- Override: Custom hooks patterns

## 10. Update Triggers

Regenerate this file when:
- New hooks are added
- React version changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./dashboard/AGENTS.md
