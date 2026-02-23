# AGENTS.md - Component Utilities

## 1. Scope

Policies for the dashboard/components/utils directory, containing utility functions for React components.

## 2. Purpose

The component utils module provides visualization helpers, color calculations, and UI-specific utilities for dashboard components, particularly for streaming graph visualizations.

## 3. Structure

```
components/utils/
├── backPressure.tsx    # Back pressure visualization helpers
└── icons.tsx           # Custom icon components
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `backPressure.tsx` | Color and width calculations for back pressure |
| `icons.tsx` | Custom SVG icon components |

## 5. Edit Rules (Must)

- Use Chakra UI theme tokens
- Export pure functions for calculations
- Support theme-aware colors
- Handle edge cases (0, undefined values)
- Document visualization logic
- Keep calculations performant
- Use TypeScript for type safety

## 6. Forbidden Changes (Must Not)

- Hardcode color values (use theme)
- Use magic numbers without constants
- Skip null/undefined checks
- Mix calculation logic with JSX

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Type check | `tsc --noEmit` |
| Visual test | Test in browser |

## 8. Dependencies & Contracts

- Chakra UI theme
- @ctrl/tinycolor for color manipulation
- TypeScript 5.x

## 9. Overrides

Inherits from `./dashboard/components/AGENTS.md`:
- Override: Component utility patterns

## 10. Update Triggers

Regenerate this file when:
- New visualization utilities are added
- Color calculation patterns change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./dashboard/components/AGENTS.md
