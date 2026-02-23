# AGENTS.md - Dashboard Components

## 1. Scope

Policies for the dashboard/components directory, containing React UI components for the RisingWave monitoring dashboard.

## 2. Purpose

The components directory houses reusable React components that render the visual interface of the RisingWave Dashboard. These components handle data visualization, graph rendering, navigation layout, and interactive UI elements for monitoring streaming jobs, cluster status, and performance metrics.

## 3. Structure

```
components/
├── CatalogModal.tsx          # Catalog information modal dialog
├── FragmentDependencyGraph.tsx  # Fragment dependency visualization
├── FragmentGraph.tsx         # Stream fragment graph renderer
├── GraphvizComponent.tsx     # Graphviz graph integration
├── Layout.tsx                # Application layout and navigation
├── metrics.tsx               # Metrics display components
├── NoData.tsx                # Empty state placeholder
├── RelationGraph.tsx         # Database relation graph view
├── Relations.tsx             # Relations list and management
├── SpinnerOverlay.tsx        # Loading overlay component
├── TimeControls.tsx          # Time range selector controls
├── Title.tsx                 # Page title component
└── utils/                    # Component utilities
    ├── backPressure.tsx      # Back pressure visualization helpers
    └── icons.tsx             # Custom icon components
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `Layout.tsx` | Main application layout with navigation sidebar |
| `FragmentGraph.tsx` | D3.js-based stream fragment visualization |
| `RelationGraph.tsx` | Database relation dependency graph |
| `TimeControls.tsx` | Time range selection for metrics |
| `utils/backPressure.tsx` | Back pressure color/width calculations |

## 5. Edit Rules (Must)

- Use TypeScript for all new components with strict typing
- Follow React functional component patterns with hooks
- Use Chakra UI components for consistent styling
- Implement proper PropTypes or TypeScript interfaces
- Add proper error boundaries for complex visualizations
- Use D3.js for custom graph visualizations
- Support both light and dark theme contexts
- Optimize re-renders with React.memo when appropriate
- Add loading states for async data fetching
- Follow component composition patterns

## 6. Forbidden Changes (Must Not)

- Use class components for new code
- Add inline styles (use Chakra UI or CSS modules)
- Import components using relative paths beyond parent
- Remove accessibility attributes from interactive elements
- Use `any` type without explicit documentation
- Break responsive design for mobile viewports
- Hardcode colors (use theme tokens)

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Type check | `cd dashboard && tsc --noEmit` |
| Lint | `cd dashboard && npm run lint` |
| Dev server | `cd dashboard && npm run dev` |
| Build | `cd dashboard && npm run build` |

## 8. Dependencies & Contracts

- React 18+ with hooks API
- Chakra UI 2.x for component library
- D3.js 7.x for data visualization
- Dagre for graph layout algorithms
- Next.js for routing and SSR
- TypeScript 5.x for type safety
- RisingWave proto definitions for data types

## 9. Overrides

Inherits from `./dashboard/AGENTS.md`:
- Override: Component-specific patterns and D3.js usage
- Override: Visualization-specific guidelines

## 10. Update Triggers

Regenerate this file when:
- Component architecture patterns change
- New visualization libraries are introduced
- Styling system changes
- Graph rendering approaches evolve

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./dashboard/AGENTS.md
