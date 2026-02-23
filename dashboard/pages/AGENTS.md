# AGENTS.md - Dashboard Pages

## 1. Scope

Policies for the dashboard/pages directory, containing Next.js pages that define routes and page-level components for the RisingWave Dashboard.

## 2. Purpose

The pages directory implements the Next.js file-based routing system, where each file represents a route in the dashboard application. Pages combine components and library functions to create complete views for monitoring different aspects of a RisingWave cluster.

## 3. Structure

```
pages/
├── _app.tsx                  # Next.js app component (global setup)
├── index.tsx                 # Dashboard home/welcome page
├── cluster.tsx               # Cluster overview and node status
├── node.tsx                  # Individual node details
├── materialized_views.tsx    # Materialized views management
├── tables.tsx                # Database tables view
├── sources.tsx               # Data sources listing
├── sinks.tsx                 # Data sinks configuration
├── indexes.tsx               # Database indexes
├── views.tsx                 # SQL views
├── internal_tables.tsx       # Internal system tables
├── functions.tsx             # User-defined functions
├── subscriptions.tsx         # Subscription management
├── batch_tasks.tsx           # Batch query tasks
├── fragment_graph.tsx        # Fragment graph visualization
├── relation_graph.tsx        # Database relation graph
├── explain_distsql.tsx       # Distributed SQL explain
├── await_tree.tsx            # Async operation tree view
├── heap_profiling.tsx        # Memory heap profiling
└── settings.tsx              # Dashboard settings
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `_app.tsx` | Global app configuration, providers, layout wrapper |
| `index.tsx` | Dashboard landing/welcome page |
| `cluster.tsx` | Cluster topology and node health |
| `materialized_views.tsx` | Stream processing job views |
| `fragment_graph.tsx` | Query execution plan visualization |
| `settings.tsx` | User preferences and configuration |

## 5. Edit Rules (Must)

- Export pages as default export from each file
- Use Next.js data fetching patterns (getStaticProps/getServerSideProps)
- Implement proper loading and error states
- Use consistent page layout structure
- Add Head component for page titles and meta tags
- Handle authentication/authorization at page level
- Implement proper error boundaries
- Support query parameters for filtering/state
- Use TypeScript for page props interfaces
- Follow Next.js 14+ patterns for routing

## 6. Forbidden Changes (Must Not)

- Create pages without proper route naming conventions
- Add client-side redirects without server-side fallback
- Use window/document during SSR (use useEffect)
- Remove _app.tsx without providing alternative
- Hardcode API endpoints (use lib/api)
- Skip error handling for data fetching
- Add global styles in page files

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Type check | `cd dashboard && tsc --noEmit` |
| Build test | `cd dashboard && npm run build` |
| Dev server | `cd dashboard && npm run dev` |
| E2E tests | Test against running RisingWave cluster |

## 8. Dependencies & Contracts

- Next.js 14+ for routing and SSR
- React 18+ for page components
- Chakra UI for page layouts
- Custom hooks from hook/ directory
- API clients from lib/api/
- Components from components/
- Proto types for data structures

## 9. Overrides

Inherits from `./dashboard/AGENTS.md`:
- Override: Next.js-specific routing patterns
- Override: Page-level data fetching conventions

## 10. Update Triggers

Regenerate this file when:
- Next.js version or routing changes
- New page patterns are introduced
- Dashboard navigation structure changes
- Page-level data fetching patterns evolve

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./dashboard/AGENTS.md
