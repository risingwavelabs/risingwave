# AGENTS.md - Web Dashboard

## 1. Scope

Policies for the dashboard directory, covering the RisingWave web-based monitoring dashboard built with Next.js.

## 2. Purpose

The dashboard module provides a web-based user interface for monitoring and managing RisingWave clusters. It displays real-time metrics, stream processing graphs, and system health information. Built with Next.js, it supports both static export (embedded in meta node) and standalone deployment modes.

## 3. Structure

```
dashboard/
├── components/           # React components
│   └── (reusable UI components)
├── pages/               # Next.js pages
│   └── (route definitions and page components)
├── lib/                 # Utility functions and classes
│   └── (API clients, data transformers)
├── hook/                # Custom React hooks
│   └── (state management, data fetching)
├── styles/              # CSS and styling
│   └── (global styles, theme definitions)
├── public/              # Static assets
│   └── (images, fonts, icons)
├── mock/                # Mock data for development
│   └── (test data and fixtures)
├── scripts/             # Build and utility scripts
├── proto/               # Generated protobuf TypeScript
├── next.config.js       # Next.js configuration
├── next-env.d.ts        # Next.js type declarations
├── tsconfig.json        # TypeScript configuration
├── package.json         # Node.js dependencies
├── .eslintrc.json       # ESLint configuration
├── .prettierrc          # Prettier formatting config
├── .node-version        # Node version specification
├── mock-server.js       # Development mock API server
└── README.md            # Documentation
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `next.config.js` | Next.js build and export configuration |
| `package.json` | Dependencies and npm scripts |
| `tsconfig.json` | TypeScript compiler settings |
| `pages/` | Application routes and page components |
| `components/` | Reusable React components |
| `lib/` | API clients and utility functions |

## 5. Edit Rules (Must)

- Follow TypeScript strict mode requirements
- Use functional components with hooks
- Run `npm run lint` before committing
- Run `npm run format` for code formatting
- Add components to appropriate directories
- Document component props with TypeScript interfaces
- Test dashboard with real RisingWave cluster
- Generate protobuf types after proto changes (`npm run gen-proto`)
- Update settings page when adding configuration options

## 6. Forbidden Changes (Must Not)

- Disable TypeScript strict mode
- Use `any` type without explicit justification
- Commit `node_modules/` or `.next/` directories
- Add client-side secrets or API keys
- Break static export compatibility
- Remove existing page routes without redirects
- Use inline styles (use CSS modules or styled-jsx)

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Type check | `npm run type-check` or `tsc --noEmit` |
| Lint | `npm run lint` |
| Dev server | `npm run dev` |
| Build | `npm run build` |
| Static export | `npm run export` |

## 8. Dependencies & Contracts

- Node.js (version specified in `.node-version`)
- Next.js 12+
- React 18+
- TypeScript 4.5+
- Material-UI or equivalent component library
- RisingWave meta node API (port 5691)
- Protocol Buffers for API types

## 9. Overrides

Inherits from `./AGENTS.md`:
- Override: Edit Rules - Next.js/React specific requirements
- Override: Dependencies - Node.js ecosystem

## 10. Update Triggers

Regenerate this file when:
- Next.js version requirements change
- Dashboard architecture changes
- New deployment modes are added
- Protobuf generation workflow changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./AGENTS.md
