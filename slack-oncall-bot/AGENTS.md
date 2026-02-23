# AGENTS.md - Slack Oncall Bot

## 1. Scope

Policies for the slack-oncall-bot directory, covering the TypeScript-based Slack bot for on-call operations and incident management.

## 2. Purpose

The Slack Oncall Bot provides automated incident response and on-call management through Slack integration. It assists operations teams with alerting, incident tracking, and coordination during system incidents.

## 3. Structure

```
slack-oncall-bot/
├── src/                      # Source code
│   ├── bot/                  # Bot core logic and handlers
│   ├── config/               # Configuration management
│   ├── services/             # External service integrations
│   └── types/                # TypeScript type definitions
├── deploy/                   # Deployment configurations
│   └── (Kubernetes manifests or deployment scripts)
├── scripts/                  # Utility scripts
│   └── (build, deploy, and maintenance scripts)
└── AGENTS.md                # This file
```

## 4. Key Files

| File/Directory | Purpose |
|----------------|---------|
| `src/bot/` | Bot command handlers and message processing |
| `src/config/` | Environment and configuration management |
| `src/services/` | Slack API and external service clients |
| `src/types/` | TypeScript interfaces and type definitions |
| `deploy/` | Deployment manifests and configurations |
| `scripts/` | Automation and utility scripts |

## 5. Edit Rules (Must)

- Write all code in TypeScript with strict type checking
- Document all bot commands and their usage
- Add error handling for all Slack API interactions
- Log all bot activities for audit purposes
- Follow existing code style (enforced by linter)
- Test bot commands in development workspace first
- Use environment variables for configuration
- Include unit tests for command handlers

## 6. Forbidden Changes (Must Not)

- Hardcode Slack tokens or credentials in source code
- Remove existing bot commands without deprecation notice
- Add commands that could expose sensitive information
- Make breaking changes to message format without coordination
- Bypass rate limiting for Slack API calls
- Commit production environment files

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `npm test` or `yarn test` |
| Type check | `tsc --noEmit` |
| Lint | `npm run lint` or `yarn lint` |
| Integration | Test in development Slack workspace |

## 8. Dependencies & Contracts

- Node.js runtime (LTS version)
- Slack Bolt SDK
- TypeScript compiler
- External APIs (PagerDuty, GitHub, etc.)
- RisingWave APIs for metrics and status

## 9. Overrides

Inherits from `./AGENTS.md`:
- Override: Edit Rules - TypeScript-specific requirements
- Override: Test Entry - Slack-specific testing workflow

## 10. Update Triggers

Regenerate this file when:
- Bot architecture changes significantly
- New command categories are added
- Deployment strategy changes
- External integrations are modified

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./AGENTS.md
