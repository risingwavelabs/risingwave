# AGENTS.md - CI Documentation

## 1. Scope

Policies for the ci/docs directory, covering documentation for continuous integration processes, workflow guides, and operational runbooks.

## 2. Purpose

The docs directory provides comprehensive documentation for the RisingWave CI system. It includes workflow guides, troubleshooting runbooks, setup instructions, and operational procedures for maintaining and troubleshooting the CI infrastructure.

## 3. Structure

```
docs/
├── README.md                  # Overview and quick start guide
├── workflow-guide.md          # Workflow development guidelines
├── troubleshooting.md         # Common issues and resolutions
├── runner-setup.md            # Self-hosted runner configuration
├── secrets-management.md      # Secret rotation and management
├── testing-strategy.md        # CI testing approach documentation
├── migration-guides/          # Workflow migration documentation
│   └── (version upgrade guides)
├── runbooks/                  # Operational runbooks
│   ├── incident-response.md
│   ├── rollback-procedures.md
│   └── capacity-planning.md
└── AGENTS.md                  # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `README.md` | Entry point and navigation guide |
| `workflow-guide.md` | How to write and modify workflows |
| `troubleshooting.md` | Common CI failures and fixes |
| `runner-setup.md` | Self-hosted runner installation guide |
| `secrets-management.md` | Secret management procedures |
| `runbooks/` | Step-by-step operational procedures |
| `migration-guides/` | Workflow upgrade instructions |

## 5. Edit Rules (Must)

- Keep documentation in sync with workflow changes
- Use clear, step-by-step instructions for procedures
- Include example commands and expected outputs
- Document all prerequisites for each guide
- Add screenshots or diagrams where helpful
- Include version information for external tools
- Update troubleshooting guide with new failure modes
- Cross-reference related documentation

## 6. Forbidden Changes (Must Not)

- Remove documentation without archiving or redirecting
- Document procedures without testing them
- Include sensitive information (tokens, internal IPs)
- Leave TODO sections in published documentation
- Break existing documentation links without redirects
- Document unofficial or unsupported workflows
- Commit credentials or environment-specific data

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Link validation | `markdown-link-check` or similar |
| Spell check | `cspell` or IDE spell checker |
| Format check | `markdownlint` |
| Accuracy review | Manual review by CI maintainers |
| Freshness check | Review last updated timestamps |

## 8. Dependencies & Contracts

- Markdown formatting standards
- Diagram tools (Mermaid, PlantUML)
- Documentation hosting (GitHub wiki, internal docs)
- Version control for documentation
- Review process for doc changes
- Link checking automation

## 9. Overrides

Inherits from `./ci/AGENTS.md`:
- Override: Test Entry - Documentation-specific validation
- Override: Edit Rules - Documentation style requirements

## 10. Update Triggers

Regenerate this file when:
- New documentation categories are added
- Documentation structure changes
- CI process documentation standards change
- New runbook types are introduced
- Documentation tooling changes

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./ci/AGENTS.md
