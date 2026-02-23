# AGENTS.md - Python Slack Oncall Bot

## 1. Scope

Policies for the slack-oncall-bot-python directory, covering the Python-based Slack bot for on-call operations.

## 2. Purpose

The Python Slack Oncall Bot provides automated incident response capabilities through Slack integration. It serves as an alternative or complementary implementation to the TypeScript bot, offering Python-based integrations with operations tools and services.

## 3. Structure

```
slack-oncall-bot-python/
├── app/                   # Application source code
│   └── services/          # Service integrations and handlers
├── deploy/                # Deployment configurations
│   └── (Kubernetes manifests, Docker files)
├── scripts/               # Utility scripts
│   └── (build, deploy, and maintenance scripts)
└── AGENTS.md             # This file
```

## 4. Key Files

| File/Directory | Purpose |
|----------------|---------|
| `app/` | Main application code and business logic |
| `app/services/` | External service integrations |
| `deploy/` | Deployment configurations and manifests |
| `scripts/` | Automation and utility scripts |

## 5. Edit Rules (Must)

- Follow PEP 8 style guidelines
- Use type hints for all function signatures
- Document functions and classes with docstrings
- Add error handling for external API calls
- Use environment variables for configuration
- Run linting tools (flake8, pylint) before committing
- Format code with black
- Add unit tests for new functionality
- Use virtual environments for development

## 6. Forbidden Changes (Must Not)

- Hardcode credentials or API tokens
- Add dependencies without security review
- Remove error handling from critical paths
- Use blocking operations in async contexts
- Commit virtual environment directories
- Add untested code to production paths

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `pytest` or `python -m pytest` |
| Lint | `flake8 app/` or `pylint app/` |
| Format check | `black --check app/` |
| Type check | `mypy app/` |
| Integration | Test in development Slack workspace |

## 8. Dependencies & Contracts

- Python 3.9+
- slack-bolt or slack-sdk
- Asyncio for concurrent operations
- External APIs (PagerDuty, GitHub, etc.)
- RisingWave APIs for metrics
- Web framework (FastAPI/Flask for HTTP endpoints)

## 9. Overrides

Inherits from `/home/k11/risingwave/AGENTS.md`:
- Override: Edit Rules - Python-specific requirements
- Override: Test Entry - Python testing tools
- Override: Dependencies - Python ecosystem

## 10. Update Triggers

Regenerate this file when:
- Bot architecture changes
- New service integrations are added
- Deployment strategy changes
- Python version requirements change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/AGENTS.md
