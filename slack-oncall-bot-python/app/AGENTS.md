# AGENTS.md - Slack Bot Application

## 1. Scope

Policies for the `slack-oncall-bot-python/app` directory, covering the core Python application code for the on-call Slack bot.

## 2. Purpose

The App module contains the core business logic and service integrations for the Slack on-call bot. It handles Slack event processing, on-call operations, incident management workflows, and integrations with external services like PagerDuty and GitHub.

## 3. Structure

```
slack-oncall-bot-python/app/
└── services/                # Service layer for external integrations
    └── (service modules)
```

## 4. Key Files

| File/Directory | Purpose |
|----------------|---------|
| `services/` | External API integrations and business logic |

## 5. Edit Rules (Must)

- Follow PEP 8 style guidelines
- Use type hints for all function signatures
- Document functions and classes with docstrings
- Handle errors gracefully with try-except blocks
- Use async/await for I/O operations
- Implement proper logging for debugging
- Use environment variables for configuration
- Add unit tests for new service modules

## 6. Forbidden Changes (Must Not)

- Hardcode credentials or API tokens
- Remove error handling from service calls
- Use blocking I/O in async contexts
- Commit debug print statements
- Add circular dependencies between modules
- Skip input validation on user commands

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `pytest app/` |
| Lint check | `flake8 app/` |
| Type check | `mypy app/` |
| Format check | `black --check app/` |

## 8. Dependencies & Contracts

- Python 3.9+
- slack-bolt for Slack app framework
- aiohttp for async HTTP requests
- External APIs (PagerDuty, GitHub, etc.)
- RisingWave APIs for metrics retrieval
- Environment-based configuration

## 9. Overrides

Inherits from `./slack-oncall-bot-python/AGENTS.md`:
- Override: Edit Rules - Application code requirements
- Override: Test Entry - App-specific test commands

## 10. Update Triggers

Regenerate this file when:
- New service modules are added
- Bot architecture changes
- New integrations are implemented
- Framework or library updates

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./slack-oncall-bot-python/AGENTS.md
