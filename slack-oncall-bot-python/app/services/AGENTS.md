# AGENTS.md - Slack Bot Services

## 1. Scope

Policies for the slack-oncall-bot-python/app/services directory.

## 2. Purpose

The services module provides business logic services for the Slack on-call bot. It handles on-call rotation management, incident tracking, and Slack integrations.

## 3. Structure

```
services/
(empty - awaiting implementation)
```

## 4. Key Files

| File | Purpose |
|------|---------|
| (Module in development) | Future service implementations |

## 5. Edit Rules (Must)

- Follow service-oriented architecture
- Implement proper error handling
- Use dependency injection
- Document service interfaces
- Add unit tests for services
- Handle Slack API rate limits

## 6. Forbidden Changes (Must Not)

- Mix business logic with presentation
- Hardcode configuration
- Skip error handling for external APIs

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `pytest` |

## 8. Dependencies & Contracts

- Python 3.9+
- Slack SDK
- FastAPI (if web components)

## 9. Overrides

Inherits from `./slack-oncall-bot-python/AGENTS.md`:
- Override: Service layer patterns

## 10. Update Triggers

Regenerate this file when:
- Service architecture is defined
- New services are added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./slack-oncall-bot-python/AGENTS.md
