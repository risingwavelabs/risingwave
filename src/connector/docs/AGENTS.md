# AGENTS.md - Connector Documentation

## 1. Scope

Policies for the `src/connector/docs/` directory containing connector-specific documentation assets.

## 2. Purpose

The docs directory houses documentation files related to connector implementations. This includes architecture diagrams, protocol specifications, and integration guides for connector developers.

## 3. Structure

```
src/connector/docs/
└── (empty - reserved for future documentation)
```

## 4. Key Files

| File | Purpose |
|------|---------|
| (To be added) | Connector architecture diagrams |
| (To be added) | Protocol integration guides |
| (To be added) | Connector development documentation |

## 5. Edit Rules (Must)

- Write all documentation in English
- Use Markdown format for text documents
- Include diagrams in SVG or PNG format
- Reference official external documentation where applicable
- Keep connector guides synchronized with code changes

## 6. Forbidden Changes (Must Not)

- Commit large binary files without compression
- Include sensitive connector credentials in examples
- Add outdated protocol documentation

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| N/A | Documentation only directory |

## 8. Dependencies & Contracts

- Documentation format: Markdown
- Diagram format: SVG preferred, PNG accepted
- Links: Must reference stable external URLs

## 9. Overrides

None. Inherits rules from parent `src/connector/AGENTS.md`.

## 10. Update Triggers

Regenerate this file when:
- New documentation category added
- Documentation structure reorganized
- New diagram or specification added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/src/connector/AGENTS.md
