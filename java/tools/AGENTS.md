# AGENTS.md - Java Development Tools

## 1. Scope

Policies for the `java/tools` directory, covering Maven configurations and development tools for the Java codebase.

## 2. Purpose

The tools module provides shared Maven configurations for code quality enforcement across all Java modules in RisingWave. This includes Checkstyle rules for consistent code formatting and style enforcement.

## 3. Structure

```
java/tools/
└── maven/
    ├── checkstyle.xml       # Checkstyle rule definitions
    └── suppressions.xml     # Checkstyle suppression rules
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `maven/checkstyle.xml` | Main Checkstyle configuration based on Apache Beam style |
| `maven/suppressions.xml` | Exception rules for specific files or patterns |

## 5. Edit Rules (Must)

- Follow Apache Beam style conventions as base
- Document any new rules with rationale
- Update suppressions when legacy code needs exemptions
- Test checkstyle changes against all Java modules
- Keep rules consistent with Google Java Format (AOSP)
- Version control changes with clear commit messages

## 6. Forbidden Changes (Must Not)

- Remove critical style rules without team approval
- Add overly restrictive rules that break builds
- Modify suppressions without justification
- Break compatibility with Maven checkstyle plugin
- Hardcode paths in configuration files

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Checkstyle validation | `mvn checkstyle:check` |
| Checkstyle format | `mvn checkstyle:checkstyle` |
| Spotless check | `mvn spotless:check` |
| Full validation | `mvn verify` |

## 8. Dependencies & Contracts

- Maven 3.8+
- Checkstyle 10.x plugin
- Google Java Format (AOSP variant)
- Integration with Maven build lifecycle
- Parent POM references from java/pom.xml

## 9. Overrides

Inherits from `./java/AGENTS.md`:
- Override: Edit Rules - Checkstyle-specific configurations
- Override: Test Entry - Maven checkstyle commands

## 10. Update Triggers

Regenerate this file when:
- Checkstyle rules are added or removed
- Maven plugin versions change
- Style guide conventions are updated
- New suppression patterns are needed

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./java/AGENTS.md
