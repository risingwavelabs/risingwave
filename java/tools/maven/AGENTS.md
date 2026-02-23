# AGENTS.md - Maven Tools

## 1. Scope

Policies for the `java/tools/maven` directory, covering Maven build configurations and code quality tools for the Java codebase.

## 2. Purpose

The Maven Tools module provides shared Maven configurations for code quality enforcement across all Java modules in RisingWave. It includes Checkstyle rules for consistent code formatting, style enforcement, and suppression configurations for legacy code exceptions.

## 3. Structure

```
maven/
├── checkstyle.xml               # Main Checkstyle configuration
└── suppressions.xml             # Checkstyle suppression rules
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `checkstyle.xml` | Checkstyle rules based on Apache Beam conventions |
| `suppressions.xml` | Exception rules for specific files or patterns |

## 5. Edit Rules (Must)

- Follow Apache Beam style as base configuration
- Document new rules with clear rationale
- Update suppressions for legacy code exemptions
- Test checkstyle changes against all Java modules
- Keep rules consistent with Google Java Format (AOSP)
- Version control all configuration changes
- Ensure IDE compatibility with checkstyle rules
- Maintain backward compatibility when possible
- Update parent POM references when needed

## 6. Forbidden Changes (Must Not)

- Remove critical style rules without team approval
- Add overly restrictive rules that break existing builds
- Modify suppressions without documented justification
- Break Maven checkstyle plugin compatibility
- Hardcode paths in configuration files
- Skip validation before committing changes

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Checkstyle validation | `mvn checkstyle:check` |
| Generate report | `mvn checkstyle:checkstyle` |
| Spotless check | `mvn spotless:check` |
| Spotless format | `mvn spotless:apply` |
| Full validation | `mvn verify` |

## 8. Dependencies & Contracts

- Maven 3.8+ for build execution
- Checkstyle 10.x plugin
- Google Java Format (AOSP variant)
- Integration with Maven build lifecycle
- Parent POM references from `java/pom.xml`
- Module path: `${project.basedir}/../tools/maven/`

## 9. Overrides

Inherits from `./java/tools/AGENTS.md` and `./java/AGENTS.md`:
- Override: Checkstyle-specific configuration rules
- Override: Code quality enforcement requirements

## 10. Update Triggers

Regenerate this file when:
- Checkstyle rules are added or removed
- Maven plugin versions change
- Style guide conventions are updated
- New suppression patterns are needed
- Build quality requirements evolve

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./java/tools/AGENTS.md
