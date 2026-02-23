# AGENTS.md - Lint UI Tests

## 1. Scope

Policies for the `lints/ui` directory, covering UI test files for custom Rust lint validation.

## 2. Purpose

The UI Tests module provides test cases that validate lint behavior by compiling code samples and verifying the expected lint output. These tests ensure custom lints (like format_error) correctly identify problematic patterns and produce helpful diagnostic messages.

## 3. Structure

```
lints/ui/
├── format_error.rs           # Test cases for error formatting lint
└── format_error.stderr       # Expected lint diagnostic output
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `format_error.rs` | Rust source with patterns that should trigger lints |
| `format_error.stderr` | Expected compiler/lint diagnostic output |

## 5. Edit Rules (Must)

- Test both positive and negative cases
- Document the lint being tested in comments
- Use clear variable names indicating test intent
- Include edge cases (references, boxes, dyn traits)
- Update .stderr files when lint messages change
- Organize test cases logically by pattern type

## 6. Forbidden Changes (Must Not)

- Remove test cases without coverage replacement
- Modify .stderr files without running actual tests
- Add test code that does not exercise lint logic
- Use non-deterministic patterns in tests
- Skip lint validation in CI

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| UI tests | `cargo test --examples` (from lints directory) |
| Specific lint test | `cargo test format_error` |
| Update expected output | `cargo test -- --bless` (if supported) |
| Full lint suite | `cargo test` (from lints directory) |

## 8. Dependencies & Contracts

- Rust nightly toolchain (from lints/rust-toolchain)
- cargo-dylint for lint execution
- UI test framework for output comparison
- Expected output files (.stderr) for validation

## 9. Overrides

Inherits from `./lints/AGENTS.md`:
- Override: Edit Rules - UI test-specific patterns
- Override: Test Entry - UI test commands

## 10. Update Triggers

Regenerate this file when:
- New lint UI tests are added
- UI test framework changes
- Lint diagnostic format changes
- New test categories are introduced

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./lints/AGENTS.md
