# Rust Analyzer CLI for RisingWave

## Commands

- `rust-analyzer diagnostics .`
  - Use for a quick diagnostics sweep.
- `rust-analyzer unresolved-references .`
  - Use when imports, modules, or symbols look broken.
- `rust-analyzer analysis-stats . --parallel`
  - Use for deeper semantic checking across the current root.
- `rust-analyzer analysis-stats . -o src/meta/src/manager/catalog/mod.rs`
  - Use to narrow analysis to one file or subtree.
- `rust-analyzer prime-caches .`
  - Use before a long session on a cold workspace.
- `rust-analyzer search '$a.foo($b)'`
  - Use for structural search instead of plain text grep when shape matters.
- `rust-analyzer ssr '$a.foo($b) ==>> bar($a, $b)'`
  - Use for structured replacement planning or execution.
- `cat path/to/file.rs | rust-analyzer parse`
  - Use for syntax-level inspection.
- `cat path/to/file.rs | rust-analyzer symbols`
  - Use for a quick symbol outline.

## RisingWave Defaults

- Open `/home/k11/risingwave` to inherit `rust-toolchain` and `.cargo/config.toml`.
- Keep `rust-analyzer.cargo.buildScripts.enable = true`.
- Keep `rust-analyzer.procMacro.enable = true`.
- Expect macro-heavy modules and generated code.

## Feature Flag Reality

`rust-analyzer --print-config-schema` exposes:

- `rust-analyzer.cargo.features`
- `rust-analyzer.cargo.noDefaultFeatures`
- `rust-analyzer.check.features`
- `rust-analyzer.check.noDefaultFeatures`
- `rust-analyzer.check.overrideCommand`
- `rust-analyzer.linkedProjects`

There is no first-class setting for "package A uses features X while package B uses features Y" inside one shared workspace root. Treat features as a linked-project-level concern.

## Practical Feature Strategies

### Strategy 1: Narrow the root

Open the crate directory or its `Cargo.toml` if one package needs a special feature set and you do not need full-workspace context.

### Strategy 2: Split linked projects

Use separate editor workspaces or linked projects when different crates need different feature sets.

### Strategy 3: Override diagnostics command

Use `rust-analyzer.check.overrideCommand` to align save-time diagnostics with a specific package:

```json
{
  "rust-analyzer.check.overrideCommand": [
    "cargo",
    "check",
    "-p",
    "risingwave_meta",
    "--features",
    "test",
    "--all-targets",
    "--message-format=json"
  ]
}
```

Use `--no-default-features` when needed:

```json
{
  "rust-analyzer.check.overrideCommand": [
    "cargo",
    "check",
    "-p",
    "risingwave_connector",
    "--no-default-features",
    "--features",
    "sink-doris",
    "--all-targets",
    "--message-format=json"
  ]
}
```

## Verification Pattern

When feature selection matters:

1. Run `scripts/list-crate-features.sh <package>`.
2. Pick the package and features explicitly.
3. Compare rust-analyzer output with `cargo check -p <package> ...`.
4. Only trust feature-specific claims after both tools agree.
