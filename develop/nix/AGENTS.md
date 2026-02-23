# AGENTS.md - Nix Development Environment

## 1. Scope

Policies for the develop/nix directory, covering Nix flake configuration for reproducible RisingWave development environments.

## 2. Purpose

The nix directory provides a declarative, reproducible development environment using Nix flakes. It ensures all developers have consistent tooling, dependencies, and build environments regardless of their host system (Linux, macOS).

## 3. Structure

```
nix/
├── flake.nix                  # Main Nix flake definition
├── flake.lock                 # Locked dependency versions
├── devshell.nix               # Development shell configuration
├── overlays.nix               # Nix package overlays
└── AGENTS.md                  # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `flake.nix` | Main flake with inputs, outputs, and system definitions |
| `flake.lock` | Pinned versions of all flake inputs |
| `devshell.nix` | Development shell with tools and environment |
| `overlays.nix` | Custom package definitions and modifications |

## 5. Edit Rules (Must)

- Pin all flake inputs to specific versions or commits
- Document purpose of each package in devshell.nix
- Test flake on all supported systems (x86_64-linux, aarch64-linux, etc.)
- Update flake.lock regularly with `nix flake update`
- Include common development tools (cargo, rustc, protobuf)
- Document how to use the development shell
- Keep flake.nix formatted with `nixpkgs-fmt`
- Include comments for non-obvious configuration

## 6. Forbidden Changes (Must Not)

- Use unpinned inputs that could break reproducibility
- Remove packages without checking downstream dependencies
- Commit broken or untested flake configurations
- Hardcode system-specific paths
- Remove support for supported platforms without notice
- Break `nix develop` command functionality
- Modify flake.lock manually (always use nix commands)

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Check flake | `nix flake check` |
| Enter devshell | `nix develop` |
| Build packages | `nix build` |
| Update lock | `nix flake update` |
| Format check | `nixpkgs-fmt --check *.nix` |
| Test all systems | `nix flake check --all-systems` |

## 8. Dependencies & Contracts

- Nix package manager (2.4+ with flakes enabled)
- Nix flake inputs:
  - nixpkgs (NixOS/nixpkgs/nixos-unstable)
  - flake-parts (hercules-ci/flake-parts)
  - devshell (numtide/devshell)
  - rust-overlay (oxalica/rust-overlay)
- Supported systems: x86_64-linux, aarch64-linux, aarch64-darwin, x86_64-darwin

## 9. Overrides

Inherits from `/home/k11/risingwave/develop/AGENTS.md`:
- Override: Test Entry - Nix-specific validation required
- Override: Dependencies - Nix ecosystem dependencies

## 10. Update Triggers

Regenerate this file when:
- New flake inputs are added
- Development shell tools change
- Supported system architectures change
- Nix configuration structure changes
- Rust toolchain versions are updated

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/develop/AGENTS.md
