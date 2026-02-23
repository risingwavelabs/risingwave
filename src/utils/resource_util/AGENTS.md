# AGENTS.md - Resource Util Crate

## 1. Scope

Policies for the `rw_resource_util` crate providing system resource detection and monitoring.

## 2. Purpose

The resource util crate provides:
- **CPU Detection**: Core count, NUMA topology detection
- **Memory Information**: Total, available, cgroup memory limits
- **Disk Information**: Available space, I/O statistics
- **Network Detection**: Interface enumeration, bandwidth estimation
- **Container Awareness**: cgroup v1/v2 resource limits

This enables RisingWave to adapt to available system resources.

## 3. Structure

```
src/utils/resource_util/
├── Cargo.toml                    # Crate manifest
└── src/
    └── lib.rs                    # Resource detection functions
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | `cpu_count()`, `memory_info()`, `disk_info()` functions |

## 5. Edit Rules (Must)

- Write all code comments in English
- Run `cargo fmt` before committing Rust code
- Handle container environments (cgroup limits) correctly
- Fall back gracefully when detection fails
- Cache expensive detection results
- Test on both bare metal and containerized environments
- Pass `./risedev c` (clippy) before submitting changes

## 6. Forbidden Changes (Must Not)

- Panic on resource detection failure (must return Option/Result)
- Ignore cgroup limits in container environments
- Use blocking I/O for resource queries
- Hardcode resource values for specific platforms
- Remove platform support without explicit deprecation

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Unit tests | `cargo test -p rw_resource_util` |
| Clippy check | `./risedev c` |

## 8. Dependencies & Contracts

- **sysinfo**: System information querying
- **fs-err**: Filesystem error handling
- **hostname**: Hostname detection

Contracts:
- Functions return `Option` or `Result` for all fallible operations
- Cgroup-aware when running in containers
- Cached results are invalidated appropriately
- No blocking syscalls in async contexts

## 9. Overrides

None. Follows parent `src/utils/AGENTS.md` rules.

## 10. Update Triggers

Regenerate this file when:
- New resource type detection added
- Container runtime support changes
- Platform-specific detection modified

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./src/utils/AGENTS.md
