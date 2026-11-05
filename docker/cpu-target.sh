#!/usr/bin/env bash

# Shared by image builds and publishers. An empty suffix preserves existing tags.
RW_TARGET_CPU=${RW_TARGET_CPU-generic}
case "$RW_TARGET_CPU" in
  generic) cpu_suffix="" ;;
  neoverse-n1|neoverse-v1|neoverse-v2) cpu_suffix="-${RW_TARGET_CPU}" ;;
  *)
    echo "Unsupported RW_TARGET_CPU: ${RW_TARGET_CPU}" >&2
    exit 1
    ;;
esac

# CPU suffixes are reserved: retagging must preserve the hardware requirement.
validate_cpu_retag() {
  local source_suffix="" target_suffix=""
  if [[ "$1" =~ (-neoverse-(n1|v1|v2))$ ]]; then
    source_suffix="${BASH_REMATCH[1]}"
  fi
  if [[ "$2" =~ (-neoverse-(n1|v1|v2))$ ]]; then
    target_suffix="${BASH_REMATCH[1]}"
  fi
  if [[ "$source_suffix" != "$target_suffix" ]]; then
    echo "Retagging must preserve the CPU suffix: $1 -> $2" >&2
    return 1
  fi
}
