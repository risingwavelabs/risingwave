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
