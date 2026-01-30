#!/usr/bin/env bash
#
# Diagnose "cannot allocate memory in static TLS block" issues for a given ELF shared object.
# Run this inside the same Linux container/image where the failure happens.
#
# What it prints:
# - DT_NEEDED entries (dependency names)
# - ldd resolved dependency paths
# - for target + each resolved dependency:
#   - whether it has a PT_TLS segment
#   - .tdata/.tbss section sizes
#   - a small sample of TLS relocations (TPOFF/GOTTPOFF/DTPOFF/DTPMOD)
#
# NOTE: "static TLS block" failures are usually triggered when a dlopen() occurs late
# (e.g., JVM System.load for JNI), and the newly loaded object (or one of its deps)
# requires initial-exec/local-exec TLS (often seen as TPOFF* relocations).
set -euo pipefail

so_path="${1:-}"
if [[ -z "${so_path}" || ! -f "${so_path}" ]]; then
  echo "usage: $0 /path/to/libfoo.so" >&2
  exit 2
fi

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 3; }
}

need_cmd objdump
need_cmd readelf
need_cmd ldd
need_cmd awk
need_cmd sed
need_cmd grep
need_cmd sort
need_cmd head

echo "== target"
echo "${so_path}"
echo

echo "== DT_NEEDED (names)"
objdump -p "${so_path}" 2>/dev/null | awk '$1=="NEEDED"{print $2}' || true
echo

echo "== ldd (resolved paths)"
ldd "${so_path}" || true
echo

echo "== TLS symbols (target)"
# If there are defined TLS symbols here, they are usually the ones requiring TLS storage.
readelf -W -s "${so_path}" 2>/dev/null | awk '$4=="TLS"{print}' || true
echo
echo "== TLS suspects (target)"
# Heuristic: tikv's jemalloc uses a TLS var like `_rjem_je_tsd_tls` which is frequently implicated in
# `cannot allocate memory in static TLS block` when a .so is loaded late (e.g. JNI System.load).
readelf -W -s "${so_path}" 2>/dev/null | awk '$4=="TLS"{print}' | grep -E '_rjem_|jemalloc|Jemalloc' || true
echo

deps=$(
  ldd "${so_path}" 2>/dev/null \
    | awk '
        $2=="=>" && $3 ~ /^\// { print $3 }
        $1 ~ /^\// { print $1 }
      ' \
    | sort -u
)

inspect_one() {
  local f="$1"
  echo "---- ${f}"

  if readelf -W -l "${f}" 2>/dev/null | grep -q ' TLS '; then
    echo "PT_TLS: yes"
  else
    echo "PT_TLS: no"
  fi

  # TLS section sizes
  readelf -W -S "${f}" 2>/dev/null | grep -E '\.(tdata|tbss)\b' || true

  # TLS symbol table entries, if any (often empty for system libs).
  readelf -W -s "${f}" 2>/dev/null | awk '$4=="TLS"{print}' | sed -n '1,30p' || true

  # Sometimes objdump prints relocation symbol names even when readelf output is sparse.
  objdump -R "${f}" 2>/dev/null | grep -E 'TPOFF|GOTTPOFF|DTPMOD|DTPOFF' | head -n 30 || true

  # Heuristic for IE/LE TLS models
  rels="$(readelf -W -r "${f}" 2>/dev/null | grep -E 'TPOFF|GOTTPOFF|DTPMOD|DTPOFF' || true)"
  if [[ -n "${rels}" ]]; then
    echo "TLS relocs (sample):"
    echo "${rels}" | sed -n '1,20p'
  else
    echo "TLS relocs: none (or not shown)"
  fi

  # Full TPOFF reloc list tends to point directly to the offending TLS symbol.
  tpoff="$(readelf -W -r "${f}" 2>/dev/null | grep -E 'TPOFF' || true)"
  if [[ -n "${tpoff}" ]]; then
    echo "TPOFF relocs (up to 50):"
    echo "${tpoff}" | sed -n '1,50p'
  fi
}

echo "== TLS scan (target + deps)"
inspect_one "${so_path}"
if [[ -n "${deps}" ]]; then
  while IFS= read -r f; do
    [[ -f "${f}" ]] || continue
    inspect_one "${f}"
  done <<< "${deps}"
fi

echo
echo "Tips:"
echo "- Prime suspects are deps with PT_TLS: yes + many TPOFF* relocs."
echo "- Use LD_DEBUG=libs on the failing java command to confirm the last loaded .so."
