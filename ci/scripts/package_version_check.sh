#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: package_version_check.sh [options]

Checks that main branch Cargo.toml version is bumped to the next minor once any
release-X.Y branch exists (suffix branches like release-X.Y-foo are ignored).

Options:
  --cargo-toml PATH         Path to Cargo.toml (default: Cargo.toml)
  --remote NAME             Remote name to scan (default: origin)
EOF
}

cargo_toml="Cargo.toml"
remote="origin"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cargo-toml)
      cargo_toml="${2:-}"; shift 2 ;;
    --remote)
      remote="${2:-}"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2 ;;
  esac
done

if [[ ! -f "$cargo_toml" ]]; then
  echo "Cargo.toml not found: $cargo_toml" >&2
  exit 2
fi

read_branches() {
  git fetch --all --prune >/dev/null

  git for-each-ref "refs/remotes/${remote}/release-*" --format '%(refname:short)'
}

all_release_branches="$(read_branches)"
echo "Release branches (remote: ${remote}):"
if [[ -n "$all_release_branches" ]]; then
  echo "$all_release_branches"
else
  echo "(none)"
  echo "Error: failed to find any ${remote}/release-* branches; refusing to bypass version check." >&2
  exit 1
fi

latest_major=-1
latest_minor=-1
latest_branch=""

while IFS= read -r branch; do
  [[ -z "$branch" ]] && continue

  if [[ "$branch" =~ ^${remote}/release-([0-9]+)\.([0-9]+)$ ]]; then
    major="${BASH_REMATCH[1]}"
    minor="${BASH_REMATCH[2]}"

    if [[ "$latest_major" -lt 0 ]] \
      || [[ "$major" -gt "$latest_major" ]] \
      || { [[ "$major" -eq "$latest_major" ]] && [[ "$minor" -gt "$latest_minor" ]]; }; then
      latest_major="$major"
      latest_minor="$minor"
      latest_branch="$branch"
    fi
  fi
done < <(printf '%s\n' "$all_release_branches")

if [[ "$latest_major" -lt 0 ]]; then
  echo "No ${remote}/release-X.Y branches found; skipping."
  exit 0
fi

required_major="$latest_major"
required_minor="$((latest_minor + 1))"

full_cargo_version="$(
  awk -F\" '
    /^[[:space:]]*version[[:space:]]*=/ {
      print $2;
      exit
    }
  ' "$cargo_toml"
)"
if [[ -z "$full_cargo_version" ]]; then
  echo "Failed to read version from $cargo_toml (expected: version = \"X.Y...\")" >&2
  exit 2
fi

if [[ "$full_cargo_version" =~ ^([0-9]+)\.([0-9]+) ]]; then
  cargo_major="${BASH_REMATCH[1]}"
  cargo_minor="${BASH_REMATCH[2]}"
else
  echo "Failed to parse major.minor from version: ${full_cargo_version}" >&2
  exit 2
fi

echo "Detected latest release series: ${latest_major}.${latest_minor} (from ${latest_branch})"
echo "Required main Cargo.toml version: >= ${required_major}.${required_minor}"
echo "Current Cargo.toml version: ${full_cargo_version}"

if [[ "$cargo_major" -lt "$required_major" ]] \
  || { [[ "$cargo_major" -eq "$required_major" ]] && [[ "$cargo_minor" -lt "$required_minor" ]]; }; then
  echo "Error: Cargo.toml version ${full_cargo_version} must be bumped to at least ${required_major}.${required_minor} once ${latest_major}.${latest_minor} release branches exist." >&2
  exit 1
fi

echo "OK: Cargo.toml version ${full_cargo_version} is >= ${required_major}.${required_minor}"
