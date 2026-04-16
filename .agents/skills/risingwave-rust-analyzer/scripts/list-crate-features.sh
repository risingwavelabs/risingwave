#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: list-crate-features.sh [package] [--features a,b] [--no-default-features] [--no-all-targets]

List RisingWave crate features and suggest Cargo / rust-analyzer commands.

Arguments:
  package                Exact or partial Cargo package name. Omit to list all packages.

Options:
  --features LIST        Comma-separated features to include in the suggested commands.
  --no-default-features  Include --no-default-features in the suggested commands.
  --no-all-targets       Omit --all-targets from the rust-analyzer override command.
  -h, --help             Show this help message.
EOF
}

package_query=""
selected_features=""
no_default_features=false
include_all_targets=true

while (($# > 0)); do
  case "$1" in
    --features)
      if (($# < 2)); then
        echo "missing value for --features" >&2
        exit 1
      fi
      selected_features="$2"
      shift 2
      ;;
    --no-default-features)
      no_default_features=true
      shift
      ;;
    --no-all-targets)
      include_all_targets=false
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    -*)
      echo "unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
    *)
      if [[ -n "$package_query" ]]; then
        echo "only one package argument is supported" >&2
        usage >&2
        exit 1
      fi
      package_query="$1"
      shift
      ;;
  esac
done

metadata="$(cargo metadata --no-deps --format-version 1)"
workspace_root="$(jq -r '.workspace_root' <<<"$metadata")"

relative_manifest() {
  local manifest_path="$1"
  if [[ "$manifest_path" == "$workspace_root/"* ]]; then
    printf '%s\n' "${manifest_path#"$workspace_root"/}"
  else
    printf '%s\n' "$manifest_path"
  fi
}

emit_package_list() {
  jq -r '
    .packages
    | sort_by(.name)
    | .[]
    | [.name, .manifest_path] | @tsv
  ' <<<"$metadata" | while IFS=$'\t' read -r name manifest_path; do
    printf '%s\t%s\n' "$name" "$(relative_manifest "$manifest_path")"
  done
}

matched_packages_json() {
  local exact_matches
  exact_matches="$(jq -c --arg query "$package_query" '
    [.packages[] | select(.name == $query)] | sort_by(.name)
  ' <<<"$metadata")"

  if [[ "$exact_matches" != "[]" ]]; then
    printf '%s\n' "$exact_matches"
    return
  fi

  jq -c --arg query "$package_query" '
    [.packages[] | select(.name | ascii_downcase | contains($query | ascii_downcase))] | sort_by(.name)
  ' <<<"$metadata"
}

print_feature_lines() {
  local package_json="$1"
  local feature_count
  feature_count="$(jq '.features | length' <<<"$package_json")"

  if [[ "$feature_count" == "0" ]]; then
    echo "  - (none)"
    return
  fi

  jq -r '
    .features
    | to_entries
    | sort_by(.key)
    | .[]
    | if (.value | length) == 0
      then "  - \(.key): (empty)"
      else "  - \(.key): \(.value | join(", "))"
      end
  ' <<<"$package_json"
}

build_cargo_command() {
  local package_name="$1"
  local command=("cargo" "check" "-p" "$package_name")

  if [[ "$no_default_features" == true ]]; then
    command+=("--no-default-features")
  fi
  if [[ -n "$selected_features" ]]; then
    command+=("--features" "$selected_features")
  fi

  printf '%s\n' "${command[*]}"
}

build_override_command() {
  local package_name="$1"

  jq -nc \
    --arg package_name "$package_name" \
    --arg features "$selected_features" \
    --argjson no_default_features "$no_default_features" \
    --argjson include_all_targets "$include_all_targets" '
    [
      "cargo",
      "check",
      "-p",
      $package_name
    ]
    + (if $no_default_features then ["--no-default-features"] else [] end)
    + (if ($features | length) > 0 then ["--features", $features] else [] end)
    + (if $include_all_targets then ["--all-targets"] else [] end)
    + ["--message-format=json"]
  '
}

print_package_summary() {
  local package_json="$1"
  local package_name
  local manifest_path
  local edition

  package_name="$(jq -r '.name' <<<"$package_json")"
  manifest_path="$(jq -r '.manifest_path' <<<"$package_json")"
  edition="$(jq -r '.edition' <<<"$package_json")"

  echo "package: $package_name"
  echo "manifest: $(relative_manifest "$manifest_path")"
  echo "edition: $edition"
  echo "features:"
  print_feature_lines "$package_json"
  echo "suggested cargo check:"
  echo "  $(build_cargo_command "$package_name")"
  echo "suggested rust-analyzer.check.overrideCommand:"
  echo "  $(build_override_command "$package_name")"
}

if [[ -z "$package_query" ]]; then
  emit_package_list
  exit 0
fi

matches="$(matched_packages_json)"
if [[ "$matches" == "[]" ]]; then
  echo "no package matched: $package_query" >&2
  exit 1
fi

first=true
while IFS= read -r package_json; do
  if [[ "$first" == false ]]; then
    echo
  fi
  first=false
  print_package_summary "$package_json"
done < <(jq -c '.[]' <<<"$matches")
