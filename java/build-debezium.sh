#!/usr/bin/env bash

set -euo pipefail

JAVA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "${JAVA_DIR}")"
DEBEZIUM_DIR="${JAVA_DIR}/debezium"
MODULES="debezium-bom,debezium-embedded,debezium-connector-mysql,debezium-connector-postgres,debezium-connector-mongodb,debezium-connector-sqlserver,debezium-connector-oracle"
VERSIONS_PLUGIN="org.codehaus.mojo:versions-maven-plugin:2.18.0"

DEBEZIUM_VERSION="$(sed -n 's|.*<debezium.version>\(.*\)</debezium.version>.*|\1|p' "${JAVA_DIR}/pom.xml")"
if [[ -z "${DEBEZIUM_VERSION}" ]]; then
  echo "Cannot read debezium.version from ${JAVA_DIR}/pom.xml" >&2
  exit 1
fi

if [[ ! -f "${DEBEZIUM_DIR}/pom.xml" ]]; then
  if ! git -C "${REPO_ROOT}" rev-parse --git-dir >/dev/null 2>&1; then
    echo "java/debezium is empty and ${REPO_ROOT} is not a git repository, so the submodule cannot be checked out here." >&2
    echo "Run 'git submodule update --init java/debezium' in the git checkout first, before 'docker build' for image builds." >&2
    exit 1
  fi
  git -C "${REPO_ROOT}" submodule update --init java/debezium
fi

LOCAL_REPO="${HOME}/.m2/repository"
for arg in "$@"; do
  case "${arg}" in
    -Dmaven.repo.local=*) LOCAL_REPO="${arg#-Dmaven.repo.local=}" ;;
  esac
done
STAMP_FILE="${LOCAL_REPO}/io/debezium/.risingwave-fork-stamp"

cd "${DEBEZIUM_DIR}"

restore_poms() {
  ./mvnw -B -ntp -q "$@" "${VERSIONS_PLUGIN}:revert"
}

if [[ -n "$(find . -name pom.xml.versionsBackup -print -quit)" ]]; then
  restore_poms "$@"
fi

working_tree_stamp() {
  local index_dir tree
  index_dir="$(mktemp -d)"
  # Use a temporary index so that `git add --all` can include every changed and untracked file in the
  # tree hash without staging those changes in the real index.
  if GIT_INDEX_FILE="${index_dir}/index" git -c safe.directory='*' add --all &&
    tree="$(GIT_INDEX_FILE="${index_dir}/index" git -c safe.directory='*' write-tree)"; then
    echo "${DEBEZIUM_VERSION} ${tree} $(git hash-object "${JAVA_DIR}/build-debezium.sh")"
  fi
  rm -rf "${index_dir}"
}

# This assumes it is only called when the stamp file exists and its first line matches the current stamp.
# The stamp file is only written after a successful build, and that build records every file it installed
# on the lines after the stamp, so the records always describe the build the stamp refers to.
fork_installed() {
  local path count=0
  while IFS= read -r path; do
    [[ -f "${LOCAL_REPO}/${path}" ]] || return 1
    count=$((count + 1))
  done < <(tail -n +2 "${STAMP_FILE}")
  ((count > 0))
}

STAMP="$(working_tree_stamp 2>/dev/null)"
if [[ -n "${STAMP}" && -f "${STAMP_FILE}" && "$(head -n 1 "${STAMP_FILE}")" == "${STAMP}" ]] && fork_installed; then
  echo "Debezium fork ${DEBEZIUM_VERSION} is up to date, skipping its build"
  exit 0
fi

rm -f "${STAMP_FILE}"
trap 'restore_poms "$@"' EXIT

./mvnw -B -ntp -q "$@" "${VERSIONS_PLUGIN}:set" \
  -DnewVersion="${DEBEZIUM_VERSION}" \
  -DgenerateBackupPoms=true

./mvnw -B -ntp "$@" -pl "${MODULES}" -am install \
  -DskipTests \
  -DskipITs \
  -Dformat.skip=true \
  -Dcheckstyle.skip=true \
  -Drevapi.skip=true \
  -Dmaven.javadoc.skip=true \
  -Dmaven.source.skip=true \
  -Dmaven.compiler.proc=full

if [[ -n "${STAMP}" ]]; then
  mkdir -p "$(dirname "${STAMP_FILE}")"
  {
    echo "${STAMP}"
    (cd "${LOCAL_REPO}" && find io/debezium -path "*/${DEBEZIUM_VERSION}/*" -type f \( -name '*.pom' -o -name '*.jar' \) | sort)
  } > "${STAMP_FILE}.tmp"
  mv "${STAMP_FILE}.tmp" "${STAMP_FILE}"
fi
