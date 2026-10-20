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
  git -C "${REPO_ROOT}" submodule update --init java/debezium
fi

LOCAL_REPO="${HOME}/.m2/repository"
for arg in "$@"; do
  case "${arg}" in
    -Dmaven.repo.local=*) LOCAL_REPO="${arg#-Dmaven.repo.local=}" ;;
  esac
done
STAMP_FILE="${LOCAL_REPO}/io/debezium/.risingwave-fork-stamp"
INSTALLED_JAR="${LOCAL_REPO}/io/debezium/debezium-core/${DEBEZIUM_VERSION}/debezium-core-${DEBEZIUM_VERSION}.jar"

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
  if GIT_INDEX_FILE="${index_dir}/index" git -c safe.directory='*' add --all &&
    tree="$(GIT_INDEX_FILE="${index_dir}/index" git -c safe.directory='*' write-tree)"; then
    echo "${DEBEZIUM_VERSION} ${tree} $(git hash-object "${JAVA_DIR}/build-debezium.sh")"
  fi
  rm -rf "${index_dir}"
}

STAMP="$(working_tree_stamp 2>/dev/null)"
if [[ -n "${STAMP}" && -f "${INSTALLED_JAR}" && -f "${STAMP_FILE}" && "$(cat "${STAMP_FILE}")" == "${STAMP}" ]]; then
  echo "Debezium fork ${DEBEZIUM_VERSION} is up to date, skipping its build"
  exit 0
fi

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
  echo "${STAMP}" > "${STAMP_FILE}"
fi
